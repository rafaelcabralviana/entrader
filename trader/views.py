from __future__ import annotations

import logging
import json
import math
import re
import sys
import time as time_mod
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST
from django.core.paginator import Paginator
from django.db import OperationalError
from django.db.models import Count, Q, Sum
from django.db.models.functions import Upper
from django.core.cache import cache

from trader.environment import (
    ENV_REAL,
    ENV_REPLAY,
    ENV_SIMULATOR,
    environment_label,
    get_current_environment,
    get_session_environment,
    normalize_environment,
    set_current_environment,
    set_session_environment,
    strategy_toggle_storage_environment,
)
from trader.market_defaults import (
    default_primary_ticker,
    default_ticker_suggestions_daytrade,
    default_ticker_suggestions_equities,
)
from trader.automacoes.prefs import (
    get_strategy_enabled_map,
    get_strategy_execute_orders_map,
    get_strategy_params,
    save_strategy_toggles_from_post,
    trailing_stop_adjustment_enabled,
)
from trader.automacoes.profiles import (
    create_sim_profile,
    list_profiles,
    resolve_active_profile,
    set_active_profile,
    start_profile_runtime,
)
from trader.automacoes.runtime import runtime_enabled, runtime_max_open_operations, set_runtime_enabled
from trader.automacoes.strategies import AUTOMATION_STRATEGIES, strategy_display_dict
from trader.automacoes.simulation import (
    clear_all_market_day_sessions,
    get_automation_market_simulation,
    set_automation_market_simulation,
)
from trader.automacoes.sim_sync import (
    clear_automation_sim_preference_for_user,
    sync_automation_sim_preference_from_request,
)
from trader.automacoes.universal_bracket_trailing import (
    BRACKET_LANE_REPLAY_SHADOW,
    BRACKET_LANE_STANDARD,
    state_cache_key,
)
from trader.automacoes.thoughts import (
    calendar_day_bounds_brt,
    fetch_thoughts_for_poll,
    parse_calendar_day_brt,
    passive_insight_cards_from_thoughts,
    record_automation_thought,
    thought_to_dict,
)
from trader.panel_context import (
    build_collateral_custody_context,
    merge_replay_shadow_custody_panel,
    _replay_shadow_custody_panel,
    build_market_context_local,
    build_market_context_local_for_session_day,
    build_orders_context,
    get_daytrade_chip_suggestions,
    invalidate_collateral_custody_cache,
    json_sanitize,
    get_daytrade_candidates_text_context,
    resolve_daytrade_base_ticker,
    resolve_ticker_for_local_snapshots,
    quote_status_is_end_of_day,
    run_order_test_form,
    set_daytrade_candidates_text,
)
from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)
from trader.services.quote_history import compute_quote_latency_ms, save_quote_snapshot
from trader.services.replay_shadow_ledger import delete_replay_shadow_ledger
from trader.services.orders import (
    fetch_custody,
    invalidate_intraday_orders_cache,
    post_cancel_order,
    post_send_market_order,
)
from trader.order_enums import ORDER_MODULE_DAY_TRADE, ORDER_SIDE_BUY, ORDER_SIDE_SELL, ORDER_TIF_DAY
from trader.models import (
    AutomationMarketSimPreference,
    AutomationStrategyToggle,
    AutomationTriggerMarker,
    AutomationThought,
    WatchedTicker,
    QuoteSnapshot,
    BookSnapshot,
    Position,
    PositionLiquidation,
    ClosedOperation,
    TradeMarker,
)
from trader.custody_simulator import build_simulator_custody_activity_display
from trader.services.trade_markers import record_trade_marker

logger = logging.getLogger(__name__)
_TZ_BRT = ZoneInfo('America/Sao_Paulo')


def _can_use_endpoint_cache() -> bool:
    try:
        return not (len(sys.argv) >= 2 and sys.argv[1] == 'test')
    except Exception:
        return True


def _quote_snapshot_session_dates_iso(ticker_sym: str) -> list[str]:
    """
    Datas (ISO YYYY-MM-DD) em que existe pelo menos um QuoteSnapshot para o ticker
    (fuso America/Sao_Paulo); mais recentes primeiro. Limite 500 dias.
    """
    sym = (ticker_sym or '').strip().upper()
    if not sym:
        return []
    with timezone.override(_TZ_BRT):
        day_qs = (
            QuoteSnapshot.objects.filter(ticker__iexact=sym)
            .dates('captured_at', 'day', order='DESC')[:500]
        )
    dates: list[str] = []
    for d in day_qs:
        if isinstance(d, date):
            dates.append(d.isoformat())
        else:
            s = str(d)[:10]
            if len(s) == 10 and s[4] == '-' and s[7] == '-':
                dates.append(s)
    return dates


def _automation_sim_date_choices(ticker_sym: str) -> list[dict[str, str]]:
    """Opções para o select de simulação: só dias com dados salvos."""
    out: list[dict[str, str]] = []
    for iso in _quote_snapshot_session_dates_iso(ticker_sym):
        iso10 = iso[:10]
        try:
            d = date.fromisoformat(iso10)
            label = d.strftime('%d/%m/%Y')
        except ValueError:
            label = iso10
        out.append({'iso': iso10, 'label_br': label})
    return out


def _quote_snapshot_tickers_with_data(*, limit: int = 2000) -> list[str]:
    """
    Tickers distintos com snapshots (ordenados).

    Usa agrupamento explícito por ``Upper('ticker')`` — ``distinct()`` + ``LIMIT`` na
    lista de linhas costumava retornar só um símbolo quando havia muitos snapshots
    do mesmo ticker no topo da tabela.
    """
    rows = (
        QuoteSnapshot.objects.annotate(tu=Upper('ticker'))
        .values('tu')
        .annotate(_n=Count('id'))
        .order_by('tu')
        .values_list('tu', flat=True)[:limit]
    )
    return [str(x).strip().upper() for x in rows if x]


def _is_boleta_ajax(request) -> bool:
    return request.headers.get('X-Requested-With') == 'XMLHttpRequest'


def _boleta_ajax_response(err: str | None, request=None) -> JsonResponse:
    if err:
        return JsonResponse({'ok': False, 'error': err}, status=400)
    payload: dict = {'ok': True}
    if request is not None:
        invalidate_collateral_custody_cache()
        is_home = False
        try:
            is_home = getattr(request.resolver_match, 'url_name', '') == 'home'
        except Exception:
            is_home = False
        payload['orders_html'] = _render_orders_html(request, dashboard_home=is_home)
    return JsonResponse(payload)


def _safe_same_origin_path(next_path: str, default: str) -> str:
    """Evita open redirect: só caminhos relativos na mesma origem."""
    p = (next_path or '').strip()
    if p.startswith('/') and not p.startswith('//'):
        return p
    return default


def _safe_next_after_liquidation(request) -> str:
    """Evita redirecionar para endpoint de fragmento HTML após liquidação."""
    default_next = reverse('trader:home')
    next_path = _safe_same_origin_path(request.POST.get('next') or '', default_next)
    frag = reverse('trader:collateral_custody_fragment')
    if next_path.startswith(frag):
        return default_next
    return next_path


def _parse_decimal_any(raw: object) -> Decimal | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, (int, float)):
        return Decimal(str(raw))
    s = str(raw).strip().replace(' ', '')
    if not s:
        return None
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    else:
        s = s.replace(',', '.')
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _row_get_ci(row: dict, *candidates: str):
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for c in candidates:
        k = c.lower()
        if k in lower_map:
            return lower_map[k]
    return None


def _extract_custody_positions(raw_custody: object) -> list[tuple[str, Decimal]]:
    rows = raw_custody
    if isinstance(raw_custody, dict):
        for key in ('items', 'Items', 'data', 'Data', 'results', 'Results', 'custody', 'Custody'):
            inner = raw_custody.get(key)
            if isinstance(inner, list):
                rows = inner
                break
    out: list[tuple[str, Decimal]] = []
    if not isinstance(rows, list):
        return out
    for item in rows:
        if not isinstance(item, dict):
            continue
        ticker = str(_row_get_ci(item, 'ticker', 'Ticker', 'symbol', 'Symbol') or '').strip().upper()
        qty = _parse_decimal_any(
            _row_get_ci(
                item,
                'availableQuantity',
                'AvailableQuantity',
                'quantity',
                'Quantity',
                'positionQuantity',
                'PositionQuantity',
                'totalQuantity',
                'TotalQuantity',
            )
        )
        if not ticker or qty is None or qty == 0:
            continue
        out.append((ticker, qty))
    return out


def _liquidation_market_order_payload(ticker: str, qty: Decimal) -> dict:
    side = ORDER_SIDE_SELL if qty > 0 else ORDER_SIDE_BUY
    q_abs = int(abs(qty))
    return {
        'Module': ORDER_MODULE_DAY_TRADE,
        'Ticker': ticker,
        'Side': side,
        'Quantity': max(1, q_abs),
        'TimeInForce': ORDER_TIF_DAY,
    }


def _trade_marker_side_from_order_side(side: str) -> str:
    s = (side or '').strip().upper()
    if s == ORDER_SIDE_SELL.upper():
        return TradeMarker.Side.SELL
    return TradeMarker.Side.BUY


def _clear_leafar_open_lock_for_ticker(*, ticker: str, trading_environment: str) -> None:
    """
    Remove lock órfão da leafaR após liquidação manual.
    """
    sym = (ticker or '').strip().upper()
    env = normalize_environment(trading_environment)
    if not sym:
        return
    for lane in ('standard', 'replay_shadow'):
        cache.delete(f'leafar:open_op:v1:{env}:{sym}:{lane}')


@login_required
@require_POST
def set_trading_environment(request):
    default_next = reverse('trader:home')
    next_path = _safe_same_origin_path(request.POST.get('next') or '', default_next)
    selected = request.POST.get('environment')
    env = set_session_environment(request, selected)
    set_current_environment(env)
    try:
        invalidate_collateral_custody_cache()
    except Exception:
        logger.exception('invalidate_collateral_custody_cache após troca de ambiente')
    try:
        invalidate_intraday_orders_cache()
    except Exception:
        logger.exception('invalidate_intraday_orders_cache após troca de ambiente')
    if env == ENV_REAL:
        clear_all_market_day_sessions(request)
        try:
            clear_automation_sim_preference_for_user(request.user, ENV_SIMULATOR)
            clear_automation_sim_preference_for_user(request.user, ENV_REPLAY)
        except Exception:
            logger.exception('clear_automation_sim_preference_for_user após REAL')
    label = environment_label(env)
    messages.success(request, f'Ambiente ativo alterado para {label}.')
    return redirect(next_path)


def _quote_last_price_for_candle(q: dict) -> float | None:
    """Preço para agregar candles; aceita variações comuns do payload da corretora."""
    if not isinstance(q, dict):
        return None
    for key in ('lastPrice', 'LastPrice', 'last_price', 'close', 'Close'):
        v = q.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _market_errors_has_404(errors: object) -> bool:
    if not isinstance(errors, dict):
        return False
    for value in errors.values():
        if value is None:
            continue
        if 'status 404' in str(value).lower():
            return True
    return False


def _resolve_market_context_local_for_poll(request, raw_ticker: str) -> dict:
    """
    Contexto local para o ticker exibido no painel, com rolamento WIN/WDO quando
    o contrato resolvido está em EndOfDay (alinhado ao snapshot JSON).
    Usa preferência por snapshots no DB para não disparar a API a cada poll.
    """
    base = raw_ticker if raw_ticker in ('WIN', 'WDO') else None
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    ctx = build_market_context_local(ticker)
    if base and quote_status_is_end_of_day(ctx.get('quote')):
        ticker = resolve_daytrade_base_ticker(request, base, force=True)
        ctx = build_market_context_local(ticker)
    return ctx


def _live_poll_flags_from_ctx(ctx: dict) -> tuple[bool, str | None]:
    has_404 = _market_errors_has_404(ctx.get('errors'))
    q = ctx.get('quote')
    live_poll_active = bool(q) and (not quote_status_is_end_of_day(q)) and (not has_404)
    pause_reason = 'status_404' if has_404 else None
    return live_poll_active, pause_reason


def _parse_candle_interval(raw: str) -> int:
    v = (raw or '').strip().lower()
    mapping = {
        '1s': 1,
        '5s': 5,
        '10s': 10,
        '15s': 15,
        '30s': 30,
        '45s': 45,
        '60s': 60,
        '1m': 60,
        '5m': 300,
        '15m': 900,
    }
    return mapping.get(v, 60)


def _parse_replay_until_param(raw: str | None):
    """ISO-8601 com fuso; naive interpretado em America/Sao_Paulo."""
    s = (raw or '').strip()
    if not s:
        return None
    dt = parse_datetime(s)
    if dt is None:
        return None
    sp_tz = ZoneInfo('America/Sao_Paulo')
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, sp_tz)
    return dt


def _replay_virtual_clock_template_context(request, env: str, sim_state: dict) -> dict[str, str]:
    """
    Limites do pregão (BRT) e cursor inicial para o relógio virtual do Replay (template/JS).
    Fora do Replay ou sem sessão activa devolve strings vazias.
    """
    empty: dict[str, str] = {
        'replay_virtual_cursor_iso': '',
        'replay_day_start_iso': '',
        'replay_day_end_iso': '',
    }
    if env != ENV_REPLAY or not sim_state.get('effective'):
        return empty
    session_d = sim_state.get('session_date')
    if not session_d:
        return empty
    tz = _TZ_BRT
    day_start = datetime.combine(session_d, time(10, 0), tzinfo=tz)
    day_end = datetime.combine(session_d, time(18, 30), tzinfo=tz)
    pref = (
        AutomationMarketSimPreference.objects.filter(
            user=request.user,
            trading_environment=ENV_REPLAY,
        )
        .only('replay_until')
        .first()
    )
    cur = getattr(pref, 'replay_until', None) if pref else None
    if cur is None:
        cur = day_start
    else:
        if timezone.is_naive(cur):
            cur = timezone.make_aware(cur, tz)
        cur = cur.astimezone(tz)
        if cur < day_start:
            cur = day_start
        elif cur > day_end:
            cur = day_end
    return {
        'replay_virtual_cursor_iso': cur.isoformat(),
        'replay_day_start_iso': day_start.isoformat(),
        'replay_day_end_iso': day_end.isoformat(),
    }


def _parse_watch_tickers_text(raw: str | None) -> list[str]:
    s = (raw or '').strip().upper()
    if not s:
        return []
    parts = re.split(r'[,\s]+', s)
    out: list[str] = []
    for p in parts:
        t = (p or '').strip().upper()
        if t and t not in out:
            out.append(t)
    return out


def _watch_tickers_text() -> str:
    tickers = list(
        WatchedTicker.objects.filter(enabled=True).order_by('ticker').values_list('ticker', flat=True)
    )
    return ','.join(tickers)


def _watch_tickers_list() -> list[str]:
    return list(
        WatchedTicker.objects.filter(enabled=True).order_by('ticker').values_list('ticker', flat=True)
    )


def _fmt_brt_datetime(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return value
    else:
        return str(value)
    if dt.tzinfo is None:
        return dt.strftime('%d/%m/%Y %H:%M:%S')
    return dt.astimezone(_TZ_BRT).strftime('%d/%m/%Y %H:%M:%S')


def _celery_tests_payload() -> dict:
    watched = list(
        WatchedTicker.objects.filter(enabled=True)
        .order_by('ticker')
        .values_list('ticker', flat=True)
    )
    summary_rows: list[dict] = []
    for ticker in watched:
        last = (
            QuoteSnapshot.objects.filter(ticker=ticker)
            .order_by('-captured_at')
            .values('captured_at', 'quote_data')
            .first()
        )
        last_book = (
            BookSnapshot.objects.filter(ticker=ticker)
            .order_by('-captured_at')
            .values('captured_at', 'book_data')
            .first()
        )
        q: dict = {}
        b: dict = {}
        last_price = None
        last_qty = None
        last_status = None
        quote_at = None
        captured_at = None
        book_captured_at = None
        book_bid_count = 0
        book_ask_count = 0
        best_bid_price = None
        best_ask_price = None
        if last:
            q = last.get('quote_data') or {}
            if isinstance(q, dict):
                last_price = q.get('lastPrice')
                last_qty = q.get('lastQuantity')
                last_status = q.get('status') or q.get('Status')
                quote_at = q.get('dateTime') or q.get('tradeDateTime')
            captured_at = last.get('captured_at')
        if last_book:
            b = last_book.get('book_data') or {}
            book_captured_at = last_book.get('captured_at')
            if isinstance(b, dict):
                bids = b.get('bids') or b.get('Bids') or []
                asks = b.get('asks') or b.get('Asks') or []
                if isinstance(bids, list):
                    book_bid_count = len(bids)
                    if bids:
                        top = bids[0] or {}
                        if isinstance(top, dict):
                            best_bid_price = (
                                top.get('price')
                                or top.get('Price')
                                or top.get('unitPrice')
                                or top.get('UnitPrice')
                            )
                if isinstance(asks, list):
                    book_ask_count = len(asks)
                    if asks:
                        top = asks[0] or {}
                        if isinstance(top, dict):
                            best_ask_price = (
                                top.get('price')
                                or top.get('Price')
                                or top.get('unitPrice')
                                or top.get('UnitPrice')
                            )
        summary_rows.append(
            {
                'ticker': ticker,
                'captured_at': captured_at,
                'captured_at_brt': _fmt_brt_datetime(captured_at),
                'quote_at': quote_at,
                'quote_at_brt': _fmt_brt_datetime(quote_at),
                'last_price': last_price,
                'last_qty': last_qty,
                'last_status': last_status,
                'open': (q.get('open') if isinstance(q, dict) else None),
                'high': (q.get('high') if isinstance(q, dict) else None),
                'low': (q.get('low') if isinstance(q, dict) else None),
                'close': (q.get('close') if isinstance(q, dict) else None),
                'book_captured_at': book_captured_at,
                'book_captured_at_brt': _fmt_brt_datetime(book_captured_at),
                'book_bid_count': book_bid_count,
                'book_ask_count': book_ask_count,
                'best_bid_price': best_bid_price,
                'best_ask_price': best_ask_price,
            }
        )

    return {
        'watch_tickers_text': _watch_tickers_text(),
        'watched_count': len(watched),
        'summary_rows': summary_rows,
        'server_time_brt': _fmt_brt_datetime(timezone.now()),
    }


def _render_orders_html(request, *, dashboard_home: bool = False) -> str:
    ctx = _orders_context_for_request(request, dashboard_home=dashboard_home)
    return render_to_string('trader/partials/orders_panel.html', ctx, request=request)


def _extract_order_id_from_result_json(result_json: str | None) -> str | None:
    if not result_json:
        return None
    try:
        payload = json.loads(result_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    for key in ('orderId', 'OrderId', 'id', 'Id', 'ID'):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _store_last_sent_order_id(request, result_json: str | None) -> None:
    oid = _extract_order_id_from_result_json(result_json)
    if oid:
        request.session['last_sent_order_id'] = oid


def _orders_context_for_request(request, *, dashboard_home: bool = False) -> dict:
    limit = 5 if dashboard_home else None
    ctx = build_orders_context(orders_limit=limit)
    last_id = request.session.get('last_sent_order_id')
    if dashboard_home:
        ctx['dashboard_home'] = True
    ctx['last_sent_order_id'] = last_id
    rows = ctx.get('orders_table_display') or []
    if last_id and rows:
        pinned: list = []
        others: list = []
        for row in rows:
            if len(row) >= 2 and row[1] == last_id:
                pinned.append(row)
            else:
                others.append(row)
        if pinned:
            ctx['orders_table_display'] = pinned + others
    return ctx


@login_required
@require_POST
def cancel_order(request):
    """
    POST interno → ``POST /v1/orders/cancel?Id=...`` (sem corpo; headers via api_auth).
    """
    default_next = reverse('trader:home') + '#ordens'
    next_path = _safe_same_origin_path(request.POST.get('next') or '', default_next)
    order_id = (request.POST.get('order_id') or '').strip()
    is_ajax = _is_boleta_ajax(request)
    if not order_id:
        if is_ajax:
            return JsonResponse({'ok': False, 'error': 'ID da ordem inválido.'}, status=400)
        messages.error(request, 'ID da ordem inválido.')
        return redirect(next_path)
    try:
        post_cancel_order(order_id)
        invalidate_collateral_custody_cache()
        try:
            invalidate_intraday_orders_cache()
        except Exception:
            pass
        if is_ajax:
            dashboard_home = request.POST.get('dashboard_home') == '1'
            payload = {
                'ok': True,
                'message': 'Cancelamento solicitado.',
                'orders_html': _render_orders_html(
                    request, dashboard_home=dashboard_home
                ),
            }
            return JsonResponse(payload)
        messages.success(request, 'Cancelamento solicitado (POST /v1/orders/cancel).')
    except ValueError as exc:
        if is_ajax:
            return JsonResponse({'ok': False, 'error': str(exc)}, status=400)
        messages.error(request, str(exc))
    except Exception:
        logger.exception('cancel_order')
        if is_ajax:
            return JsonResponse({'ok': False, 'error': 'Falha ao cancelar ordem.'}, status=500)
        messages.error(request, 'Falha ao cancelar ordem.')
    return redirect(next_path)


@login_required
def panel_hub(request):
    """Primeira tela após login: escolha entre painel manual ou automações."""
    return render(
        request,
        'trader/panel_hub.html',
        {
            'page_title': 'Menu',
            'nav_section': 'hub',
            'hide_sidebar_boleta': True,
        },
    )


def home(request):
    """
    Painel único: mercado, ordens do dia e envio de teste no mesmo template
    (dados completos para usuário autenticado).
    """
    ctx: dict = {
        'nav_section': 'home',
        'dashboard_home': True,
    }
    t0 = time_mod.perf_counter()
    uid = getattr(request.user, 'id', None) if getattr(request, 'user', None) else None
    logger.info(
        'env_manual:init user_id=%s method=%s path=%s',
        uid,
        request.method,
        request.path,
    )

    if not request.user.is_authenticated:
        ctx['page_title'] = getattr(settings, 'PUBLIC_SITE_NAME', 'Privado')
        ctx['dashboard_locked'] = True
        return render(request, 'trader/home.html', ctx)

    ctx['page_title'] = getattr(settings, 'SESSION_APP_TITLE', 'Painel')

    if request.method == 'POST' and _is_boleta_ajax(request):
        _, result_json, err = run_order_test_form(request)
        if not err:
            _store_last_sent_order_id(request, result_json)
        logger.info(
            'env_manual:boleta_ajax_done user_id=%s elapsed_ms=%.1f err=%s',
            uid,
            (time_mod.perf_counter() - t0) * 1000.0,
            bool(err),
        )
        return _boleta_ajax_response(err, request)

    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    base = raw_ticker if raw_ticker in ('WIN', 'WDO') else None
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    logger.info(
        'env_manual:market_ctx_start user_id=%s raw_ticker=%s resolved_ticker=%s',
        uid,
        raw_ticker,
        ticker,
    )
    ctx.update(build_market_context_local(ticker))
    if base and quote_status_is_end_of_day(ctx.get('quote')):
        ticker = resolve_daytrade_base_ticker(request, base, force=True)
        ctx.update(build_market_context_local(ticker))
    ctx.update(_orders_context_for_request(request, dashboard_home=True))
    logger.info(
        'env_manual:orders_ctx_done user_id=%s elapsed_ms=%.1f',
        uid,
        (time_mod.perf_counter() - t0) * 1000.0,
    )
    ctx.update(get_daytrade_candidates_text_context(request))
    ctx['watch_tickers_text'] = _watch_tickers_text()
    chips = get_daytrade_chip_suggestions(request)
    ctx['ticker_suggestions_daytrade'] = chips
    ctx['ticker_suggestions_equities'] = default_ticker_suggestions_equities()
    ctx['market_live_poll'] = True
    ctx['default_ticker_js'] = chips[0]

    form, result_json, err = run_order_test_form(request)
    ctx['form'] = form
    ctx['result_json'] = result_json
    ctx['send_error'] = err
    logger.info(
        'env_manual:ready user_id=%s elapsed_ms=%.1f ticker=%s live_poll=%s',
        uid,
        (time_mod.perf_counter() - t0) * 1000.0,
        ctx.get('ticker'),
        ctx.get('market_live_poll'),
    )

    return render(request, 'trader/home.html', ctx)


@login_required
def send_order_test(request):
    """Página dedicada ao mesmo formulário da home (envio de teste)."""
    if request.method == 'POST' and _is_boleta_ajax(request):
        _, result_json, err = run_order_test_form(request)
        if not err:
            _store_last_sent_order_id(request, result_json)
        return _boleta_ajax_response(err, request)
    form, result_json, err = run_order_test_form(request)
    return render(
        request,
        'trader/send_order_test.html',
        {
            'page_title': 'Envio de ordem (teste)',
            'nav_section': 'send_test',
            'form': form,
            'result_json': result_json,
            'send_error': err,
        },
    )


@login_required
def orders_intraday(request):
    """Ordens do dia (mesmo bloco da home, URL dedicada)."""
    if request.method == 'POST' and _is_boleta_ajax(request):
        _, result_json, err = run_order_test_form(request)
        if not err:
            _store_last_sent_order_id(request, result_json)
        return _boleta_ajax_response(err, request)
    ctx: dict = {
        'page_title': 'Ordens do dia',
        'nav_section': 'orders',
        **_orders_context_for_request(request),
    }
    form, result_json, err = run_order_test_form(request)
    ctx['form'] = form
    ctx['result_json'] = result_json
    ctx['send_error'] = err
    return render(request, 'trader/orders_intraday.html', ctx)


@login_required
def market_quote(request):
    """Mercado (mesmo bloco da home, URL dedicada)."""
    if request.method == 'POST' and _is_boleta_ajax(request):
        _, result_json, err = run_order_test_form(request)
        if not err:
            _store_last_sent_order_id(request, result_json)
        return _boleta_ajax_response(err, request)
    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    base = raw_ticker if raw_ticker in ('WIN', 'WDO') else None
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    ctx_t = build_market_context_local(ticker)
    if base and quote_status_is_end_of_day(ctx_t.get('quote')):
        ticker = resolve_daytrade_base_ticker(request, base, force=True)
        ctx_t = build_market_context_local(ticker)
    chips = get_daytrade_chip_suggestions(request)
    ctx: dict = {
        'page_title': f'Mercado — {ticker}',
        'nav_section': 'market',
        'ticker_suggestions_daytrade': chips,
        'ticker_suggestions_equities': default_ticker_suggestions_equities(),
        **ctx_t,
    }
    ctx.update(get_daytrade_candidates_text_context(request))
    ctx['watch_tickers_text'] = _watch_tickers_text()
    ctx['market_live_poll'] = True
    ctx['default_ticker_js'] = chips[0]
    form, result_json, err = run_order_test_form(request)
    ctx['form'] = form
    ctx['result_json'] = result_json
    ctx['send_error'] = err
    return render(request, 'trader/market_quote.html', ctx)


@login_required
@require_GET
def orders_panel_fragment(request):
    """HTML parcial da tabela de ordens (atualização após envio/cancelamento via AJAX)."""
    home_flag = (request.GET.get('home') or '').strip() == '1'
    return render(
        request,
        'trader/partials/orders_panel.html',
        _orders_context_for_request(request, dashboard_home=home_flag),
    )


@login_required
@require_GET
def collateral_custody_fragment(request):
    """
    HTML das seções Garantias + Custódia (atualização dinâmica / polling).

    Usa o cache por ambiente em :func:`~trader.panel_context.build_collateral_custody_context`
    (TTL ~30s). Invalidação após ordens/cancelamento/liquidação e ao alternar real/simulador.
    """
    base = merge_replay_shadow_custody_panel(build_collateral_custody_context(), request)
    html = render_to_string(
        'trader/partials/collateral_custody_fragment_inner.html',
        base,
        request=request,
    )
    return HttpResponse(html)


@login_required
@require_GET
def market_snapshot_json(request):
    """
    Snapshot JSON para atualização ao vivo (sem cache HTTP da API remota).
    ``poll_ms`` no JSON é sugestão ao cliente (evitar valores muito baixos —
    vários pollers na mesma página somam carga). Limites doc: 1 req/s por
    endpoint+ticker em details/book/aggregate; quote até 20/5 s.
    """
    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    env = get_session_environment(request)
    cache_key = f'market_snapshot_json:v2:{env}:{raw_ticker}'
    if _can_use_endpoint_cache():
        hit = cache.get(cache_key)
        if isinstance(hit, dict):
            return JsonResponse(hit)
    ctx = _resolve_market_context_local_for_poll(request, raw_ticker)
    quote_latency_ms = compute_quote_latency_ms(ctx.get('quote'))
    live_poll_active, pause_reason = _live_poll_flags_from_ctx(ctx)
    payload = {
        'ticker': ctx['ticker'],
        'errors': json_sanitize(ctx['errors']),
        'details': json_sanitize(ctx['details']),
        'quote': json_sanitize(ctx['quote']),
        'book': json_sanitize(ctx['book']),
        'aggregate_book': json_sanitize(ctx['aggregate_book']),
        'agg_bids': json_sanitize(ctx['agg_bids']),
        'agg_asks': json_sanitize(ctx['agg_asks']),
        'chart_payload': json_sanitize(ctx['chart_payload']),
        'operation_hints': json_sanitize(ctx['operation_hints']),
        'poll_ms': 2200,
        'live_poll_active': live_poll_active,
        'pause_reason': pause_reason,
        'quote_latency_ms': quote_latency_ms,
        # Simulador: mantém o poll do strip financeiro (custódia + marcações) mesmo fora do pregão ao vivo (ex.: replay).
        'simulator_custody_poll': env in (ENV_SIMULATOR, ENV_REPLAY),
    }
    if _can_use_endpoint_cache():
        cache.set(cache_key, payload, 1)
    return JsonResponse(payload)


@login_required
@require_GET
def quote_history_json(request):
    ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, ticker)
    try:
        limit = int(request.GET.get('limit') or '300')
    except ValueError:
        limit = 300
    limit = min(2000, max(1, limit))
    from trader.models import QuoteSnapshot

    rows = list(
        QuoteSnapshot.objects.filter(ticker__iexact=ticker)
        .order_by('-captured_at')
        .values('captured_at', 'quote_data')[:limit]
    )
    return JsonResponse(
        {
            'ticker': ticker,
            'count': len(rows),
            'items': json_sanitize(rows),
        }
    )


def _parse_session_date_param(raw: str | None) -> date | None:
    if not raw:
        return None
    s = str(raw).strip()[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


@login_required
@require_GET
def quote_candles_session_dates_json(request):
    """
    Dias de pregão (America/Sao_Paulo) em que existem snapshots de quote para o ticker.
    Usado para preencher o seletor de sessão apenas com datas que possuem dados.
    """
    ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, ticker)
    dates = _quote_snapshot_session_dates_iso(ticker)
    return JsonResponse({'ticker': ticker, 'dates': json_sanitize(dates)})


@login_required
@require_GET
def quote_candles_json(request):
    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    interval_sec = _parse_candle_interval(request.GET.get('interval') or '1m')
    try:
        limit = int(request.GET.get('limit') or '120')
    except ValueError:
        limit = 120
    session_day = _parse_session_date_param(request.GET.get('session_date'))
    replay_raw = (request.GET.get('replay_until') or '').strip()
    replay_day_master = request.GET.get('replay_day_master', '').strip().lower() in (
        '1',
        'true',
        'yes',
    )
    progressive_raw = (request.GET.get('progressive') or '').strip().lower()
    progressive_mode = session_day is not None and progressive_raw in ('1', 'true', 'yes')
    try:
        prog_chunk_index = max(0, int(request.GET.get('chunk_index') or '0'))
    except ValueError:
        prog_chunk_index = 0
    try:
        prog_segment_index = max(0, int(request.GET.get('segment_index') or '0'))
    except ValueError:
        prog_segment_index = 0
    try:
        chunk_hours = float(request.GET.get('chunk_hours') or '2')
    except ValueError:
        chunk_hours = 2.0
    chunk_hours = max(0.25, min(float(chunk_hours), 8.0))
    env = get_session_environment(request)
    active_profile = resolve_active_profile(request.user, env)
    active_profile_id = int(getattr(active_profile, 'id', 0) or 0)
    cache_key = (
        f'quote_candles_json:v8:{env}:{active_profile_id}:{ticker}:{interval_sec}:'
        f'{session_day.isoformat() if session_day else "-"}:{replay_raw or "-"}:{limit}'
        f':p:{int(progressive_mode)}:{prog_chunk_index if progressive_mode else 0}:'
        f'{prog_segment_index if progressive_mode else 0}:{chunk_hours if progressive_mode else 0}'
        f':rdm:{int(replay_day_master)}'
    )
    if _can_use_endpoint_cache():
        hit = cache.get(cache_key)
        if isinstance(hit, dict):
            return JsonResponse(hit)

    from trader.models import QuoteSnapshot

    sp_tz = ZoneInfo('America/Sao_Paulo')
    progressive_meta: dict[str, Any] | None = None
    if progressive_mode:
        # Simulação/replay: fatias por tempo + **1000 snapshots por GET** (segmentos), para dias com 50k+ linhas.
        row_page = 1000
        day_start = datetime.combine(session_day, time.min, tzinfo=sp_tz)
        day_end = day_start + timedelta(days=1)
        ws = day_start + timedelta(hours=prog_chunk_index * chunk_hours)
        we = min(day_end, day_start + timedelta(hours=(prog_chunk_index + 1) * chunk_hours))
        if replay_raw:
            dtu_cap = _parse_replay_until_param(replay_raw)
            if dtu_cap is not None:
                we = min(we, dtu_cap)
        rows: list[dict[str, Any]] = []
        lo = prog_segment_index * row_page
        hi = lo + row_page
        has_more = False
        next_chunk = prog_chunk_index
        next_segment = prog_segment_index
        if ws < we:
            qprog = QuoteSnapshot.objects.filter(
                ticker__iexact=ticker,
                captured_at__gte=ws,
                captured_at__lt=we,
            )
            rows = list(
                qprog.order_by('captured_at').values('captured_at', 'quote_data')[lo:hi]
            )
            if rows:
                if len(rows) >= row_page:
                    has_more = True
                    next_chunk = prog_chunk_index
                    next_segment = prog_segment_index + 1
                else:
                    if we < day_end:
                        has_more = True
                        next_chunk = prog_chunk_index + 1
                        next_segment = 0
            else:
                if lo == 0:
                    if we < day_end:
                        has_more = True
                        next_chunk = prog_chunk_index + 1
                        next_segment = 0
                else:
                    if we < day_end:
                        has_more = True
                        next_chunk = prog_chunk_index + 1
                        next_segment = 0
        elif we < day_end:
            has_more = True
            next_chunk = prog_chunk_index + 1
            next_segment = 0
        progressive_meta = {
            'enabled': True,
            'has_more': has_more,
            'chunk_index': prog_chunk_index,
            'segment_index': prog_segment_index,
            'next_chunk_index': next_chunk,
            'next_segment_index': next_segment,
            'row_page': row_page,
            'chunk_hours': chunk_hours,
            'window_start': ws.isoformat(),
            'window_end': we.isoformat(),
        }
        limit = min(8000, max(200, limit))
    else:
        qs = QuoteSnapshot.objects.filter(ticker__iexact=ticker)
        if session_day is not None:
            day_start = datetime.combine(session_day, time.min, tzinfo=sp_tz)
            day_end = day_start + timedelta(days=1)
            qs = qs.filter(captured_at__gte=day_start, captured_at__lt=day_end)
            if replay_raw:
                dtu = _parse_replay_until_param(replay_raw)
                if dtu is not None:
                    qs = qs.filter(captured_at__lte=dtu)

        # Dia específico: agrega até ``max_rows`` snapshots; tem de ser a **cauda** do intervalo
        # (até ``replay_until`` quando aplicável). ``[:max_rows]`` em ordem crescente cortava só a
        # manhã — o gráfico «parava» no meio do dia enquanto replay/automações seguiam no fim.
        # Sem session_date: mesma ideia (cauda) para o gráfico ao vivo.
        if session_day is not None:
            limit = min(8000, max(10, limit))
            # Antes: até 250k linhas por GET — estourava CPU/memória no worker e no JSON.
            # Agregação em candles não precisa de tantos pontos brutos; limite duro protege o servidor.
            # ``replay_day_master``: desde o início civil do dia (ordem crescente), para o cache do
            # replay no browser — a cauda ``-captured_at`` cortava a manhã em dias com 10k+ snapshots.
            if replay_day_master and not replay_raw:
                max_rows = min(150_000, max(50_000, min(limit, 8000) * 40))
                rows = list(
                    qs.order_by('captured_at').values('captured_at', 'quote_data')[:max_rows]
                )
            else:
                max_rows = min(30_000, max(4_000, min(limit, 8000) * 25))
                rows = list(
                    qs.order_by('-captured_at').values('captured_at', 'quote_data')[:max_rows]
                )
                rows.reverse()
        else:
            limit = min(500, max(10, limit))
            max_rows = min(5000, limit * 40)
            rows = list(
                qs.order_by('-captured_at').values('captured_at', 'quote_data')[:max_rows]
            )
            rows.reverse()

    buckets: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        q = row.get('quote_data') or {}
        if not isinstance(q, dict):
            continue
        raw_status = q.get('status')
        if raw_status is None:
            raw_status = q.get('Status')
        status = None
        if raw_status is not None:
            s = str(raw_status).strip()
            status = s if s else None
        last_price = _quote_last_price_for_candle(q)
        if last_price is None:
            continue
        raw_qty = q.get('lastQuantity')
        try:
            qty = float(raw_qty) if raw_qty is not None else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        captured_at = row.get('captured_at')
        if captured_at is None:
            continue
        ts = int(captured_at.timestamp())
        bucket = (ts // interval_sec) * interval_sec
        buckets[bucket].append(
            {
                'captured_at': captured_at,
                'last_price': last_price,
                'last_quantity': qty,
                'status': status,
            }
        )

    def _stable_unit(seed: int) -> float:
        # Pseudo-aleatório determinístico por bucket (evita "piscada" entre polls).
        x = math.sin(float(seed) * 12.9898) * 43758.5453
        return x - math.floor(x)

    candles: list[dict] = []
    prev_close: float | None = None
    for bucket in sorted(buckets.keys()):
        points = buckets[bucket]
        if not points:
            continue
        opens = points[0]['last_price']
        closes = points[-1]['last_price']
        highs = max(p['last_price'] for p in points)
        lows = min(p['last_price'] for p in points)
        volume = sum(p['last_quantity'] for p in points)
        if len(points) == 1:
            # Em buckets com 1 único ponto, evita candles com tamanho visual idêntico.
            # Usa open anterior + micro-range determinístico para pavio/corpo variáveis.
            if prev_close is not None:
                opens = float(prev_close)
            else:
                opens = float(closes)
            closes = float(closes)
            body = abs(closes - opens)
            u1 = _stable_unit(bucket + int(volume * 10.0) + 17)
            u2 = _stable_unit(bucket + int(abs(closes) * 100.0) + 31)
            ref = max(abs(closes), abs(opens), 1.0)
            micro = max(ref * 0.00008, 0.002)
            wick_base = max(micro, body * (0.55 + u1 * 1.45))
            high_extra = wick_base * (0.55 + u2 * 1.25)
            low_extra = wick_base * (0.45 + (1.0 - u2) * 1.10)
            highs = max(opens, closes) + high_extra
            lows = min(opens, closes) - low_extra
        last_status = None
        for p in points:
            s = p.get('status')
            if s:
                last_status = s
        candle_dt = timezone.datetime.fromtimestamp(
            bucket,
            tz=ZoneInfo('America/Sao_Paulo'),
        )
        label_fmt = '%d/%m %H:%M:%S' if interval_sec < 60 else '%d/%m %H:%M'
        candles.append(
            {
                'bucket_ts': bucket,
                'bucket_start': candle_dt.isoformat(),
                'label': candle_dt.strftime(label_fmt),
                'open': round(opens, 6),
                'high': round(highs, 6),
                'low': round(lows, 6),
                'close': round(closes, 6),
                'volume': round(volume, 6),
                'status': last_status,
            }
        )
        prev_close = float(closes)

    if session_day is not None:
        # Modo progressivo: já limitado pela janela temporal; só corta excesso extremo.
        if progressive_mode:
            cap = max(limit * 3, 600)
            if len(candles) > cap:
                candles = candles[:cap]
        elif len(candles) > limit:
            # Mantém o **fim** do pregão quando há mais candles que o limite ([:limit] cortava só a manhã).
            candles = candles[-limit:]
    else:
        candles = candles[-limit:]
    trade_markers: list[dict[str, Any]] = []
    strategy_markers: list[dict[str, Any]] = []
    bucket_to_idx: dict[int, int] = {}
    for i, c in enumerate(candles):
        bts = c.get('bucket_ts')
        if isinstance(bts, int):
            bucket_to_idx[bts] = i
    if bucket_to_idx:
        first_bucket = min(bucket_to_idx.keys())
        last_bucket = max(bucket_to_idx.keys())
        from_dt = timezone.datetime.fromtimestamp(first_bucket, tz=ZoneInfo('America/Sao_Paulo'))
        to_dt = timezone.datetime.fromtimestamp(
            last_bucket + interval_sec,
            tz=ZoneInfo('America/Sao_Paulo'),
        )
        markers = list(
            TradeMarker.objects.filter(
                ticker=ticker,
                marker_at__gte=from_dt,
                marker_at__lt=to_dt,
            )
            .order_by('marker_at')
            .values('side', 'quantity', 'price', 'marker_at')
        )
        for m in markers:
            marker_at = m.get('marker_at')
            if marker_at is None:
                continue
            bucket = (int(marker_at.timestamp()) // interval_sec) * interval_sec
            idx = bucket_to_idx.get(bucket)
            if idx is None:
                continue
            side = str(m.get('side') or '').upper()
            trade_markers.append(
                {
                    'idx': idx,
                    'side': side,
                    'quantity': str(m.get('quantity') or ''),
                    'price': (str(m['price']) if m.get('price') is not None else ''),
                    'marker_at': marker_at.isoformat(),
                }
            )
        enabled_map = get_strategy_enabled_map(
            request.user,
            env,
            execution_profile=active_profile,
        )
        enabled_strategy_keys = [k for k, v in enabled_map.items() if bool(v)]
        aqs = AutomationTriggerMarker.objects.filter(
            user=request.user,
            trading_environment=env,
            execution_profile=active_profile,
            ticker=ticker,
            marker_at__gte=from_dt,
            marker_at__lt=to_dt,
        )
        if enabled_strategy_keys:
            aqs = aqs.filter(strategy_key__in=enabled_strategy_keys)
        else:
            aqs = aqs.none()
        started = getattr(active_profile, 'execution_started_at', None)
        if started is not None:
            aqs = aqs.filter(created_at__gte=started)
        amarkers = list(
            aqs.order_by('marker_at').values('strategy_key', 'price', 'marker_at', 'message')
        )
        for m in amarkers:
            marker_at = m.get('marker_at')
            if marker_at is None:
                continue
            bucket = (int(marker_at.timestamp()) // interval_sec) * interval_sec
            idx = bucket_to_idx.get(bucket)
            if idx is None:
                continue
            strategy_markers.append(
                {
                    'idx': idx,
                    'strategy_key': str(m.get('strategy_key') or ''),
                    'price': (str(m['price']) if m.get('price') is not None else ''),
                    'marker_at': marker_at.isoformat(),
                    'message': str(m.get('message') or '')[:300],
                }
            )
        # Compatibilidade: quando a leafaR foi gravada em formato legado (somente thought),
        # sintetiza marcador no gráfico a partir da mensagem do pensamento.
        has_leafar_marker = any(
            str(m.get('strategy_key') or '') == 'leafar' for m in strategy_markers
        )
        if not has_leafar_marker and 'leafar' in enabled_strategy_keys and candles:
            # Fallback robusto: usa o último thought da leafaR (inclusive legado sem profile),
            # e ancora no último candle visível para sempre aparecer no replay corrente.
            t = (
                AutomationThought.objects.filter(
                    user=request.user,
                    trading_environment=env,
                    source='leafar',
                )
                .filter(Q(execution_profile=active_profile) | Q(execution_profile__isnull=True))
                .order_by('-id')
                .first()
            )
            if t is not None:
                msg = str(t.message or '')
                m_last = re.search(r'último=([0-9]+(?:[.,][0-9]+)?)', msg, flags=re.IGNORECASE)
                m_tp = re.search(r'(?:TP≈|preço alvo \(POC\)=)([0-9]+(?:[.,][0-9]+)?)', msg, flags=re.IGNORECASE)
                m_sl = re.search(r'(?:SL≈|Stop:\s*)([0-9]+(?:[.,][0-9]+)?)', msg, flags=re.IGNORECASE)
                m_side = re.search(r'sinal\s+(Buy|Sell)', msg, flags=re.IGNORECASE)
                if m_last:
                    last_s = m_last.group(1).replace(',', '.')
                    try:
                        float(last_s)
                    except (TypeError, ValueError):
                        last_s = ''
                    if last_s:
                        side_s = (m_side.group(1).upper() if m_side else '')
                        target_s = m_tp.group(1).replace(',', '.') if m_tp else ''
                        stop_s = m_sl.group(1).replace(',', '.') if m_sl else ''
                        compat_msg = (
                            f'direction={side_s};last={last_s};target={target_s};stop={stop_s};'
                            f'legacy_thought_id={t.id}'
                        )
                        strategy_markers.append(
                            {
                                'idx': len(candles) - 1,
                                'strategy_key': 'leafar',
                                'price': last_s,
                                'marker_at': (t.created_at.isoformat() if t.created_at else ''),
                                'message': compat_msg[:300],
                            }
                        )

    status_changes: list[dict[str, Any]] = []
    prev_status = None
    for i, c in enumerate(candles):
        s = c.get('status')
        if s and s != prev_status:
            status_changes.append({'idx': i, 'label': s})
            prev_status = s

    last_quote_close: float | None = None
    candle_min_price: float | None = None
    candle_max_price: float | None = None
    if candles:
        try:
            last_quote_close = float(candles[-1].get('close'))
        except (TypeError, ValueError):
            last_quote_close = None
        lows: list[float] = []
        highs: list[float] = []
        for c in candles:
            try:
                lo = float(c.get('low'))
                hi = float(c.get('high'))
            except (TypeError, ValueError):
                continue
            if lo > 0 and hi > 0:
                lows.append(lo)
                highs.append(hi)
        if lows and highs:
            candle_min_price = min(lows)
            candle_max_price = max(highs)

    bracket_lanes: list[dict[str, Any]] = []
    for lane in (BRACKET_LANE_STANDARD, BRACKET_LANE_REPLAY_SHADOW):
        pos_open = (
            Position.objects.filter(
                ticker=ticker,
                trading_environment=env,
                position_lane=lane,
                is_active=True,
                closed_at__isnull=True,
                quantity_open__gt=Decimal('0.000001'),
                avg_open_price__gt=Decimal('0.000001'),
            )
            .order_by('-opened_at')
            .first()
        )
        if pos_open is None:
            continue
        raw_st = cache.get(
            state_cache_key(ticker, bracket_lane=lane, trading_environment=env)
        )
        st_br: dict[str, Any] = {}
        if raw_st and isinstance(raw_st, str):
            try:
                parsed = json.loads(raw_st)
                if isinstance(parsed, dict):
                    st_br = parsed
            except json.JSONDecodeError:
                st_br = {}

        def _br_float(key: str) -> float | None:
            v = st_br.get(key)
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _is_reasonable_price(v: float | None) -> bool:
            if v is None or v <= 0:
                return False
            # Evita âncoras fantasmas (ex.: 0.01) que quebram a escala do gráfico.
            if (
                candle_min_price is not None
                and candle_max_price is not None
                and candle_max_price > candle_min_price
            ):
                lo = candle_min_price * 0.7
                hi = candle_max_price * 1.3
                return lo <= v <= hi
            if last_quote_close is not None and last_quote_close > 0:
                lo = last_quote_close * 0.7
                hi = last_quote_close * 1.3
                return lo <= v <= hi
            return True

        def _br_price_pos(key: str) -> float | None:
            v = _br_float(key)
            if v is None or v <= 0:
                return None
            if not _is_reasonable_price(v):
                return None
            return v

        tp_p = _br_price_pos('tp_price')
        sl_t = _br_price_pos('sl_trigger')
        entry_a = _br_price_pos('entry_anchor')
        if entry_a is None:
            try:
                candidate_entry = float(pos_open.avg_open_price)
            except (TypeError, ValueError):
                candidate_entry = None
            entry_a = candidate_entry if _is_reasonable_price(candidate_entry) else None
        last_px = _br_float('last')
        if last_px is None or last_px <= 0:
            last_px = last_quote_close
        side_from_pos = (
            'Buy' if pos_open.side == Position.Side.LONG else 'Sell'
        )
        entry_side = str(st_br.get('entry_side') or '').strip() or side_from_pos
        if tp_p is None and sl_t is None and entry_a is None:
            continue
        bracket_lanes.append(
            {
                'lane': lane,
                'strategy_source': (str(st_br.get('strategy_source') or '').strip() or None),
                'entry_side': entry_side or None,
                'operation_id': (str(st_br.get('operation_id') or st_br.get('market_order_id') or '').strip() or None),
                'tp_price': tp_p,
                'sl_trigger': sl_t,
                'sl_order_price': _br_price_pos('sl_order_price'),
                'entry_anchor': entry_a,
                'last': last_px,
                'peak': _br_float('peak'),
                'trough': _br_float('trough'),
                'force_close_done': bool(st_br.get('force_close_done')),
            }
        )
    bracket_live = {'lanes': bracket_lanes}

    if session_day is not None:
        live_poll_active = False
        pause_reason = None
    else:
        # Não chamar resolve_daytrade / API aqui: o cliente faz poll a cada ~500 ms;
        # acoplado a _ACTIVE_CACHE_SEC baixo isso gerava rajadas de fetch_quote e
        # degradava o Gunicorn. O pregão «ao vivo» vem do último snapshot já carregado do DB.
        live_poll_active = True
        pause_reason = None
        if rows:
            last_q = rows[-1].get('quote_data')
            if isinstance(last_q, dict) and quote_status_is_end_of_day(last_q):
                live_poll_active = False
    payload = {
        'ticker': ticker,
        'interval_sec': interval_sec,
        'session_date': session_day.isoformat() if session_day else None,
        'count': len(candles),
        'candles': json_sanitize(
            [{k: v for k, v in c.items() if k != 'bucket_ts'} for c in candles]
        ),
        'status_changes': status_changes,
        'trade_markers': trade_markers,
        'strategy_markers': strategy_markers,
        'bracket_live': json_sanitize(bracket_live),
        'live_poll_active': live_poll_active,
        'pause_reason': pause_reason,
        'progressive_meta': json_sanitize(progressive_meta or {'enabled': False}),
    }
    if _can_use_endpoint_cache():
        cache.set(cache_key, payload, 2 if session_day is None else 5)
    return JsonResponse(payload)


@login_required
@require_POST
def save_daytrade_candidates(request):
    """
    Salva a lista de tickers candidatos de rolamento para resolver `WIN`/`WDO`
    no backend (via `quote.status`).
    """
    default_next = reverse('trader:home')
    next_path = _safe_same_origin_path(request.POST.get('next') or '', default_next)
    set_daytrade_candidates_text(
        request,
        base='WIN',
        raw_text=request.POST.get('win_candidates'),
    )
    set_daytrade_candidates_text(
        request,
        base='WDO',
        raw_text=request.POST.get('wdo_candidates'),
    )
    return redirect(next_path)


@login_required
@require_POST
def save_watch_tickers(request):
    """
    Substitui a lista de tickers monitorados pelo Celery (coleta/snapshot de quote).
    Para excluir um ticker, basta removê-lo do campo e salvar.
    """
    default_next = reverse('trader:home')
    next_path = _safe_same_origin_path(request.POST.get('next') or '', default_next)
    desired = _parse_watch_tickers_text(request.POST.get('watch_tickers'))
    current = set(
        WatchedTicker.objects.filter(enabled=True).values_list('ticker', flat=True)
    )
    desired_set = set(desired)

    for ticker in sorted(current - desired_set):
        WatchedTicker.objects.filter(ticker=ticker).update(enabled=False)
    for ticker in desired:
        obj, _ = WatchedTicker.objects.get_or_create(
            ticker=ticker,
            defaults={'enabled': True},
        )
        if not obj.enabled:
            obj.enabled = True
            obj.save(update_fields=['enabled', 'updated_at'])

    messages.success(
        request,
        f'Tickers monitorados atualizados ({len(desired)} ativo(s)).',
    )
    return redirect(next_path)


@login_required
@require_POST
def liquidate_single_asset(request):
    next_path = _safe_next_after_liquidation(request)
    ticker = (request.POST.get('ticker') or '').strip().upper()
    qty = _parse_decimal_any(request.POST.get('quantity'))
    if not ticker or qty is None or qty == 0:
        messages.error(request, 'Dados inválidos para liquidação do ativo.')
        return redirect(next_path)
    try:
        body = _liquidation_market_order_payload(ticker, qty)
        resp = post_send_market_order(body)
        hist_price = infer_execution_price(body, resp)
        if should_record_local_history('market', resp):
            try:
                register_trade_execution(
                    ticker=ticker,
                    side=str(body.get('Side') or ''),
                    quantity=body.get('Quantity') or 0,
                    price=hist_price,
                    source='liquidate_single',
                    trading_environment=get_current_environment(),
                )
            except Exception:
                logger.exception('register_trade_execution liquidate_single')
        record_trade_marker(
            ticker=ticker,
            side=_trade_marker_side_from_order_side(str(body.get('Side') or '')),
            quantity=body.get('Quantity') or 0,
            price=hist_price,
            source='liquidate_single',
            metadata={
                'module': body.get('Module'),
                'time_in_force': body.get('TimeInForce'),
                'custody_channel': 'live',
                'data_source': 'api_liquidacao',
            },
        )
        _clear_leafar_open_lock_for_ticker(
            ticker=ticker,
            trading_environment=get_current_environment(),
        )
        invalidate_collateral_custody_cache()
        messages.success(request, f'Liquidação enviada para {ticker}.')
    except ValueError as exc:
        messages.error(request, str(exc))
    except Exception:
        logger.exception('liquidate_single_asset')
        messages.error(request, f'Falha ao liquidar {ticker}.')
    return redirect(next_path)


@login_required
@require_POST
def liquidate_all_assets(request):
    next_path = _safe_next_after_liquidation(request)
    try:
        raw = fetch_custody()
        positions = _extract_custody_positions(raw)
    except ValueError as exc:
        messages.error(request, str(exc))
        return redirect(next_path)
    except Exception:
        logger.exception('liquidate_all_assets:fetch_custody')
        messages.error(request, 'Falha ao consultar custódia para liquidação.')
        return redirect(next_path)

    if not positions:
        messages.info(request, 'Nenhum ativo elegível para liquidação.')
        return redirect(next_path)

    ok = 0
    errs = 0
    for ticker, qty in positions:
        try:
            body = _liquidation_market_order_payload(ticker, qty)
            resp = post_send_market_order(body)
            hist_price = infer_execution_price(body, resp)
            if should_record_local_history('market', resp):
                try:
                    register_trade_execution(
                        ticker=ticker,
                        side=str(body.get('Side') or ''),
                        quantity=body.get('Quantity') or 0,
                        price=hist_price,
                        source='liquidate_all',
                        trading_environment=get_current_environment(),
                    )
                except Exception:
                    logger.exception('register_trade_execution liquidate_all ticker=%s', ticker)
            record_trade_marker(
                ticker=ticker,
                side=_trade_marker_side_from_order_side(str(body.get('Side') or '')),
                quantity=body.get('Quantity') or 0,
                price=hist_price,
                source='liquidate_all',
                metadata={
                    'module': body.get('Module'),
                    'time_in_force': body.get('TimeInForce'),
                    'custody_channel': 'live',
                    'data_source': 'api_liquidacao',
                },
            )
            _clear_leafar_open_lock_for_ticker(
                ticker=ticker,
                trading_environment=get_current_environment(),
            )
            ok += 1
        except Exception:
            errs += 1
            logger.exception('liquidate_all_assets:ticker=%s', ticker)
    invalidate_collateral_custody_cache()
    if ok:
        messages.success(request, f'Liquidação enviada para {ok} ativo(s).')
    if errs:
        messages.warning(request, f'Falhas em {errs} ativo(s) durante a liquidação.')
    return redirect(next_path)


@login_required
@require_GET
@never_cache
def celery_tests(request):
    """
    Página de testes para inspecionar os dados que o Celery está salvando
    em QuoteSnapshot para os tickers monitorados.
    """
    payload = _celery_tests_payload()

    return render(
        request,
        'trader/celery_tests.html',
        {
            'page_title': 'Testes Celery',
            'nav_section': 'celery_tests',
            **payload,
        },
    )


@login_required
@require_GET
@never_cache
def celery_tests_json(request):
    resp = JsonResponse(_celery_tests_payload())
    resp['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp['Pragma'] = 'no-cache'
    resp['Expires'] = '0'
    return resp


@login_required
@require_GET
def liquidation_history(request):
    """
    Por padrão mostra só o histórico do ambiente ativo na sessão (real vs simulador),
    para não misturar operações. ``?all=1`` exibe os dois ambientes (comparação / auditoria).
    """
    show_both_environments = (request.GET.get('all') or '').strip() == '1'
    env = get_session_environment(request)
    need_sim = show_both_environments or env == ENV_SIMULATOR
    need_real = show_both_environments or env == ENV_REAL
    need_replay = show_both_environments or env == ENV_REPLAY

    base_pos = Position.objects.select_related('closed_operation')
    if need_sim:
        positions_sim = (
            base_pos.filter(
                trading_environment=ENV_SIMULATOR,
                position_lane=Position.Lane.STANDARD,
            )
            .order_by('-opened_at')[:200]
        )
    else:
        positions_sim = Position.objects.none()
    if need_replay:
        positions_sim_replay = (
            base_pos.filter(
                trading_environment=ENV_REPLAY,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
            .order_by('-opened_at')[:200]
        )
    else:
        positions_sim_replay = Position.objects.none()
    if need_real:
        positions_real = base_pos.filter(trading_environment=ENV_REAL).order_by('-opened_at')[:200]
    else:
        positions_real = Position.objects.none()

    base_liq = PositionLiquidation.objects.select_related('position')
    if need_sim:
        liquidations_sim = base_liq.filter(
            position__trading_environment=ENV_SIMULATOR,
            position__position_lane=Position.Lane.STANDARD,
        ).order_by('-executed_at')[:300]
    else:
        liquidations_sim = PositionLiquidation.objects.none()
    if need_replay:
        liquidations_sim_replay = base_liq.filter(
            position__trading_environment=ENV_REPLAY,
            position__position_lane=Position.Lane.REPLAY_SHADOW,
        ).order_by('-executed_at')[:300]
    else:
        liquidations_sim_replay = PositionLiquidation.objects.none()
    if need_real:
        liquidations_real = base_liq.filter(position__trading_environment=ENV_REAL).order_by(
            '-executed_at'
        )[:300]
    else:
        liquidations_real = PositionLiquidation.objects.none()

    base_closed = ClosedOperation.objects.select_related('position')
    if need_sim:
        closed_ops_sim = base_closed.filter(
            position__trading_environment=ENV_SIMULATOR,
            position__position_lane=Position.Lane.STANDARD,
        ).order_by('-closed_at')[:300]
    else:
        closed_ops_sim = ClosedOperation.objects.none()
    if need_replay:
        closed_ops_sim_replay = base_closed.filter(
            position__trading_environment=ENV_REPLAY,
            position__position_lane=Position.Lane.REPLAY_SHADOW,
        ).order_by('-closed_at')[:300]
    else:
        closed_ops_sim_replay = ClosedOperation.objects.none()
    if need_real:
        closed_ops_real = base_closed.filter(position__trading_environment=ENV_REAL).order_by(
            '-closed_at'
        )[:300]
    else:
        closed_ops_real = ClosedOperation.objects.none()

    trade_markers = (
        TradeMarker.objects.order_by('-marker_at')[:400]
        if show_both_environments
        else TradeMarker.objects.none()
    )
    simulator_custody_activity = (
        build_simulator_custody_activity_display() if need_sim else None
    )
    return render(
        request,
        'trader/liquidation_history.html',
        {
            'page_title': 'Histórico de liquidações',
            'nav_section': 'liquidations',
            'history_show_sim': need_sim,
            'history_show_real': need_real,
            'history_show_replay': need_replay,
            'show_both_environments': show_both_environments,
            'positions_sim': positions_sim,
            'positions_sim_replay': positions_sim_replay,
            'positions_real': positions_real,
            'liquidations_sim': liquidations_sim,
            'liquidations_sim_replay': liquidations_sim_replay,
            'liquidations_real': liquidations_real,
            'closed_operations_sim': closed_ops_sim,
            'closed_operations_sim_replay': closed_ops_sim_replay,
            'closed_operations_real': closed_ops_real,
            'trade_markers': trade_markers,
            'simulator_custody_activity': simulator_custody_activity,
        },
    )


@login_required
@never_cache
def automations_dashboard(request):
    """
    Painel de automações: mercado e candles como a home, sem ordens nem boleta manual.
    Toggles de estratégia são por usuário e pelo ambiente da sessão (simulador/real).
    """
    env = get_session_environment(request)
    active_profile = resolve_active_profile(request.user, env)
    profiles = list_profiles(request.user, env)
    t0 = time_mod.perf_counter()
    uid = getattr(request.user, 'id', None) if getattr(request, 'user', None) else None
    logger.info(
        'env_auto:init user_id=%s method=%s path=%s env=%s',
        uid,
        request.method,
        request.path,
        env,
    )
    sim_state = get_automation_market_simulation(request)
    try:
        sync_automation_sim_preference_from_request(request)
    except Exception:
        logger.exception('sync_automation_sim_preference_from_request automations_dashboard')
    if request.method == 'POST' and request.POST.get('form_name') == 'automation_strategies':
        save_strategy_toggles_from_post(
            request.user,
            env,
            request.POST,
            execution_profile=active_profile,
        )
        states_after = get_strategy_enabled_map(
            request.user,
            env,
            execution_profile=active_profile,
        )
        exec_after = get_strategy_execute_orders_map(
            request.user,
            env,
            execution_profile=active_profile,
        )
        active_titles = [
            next((s['title'] for s in AUTOMATION_STRATEGIES if s['key'] == k), k)
            for k, on in states_after.items()
            if on
        ]
        summary = ', '.join(active_titles) if active_titles else 'nenhuma estratégia ativa'
        exec_titles = [
            next((s['title'] for s in AUTOMATION_STRATEGIES if s['key'] == k), k)
            for k, on in exec_after.items()
            if on
        ]
        exec_summary = ', '.join(exec_titles) if exec_titles else 'nenhuma'
        live_ticker_note = ''
        if active_profile is not None:
            live_ticker_note = (getattr(active_profile, 'live_ticker', '') or '').strip().upper()
        record_automation_thought(
            request.user,
            env,
            (
                f'Estratégias salvas ({environment_label(env)}). '
                f'Ativas: {summary}. Execução de ordem: {exec_summary}. '
                f'Ticker de execução: {live_ticker_note or "todos os monitorados"} '
                f'(definido no cartão Robô principal).'
            ),
            source='estrategias',
            execution_profile=active_profile,
        )
        messages.success(
            request,
            f'Estratégias atualizadas para o ambiente {environment_label(env)}.',
        )
        return redirect('trader:automations_dashboard')
    if request.method == 'POST' and request.POST.get('form_name') == 'automation_runtime_toggle':
        runtime_env = normalize_environment(request.POST.get('runtime_environment') or env)
        runtime_profile = resolve_active_profile(request.user, runtime_env)
        action = (request.POST.get('runtime_action') or '').strip().lower()
        if action in ('save_limit', 'save_all'):
            target_enabled = runtime_enabled(request.user, runtime_env)
        else:
            target_enabled = (request.POST.get('robot_enabled') or '').strip().lower() in (
                '1',
                'true',
                'on',
                'yes',
            )
        try:
            max_open_ops = int(request.POST.get('max_open_operations') or '1')
        except (TypeError, ValueError):
            max_open_ops = 1
        live_ticker_selected = (request.POST.get('automation_live_ticker') or '').strip().upper()
        watch_tickers = set(_watch_tickers_list())
        if live_ticker_selected and live_ticker_selected not in watch_tickers:
            messages.error(
                request,
                'Ticker do bot inválido para execução. Escolha um ativo monitorado.',
            )
            return redirect('trader:automations_dashboard')
        if runtime_profile is not None:
            runtime_profile.live_ticker = live_ticker_selected
            runtime_profile.save(update_fields=['live_ticker', 'updated_at'])
        row_rt = set_runtime_enabled(
            request.user,
            runtime_env,
            enabled=target_enabled,
            max_open_operations=max_open_ops,
        )
        max_open_final = int(getattr(row_rt, 'max_open_operations', max_open_ops) or 1)
        ticker_line = live_ticker_selected or 'todos os monitorados'
        if action in ('save_limit', 'save_all'):
            thought_msg = (
                f'Configuração do robô guardada ({environment_label(runtime_env)}). '
                f'Estado: {"ligado" if target_enabled else "desligado"}. '
                f'Máx. operações abertas: {max_open_final}. Ticker: {ticker_line}.'
            )
            user_msg = (
                f'Definições guardadas para {environment_label(runtime_env)}. '
                f'Limite: {max_open_final}. Ticker: {ticker_line}. '
                f'Robô: {"ativo" if target_enabled else "inativo"}.'
            )
        else:
            thought_msg = (
                f'Robô de automações {"ATIVADO" if target_enabled else "DESATIVADO"} '
                f'no ambiente {environment_label(runtime_env)}. '
                f'Máx. operações abertas simultâneas: {max_open_final}. '
                f'Ticker de execução: {ticker_line}.'
            )
            user_msg = (
                f'Robô {"ativado" if target_enabled else "desativado"} '
                f'para {environment_label(runtime_env)}. '
                f'Limite: {max_open_final}. Ticker do bot: {ticker_line}.'
            )
        record_automation_thought(
            request.user,
            runtime_env,
            thought_msg,
            source='robo_global',
            execution_profile=runtime_profile,
        )
        messages.success(request, user_msg)
        return redirect('trader:automations_dashboard')

    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    base = raw_ticker if raw_ticker in ('WIN', 'WDO') else None
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    logger.info(
        'env_auto:market_ctx_start user_id=%s raw_ticker=%s resolved_ticker=%s sim_effective=%s',
        uid,
        raw_ticker,
        ticker,
        bool(sim_state.get('effective')),
    )
    strat_states = get_strategy_enabled_map(
        request.user,
        env,
        execution_profile=active_profile,
    )
    exec_states = get_strategy_execute_orders_map(
        request.user,
        env,
        execution_profile=active_profile,
    )
    strategies_display = sorted(
        [
            strategy_display_dict(
                s,
                enabled=strat_states[s['key']],
                execute_orders=exec_states.get(s['key'], False),
                params=get_strategy_params(
                    request.user,
                    s['key'],
                    env,
                    execution_profile=active_profile,
                ),
            )
            for s in AUTOMATION_STRATEGIES
        ],
        key=lambda d: (
            0 if d.get('automation_role') == 'active' else 1,
            d.get('group') or '',
            d.get('title') or '',
        ),
    )
    thought_rows = fetch_thoughts_for_poll(
        request.user,
        env,
        since_id=None,
        execution_profile=active_profile,
    )
    thoughts_initial = [thought_to_dict(r) for r in thought_rows]
    thoughts_last_id = max((r.id for r in thought_rows), default=0)
    passive_insights_initial = passive_insight_cards_from_thoughts(thoughts_initial)
    ctx: dict = {
        'page_title': 'Automações',
        'nav_section': 'automations',
        'dashboard_home': True,
        'hide_sidebar_boleta': True,
        'strategies_display': strategies_display,
        'automation_environment_label': environment_label(env),
        'automation_market_sim': sim_state,
        'thoughts_initial': thoughts_initial,
        'thoughts_last_id': thoughts_last_id,
        'passive_insights_initial': passive_insights_initial,
        'automation_thoughts_poll_ms': 3200,
        'automation_profiles': profiles,
        'automation_active_profile_id': getattr(active_profile, 'id', None),
        'automation_active_profile_name': getattr(active_profile, 'name', ''),
        'automation_trailing_adjustment_enabled': trailing_stop_adjustment_enabled(
            request.user, env, execution_profile=active_profile
        ),
        'automation_robot_enabled': runtime_enabled(request.user, env),
        'automation_max_open_operations': runtime_max_open_operations(request.user, env),
        'automation_environment_value': env,
        'replay_fiction_profile': _replay_shadow_custody_panel(request)
        if env == ENV_REPLAY
        else {'show': False},
    }
    use_day_sim = bool(sim_state.get('effective'))
    if use_day_sim and sim_state.get('session_date') and sim_state.get('sim_ticker'):
        sim_sym = sim_state['sim_ticker']
        ctx.update(
            build_market_context_local_for_session_day(sim_sym, sim_state['session_date'])
        )
        ctx['ticker'] = sim_sym
        ctx['market_live_poll'] = False
    else:
        ctx.update(build_market_context_local(ticker))
        if base and quote_status_is_end_of_day(ctx.get('quote')):
            ticker = resolve_daytrade_base_ticker(request, base, force=True)
            ctx.update(build_market_context_local(ticker))
        ctx['market_live_poll'] = True
    logger.info(
        'env_auto:market_ctx_done user_id=%s elapsed_ms=%.1f live_poll=%s',
        uid,
        (time_mod.perf_counter() - t0) * 1000.0,
        ctx.get('market_live_poll'),
    )
    ctx.update(get_daytrade_candidates_text_context(request))
    ctx['watch_tickers_text'] = _watch_tickers_text()
    watch_tickers = _watch_tickers_list()
    live_ticker_selected = (getattr(active_profile, 'live_ticker', '') or '').strip().upper()
    if live_ticker_selected and live_ticker_selected not in watch_tickers:
        live_ticker_selected = ''
    chips = get_daytrade_chip_suggestions(request)
    ctx['ticker_suggestions_daytrade'] = chips
    ctx['ticker_suggestions_equities'] = default_ticker_suggestions_equities()
    if 'market_live_poll' not in ctx:
        ctx['market_live_poll'] = True
    ctx['default_ticker_js'] = chips[0]
    sim_tickers = _quote_snapshot_tickers_with_data()
    if sim_state.get('sim_ticker') and sim_state['sim_ticker'] in sim_tickers:
        sim_sel = sim_state['sim_ticker']
    elif ticker in sim_tickers:
        sim_sel = ticker
    elif sim_tickers:
        sim_sel = sim_tickers[0]
    else:
        sim_sel = ''
    ctx['automation_sim_ticker_choices'] = sim_tickers
    ctx['automation_live_ticker_choices'] = watch_tickers
    ctx['automation_live_selected_ticker'] = live_ticker_selected
    ctx['automation_sim_selected_ticker'] = sim_sel
    ctx['automation_sim_date_choices'] = (
        _automation_sim_date_choices(sim_sel) if sim_sel else []
    )
    ctx['form'] = None
    ctx['result_json'] = None
    ctx['send_error'] = None
    logger.info(
        'env_auto:ready user_id=%s elapsed_ms=%.1f ticker=%s sim_ticker_choices=%s',
        uid,
        (time_mod.perf_counter() - t0) * 1000.0,
        ctx.get('ticker'),
        len(ctx.get('automation_sim_ticker_choices') or []),
    )
    ctx.update(_replay_virtual_clock_template_context(request, env, sim_state))
    return render(request, 'trader/automacoes/dashboard.html', ctx)


@login_required
@never_cache
@require_GET
def automations_logs_day(request):
    """Logs por dia civil (BRT), com filtro de data e ambiente."""
    session_env = get_session_environment(request)
    active_profile = resolve_active_profile(request.user, session_env)
    raw_env = (request.GET.get('env') or '').strip().lower()
    filter_env = raw_env if raw_env in (ENV_REAL, ENV_SIMULATOR, ENV_REPLAY) else session_env
    filter_profile = resolve_active_profile(request.user, filter_env)
    day = parse_calendar_day_brt(request.GET.get('day'))
    start, end = calendar_day_bounds_brt(day)
    qs = (
        AutomationThought.objects.filter(
            user=request.user,
            trading_environment=filter_env,
            created_at__gte=start,
            created_at__lt=end,
        )
        .filter(Q(execution_profile=filter_profile) | Q(execution_profile__isnull=True))
        .order_by('-created_at')
        .only('id', 'created_at', 'source', 'kind', 'message')
    )
    paginator = Paginator(qs, 150)
    page_obj = paginator.get_page(request.GET.get('page'))
    log_rows = [thought_to_dict(r) for r in page_obj.object_list]
    today_brt = timezone.now().astimezone(_TZ_BRT).date()
    prev_day = day - timedelta(days=1)
    next_day = day + timedelta(days=1)
    sim_state = get_automation_market_simulation(request)
    strat_states = get_strategy_enabled_map(
        request.user,
        session_env,
        execution_profile=active_profile,
    )
    exec_states = get_strategy_execute_orders_map(
        request.user,
        session_env,
        execution_profile=active_profile,
    )
    strategies_display = [
        strategy_display_dict(
            s,
            enabled=strat_states[s['key']],
            execute_orders=exec_states.get(s['key'], False),
            params=get_strategy_params(
                request.user,
                s['key'],
                session_env,
                execution_profile=active_profile,
            ),
        )
        for s in AUTOMATION_STRATEGIES
    ]
    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    sim_tickers = _quote_snapshot_tickers_with_data()
    if sim_state.get('sim_ticker') and sim_state['sim_ticker'] in sim_tickers:
        sim_sel = sim_state['sim_ticker']
    elif ticker in sim_tickers:
        sim_sel = ticker
    elif sim_tickers:
        sim_sel = sim_tickers[0]
    else:
        sim_sel = ''
    ctx = {
        'page_title': 'Logs por dia',
        'nav_section': 'automations',
        'hide_sidebar_boleta': True,
        'strategies_display': strategies_display,
        'automation_environment_label': environment_label(session_env),
        'automation_market_sim': sim_state,
        'automation_sim_ticker_choices': sim_tickers,
        'automation_sim_selected_ticker': sim_sel,
        'automation_sim_date_choices': (
            _automation_sim_date_choices(sim_sel) if sim_sel else []
        ),
        'logs_day': day,
        'logs_day_iso': day.isoformat(),
        'logs_filter_env': filter_env,
        'logs_filter_env_label': environment_label(filter_env),
        'log_rows': log_rows,
        'page_obj': page_obj,
        'logs_total': paginator.count,
        'prev_day_iso': prev_day.isoformat(),
        'next_day_iso': next_day.isoformat(),
        'next_day_disabled': day >= today_brt,
        'today_iso': today_brt.isoformat(),
        'automation_trailing_adjustment_enabled': trailing_stop_adjustment_enabled(
            request.user, session_env, execution_profile=active_profile
        ),
    }
    return render(request, 'trader/automacoes/logs_day.html', ctx)


@login_required
@require_POST
def automation_market_simulation(request):
    """Ativa/desativa simulação de mercado por dia (somente ambiente simulador)."""
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    try:
        return _automation_market_simulation_inner(request, next_path)
    finally:
        try:
            sync_automation_sim_preference_from_request(request)
        except Exception:
            logger.exception('sync_automation_sim_preference automation_market_simulation')


def _automation_market_simulation_inner(request, next_path: str):
    if get_session_environment(request) not in (ENV_SIMULATOR, ENV_REPLAY):
        messages.warning(
            request,
            'Sessão por dia (snapshots locais) só está disponível no Simulador ou em Replay.',
        )
        return redirect(next_path)
    enabled = request.POST.get('sim_enabled') == 'on'
    raw_date = (request.POST.get('session_date') or '').strip()[:10]
    raw_sym = (request.POST.get('sim_ticker') or '').strip().upper()
    allowed_syms = set(_quote_snapshot_tickers_with_data())
    # Símbolo exato da lista (ex.: WINJ26 no banco) não passa por resolução WIN→contrato ativo.
    if raw_sym and raw_sym in allowed_syms:
        sim_ticker = raw_sym
    elif raw_sym:
        sim_ticker = resolve_daytrade_base_ticker(request, raw_sym)
    else:
        sim_ticker = ''

    if enabled:
        if not raw_sym:
            messages.error(
                request,
                'Escolha uma ação que tenha snapshots salvos no banco.',
            )
            return redirect(next_path)
        if sim_ticker not in allowed_syms:
            messages.error(
                request,
                'Esta ação não possui dados salvos. Escolha outra na lista.',
            )
            return redirect(next_path)
        if not raw_date:
            messages.error(
                request,
                'Escolha um dia da lista com snapshots para esta ação.',
            )
            return redirect(next_path)
        allowed_dates = set(_quote_snapshot_session_dates_iso(sim_ticker))
        if raw_date not in allowed_dates:
            messages.error(
                request,
                'Esse dia não tem snapshots salvos para esta ação. Escolha outra data.',
            )
            return redirect(next_path)

    st = set_automation_market_simulation(
        request,
        enabled=enabled,
        session_date_iso=raw_date if enabled else None,
        sim_ticker=sim_ticker if enabled else None,
    )
    env = get_session_environment(request)
    if enabled:
        if not st['effective']:
            messages.error(request, 'Não foi possível ativar a simulação. Tente novamente.')
        else:
            record_automation_thought(
                request.user,
                env,
                (
                    f'Simulação de mercado ativa: {st["sim_ticker"]} · dia {st["label_br"]}. '
                    'O painel usa o último snapshot salvo desse dia como referência; '
                    'o gráfico pode ser alinhado à mesma ação e dia.'
                ),
                source='simulacao_mercado',
            )
            messages.success(
                request,
                f'Simulação: pregão de {st["label_br"]} (dados locais).',
            )
    else:
        record_automation_thought(
            request.user,
            env,
            'Simulação de mercado desativada; voltando à cotação ao vivo no painel.',
            source='simulacao_mercado',
        )
        messages.success(request, 'Simulação de mercado desativada.')
    return redirect(next_path)


@login_required
@require_GET
def automations_state_json(request):
    """Estado das estratégias para o ambiente ativo na sessão (consumo por robô ou scripts)."""
    env = get_session_environment(request)
    active_profile = resolve_active_profile(request.user, env)
    profiles = list_profiles(request.user, env)
    states = get_strategy_enabled_map(
        request.user,
        env,
        execution_profile=active_profile,
    )
    sim = get_automation_market_simulation(request)
    return JsonResponse(
        {
            'environment': env,
            'environment_label': environment_label(env),
            'strategies': states,
            'market_simulation': {
                'available': env in (ENV_SIMULATOR, ENV_REPLAY),
                'effective': sim['effective'],
                'session_date': sim['session_date_iso'],
                'sim_ticker': sim.get('sim_ticker') or '',
                'label': sim['label_br'],
            },
            'execution_profile': {
                'active_id': getattr(active_profile, 'id', None),
                'active_name': getattr(active_profile, 'name', ''),
                'live_ticker': getattr(active_profile, 'live_ticker', '') or '',
                'items': [
                    {
                        'id': int(p.id),
                        'name': p.name,
                        'mode': p.mode,
                        'is_active': bool(p.is_active),
                        'live_ticker': p.live_ticker or '',
                    }
                    for p in profiles
                ],
            },
            'robot_enabled': runtime_enabled(request.user, env),
            'max_open_operations': runtime_max_open_operations(request.user, env),
        }
    )


@login_required
@require_POST
def automation_profile_select(request):
    env = get_session_environment(request)
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    try:
        pid = int(request.POST.get('profile_id') or '0')
    except ValueError:
        pid = 0
    p = set_active_profile(request.user, env, pid)
    messages.success(request, f'Perfil ativo: {p.name}.')
    return redirect(next_path)


@login_required
@require_POST
def automation_profile_create(request):
    env = get_session_environment(request)
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    sim = get_automation_market_simulation(request)
    if env not in (ENV_SIMULATOR, ENV_REPLAY) or not sim.get('effective'):
        messages.error(
            request,
            'Para criar perfil, ative primeiro a sessão com ticker e dia válidos (Simulador ou Replay).',
        )
        return redirect(next_path)
    st_env = strategy_toggle_storage_environment(env)
    source_profile = resolve_active_profile(request.user, env)
    name = (request.POST.get('profile_name') or '').strip()
    sim_ticker = (sim.get('sim_ticker') or '').strip().upper()
    sd = sim.get('session_date')
    if not sim_ticker or sd is None:
        messages.error(request, 'Simulação ativa sem ticker/dia válidos.')
        return redirect(next_path)
    p = create_sim_profile(
        request.user,
        env,
        name=name,
        sim_ticker=sim_ticker,
        session_date=sd,
    )
    src_rows = list(
        AutomationStrategyToggle.objects.filter(
            user=request.user,
            trading_environment=st_env,
            execution_profile=source_profile,
        ).values('strategy_key', 'enabled')
    )
    for row in src_rows:
        key = str(row.get('strategy_key') or '').strip()
        if not key:
            continue
        AutomationStrategyToggle.objects.update_or_create(
            user=request.user,
            trading_environment=st_env,
            strategy_key=key,
            execution_profile=p,
            defaults={'enabled': bool(row.get('enabled'))},
        )
    set_active_profile(request.user, env, int(p.id))
    start_profile_runtime(p, clear_cursor=True)
    messages.success(request, f'Perfil criado: {p.name}.')
    return redirect(next_path)


@login_required
@require_POST
def automation_profile_start(request):
    env = get_session_environment(request)
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    p = resolve_active_profile(request.user, env)
    clear_logs = request.POST.get('clear_logs') == 'on'
    start_profile_runtime(p, clear_cursor=True)
    if clear_logs:
        AutomationThought.objects.filter(
            user=request.user,
            trading_environment=env,
            execution_profile=p,
        ).delete()
        AutomationTriggerMarker.objects.filter(
            user=request.user,
            trading_environment=env,
            execution_profile=p,
        ).delete()
        if env == ENV_REPLAY:
            try:
                delete_replay_shadow_ledger()
            except Exception:
                logger.exception('delete_replay_shadow_ledger após limpar logs (iniciar perfil)')
            try:
                invalidate_collateral_custody_cache()
            except Exception:
                logger.exception('invalidate_collateral_custody_cache após limpar replay')
    messages.success(request, f'Execução iniciada no perfil {p.name}.')
    return redirect(next_path)


@login_required
@require_GET
def automation_thoughts_json(request):
    """Novos pensamentos com id maior que ``since`` (polling)."""
    env = get_session_environment(request)
    active_profile = resolve_active_profile(request.user, env)
    try:
        since_raw = request.GET.get('since') or '0'
        since_id = int(since_raw)
    except ValueError:
        since_id = 0
    rows = fetch_thoughts_for_poll(
        request.user,
        env,
        since_id=since_id,
        execution_profile=active_profile,
    )
    return JsonResponse(
        {
            'environment': env,
            'execution_profile': getattr(active_profile, 'name', ''),
            'thoughts': [thought_to_dict(r) for r in rows],
            'poll_ms': 3200,
        }
    )


@login_required
@require_POST
def automation_clear_thoughts(request):
    """
    Limpa logs de automação do utilizador no ambiente selecionado.
    """
    session_env = get_session_environment(request)
    env_raw = (request.POST.get('env') or '').strip().lower()
    env = env_raw if env_raw in (ENV_SIMULATOR, ENV_REAL, ENV_REPLAY) else session_env
    active_profile = resolve_active_profile(request.user, env)
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    q_logs = AutomationThought.objects.filter(
        user=request.user,
        trading_environment=env,
    )
    if active_profile is not None:
        q_logs = q_logs.filter(
            Q(execution_profile=active_profile) | Q(execution_profile__isnull=True)
        )
    deleted, _ = q_logs.delete()
    q_markers = AutomationTriggerMarker.objects.filter(
        user=request.user,
        trading_environment=env,
    )
    if active_profile is not None:
        q_markers = q_markers.filter(
            Q(execution_profile=active_profile) | Q(execution_profile__isnull=True)
        )
    q_markers.delete()
    replay_note = ''
    if env == ENV_REPLAY:
        try:
            rs = delete_replay_shadow_ledger()
            replay_note = (
                f' Ledger replay fictício limpo ({rs["positions"]} pos., '
                f'{rs["closed_operations"]} PnL encerrados).'
            )
        except Exception:
            logger.exception('delete_replay_shadow_ledger após automation_clear_thoughts')
            replay_note = ' (Aviso: falha ao limpar ledger replay fictício.)'
        try:
            invalidate_collateral_custody_cache()
        except Exception:
            logger.exception('invalidate_collateral_custody_cache após limpar logs')
    messages.success(
        request,
        (
            f'Logs limpos em {environment_label(env)} / perfil '
            f'{getattr(active_profile, "name", "Tempo_Real")} ({deleted} registro(s) removido(s)).'
            f'{replay_note}'
        ),
    )
    return redirect(next_path)


@login_required
@require_POST
def automation_replay_shadow_ledger_clear(request):
    """
    Apaga só o ledger ``replay_shadow`` (posições fictícias e P/L encerrado), sem limpar logs.
    Só no ambiente Replay.
    """
    session_env = get_session_environment(request)
    next_path = _safe_same_origin_path(request.POST.get('next') or '', '/automacoes/')
    if session_env != ENV_REPLAY:
        messages.error(request, 'Limpar o ledger fictício só está disponível no ambiente Replay.')
        return redirect(next_path)
    try:
        rs = delete_replay_shadow_ledger()
        try:
            invalidate_collateral_custody_cache()
        except Exception:
            logger.exception('invalidate_collateral_custody_cache após limpar ledger replay')
        messages.success(
            request,
            (
                'Ledger replay fictício limpo: '
                f'{rs["positions"]} posição(ões), '
                f'{rs["closed_operations"]} registo(s) de P/L encerrado.'
            ),
        )
    except Exception:
        logger.exception('delete_replay_shadow_ledger em automation_replay_shadow_ledger_clear')
        messages.error(request, 'Não foi possível limpar o ledger replay fictício.')
    return redirect(next_path)


@login_required
@require_POST
def automation_sim_replay_cursor(request):
    """
    Grava o instante do scrubber de replay em ``AutomationMarketSimPreference.replay_until``
    (mesmo critério temporal que ``replay_until`` em ``quote_candles_json``).
    """
    session_env = get_session_environment(request)
    if session_env != ENV_REPLAY:
        return JsonResponse({'ok': False, 'error': 'replay_only'}, status=403)
    sim = get_automation_market_simulation(request)
    if not sim.get('effective'):
        return JsonResponse({'ok': False, 'error': 'simulation_inactive'}, status=400)
    from trader.automacoes.leafar_candles import parse_replay_until_iso
    from trader.models import AutomationMarketSimPreference

    raw = (request.POST.get('replay_until') or '').strip()
    dt = parse_replay_until_iso(raw) if raw else None
    sd = sim.get('session_date')
    sym = (sim.get('sim_ticker') or '').strip().upper()
    # Replay envia cursor em alta frequência; evita escrita redundante e tolera lock transitório no SQLite.
    replay_fp = dt.isoformat() if dt else '-'
    cursor_ck = (
        f'automation:replay_cursor:last_fp:{request.user.id}:{session_env}:'
        f'{sym}:{sd.isoformat() if sd else "-"}'
    )
    last_fp = cache.get(cursor_ck)
    if last_fp != replay_fp:
        wrote = False
        for attempt in range(3):
            try:
                AutomationMarketSimPreference.objects.update_or_create(
                    user=request.user,
                    trading_environment=session_env,
                    defaults={
                        'enabled': True,
                        'session_date': sd,
                        'sim_ticker': sym,
                        'replay_until': dt,
                    },
                )
                cache.set(cursor_ck, replay_fp, timeout=12 * 3600)
                wrote = True
                break
            except OperationalError as exc:
                msg = str(exc).lower()
                if 'database is locked' not in msg:
                    raise
                if attempt >= 2:
                    logger.warning(
                        'automation_sim_replay_cursor: sqlite lock persistente (user=%s sym=%s day=%s)',
                        getattr(request.user, 'id', None),
                        sym,
                        sd,
                    )
                    break
                time_mod.sleep(0.06 * (attempt + 1))
        if not wrote:
            # Não derruba o replay por lock transitório de escrita.
            pass
    want_strategies = (request.POST.get('strategies', '1') or '1').strip().lower() not in (
        '0',
        'false',
        'no',
        'off',
    )
    try:
        from trader.automacoes.automation_engine import run_automation_session_replay_now

        if want_strategies and sd is not None and sym:
            # Replay pode avançar em frames muito curtos (1s); para manter comportamento
            # próximo ao "tempo real", dispara estratégias apenas quando muda o bucket
            # do intervalo do motor (ex.: 10s), evitando rajadas de alertas.
            iv_raw = getattr(settings, 'TRADER_LEAFAR_INTERVAL_SEC', 10)
            try:
                iv = max(1, min(int(iv_raw), 300))
            except (TypeError, ValueError):
                iv = 10

            should_dispatch = True
            if dt is not None:
                bucket = int(dt.timestamp()) // iv
                ck = (
                    f'automation:replay_cursor:last_bucket:'
                    f'{request.user.id}:{session_env}:{sym}:{sd.isoformat()}'
                )
                prev = cache.get(ck)
                should_dispatch = prev != bucket
                if should_dispatch:
                    cache.set(ck, bucket, timeout=12 * 3600)

            if should_dispatch:
                run_automation_session_replay_now(
                    request.user,
                    session_day=sd,
                    sim_ticker=sym,
                    replay_until=dt,
                    trading_environment=ENV_REPLAY,
                )
    except Exception:
        logger.exception('automation_sim_replay_cursor: motor de estratégias no instante do replay')

    return JsonResponse(
        {
            'ok': True,
            'replay_until': dt.isoformat() if dt else None,
        }
    )


@login_required
@require_POST
def automation_replay_stream_start(request):
    """
    Enfileira :func:`~trader.tasks.stream_replay_ticks_task` — mesmo pipeline temporal
    que :func:`~trader.services.replay_stream_motor.stream_session_replay_ticks`
    (instantes ``QuoteSnapshot`` em ordem, ``force=True`` no motor).
    """
    if get_session_environment(request) != ENV_REPLAY:
        return JsonResponse({'ok': False, 'error': 'replay_only'}, status=403)
    sim = get_automation_market_simulation(request)
    if not sim['effective'] or sim.get('session_date') is None:
        return JsonResponse({'ok': False, 'error': 'simulation_inactive'}, status=400)
    if not runtime_enabled(request.user, ENV_REPLAY):
        return JsonResponse({'ok': False, 'error': 'runtime_disabled'}, status=400)

    body: dict[str, Any] = {}
    ct = (request.content_type or '').lower()
    if 'application/json' in ct:
        try:
            body = json.loads(request.body.decode() or '{}')
        except json.JSONDecodeError:
            body = {}
    elif request.POST:
        body = {k: v for k, v in request.POST.items()}

    try:
        pace_sec = float(body.get('pace_sec', 1.0))
    except (TypeError, ValueError):
        pace_sec = 1.0
    max_snapshots: int | None
    raw_max = body.get('max_snapshots')
    if isinstance(raw_max, (list, tuple)) and raw_max:
        raw_max = raw_max[0]
    if raw_max is None or raw_max == '':
        max_snapshots = None
    else:
        try:
            max_snapshots = int(raw_max)
        except (TypeError, ValueError):
            max_snapshots = None
        if max_snapshots is not None and max_snapshots < 1:
            max_snapshots = None

    sd = sim['session_date']
    default_t = (sim.get('sim_ticker') or '').strip().upper() or default_primary_ticker()
    raw_ticker = (body.get('ticker') or default_t)
    raw_ticker = str(raw_ticker or '').strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)

    from trader.tasks import stream_replay_ticks_task

    ar = stream_replay_ticks_task.delay(
        int(request.user.id),
        ticker,
        sd.isoformat(),
        pace_sec,
        max_snapshots,
    )
    return JsonResponse(
        {
            'ok': True,
            'task_id': getattr(ar, 'id', None),
            'ticker': ticker,
            'session_date': sd.isoformat(),
            'pace_sec': pace_sec,
            'max_snapshots': max_snapshots,
        }
    )


@login_required
@require_GET
def automation_replay_day_json(request):
    """Snapshots do dia em ordem (ambiente Replay com sessão activa)."""
    if get_session_environment(request) != ENV_REPLAY:
        return JsonResponse({'error': 'replay_only'}, status=403)
    sim = get_automation_market_simulation(request)
    if not sim['effective'] or sim.get('session_date') is None:
        return JsonResponse({'error': 'simulation_inactive'}, status=400)
    default_t = (sim.get('sim_ticker') or '').strip().upper() or default_primary_ticker()
    raw_ticker = (request.GET.get('ticker') or default_t).strip().upper()
    ticker = resolve_ticker_for_local_snapshots(request, raw_ticker)
    from trader.automacoes.replay import (
        DEFAULT_REPLAY_CHUNK,
        MAX_REPLAY_CHUNK,
        build_replay_frames_page,
    )

    try:
        offset = int(request.GET.get('offset') or '0')
    except ValueError:
        offset = 0
    try:
        limit = int(request.GET.get('limit') or str(DEFAULT_REPLAY_CHUNK))
    except ValueError:
        limit = DEFAULT_REPLAY_CHUNK
    offset = max(0, offset)
    limit = max(1, min(limit, MAX_REPLAY_CHUNK))
    align_bucket = (request.GET.get('align_bucket_start') or '').strip()

    frames, meta = build_replay_frames_page(
        ticker,
        sim['session_date'],
        offset=offset,
        limit=limit,
        align_bucket_start=align_bucket or None,
    )
    return JsonResponse(
        {
            'ticker': ticker,
            'session_date': sim['session_date'].isoformat(),
            'frames': frames,
            'meta': meta,
        }
    )
