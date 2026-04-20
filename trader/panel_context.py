"""Contexto compartilhado para painel Mercado e Ordens (home + páginas dedicadas)."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, time as time_cls, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.cache import cache
from django.db.models import Sum
from django.utils.html import escape, format_html
from django.utils.safestring import SafeString

from api_auth.exceptions import SmartTraderConfigurationError, SmartTraderSignatureError

from trader.environment import (
    ENV_REAL,
    ENV_SIMULATOR,
    environment_label,
    get_current_environment,
    get_session_environment,
)
from trader.market_defaults import default_primary_ticker
from trader.market_defaults import default_daytrade_win_ticker, default_daytrade_wdo_ticker
from trader.order_enums import (
    ORDER_MODULE_DAY_TRADE,
    ORDER_SIDES,
    ORDER_TIME_IN_FORCE_VALUES,
    ORDER_TIF_DAY,
    ORDER_TYPES_INTERNAL,
    ORDER_TYPE_MARKET_INTERNAL,
)
from trader.services.marketdata import (
    fetch_aggregate_book,
    fetch_book,
    fetch_quote,
    fetch_ticker_details,
    ohlc_bar_chart_payload,
)
from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)
from trader.services.orders import (
    SIMULATOR_SETUP_ORDER_STATUSES,
    fetch_orders_cached,
    invalidate_intraday_orders_cache,
)
from trader.services.trade_markers import record_trade_marker
from trader.smart_trader_limits import (
    daily_order_limit_for_ticker,
    extract_bmf_base,
    ticket_limit_for_ticker,
)

logger = logging.getLogger(__name__)
_DISPLAY_TZ_BRT = ZoneInfo('America/Sao_Paulo')

# Preset da boleta: não chama POST /v1/setup/orders; envia limitada com preço que não executa.
ORDER_TEST_SETUP_OPEN_LIMITED = 'open_limited'
ORDER_TEST_SETUP_CHOICES: frozenset[str] = frozenset(
    {ORDER_TEST_SETUP_OPEN_LIMITED, *SIMULATOR_SETUP_ORDER_STATUSES}
)

# --- Resolução dinâmica de day trade BMF (WIN/WDO → contrato ativo) ---
#
# A UI pode aceitar `WIN`/`WDO` como atalho; o backend resolve o ticker completo
# consultando `quote.status` (evita manter "rolamento" fixo no .env).
#
# Os candidatos de rolamento ficam salvos na `session` do usuário (digitados por você).

_SESSION_DAYTRADE_WIN_CANDIDATES = 'daytrade_win_candidates_text'
_SESSION_DAYTRADE_WDO_CANDIDATES = 'daytrade_wdo_candidates_text'

_SESSION_DAYTRADE_ACTIVE_WIN = 'daytrade_active_win_ticker'
_SESSION_DAYTRADE_ACTIVE_WIN_AT = 'daytrade_active_win_ticker_at'

_SESSION_DAYTRADE_ACTIVE_WDO = 'daytrade_active_wdo_ticker'
_SESSION_DAYTRADE_ACTIVE_WDO_AT = 'daytrade_active_wdo_ticker_at'

# Reduz chamadas repetidas em polling (quote até 20/5s; detalhes/book limitam 1/s).
# Evita re-probing da API (vários fetch_quote) a cada poucos segundos nos pollers do painel.
_ACTIVE_CACHE_SEC = 120.0
_MAX_CANDIDATES_TO_TEST = 6
_TICKER_NOT_ALLOWED_CACHE_KEY_PREFIX = 'orders:ticker_not_allowed:v1'

_CUSTODY_TICKER_KEYS = ('ticker', 'Ticker', 'symbol', 'Symbol')
_CUSTODY_QTY_KEYS = (
    'availableQuantity',
    'AvailableQuantity',
    'quantity',
    'Quantity',
    'positionQuantity',
    'PositionQuantity',
    'totalQuantity',
    'TotalQuantity',
)


def _ticker_not_allowed_cache_key(module: str, ticker: str) -> str:
    mod = (module or '').strip().upper()
    sym = (ticker or '').strip().upper()
    return f'{_TICKER_NOT_ALLOWED_CACHE_KEY_PREFIX}:{mod}:{sym}'


def _ticker_not_allowed_ttl_sec() -> int:
    raw = getattr(settings, 'TRADER_TICKER_NOT_ALLOWED_TTL_SEC', 600)
    try:
        ttl = int(raw)
    except (TypeError, ValueError):
        ttl = 600
    return max(30, ttl)


def _cache_ticker_not_allowed(module: str, ticker: str, reason: str) -> None:
    key = _ticker_not_allowed_cache_key(module, ticker)
    cache.set(key, str(reason or '').strip(), timeout=_ticker_not_allowed_ttl_sec())


def _get_cached_ticker_not_allowed(module: str, ticker: str) -> str | None:
    key = _ticker_not_allowed_cache_key(module, ticker)
    hit = cache.get(key)
    if isinstance(hit, str) and hit.strip():
        return hit.strip()
    return None


def _clear_cached_ticker_not_allowed(module: str, ticker: str) -> None:
    key = _ticker_not_allowed_cache_key(module, ticker)
    cache.delete(key)


def _base_ticker_from_input(ticker: str) -> str | None:
    t = (ticker or '').strip().upper()
    if t in ('WIN', 'WDO'):
        return t
    return None


def _parse_candidates_text(raw: str | None) -> list[str]:
    if raw is None:
        return []
    s = str(raw).strip().upper()
    if not s:
        return []
    parts = re.split(r'[,\s]+', s)
    out: list[str] = []
    for p in parts:
        p = (p or '').strip().upper()
        if p and p not in out:
            out.append(p)
    return out


def _get_candidates_text_by_base(request, base: str) -> str:
    key = _SESSION_DAYTRADE_WIN_CANDIDATES if base == 'WIN' else _SESSION_DAYTRADE_WDO_CANDIDATES
    fallback = default_daytrade_win_ticker() if base == 'WIN' else default_daytrade_wdo_ticker()
    raw = getattr(request, 'session', {}).get(key)  # type: ignore[union-attr]
    if raw is None:
        return fallback
    v = str(raw).strip()
    return v if v else fallback


def _get_candidates_list_by_base(request, base: str) -> list[str]:
    text = _get_candidates_text_by_base(request, base)
    candidates = _parse_candidates_text(text)
    return candidates if candidates else [default_daytrade_win_ticker() if base == 'WIN' else default_daytrade_wdo_ticker()]


def get_daytrade_candidates_text_context(request) -> dict[str, str]:
    return {
        'daytrade_win_candidates_text': _get_candidates_text_by_base(request, 'WIN'),
        'daytrade_wdo_candidates_text': _get_candidates_text_by_base(request, 'WDO'),
    }


def get_daytrade_chip_suggestions(request) -> tuple[str, str]:
    """
    Sugestões para chips e defaults do painel.
    Pega o 1o candidato de cada base (WIN e WDO) salvo na session; se não houver,
    usa fallback do .env.
    """
    win = (_get_candidates_list_by_base(request, 'WIN') or [default_daytrade_win_ticker()])[0]
    wdo = (_get_candidates_list_by_base(request, 'WDO') or [default_daytrade_wdo_ticker()])[0]
    return win, wdo


def get_daytrade_primary_ticker_from_session(request) -> str:
    """Default do painel (mini índice) usando o 1o candidato de WIN da session."""
    return get_daytrade_chip_suggestions(request)[0]


def set_daytrade_candidates_text(request, *, base: str, raw_text: str | None) -> None:
    if base not in ('WIN', 'WDO'):
        return
    key = _SESSION_DAYTRADE_WIN_CANDIDATES if base == 'WIN' else _SESSION_DAYTRADE_WDO_CANDIDATES
    candidates = _parse_candidates_text(raw_text)
    if not candidates:
        return
    # Mantém ordenação digitada; evita duplicatas via _parse_candidates_text.
    request.session[key] = ','.join(candidates)  # type: ignore[index]


def _get_active_cached(request, base: str) -> str | None:
    now = time.time()
    if base == 'WIN':
        at = request.session.get(_SESSION_DAYTRADE_ACTIVE_WIN_AT)
        val = request.session.get(_SESSION_DAYTRADE_ACTIVE_WIN)
    else:
        at = request.session.get(_SESSION_DAYTRADE_ACTIVE_WDO_AT)
        val = request.session.get(_SESSION_DAYTRADE_ACTIVE_WDO)
    if not at or not val:
        return None
    try:
        if now - float(at) <= _ACTIVE_CACHE_SEC:
            return str(val).strip().upper()
    except Exception:
        return None
    return None


def _set_active_cached(request, base: str, ticker: str) -> None:
    t = (ticker or '').strip().upper()
    if not t:
        return
    now = time.time()
    if base == 'WIN':
        request.session[_SESSION_DAYTRADE_ACTIVE_WIN] = t
        request.session[_SESSION_DAYTRADE_ACTIVE_WIN_AT] = now
    else:
        request.session[_SESSION_DAYTRADE_ACTIVE_WDO] = t
        request.session[_SESSION_DAYTRADE_ACTIVE_WDO_AT] = now


def resolve_daytrade_base_ticker(request, ticker: str, *, force: bool = False) -> str:
    """
    Se `ticker` for `WIN` ou `WDO`, resolve para um contrato completo consultando
    `quote.status` (Trading != EndOfDay).

    Caso contrário, retorna o ticker normalizado.
    """
    base = _base_ticker_from_input(ticker)
    if base is None:
        return (ticker or '').strip().upper()

    if not force:
        cached = _get_active_cached(request, base)
        if cached:
            return cached

    candidates = _get_candidates_list_by_base(request, base)[:_MAX_CANDIDATES_TO_TEST]
    if not candidates:
        return base

    # Escolha: primeiro candidato cuja cotação NÃO esteja em EndOfDay.
    for c in candidates:
        try:
            q = fetch_quote(c, use_cache=False)
            if not quote_status_is_end_of_day(q):
                _set_active_cached(request, base, c)
                return c
        except Exception:
            # Ignora falhas de um candidato e tenta o próximo.
            continue

    chosen = candidates[0]
    _set_active_cached(request, base, chosen)
    return chosen


def resolve_ticker_for_local_snapshots(request, raw_ticker: str) -> str:
    """
    Símbolo para leitura de ``QuoteSnapshot`` (histórico, gráfico, datas de sessão).

    Para ``WIN``/``WDO``, o ``resolve_daytrade_base_ticker`` usa a API para o contrato
    «ativo»; o SQLite pode ter outro vencimento (GB de dados mas zero linhas para o
    símbolo resolvido). Aqui priorizamos candidatos e, em último caso, qualquer ticker
    salvo com o mesmo prefixo no banco local; só então caímos na resolução via API.
    """
    t = (raw_ticker or '').strip().upper()
    base = _base_ticker_from_input(t)
    if base is None:
        return t
    from trader.models import QuoteSnapshot

    candidates = _get_candidates_list_by_base(request, base)[:_MAX_CANDIDATES_TO_TEST]
    for c in candidates:
        if QuoteSnapshot.objects.filter(ticker__iexact=c).exists():
            return c
    any_ticker = (
        QuoteSnapshot.objects.filter(ticker__istartswith=base)
        .order_by('-captured_at')
        .values_list('ticker', flat=True)
        .first()
    )
    if any_ticker:
        return str(any_ticker).strip().upper()
    return resolve_daytrade_base_ticker(request, t)


def _parse_order_test_setup(raw: str | None) -> str:
    """Valor vindo do POST; vazio costumava ocorrer com <select disabled> (não enviado)."""
    v = (raw or '').strip().lower()
    if not v:
        v = ORDER_TEST_SETUP_OPEN_LIMITED
    if v in ORDER_TEST_SETUP_CHOICES:
        return v
    return ORDER_TEST_SETUP_OPEN_LIMITED


def quote_status_is_end_of_day(quote: Any) -> bool:
    """
    True se a cotação indica fim do pregão (não vale manter polling agressivo).
    Aceita variações como ``EndOfDay``, ``endofday``, ``end_of_day``.
    """
    if not quote or not isinstance(quote, dict):
        return False
    s = quote.get('status')
    if s is None:
        s = quote.get('Status')
    if s is None:
        return False
    norm = str(s).strip().lower().replace('_', '')
    return norm == 'endofday'


def json_sanitize(obj: Any) -> Any:
    """Converte estruturas da API em tipos seguros para JsonResponse."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_sanitize(x) for x in obj]
    return str(obj)


def _order_dict_id(item: dict) -> str | None:
    """Identificador da ordem para POST /v1/orders/cancel (query ``Id``)."""
    for key in ('Id', 'id', 'OrderId', 'orderId', 'ID'):
        v = item.get(key)
        if v is not None and str(v).strip() != '':
            return str(v).strip()
    return None


# ISO com fração > 6 dígitos (ex.: API .NET) quebra ``fromisoformat`` no Python 3.9.
_ISO_FRAC_TRIM = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)([Zz]|[+-]\d{2}:\d{2}(?::\d{2})?)$'
)


def _normalize_iso_timestamp(raw: str) -> str:
    s = raw.strip()
    m = _ISO_FRAC_TRIM.match(s)
    if not m or not m.group(2):
        return s.replace('Z', '+00:00').replace('z', '+00:00')
    frac = m.group(2)  # includes leading dot
    if len(frac) <= 7:
        return s.replace('Z', '+00:00').replace('z', '+00:00')
    return m.group(1) + frac[:7] + m.group(3)


def _parse_iso_datetime(raw: str) -> datetime | None:
    """Parse ISO 8601 tolerante a frações longas e sufixo Z."""
    if not raw or not isinstance(raw, str):
        return None
    s = _normalize_iso_timestamp(raw.strip())
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _order_sort_ts(item: dict) -> float:
    """Timestamp para ordenar (mais recente = maior)."""
    for key in (
        'received',
        'Received',
        'creationTime',
        'CreationTime',
        'createdAt',
        'CreatedAt',
        'updatedAt',
        'UpdatedAt',
    ):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            dt = _parse_iso_datetime(v.strip())
            if dt is not None:
                return dt.timestamp()
    return 0.0


def _order_status_raw(item: dict) -> str | None:
    """
    Valor de status usado para UI (cancelar ou não).

    Prioriza ``orderStatus`` / ``OrderStatus`` (Smart Trader) antes de ``status``
    genérico, para não confundir com outro campo ``status`` no mesmo objeto.
    """
    for k in (
        'orderStatus',
        'OrderStatus',
        'status',
        'Status',
        'state',
        'State',
        'orderState',
        'OrderState',
    ):
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _order_is_cancellable(item: dict) -> bool:
    """False para ordens já encerradas ou com cancelamento em andamento."""
    raw = _order_status_raw(item)
    if raw is None:
        return True
    norm = raw.lower().replace(' ', '').replace('_', '')
    terminal = {
        'filled',
        'canceled',
        'cancelled',
        'rejected',
        'expired',
        'done',
        'complete',
        'completed',
        'inactive',
        # já em processo de cancelamento — não oferecer de novo
        'pendingcancel',
        'pendingcancellation',
        'waittocancel',
        'cancelling',
        'canceling',
    }
    return norm not in terminal


def _status_column_index(keys: list[str]) -> int | None:
    """Índice da coluna de status da ordem (para ícone + legenda)."""
    priority_map = {
        'orderstatus': 0,
        'status': 1,
        'orderstate': 2,
        'state': 3,
    }
    best: tuple[int, int] | None = None
    for i, k in enumerate(keys):
        nk = (k or '').strip().lower().replace('_', '')
        if nk in priority_map:
            p = priority_map[nk]
            if best is None or p < best[0]:
                best = (p, i)
    if best is not None:
        return best[1]
    for i, k in enumerate(keys):
        if 'status' in (k or '').lower():
            return i
    return None


def _order_status_badge_parts(raw: str | None) -> tuple[str, str, str]:
    """(símbolo, classe CSS, título) para a coluna de status."""
    if raw is None or not str(raw).strip():
        return ('?', 'ord-st-unknown', 'Status não informado')
    n = str(raw).strip().lower().replace(' ', '').replace('_', '')
    if n in ('filled', 'complete', 'completed', 'done'):
        return ('✓', 'ord-st-filled', 'Ordem executada')
    if n in ('canceled', 'cancelled'):
        return ('⊗', 'ord-st-canceled', 'Ordem cancelada')
    if n == 'rejected':
        return ('⊘', 'ord-st-rejected', 'Ordem rejeitada')
    if n == 'expired':
        return ('⌛', 'ord-st-expired', 'Ordem expirada')
    if n in (
        'pendingcancel',
        'pendingcancellation',
        'waittocancel',
        'cancelling',
        'canceling',
    ):
        return ('⏳', 'ord-st-pendingcx', 'Cancelamento em andamento')
    if n == 'inactive':
        return ('·', 'ord-st-inactive', 'Inativa')
    if n in (
        'new',
        'working',
        'pending',
        'accepted',
        'open',
        'sent',
        'queued',
        'partiallyfilled',
        'partialfilled',
    ):
        return ('○', 'ord-st-open', 'Em aberto (não finalizada)')
    label = str(raw).strip()
    if len(label) > 64:
        label = label[:61] + '…'
    return ('◆', 'ord-st-other', label)


def _wrap_order_status_cell(cells: list, idx: int, item: dict) -> None:
    raw = _order_status_raw(item)
    icon, css, title = _order_status_badge_parts(raw)
    val = cells[idx]
    if val is None:
        inner: str | SafeString = '—'
    elif isinstance(val, SafeString):
        inner = val
    else:
        inner = escape(str(val))
    cells[idx] = format_html(
        '<span class="order-status-badge {}" title="{}">'
        '<span class="order-status-ico" aria-hidden="true">{}</span> '
        '<span class="order-status-txt">{}</span></span>',
        css,
        title,
        icon,
        inner,
    )


# Cabeçalhos da tabela de ordens (chave da API normalizada → rótulo em português).
_ORDER_COLUMN_HEADINGS_PT: dict[str, str] = {
    'id': 'ID',
    'orderid': 'ID da ordem',
    'message': 'Mensagem',
    'module': 'Módulo',
    'type': 'Tipo',
    'ordertype': 'Tipo de ordem',
    'ticker': 'Ativo',
    'side': 'Lado',
    'quantity': 'Quantidade',
    'price': 'Preço',
    'limitprice': 'Preço limite',
    'timeinforce': 'Validade (TIF)',
    'stop': 'Stop',
    'stoptriggerprice': 'Disparo stop',
    'stoporderprice': 'Preço stop',
    'averageprice': 'Preço médio',
    'averagecost': 'Preço médio compra',
    'avgprice': 'Preço médio',
    'openquantity': 'Qtd. em aberto',
    'executedquantity': 'Qtd. executada',
    'filledquantity': 'Qtd. executada',
    'received': 'Recebida em',
    'status': 'Status',
    'orderstatus': 'Status',
    'state': 'Estado',
    'orderstate': 'Estado',
    'creationtime': 'Criada em',
    'createdat': 'Criada em',
    'updatedat': 'Atualizada em',
    'broker': 'Corretora',
    'account': 'Conta',
    'clientorderid': 'ID cliente',
    'externalid': 'ID externo',
    # Garantias / custódia e campos comuns REST
    'availablecollateral': 'Garantia disponível',
    'usedcollateral': 'Garantia utilizada',
    'availablequantity': 'Qtd. disponível',
    'blockedquantity': 'Qtd. bloqueada',
    'totalquantity': 'Qtd. total',
    'positionquantity': 'Qtd. posição',
    'marketvalue': 'Valor de mercado',
    'costprice': 'Preço de custo',
    # Enriquecimento custódia (cotação + resultado)
    'markprice': 'Preço atual',
    'pnlbrl': 'Resultado (R$)',
    'pnlpct': 'Resultado (%)',
    'sessionstatus': 'Pregão (cotação)',
}


def _order_status_label_pt(raw: Any) -> str:
    """Legenda em português para valor bruto de status (ordens)."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return '—'
    n = str(raw).strip().lower().replace(' ', '').replace('_', '')
    mapping = {
        'filled': 'Executada',
        'complete': 'Concluída',
        'completed': 'Concluída',
        'done': 'Concluída',
        'canceled': 'Cancelada',
        'cancelled': 'Cancelada',
        'rejected': 'Rejeitada',
        'expired': 'Expirada',
        'pendingcancel': 'Cancelando',
        'pendingcancellation': 'Cancelando',
        'waittocancel': 'Aguard. cancel.',
        'cancelling': 'Cancelando',
        'canceling': 'Cancelando',
        'inactive': 'Inativa',
        'new': 'Nova',
        'working': 'Em negociação',
        'pending': 'Pendente',
        'accepted': 'Aceita',
        'open': 'Aberta',
        'sent': 'Enviada',
        'queued': 'Na fila',
        'partiallyfilled': 'Parcial',
        'partialfilled': 'Parcial',
    }
    if n in mapping:
        return mapping[n]
    s = str(raw).strip()
    return s if len(s) <= 48 else s[:45] + '…'


def _fallback_column_heading(raw_key: str) -> str:
    """Último recurso: separa camelCase e capitaliza (pode ficar em inglês)."""
    s = (raw_key or '').strip()
    if not s:
        return '—'
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    s = s.replace('_', ' ')
    return s.strip().title()


def order_column_heading_pt(key: str) -> str:
    """Rótulo de coluna para exibição (PT-BR)."""
    nk = (key or '').strip().lower().replace('_', '')
    if nk in _ORDER_COLUMN_HEADINGS_PT:
        return _ORDER_COLUMN_HEADINGS_PT[nk]
    return _fallback_column_heading(key)


def api_field_heading_pt(key: str) -> str:
    """Rótulo PT-BR para colunas de tabelas genéricas (garantias, custódia, etc.)."""
    return order_column_heading_pt(key)


def _format_order_cell(key: str, value: Any) -> Any:
    if value is None:
        return value
    key_norm = (key or '').strip().lower().replace('_', '')
    if key_norm in (
        'orderstatus',
        'status',
        'orderstate',
        'state',
    ):
        return _order_status_label_pt(value)
    key_norm = (key or '').strip().lower()
    if key_norm == 'side':
        s = str(value).strip().lower()
        if s == 'buy':
            return 'Compra'
        if s == 'sell':
            return 'Venda'
    if key_norm in ('timeinforce', 'tif'):
        s = str(value).strip().lower().replace(' ', '')
        if s == 'day':
            return 'Dia'
        if s in ('fillorkill', 'fok'):
            return 'Tudo ou nada (FOK)'
    if 'date' not in key_norm and 'time' not in key_norm and key_norm not in ('received',):
        return value
    if not isinstance(value, str):
        return value
    raw = value.strip()
    if not raw:
        return value
    dt = _parse_iso_datetime(raw)
    if dt is None:
        return value
    try:
        return dt.astimezone(_DISPLAY_TZ_BRT).strftime('%d/%m/%Y %H:%M')
    except Exception:
        return value


def normalize_book_levels(levels: list) -> list[dict]:
    out: list[dict] = []
    for raw in levels:
        if not isinstance(raw, dict):
            continue
        out.append(
            {
                'position': raw.get('position', raw.get('Position')),
                'price': raw.get('price', raw.get('Price')),
                'quantity': raw.get('quantity', raw.get('Quantity')),
                'broker': raw.get('broker', raw.get('Broker')),
            }
        )
    return out


def build_market_context(ticker: str, *, use_cache: bool = True) -> dict[str, Any]:
    """Detalhes, quote, book, aggregate, gráfico OHLC e limites para um ticker."""
    sym = ticker.strip().upper()
    details = None
    quote = None
    book = None
    aggregate_book = None
    errors: dict[str, str] = {}

    for name, fetcher in (
        ('details', lambda: fetch_ticker_details(sym, use_cache=use_cache)),
        ('quote', lambda: fetch_quote(sym, use_cache=use_cache)),
        ('book', lambda: fetch_book(sym, use_cache=use_cache)),
        ('aggregate_book', lambda: fetch_aggregate_book(sym, use_cache=use_cache)),
    ):
        try:
            result = fetcher()
            if name == 'details':
                details = result
            elif name == 'quote':
                quote = result
            elif name == 'book':
                book = result
            else:
                aggregate_book = result
        except ValueError as exc:
            errors[name] = str(exc)
        except Exception:
            logger.exception('Erro market %s ticker=%s', name, sym)
            errors[name] = 'Erro inesperado.'

    agg_bids: list = []
    agg_asks: list = []
    if aggregate_book:
        raw_b = aggregate_book.get('bids') or aggregate_book.get('Bids') or []
        raw_a = aggregate_book.get('asks') or aggregate_book.get('Asks') or []
        agg_bids = normalize_book_levels(raw_b)
        agg_asks = normalize_book_levels(raw_a)

    chart_payload = ohlc_bar_chart_payload(quote)
    bmf_base = extract_bmf_base(sym)
    operation_hints = {
        'mercado': 'BMF' if bmf_base else 'BOVESPA',
        'base_bmf': bmf_base or '—',
        'limite_ordens_dia': daily_order_limit_for_ticker(sym),
        'limite_boleta': ticket_limit_for_ticker(sym),
    }

    return {
        'ticker': sym,
        'details': details,
        'quote': quote,
        'book': book,
        'aggregate_book': aggregate_book,
        'agg_bids': agg_bids,
        'agg_asks': agg_asks,
        'errors': errors,
        'chart_payload': chart_payload,
        'operation_hints': operation_hints,
    }


def build_market_context_local(ticker: str) -> dict[str, Any]:
    """
    Contexto de mercado usando somente dados locais já salvos em QuoteSnapshot.
    Não faz chamadas HTTP para marketdata (evita 429 no painel).
    """
    sym = ticker.strip().upper()
    t0 = time.perf_counter()
    quote = None
    book = None
    agg_bids: list[dict] = []
    agg_asks: list[dict] = []
    read_local_book = str(os.environ.get('TRADER_LOCAL_BOOK_READ_ENABLED', '1')).strip().lower() in (
        '1',
        'true',
        'yes',
    )
    logger.debug('env_diag:market_local:start ticker=%s', sym)
    try:
        from trader.models import QuoteSnapshot, BookSnapshot

        tq0 = time.perf_counter()
        row = (
            QuoteSnapshot.objects.filter(ticker__iexact=sym)
            .order_by('-captured_at')
            .values('quote_data')
            .first()
        )
        logger.debug(
            'env_diag:market_local:quote_done ticker=%s elapsed_ms=%.1f',
            sym,
            (time.perf_counter() - tq0) * 1000.0,
        )
        q = (row or {}).get('quote_data')
        if isinstance(q, dict):
            quote = q

        if read_local_book:
            tb0 = time.perf_counter()
            book_row = (
                BookSnapshot.objects.filter(ticker__iexact=sym)
                .order_by('-captured_at')
                .values('book_data')
                .first()
            )
            logger.debug(
                'env_diag:market_local:book_done ticker=%s elapsed_ms=%.1f',
                sym,
                (time.perf_counter() - tb0) * 1000.0,
            )
            b = (book_row or {}).get('book_data')
            if isinstance(b, dict):
                raw_bids = b.get('bids') or b.get('Bids') or []
                raw_asks = b.get('asks') or b.get('Asks') or []
                agg_bids = normalize_book_levels(raw_bids)
                agg_asks = normalize_book_levels(raw_asks)
                # Normaliza para o template/render JS (esperam bids/asks em minúsculas).
                book = {'bids': agg_bids, 'asks': agg_asks}
        else:
            logger.debug('env_diag:market_local:book_skipped ticker=%s reason=disabled_by_env', sym)
    except Exception:
        logger.exception('Erro ao ler snapshot local ticker=%s', sym)
    logger.debug(
        'env_diag:market_local:ready ticker=%s elapsed_ms=%.1f has_quote=%s has_book=%s',
        sym,
        (time.perf_counter() - t0) * 1000.0,
        bool(quote),
        bool(book),
    )

    bmf_base = extract_bmf_base(sym)
    operation_hints = {
        'mercado': 'BMF' if bmf_base else 'BOVESPA',
        'base_bmf': bmf_base or '—',
        'limite_ordens_dia': daily_order_limit_for_ticker(sym),
        'limite_boleta': ticket_limit_for_ticker(sym),
    }
    return {
        'ticker': sym,
        'details': None,
        'quote': quote,
        'book': book,
        'aggregate_book': {'source': 'book_snapshot_local'} if (agg_bids or agg_asks) else None,
        'agg_bids': agg_bids,
        'agg_asks': agg_asks,
        'errors': {},
        'chart_payload': ohlc_bar_chart_payload(quote),
        'operation_hints': operation_hints,
    }


def build_market_context_local_for_session_day(ticker: str, session_day: date) -> dict[str, Any]:
    """
    Último quote/book de ``QuoteSnapshot``/``BookSnapshot`` no dia de pregão BRT indicado.
    Usado na simulação de mercado (modo teste) para reproduzir um dia histórico como referência «atual».
    """
    sym = ticker.strip().upper()
    sp_tz = ZoneInfo('America/Sao_Paulo')
    day_start = datetime.combine(session_day, time_cls.min, tzinfo=sp_tz)
    day_end = day_start + timedelta(days=1)
    quote = None
    book = None
    agg_bids: list[dict] = []
    agg_asks: list[dict] = []
    read_local_book = str(os.environ.get('TRADER_LOCAL_BOOK_READ_ENABLED', '1')).strip().lower() in (
        '1',
        'true',
        'yes',
    )
    errors: dict[str, str] = {}
    try:
        from trader.models import BookSnapshot, QuoteSnapshot

        row = (
            QuoteSnapshot.objects.filter(
                ticker__iexact=sym,
                captured_at__gte=day_start,
                captured_at__lt=day_end,
            )
            .order_by('-captured_at')
            .values('quote_data')
            .first()
        )
        q = (row or {}).get('quote_data')
        if isinstance(q, dict):
            quote = q
        else:
            errors['quote'] = 'Sem snapshots de cotação neste dia para este ticker.'

        if read_local_book:
            book_row = (
                BookSnapshot.objects.filter(
                    ticker__iexact=sym,
                    captured_at__gte=day_start,
                    captured_at__lt=day_end,
                )
                .order_by('-captured_at')
                .values('book_data')
                .first()
            )
            b = (book_row or {}).get('book_data')
            if isinstance(b, dict):
                raw_bids = b.get('bids') or b.get('Bids') or []
                raw_asks = b.get('asks') or b.get('Asks') or []
                agg_bids = normalize_book_levels(raw_bids)
                agg_asks = normalize_book_levels(raw_asks)
                book = {'bids': agg_bids, 'asks': agg_asks}
    except Exception:
        logger.exception('Erro ao ler snapshot por dia ticker=%s day=%s', sym, session_day)
        errors['quote'] = 'Erro ao ler snapshots locais.'

    bmf_base = extract_bmf_base(sym)
    operation_hints = {
        'mercado': 'BMF' if bmf_base else 'BOVESPA',
        'base_bmf': bmf_base or '—',
        'limite_ordens_dia': daily_order_limit_for_ticker(sym),
        'limite_boleta': ticket_limit_for_ticker(sym),
    }
    return {
        'ticker': sym,
        'details': None,
        'quote': quote,
        'book': book,
        'aggregate_book': {'source': 'book_snapshot_local_session_day'} if (agg_bids or agg_asks) else None,
        'agg_bids': agg_bids,
        'agg_asks': agg_asks,
        'errors': errors,
        'chart_payload': ohlc_bar_chart_payload(quote),
        'operation_hints': operation_hints,
    }


def build_orders_context(*, orders_limit: int | None = None) -> dict[str, Any]:
    """Ordens intraday processadas para tabela ou JSON.

    ``orders_limit`` — na home, tipicamente 5 (mais recentes primeiro); ``None`` = todas.
    """
    orders_payload = None
    error: str | None = None
    try:
        orders_payload = fetch_orders_cached()
    except ValueError as exc:
        error = str(exc)
    except Exception:
        logger.exception('Erro ao buscar ordens intraday')
        error = 'Erro inesperado ao consultar a API.'

    orders_rows: list[dict] | None = None
    orders_raw_json = None
    order_column_keys: list[str] = []
    order_column_labels: list[str] = []
    order_table_rows: list[list[object]] = []
    order_cancel_ids: list[str | None] = []
    orders_table_display: list[tuple[list[object], str | None, bool]] = []

    preferred_order_cols = [
        'id',
        'ticker',
        'side',
        'quantity',
        'price',
        'averageprice',
        'orderstatus',
        'status',
        'received',
    ]

    def _pick_order_cols(row: dict[str, Any]) -> list[str]:
        keys = list(row.keys())
        chosen: list[str] = []
        norm_map = {(k or '').strip().lower().replace('_', ''): k for k in keys}
        for pref in preferred_order_cols:
            mk = norm_map.get(pref)
            if mk and mk not in chosen:
                chosen.append(mk)
        if not chosen:
            chosen = keys[:8]
        return chosen[:8]

    if orders_payload is not None:
        if isinstance(orders_payload, list):
            row_dicts = [x for x in orders_payload if isinstance(x, dict)]
            row_dicts.sort(key=_order_sort_ts, reverse=True)
            if orders_limit is not None and orders_limit > 0:
                row_dicts = row_dicts[:orders_limit]
            orders_rows = row_dicts
            if row_dicts:
                order_column_keys = _pick_order_cols(row_dicts[0])
                order_column_labels = [order_column_heading_pt(k) for k in order_column_keys]
                status_col_idx = _status_column_index(order_column_keys)
                for item in row_dicts:
                    cells = [_format_order_cell(k, item.get(k)) for k in order_column_keys]
                    if status_col_idx is not None:
                        _wrap_order_status_cell(cells, status_col_idx, item)
                    order_table_rows.append(cells)
                    oid = _order_dict_id(item)
                    order_cancel_ids.append(oid)
                    can_cancel = bool(oid) and _order_is_cancellable(item)
                    orders_table_display.append((cells, oid, can_cancel))
        else:
            orders_raw_json = json.dumps(
                orders_payload, indent=2, ensure_ascii=False
            )
    if (
        orders_payload is not None
        and isinstance(orders_payload, list)
        and orders_payload
        and not order_table_rows
        and not orders_raw_json
    ):
        orders_raw_json = json.dumps(
            orders_payload, indent=2, ensure_ascii=False
        )

    floating = _orders_open_floating_summary(orders_rows)

    return {
        'orders_rows': orders_rows,
        'order_column_keys': order_column_keys,
        'order_column_labels': order_column_labels,
        'order_table_rows': order_table_rows,
        'order_cancel_ids': order_cancel_ids,
        'orders_table_display': orders_table_display,
        'orders_raw_json': orders_raw_json,
        'orders_error': error,
        **floating,
    }


def order_test_form_defaults(*, request=None) -> dict[str, Any]:
    ticker = (
        get_daytrade_primary_ticker_from_session(request)
        if request is not None
        else default_primary_ticker()
    )
    is_real = request is not None and get_session_environment(request) == ENV_REAL
    return {
        'setup': ORDER_TEST_SETUP_OPEN_LIMITED,
        'no_setup': False,
        # Real: só comandos de ordem (mercado/limitada/stop); simulador mantém limitada + preço fora do book.
        'order_type': 'market' if is_real else 'limited',
        'ticker': ticker,
        'side': 'Buy',
        'quantity': 1,
        'tif': 'Day',
        'price': '' if is_real else '1',
        'stop_trigger': '',
        'stop_order': '',
    }


def run_order_test_form(request) -> tuple[dict[str, Any], str | None, str | None]:
    """
    Processa GET/POST do formulário de envio de ordem de teste.
    Retorna (defaults, result_json formatado ou None, erro ou None).
    """
    from trader.services.orders import (
        post_send_limited_order,
        post_send_market_order,
        post_send_stop_limit_order,
        post_simulator_setup_orders,
    )

    defaults = order_test_form_defaults(request=request)
    result_json = None
    error = None

    if request.method != 'POST':
        return defaults, None, None

    is_real = get_session_environment(request) == ENV_REAL
    setup = _parse_order_test_setup(request.POST.get('setup'))
    no_setup = request.POST.get('no_setup') == 'on'
    order_type = (request.POST.get('order_type') or 'market').strip()
    if order_type not in ORDER_TYPES_INTERNAL:
        order_type = ORDER_TYPE_MARKET_INTERNAL
    if not is_real:
        if setup == ORDER_TEST_SETUP_OPEN_LIMITED:
            order_type = 'limited'
        elif setup in SIMULATOR_SETUP_ORDER_STATUSES:
            # Com filled/rejected o simulador espera envio que execute (ou rejeite) de imediato.
            # Boleta padrão é limitada + preço fora do mercado → lista fica "new"/working.
            order_type = 'market'
    ticker = (
        request.POST.get('order_ticker') or request.POST.get('ticker') or default_primary_ticker()
    ).strip().upper()
    ticker = resolve_daytrade_base_ticker(request, ticker)
    side = request.POST.get('side') or 'Buy'
    if side not in ORDER_SIDES:
        side = 'Buy'
    try:
        quantity = max(1, int(request.POST.get('quantity') or '1'))
    except ValueError:
        quantity = 1
    tif = request.POST.get('tif') or ORDER_TIF_DAY
    if tif not in ORDER_TIME_IN_FORCE_VALUES:
        tif = ORDER_TIF_DAY

    price_raw = (request.POST.get('price') or '').strip()
    st_raw = (request.POST.get('stop_trigger') or '').strip()
    so_raw = (request.POST.get('stop_order') or '').strip()

    if (
        not is_real
        and setup == ORDER_TEST_SETUP_OPEN_LIMITED
        and not price_raw
    ):
        price_raw = '1' if side == 'Buy' else '999999999'

    defaults.update(
        {
            'setup': setup,
            'no_setup': no_setup,
            'order_type': order_type,
            'ticker': ticker,
            'side': side,
            'quantity': quantity,
            'tif': tif,
            'price': price_raw,
            'stop_trigger': st_raw,
            'stop_order': so_raw,
        }
    )

    module_name = 'DayTrade'
    try:
        if order_type == 'limited' and not price_raw:
            raise ValueError('Informe o preço para ordem limitada.')
        if order_type == 'stop-limit' and (not st_raw or not so_raw):
            raise ValueError(
                'Informe stop de disparo e stop da ordem para stop-limit.'
            )

        body_market = {
            'Module': ORDER_MODULE_DAY_TRADE,
            'Ticker': ticker,
            'Side': side,
            'Quantity': quantity,
            'TimeInForce': tif,
        }
        module_name = str(body_market.get('Module') or '').strip() or ORDER_MODULE_DAY_TRADE
        cached_block_reason = _get_cached_ticker_not_allowed(module_name, ticker)
        if cached_block_reason:
            raise ValueError(
                f'Ticker {ticker} bloqueado temporariamente para módulo {module_name}: '
                f'{cached_block_reason}'
            )

        explicit_sim_setup = (
            not is_real
            and setup in SIMULATOR_SETUP_ORDER_STATUSES
            and not no_setup
        )
        if explicit_sim_setup:
            post_simulator_setup_orders(setup)

        if order_type == 'market':
            resp = post_send_market_order(
                body_market,
                skip_simulator_auto_filled=explicit_sim_setup,
            )
            marker_price = None
            body_for_infer = body_market
        elif order_type == 'limited':
            body = {**body_market, 'Price': float(price_raw.replace(',', '.'))}
            resp = post_send_limited_order(body)
            marker_price = body.get('Price')
            body_for_infer = body
        else:
            body = {
                **body_market,
                'StopTriggerPrice': float(st_raw.replace(',', '.')),
                'StopOrderPrice': float(so_raw.replace(',', '.')),
            }
            resp = post_send_stop_limit_order(body)
            marker_price = body.get('StopOrderPrice')
            body_for_infer = body

        _clear_cached_ticker_not_allowed(module_name, ticker)
        hist_price = infer_execution_price(body_for_infer, resp)
        if should_record_local_history(order_type, resp):
            try:
                register_trade_execution(
                    ticker=ticker,
                    side=side,
                    quantity=quantity,
                    price=hist_price,
                    source='order_test_form',
                    trading_environment=get_current_environment(),
                )
            except Exception:
                logger.exception('register_trade_execution order_test_form')
        chart_price = hist_price if hist_price is not None else marker_price
        record_trade_marker(
            ticker=ticker,
            side=side,
            quantity=quantity,
            price=chart_price,
            source='order_test_form',
            metadata={
                'order_type': order_type,
                'module': module_name,
                'custody_channel': 'live',
                'data_source': 'api_boleta',
            },
        )
        result_json = json.dumps(resp, indent=2, ensure_ascii=False)
        try:
            invalidate_intraday_orders_cache()
        except Exception:
            pass
    except ValueError as exc:
        error = str(exc)
        if 'TICKER_NOT_ALLOWED' in error and 'bloqueado temporariamente' not in error.lower():
            _cache_ticker_not_allowed(module_name, ticker, error)
    except SmartTraderConfigurationError:
        error = (
            'Chave RSA não configurada. O envio de ordem exige BODY_SIGNATURE: defina no .env '
            'SMART_TRADER_PRIVATE_RSA_PATH (caminho absoluto do arquivo .pem) ou '
            'SMART_TRADER_PRIVATE_RSA_PEM_B64 (PEM inteiro em Base64, uma linha). '
            'Reinicie o runserver após salvar o .env.'
        )
        logger.warning('Envio ordem teste: RSA não configurada (SmartTraderConfigurationError).')
    except SmartTraderSignatureError as exc:
        error = f'Falha ao gerar assinatura do corpo: {exc}'
        logger.warning('Envio ordem teste: assinatura — %s', exc)
    except Exception:
        logger.exception('run_order_test_form')
        error = 'Falha ao enviar ordem de teste.'

    return defaults, result_json, error


def _collateral_custody_cache_key() -> str:
    """Uma entrada por ambiente (real/simulator) para não misturar dados ao alternar o seletor."""
    return f'api:collateral_custody:v1:{get_current_environment()}'


def _collateral_custody_cooldown_key() -> str:
    return f'api:collateral_custody:cooldown_until:{get_current_environment()}'


def invalidate_collateral_custody_cache() -> None:
    """Limpa cache de garantias/custódia (após ordem/cancelamento ou refresh explícito)."""
    cache.delete(_collateral_custody_cache_key())
    cache.delete(_collateral_custody_cooldown_key())


def _replay_shadow_sim_replay_until(request: Any | None) -> datetime | None:
    """Instante do scrubber (prefs Celery) para alinhar cotação ao frame do replay fictício."""
    if request is None:
        return None
    user = getattr(request, 'user', None)
    if user is None or not getattr(user, 'is_authenticated', False):
        return None
    from trader.models import AutomationMarketSimPreference

    env = get_session_environment(request)
    pref = AutomationMarketSimPreference.objects.filter(
        user=user, trading_environment=env
    ).only('replay_until').first()
    if pref is None or pref.replay_until is None:
        return None
    return pref.replay_until


def _replay_shadow_mark_decimal(ticker: str, until: datetime | None) -> Decimal | None:
    """Último preço em ``QuoteSnapshot`` até ``until`` (ou último do ticker se ``until`` é None)."""
    from trader.custody_enrichment import mark_price_from_quote
    from trader.models import QuoteSnapshot

    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    qs = QuoteSnapshot.objects.filter(ticker__iexact=sym)
    if until is not None:
        qs = qs.filter(captured_at__lte=until)
    row = qs.order_by('-captured_at').values_list('quote_data', flat=True).first()
    if not isinstance(row, dict):
        return None
    m = mark_price_from_quote(row)
    if m is None:
        return None
    try:
        return Decimal(str(m))
    except InvalidOperation:
        return None


def _fmt_decimal_br(d: Decimal, quant: Decimal) -> str:
    s = format(d.quantize(quant, rounding=ROUND_HALF_UP), 'f').rstrip('0').rstrip('.')
    return s.replace('.', ',') if s else '0'


def _fmt_money_brl_simple(d: Decimal | None) -> str:
    if d is None:
        return '—'
    q = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    neg = q < 0
    a = abs(q)
    body = format(a, 'f').replace('.', ',')
    return ('−' if neg else '') + 'R$ ' + body


def _fmt_pct_br(d: Decimal | None) -> str:
    if d is None:
        return '—'
    q = d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    neg = q < 0
    a = abs(q)
    body = format(a, 'f').replace('.', ',') + '%'
    return ('−' if neg else '') + body


def _replay_pnl_css_class(val: Decimal | None) -> str:
    if val is None:
        return ''
    if val > 0:
        return 'replay-pnl-pos'
    if val < 0:
        return 'replay-pnl-neg'
    return ''


def _orders_open_floating_summary(orders_rows: list[dict[str, Any]] | None) -> dict[str, Any]:
    """
    Soma (mark − limite)×qtd em ordens ainda canceláveis com preço limite e cotação disponível.

    Compra: (mark − limit) × qtd ; Venda: (limit − mark) × qtd — desvio vs. executar ao mercado.
    """
    out: dict[str, Any] = {
        'orders_floating_show': False,
        'orders_floating_count': 0,
        'orders_floating_total_str': '',
        'orders_floating_pnl_class': '',
    }
    if not orders_rows:
        return out
    from trader.custody_enrichment import mark_price_from_quote
    from trader.services.marketdata import fetch_quote

    quotes_mem: dict[str, dict[str, Any]] = {}

    def _quote_dict(sym: str) -> dict[str, Any]:
        if sym in quotes_mem:
            return quotes_mem[sym]
        try:
            q = fetch_quote(sym, use_cache=True)
        except Exception:
            q = None
        d = q if isinstance(q, dict) else {}
        quotes_mem[sym] = d
        return d

    total = Decimal(0)
    n_used = 0
    for item in orders_rows:
        if not isinstance(item, dict) or not _order_is_cancellable(item):
            continue
        sym = str(_row_get_ci(item, 'ticker', 'Ticker', 'symbol', 'Symbol') or '').strip().upper()
        if not sym:
            continue
        qty = _parse_decimal_any(
            _row_get_ci(
                item,
                'openQuantity',
                'OpenQuantity',
                'remainingQuantity',
                'RemainingQuantity',
                'quantity',
                'Quantity',
            )
        )
        if qty is None or qty == 0:
            continue
        lim = _parse_decimal_any(
            _row_get_ci(item, 'price', 'Price', 'limitPrice', 'LimitPrice')
        )
        if lim is None or lim <= 0:
            continue
        side = str(_row_get_ci(item, 'side', 'Side') or '').strip().lower()
        mark = mark_price_from_quote(_quote_dict(sym))
        if mark is None:
            continue
        mark_d = Decimal(str(mark))
        if side == 'buy':
            edge = (mark_d - lim) * qty
        elif side == 'sell':
            edge = (lim - mark_d) * qty
        else:
            continue
        total += edge
        n_used += 1
    if n_used == 0:
        return out
    out['orders_floating_show'] = True
    out['orders_floating_count'] = n_used
    out['orders_floating_total_str'] = _fmt_money_brl_simple(total)
    out['orders_floating_pnl_class'] = _replay_pnl_css_class(total)
    return out


def _replay_shadow_custody_panel(request: Any | None = None) -> dict[str, Any]:
    """
    Ledger ``replay_shadow``: posições abertas com preço de mercado alinhado ao replay
    (``AutomationMarketSimPreference.replay_until``) e histórico de PnL de posições encerradas.
    """
    if get_current_environment() != ENV_SIMULATOR:
        return {
            'show': False,
            'active_rows': [],
            'history_rows': [],
            'empty_active': True,
            'empty_history': True,
            'active_unreal_total_show': False,
            'active_unreal_total_str': '—',
            'active_unreal_total_class': '',
            'history_net_total_all_str': '—',
            'history_net_total_all_class': '',
            'history_full_count': 0,
        }
    from trader.models import ClosedOperation, Position

    until = _replay_shadow_sim_replay_until(request)
    price_q = Decimal('0.000001')

    active_base_qs = Position.objects.filter(
        trading_environment=ENV_SIMULATOR,
        position_lane=Position.Lane.REPLAY_SHADOW,
        is_active=True,
    )
    active_unreal_sum = Decimal(0)
    active_unreal_n = 0
    for p in active_base_qs.iterator(chunk_size=64):
        mark = _replay_shadow_mark_decimal(p.ticker, until)
        if mark is None:
            continue
        avg = p.avg_open_price
        qty = p.quantity_open
        if p.side == Position.Side.LONG:
            unreal = (mark - avg) * qty
        else:
            unreal = (avg - mark) * qty
        active_unreal_sum += unreal
        active_unreal_n += 1

    active_qs = active_base_qs.order_by('-opened_at')[:32]
    active_rows: list[dict[str, Any]] = []
    for p in active_qs:
        mark = _replay_shadow_mark_decimal(p.ticker, until)
        avg = p.avg_open_price
        qty = p.quantity_open
        unreal: Decimal | None = None
        var_pct: Decimal | None = None
        if mark is not None:
            if p.side == Position.Side.LONG:
                unreal = (mark - avg) * qty
                if avg != 0:
                    var_pct = ((mark - avg) / avg) * Decimal('100')
            else:
                unreal = (avg - mark) * qty
                if avg != 0:
                    var_pct = ((avg - mark) / avg) * Decimal('100')
        active_rows.append(
            {
                'ticker': p.ticker,
                'side': p.get_side_display(),
                'quantity_open_str': _fmt_decimal_br(qty, Decimal('0.000001')),
                'ref_price_str': _fmt_decimal_br(avg, price_q),
                'mark_price_str': _fmt_decimal_br(mark, price_q) if mark is not None else '—',
                'var_pct_str': _fmt_pct_br(var_pct),
                'unreal_str': _fmt_money_brl_simple(unreal) if mark is not None else '—',
                'pnl_class': _replay_pnl_css_class(unreal if mark is not None else None),
                'opened_at': p.opened_at,
                'status_label': 'Aberta',
            }
        )

    hist_co_qs = ClosedOperation.objects.filter(
        position__trading_environment=ENV_SIMULATOR,
        position__position_lane=Position.Lane.REPLAY_SHADOW,
        position__is_active=False,
    )
    hist_agg = hist_co_qs.aggregate(s=Sum('net_pnl'))
    history_net_total_all = hist_agg['s'] if hist_agg['s'] is not None else Decimal(0)
    history_full_count = hist_co_qs.count()

    hist_qs = hist_co_qs.select_related('position').order_by('-closed_at')[:200]
    history_rows: list[dict[str, Any]] = []
    for co in hist_qs:
        p = co.position
        net = co.net_pnl
        history_rows.append(
            {
                'ticker': p.ticker,
                'side': p.get_side_display(),
                'closed_at': co.closed_at,
                'ref_price_str': _fmt_decimal_br(p.avg_open_price, price_q),
                'net_pnl_str': _fmt_money_brl_simple(net),
                'pnl_class': _replay_pnl_css_class(net),
            }
        )

    return {
        'show': True,
        'active_rows': active_rows,
        'history_rows': history_rows,
        'empty_active': len(active_rows) == 0,
        'empty_history': len(history_rows) == 0,
        'active_unreal_total_show': active_unreal_n > 0,
        'active_unreal_total_str': _fmt_money_brl_simple(active_unreal_sum)
        if active_unreal_n
        else '—',
        'active_unreal_total_class': _replay_pnl_css_class(active_unreal_sum)
        if active_unreal_n
        else '',
        'history_net_total_all_str': _fmt_money_brl_simple(history_net_total_all),
        'history_net_total_all_class': _replay_pnl_css_class(history_net_total_all),
        'history_full_count': history_full_count,
    }


def merge_replay_shadow_custody_panel(
    base: dict[str, Any], request: Any | None = None
) -> dict[str, Any]:
    """Anexa resumo replay ao contexto sem mutar dicts guardados em cache."""
    return {**base, 'replay_shadow_custody_panel': _replay_shadow_custody_panel(request)}


def collateral_custody_context_for_template_request(request: Any | None = None) -> dict[str, Any]:
    """
    Garantias/custódia para templates: usa cache compartilhado (~30s) quando existir.

    Por defeito (**DJANGO_DEFER_COLLATERAL_SSR**), em cache miss não bloqueia o HTML com
    GETs remotos — mostra «Carregando» e o ``panel_finance_strip_poll`` preenche via
    fragmento (evita esgotar workers no primeiro GET a /painel/).
    Defina ``DJANGO_DEFER_COLLATERAL_SSR=0`` para pré-carregar na SSR como antes.
    """
    hit = cache.get(_collateral_custody_cache_key())
    if hit is not None:
        return merge_replay_shadow_custody_panel(dict(hit), request)
    defer = os.environ.get('DJANGO_DEFER_COLLATERAL_SSR', 'true').strip().lower()
    if defer not in ('0', 'false', 'no'):
        return merge_replay_shadow_custody_panel(
            {
                'api_collateral_display': {'pending': True},
                'api_custody_display': {'pending': True},
            },
            request,
        )
    return merge_replay_shadow_custody_panel(build_collateral_custody_context(), request)


def _cell_str_for_table(v: Any) -> str:
    if v is None:
        return '—'
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _row_get_ci(row: dict[str, Any], *candidates: str) -> Any:
    if not row:
        return None
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for c in candidates:
        k = c.lower()
        if k in lower_map:
            return lower_map[k]
    return None


def _parse_decimal_any(raw: Any) -> Decimal | None:
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


def _build_custody_liquidation_actions(rows: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            out.append({'enabled': '0'})
            continue
        ticker_raw = _row_get_ci(row, *_CUSTODY_TICKER_KEYS)
        qty_raw = _row_get_ci(row, *_CUSTODY_QTY_KEYS)
        ticker = str(ticker_raw or '').strip().upper()
        qty = _parse_decimal_any(qty_raw)
        if (not ticker) or (qty is None) or (qty == 0):
            out.append({'enabled': '0'})
            continue
        out.append(
            {
                'enabled': '1',
                'ticker': ticker,
                'quantity': str(qty),
                'side': 'Sell' if qty > 0 else 'Buy',
            }
        )
    return out


def tabular_from_api_payload(payload: Any) -> dict[str, Any]:
    """Normaliza JSON da API para tabela (keys/rows), vazio ou bloco texto."""
    if payload is None:
        return {'empty': True}
    if isinstance(payload, list):
        if len(payload) == 0:
            return {'empty': True}
        if all(isinstance(x, dict) for x in payload):
            keys: list[str] = []
            for row in payload:
                for k in row.keys():
                    if k not in keys:
                        keys.append(k)
            keys = keys[:32]
            key_labels = [api_field_heading_pt(k) for k in keys]
            rows = [
                [_cell_str_for_table(row.get(k)) for k in keys]
                for row in payload
                if isinstance(row, dict)
            ]
            return {'keys': keys, 'key_labels': key_labels, 'rows': rows}
        rows = [[str(i + 1), _cell_str_for_table(x)] for i, x in enumerate(payload)]
        return {'keys': ['#', 'valor'], 'rows': rows}
    if isinstance(payload, dict):
        for alt in ('items', 'Items', 'data', 'Data', 'results', 'Results', 'collateral', 'Custody'):
            inner = payload.get(alt)
            if isinstance(inner, list):
                return tabular_from_api_payload(inner)
        return {
            'pre': json.dumps(payload, indent=2, ensure_ascii=False),
        }
    return {'pre': str(payload)}


def build_collateral_custody_context() -> dict[str, Any]:
    """GET /v1/collateral e /v1/custody com cache em memória (~30s; limite doc: 10 req/5s na API)."""
    from trader.custody_enrichment import (
        apply_custody_enrichment_meta,
        prepare_custody_payload,
    )

    hit = cache.get(_collateral_custody_cache_key())
    if hit is not None:
        return hit
    cooldown_until = cache.get(_collateral_custody_cooldown_key())
    if isinstance(cooldown_until, (int, float)) and time.time() < float(cooldown_until):
        return {
            'api_collateral_display': {'pending': True},
            'api_custody_display': {'pending': True},
        }

    from trader.services.orders import fetch_custody, fetch_collateral

    had_remote_error = False
    try:
        collateral_display = tabular_from_api_payload(fetch_collateral())
    except ValueError as exc:
        had_remote_error = True
        collateral_display = {'error': str(exc)}
    except Exception:
        had_remote_error = True
        logger.exception('fetch_collateral')
        collateral_display = {'error': 'Erro inesperado ao obter garantias.'}

    try:
        raw_custody = fetch_custody()
        prepared, custody_meta = prepare_custody_payload(raw_custody)
        custody_actions = _build_custody_liquidation_actions(prepared)
        custody_display = tabular_from_api_payload(prepared)
        custody_display = apply_custody_enrichment_meta(custody_display, custody_meta)
        custody_display['liquidation_actions'] = custody_actions
        custody_display['liquidation_enabled_count'] = sum(
            1 for x in custody_actions if x.get('enabled') == '1'
        )
    except ValueError as exc:
        had_remote_error = True
        custody_display = {'error': str(exc)}
    except Exception:
        had_remote_error = True
        logger.exception('fetch_custody')
        custody_display = {'error': 'Erro inesperado ao obter custódia.'}

    def _compact_table(
        display: dict[str, Any],
        preferred_norm_keys: list[str],
        *,
        max_cols: int,
    ) -> dict[str, Any]:
        if not isinstance(display, dict):
            return display
        keys = display.get('keys')
        rows = display.get('rows')
        if not isinstance(keys, list) or not isinstance(rows, list) or not keys:
            return display
        norm_to_idx = {
            (str(k).strip().lower().replace('_', '')): idx
            for idx, k in enumerate(keys)
        }
        selected_idx: list[int] = []
        for pref in preferred_norm_keys:
            idx = norm_to_idx.get(pref)
            if idx is not None and idx not in selected_idx:
                selected_idx.append(idx)
        if not selected_idx:
            selected_idx = list(range(min(max_cols, len(keys))))
        selected_idx = selected_idx[:max_cols]

        compact_keys = [keys[i] for i in selected_idx]
        compact_rows = [
            [row[i] if i < len(row) else '—' for i in selected_idx]
            for row in rows
        ]
        display['keys'] = compact_keys
        display['rows'] = compact_rows
        display['key_labels'] = [api_field_heading_pt(k) for k in compact_keys]

        pnl_idx = display.get('pnl_column_index')
        if isinstance(pnl_idx, int):
            try:
                display['pnl_column_index'] = selected_idx.index(pnl_idx)
            except ValueError:
                display['pnl_column_index'] = None
        return display

    collateral_display = _compact_table(
        collateral_display,
        ['ticker', 'availablecollateral', 'usedcollateral'],
        max_cols=3,
    )
    custody_display = _compact_table(
        custody_display,
        [
            'ticker',
            'quantity',
            'totalquantity',
            'positionquantity',
            'availablequantity',
            'averageprice',
            'averagecost',
            'costprice',
            'markprice',
            'pnlbrl',
        ],
        max_cols=5,
    )

    out = {
        'api_collateral_display': collateral_display,
        'api_custody_display': custody_display,
    }
    if had_remote_error:
        cache.set(_collateral_custody_cooldown_key(), time.time() + 20, 20)
    # TTL maior: fragmento não invalida a cada GET; menos pressão na API e navegação mais fluida.
    cache.set(_collateral_custody_cache_key(), out, 30)
    return out
