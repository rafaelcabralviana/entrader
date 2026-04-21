"""
Motor único de automação no Celery: todas as estratégias activas recebem o mesmo
``ObservationContext`` (ao vivo ou replay de ``QuoteSnapshot``) e hooks opcionais.

Novas estratégias: registar ``evaluate`` + opcional ``register_celery_tick``; definir
``celery_scope`` em ``strategies.py`` (``once`` vs ``per_ticker``).
"""

from __future__ import annotations

import re
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone as dj_tz

from trader.automacoes.leafar_candles import load_session_day_candles, trim_candles_to_replay_until
from trader.automacoes.observer import run_strategy_observers
from trader.automacoes.prefs import is_strategy_enabled
from trader.automacoes.profiles import resolve_active_profile
from trader.automacoes.execution_guard import has_open_position_for_ticker
from trader.automacoes.runtime import runtime_enabled, runtime_enabled_map
from trader.automacoes.strategies import (
    AUTOMATION_STRATEGY_KEYS,
    is_passive_strategy,
    strategy_celery_scope,
)
from trader.automacoes.strategy_registry import get_celery_tick
from trader.automacoes.thoughts import record_automation_thought
from trader.environment import ENV_REAL, ENV_REPLAY, ENV_SIMULATOR, normalize_environment, set_current_environment
from trader.panel_context import quote_live_allows_automation_orders
from trader.models import (
    AutomationExecutionProfile,
    AutomationMarketSimPreference,
    AutomationStrategyToggle,
    AutomationThought,
    AutomationTriggerMarker,
)
from trader.trading_system.contracts.context import ObservationContext
from trader.trading_system.data.readers import (
    book_dict_from_row,
    latest_book_snapshot,
    latest_quote_snapshot,
    quote_dict_from_row,
)

logger = logging.getLogger(__name__)

User = get_user_model()
_TZ_BRT = ZoneInfo('America/Sao_Paulo')
_ALERT_REINFORCE_SEC = 30


def _normalized_alert_signature(msg: str) -> str:
    s = (msg or '').strip().lower()
    if not s:
        return ''
    # Neutraliza números para evitar spam por micro-variação de preço.
    s = re.sub(r'\d+[.,]?\d*', '<n>', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:240]


def _should_emit_strategy_thought(
    user,
    env: str,
    ctx: ObservationContext,
    strategy_key: str,
    msg: str,
) -> tuple[bool, bool]:
    """
    Retorna (emitir, reforco).

    - mesma assinatura recente: suprime;
    - mesma assinatura após janela: emite como reforço.
    """
    uid = int(getattr(user, 'id', 0) or 0)
    if not uid:
        return True, False
    envn = normalize_environment(env)
    sym = (ctx.ticker or '').strip().upper() or '—'
    sess = (ctx.session_date_iso or 'live').strip() or 'live'
    sk = (strategy_key or '').strip() or '—'
    if sk == 'teste_limite_preco_34':
        # Estratégia de limiar em modo evento: cada novo cruzamento deve gerar alerta.
        return True, False
    sig = _normalized_alert_signature(msg)
    if not sig:
        return True, False
    ck = f'automation:obs:lastsig:{uid}:{envn}:{sym}:{sess}:{sk}'
    now_ts = dj_tz.now().timestamp()
    prev = cache.get(ck)
    prev_sig = ''
    prev_ts = 0.0
    if isinstance(prev, dict):
        prev_sig = str(prev.get('sig') or '')
        try:
            prev_ts = float(prev.get('ts') or 0.0)
        except (TypeError, ValueError):
            prev_ts = 0.0
    reinforce_sec = _ALERT_REINFORCE_SEC
    if prev_sig == sig and (now_ts - prev_ts) < float(reinforce_sec):
        return False, False
    cache.set(ck, {'sig': sig, 'ts': now_ts}, timeout=24 * 3600)
    return True, prev_sig == sig


def _calendar_date_brt() -> date:
    """Dia civil em São Paulo (mesmo critério do gráfico / simulação)."""
    return dj_tz.now().astimezone(_TZ_BRT).date()


def _enabled_strategies_by_env_user() -> dict[str, dict[int, dict[str, Any]]]:
    """``env -> user_id -> {keys, profile}`` com estratégias activas do perfil activo."""
    rows = AutomationStrategyToggle.objects.filter(
        enabled=True,
        strategy_key__in=AUTOMATION_STRATEGY_KEYS,
        trading_environment__in=(ENV_SIMULATOR, ENV_REAL, ENV_REPLAY),
    ).filter(
        Q(execution_profile__isnull=True) | Q(execution_profile__is_active=True)
    ).select_related('execution_profile')
    out: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in rows.iterator(chunk_size=256):
        uid = int(getattr(row, 'user_id', 0) or 0)
        env = getattr(row, 'trading_environment', '')
        sk = getattr(row, 'strategy_key', '')
        e = normalize_environment(str(env))
        if not uid or not sk:
            continue
        slot = out[e].get(uid)
        if slot is None:
            slot = {'keys': [], 'profile': getattr(row, 'execution_profile', None)}
            out[e][uid] = slot
        if slot.get('profile') is None and getattr(row, 'execution_profile', None) is not None:
            slot['profile'] = getattr(row, 'execution_profile')
        if sk not in slot['keys']:
            slot['keys'].append(sk)
    merged: dict[str, dict[int, dict[str, Any]]] = {env: users for env, users in out.items()}
    # Replay possui estado próprio; se o utilizador não tiver toggles em replay,
    # mantém fallback ao simulador para retrocompatibilidade.
    sim_users = dict(merged.get(ENV_SIMULATOR) or {})
    replay_users = dict(merged.get(ENV_REPLAY) or {})
    replay_slots: dict[int, dict[str, Any]] = {}
    replay_uids = set(replay_users.keys()) | set(sim_users.keys())
    for uid in replay_uids:
        slot_replay = replay_users.get(uid) or {}
        keys_replay = list(slot_replay.get('keys') or [])
        if keys_replay:
            replay_slots[uid] = {
                'keys': keys_replay,
                'profile': slot_replay.get('profile'),
            }
            continue
        slot_sim = sim_users.get(uid) or {}
        keys_sim = list(slot_sim.get('keys') or [])
        if keys_sim:
            replay_slots[uid] = {'keys': keys_sim, 'profile': None}
    if replay_slots:
        merged[ENV_REPLAY] = replay_slots
    return merged


def _sim_prefs_map(user_ids: list[int], env: str) -> dict[int, AutomationMarketSimPreference]:
    env = normalize_environment(env)
    rows = AutomationMarketSimPreference.objects.filter(
        user_id__in=user_ids,
        trading_environment=env,
    )
    return {p.user_id: p for p in rows}


def _sim_pref_active(p: AutomationMarketSimPreference | None) -> bool:
    return bool(
        p
        and p.enabled
        and p.session_date is not None
        and (p.sim_ticker or '').strip()
    )


def _last_quote_snapshot_row_session(
    ticker: str,
    session_day: date,
    replay_until: datetime | None,
):
    from datetime import time as dtime
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    from trader.models import QuoteSnapshot

    tz = ZoneInfo('America/Sao_Paulo')
    day_start = datetime.combine(session_day, dtime.min, tzinfo=tz)
    day_end = day_start + timedelta(days=1)
    sym = (ticker or '').strip().upper()
    qs = QuoteSnapshot.objects.filter(
        ticker=sym,
        captured_at__gte=day_start,
        captured_at__lt=day_end,
    )
    if replay_until is not None:
        qs = qs.filter(captured_at__lte=replay_until)
    return (
        qs.order_by('-captured_at')
        .only('captured_at', 'quote_data', 'quote_event_at', 'latency_ms', 'ticker', 'id')
        .first()
    )


def _build_live_context(
    ticker: str,
    trading_environment: str,
    candles: list[dict[str, Any]],
    *,
    session_day: date | None = None,
) -> ObservationContext:
    sym = (ticker or '').strip().upper()
    qrow = latest_quote_snapshot(sym)
    brow = latest_book_snapshot(sym)
    q = quote_dict_from_row(qrow)
    b = book_dict_from_row(brow)
    cap = getattr(qrow, 'captured_at', None) if qrow is not None else None
    return ObservationContext(
        mode='live',
        ticker=sym,
        trading_environment=normalize_environment(trading_environment),
        captured_at=cap,
        quote=q,
        book=b,
        session_date_iso=session_day.isoformat() if session_day else None,
        replay_until_iso=None,
        market_sim_effective=False,
        data_source='live_tail',
        extra={
            'candles': candles,
            'candles_full_day': True,
            'session_day_iso': session_day.isoformat() if session_day else None,
        },
    )


def _build_session_replay_context(
    ticker: str,
    trading_environment: str,
    session_day: date,
    replay_until: datetime | None,
    candles: list[dict[str, Any]],
) -> ObservationContext:
    sym = (ticker or '').strip().upper()
    qrow = _last_quote_snapshot_row_session(sym, session_day, replay_until)
    q = quote_dict_from_row(qrow)
    b: dict[str, Any] = {}
    cap = getattr(qrow, 'captured_at', None) if qrow is not None else None
    rui = replay_until.isoformat() if replay_until else None
    return ObservationContext(
        mode='session_day',
        ticker=sym,
        trading_environment=normalize_environment(trading_environment),
        captured_at=cap,
        quote=q,
        book=b,
        session_date_iso=session_day.isoformat(),
        replay_until_iso=rui,
        market_sim_effective=True,
        data_source='session_replay',
        extra={
            'candles': candles,
            'candles_full_day': True,
            'session_day_iso': session_day.isoformat(),
        },
    )


def _interval_sec_from_settings() -> int:
    from django.conf import settings

    iv = getattr(settings, 'TRADER_LEAFAR_INTERVAL_SEC', 10)
    try:
        return max(1, min(int(iv), 300))
    except (TypeError, ValueError):
        return 10


def _record_strategy_observer_thoughts(
    user,
    env: str,
    ctx: ObservationContext,
    keys: list[str],
    *,
    execution_profile: AutomationExecutionProfile | None = None,
) -> None:
    """Apenas ``evaluate`` + pensamentos (sem ``celery_tick``)."""
    if not keys:
        return
    env = normalize_environment(env)
    msgs = run_strategy_observers(user, ctx, keys)
    for sk, msg in msgs:
        try:
            emit, reinforce = _should_emit_strategy_thought(user, env, ctx, sk, msg)
            if not emit:
                continue
            out_msg = f'{msg} [reforço]' if reinforce else msg
            if sk == 'teste_limite_preco_34':
                kind = AutomationThought.Kind.WARN
            elif is_passive_strategy(sk):
                kind = AutomationThought.Kind.NOTICE
            else:
                kind = AutomationThought.Kind.INFO
            record_automation_thought(
                user,
                env,
                out_msg,
                source=sk,
                kind=kind,
                execution_profile=execution_profile,
            )
            marker_price = None
            raw_c = ctx.extra.get('candles') if isinstance(ctx.extra, dict) else None
            if isinstance(raw_c, list) and raw_c:
                try:
                    marker_price = float(raw_c[-1].get('close'))
                except (TypeError, ValueError, AttributeError):
                    marker_price = None
            if marker_price is None:
                try:
                    marker_price = float(ctx.quote.get('lastPrice'))
                except (TypeError, ValueError, AttributeError):
                    marker_price = None
            AutomationTriggerMarker.objects.create(
                user=user,
                execution_profile=execution_profile,
                trading_environment=env,
                ticker=(ctx.ticker or '').strip().upper(),
                strategy_key=sk,
                marker_at=(ctx.captured_at or dj_tz.now()),
                price=marker_price,
                message=(out_msg or '')[:500],
            )
        except Exception:
            logger.exception('automation_engine thought %s', sk)


def _extract_market_trend_reversal_pct(message: str) -> int | None:
    m = re.search(r'Revers[aã]o \(estimada\):\s*(\d{1,3})%', str(message or ''), flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _extract_market_trend_label(message: str) -> str:
    m = re.search(r'Dire[cç][aã]o:\s*(Alta|Baixa|Lateralizado)', str(message or ''), flags=re.IGNORECASE)
    if not m:
        return ''
    raw = str(m.group(1) or '').strip().lower()
    if raw == 'alta':
        return 'Alta'
    if raw == 'baixa':
        return 'Baixa'
    if raw == 'lateralizado':
        return 'Lateralizado'
    return ''


def _extract_hvn_mountains(message: str) -> list[dict[str, float]]:
    """
    Extrai montanhas HVN do texto passivo:
    "Vol. Montanhas: (47.123 - 123456)  (47.456 - 98765) ..."
    """
    out: list[dict[str, float]] = []
    msg = str(message or '')
    for px_s, vol_s in re.findall(r'\(\s*([0-9]+(?:[.,][0-9]+)?)\s*-\s*([0-9]+(?:[.,][0-9]+)?)\s*\)', msg):
        try:
            px = float(str(px_s).replace(',', '.'))
            vol = float(str(vol_s).replace(',', '.'))
        except (TypeError, ValueError):
            continue
        if px <= 0 or vol <= 0:
            continue
        out.append({'price': px, 'volume': vol})
    out.sort(key=lambda it: float(it.get('volume') or 0.0), reverse=True)
    return out[:5]


def _passive_context_from_logs(
    user,
    env: str,
    ctx: ObservationContext,
    keys: list[str],
    *,
    execution_profile: AutomationExecutionProfile | None = None,
) -> dict[str, Any]:
    sym = (ctx.ticker or '').strip().upper()
    out: dict[str, Any] = {
        'ticker': sym,
        'sources_enabled': [],
        'market_trend': None,
        'hvn_mountains': [],
        'block_active_ticks': False,
        'block_reason': '',
    }
    passive_sources = [k for k in (keys or []) if is_passive_strategy(k)]
    out['sources_enabled'] = passive_sources
    if not sym or not passive_sources:
        return out
    if 'perfil_volume_montanhas' in passive_sources:
        since_hvn = dj_tz.now() - timedelta(minutes=8)
        q_hvn = AutomationThought.objects.filter(
            user=user,
            trading_environment=env,
            source='perfil_volume_montanhas',
            created_at__gte=since_hvn,
        ).order_by('-id')
        if execution_profile is not None:
            q_hvn = q_hvn.filter(Q(execution_profile=execution_profile) | Q(execution_profile__isnull=True))
        rows_hvn = list(q_hvn[:40])
        hvn_thought = None
        token_hvn = f'· {sym}]'.lower()
        for r in rows_hvn:
            msg_h = str(getattr(r, 'message', '') or '')
            if token_hvn in msg_h.lower():
                hvn_thought = r
                break
        if hvn_thought is not None:
            out['hvn_mountains'] = _extract_hvn_mountains(
                str(getattr(hvn_thought, 'message', '') or '')
            )

    if 'tendencia_mercado' not in passive_sources:
        return out

    since = dj_tz.now() - timedelta(minutes=8)
    q = AutomationThought.objects.filter(
        user=user,
        trading_environment=env,
        source='tendencia_mercado',
        created_at__gte=since,
    ).order_by('-id')
    if execution_profile is not None:
        q = q.filter(Q(execution_profile=execution_profile) | Q(execution_profile__isnull=True))
    rows = list(q[:40])
    thought = None
    token = f'· {sym}]'.lower()
    for r in rows:
        msg = str(getattr(r, 'message', '') or '')
        if token in msg.lower():
            thought = r
            break
    if thought is None:
        return out

    msg = str(getattr(thought, 'message', '') or '')
    reversal_pct = _extract_market_trend_reversal_pct(msg)
    label = _extract_market_trend_label(msg)
    out['market_trend'] = {
        'label': label,
        'reversal_pct': reversal_pct,
        'thought_id': int(getattr(thought, 'id', 0) or 0),
    }
    block_enabled = bool(getattr(settings, 'TRADER_PASSIVE_GUARD_BLOCK_ENABLED', False))
    if block_enabled:
        try:
            block_pct = int(getattr(settings, 'TRADER_PASSIVE_REVERSAL_BLOCK_PCT', 70))
        except (TypeError, ValueError):
            block_pct = 70
        if reversal_pct is not None and reversal_pct >= max(50, min(block_pct, 99)):
            out['block_active_ticks'] = True
            out['block_reason'] = (
                f'tendencia_mercado reversão alta ({reversal_pct}%) para {sym}'
            )
    return out


def _dispatch_strategies_for_context(
    user,
    trading_environment: str,
    ctx: ObservationContext,
    keys: list[str],
    *,
    execution_profile: AutomationExecutionProfile | None = None,
) -> None:
    """
    Em cada chamada (cada rodada do Celery após novos snapshots), corre ``evaluate``
    e depois os ``celery_tick`` — cada estratégia usa aproximações nos seus próprios limiares.

    Estratégias com :func:`~trader.automacoes.prefs.is_strategy_enabled` falso para o
    utilizador/ambiente/perfil são omitidas (silêncio: sem observer nem tick).
    """
    if not keys:
        return
    env = normalize_environment(trading_environment)
    if ctx.data_source == 'live_tail':
        profile = execution_profile or resolve_active_profile(user, env)
        target = (getattr(profile, 'live_ticker', '') or '').strip().upper()
        current = (ctx.ticker or '').strip().upper()
        # Modo estrito no ao vivo: só processa o ticker explicitamente selecionado.
        if not target or not current or current != target:
            return
    keys_run = list(keys)
    pause_passive = bool(getattr(settings, 'TRADER_PASSIVE_PAUSE_WHEN_OPEN_POSITION', False))
    if pause_passive:
        lane = 'replay_shadow' if (env == ENV_REPLAY and ctx.data_source == 'session_replay') else 'standard'
        sym = (ctx.ticker or '').strip().upper()
        has_open = bool(sym) and has_open_position_for_ticker(sym, position_lane=lane)
        if has_open:
            keys_run = [k for k in keys if not is_passive_strategy(k)]
            if len(keys_run) != len(keys):
                uid = int(getattr(user, 'id', 0) or 0)
                ck = f'automation:passive_pause:open:{uid}:{env}:{sym}:{lane}'
                if cache.add(ck, '1', timeout=120):
                    try:
                        record_automation_thought(
                            user,
                            env,
                            (
                                f'Estratégias passivas pausadas em [{sym}] enquanto houver operação aberta '
                                f'({lane}). Reativação automática após liquidação.'
                            )[:3900],
                            source='passive_pause_global',
                            kind=AutomationThought.Kind.NOTICE,
                            execution_profile=execution_profile,
                        )
                    except Exception:
                        logger.exception('automation_engine passive global pause thought')
        else:
            uid = int(getattr(user, 'id', 0) or 0)
            ck = f'automation:passive_pause:open:{uid}:{env}:{sym}:{lane}'
            if cache.get(ck):
                cache.delete(ck)
                rk = f'automation:passive_pause:resume:{uid}:{env}:{sym}:{lane}'
                if cache.add(rk, '1', timeout=120):
                    try:
                        record_automation_thought(
                            user,
                            env,
                            (
                                f'Estratégias passivas reativadas em [{sym}] após liquidação '
                                f'da posição ({lane}).'
                            )[:3900],
                            source='passive_pause_global',
                            kind=AutomationThought.Kind.NOTICE,
                            execution_profile=execution_profile,
                        )
                    except Exception:
                        logger.exception('automation_engine passive global resume thought')
    if not keys_run:
        return
    # Toggle efetivo (prefs + armazenamento replay/simulador): se desligado, não corre
    # evaluate/observer nem celery_tick — evita WARN “estratégia desligada” e ruído no painel.
    _prof = execution_profile or (
        resolve_active_profile(user, env) if user is not None else None
    )
    keys_run = [
        k
        for k in keys_run
        if user is None
        or is_strategy_enabled(user, k, env, execution_profile=_prof)
    ]
    if not keys_run:
        return
    _record_strategy_observer_thoughts(
        user,
        env,
        ctx,
        keys_run,
        execution_profile=execution_profile,
    )
    passive_ctx = _passive_context_from_logs(
        user,
        env,
        ctx,
        keys_run,
        execution_profile=execution_profile,
    )
    if isinstance(ctx.extra, dict):
        ctx.extra['passive_context'] = passive_ctx
    else:
        ctx.extra = {'passive_context': passive_ctx}
    for sk in keys_run:
        tick = get_celery_tick(sk)
        if tick is None:
            continue
        if passive_ctx.get('block_active_ticks') and not is_passive_strategy(sk):
            uid = int(getattr(user, 'id', 0) or 0)
            k = f'automation:passive_guard:block:{uid}:{env}:{(ctx.ticker or "").strip().upper()}:{sk}'
            if cache.add(k, '1', timeout=45):
                try:
                    record_automation_thought(
                        user,
                        env,
                        (
                            f'Execução ativa bloqueada por passiva [{sk}]: '
                            f'{passive_ctx.get("block_reason") or "guard-rail passivo"}.'
                        )[:3900],
                        source='passive_guard',
                        kind=AutomationThought.Kind.NOTICE,
                        execution_profile=execution_profile,
                    )
                except Exception:
                    logger.exception('automation_engine passive guard thought')
            continue
        if ctx.data_source == 'live_tail' and not quote_live_allows_automation_orders(
            ctx.quote
        ) and not is_passive_strategy(sk):
            uid = int(getattr(user, 'id', 0) or 0)
            sym_u = (ctx.ticker or '').strip().upper() or '—'
            k = f'automation:live_mkt:block:{uid}:{env}:{sym_u}:{sk}'
            if cache.add(k, '1', timeout=900):
                try:
                    q = ctx.quote if isinstance(ctx.quote, dict) else {}
                    st = q.get('status') or q.get('Status') or '—'
                    record_automation_thought(
                        user,
                        env,
                        (
                            f'Execução ativa em pausa: mercado fora de negociação contínua '
                            f'(status={st!s}) [{sk}].'
                        )[:3900],
                        source='market_session',
                        kind=AutomationThought.Kind.NOTICE,
                        execution_profile=execution_profile,
                    )
                except Exception:
                    logger.exception('automation_engine live market session thought')
            continue
        try:
            tick(ctx, user, env)
        except Exception:
            logger.exception('automation_engine celery_tick %s', sk)


def run_automation_session_replay_now(
    user,
    *,
    session_day: date,
    sim_ticker: str,
    replay_until: datetime | None,
    trading_environment: str = ENV_REPLAY,
    force: bool = False,
) -> None:
    """
    Avalia estratégias no instante ``replay_until`` da simulação (mesmo critério do gráfico).

    Chamado pelo POST do cursor de replay para disparar regras **a cada instante reproduzido**,
    sem esperar só pelo ciclo do Celery.

    Executa o mesmo pipeline do worker (``evaluate`` + ``celery_tick``) para manter o
    comportamento consistente entre replay manual (scrubber) e ciclo normal do Celery.

    ``force=True`` ignora o guard monotónico do cursor em perfil de simulação (útil para
    reprocessar instantes em ordem, ex.: stream de replay no servidor).
    """
    env = normalize_environment(trading_environment)
    if env != ENV_REPLAY:
        return
    uid = int(getattr(user, 'id', 0) or 0)
    if not uid:
        return
    if not runtime_enabled(user, env):
        return
    strat_map = _enabled_strategies_by_env_user()
    user_slot = (strat_map.get(env) or {}).get(uid, {})
    keys = list(user_slot.get('keys') or [])
    profile = user_slot.get('profile') or resolve_active_profile(user, env)
    if not keys:
        return
    sym = (sim_ticker or '').strip().upper()
    if not sym or session_day is None:
        return
    cursor_ck = (
        f'automation:runtime_cursor:v2:{uid}:{env}:{sym}:'
        f'{session_day.isoformat()}:{int(getattr(profile, "id", 0) or 0)}:'
        f'{int(getattr(getattr(profile, "execution_started_at", None), "timestamp", lambda: 0)() or 0)}'
    )
    cache_cursor = None
    if replay_until is not None:
        cache_cursor = cache.get(cursor_ck)
        if isinstance(cache_cursor, str):
            try:
                cache_cursor = datetime.fromisoformat(cache_cursor)
            except ValueError:
                cache_cursor = None
    if profile is not None and profile.mode == AutomationExecutionProfile.Mode.SIMULATION:
        if profile.execution_started_at is None:
            return
        if not force:
            last_cursor = profile.last_runtime_cursor_at
            if isinstance(cache_cursor, datetime):
                if last_cursor is None or cache_cursor > last_cursor:
                    last_cursor = cache_cursor
            if replay_until is not None and last_cursor is not None:
                if replay_until <= last_cursor:
                    return
    set_current_environment(env)
    interval_sec = _interval_sec_from_settings()
    candles = load_session_day_candles(
        sym,
        session_day,
        interval_sec=interval_sec,
        replay_until=replay_until,
    )
    if replay_until is not None:
        candles = trim_candles_to_replay_until(candles, replay_until)
    ctx = _build_session_replay_context(sym, env, session_day, replay_until, candles)
    _dispatch_strategies_for_context(user, env, ctx, keys, execution_profile=profile)
    if replay_until is not None:
        # Cursor monotônico em cache: evita escrita em SQLite a cada frame do replay.
        cache.set(cursor_ck, replay_until.isoformat(), timeout=12 * 3600)


def run_automation_after_quote_collect(
    tickers: list[str],
    *,
    probe_quote: dict[str, Any] | None,
    interval_sec: int = 10,
) -> None:
    """
    Invocado no fim de ``collect_watch_quotes``.

    Para cada utilizador com estratégias activas, monta contexto(s) com candles do **dia inteiro**
    em BRT (``QuoteSnapshot`` até ao instante actual; no simulador, ``replay_until``) e corre
    ``evaluate`` + ``celery_tick``.
    """
    if not tickers:
        return

    strat_map = _enabled_strategies_by_env_user()
    if not strat_map:
        return

    try:
        iv = max(1, min(int(interval_sec), 300))
    except (TypeError, ValueError):
        iv = _interval_sec_from_settings()

    for env, user_to_keys in strat_map.items():
        set_current_environment(env)
        uids = list(user_to_keys.keys())
        env_runtime_map = runtime_enabled_map(uids, env)
        users = {
            u.id: u
            for u in User.objects.filter(id__in=uids, is_active=True)
            if env_runtime_map.get(int(u.id), True)
        }
        if not users:
            continue
        prefs = _sim_prefs_map(uids, env)

        users_sim_day = [
            users[uid]
            for uid in uids
            if uid in users and env == ENV_SIMULATOR and _sim_pref_active(prefs.get(uid))
        ]
        users_replay_day = [
            users[uid]
            for uid in uids
            if uid in users and env == ENV_REPLAY and _sim_pref_active(prefs.get(uid))
        ]
        users_live = [
            users[uid]
            for uid in uids
            if uid in users and users[uid] not in users_sim_day and users[uid] not in users_replay_day
        ]

        for u in users_sim_day:
            slot = user_to_keys.get(u.id, {}) or {}
            keys = list(slot.get('keys') or [])
            profile = slot.get('profile') or resolve_active_profile(u, env)
            if not keys:
                continue
            if profile is not None and profile.mode == AutomationExecutionProfile.Mode.SIMULATION:
                if profile.execution_started_at is None:
                    continue
            p = prefs[u.id]
            sd: date = p.session_date  # type: ignore[assignment]
            sym = (p.sim_ticker or '').strip().upper()
            # Simulador: dia histórico sempre até ao fim dos dados locais (replay só no ambiente Replay).
            candles = load_session_day_candles(
                sym,
                sd,
                interval_sec=iv,
                replay_until=None,
            )
            ctx = _build_session_replay_context(sym, env, sd, None, candles)
            _dispatch_strategies_for_context(u, env, ctx, keys, execution_profile=profile)

        for u in users_replay_day:
            slot = user_to_keys.get(u.id, {}) or {}
            keys = list(slot.get('keys') or [])
            profile = slot.get('profile') or resolve_active_profile(u, env)
            if not keys:
                continue
            if profile is not None and profile.mode == AutomationExecutionProfile.Mode.SIMULATION:
                if profile.execution_started_at is None:
                    continue
            p = prefs[u.id]
            sd = p.session_date  # type: ignore[assignment]
            sym = (p.sim_ticker or '').strip().upper()
            candles = load_session_day_candles(
                sym,
                sd,
                interval_sec=iv,
                replay_until=p.replay_until,
            )
            if p.replay_until is not None:
                candles = trim_candles_to_replay_until(candles, p.replay_until)
            ctx = _build_session_replay_context(sym, env, sd, p.replay_until, candles)
            _dispatch_strategies_for_context(u, env, ctx, keys, execution_profile=profile)

        if not users_live:
            continue

        selected_live = []
        for raw in tickers:
            sym = (raw or '').strip().upper()
            if sym and sym not in selected_live:
                selected_live.append(sym)
        if not selected_live:
            continue
        selected_live_set = set(selected_live)
        primary = selected_live[0]
        day_live = _calendar_date_brt()
        live_ctx_cache: dict[str, ObservationContext] = {}

        def get_live_ctx(sym: str) -> ObservationContext:
            ctx = live_ctx_cache.get(sym)
            if ctx is not None:
                return ctx
            candles = load_session_day_candles(
                sym,
                day_live,
                interval_sec=iv,
                replay_until=None,
            )
            ctx = _build_live_context(sym, env, candles, session_day=day_live)
            live_ctx_cache[sym] = ctx
            return ctx

        for u in users_live:
            slot = user_to_keys.get(u.id, {}) or {}
            keys = list(slot.get('keys') or [])
            if not keys:
                continue
            profile = slot.get('profile') or resolve_active_profile(u, env)
            target = (getattr(profile, 'live_ticker', '') or '').strip().upper()
            # Modo estrito: robô ao vivo só opera no ticker explicitamente selecionado.
            if not target:
                continue
            if target not in selected_live_set:
                continue
            user_symbols = [target]

            keys_once = [k for k in keys if strategy_celery_scope(k) == 'once']
            if keys_once:
                once_sym = user_symbols[0] if user_symbols else primary
                _dispatch_strategies_for_context(
                    u,
                    env,
                    get_live_ctx(once_sym),
                    keys_once,
                    execution_profile=profile,
                )

            keys_per = [k for k in keys if strategy_celery_scope(k) == 'per_ticker']
            if not keys_per:
                continue
            for sym in user_symbols:
                _dispatch_strategies_for_context(
                    u,
                    env,
                    get_live_ctx(sym),
                    keys_per,
                    execution_profile=profile,
                )
