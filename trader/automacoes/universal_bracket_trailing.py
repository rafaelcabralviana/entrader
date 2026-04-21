"""
Ajuste central de stop (trailing) após bracket enviado por qualquer estratégia.

As estratégias definem TP/SL iniciais e persistem o estado aqui; o mesmo motor
de trailing aplica-se a leafaR, tendência ativa, etc.

A partir da v2 da chave de cache, o estado separa ``standard`` (API) de
``replay_shadow`` (replay fictício, sem chamadas à corretora).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import time as time_cls
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone as dj_tz

from trader.automacoes.bracket_width import (
    trailing_breakeven_arm_ticks,
    trailing_breakeven_offset_ticks,
    trailing_lock_profit_arm_pct,
    trailing_lock_profit_floor_pct,
    trailing_min_favorable_ticks,
    trailing_protective_floor_ticks,
    trailing_relax_max_ticks,
    trailing_relax_pullback_ticks,
    trailing_stop_tick_steps,
    trailing_tp_peak_follow_ticks,
)
from trader.automacoes.execution_simulation import simulate_non_real_fill
from trader.environment import get_current_environment, normalize_environment
from trader.models import Position
from trader.order_enums import ORDER_MODULE_DAY_TRADE, ORDER_TIF_DAY
from trader.services.orders import (
    post_cancel_order,
    post_replace_limited_order,
    post_replace_stop_limit_order,
    post_send_limited_order,
    post_send_market_order,
    post_send_stop_limit_order,
)
from trader.services.operations_history import register_trade_execution

logger = logging.getLogger(__name__)
_DEBUG_LOG_PATH = '/home/APICLEAR/.cursor/debug-c8b049.log'

_STATE_PREFIX_V2 = 'automation:bracket_trail:v2'

BRACKET_LANE_STANDARD = Position.Lane.STANDARD
BRACKET_LANE_REPLAY_SHADOW = Position.Lane.REPLAY_SHADOW
_TZ_BRT = ZoneInfo('America/Sao_Paulo')


# region agent log
def _agent_debug_log(
    hypothesis_id: str,
    location: str,
    message: str,
    data: dict[str, Any],
) -> None:
    try:
        payload = {
            'sessionId': 'c8b049',
            'runId': 'replay-stop-rules-v3',
            'hypothesisId': hypothesis_id,
            'location': location,
            'message': message,
            'data': data,
            'timestamp': int(time.time() * 1000),
        }
        with open(_DEBUG_LOG_PATH, 'a', encoding='utf-8') as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + '\n')
    except Exception:
        pass
# endregion


def _round_px(x: float) -> float:
    return round(float(x), 6)


def state_cache_key(
    ticker: str,
    *,
    bracket_lane: str = BRACKET_LANE_STANDARD,
    trading_environment: str | None = None,
) -> str:
    """
    Chave do estado de bracket no cache.

    ``trading_environment`` explícito (ex.: sessão do utilizador no JSON de candles)
    evita ler o estado com env errado quando o ContextVar do worker não coincide.
    """
    env = normalize_environment(trading_environment or get_current_environment())
    sym = (ticker or '').strip().upper()
    lane = (bracket_lane or BRACKET_LANE_STANDARD).strip() or BRACKET_LANE_STANDARD
    return f'{_STATE_PREFIX_V2}:{env}:{sym}:{lane}'


def save_bracket_state(
    ticker: str,
    payload: dict[str, Any],
    *,
    bracket_lane: str = BRACKET_LANE_STANDARD,
    trading_environment: str | None = None,
) -> None:
    """Grava estado do bracket (por ticker, ambiente e faixa de ledger) para trailing."""
    sym = (ticker or '').strip().upper()
    lane = (bracket_lane or BRACKET_LANE_STANDARD).strip() or BRACKET_LANE_STANDARD
    merged = dict(payload)
    merged.setdefault('bracket_lane', lane)
    cache.set(
        state_cache_key(sym, bracket_lane=lane, trading_environment=trading_environment),
        json.dumps(merged),
        timeout=6 * 3600,
    )


def _float_state(st: dict[str, Any], key: str, default: float) -> float:
    v = st.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _replay_shadow_state(st: dict[str, Any]) -> bool:
    return str(st.get('execution_mode') or '').strip() == 'replay_shadow'


def _force_close_hhmm() -> tuple[int, int]:
    raw = str(getattr(settings, 'TRADER_DAYTRADE_FORCE_CLOSE_HHMM', '17:55') or '17:55').strip()
    try:
        hh_s, mm_s = raw.split(':', 1)
        hh = max(0, min(23, int(hh_s)))
        mm = max(0, min(59, int(mm_s)))
        return hh, mm
    except Exception:
        return 17, 55


def _force_close_window_open() -> bool:
    hh, mm = _force_close_hhmm()
    now = dj_tz.now().astimezone(_TZ_BRT)
    return now.time() >= time_cls(hh, mm, 0)


def _order_id_from_response(resp: Any) -> str | None:
    if not isinstance(resp, dict):
        return None
    for key in ('Id', 'id', 'OrderId', 'orderId', 'ID'):
        v = resp.get(key)
        if v is not None and str(v).strip() != '':
            return str(v).strip()
    return None


def _bootstrap_protection_orders(
    *,
    sym: str,
    st: dict[str, Any],
    qty: int,
    tick: float,
    shadow: bool,
    src: str,
) -> list[str]:
    messages: list[str] = []
    entry_side = st.get('entry_side')
    exit_side = st.get('exit_side')
    if exit_side not in ('Buy', 'Sell'):
        return messages
    tp_oid = str(st.get('tp_order_id') or '').strip()
    sl_oid = str(st.get('sl_order_id') or '').strip()
    tp_price = _float_state(st, 'tp_price', 0.0)
    trig = _float_state(st, 'sl_trigger', 0.0)
    ord_px = _float_state(st, 'sl_order_price', 0.0)
    if trig <= 0:
        anchor = _float_state(st, 'entry_anchor', _float_state(st, 'last', 0.0))
        if anchor > 0:
            trig = anchor - tick * 6 if entry_side == 'Buy' else anchor + tick * 6
            st['sl_trigger'] = trig
    if ord_px <= 0 and trig > 0:
        ord_px = trig - tick * 2 if exit_side == 'Sell' else trig + tick * 2
        st['sl_order_price'] = ord_px

    if shadow:
        if not sl_oid and trig > 0 and ord_px > 0:
            st['sl_order_id'] = f'replay-shadow:auto-sl:{sym}'
            messages.append(f'{src}: SL inicial (replay) armado.')
        if not tp_oid and tp_price > 0:
            st['tp_order_id'] = f'replay-shadow:auto-tp:{sym}'
            messages.append(f'{src}: TP inicial (replay) armado.')
        return messages

    base = {
        'Module': ORDER_MODULE_DAY_TRADE,
        'Ticker': sym,
        'Quantity': qty,
        'TimeInForce': ORDER_TIF_DAY,
        'Side': exit_side,
    }
    if not sl_oid and trig > 0 and ord_px > 0:
        try:
            sl_resp = post_send_stop_limit_order(
                {
                    **base,
                    'StopTriggerPrice': _round_px(trig),
                    'StopOrderPrice': _round_px(ord_px),
                }
            )
            st['sl_order_id'] = _order_id_from_response(sl_resp)
            if st.get('sl_order_id'):
                messages.append(f'{src}: SL inicial enviado.')
        except Exception as exc:
            messages.append(f'{src}: falha ao enviar SL inicial: {exc}')
    if not tp_oid and tp_price > 0:
        try:
            tp_resp = post_send_limited_order(
                {**base, 'Price': _round_px(tp_price)}
            )
            st['tp_order_id'] = _order_id_from_response(tp_resp)
            if st.get('tp_order_id'):
                messages.append(f'{src}: TP inicial enviado.')
        except Exception as exc:
            messages.append(f'{src}: falha ao enviar TP inicial: {exc}')
    return messages


def _force_close_daytrade_if_needed(
    *,
    sym: str,
    st: dict[str, Any],
    qty: int,
    shadow: bool,
    src: str,
) -> str | None:
    if st.get('force_close_done'):
        return None
    if not _force_close_window_open():
        return None
    exit_side = str(st.get('exit_side') or '').strip()
    if exit_side not in ('Buy', 'Sell'):
        return None
    entry_id = str(st.get('market_order_id') or st.get('operation_id') or '').strip()
    if shadow:
        pos_open = (
            Position.objects.filter(
                ticker=sym,
                trading_environment=Position.TradingEnvironment.REPLAY,
                position_lane=Position.Lane.REPLAY_SHADOW,
                is_active=True,
                closed_at__isnull=True,
                quantity_open__gt=0,
            )
            .order_by('-opened_at')
            .first()
        )
        if pos_open is None:
            st['closed_reason'] = 'stale_no_position'
            return f'{src}: trailing replay sem posição ativa para fechamento fim do dia.'
        expected_exit = 'Sell' if pos_open.side == Position.Side.LONG else 'Buy'
        if exit_side != expected_exit:
            st['closed_reason'] = 'stale_side_mismatch'
            return f'{src}: trailing replay divergente do ledger no fechamento fim do dia.'
        try:
            open_qty = float(pos_open.quantity_open)
        except (TypeError, ValueError):
            open_qty = float(qty)
        close_qty = max(1, int(min(float(qty), open_qty)))
        sim = simulate_non_real_fill(
            trading_environment=Position.TradingEnvironment.REPLAY,
            side=exit_side,
            reference_price=_float_state(st, 'last', 0.0) or 0.01,
            is_exit=True,
        )
        if not sim.filled:
            return f'{src}: replay tentou fechamento fim do dia, mas sem execução simulada.'
        try:
            register_trade_execution(
                ticker=sym,
                side=exit_side,
                quantity=close_qty,
                price=_round_px(float(sim.price)),
                source='trailing_replay_eod',
                trading_environment=Position.TradingEnvironment.REPLAY,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
            try:
                from trader.models import AutomationExecutionProfile, AutomationTriggerMarker

                uid = int(st.get('user_id') or 0)
                ep_id = int(st.get('execution_profile_id') or 0)
                if uid > 0:
                    ep = (
                        AutomationExecutionProfile.objects.filter(id=ep_id).first()
                        if ep_id > 0
                        else None
                    )
                    AutomationTriggerMarker.objects.create(
                        user_id=uid,
                        execution_profile=ep,
                        trading_environment=Position.TradingEnvironment.REPLAY,
                        ticker=sym,
                        strategy_key='trade_exit',
                        marker_at=dj_tz.now(),
                        price=_round_px(float(sim.price)),
                        message=f'source=trailing_replay_eod;side={exit_side};qty={close_qty};reason=eod'[:500],
                    )
            except Exception:
                logger.exception('universal_bracket_trailing replay eod exit marker')
        except Exception as exc:
            return f'{src}: replay falhou ao registrar fechamento fim do dia ({exc}).'
        st['closed_price'] = _round_px(float(sim.price))
        st['closed_reason'] = 'replay_eod'
        st['force_close_done'] = True
        st['force_close_reason'] = 'eod_replay'
        st['closed_operation_id'] = entry_id or None
        try:
            from trader.panel_context import invalidate_collateral_custody_cache

            invalidate_collateral_custody_cache()
        except Exception:
            logger.exception('universal_bracket_trailing invalidate custody cache replay eod')
        return (
            f'{src}: fechamento forçado fim do dia (replay) para operação '
            f'{entry_id or "sem_id"}'
        )
    for oid_key in ('tp_order_id', 'sl_order_id'):
        oid = str(st.get(oid_key) or '').strip()
        if not oid:
            continue
        try:
            post_cancel_order(oid)
        except Exception:
            logger.warning('universal_bracket_trailing cancel %s falhou (%s)', oid_key, oid)
    body = {
        'Module': ORDER_MODULE_DAY_TRADE,
        'Ticker': sym,
        'Side': exit_side,
        'Quantity': qty,
        'TimeInForce': ORDER_TIF_DAY,
    }
    resp = post_send_market_order(body)
    close_id = _order_id_from_response(resp)
    if not close_id:
        return (
            f'{src}: falha ao fechar operação {entry_id or "sem_id"} no fim do dia '
            f'(API sem id de fechamento).'
        )
    st['force_close_done'] = True
    st['force_close_reason'] = 'eod'
    st['close_market_order_id'] = close_id
    st['tp_order_id'] = None
    st['sl_order_id'] = None
    return (
        f'{src}: operação {entry_id or "sem_id"} encerrada no fim do dia '
        f'(ordem fechamento id={close_id}).'
    )


def _close_replay_shadow_if_price_hit(
    *,
    sym: str,
    st: dict[str, Any],
    last_price: float,
    candle_high: float | None,
    candle_low: float | None,
    qty: int,
    src: str,
) -> str | None:
    """
    No replay_shadow não há corretora para executar TP/SL; fecha no ledger quando preço toca níveis.
    """
    if st.get('closed_reason'):
        return None
    entry_side = str(st.get('entry_side') or '').strip()
    exit_side = str(st.get('exit_side') or '').strip()
    if entry_side not in ('Buy', 'Sell') or exit_side not in ('Buy', 'Sell'):
        return None
    tp = _float_state(st, 'tp_price', 0.0)
    sl = _float_state(st, 'sl_trigger', 0.0)
    entry_ref = _float_state(st, 'entry_anchor', _float_state(st, 'last', last_price))
    initial_sl = _float_state(st, 'initial_sl_trigger', 0.0)
    if initial_sl <= 0 and sl > 0:
        initial_sl = sl
        st['initial_sl_trigger'] = sl
    tick = 0.01 if float(last_price) < 1000 else 0.05
    high_touch = float(candle_high) if candle_high is not None else float(last_price)
    low_touch = float(candle_low) if candle_low is not None else float(last_price)
    hit_reason = ''
    hit_px = 0.0
    if entry_side == 'Buy' and exit_side == 'Sell':
        if tp > 0 and high_touch >= tp - 1e-12:
            hit_reason = 'tp'
            hit_px = tp
        elif sl > 0 and low_touch <= sl + 1e-12:
            hit_reason = 'sl'
            hit_px = sl
    elif entry_side == 'Sell' and exit_side == 'Buy':
        if tp > 0 and low_touch <= tp + 1e-12:
            hit_reason = 'tp'
            hit_px = tp
        elif sl > 0 and high_touch >= sl - 1e-12:
            hit_reason = 'sl'
            hit_px = sl
    if not hit_reason:
        return None
    # region agent log
    _agent_debug_log(
        'H2',
        'universal_bracket_trailing.py:_close_replay_shadow_if_price_hit',
        'hit reason resolved',
        {
            'ticker': sym,
            'hit_reason': hit_reason,
            'tp': float(tp),
            'sl': float(sl),
            'last_price': float(last_price),
        },
    )
    # endregion
    pos_open = (
        Position.objects.filter(
            ticker=sym,
            trading_environment=Position.TradingEnvironment.REPLAY,
            position_lane=Position.Lane.REPLAY_SHADOW,
            is_active=True,
            closed_at__isnull=True,
            quantity_open__gt=0,
        )
        .order_by('-opened_at')
        .first()
    )
    if pos_open is None:
        st['closed_reason'] = 'stale_no_position'
        return f'{src}: estado trailing replay removido (sem posição ativa no ledger).'
    expected_exit = 'Sell' if pos_open.side == Position.Side.LONG else 'Buy'
    if exit_side != expected_exit:
        st['closed_reason'] = 'stale_side_mismatch'
        return f'{src}: estado trailing replay removido (lado divergente do ledger).'
    try:
        open_qty = float(pos_open.quantity_open)
    except (TypeError, ValueError):
        open_qty = float(qty)
    close_qty = max(1, int(min(float(qty), open_qty)))

    sim = simulate_non_real_fill(
        trading_environment=Position.TradingEnvironment.REPLAY,
        side=exit_side,
        reference_price=float(hit_px if hit_px > 0 else float(last_price)),
        is_exit=True,
    )
    if not sim.filled:
        return f'{src}: replay tentou fechar por {hit_reason.upper()}, mas sem execução simulada.'
    register_trade_execution(
        ticker=sym,
        side=exit_side,
        quantity=close_qty,
        price=_round_px(float(sim.price)),
        source=f'trailing_replay_{hit_reason}',
        trading_environment=Position.TradingEnvironment.REPLAY,
        position_lane=Position.Lane.REPLAY_SHADOW,
    )
    try:
        from trader.models import AutomationExecutionProfile, AutomationTriggerMarker

        uid = int(st.get('user_id') or 0)
        ep_id = int(st.get('execution_profile_id') or 0)
        if uid > 0:
            ep = (
                AutomationExecutionProfile.objects.filter(id=ep_id).first()
                if ep_id > 0
                else None
            )
            AutomationTriggerMarker.objects.create(
                user_id=uid,
                execution_profile=ep,
                trading_environment=Position.TradingEnvironment.REPLAY,
                ticker=sym,
                strategy_key='trade_exit',
                marker_at=dj_tz.now(),
                price=_round_px(float(sim.price)),
                message=f'source=trailing_replay_{hit_reason};side={exit_side};qty={close_qty};reason={hit_reason}'[:500],
            )
    except Exception:
        logger.exception('universal_bracket_trailing replay exit marker')
    # region agent log
    _agent_debug_log(
        'H2',
        'universal_bracket_trailing.py:_close_replay_shadow_if_price_hit',
        'close execution registered',
        {
            'ticker': sym,
            'exit_side': exit_side,
            'close_qty': int(close_qty),
            'close_price': float(sim.price),
            'hit_reason': hit_reason,
        },
    )
    # endregion
    st['closed_reason'] = f'replay_{hit_reason}'
    st['closed_price'] = _round_px(float(sim.price))
    st['tp_order_id'] = None
    st['sl_order_id'] = None
    st['close_market_order_id'] = f'replay-shadow:auto-{hit_reason}'
    st['closed_at_iso'] = dj_tz.now().isoformat()
    try:
        from trader.panel_context import invalidate_collateral_custody_cache

        invalidate_collateral_custody_cache()
    except Exception:
        logger.exception('universal_bracket_trailing invalidate custody cache replay close')
    return (
        f'{src}: replay fechou por {"TP" if hit_reason == "tp" else "SL"} '
        f'em {_round_px(st["closed_price"])}.'
    )


def try_trailing_stop_update(
    ticker: str,
    last_price: float,
    *,
    bracket_lane: str = BRACKET_LANE_STANDARD,
    trading_environment: str | None = None,
    trail_ticks: float | None = None,
    candle_high: float | None = None,
    candle_low: float | None = None,
) -> str | None:
    """
    Ajusta stop-limit (e opcionalmente TP limite) quando o preço evolui a favor.

    - MFE, piso de proteção, travamento de lucro mínimo (arm/floor em %).
    - TP segue o pico/vale (``TRADER_TRAILING_TP_FOLLOW_PEAK_TICKS``).

    Em ``replay_shadow`` só actualiza cache; na API chama replace nas ordens.
    """
    if trail_ticks is None:
        trail_ticks = trailing_stop_tick_steps()
    sym = (ticker or '').strip().upper()
    lane = (bracket_lane or BRACKET_LANE_STANDARD).strip() or BRACKET_LANE_STANDARD
    key = state_cache_key(sym, bracket_lane=lane, trading_environment=trading_environment)
    raw = cache.get(key)
    if not raw or not isinstance(raw, str):
        return None
    try:
        st = json.loads(raw)
    except json.JSONDecodeError:
        return None
    entry_side = st.get('entry_side')
    exit_side = st.get('exit_side')
    old_trig = float(st.get('sl_trigger') or 0)
    old_ord = float(st.get('sl_order_price') or 0)
    qty = max(1, int(st.get('quantity') or 1))
    tick = 0.01 if last_price < 1000 else 0.05
    high_ref = float(candle_high) if candle_high is not None else float(last_price)
    low_ref = float(candle_low) if candle_low is not None else float(last_price)
    step = tick * trail_ticks
    src = (st.get('strategy_source') or 'automação').strip()
    shadow = _replay_shadow_state(st)
    # region agent log
    _agent_debug_log(
        'H1',
        'universal_bracket_trailing.py:try_trailing_stop_update',
        'trailing tick',
        {
            'ticker': sym,
            'shadow': bool(shadow),
            'entry_side': str(entry_side or ''),
            'exit_side': str(exit_side or ''),
            'qty_cache': int(qty),
            'sl_trigger': float(old_trig),
            'last_price': float(last_price),
        },
    )
    # endregion
    bootstrap_msgs = _bootstrap_protection_orders(
        sym=sym,
        st=st,
        qty=qty,
        tick=tick,
        shadow=shadow,
        src=src,
    )
    forced_msg = _force_close_daytrade_if_needed(
        sym=sym,
        st=st,
        qty=qty,
        shadow=shadow,
        src=src,
    )
    if forced_msg:
        if st.get('closed_reason'):
            cache.delete(key)
        else:
            cache.set(key, json.dumps(st), timeout=30 * 60)
        if bootstrap_msgs:
            return ' | '.join([*bootstrap_msgs, forced_msg])
        return forced_msg
    if shadow:
        try:
            hit_msg = _close_replay_shadow_if_price_hit(
                sym=sym,
                st=st,
                last_price=last_price,
                candle_high=high_ref,
                candle_low=low_ref,
                qty=qty,
                src=src,
            )
            if hit_msg:
                # Só remove estado quando houve fechamento real.
                # Mensagens de "chance" do SL flutuante devem manter trailing ativo.
                if st.get('closed_reason'):
                    cache.delete(key)
                else:
                    cache.set(key, json.dumps(st), timeout=6 * 3600)
                if bootstrap_msgs:
                    return ' | '.join([*bootstrap_msgs, hit_msg])
                return hit_msg
        except Exception as exc:
            logger.warning('universal_bracket_trailing replay close %s', exc)
    oid = st.get('sl_order_id')
    if not oid:
        cache.set(key, json.dumps(st), timeout=6 * 3600)
        return ' | '.join(bootstrap_msgs) if bootstrap_msgs else None
    if shadow:
        env_q = normalize_environment(trading_environment or get_current_environment())
        pos = (
            Position.objects.filter(
                ticker=sym,
                trading_environment=env_q,
                position_lane=Position.Lane.REPLAY_SHADOW,
                is_active=True,
                closed_at__isnull=True,
                quantity_open__gt=0,
            )
            .order_by('-opened_at')
            .first()
        )
        # region agent log
        _agent_debug_log(
            'H1',
            'universal_bracket_trailing.py:try_trailing_stop_update',
            'shadow reconcile',
            {
                'ticker': sym,
                'env': env_q,
                'has_pos': bool(pos is not None),
                'pos_side': str(getattr(pos, 'side', '')),
                'pos_qty': str(getattr(pos, 'quantity_open', '')),
                'cache_entry_side': str(entry_side or ''),
            },
        )
        # endregion
        if pos is None:
            cache.delete(key)
            return None
        expected_entry = 'Buy' if pos.side == Position.Side.LONG else 'Sell'
        if str(entry_side or '').strip() != expected_entry:
            cache.delete(key)
            return f'{src}: trailing replay reiniciado (mudança de lado da posição).'
    entry_ref = _float_state(st, 'entry_anchor', _float_state(st, 'last', last_price))
    initial_sl = _float_state(st, 'initial_sl_trigger', 0.0)
    if initial_sl <= 0 and old_trig > 0:
        initial_sl = old_trig
        st['initial_sl_trigger'] = _round_px(initial_sl)
    mfe_ticks = trailing_min_favorable_ticks()
    if mfe_ticks > 0:
        min_fav = tick * float(mfe_ticks)
        if entry_side == 'Buy' and exit_side == 'Sell':
            peak_chk = max(_float_state(st, 'peak', last_price), last_price)
            if peak_chk - entry_ref < min_fav - 1e-12:
                return None
        elif entry_side == 'Sell' and exit_side == 'Buy':
            trough_chk = min(_float_state(st, 'trough', last_price), last_price)
            if entry_ref - trough_chk < min_fav - 1e-12:
                return None

    tp_follow = trailing_tp_peak_follow_ticks()
    messages: list[str] = list(bootstrap_msgs)

    def _persist() -> None:
        st['last'] = last_price
        cache.set(key, json.dumps(st), timeout=6 * 3600)

    def _try_close_shadow_after_updates() -> str | None:
        if not shadow:
            return None
        hit_msg = _close_replay_shadow_if_price_hit(
            sym=sym,
            st=st,
            last_price=last_price,
            candle_high=high_ref,
            candle_low=low_ref,
            qty=qty,
            src=src,
        )
        if not hit_msg:
            return None
        if st.get('closed_reason'):
            cache.delete(key)
        else:
            _persist()
        return hit_msg

    try:
        if entry_side == 'Buy' and exit_side == 'Sell':
            peak = max(_float_state(st, 'peak', last_price), last_price, high_ref)
            st['peak'] = peak
            new_trig = old_trig if old_trig > 0 else initial_sl
            if new_trig <= 0:
                new_trig = peak - step
            # Regra simples:
            # 1) passou da entrada -> SL vai no mínimo para a entrada;
            # 2) depois acompanha o pico por distância fixa.
            if peak >= entry_ref + 1e-12:
                trail_candidate = peak - step
                new_trig = max(new_trig, entry_ref, trail_candidate)
                st['sl_profit_lock'] = True
            if initial_sl > 0:
                new_trig = max(new_trig, initial_sl)
            new_ord = new_trig - tick * 2
            sl_changed = abs(new_trig - old_trig) > tick * 0.5
            if sl_changed:
                # region agent log
                _agent_debug_log(
                    'H6',
                    'universal_bracket_trailing.py:try_trailing_stop_update',
                    'buy sl candidate',
                    {
                        'ticker': sym,
                        'entry_ref': float(entry_ref),
                        'old_sl': float(old_trig),
                        'new_sl': float(new_trig),
                        'progress_tp': 0.0,
                        'be_armed': bool(peak >= entry_ref + 1e-12),
                        'profit_lock': bool(st.get('sl_profit_lock')),
                    },
                )
                # endregion
                if shadow:
                    st['sl_trigger'] = new_trig
                    st['sl_order_price'] = new_ord
                    messages.append(
                        f'{src}: trailing SL (compra, replay) gatilho→{_round_px(new_trig)}.'
                    )
                else:
                    body = {
                        'Quantity': qty,
                        'StopTriggerPrice': _round_px(new_trig),
                        'StopOrderPrice': _round_px(new_ord),
                        'TimeInForce': ORDER_TIF_DAY,
                    }
                    post_replace_stop_limit_order(oid, body)
                    st['sl_trigger'] = new_trig
                    st['sl_order_price'] = new_ord
                    messages.append(f'{src}: trailing SL (compra) gatilho→{_round_px(new_trig)}.')
            _persist()

            tp_oid = st.get('tp_order_id')
            old_tp = _float_state(st, 'tp_price', 0)
            if (shadow or tp_oid) and tp_follow > 0 and exit_side == 'Sell':
                tp_follow_eff = max(1.0, float(tp_follow))
                step_tp = tick * tp_follow_eff
                cand_tp = peak - step_tp
                if cand_tp > entry_ref + 1e-12:
                    new_tp = max(old_tp, cand_tp) if old_tp > 0 else cand_tp
                    if old_tp <= 0 or new_tp > old_tp + tick * 0.5:
                        # region agent log
                        _agent_debug_log(
                            'H7',
                            'universal_bracket_trailing.py:try_trailing_stop_update',
                            'buy tp adjust',
                            {
                                'ticker': sym,
                                'entry_ref': float(entry_ref),
                                'old_tp': float(old_tp),
                                'cand_tp': float(cand_tp),
                                'new_tp': float(new_tp),
                                'peak': float(peak),
                                'tp_follow_eff': float(tp_follow_eff),
                            },
                        )
                        # endregion
                        st['tp_price'] = new_tp
                        if shadow:
                            messages.append(f'{src}: TP limite (compra, replay)→{_round_px(new_tp)}.')
                        else:
                            post_replace_limited_order(
                                str(tp_oid).strip(),
                                {
                                    'Quantity': qty,
                                    'Price': _round_px(new_tp),
                                    'TimeInForce': ORDER_TIF_DAY,
                                },
                            )
                            messages.append(f'{src}: TP limite (compra)→{_round_px(new_tp)}.')
                        _persist()

            post_hit_msg = _try_close_shadow_after_updates()
            if post_hit_msg:
                messages.append(post_hit_msg)
            return ' | '.join(messages) if messages else None

        if entry_side == 'Sell' and exit_side == 'Buy':
            trough = min(_float_state(st, 'trough', last_price), last_price, low_ref)
            st['trough'] = trough
            new_trig = old_trig if old_trig > 0 else initial_sl
            if new_trig <= 0:
                new_trig = trough + step
            # Regra simples:
            # 1) passou da entrada -> SL vai no máximo para a entrada;
            # 2) depois acompanha o vale por distância fixa.
            if trough <= entry_ref - 1e-12:
                trail_candidate = trough + step
                new_trig = min(new_trig, entry_ref, trail_candidate)
                st['sl_profit_lock'] = True
            if initial_sl > 0:
                new_trig = min(new_trig, initial_sl)
            new_ord = new_trig + tick * 2
            sl_changed = abs(new_trig - old_trig) > tick * 0.5
            if sl_changed:
                # region agent log
                _agent_debug_log(
                    'H6',
                    'universal_bracket_trailing.py:try_trailing_stop_update',
                    'sell sl candidate',
                    {
                        'ticker': sym,
                        'entry_ref': float(entry_ref),
                        'old_sl': float(old_trig),
                        'new_sl': float(new_trig),
                        'progress_tp': 0.0,
                        'be_armed': bool(trough <= entry_ref - 1e-12),
                        'profit_lock': bool(st.get('sl_profit_lock')),
                    },
                )
                # endregion
                if shadow:
                    st['sl_trigger'] = new_trig
                    st['sl_order_price'] = new_ord
                    messages.append(f'{src}: trailing SL (venda, replay) gatilho→{_round_px(new_trig)}.')
                else:
                    body = {
                        'Quantity': qty,
                        'StopTriggerPrice': _round_px(new_trig),
                        'StopOrderPrice': _round_px(new_ord),
                        'TimeInForce': ORDER_TIF_DAY,
                    }
                    post_replace_stop_limit_order(oid, body)
                    st['sl_trigger'] = new_trig
                    st['sl_order_price'] = new_ord
                    messages.append(f'{src}: trailing SL (venda) gatilho→{_round_px(new_trig)}.')
            _persist()

            tp_oid = st.get('tp_order_id')
            old_tp = _float_state(st, 'tp_price', 0)
            if (shadow or tp_oid) and tp_follow > 0 and exit_side == 'Buy':
                tp_follow_eff = max(1.0, float(tp_follow))
                step_tp = tick * tp_follow_eff
                cand_tp = trough + step_tp
                if cand_tp < entry_ref - 1e-12 and cand_tp > 0:
                    new_tp = min(old_tp, cand_tp) if old_tp > 0 else cand_tp
                    if old_tp <= 0 or new_tp < old_tp - tick * 0.5:
                        # region agent log
                        _agent_debug_log(
                            'H7',
                            'universal_bracket_trailing.py:try_trailing_stop_update',
                            'sell tp adjust',
                            {
                                'ticker': sym,
                                'entry_ref': float(entry_ref),
                                'old_tp': float(old_tp),
                                'cand_tp': float(cand_tp),
                                'new_tp': float(new_tp),
                                'trough': float(trough),
                                'tp_follow_eff': float(tp_follow_eff),
                            },
                        )
                        # endregion
                        st['tp_price'] = new_tp
                        if shadow:
                            messages.append(f'{src}: TP limite (venda, replay)→{_round_px(new_tp)}.')
                        else:
                            post_replace_limited_order(
                                str(tp_oid).strip(),
                                {
                                    'Quantity': qty,
                                    'Price': _round_px(new_tp),
                                    'TimeInForce': ORDER_TIF_DAY,
                                },
                            )
                            messages.append(f'{src}: TP limite (venda)→{_round_px(new_tp)}.')
                        _persist()

            post_hit_msg = _try_close_shadow_after_updates()
            if post_hit_msg:
                messages.append(post_hit_msg)
            return ' | '.join(messages) if messages else None
    except Exception as exc:
        logger.warning('universal_bracket_trailing %s', exc)
        return f'{src}: trailing falhou: {exc}'
    return None


try_trend_ativa_trailing_stop_update = try_trailing_stop_update

__all__ = [
    'BRACKET_LANE_REPLAY_SHADOW',
    'BRACKET_LANE_STANDARD',
    'save_bracket_state',
    'state_cache_key',
    'try_trailing_stop_update',
    'try_trend_ativa_trailing_stop_update',
]
