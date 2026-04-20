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
from datetime import time as time_cls
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone as dj_tz

from trader.automacoes.bracket_width import (
    trailing_lock_profit_arm_pct,
    trailing_lock_profit_floor_pct,
    trailing_min_favorable_ticks,
    trailing_protective_floor_ticks,
    trailing_stop_tick_steps,
    trailing_tp_peak_follow_ticks,
)
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

logger = logging.getLogger(__name__)

_STATE_PREFIX_V2 = 'automation:bracket_trail:v2'

BRACKET_LANE_STANDARD = Position.Lane.STANDARD
BRACKET_LANE_REPLAY_SHADOW = Position.Lane.REPLAY_SHADOW
_TZ_BRT = ZoneInfo('America/Sao_Paulo')


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
) -> None:
    """Grava estado do bracket (por ticker, ambiente e faixa de ledger) para trailing."""
    sym = (ticker or '').strip().upper()
    lane = (bracket_lane or BRACKET_LANE_STANDARD).strip() or BRACKET_LANE_STANDARD
    merged = dict(payload)
    merged.setdefault('bracket_lane', lane)
    cache.set(state_cache_key(sym, bracket_lane=lane), json.dumps(merged), timeout=6 * 3600)


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
        st['force_close_done'] = True
        st['force_close_reason'] = 'eod_replay'
        st['closed_operation_id'] = entry_id or None
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


def try_trailing_stop_update(
    ticker: str,
    last_price: float,
    *,
    bracket_lane: str = BRACKET_LANE_STANDARD,
    trail_ticks: float | None = None,
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
    key = state_cache_key(sym, bracket_lane=lane)
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
    step = tick * trail_ticks
    src = (st.get('strategy_source') or 'automação').strip()
    shadow = _replay_shadow_state(st)
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
        cache.set(key, json.dumps(st), timeout=30 * 60)
        if bootstrap_msgs:
            return ' | '.join([*bootstrap_msgs, forced_msg])
        return forced_msg
    oid = st.get('sl_order_id')
    if not oid:
        cache.set(key, json.dumps(st), timeout=6 * 3600)
        return ' | '.join(bootstrap_msgs) if bootstrap_msgs else None
    entry_ref = _float_state(st, 'entry_anchor', _float_state(st, 'last', last_price))
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

    arm_pct = trailing_lock_profit_arm_pct()
    lock_floor_pct = trailing_lock_profit_floor_pct()
    tp_follow = trailing_tp_peak_follow_ticks()
    messages: list[str] = list(bootstrap_msgs)

    def _persist() -> None:
        st['last'] = last_price
        cache.set(key, json.dumps(st), timeout=6 * 3600)

    try:
        if entry_side == 'Buy' and exit_side == 'Sell':
            peak = max(_float_state(st, 'peak', last_price), last_price)
            st['peak'] = peak
            new_trig = max(old_trig, peak - step)
            fl = trailing_protective_floor_ticks()
            if fl > 0:
                cap_trig = entry_ref - tick * float(fl)
                new_trig = min(new_trig, cap_trig)
            if arm_pct > 1e-15 and lock_floor_pct > 1e-15:
                favorable = (peak - entry_ref) / max(entry_ref, 1e-12)
                if favorable >= arm_pct - 1e-12:
                    lock_px = entry_ref * (1.0 + lock_floor_pct)
                    new_trig = max(new_trig, lock_px)
            new_ord = max(old_ord, new_trig - tick * 2)
            sl_changed = new_trig > old_trig + tick * 0.5
            if sl_changed:
                if shadow:
                    st['sl_trigger'] = new_trig
                    st['sl_order_price'] = new_ord
                    messages.append(f'{src}: trailing SL (compra, replay) gatilho→{_round_px(new_trig)}.')
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
            if tp_oid and tp_follow > 0 and exit_side == 'Sell':
                step_tp = tick * tp_follow
                cand_tp = peak - step_tp
                if cand_tp > entry_ref + 1e-12:
                    new_tp = max(old_tp, cand_tp) if old_tp > 0 else cand_tp
                    if old_tp <= 0 or new_tp > old_tp + tick * 0.5:
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

            return ' | '.join(messages) if messages else None

        if entry_side == 'Sell' and exit_side == 'Buy':
            trough = min(_float_state(st, 'trough', last_price), last_price)
            st['trough'] = trough
            new_trig = min(old_trig, trough + step)
            fl = trailing_protective_floor_ticks()
            if fl > 0:
                floor_trig = entry_ref + tick * float(fl)
                new_trig = max(new_trig, floor_trig)
            if arm_pct > 1e-15 and lock_floor_pct > 1e-15:
                favorable = (entry_ref - trough) / max(entry_ref, 1e-12)
                if favorable >= arm_pct - 1e-12:
                    lock_px = entry_ref * (1.0 - lock_floor_pct)
                    new_trig = min(new_trig, lock_px)
            new_ord = min(old_ord, new_trig + tick * 2)
            sl_changed = new_trig < old_trig - tick * 0.5
            if sl_changed:
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
            if tp_oid and tp_follow > 0 and exit_side == 'Buy':
                step_tp = tick * tp_follow
                cand_tp = trough + step_tp
                if cand_tp < entry_ref - 1e-12 and cand_tp > 0:
                    new_tp = min(old_tp, cand_tp) if old_tp > 0 else cand_tp
                    if old_tp <= 0 or new_tp < old_tp - tick * 0.5:
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
