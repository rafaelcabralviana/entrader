"""Padrão único de entrada: ordem a mercado + proteção inicial via trailing."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable
import uuid

from trader.automacoes.execution_guard import (
    has_open_position_for_ticker,
    total_open_quantity_for_ticker,
)
from trader.automacoes.runtime import runtime_max_position_units
from trader.automacoes.universal_bracket_trailing import (
    BRACKET_LANE_REPLAY_SHADOW,
    BRACKET_LANE_STANDARD,
    save_bracket_state,
)
from trader.environment import get_current_environment, normalize_environment
from trader.models import Position
from trader.order_enums import ORDER_MODULE_DAY_TRADE, ORDER_TIF_DAY
from trader.services.orders import invalidate_intraday_orders_cache, post_send_market_order


def _order_id_from_response(resp: Any) -> str | None:
    if not isinstance(resp, dict):
        return None
    for key in ('Id', 'id', 'OrderId', 'orderId', 'ID'):
        v = resp.get(key)
        if v is not None and str(v).strip() != '':
            return str(v).strip()
    return None


def _round_px(x: float) -> float:
    return round(float(x), 6)


@dataclass
class TrailingEntryStageResult:
    ok: bool
    reason: str
    market_resp: dict[str, Any] | None = None
    market_order_id: str | None = None
    exit_side: str | None = None
    tp_price: float | None = None
    sl_trigger: float | None = None
    sl_order_price: float | None = None


def stage_market_entry_for_trailing(
    ticker: str,
    *,
    entry_side: str,
    last: float,
    take_profit: float,
    stop_loss: float,
    quantity: int,
    strategy_source: str,
    position_lane: str = Position.Lane.STANDARD,
    bracket_lane: str = BRACKET_LANE_STANDARD,
    market_sender: Callable[[dict[str, Any]], Any] | None = None,
    user: Any | None = None,
    trading_environment: str | None = None,
) -> TrailingEntryStageResult:
    sym = (ticker or '').strip().upper()
    if has_open_position_for_ticker(sym, position_lane=position_lane):
        return TrailingEntryStageResult(
            ok=False,
            reason=f'Bloqueado: já existe posição ativa em {sym} ({position_lane}).',
        )
    qty = max(1, int(quantity))
    uid = int(getattr(user, 'id', 0) or 0)
    if uid:
        env_u = normalize_environment(
            trading_environment if trading_environment is not None else get_current_environment()
        )
        cap_u = runtime_max_position_units(user, env_u)
        total_u = total_open_quantity_for_ticker(sym, position_lane=position_lane)
        if total_u + Decimal(qty) > Decimal(cap_u):
            return TrailingEntryStageResult(
                ok=False,
                reason=(
                    f'Bloqueado: posição em {sym} já soma {total_u} (teto {cap_u} p/ este limite). '
                    f'Nova entrada de {qty} ultrapassaria a cota.'
                ),
            )
    sender = market_sender or post_send_market_order
    body_in = {
        'Module': ORDER_MODULE_DAY_TRADE,
        'Ticker': sym,
        'Quantity': qty,
        'TimeInForce': ORDER_TIF_DAY,
        'Side': entry_side,
    }
    try:
        mresp = sender(body_in)
    except Exception as exc:
        return TrailingEntryStageResult(ok=False, reason=f'Falha entrada mercado: {exc}')
    mkt_resp = mresp if isinstance(mresp, dict) else {}
    mkt_id = _order_id_from_response(mkt_resp)
    if not mkt_id:
        return TrailingEntryStageResult(
            ok=False,
            reason='Falha entrada mercado: API não devolveu id de ordem.',
            market_resp=mkt_resp,
        )

    exit_side = 'Sell' if str(entry_side).strip() == 'Buy' else 'Buy'
    tp_price = _round_px(take_profit)
    tick = 0.01 if float(last) < 1000 else 0.05
    trig = _round_px(stop_loss)
    order_px = _round_px(trig - tick if exit_side == 'Sell' else trig + tick)
    save_bracket_state(
        sym,
        {
            'strategy_source': strategy_source,
            'execution_mode': 'api',
            'operation_id': mkt_id,
            'entry_side': entry_side,
            'exit_side': exit_side,
            'quantity': qty,
            'market_order_id': mkt_id,
            'tp_order_id': None,
            'sl_order_id': None,
            'sl_trigger': trig,
            'sl_order_price': order_px,
            'tp_price': tp_price,
            'last': _round_px(last),
            'entry_anchor': _round_px(last),
            'peak': _round_px(last) if str(entry_side).strip() == 'Buy' else None,
            'trough': _round_px(last) if str(entry_side).strip() == 'Sell' else None,
        },
        bracket_lane=bracket_lane,
    )
    try:
        from trader.panel_context import invalidate_collateral_custody_cache

        invalidate_collateral_custody_cache()
    except Exception:
        pass
    try:
        invalidate_intraday_orders_cache()
    except Exception:
        pass
    return TrailingEntryStageResult(
        ok=True,
        reason='Entrada enviada; trailing arma TP/SL.',
        market_resp=mkt_resp,
        market_order_id=mkt_id,
        exit_side=exit_side,
        tp_price=tp_price,
        sl_trigger=trig,
        sl_order_price=order_px,
    )


def stage_replay_entry_for_trailing(
    ticker: str,
    *,
    entry_side: str,
    last: float,
    take_profit: float,
    stop_loss: float,
    quantity: int,
    strategy_source: str,
    position_lane: str = Position.Lane.REPLAY_SHADOW,
    bracket_lane: str = BRACKET_LANE_REPLAY_SHADOW,
    user: Any | None = None,
    trading_environment: str | None = None,
) -> TrailingEntryStageResult:
    sym = (ticker or '').strip().upper()
    if has_open_position_for_ticker(sym, position_lane=position_lane):
        return TrailingEntryStageResult(
            ok=False,
            reason=f'Bloqueado: já existe posição ativa em {sym} ({position_lane}).',
        )
    qty = max(1, int(quantity))
    uid = int(getattr(user, 'id', 0) or 0)
    if uid:
        env_u = normalize_environment(
            trading_environment if trading_environment is not None else get_current_environment()
        )
        cap_u = runtime_max_position_units(user, env_u)
        total_u = total_open_quantity_for_ticker(sym, position_lane=position_lane)
        if total_u + Decimal(qty) > Decimal(cap_u):
            return TrailingEntryStageResult(
                ok=False,
                reason=(
                    f'Bloqueado (replay): posição em {sym} já soma {total_u} (teto {cap_u}).'
                ),
            )
    mkt_id = f'replay-shadow:mkt:{uuid.uuid4().hex[:14]}'
    exit_side = 'Sell' if str(entry_side).strip() == 'Buy' else 'Buy'
    tp_price = _round_px(take_profit)
    tick = 0.01 if float(last) < 1000 else 0.05
    trig = _round_px(stop_loss)
    order_px = _round_px(trig - tick if exit_side == 'Sell' else trig + tick)
    save_bracket_state(
        sym,
        {
            'strategy_source': strategy_source,
            'execution_mode': 'replay_shadow',
            'operation_id': mkt_id,
            'entry_side': entry_side,
            'exit_side': exit_side,
            'quantity': qty,
            'market_order_id': mkt_id,
            'tp_order_id': None,
            'sl_order_id': None,
            'sl_trigger': trig,
            'sl_order_price': order_px,
            'tp_price': tp_price,
            'last': _round_px(last),
            'entry_anchor': _round_px(last),
            'peak': _round_px(last) if str(entry_side).strip() == 'Buy' else None,
            'trough': _round_px(last) if str(entry_side).strip() == 'Sell' else None,
        },
        bracket_lane=bracket_lane,
    )
    return TrailingEntryStageResult(
        ok=True,
        reason='Entrada replay criada; trailing replay arma TP/SL.',
        market_resp={'Id': mkt_id},
        market_order_id=mkt_id,
        exit_side=exit_side,
        tp_price=tp_price,
        sl_trigger=trig,
        sl_order_price=order_px,
    )

