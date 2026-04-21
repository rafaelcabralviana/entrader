"""Envio de ordens alinhado à boleta (DayTrade) para sinais leafaR."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from django.utils import timezone as dj_tz

from trader.automacoes.market_entry_trailing import (
    stage_market_entry_for_trailing,
    stage_replay_entry_for_trailing,
)
from trader.automacoes.order_limits import clamp_quantity_to_ticket_limit
from trader.automacoes.thoughts import record_automation_thought
from trader.custody_simulator import record_bracket_execution_marker
from trader.environment import ENV_REPLAY, ENV_SIMULATOR, get_current_environment, normalize_environment
from trader.automacoes.execution_guard import release_market_entry_lock, try_acquire_market_entry_lock
from trader.automacoes.universal_bracket_trailing import BRACKET_LANE_REPLAY_SHADOW, state_cache_key
from trader.models import AutomationThought, Position
from trader.order_enums import (ORDER_SIDE_BUY, ORDER_SIDE_SELL)
from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)
from trader.services.orders import (
    invalidate_intraday_orders_cache,
    post_send_market_order,
)

from trader.automacoes.leafar_vp import LeafarSignal

logger = logging.getLogger(__name__)


def _round_px(x: float) -> float:
    return round(float(x), 6)


def _normalize_order_side(side: Any) -> str:
    s = str(side or '').strip().lower()
    if s in ('buy', 'compra'):
        return ORDER_SIDE_BUY
    if s in ('sell', 'venda'):
        return ORDER_SIDE_SELL
    raise ValueError(f'lado inválido para envio de ordem: {side!r}')


def _send_market_with_retry(body_in: dict[str, Any], *, tries: int = 2) -> Any:
    attempts = max(1, int(tries))
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return post_send_market_order(body_in)
        except Exception as exc:
            last_exc = exc
            if i + 1 >= attempts:
                break
            time.sleep(0.25)
    assert last_exc is not None
    raise last_exc


@dataclass
class LeafarExecutionResult:
    ok: bool
    messages: list[str] = field(default_factory=list)
    market_resp: dict[str, Any] | None = None
    tp_order_id: str | None = None
    sl_order_id: str | None = None


def execute_leafar_bracket(
    ticker: str,
    signal: LeafarSignal,
    *,
    quantity: int = 1,
    user: Any | None = None,
    execution_profile: Any | None = None,
) -> LeafarExecutionResult:
    """
    Entrada a mercado; proteção inicial (TP/SL) fica a cargo do trailing central.

    Em falhas parciais devolve ``ok=False`` com mensagens acumuladas.
    """
    sym = (ticker or '').strip().upper()
    out = LeafarExecutionResult(ok=False, messages=[])
    qty = clamp_quantity_to_ticket_limit(sym, max(1, int(quantity)))
    entry_side = _normalize_order_side(signal.side)
    env_cur = get_current_environment()
    uid = int(getattr(user, 'id', 0) or 0)
    locked = False
    if uid:
        locked = try_acquire_market_entry_lock(
            user_id=uid,
            ticker=sym,
            trading_environment=env_cur,
            position_lane=Position.Lane.STANDARD,
        )
        if not locked:
            out.messages.append(
                'Bloqueado: outra entrada automática está em processamento neste ticker (anti-corrida).'
            )
            return out
    try:
        staged = stage_market_entry_for_trailing(
            sym,
            entry_side=entry_side,
            last=float(signal.last),
            take_profit=float(signal.take_profit),
            stop_loss=float(signal.stop_loss),
            quantity=qty,
            strategy_source='leafar',
            position_lane=Position.Lane.STANDARD,
            market_sender=lambda body: _send_market_with_retry(body, tries=2),
            user=user,
            execution_profile=execution_profile,
            trading_environment=env_cur,
        )
        out.market_resp = staged.market_resp if isinstance(staged.market_resp, dict) else {}
        if not staged.ok:
            out.messages.append(staged.reason)
            logger.warning('leafar_execution market %s', staged.reason)
            return out

        mkt_id = str(staged.market_order_id or '').strip()
        tp_price = _round_px(float(staged.tp_price or 0.0))
        trig = _round_px(float(staged.sl_trigger or 0.0))
        out.ok = True
        out.messages.append(f'Entrada mercado {entry_side}: {out.market_resp!s}'[:900])
        out.messages.append(
            f'Entrada enviada (id={mkt_id}); trailing vai armar TP/SL iniciais (TP≈{tp_price}, SL≈{trig}).'
        )

        try:
            from trader.panel_context import invalidate_collateral_custody_cache

            invalidate_collateral_custody_cache()
        except Exception:
            logger.exception('leafar invalidate custody cache')
        try:
            invalidate_intraday_orders_cache()
        except Exception:
            logger.exception('leafar invalidate intraday orders cache')

        if should_record_local_history('market', out.market_resp or {}):
            try:
                exec_px = staged.executed_price
                hist = infer_execution_price(
                    {'Ticker': sym, 'Side': entry_side, 'Quantity': qty},
                    out.market_resp or {},
                )
                if exec_px is not None:
                    hist = Decimal(str(exec_px))
                register_trade_execution(
                    ticker=sym,
                    side=entry_side,
                    quantity=qty,
                    price=hist,
                    source='leafar',
                    trading_environment=get_current_environment(),
                    position_lane=Position.Lane.STANDARD,
                )
                try:
                    from trader.models import AutomationTriggerMarker

                    if user is not None:
                        AutomationTriggerMarker.objects.create(
                            user=user,
                            execution_profile=execution_profile,
                            trading_environment=env_cur,
                            ticker=sym,
                            strategy_key='trade_entry',
                            marker_at=dj_tz.now(),
                            price=hist,
                            message=f'source=leafar;side={entry_side};qty={qty};market_id={mkt_id}'[:500],
                        )
                except Exception:
                    logger.exception('leafar entry marker create')
            except Exception:
                logger.exception('leafar register_trade_execution')

        return out
    finally:
        if locked and uid:
            release_market_entry_lock(
                user_id=uid,
                ticker=sym,
                trading_environment=env_cur,
                position_lane=Position.Lane.STANDARD,
            )


def execute_leafar_bracket_replay_shadow(
    ticker: str,
    signal: LeafarSignal,
    *,
    quantity: int = 1,
    data_label: str,
    log_user: Any,
    env: str,
    execution_profile: Any | None = None,
) -> LeafarExecutionResult:
    """
    Bracket fictício no replay: preço de entrada = ``signal.last`` (fecho da vela reproduzida).
    """
    sym = (ticker or '').strip().upper()
    qty = clamp_quantity_to_ticket_limit(sym, max(1, int(quantity)))
    entry_side = _normalize_order_side(signal.side)
    env_norm = normalize_environment(env)
    uid = int(getattr(log_user, 'id', 0) or 0)
    locked = False
    if uid:
        locked = try_acquire_market_entry_lock(
            user_id=uid,
            ticker=sym,
            trading_environment=env_norm,
            position_lane=Position.Lane.REPLAY_SHADOW,
        )
        if not locked:
            return LeafarExecutionResult(
                ok=False,
                messages=['Bloqueado: replay deste ticker em processamento (anti-corrida).'],
            )
    try:
        staged = stage_replay_entry_for_trailing(
            sym,
            entry_side=entry_side,
            last=float(signal.last),
            take_profit=float(signal.take_profit),
            stop_loss=float(signal.stop_loss),
            quantity=qty,
            strategy_source='leafar',
            position_lane=Position.Lane.REPLAY_SHADOW,
            user=log_user,
            execution_profile=execution_profile,
            trading_environment=env_norm,
        )
        if not staged.ok:
            return LeafarExecutionResult(ok=False, messages=[staged.reason])
        mkt_id = str(staged.market_order_id or '').strip()
        tp_price = _round_px(float(staged.tp_price or 0.0))
        trig = _round_px(float(staged.sl_trigger or 0.0))

        out = LeafarExecutionResult(
            ok=True,
            messages=[
                f'Entrada replay (sem API) @ {_round_px(signal.last)}; trailing replay vai armar TP/SL (TP≈{tp_price} SL≈{trig})'
            ],
            market_resp={'Id': mkt_id},
            tp_order_id=None,
            sl_order_id=None,
        )

        if log_user is not None:
            try:
                record_automation_thought(
                    log_user,
                    env,
                    (
                        f'leafaR bracket simulado (replay · {data_label} · {sym}) '
                        f'{entry_side} qtd={qty} | preço ref. vela={_round_px(signal.last)} — sem API'
                    )[:3900],
                    source='leafar',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=execution_profile,
                )
            except Exception:
                logger.exception('leafar replay shadow thought')

        try:
            register_trade_execution(
                ticker=sym,
                side=entry_side,
                quantity=qty,
                price=Decimal(str(staged.executed_price or _round_px(signal.last))),
                source='leafar_replay',
                trading_environment=env_norm,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
            try:
                from trader.models import AutomationTriggerMarker

                if log_user is not None:
                    AutomationTriggerMarker.objects.create(
                        user=log_user,
                        execution_profile=execution_profile,
                        trading_environment=env_norm,
                        ticker=sym,
                        strategy_key='trade_entry',
                        marker_at=dj_tz.now(),
                        price=Decimal(str(staged.executed_price or _round_px(signal.last))),
                        message=f'source=leafar_replay;side={entry_side};qty={qty};market_id={mkt_id}'[:500],
                    )
            except Exception:
                logger.exception('leafar replay entry marker create')
            try:
                from trader.panel_context import invalidate_collateral_custody_cache

                invalidate_collateral_custody_cache()
            except Exception:
                logger.exception('leafar invalidate custody cache replay')
        except Exception:
            logger.exception('leafar register_trade_execution replay')
            try:
                from django.core.cache import cache

                cache.delete(
                    state_cache_key(
                        sym,
                        bracket_lane=BRACKET_LANE_REPLAY_SHADOW,
                        trading_environment=env_norm,
                    )
                )
            except Exception:
                logger.exception('leafar rollback trailing state replay')
            return LeafarExecutionResult(
                ok=False,
                messages=['Falha ao registrar execução replay; estado do trailing foi resetado.'],
            )
        if get_current_environment() in (ENV_SIMULATOR, ENV_REPLAY):
            try:
                record_bracket_execution_marker(
                    ticker=sym,
                    side=entry_side,
                    quantity=qty,
                    last=float(signal.last),
                    strategy_source='leafar',
                    log_session_label=data_label,
                    market_order_id=mkt_id,
                )
            except Exception:
                logger.exception('leafar custody marker replay')

        return out
    finally:
        if locked and uid:
            release_market_entry_lock(
                user_id=uid,
                ticker=sym,
                trading_environment=env_norm,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
