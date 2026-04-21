"""Bracket + envio de ordens para estratégia ativa de tendência (trailing centralizado)."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from django.utils import timezone as dj_tz

from trader.automacoes.execution_guard import release_market_entry_lock, try_acquire_market_entry_lock
from trader.automacoes.universal_bracket_trailing import BRACKET_LANE_REPLAY_SHADOW, state_cache_key
from trader.automacoes.market_entry_trailing import (
    stage_market_entry_for_trailing,
    stage_replay_entry_for_trailing,
)
from trader.automacoes.order_limits import clamp_quantity_to_ticket_limit
from trader.custody_simulator import record_bracket_execution_marker
from trader.environment import (
    ENV_REPLAY,
    ENV_SIMULATOR,
    get_current_environment,
    normalize_environment,
    order_api_mode_label,
)
from trader.models import Position
from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)
from trader.services.orders import (
    invalidate_intraday_orders_cache,
    post_send_market_order,
)

logger = logging.getLogger(__name__)


def _round_px(x: float) -> float:
    return round(float(x), 6)


def execute_trend_ativa_bracket(
    ticker: str,
    *,
    side: str,
    last: float,
    take_profit: float,
    stop_loss: float,
    quantity: int = 1,
    log_user: Any | None = None,
    log_environment: str | None = None,
    log_execution_profile: Any | None = None,
    log_session_label: str | None = None,
) -> bool:
    """Entrada a mercado; trailing central arma e gere TP/SL.

    Com ``log_user`` + ``log_environment`` grava linha **NOTICE** no painel de automações (ids API).
    """
    sym = (ticker or '').strip().upper()
    qty = clamp_quantity_to_ticket_limit(sym, max(1, int(quantity)))
    env_cur = get_current_environment()
    uid = int(getattr(log_user, 'id', 0) or 0)
    locked = False
    if uid:
        locked = try_acquire_market_entry_lock(
            user_id=uid,
            ticker=sym,
            trading_environment=env_cur,
            position_lane=Position.Lane.STANDARD,
        )
        if not locked:
            logger.warning('trend_ativa lock busy %s', sym)
            return False
    try:
        staged = stage_market_entry_for_trailing(
            sym,
            entry_side=side,
            last=float(last),
            take_profit=float(take_profit),
            stop_loss=float(stop_loss),
            quantity=qty,
            strategy_source='tendencia_mercado_ativa',
            position_lane=Position.Lane.STANDARD,
            market_sender=post_send_market_order,
            user=log_user,
            execution_profile=log_execution_profile,
            trading_environment=env_cur,
        )
        if not staged.ok:
            logger.warning('trend_ativa market %s', staged.reason)
            return False
        mkt_resp = staged.market_resp if isinstance(staged.market_resp, dict) else {}
        mkt_id = str(staged.market_order_id or '').strip()
        tp_price = _round_px(float(staged.tp_price or 0.0))
        trig = _round_px(float(staged.sl_trigger or 0.0))

        if log_user is not None and (log_environment or '').strip():
            try:
                from trader.automacoes.thoughts import record_automation_thought
                from trader.models import AutomationThought

                lbl = (log_session_label or '').strip() or '—'
                api_lbl = order_api_mode_label()
                record_automation_thought(
                    log_user,
                    log_environment,
                    (
                        f'Ordens enviadas [API {api_lbl} · tendência ativa · {lbl} · {sym}] '
                        f'{side} qtd={qty} | mercado id={mkt_id} | proteção inicial via trailing '
                        f'(TP≈{tp_price} | SL gatilho≈{trig})'
                    )[:3900],
                    source='tendencia_mercado_ativa',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=log_execution_profile,
                )
            except Exception:
                logger.exception('trend_ativa thought order sent panel')

        if get_current_environment() in (ENV_SIMULATOR, ENV_REPLAY):
            try:
                record_bracket_execution_marker(
                    ticker=sym,
                    side=side,
                    quantity=qty,
                    last=last,
                    strategy_source='tendencia_mercado_ativa',
                    log_session_label=log_session_label,
                    market_order_id=mkt_id,
                )
            except Exception:
                logger.exception('trend_ativa custody marker')

        try:
            from trader.panel_context import invalidate_collateral_custody_cache

            invalidate_collateral_custody_cache()
        except Exception:
            logger.exception('trend_ativa invalidate custody cache')
        try:
            invalidate_intraday_orders_cache()
        except Exception:
            logger.exception('trend_ativa invalidate intraday orders cache')

        if should_record_local_history('market', mkt_resp):
            try:
                exec_px = staged.executed_price
                hist = infer_execution_price(
                    {'Ticker': sym, 'Side': side, 'Quantity': qty},
                    mkt_resp,
                )
                if exec_px is not None:
                    hist = Decimal(str(exec_px))
                register_trade_execution(
                    ticker=sym,
                    side=side,
                    quantity=qty,
                    price=hist,
                    source='tendencia_mercado_ativa',
                    trading_environment=get_current_environment(),
                    position_lane=Position.Lane.STANDARD,
                )
                try:
                    from trader.models import AutomationTriggerMarker

                    if log_user is not None:
                        AutomationTriggerMarker.objects.create(
                            user=log_user,
                            execution_profile=log_execution_profile,
                            trading_environment=env_cur,
                            ticker=sym,
                            strategy_key='trade_entry',
                            marker_at=dj_tz.now(),
                            price=hist,
                            message=f'source=tendencia_mercado_ativa;side={side};qty={qty};market_id={mkt_id}'[:500],
                        )
                except Exception:
                    logger.exception('trend_ativa entry marker create')
            except Exception:
                logger.exception('trend_ativa register_trade_execution')

        return True
    finally:
        if locked and uid:
            release_market_entry_lock(
                user_id=uid,
                ticker=sym,
                trading_environment=env_cur,
                position_lane=Position.Lane.STANDARD,
            )


def execute_trend_ativa_bracket_replay_shadow(
    ticker: str,
    *,
    side: str,
    last: float,
    take_profit: float,
    stop_loss: float,
    quantity: int = 1,
    log_user: Any | None = None,
    log_environment: str | None = None,
    log_execution_profile: Any | None = None,
    log_session_label: str | None = None,
) -> bool:
    """
    Bracket só em memória + ledger ``replay_shadow``: preço de execução = fecho da vela do replay.

    Não chama a corretora (a API não aceita preço fictício alinhado ao dia histórico).
    """
    sym = (ticker or '').strip().upper()
    qty = clamp_quantity_to_ticket_limit(sym, max(1, int(quantity)))
    env_n = normalize_environment(log_environment) if log_environment else get_current_environment()
    uid = int(getattr(log_user, 'id', 0) or 0)
    locked = False
    if uid:
        locked = try_acquire_market_entry_lock(
            user_id=uid,
            ticker=sym,
            trading_environment=env_n,
            position_lane=Position.Lane.REPLAY_SHADOW,
        )
        if not locked:
            logger.warning('trend_ativa replay lock busy %s', sym)
            return False
    try:
        staged = stage_replay_entry_for_trailing(
            sym,
            entry_side=side,
            last=float(last),
            take_profit=float(take_profit),
            stop_loss=float(stop_loss),
            quantity=qty,
            strategy_source='tendencia_mercado_ativa',
            position_lane=Position.Lane.REPLAY_SHADOW,
            user=log_user,
            execution_profile=log_execution_profile,
            trading_environment=env_n,
        )
        if not staged.ok:
            logger.warning('trend_ativa replay %s', staged.reason)
            return False
        mkt_id = str(staged.market_order_id or '').strip()
        tp_price = _round_px(float(staged.tp_price or 0.0))
        trig = _round_px(float(staged.sl_trigger or 0.0))

        if log_user is not None and (log_environment or '').strip():
            try:
                from trader.automacoes.thoughts import record_automation_thought
                from trader.models import AutomationThought

                lbl = (log_session_label or '').strip() or '—'
                record_automation_thought(
                    log_user,
                    log_environment,
                    (
                        f'Bracket simulado (replay · tendência ativa · {lbl} · {sym}) '
                        f'{side} qtd={qty} | preço ref. vela={_round_px(last)} | TP≈{tp_price} | '
                        f'SL gatilho≈{trig} — sem envio à API'
                    )[:3900],
                    source='tendencia_mercado_ativa',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=log_execution_profile,
                )
            except Exception:
                logger.exception('trend_ativa replay shadow thought')

        try:
            register_trade_execution(
                ticker=sym,
                side=side,
                quantity=qty,
                price=Decimal(str(staged.executed_price or _round_px(last))),
                source='tendencia_mercado_ativa_replay',
                trading_environment=env_n,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
            try:
                from trader.models import AutomationTriggerMarker

                if log_user is not None:
                    AutomationTriggerMarker.objects.create(
                        user=log_user,
                        execution_profile=log_execution_profile,
                        trading_environment=env_n,
                        ticker=sym,
                        strategy_key='trade_entry',
                        marker_at=dj_tz.now(),
                        price=Decimal(str(staged.executed_price or _round_px(last))),
                        message=f'source=tendencia_mercado_ativa_replay;side={side};qty={qty};market_id={mkt_id}'[:500],
                    )
            except Exception:
                logger.exception('trend_ativa replay entry marker create')
            try:
                from trader.panel_context import invalidate_collateral_custody_cache

                invalidate_collateral_custody_cache()
            except Exception:
                logger.exception('trend_ativa invalidate custody cache replay')
        except Exception:
            logger.exception('trend_ativa register_trade_execution replay')
            try:
                from django.core.cache import cache

                cache.delete(
                    state_cache_key(
                        sym,
                        bracket_lane=BRACKET_LANE_REPLAY_SHADOW,
                        trading_environment=env_n,
                    )
                )
            except Exception:
                logger.exception('trend_ativa rollback trailing state replay')
            return False
        if get_current_environment() in (ENV_SIMULATOR, ENV_REPLAY):
            try:
                record_bracket_execution_marker(
                    ticker=sym,
                    side=side,
                    quantity=qty,
                    last=last,
                    strategy_source='tendencia_mercado_ativa',
                    log_session_label=log_session_label,
                    market_order_id=mkt_id,
                )
            except Exception:
                logger.exception('trend_ativa custody marker replay')

        return True
    finally:
        if locked and uid:
            release_market_entry_lock(
                user_id=uid,
                ticker=sym,
                trading_environment=env_n,
                position_lane=Position.Lane.REPLAY_SHADOW,
            )
