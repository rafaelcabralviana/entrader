"""
Registo de avaliadores por ``strategy_key`` (plugins em ``strategies_plugins``).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from trader.trading_system.contracts.context import ObservationContext

logger = logging.getLogger(__name__)

StrategyEvaluateFn = Callable[[ObservationContext, Any], Optional[str]]
StrategyCeleryTickFn = Callable[[ObservationContext, Any, str], None]

_EVALUATORS: dict[str, StrategyEvaluateFn] = {}
_CELERY_TICKS: dict[str, StrategyCeleryTickFn] = {}


def register_evaluator(strategy_key: str, fn: StrategyEvaluateFn) -> None:
    k = (strategy_key or '').strip()
    if not k:
        return
    _EVALUATORS[k] = fn
    logger.debug('strategy_registry: registered %s', k)


def get_evaluator(strategy_key: str) -> Optional[StrategyEvaluateFn]:
    return _EVALUATORS.get((strategy_key or '').strip())


def registered_keys() -> frozenset[str]:
    return frozenset(_EVALUATORS.keys())


def register_celery_tick(strategy_key: str, fn: StrategyCeleryTickFn) -> None:
    """Hook após ``collect_watch_quotes`` (efeitos laterais: ordens, trailing, etc.)."""
    k = (strategy_key or '').strip()
    if not k:
        return
    _CELERY_TICKS[k] = fn
    logger.debug('strategy_registry: celery_tick %s', k)


def get_celery_tick(strategy_key: str) -> Optional[StrategyCeleryTickFn]:
    return _CELERY_TICKS.get((strategy_key or '').strip())


def registered_celery_tick_keys() -> frozenset[str]:
    return frozenset(_CELERY_TICKS.keys())
