from __future__ import annotations

from typing import Any, Optional

from trader.automacoes.leafar_runner import run_leafar_for_context
from trader.automacoes.strategy_registry import register_celery_tick, register_evaluator
from trader.trading_system.contracts.context import ObservationContext


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    """
    Pensamentos e execução ficam no ``celery_tick`` (:func:`run_leafar_for_context`)
    para evitar duplicar detecção VP.
    """
    return None


register_evaluator('leafar', evaluate)
register_celery_tick('leafar', run_leafar_for_context)
