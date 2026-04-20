from __future__ import annotations

from typing import Any, Optional

from trader.automacoes.strategy_registry import register_evaluator
from trader.trading_system.contracts.context import ObservationContext


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    return None


register_evaluator('ts_signals_stub', evaluate)
