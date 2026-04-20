from __future__ import annotations

from typing import Any, Optional

from trader.automacoes.strategy_registry import register_evaluator
from trader.trading_system.contracts.context import ObservationContext


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    """
    O motor de automação já envia ``ObservationContext`` com ``live_tail`` ou
    ``session_replay``; a lógica de % sobre book/último pode usar ``ctx.quote`` / ``ctx.book``.
    """
    return None


register_evaluator('stop_percentual_book', evaluate)
