from __future__ import annotations

import logging
from typing import Any, Iterable, List, Tuple

from trader.automacoes.strategy_registry import get_evaluator
from trader.trading_system.contracts.context import ObservationContext

logger = logging.getLogger(__name__)


def run_strategy_observers(
    user: Any,
    ctx: ObservationContext,
    enabled_keys: Iterable[str],
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    seen = set()
    for key in enabled_keys:
        sk = (key or '').strip()
        if not sk or sk in seen:
            continue
        seen.add(sk)
        fn = get_evaluator(sk)
        if fn is None:
            continue
        try:
            msg = fn(ctx, user)
        except Exception:
            logger.exception('strategy_observer evaluate failed key=%s', sk)
            continue
        if msg and str(msg).strip():
            out.append((sk, str(msg).strip()))
    return out
