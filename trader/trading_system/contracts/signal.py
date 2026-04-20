from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class SignalDecision:
    prob_up: float = 0.0
    prob_down: float = 0.0
    expected_return: Optional[float] = None
    strategy_scores: dict[str, float] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
