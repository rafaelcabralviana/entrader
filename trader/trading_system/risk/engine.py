from __future__ import annotations

from trader.trading_system.contracts.risk import RiskVerdict
from trader.trading_system.contracts.signal import SignalDecision


class RiskEngine:
    def evaluate(self, signal: SignalDecision) -> RiskVerdict:
        return RiskVerdict(allowed=True, reason_codes=[], suggested_size=None)
