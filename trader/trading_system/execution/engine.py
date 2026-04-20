from __future__ import annotations

from trader.trading_system.contracts.risk import RiskVerdict
from trader.trading_system.contracts.signal import SignalDecision


class ExecutionEngine:
    def plan(self, signal: SignalDecision, risk: RiskVerdict) -> dict:
        return {'would_execute': False, 'reason': 'stub'}
