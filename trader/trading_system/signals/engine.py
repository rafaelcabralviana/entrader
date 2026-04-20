from __future__ import annotations

from trader.trading_system.contracts.features import FeatureVector
from trader.trading_system.contracts.signal import SignalDecision


class SignalEngine:
    def score(self, feature_vector: FeatureVector) -> SignalDecision:
        return SignalDecision()
