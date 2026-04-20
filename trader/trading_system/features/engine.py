from __future__ import annotations

from typing import Any

from trader.trading_system.contracts.features import FeatureVector


class FeatureEngine:
    schema_version = 1

    def run(self, *, ticker: str, as_of_ts_ms: int, **kwargs: Any) -> FeatureVector:
        return FeatureVector(
            ticker=(ticker or '').strip().upper(),
            as_of_ts_ms=int(as_of_ts_ms),
            schema_version=self.schema_version,
            regime='',
            features={},
        )
