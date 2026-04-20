from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FeatureVector:
    """Features derivadas num instante."""

    ticker: str
    as_of_ts_ms: int
    schema_version: int
    regime: str = ''
    features: dict[str, Any] = field(default_factory=dict)
