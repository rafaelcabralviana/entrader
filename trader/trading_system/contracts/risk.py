from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RiskVerdict:
    allowed: bool = True
    reason_codes: list[str] = field(default_factory=list)
    suggested_size: Optional[float] = None
