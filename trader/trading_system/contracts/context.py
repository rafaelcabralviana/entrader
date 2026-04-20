from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

ObservationMode = Literal['live', 'session_day', 'replay_frame']
DataSource = Literal['live_tail', 'session_replay']


@dataclass
class ObservationContext:
    """
    Contexto único para avaliar estratégias no Celery.

    Tudo deriva de ``QuoteSnapshot`` / ``BookSnapshot`` no banco: ``live_tail`` é a cauda
    recente; ``session_replay`` é o mesmo dia (com ``replay_until`` opcional), como no gráfico.
    """

    mode: ObservationMode
    ticker: str
    trading_environment: str
    captured_at: datetime | None
    quote: dict[str, Any] = field(default_factory=dict)
    book: dict[str, Any] = field(default_factory=dict)
    session_date_iso: str | None = None
    replay_until_iso: str | None = None
    market_sim_effective: bool = False
    data_source: DataSource | None = None
    extra: dict[str, Any] = field(default_factory=dict)
