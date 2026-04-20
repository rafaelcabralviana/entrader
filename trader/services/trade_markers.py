from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from django.utils import timezone

from trader.models import TradeMarker


def _norm_side(side: str) -> str:
    s = str(side or '').strip().upper()
    if s in ('BUY', 'COMPRA'):
        return TradeMarker.Side.BUY
    if s in ('SELL', 'VENDA'):
        return TradeMarker.Side.SELL
    return s


def record_trade_marker(
    *,
    ticker: str,
    side: str,
    quantity: int | float | Decimal,
    price: int | float | Decimal | None = None,
    marker_at: datetime | None = None,
    source: str = '',
    metadata: dict[str, Any] | None = None,
) -> TradeMarker:
    when = marker_at or timezone.now()
    return TradeMarker.objects.create(
        ticker=(ticker or '').strip().upper(),
        side=_norm_side(side),
        quantity=Decimal(str(quantity)),
        price=(Decimal(str(price)) if price is not None else None),
        marker_at=when,
        source=(source or '').strip(),
        metadata=metadata or {},
    )
