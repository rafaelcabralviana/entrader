from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from trader.models import BookSnapshot, QuoteSnapshot


def latest_quote_snapshot(ticker: str) -> Optional['QuoteSnapshot']:
    from trader.models import QuoteSnapshot

    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    return (
        QuoteSnapshot.objects.filter(ticker=sym)
        .order_by('-captured_at')
        .only('captured_at', 'quote_data', 'quote_event_at', 'latency_ms', 'ticker', 'id')
        .first()
    )


def latest_book_snapshot(ticker: str) -> Optional['BookSnapshot']:
    from trader.models import BookSnapshot

    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    return (
        BookSnapshot.objects.filter(ticker=sym)
        .order_by('-captured_at')
        .only('captured_at', 'book_data', 'ticker', 'id')
        .first()
    )


def quote_dict_from_row(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    q = getattr(row, 'quote_data', None)
    return q if isinstance(q, dict) else {}


def book_dict_from_row(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    b = getattr(row, 'book_data', None)
    return b if isinstance(b, dict) else {}
