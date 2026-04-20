from __future__ import annotations

from typing import Any

from django.utils import timezone

from trader.models import QuoteSnapshot, BookSnapshot


def _parse_quote_event_datetime(quote: dict[str, Any]) -> Any:
    raw = quote.get('dateTime') or quote.get('tradeDateTime')
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        dt = timezone.datetime.fromisoformat(text)
    except ValueError:
        return None
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def compute_quote_latency_ms(quote: Any) -> float | None:
    if not isinstance(quote, dict):
        return None
    event_dt = _parse_quote_event_datetime(quote)
    if event_dt is None:
        return None
    now_dt = timezone.now()
    latency = (now_dt - event_dt).total_seconds() * 1000.0
    return round(latency, 3)


def save_quote_snapshot(ticker: str, quote: Any) -> QuoteSnapshot | None:
    if not isinstance(quote, dict):
        return None
    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    event_dt = _parse_quote_event_datetime(quote)
    latency_ms = compute_quote_latency_ms(quote)
    return QuoteSnapshot.objects.create(
        ticker=sym,
        quote_data=quote,
        quote_event_at=event_dt,
        latency_ms=latency_ms,
    )


def save_book_snapshot(ticker: str, book: Any) -> BookSnapshot | None:
    if not isinstance(book, dict):
        return None
    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    return BookSnapshot.objects.create(
        ticker=sym,
        book_data=book,
    )
