from __future__ import annotations

from datetime import time as dtime
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.utils import timezone

from trader.models import QuoteSnapshot, BookSnapshot

_BRT = ZoneInfo('America/Sao_Paulo')


def brt_save_window_allows_now() -> bool:
    """
    Quando ``TRADER_QUOTE_SAVE_BRT_WINDOW_ENABLED`` é True, só permite gravar
    entre **09:00 e 19:00** (BRT, inclusive nos extremos) de **segunda a sexta**.

    Com a flag False, permite qualquer horário (testes / coleta manual).
    """
    if not getattr(settings, 'TRADER_QUOTE_SAVE_BRT_WINDOW_ENABLED', True):
        return True
    now = timezone.now().astimezone(_BRT)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 0, 0) <= t <= dtime(19, 0, 0)


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
    if not brt_save_window_allows_now():
        return None
    if not isinstance(quote, dict):
        return None
    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    event_dt = _parse_quote_event_datetime(quote)
    latency_ms = compute_quote_latency_ms(quote)
    # Alinha ao ``simular_cotacoes_dia`` / ``quote_candles_json``: o pregão no BD usa ``captured_at``,
    # não só o instante em que o worker gravou a linha (evita dia «cortado» no replay vs. cotação).
    captured_at = event_dt if event_dt is not None else timezone.now()
    return QuoteSnapshot.objects.create(
        ticker=sym,
        captured_at=captured_at,
        quote_data=quote,
        quote_event_at=event_dt,
        latency_ms=latency_ms,
    )


def save_book_snapshot(ticker: str, book: Any) -> BookSnapshot | None:
    if not brt_save_window_allows_now():
        return None
    if not isinstance(book, dict):
        return None
    sym = (ticker or '').strip().upper()
    if not sym:
        return None
    return BookSnapshot.objects.create(
        ticker=sym,
        book_data=book,
    )
