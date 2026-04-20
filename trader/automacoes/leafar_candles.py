"""
Agrega ``QuoteSnapshot`` em candles OHLCV (leafaR / Celery).

**Fonte única de dados:** tudo o que o Celery grava no banco (tempo real) e o que
a UI mostra são leituras dessas mesmas linhas. A «simulação» de pregão é o mesmo
conjunto de snapshots, filtrado por dia e, no replay, por ``replay_until``.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.utils.dateparse import parse_datetime
from django.utils import timezone as dj_tz

from trader.models import QuoteSnapshot

_TZ_SP = ZoneInfo('America/Sao_Paulo')


def calendar_date_brt(now: datetime | None = None) -> date:
    """Data civil em ``America/Sao_Paulo`` (pregão B3 / referência do motor)."""
    if now is None:
        now = dj_tz.now()
    if dj_tz.is_naive(now):
        now = dj_tz.make_aware(now, dj_tz.get_current_timezone())
    return now.astimezone(_TZ_SP).date()


def parse_replay_until_iso(raw: str | None) -> datetime | None:
    """ISO-8601 com fuso; naive interpretado em America/Sao_Paulo (alinhado a ``quote_candles_json``)."""
    s = (raw or '').strip()
    if not s:
        return None
    dt = parse_datetime(s)
    if dt is None:
        return None
    if dj_tz.is_naive(dt):
        dt = dj_tz.make_aware(dt, _TZ_SP)
    return dt


def _quote_last_price_for_candle(q: dict[str, Any]) -> float | None:
    if not isinstance(q, dict):
        return None
    for key in ('lastPrice', 'LastPrice', 'last_price', 'close', 'Close'):
        v = q.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def load_recent_candles(
    ticker: str,
    *,
    interval_sec: int = 10,
    max_candles: int = 160,
    max_rows: int = 12_000,
) -> list[dict[str, Any]]:
    """
    Cauda cronológica recente de ``QuoteSnapshot`` (últimos instantes gravados pelo watch).

    Equivale ao modo «últimos dados» do gráfico: mesma tabela, janela móvel no tempo.
    """
    sym = (ticker or '').strip().upper()
    if not sym or interval_sec < 1:
        return []
    limit = min(500, max(20, max_candles))
    max_rows = min(20_000, max(2_000, max_rows))

    rows = list(
        QuoteSnapshot.objects.filter(ticker__iexact=sym)
        .order_by('-captured_at')
        .values('captured_at', 'quote_data')[:max_rows]
    )
    rows.reverse()

    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        q = row.get('quote_data') or {}
        if not isinstance(q, dict):
            continue
        last_price = _quote_last_price_for_candle(q)
        if last_price is None:
            continue
        raw_qty = q.get('lastQuantity')
        try:
            qty = float(raw_qty) if raw_qty is not None else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        captured_at = row.get('captured_at')
        if captured_at is None:
            continue
        ts = int(captured_at.timestamp())
        bucket = (ts // interval_sec) * interval_sec
        buckets[bucket].append(
            {
                'captured_at': captured_at,
                'last_price': last_price,
                'last_quantity': qty,
            }
        )

    candles: list[dict[str, Any]] = []
    for bkey in sorted(buckets.keys()):
        points = buckets[bkey]
        if not points:
            continue
        opens = points[0]['last_price']
        closes = points[-1]['last_price']
        highs = max(p['last_price'] for p in points)
        lows = min(p['last_price'] for p in points)
        vol = sum(p['last_quantity'] for p in points)
        candle_dt = datetime.fromtimestamp(bkey, tz=_TZ_SP)
        candles.append(
            {
                'bucket_start': candle_dt.isoformat(),
                'open': round(opens, 6),
                'high': round(highs, 6),
                'low': round(lows, 6),
                'close': round(closes, 6),
                'volume': round(vol, 6),
            }
        )

    if len(candles) > limit:
        candles = candles[-limit:]
    return candles


def load_session_day_candles(
    ticker: str,
    session_day: date,
    *,
    interval_sec: int = 10,
    max_candles: int = 200,
    max_rows: int = 24_000,
    replay_until: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Candles a partir de ``QuoteSnapshot`` num único dia civil BRT.

    Percorre **todos** os snapshots do intervalo (``iterator``) — não corta o fim do dia
    com ``[:max_rows]`` (erro que fazia o replay «perder» o instante actual quando havia
    muitas linhas de manhã). ``max_candles`` / ``max_rows`` ficam só por compatibilidade;
    a agregação usa o conjunto completo até ``replay_until``.
    """
    _ = max_candles, max_rows  # parâmetros mantidos por compatibilidade com chamadas antigas

    sym = (ticker or '').strip().upper()
    if not sym or interval_sec < 1 or session_day is None:
        return []

    day_start = datetime.combine(session_day, dtime.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)

    qs = QuoteSnapshot.objects.filter(
        ticker__iexact=sym,
        captured_at__gte=day_start,
        captured_at__lt=day_end,
    )
    if replay_until is not None:
        qs = qs.filter(captured_at__lte=replay_until)

    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in qs.order_by('captured_at').values('captured_at', 'quote_data').iterator(chunk_size=2048):
        q = row.get('quote_data') or {}
        if not isinstance(q, dict):
            continue
        last_price = _quote_last_price_for_candle(q)
        if last_price is None:
            continue
        raw_qty = q.get('lastQuantity')
        try:
            qty = float(raw_qty) if raw_qty is not None else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        captured_at = row.get('captured_at')
        if captured_at is None:
            continue
        ts = int(captured_at.timestamp())
        bucket = (ts // interval_sec) * interval_sec
        buckets[bucket].append(
            {
                'captured_at': captured_at,
                'last_price': last_price,
                'last_quantity': qty,
            }
        )

    candles: list[dict[str, Any]] = []
    for bkey in sorted(buckets.keys()):
        points = buckets[bkey]
        if not points:
            continue
        opens = points[0]['last_price']
        closes = points[-1]['last_price']
        highs = max(p['last_price'] for p in points)
        lows = min(p['last_price'] for p in points)
        vol = sum(p['last_quantity'] for p in points)
        candle_dt = datetime.fromtimestamp(bkey, tz=_TZ_SP)
        candles.append(
            {
                'bucket_start': candle_dt.isoformat(),
                'open': round(opens, 6),
                'high': round(highs, 6),
                'low': round(lows, 6),
                'close': round(closes, 6),
                'volume': round(vol, 6),
            }
        )

    return candles
