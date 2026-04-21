"""
Montagem de frames de replay (quote + livro alinhado no tempo) a partir de snapshots do banco.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, time as time_cls
from typing import Any
from zoneinfo import ZoneInfo

from django.core.cache import cache
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from trader.models import BookSnapshot, QuoteSnapshot
from trader.panel_context import (
    json_sanitize,
    normalize_book_levels,
    ohlc_bar_chart_payload,
)
from trader.smart_trader_limits import (
    daily_order_limit_for_ticker,
    extract_bmf_base,
    ticket_limit_for_ticker,
)

_MAX_FRAMES = 25_000
_TZ_SP = ZoneInfo('America/Sao_Paulo')
# Paginação: evita um único JSON gigante; livros do dia ficam em cache curto entre chunks.
_REPLAY_BOOKS_CACHE_KEY = 'automation_replay_books:v1:{sym}:{day}'
_REPLAY_QCOUNT_CACHE_KEY = 'automation_replay_qcount:v1:{sym}:{day}'
_REPLAY_ALIGN_OFF_CACHE_KEY = 'automation_replay_align_off:v1:{sym}:{day}:{align_key}'
# TTL maior: replay reutiliza os mesmos dados em sequência (menos COUNT/list no BD).
_REPLAY_BOOKS_CACHE_SEC = 300
DEFAULT_REPLAY_CHUNK = 1500
MAX_REPLAY_CHUNK = 2500


def _captured_as_sp(dt: datetime) -> datetime:
    """Normaliza instante para America/Sao_Paulo (comparação estável quote vs book)."""
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, _TZ_SP)
    return dt.astimezone(_TZ_SP)


def _operation_hints(sym: str) -> dict[str, Any]:
    bmf_base = extract_bmf_base(sym)
    return {
        'mercado': 'BMF' if bmf_base else 'BOVESPA',
        'base_bmf': bmf_base or '—',
        'limite_ordens_dia': daily_order_limit_for_ticker(sym),
        'limite_boleta': ticket_limit_for_ticker(sym),
    }


def _books_for_replay_day(sym: str, session_day: date) -> list[dict[str, Any]]:
    """Livros do pregão (BRT) em ordem; reuso entre requests via cache para chunks seguintes."""
    cache_key = _REPLAY_BOOKS_CACHE_KEY.format(sym=sym, day=session_day.isoformat())
    hit = cache.get(cache_key)
    if isinstance(hit, list):
        return hit
    day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)
    books = list(
        BookSnapshot.objects.filter(
            ticker=sym,
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        )
        .order_by('captured_at')
        .values('captured_at', 'book_data')
    )
    cache.set(cache_key, books, timeout=_REPLAY_BOOKS_CACHE_SEC)
    return books


def _quote_rows_for_day_cached(sym: str, session_day: date) -> int:
    key = _REPLAY_QCOUNT_CACHE_KEY.format(sym=sym, day=session_day.isoformat())
    hit = cache.get(key)
    if hit is not None:
        return int(hit)
    day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)
    n = int(
        QuoteSnapshot.objects.filter(
            ticker=sym,
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        ).count()
    )
    cache.set(key, n, timeout=_REPLAY_BOOKS_CACHE_SEC)
    return n


def _frames_from_quotes(
    sym: str,
    quotes_slice: list[dict[str, Any]],
    books: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Alinha cada quote ao último book com ``captured_at`` <= instante da cotação.

    Quotes e books vêm ordenados por ``captured_at``; percorre com um único índice no
    livro (O(n+m)) em vez de ``bisect`` por frame (O(n log m)).
    """
    hints = _operation_hints(sym)
    hints_json = json_sanitize(hints)
    norm_book_ts = [_captured_as_sp(b['captured_at']) for b in books]
    n_books = len(books)
    book_idx = -1
    frames: list[dict[str, Any]] = []
    for q in quotes_slice:
        q_raw = q['captured_at']
        qdt = _captured_as_sp(q_raw)
        while book_idx + 1 < n_books and norm_book_ts[book_idx + 1] <= qdt:
            book_idx += 1
        book_raw: dict[str, Any] = {}
        if book_idx >= 0:
            bd = books[book_idx].get('book_data')
            if isinstance(bd, dict):
                book_raw = bd
        raw_bids = book_raw.get('bids') or book_raw.get('Bids') or []
        raw_asks = book_raw.get('asks') or book_raw.get('Asks') or []
        agg_bids = normalize_book_levels(raw_bids)
        agg_asks = normalize_book_levels(raw_asks)
        book_ui = {'bids': agg_bids, 'asks': agg_asks}
        qd = q.get('quote_data')
        quote = qd if isinstance(qd, dict) else {}
        frames.append(
            {
                'id': q['id'],
                'captured_at': qdt.isoformat(),
                'ticker': sym,
                'quote': json_sanitize(quote),
                'book': json_sanitize(book_ui),
                'agg_bids': json_sanitize(agg_bids),
                'agg_asks': json_sanitize(agg_asks),
                'aggregate_book': {'source': 'replay_book_snapshot'}
                if (agg_bids or agg_asks)
                else None,
                'errors': {},
                'details': None,
                'operation_hints': hints_json,
                'chart_payload': json_sanitize(ohlc_bar_chart_payload(quote)),
                'live_poll_active': True,
            }
        )
    return frames


def _parse_align_bucket_iso(raw: str | None) -> datetime | None:
    """Interpreta ISO vindo do candle (`bucket_start`) para alinhar o 1º trecho do replay."""
    s = (raw or '').strip()
    if not s:
        return None
    dt = parse_datetime(s)
    if dt is None:
        try:
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        except ValueError:
            return None
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, _TZ_SP)
    else:
        dt = dt.astimezone(_TZ_SP)
    return dt


def _snapshot_offset_before_time(sym: str, session_day: date, align_dt: datetime) -> int:
    """Quantos QuoteSnapshot existem antes de ``align_dt`` no dia (índice global do 1º snapshot ≥ instante do candle)."""
    day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)
    adt = _captured_as_sp(align_dt)
    if adt < day_start or adt >= day_end:
        return 0
    align_key = adt.isoformat(timespec='microseconds')
    ck = _REPLAY_ALIGN_OFF_CACHE_KEY.format(sym=sym, day=session_day.isoformat(), align_key=align_key)
    hit = cache.get(ck)
    if isinstance(hit, int) and hit >= 0:
        return hit
    n = int(
        QuoteSnapshot.objects.filter(
            ticker=sym,
            captured_at__gte=day_start,
            captured_at__lt=adt,
        ).count()
    )
    cache.set(ck, n, timeout=_REPLAY_BOOKS_CACHE_SEC)
    return n


def build_replay_frames_page(
    ticker: str,
    session_day: date,
    *,
    offset: int,
    limit: int,
    align_bucket_start: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Um segmento da lista cronológica de frames (cotação + livro alinhado).

    ``total`` no meta é o número de instantes disponíveis para o scrub (até ``_MAX_FRAMES``).
    O cliente deve pedir ``offset`` em sequência (0, depois len acumulado, …) até ``has_more`` falso.

    Com ``offset == 0`` e ``align_bucket_start`` (ISO do 1º candle visível no gráfico), o servidor
    calcula o skip para o primeiro snapshot naquele instante — o replay não começa mais «sempre do zero»
    quando o usuário já está vendo o fim do dia no gráfico.
    """
    sym = (ticker or '').strip().upper()
    day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)

    raw_total = _quote_rows_for_day_cached(sym, session_day)
    cap = min(raw_total, _MAX_FRAMES)
    truncated = raw_total > _MAX_FRAMES

    offset = max(0, int(offset))
    limit = max(1, min(int(limit), MAX_REPLAY_CHUNK))
    if offset == 0 and align_bucket_start and str(align_bucket_start).strip():
        adt = _parse_align_bucket_iso(str(align_bucket_start))
        if adt is not None:
            offset = _snapshot_offset_before_time(sym, session_day, adt)
    if offset >= cap or cap == 0:
        return [], {
            'quote_rows': raw_total,
            'book_rows': 0,
            'total': cap,
            'offset': offset,
            'limit': limit,
            'returned': 0,
            'has_more': False,
            'truncated': truncated,
        }

    end = min(offset + limit, cap)
    quotes_slice = list(
        QuoteSnapshot.objects.filter(
            ticker=sym,
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        )
        .order_by('captured_at')
        .values('id', 'captured_at', 'quote_data')[offset:end]
    )
    books = _books_for_replay_day(sym, session_day)
    frames = _frames_from_quotes(sym, quotes_slice, books)
    next_offset = offset + len(frames)
    meta: dict[str, Any] = {
        'quote_rows': raw_total,
        'book_rows': len(books),
        'total': cap,
        'offset': offset,
        'limit': limit,
        'returned': len(frames),
        'has_more': next_offset < cap,
        'truncated': truncated,
    }
    return frames, meta
