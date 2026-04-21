from __future__ import annotations

import logging
import threading

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

from trader.models import WatchedTicker
from trader.panel_context import quote_status_is_end_of_day
from trader.services.marketdata import fetch_quote, fetch_book
from trader.services.quote_history import (
    brt_save_window_allows_now,
    save_book_snapshot,
    save_quote_snapshot,
)

logger = logging.getLogger(__name__)

_WATCH_TASK_LOCK_KEY = 'trader:watch_quotes:lock:v1'
_WATCH_STANDBY_UNTIL_KEY = 'trader:watch_quotes:standby_until_ts:v1'
_WATCH_RR_INDEX_KEY = 'trader:watch_quotes:rr_index:v1'

_redis_conn = None
_redis_conn_failed = False


def _broker_redis():
    """
    Cliente Redis do broker Celery (compartilhado entre workers).

    O cache default do Django costuma ser LocMem: ``cache.add`` do lock **não** impede duas
    instâncias da mesma tarefa em processos diferentes — principal causa de CPU/API no Beat.
    """
    global _redis_conn, _redis_conn_failed
    if _redis_conn_failed:
        return None
    if _redis_conn is not None:
        return _redis_conn
    try:
        import redis

        url = getattr(settings, 'CELERY_BROKER_URL', '') or ''
        if not isinstance(url, str) or not url.strip():
            _redis_conn_failed = True
            return None
        _redis_conn = redis.from_url(url, decode_responses=True)
        return _redis_conn
    except Exception as exc:
        logger.warning('trader.tasks: Redis indisponível para lock/standby watch (%s).', exc)
        _redis_conn_failed = True
        return None


def _watch_try_acquire_lock(ttl: int) -> bool:
    sec = max(3, int(ttl))
    r = _broker_redis()
    if r is not None:
        try:
            return bool(r.set(_WATCH_TASK_LOCK_KEY, '1', nx=True, ex=sec))
        except Exception:
            pass
    return cache.add(_WATCH_TASK_LOCK_KEY, 1, timeout=sec)


def _watch_release_lock() -> None:
    r = _broker_redis()
    if r is not None:
        try:
            r.delete(_WATCH_TASK_LOCK_KEY)
        except Exception:
            pass
    cache.delete(_WATCH_TASK_LOCK_KEY)


def _standby_until_get() -> float | None:
    r = _broker_redis()
    if r is not None:
        try:
            raw = r.get(_WATCH_STANDBY_UNTIL_KEY)
            if raw is None or raw == '':
                return None
            return float(raw)
        except Exception:
            pass
    raw = cache.get(_WATCH_STANDBY_UNTIL_KEY)
    if isinstance(raw, (int, float)):
        return float(raw)
    return None


def _standby_until_set(until: float, ttl_sec: int) -> None:
    r = _broker_redis()
    if r is not None:
        try:
            r.set(_WATCH_STANDBY_UNTIL_KEY, str(until), ex=max(60, int(ttl_sec)))
            return
        except Exception:
            pass
    cache.set(_WATCH_STANDBY_UNTIL_KEY, until, timeout=max(60, int(ttl_sec)))


def _standby_until_delete() -> None:
    r = _broker_redis()
    if r is not None:
        try:
            r.delete(_WATCH_STANDBY_UNTIL_KEY)
        except Exception:
            pass
    cache.delete(_WATCH_STANDBY_UNTIL_KEY)


def _watch_list_tickers() -> list[str]:
    """Lista de símbolos do watch (BD ou settings), normalizada."""
    tickers = list(
        WatchedTicker.objects.filter(enabled=True).values_list('ticker', flat=True)
    )
    if not tickers:
        tickers = list(getattr(settings, 'TRADER_WATCH_TICKERS', []) or [])
    return [str(t).strip().upper() for t in tickers if str(t).strip()]


def _invoke_watch_automation(selected: list[str], probe_quote: dict | None) -> None:
    """Motor de estratégias (simulação de sessão funciona mesmo com probe em fim de pregão)."""
    try:
        from trader.automacoes.automation_engine import run_automation_after_quote_collect

        iv = getattr(settings, 'TRADER_LEAFAR_INTERVAL_SEC', 10)
        try:
            interval_sec = max(1, min(int(iv), 300))
        except (TypeError, ValueError):
            interval_sec = 10
        run_automation_after_quote_collect(
            selected,
            probe_quote=probe_quote,
            interval_sec=interval_sec,
        )
    except Exception:
        logger.exception('collect_watch_quotes: automation_engine')


def _tickers_round_robin_slice(tickers: list[str]) -> list[str]:
    """Limita tickers por execução (``TRADER_WATCH_MAX_QUOTES_PER_RUN``) com rodízio."""
    max_per = max(1, _safe_int_setting('TRADER_WATCH_MAX_QUOTES_PER_RUN', 8))
    if len(tickers) <= max_per:
        return tickers
    n = len(tickers)
    start = 0
    r = _broker_redis()
    try:
        if r is not None:
            raw = r.get(_WATCH_RR_INDEX_KEY)
            start = int(float(raw)) % n if raw not in (None, '') else 0
            r.set(_WATCH_RR_INDEX_KEY, str((start + max_per) % n), ex=86400)
        else:
            raw = cache.get(_WATCH_RR_INDEX_KEY)
            start = int(raw) % n if raw is not None else 0
            cache.set(_WATCH_RR_INDEX_KEY, (start + max_per) % n, timeout=86400)
    except Exception:
        start = 0
    return [tickers[(start + i) % n] for i in range(max_per)]


def _safe_int_setting(name: str, default: int) -> int:
    raw = getattr(settings, name, default)
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if s:
            try:
                return int(s)
            except ValueError:
                return default
    return default


def _now_ts() -> float:
    return timezone.now().timestamp()


def _standby_interval_sec() -> int:
    return max(60, _safe_int_setting('TRADER_WATCH_CLOSED_INTERVAL_SEC', 1800))


def _standby_enabled() -> bool:
    return bool(getattr(settings, 'TRADER_WATCH_STANDBY_ENABLED', True))


def _clear_watch_standby() -> None:
    _standby_until_delete()


def _set_watch_standby() -> float:
    """Grava até quando ficar em standby; retorna o timestamp alvo (epoch)."""
    until = _now_ts() + float(_standby_interval_sec())
    ttl = int(_standby_interval_sec()) + 600
    _standby_until_set(until, ttl)
    return until


def watch_standby_info() -> dict:
    """Útil para diagnóstico (ex.: página de testes Celery)."""
    until = _standby_until_get()
    now = _now_ts()
    active = until is not None and now < float(until)
    return {
        'enabled': _standby_enabled(),
        'interval_sec': _standby_interval_sec(),
        'standby_until_ts': until,
        'standby_active': active,
        'next_probe_in_sec': max(0.0, until - now) if until is not None else None,
    }


def _safe_float_setting(name: str, default: float) -> float:
    raw = getattr(settings, name, default)
    if isinstance(raw, (float, int)):
        return float(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s:
            try:
                return float(s)
            except ValueError:
                return default
    return default


def _fetch_book_via_websocket(ticker: str, timeout_sec: float) -> dict | None:
    """
    Captura um snapshot de book por ticker via WS (`SubscribeBook`).

    Se não chegar evento de book dentro do timeout, retorna ``None``.
    """
    sym = (ticker or '').strip().upper()
    if not sym:
        return None

    done = threading.Event()
    holder: dict[str, dict] = {}
    try:
        from clearxp_websocket.protocol import SubscribeBook
        from clearxp_websocket.subscriptions import sign_ticker, unsign_ticker
    except Exception as exc:
        logger.warning('book websocket indisponível (import) ticker=%s err=%s', sym, exc)
        return None

    def _on_marketdata(target: str | None, msg: dict) -> None:
        if (target or '').strip() != 'Book':
            return
        args = msg.get('arguments')
        if not isinstance(args, list) or not args:
            return
        payload = args[0]
        if not isinstance(payload, dict):
            return
        if not isinstance(payload.get('bids'), list) or not isinstance(payload.get('asks'), list):
            return
        # Alguns ambientes não retornam `ticker` no payload de Book.
        if not payload.get('ticker'):
            payload = {**payload, 'ticker': sym}
        holder['book'] = payload
        done.set()

    try:
        sign_ticker(
            sym,
            subscriptions=[SubscribeBook],
            marketdata_callback=_on_marketdata,
            orders_callback=None,
        )
        done.wait(timeout=max(0.2, timeout_sec))
    except Exception as exc:
        logger.warning('book websocket falhou ticker=%s err=%s', sym, exc)
    finally:
        try:
            unsign_ticker(sym, ['UnsubscribeBook'])
        except Exception:
            # Pode não estar conectado ou o socket pode ter fechado; segue fluxo.
            pass

    return holder.get('book')


def _fetch_book_for_snapshot(ticker: str) -> dict | None:
    """
    Busca book priorizando WebSocket e faz fallback para REST.

    Controle por settings:
    - TRADER_BOOK_WS_ENABLED (default: True)
    - TRADER_BOOK_WS_TIMEOUT_SEC (default: 1.2)
    """
    ws_enabled = bool(getattr(settings, 'TRADER_BOOK_WS_ENABLED', True))
    ws_timeout = _safe_float_setting('TRADER_BOOK_WS_TIMEOUT_SEC', 1.2)

    if ws_enabled:
        book_ws = _fetch_book_via_websocket(ticker, timeout_sec=ws_timeout)
        if isinstance(book_ws, dict):
            return book_ws

    try:
        return fetch_book(ticker, use_cache=False)
    except Exception as exc:
        logger.warning('book fallback REST falhou ticker=%s err=%s', ticker, exc)
        return None


@shared_task
def collect_watch_quotes() -> dict:
    if not getattr(settings, 'TRADER_WATCH_ENABLED', False):
        return {'ok': True, 'enabled': False, 'saved': 0}

    # Standby após detecção de pregão encerrado: sem HTTP, mas o motor de automação
    # ainda corre (ex.: simulação de sessão só com QuoteSnapshot no BD).
    if _standby_enabled():
        raw_until = _standby_until_get()
        if raw_until is not None and _now_ts() < float(raw_until):
            remain = max(0.0, float(raw_until) - _now_ts())
            tickers_sb = _watch_list_tickers()
            if tickers_sb:
                _invoke_watch_automation(_tickers_round_robin_slice(tickers_sb), None)
            return {
                'ok': True,
                'enabled': True,
                'saved': 0,
                'market_standby': True,
                'standby_next_probe_in_sec': round(remain, 3),
                'errors': [],
            }

    # Seg–sex, 9h–19h BRT: não chama API nem grava fora da janela (gravação também bloqueada em ``save_*``).
    if not brt_save_window_allows_now():
        return {
            'ok': True,
            'enabled': True,
            'saved': 0,
            'outside_brt_save_window': True,
            'errors': [],
        }

    # Evita sobreposição de execuções quando o beat agenda mais rápido
    # que o tempo real da rodada (principal causa de 429 em rajadas).
    lock_ttl = max(3, _safe_int_setting('TRADER_WATCH_TASK_LOCK_SEC', 6))
    got_lock = _watch_try_acquire_lock(lock_ttl)
    if not got_lock:
        return {
            'ok': True,
            'enabled': True,
            'saved': 0,
            'skipped_overlap': True,
        }
    try:
        tickers = _watch_list_tickers()
        if not tickers:
            return {'ok': True, 'enabled': True, 'saved': 0, 'errors': []}

        selected = _tickers_round_robin_slice(tickers)
        saved = 0
        errors: list[str] = []

        # Uma sonda no primeiro ticker: se EndOfDay, entra em standby (sem book nos demais).
        first = selected[0]
        try:
            probe = fetch_quote(first, use_cache=False)
        except Exception as exc:
            msg = f'{first}: {exc}'
            errors.append(msg)
            logger.warning('collect_watch_quotes %s', msg)
            return {
                'ok': False,
                'enabled': True,
                'saved': 0,
                'selected': selected,
                'budget': len(selected),
                'total_configured': len(tickers),
                'errors': errors,
            }

        if _standby_enabled() and quote_status_is_end_of_day(probe):
            try:
                snap = save_quote_snapshot(first, probe)
                if snap is not None:
                    saved += 1
            except Exception as exc:
                msg = f'{first}: {exc}'
                errors.append(msg)
                logger.warning('collect_watch_quotes %s', msg)
            until = _set_watch_standby()
            _invoke_watch_automation(selected, probe)
            return {
                'ok': len(errors) == 0,
                'enabled': True,
                'saved': saved,
                'market_standby': True,
                'standby_until_ts': until,
                'standby_interval_sec': _standby_interval_sec(),
                'selected': selected,
                'budget': len(selected),
                'total_configured': len(tickers),
                'errors': errors,
            }

        _clear_watch_standby()

        for i, ticker in enumerate(selected):
            try:
                quote = probe if i == 0 else fetch_quote(ticker, use_cache=False)
                snap = save_quote_snapshot(ticker, quote)
                if snap is not None:
                    saved += 1
                book = _fetch_book_for_snapshot(ticker)
                if book is not None:
                    save_book_snapshot(ticker, book)
            except Exception as exc:
                msg = f'{ticker}: {exc}'
                errors.append(msg)
                logger.warning('collect_watch_quotes %s', msg)
        result = {
            'ok': len(errors) == 0,
            'enabled': True,
            'saved': saved,
            'selected': selected,
            'budget': len(selected),
            'total_configured': len(tickers),
            'errors': errors,
        }
        _invoke_watch_automation(selected, probe)
        return result
    finally:
        _watch_release_lock()


@shared_task
def stream_replay_ticks_task(
    user_id: int,
    ticker: str,
    session_date_iso: str,
    pace_sec: float = 1.0,
    max_snapshots: int | None = None,
) -> dict:
    """
    Simula a chegada ordenada de cotações (``QuoteSnapshot``) do dia, disparando o mesmo
    pipeline de estratégias que o replay por instante (``run_automation_session_replay_now``).

    ``session_date_iso``: ``YYYY-MM-DD`` (prefixo de ISO aceite).
    """
    from datetime import date as date_cls

    from trader.services.replay_stream_motor import stream_session_replay_ticks

    raw = (session_date_iso or '').strip()[:10]
    try:
        sd = date_cls.fromisoformat(raw)
    except ValueError:
        return {'ok': False, 'error': 'invalid_session_date', 'session_date_iso': session_date_iso}
    return stream_session_replay_ticks(
        user_id=int(user_id),
        ticker=str(ticker or '').strip().upper(),
        session_day=sd,
        pace_sec=pace_sec,
        max_snapshots=max_snapshots,
    )
