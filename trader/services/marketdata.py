"""
REST Market Data (Smart Trader): quote, details, book, aggregate-book.

Limites (doc oficial):
- ``details``, ``book``, ``aggregate-book``: 1 requisição/s (por endpoint + ticker).
- ``quote``: 20 requisições em 5 s (janela deslizante global para GETs reais de quote).

Respostas são cacheadas (TTL por recurso) para aliviar F5 na página Mercado.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Any

import requests
from django.core.cache import cache

from api_auth import config as api_config
from api_auth.services.auth import get_access_token
from trader.environment import get_current_environment

logger = logging.getLogger(__name__)

# Evita repetir GETs ao mesmo ticker+endpoint após 404 (contrato inválido / fora da API).
_MISS_CACHE_SUFFIX = ':miss'
_QUOTE_CACHE = 8
_DETAILS_CACHE = 15
_BOOK_CACHE = 15
_AGG_BOOK_CACHE = 15

_QUOTE_WINDOW_SEC = 5.0
_QUOTE_MAX_PER_WINDOW = 20

_rl_lock = threading.Lock()
_last_http_at: dict[str, float] = {}
_quote_call_times: deque[float] = deque()


def _rate_limit_key(endpoint: str, ticker: str) -> str:
    return f'{get_current_environment()}:{endpoint}:{ticker.strip().upper()}'


def _respect_quote_rate_limit() -> None:
    """Doc quote: até 20 GETs reais em qualquer janela de 5 s (global)."""
    with _rl_lock:
        now = time.monotonic()
        while _quote_call_times and now - _quote_call_times[0] > _QUOTE_WINDOW_SEC:
            _quote_call_times.popleft()
        if len(_quote_call_times) >= _QUOTE_MAX_PER_WINDOW:
            wait = _QUOTE_WINDOW_SEC - (now - _quote_call_times[0]) + 0.001
            if wait > 0:
                time.sleep(wait)
            now = time.monotonic()
            while _quote_call_times and now - _quote_call_times[0] > _QUOTE_WINDOW_SEC:
                _quote_call_times.popleft()
        _quote_call_times.append(time.monotonic())


def _respect_rate_limit(endpoint: str, ticker: str) -> None:
    """1 s entre GETs reais ao mesmo par endpoint + ticker (details, book, aggregate-book)."""
    key = _rate_limit_key(endpoint, ticker)
    with _rl_lock:
        now = time.monotonic()
        prev = _last_http_at.get(key, 0.0)
        wait = 1.0 - (now - prev)
        if wait > 0:
            time.sleep(wait)
        _last_http_at[key] = time.monotonic()


def _headers() -> dict[str, str]:
    return {
        'Ocp-Apim-Subscription-Key': api_config.subscription_key(),
        'Authorization': f'Bearer {get_access_token()}',
        'Content-Type': 'application/json',
        'User-Agent': api_config.user_agent(),
    }


def _miss_cache_key(full_cache_key: str) -> str:
    return f'{full_cache_key}{_MISS_CACHE_SUFFIX}'


def _negative_miss_ttl_sec() -> int:
    try:
        return max(15, int(os.environ.get('TRADER_MARKETDATA_404_CACHE_SEC', '120')))
    except ValueError:
        return 120


def _http_timeout_sec() -> float:
    try:
        return float(max(5.0, min(60.0, float(os.environ.get('TRADER_MARKETDATA_TIMEOUT_SEC', '20')))))
    except ValueError:
        return 20.0


def _get_json(
    endpoint: str,
    *,
    params: dict[str, str],
    cache_key: str,
    cache_ttl: int,
    use_cache: bool,
) -> dict[str, Any]:
    env = get_current_environment()
    cache_key = f'{env}:{cache_key}'
    if cache.get(_miss_cache_key(cache_key)):
        raise ValueError(
            'Ticker indisponível na API (cache após 404). Atualize o contrato em '
            'TRADER_WATCH_TICKERS / candidatos WIN ou aguarde alguns minutos.'
        )
    if use_cache:
        hit = cache.get(cache_key)
        if isinstance(hit, dict):
            return hit

    sym = params.get('ticker') or params.get('Ticker', '')
    if endpoint == 'quote':
        _respect_quote_rate_limit()
    else:
        _respect_rate_limit(endpoint, str(sym))

    base = api_config.api_base_url().rstrip('/')
    url = f'{base}/v1/marketdata/{endpoint}'

    try:
        response = requests.get(
            url, headers=_headers(), params=params, timeout=_http_timeout_sec()
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede marketdata %s params=%s', endpoint, params)
        raise ValueError('Não foi possível contatar a API de market data.') from exc

    if response.status_code != 200:
        logger.warning(
            'Market data %s status=%s params=%s',
            endpoint,
            response.status_code,
            params,
        )
        if response.status_code in (404, 410):
            cache.set(_miss_cache_key(cache_key), 1, _negative_miss_ttl_sec())
        raise ValueError(
            f'A API retornou status {response.status_code} em /{endpoint}.'
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError('Resposta JSON inválida.') from exc

    if not isinstance(data, dict):
        raise ValueError('Resposta inesperada (não é objeto JSON).')

    cache.delete(_miss_cache_key(cache_key))
    cache.set(cache_key, data, cache_ttl)
    return data


def fetch_quote(ticker: str, *, use_cache: bool = True) -> dict[str, Any]:
    """GET ``/v1/marketdata/quote`` — parâmetro ``ticker`` (doc)."""
    sym = ticker.strip().upper()
    if not sym:
        raise ValueError('Ticker inválido.')
    return _get_json(
        'quote',
        params={'ticker': sym},
        cache_key=f'trader:quote:{sym}',
        cache_ttl=_QUOTE_CACHE,
        use_cache=use_cache,
    )


def fetch_ticker_details(ticker: str, *, use_cache: bool = True) -> dict[str, Any]:
    """GET ``/v1/marketdata/details`` — entidade ``TickerDetails``."""
    sym = ticker.strip().upper()
    if not sym:
        raise ValueError('Ticker inválido.')
    return _get_json(
        'details',
        params={'ticker': sym},
        cache_key=f'trader:details:{sym}',
        cache_ttl=_DETAILS_CACHE,
        use_cache=use_cache,
    )


def fetch_book(ticker: str, *, use_cache: bool = True) -> dict[str, Any]:
    """GET ``/v1/marketdata/book`` — livro por ordem."""
    sym = ticker.strip().upper()
    if not sym:
        raise ValueError('Ticker inválido.')
    return _get_json(
        'book',
        params={'ticker': sym},
        cache_key=f'trader:book:{sym}',
        cache_ttl=_BOOK_CACHE,
        use_cache=use_cache,
    )


def fetch_aggregate_book(ticker: str, *, use_cache: bool = True) -> dict[str, Any]:
    """GET ``/v1/marketdata/aggregate-book`` — livro agregado por preço."""
    sym = ticker.strip().upper()
    if not sym:
        raise ValueError('Ticker inválido.')
    return _get_json(
        'aggregate-book',
        params={'ticker': sym},
        cache_key=f'trader:aggbook:{sym}',
        cache_ttl=_AGG_BOOK_CACHE,
        use_cache=use_cache,
    )


def ohlc_bar_chart_payload(quote: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Dados para um gráfico de barras OHLC (instantâneo) a partir de uma ``Quote``.

    Se ``close`` for nulo, usa ``lastPrice`` como fechamento exibido.
    """
    if not quote:
        return None
    keys = ('open', 'high', 'low', 'close')
    labels = ['Abertura', 'Máxima', 'Mínima', 'Fechamento']
    values: list[float] = []
    for k in keys:
        v = quote.get(k)
        if v is None and k == 'close':
            v = quote.get('lastPrice')
        if v is None:
            return None
        try:
            values.append(float(v))
        except (TypeError, ValueError):
            return None
    return {'labels': labels, 'values': values}
