"""
REST Ordens (Smart Trader / Clear): consulta intraday, histórico, cancelamento e envio.

**Ambiente simulado:** defina ``SMART_TRADER_API_BASE_URL`` com a URL do simulador, por exemplo
``https://variableincome-openapi-simulator.xpi.com.br/api``. Credenciais: ``SMART_TRADER_*`` e
chave RSA para ``BODY_SIGNATURE`` nos POSTs de envio/replace de ordens.

**Fluxo típico (simulador):** (opcional) :func:`post_simulator_setup_orders` com
``filled`` ou ``rejected`` (comportamento do ``POST /v1/setup/orders``). Ordens **em
aberto** no simulador não vêm desse setup com ordem **a mercado** (executa na hora);
use **ordem limitada** com preço fora do mercado e **sem** setup — ver modo
``open_limited`` na boleta. Em seguida chame
:func:`post_send_market_order`, :func:`post_send_limited_order`, etc., com o dicionário
de corpo conforme a documentação. Para envios **só** via código (ex.: Celery), sem
setup prévio, :func:`post_send_market_order` pode chamar ``filled`` automaticamente no
simulador (desligue com ``SMART_TRADER_SIMULATOR_MARKET_AUTO_FILLED=0``).

Limites (doc):
- ``GET /v1/orders`` e ``GET /v1/orders/history``: 1 requisição/s (cada rota).
- ``POST .../cancel`` e POSTs de envio/replace (exceto send/limited): 10 req / 5 s por rota.
- ``POST .../send/limited``: 20 req / 5 s.
- ``POST .../setup/orders`` (só simulador): 5 req / min.

POSTs de **envio/replace** de ordens exigem ``BODY_SIGNATURE`` (RSA-SHA256). O setup do
simulador (**/v1/setup/orders**) não usa assinatura de corpo.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from typing import Any

import requests

from api_auth import config as api_config
from api_auth.services.auth import get_access_token
from api_auth.services.signature import generate_body_signature
from trader.environment import ENV_REAL, ENV_SIMULATOR, get_current_environment

logger = logging.getLogger(__name__)

_RL_LOCK = threading.Lock()
_get_last_at: dict[str, float] = {}
_sliding: dict[str, deque[float]] = {}

# JSON idêntico ao byte usado em BODY_SIGNATURE
_JSON_SEP = (',', ':')

# Valores aceitos em POST /v1/setup/orders (corpo ``orderStatus``). Na prática o
# simulador costuma honrar só ``filled`` / ``rejected``; ordem em aberto para testar
# cancelamento = UI ``open_limited`` (sem este POST + limitada fora do preço).
SIMULATOR_SETUP_ORDER_STATUSES: frozenset[str] = frozenset({'filled', 'rejected'})


def _respect_1_per_second(route_key: str) -> None:
    with _RL_LOCK:
        now = time.monotonic()
        prev = _get_last_at.get(route_key, 0.0)
        wait = 1.0 - (now - prev)
        if wait > 0:
            time.sleep(wait)
        _get_last_at[route_key] = time.monotonic()


def _respect_sliding(route_key: str, max_calls: int, window_sec: float) -> None:
    dq = _sliding.setdefault(route_key, deque())
    with _RL_LOCK:
        now = time.monotonic()
        while dq and now - dq[0] > window_sec:
            dq.popleft()
        if len(dq) >= max_calls:
            wait = window_sec - (now - dq[0]) + 0.001
            if wait > 0:
                time.sleep(wait)
            now = time.monotonic()
            while dq and now - dq[0] > window_sec:
                dq.popleft()
        dq.append(time.monotonic())


def _env_route_key(route_key: str) -> str:
    return f'{get_current_environment()}:{route_key}'


def _headers_json() -> dict[str, str]:
    return {
        'Ocp-Apim-Subscription-Key': api_config.subscription_key(),
        'Authorization': f'Bearer {get_access_token()}',
        'Content-Type': 'application/json',
        'User-Agent': api_config.user_agent(),
    }


def _headers_signed_body(body_str: str) -> dict[str, str]:
    h = _headers_json()
    h['BODY_SIGNATURE'] = generate_body_signature(body_str)
    return h


def _base_v1(path: str) -> str:
    base = api_config.api_base_url().rstrip('/')
    p = path.strip('/')
    return f'{base}/v1/{p}'


def _orders_error_message(response: requests.Response) -> str:
    """Extrai texto útil do JSON de erro da API (ex.: Errors[].Message)."""
    raw = (response.text or '').strip()
    if not raw:
        return f'HTTP {response.status_code} sem corpo.'
    try:
        data = response.json()
    except ValueError:
        return raw[:400]
    errs = data.get('Errors') or data.get('errors')
    if isinstance(errs, list) and errs:
        parts: list[str] = []
        for item in errs:
            if not isinstance(item, dict):
                continue
            code = item.get('Code') or item.get('code')
            msg = item.get('Message') or item.get('message')
            if msg:
                parts.append(f'{code}: {msg}' if code else str(msg))
        if parts:
            return '; '.join(parts)
    if isinstance(data.get('message'), str):
        return data['message']
    return raw[:400]


def _parse_response(response: requests.Response, *, api_label: str = 'ordens') -> Any:
    if response.status_code >= 400:
        text = (response.text or '')[:800]
        logger.warning(
            'REST API (%s) status=%s body=%s',
            api_label,
            response.status_code,
            text,
        )
        detail = _orders_error_message(response)
        raise ValueError(
            f'A API de {api_label} retornou status {response.status_code}. {detail}'
        )
    if not response.content or not response.content.strip():
        return {}
    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(f'Resposta JSON inválida da API de {api_label}.') from exc


def _canonical_json_body(body: dict[str, Any]) -> str:
    return json.dumps(body, separators=_JSON_SEP, ensure_ascii=False)


# --- GET ---


_ORDERS_GET_CACHE_KEY = 'trader:v1:orders_get'


def _http_timeout_sec() -> float:
    try:
        return float(
            max(3.0, min(30.0, float(os.environ.get('TRADER_ORDERS_HTTP_TIMEOUT_SEC', '8'))))
        )
    except ValueError:
        return 8.0


def fetch_orders() -> Any:
    """
    GET ``/v1/orders`` — ordens do dia (intraday).

    Limite: 1 requisição/s.
    """
    _respect_1_per_second(_env_route_key('orders:get:intraday'))
    url = _base_v1('orders')
    try:
        r = requests.get(url, headers=_headers_json(), timeout=_http_timeout_sec())
    except requests.RequestException as exc:
        logger.warning('Falha de rede GET /orders')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def fetch_orders_cached() -> Any:
    """
    Mesmo que :func:`fetch_orders`, com TTL curto no cache Django.

    Ao abrir ``/painel/`` o browser dispara vários pedidos; sem isto cada um
    esperava o rate limit (``sleep``) e a API — workers ficavam presos e o site «a carregar para sempre».
    """
    from django.core.cache import cache

    key = f'{get_current_environment()}:{_ORDERS_GET_CACHE_KEY}'
    hit = cache.get(key)
    if hit is not None:
        return hit
    data = fetch_orders()
    try:
        ttl = max(2, int(os.environ.get('DJANGO_ORDERS_INTRADAY_CACHE_SEC', '10') or '10'))
    except ValueError:
        ttl = 10
    cache.set(key, data, ttl)
    return data


def invalidate_intraday_orders_cache() -> None:
    """Chamar após envio/cancelamento de ordem para a lista não ficar desatualizada."""
    from django.core.cache import cache

    cache.delete(f'{get_current_environment()}:{_ORDERS_GET_CACHE_KEY}')


def fetch_collateral() -> Any:
    """
    GET ``/v1/collateral`` — lista de garantias do cliente.

    Limite: 10 requisições em 5 segundos (janela deslizante).
    """
    _respect_sliding(_env_route_key('rest:get:collateral'), 10, 5.0)
    url = _base_v1('collateral')
    try:
        r = requests.get(url, headers=_headers_json(), timeout=_http_timeout_sec())
    except requests.RequestException as exc:
        logger.warning('Falha de rede GET /collateral')
        raise ValueError('Não foi possível contatar a API de garantias.') from exc
    return _parse_response(r, api_label='garantias')


def fetch_custody() -> Any:
    """
    GET ``/v1/custody`` — lista de custódia do cliente.

    Limite: 10 requisições em 5 segundos (janela deslizante).
    """
    _respect_sliding(_env_route_key('rest:get:custody'), 10, 5.0)
    url = _base_v1('custody')
    try:
        r = requests.get(url, headers=_headers_json(), timeout=_http_timeout_sec())
    except requests.RequestException as exc:
        logger.warning('Falha de rede GET /custody')
        raise ValueError('Não foi possível contatar a API de custódia.') from exc
    return _parse_response(r, api_label='custódia')


def fetch_orders_history(
    date_from: str,
    date_to: str,
    *,
    page_index: int = 0,
    page_size: int = 20,
) -> Any:
    """
    GET ``/v1/orders/history`` — histórico com paginação.

    ``date_from`` / ``date_to``: ``YYYY-MM-DD``.
    Limite: 1 requisição/s.
    """
    _respect_1_per_second(_env_route_key('orders:get:history'))
    url = _base_v1('orders/history')
    params = {
        'from': date_from,
        'to': date_to,
        'pageIndex': page_index,
        'pageSize': page_size,
    }
    try:
        r = requests.get(url, headers=_headers_json(), params=params, timeout=30)
    except requests.RequestException as exc:
        logger.warning('Falha de rede GET /orders/history')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


# --- Simulador (sem BODY_SIGNATURE) ---


def post_simulator_setup_orders(order_status: str) -> Any:
    """
    POST ``/v1/setup/orders`` — **somente ambiente simulado**.

    ``orderStatus`` no corpo: ``filled`` ou ``rejected``. Para ordem em aberto use
    envio **sem** este endpoint + ordem limitada com preço que não execute (boleta
    ``open_limited``).

    Limite: 5 requisições / minuto. Não envia ``BODY_SIGNATURE`` (conforme doc).
    """
    if get_current_environment() != ENV_SIMULATOR:
        raise ValueError('POST /v1/setup/orders é exclusivo do ambiente simulador.')
    status = (order_status or '').strip().lower()
    if status not in SIMULATOR_SETUP_ORDER_STATUSES:
        allowed = ', '.join(sorted(SIMULATOR_SETUP_ORDER_STATUSES))
        raise ValueError(f'order_status deve ser um de: {allowed}.')
    _respect_sliding(_env_route_key('simulator:setup:orders'), 5, 60.0)
    body = {'orderStatus': status}
    body_str = _canonical_json_body(body)
    url = _base_v1('setup/orders')
    try:
        r = requests.post(
            url,
            headers=_headers_json(),
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /setup/orders')
        raise ValueError('Não foi possível contatar a API de setup (simulador).') from exc
    return _parse_response(r)


# --- POST sem BODY_SIGNATURE ---


def post_cancel_order(order_id: str) -> Any:
    """
    POST ``/v1/orders/cancel`` — cancela ordem.

    Query: ``Id`` (string) — identificador da ordem.
    Sem corpo; headers: subscription key, ``Authorization: Bearer``, ``User-Agent``.
    Limite: 10 requisições em 5 s (janela deslizante).

    Exemplo: ``.../api/v1/orders/cancel?Id=example_string``
    """
    oid = (order_id or '').strip()
    if not oid:
        raise ValueError('Id da ordem inválido.')
    _respect_sliding(_env_route_key('orders:post:cancel'), 10, 5.0)
    url = _base_v1('orders/cancel')

    def _do_post(params: dict[str, str]) -> requests.Response:
        return requests.post(
            url,
            headers=_headers_json(),
            params=params,
            timeout=30,
        )

    try:
        r = _do_post({'Id': oid})
        # Compatibilidade: alguns ambientes aceitam apenas "id" minúsculo.
        if r.status_code >= 400:
            r_alt = _do_post({'id': oid})
            if r_alt.status_code < r.status_code:
                r = r_alt
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/cancel')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


# --- POST com BODY_SIGNATURE ---


def _order_api_env_tag() -> str:
    return 'REAL' if get_current_environment() == ENV_REAL else 'SIMULADOR'


def _log_order_api_dispatch(route: str, body: dict[str, Any]) -> None:
    """Log único por POST de envio: diferencia API real vs simulador (todas as origens)."""
    tk = (str(body.get('Ticker') or body.get('ticker') or '')).strip() or '—'
    logger.info('Ordem API [%s] POST /v1/orders/%s ticker=%s', _order_api_env_tag(), route, tk)


def _simulator_market_auto_filled_enabled() -> bool:
    """Se verdadeiro, ``post_send_market_order`` chama setup ``filled`` antes do POST (simulador)."""
    raw = (os.environ.get('SMART_TRADER_SIMULATOR_MARKET_AUTO_FILLED') or '1').strip().lower()
    return raw not in ('0', 'false', 'no', 'off')


def post_send_market_order(body: dict[str, Any], *, skip_simulator_auto_filled: bool = False) -> Any:
    """
    POST ``/v1/orders/send/market`` — ordem a mercado.

    Ex.: ``Module``, ``Ticker``, ``Side``, ``Quantity``, ``TimeInForce``.
    Limite: 10 req / 5 s.

    **Simulador:** a API de simulação costuma exigir ``POST /v1/setup/orders`` com
    ``orderStatus: filled`` (ou ``rejected``) *antes* do envio para a próxima ordem
    a mercado refletir esse resultado. Quem já chamou :func:`post_simulator_setup_orders`
    no mesmo fluxo (boleta de teste, comando) deve passar ``skip_simulator_auto_filled=True``.
    Caso contrário, em ambiente simulador e com
    ``SMART_TRADER_SIMULATOR_MARKET_AUTO_FILLED`` ativo (default), este método chama
    ``filled`` automaticamente para evitar mercado «pendente» sem execução simulada.

    **Ambiente REAL:** não há ``setup``; o envio é direto à API de produção. Ambos os
    modos geram log ``Ordem API [REAL|SIMULADOR] ...`` via :func:`_log_order_api_dispatch`.
    """
    if (
        get_current_environment() == ENV_SIMULATOR
        and not skip_simulator_auto_filled
        and _simulator_market_auto_filled_enabled()
    ):
        try:
            post_simulator_setup_orders('filled')
        except Exception as exc:
            logger.warning(
                'Simulador: auto-filled antes de POST /orders/send/market falhou (%s); segue envio.',
                exc,
            )
    _respect_sliding(_env_route_key('orders:post:send:market'), 10, 5.0)
    _log_order_api_dispatch('send/market', body)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/send/market')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/send/market')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def post_replace_market_order(order_id: str, body: dict[str, Any]) -> Any:
    """
    POST ``/v1/orders/replace/market`` — substitui por ordem a mercado.

    Query: ``id``. Corpo: ``Quantity``, ``TimeInForce``. Limite: 10 req / 5 s.
    """
    oid = (order_id or '').strip()
    if not oid:
        raise ValueError('id da ordem inválido.')
    _respect_sliding(_env_route_key('orders:post:replace:market'), 10, 5.0)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/replace/market')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            params={'id': oid},
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/replace/market')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def post_send_limited_order(body: dict[str, Any]) -> Any:
    """
    POST ``/v1/orders/send/limited`` — ordem limitada.

    Ex.: ``Module``, ``Ticker``, ``Side``, ``Quantity``, ``Price``, ``TimeInForce``.
    Limite: 20 req / 5 s.
    """
    _respect_sliding(_env_route_key('orders:post:send:limited'), 20, 5.0)
    _log_order_api_dispatch('send/limited', body)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/send/limited')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/send/limited')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def post_replace_limited_order(order_id: str, body: dict[str, Any]) -> Any:
    """
    POST ``/v1/orders/replace/limited`` — substitui por ordem limitada.

    Query: ``id``. Corpo: ``Quantity``, ``Price``, ``TimeInForce``. Limite: 10 req / 5 s.
    """
    oid = (order_id or '').strip()
    if not oid:
        raise ValueError('id da ordem inválido.')
    _respect_sliding(_env_route_key('orders:post:replace:limited'), 10, 5.0)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/replace/limited')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            params={'id': oid},
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/replace/limited')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def post_send_stop_limit_order(body: dict[str, Any]) -> Any:
    """
    POST ``/v1/orders/send/stop-limit`` — ordem stop limit.

    Ex.: ``Module``, ``Ticker``, ``Side``, ``Quantity``,
    ``StopTriggerPrice``, ``StopOrderPrice``, ``TimeInForce``.
    Limite: 10 req / 5 s.
    """
    _respect_sliding(_env_route_key('orders:post:send:stop-limit'), 10, 5.0)
    _log_order_api_dispatch('send/stop-limit', body)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/send/stop-limit')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/send/stop-limit')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)


def post_replace_stop_limit_order(order_id: str, body: dict[str, Any]) -> Any:
    """
    POST ``/v1/orders/replace/stop-limit`` — substitui por stop limit.

    Query: ``id``. Corpo: ``Quantity``, ``StopTriggerPrice``, ``StopOrderPrice``, ``TimeInForce``.
    Limite: 10 req / 5 s.
    """
    oid = (order_id or '').strip()
    if not oid:
        raise ValueError('id da ordem inválido.')
    _respect_sliding(_env_route_key('orders:post:replace:stop-limit'), 10, 5.0)
    body_str = _canonical_json_body(body)
    url = _base_v1('orders/replace/stop-limit')
    try:
        r = requests.post(
            url,
            headers=_headers_signed_body(body_str),
            params={'id': oid},
            data=body_str.encode('utf-8'),
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede POST /orders/replace/stop-limit')
        raise ValueError('Não foi possível contatar a API de ordens.') from exc
    return _parse_response(r)
