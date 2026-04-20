from __future__ import annotations

"""
Configuração do WebSocket Smart Trader / Clear.

Variável de ambiente:

``SMART_TRADER_WS_BASE_URL``
    URL base até ``/ws/v1`` (sem barra final ou com; o cliente normaliza).
    Default: simulador oficial (documentação).

Exemplo simulador (base comum às rotas ``marketdata`` e ``orders``)::

    wss://variableincome-openapi-simulator.xpi.com.br:443/ws/v1

O cliente monta ``{base}/marketdata`` ou ``{base}/orders`` (ex.: ``.../ws/v1/orders``).

Em produção, use a URL fornecida pela corretora para o ambiente real.
"""

import os

from trader.environment import ENV_REAL, get_current_environment

DEFAULT_WS_BASE_URL_SIMULATOR = 'wss://variableincome-openapi-simulator.xpi.com.br:443/ws'
DEFAULT_WS_BASE_URL_REAL = 'wss://variableincome-openapi.xpi.com.br:443/ws'

_ws_base_url_override: str | None = None


def set_ws_base_url_override(url: str | None) -> None:
    """
    Define URL base (até ``/ws/v1``) manualmente, ignorando env temporariamente.

    Use ``None`` para voltar ao comportamento padrão (variável de ambiente).
    """
    global _ws_base_url_override
    if url is None or not str(url).strip():
        _ws_base_url_override = None
    else:
        _ws_base_url_override = str(url).strip().rstrip('/')


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        return default
    return value.strip()


def _normalize_ws_base_url(raw: str) -> str:
    base = (raw or '').strip().rstrip('/')
    if base.endswith('/ws'):
        return f'{base}/v1'
    return base


def ws_base_url() -> str:
    if _ws_base_url_override:
        return _normalize_ws_base_url(_ws_base_url_override)
    env = get_current_environment()
    if env == ENV_REAL:
        raw = (
            _env('SMART_TRADER_REAL_WS_BASE_URL', DEFAULT_WS_BASE_URL_REAL)
            or _env('SMART_TRADER_WS_BASE_URL', DEFAULT_WS_BASE_URL_REAL)
            or DEFAULT_WS_BASE_URL_REAL
        )
    else:
        raw = (
            _env('SMART_TRADER_SIM_WS_BASE_URL', DEFAULT_WS_BASE_URL_SIMULATOR)
            or _env('SMART_TRADER_WS_BASE_URL', DEFAULT_WS_BASE_URL_SIMULATOR)
            or DEFAULT_WS_BASE_URL_SIMULATOR
        )
    return _normalize_ws_base_url(raw)
