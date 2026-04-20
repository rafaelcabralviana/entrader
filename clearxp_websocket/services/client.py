"""
Cliente WebSocket Smart Trader (rotas marketdata / orders).

Reutiliza token e User-Agent de ``api_auth``. Não registre o token em logs.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

import websocket

from api_auth.config import user_agent as get_user_agent
from api_auth.services.auth import get_access_token
from clearxp_websocket import config
from clearxp_websocket.exceptions import WebSocketNotConnectedError, WebSocketSendError
from clearxp_websocket.protocol import (
    DEFINE_PROTOCOL_MESSAGE,
    RECORD_SEPARATOR,
    WebSocketRequestMessage,
    WebSocketRoute,
)
from trader.environment import get_current_environment

logger = logging.getLogger(__name__)

_connections_lock = threading.Lock()
_ws_connections: dict[str, websocket.WebSocketApp] = {}


def _conn_key(route: WebSocketRoute) -> str:
    return f'{get_current_environment()}:{route}'


def _header_list(token: str, ua: str) -> list[str]:
    return [
        f'Authorization: Bearer {token}',
        f'User-Agent: {ua}',
    ]


def is_websocket_connected(route: WebSocketRoute) -> bool:
    """Indica se a rota já tem socket ativo (após handshake)."""
    with _connections_lock:
        ws = _ws_connections.get(_conn_key(route))
    if ws is None or ws.sock is None:
        return False
    connected = getattr(ws.sock, 'connected', False)
    return connected is True


def connect_websocket(
    on_message_callback: Callable[[str], None],
    on_open_callback: Callable[[], None],
    route: WebSocketRoute,
) -> None:
    """
    Abre WebSocket na rota indicada (thread daemon + ``run_forever``).

    Em ``on_open`` envia ``DEFINE_PROTOCOL_MESSAGE`` e depois chama ``on_open_callback``.

    Se a rota já estiver conectada, não cria nova thread; apenas executa
    ``on_open_callback`` (útil para novas assinaturas).
    """
    if is_websocket_connected(route):
        logger.info('WebSocket route=%s já conectado; executando on_open apenas.', route)
        try:
            on_open_callback()
        except Exception:
            logger.exception('Erro no callback on_open (reuso de conexão).')
        return

    token = get_access_token()
    base = config.ws_base_url().rstrip('/')
    url = f'{base}/{route}'
    ua = get_user_agent()

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        parts = message.split(RECORD_SEPARATOR)
        for raw in parts:
            if raw.strip():
                try:
                    on_message_callback(raw)
                except Exception:
                    logger.exception('Erro no callback on_message do WebSocket.')

    def on_open(ws: websocket.WebSocketApp) -> None:
        try:
            send_message_to_websocket(route, DEFINE_PROTOCOL_MESSAGE)
        except Exception:
            logger.exception('Falha ao enviar mensagem de protocolo inicial.')
            return
        try:
            on_open_callback()
        except Exception:
            logger.exception('Erro no callback on_open do WebSocket.')

    def on_error(ws: websocket.WebSocketApp, error: object) -> None:
        logger.warning('Erro no WebSocket route=%s: %s', route, error)

    def on_close(
        ws: websocket.WebSocketApp,
        close_status_code: int | None,
        close_msg: str | None,
    ) -> None:
        logger.info(
            'WebSocket fechado route=%s code=%s msg=%s',
            route,
            close_status_code,
            close_msg,
        )
        with _connections_lock:
            key = _conn_key(route)
            if _ws_connections.get(key) is ws:
                _ws_connections.pop(key, None)

    ws = websocket.WebSocketApp(
        url,
        header=_header_list(token, ua),
        on_message=on_message,
        on_open=on_open,
        on_close=on_close,
        on_error=on_error,
    )

    with _connections_lock:
        _ws_connections[_conn_key(route)] = ws

    thread = threading.Thread(target=ws.run_forever, daemon=True, name=f'ws-{route}')
    thread.start()
    logger.info('Thread WebSocket iniciada route=%s', route)


def send_message_to_websocket(
    route: WebSocketRoute,
    message: str | dict[str, Any] | WebSocketRequestMessage,
) -> None:
    """Envia mensagem JSON (ou texto) terminada com ``RECORD_SEPARATOR``."""
    if isinstance(message, WebSocketRequestMessage):
        payload = message.to_json()
    elif isinstance(message, dict):
        payload = json.dumps(message)
    else:
        payload = str(message)
    payload += RECORD_SEPARATOR

    with _connections_lock:
        ws = _ws_connections.get(_conn_key(route))

    if ws is None or ws.sock is None or not ws.sock.connected:
        raise WebSocketNotConnectedError(
            f'WebSocket não está conectado para a rota "{route}".'
        )

    try:
        ws.send(payload)
    except Exception as exc:
        logger.warning('Falha ao enviar mensagem WebSocket route=%s', route)
        raise WebSocketSendError('Não foi possível enviar a mensagem.') from exc
