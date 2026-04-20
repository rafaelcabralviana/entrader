"""
Assinaturas de alto nível (estilo documentação smart_trader_client).

Requer WebSocket já autenticado via ``api_auth``. Use ``initialize_websocket`` para
fixar URL base manualmente ou configure ``SMART_TRADER_WS_BASE_URL`` no ambiente.

Não há equivalente a ``start_simulator()`` / ``start_production()`` neste pacote:
use variáveis de ambiente (``.env``) para simulador vs produção.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

from clearxp_websocket import config
from clearxp_websocket.exceptions import WebSocketNotConnectedError
from clearxp_websocket.protocol import (
    SubscribeAggregateBook,
    SubscribeBook,
    SubscribeQuote,
    subscribe_aggregate_book,
    subscribe_book,
    subscribe_quote,
    subscribe_orders_status,
    unsubscribe_aggregate_book,
    unsubscribe_book,
    unsubscribe_quote,
    unsubscribe_orders_status,
)
from clearxp_websocket.services.client import (
    connect_websocket,
    is_websocket_connected,
    send_message_to_websocket,
)

logger = logging.getLogger(__name__)

_MARKET_SUB_NAMES: frozenset[str] = frozenset(
    {SubscribeQuote, SubscribeBook, SubscribeAggregateBook}
)

_SUBSCRIBE_BUILDERS = {
    SubscribeQuote: subscribe_quote,
    SubscribeBook: subscribe_book,
    SubscribeAggregateBook: subscribe_aggregate_book,
}

_UNSUBSCRIBE_BUILDERS = {
    'UnsubscribeQuote': unsubscribe_quote,
    'UnsubscribeBook': unsubscribe_book,
    'UnsubscribeAggregateBook': unsubscribe_aggregate_book,
}


class _SubscriptionCallbacks:
    marketdata: Callable[[str | None, dict[str, Any]], None] | None = None
    orders: Callable[[str | None, dict[str, Any]], None] | None = None


_callbacks = _SubscriptionCallbacks()


def initialize_websocket(
    ws_url: str,
    on_open_callback: Callable[[], None] | None = None,
    on_message_callback: Callable[[str], None] | None = None,
    version: int = 1,
) -> None:
    """
    Define a URL base do WebSocket (equivalente a ``SMART_TRADER_WS_BASE_URL``).

    Não abre sockets sozinho: após isso use ``sign_ticker`` ou ``connect_websocket``.
    ``on_message_callback`` / ``on_open_callback`` ficam reservados para evolução
    futura (a doc da biblioteca oficial conecta implicitamente).
    """
    if version != 1:
        logger.warning(
            'clearxp_websocket: apenas protocol version 1 é suportada; recebido version=%s',
            version,
        )
    config.set_ws_base_url_override(ws_url)
    if on_open_callback is not None or on_message_callback is not None:
        logger.info(
            'initialize_websocket: callbacks informados serão ignorados nesta versão; '
            'use sign_ticker ou connect_websocket.'
        )


def sign_ticker(
    ticker: str,
    subscriptions: list[str] | None = None,
    marketdata_callback: Callable[[str | None, dict[str, Any]], None] | None = None,
    orders_callback: Callable[[str | None, dict[str, Any]], None] | None = None,
) -> None:
    """
    Assina um ticker (mercado) e opcionalmente status de ordens.

    ``subscriptions``: nomes ``SubscribeQuote`` / ``SubscribeBook`` /
    ``SubscribeAggregateBook``. Se ``None``, usa apenas ``SubscribeQuote``.

    Callbacks recebem ``(target, message)`` onde ``message`` é o dict JSON completo
    da notificação (inclui ``arguments``).
    """
    sym = ticker.strip().upper()
    subs = list(subscriptions) if subscriptions is not None else [SubscribeQuote]

    for name in subs:
        if name not in _MARKET_SUB_NAMES:
            raise ValueError(
                f'Assinatura inválida: {name!r}. Use SubscribeQuote, SubscribeBook ou '
                'SubscribeAggregateBook.'
            )

    if not subs and orders_callback is None:
        raise ValueError('Informe subscriptions de mercado ou orders_callback.')
    if not subs and marketdata_callback is not None:
        raise ValueError('subscriptions vazio não combina com marketdata_callback.')

    _callbacks.marketdata = marketdata_callback
    _callbacks.orders = orders_callback

    def _market_dispatch(raw: str) -> None:
        if not _callbacks.marketdata:
            return
        try:
            msg: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError:
            return
        if msg.get('type') == 6:
            return
        target = msg.get('target')
        if isinstance(target, str) or target is None:
            try:
                _callbacks.marketdata(target, msg)
            except Exception:
                logger.exception('Erro em marketdata_callback.')

    def _market_open() -> None:
        for name in subs:
            send_message_to_websocket('marketdata', _SUBSCRIBE_BUILDERS[name](sym))

    if subs:
        connect_websocket(_market_dispatch, _market_open, 'marketdata')

    def _orders_dispatch(raw: str) -> None:
        if not _callbacks.orders:
            return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if msg.get('type') == 6:
            return
        target = msg.get('target')
        try:
            _callbacks.orders(target, msg)
        except Exception:
            logger.exception('Erro em orders_callback.')

    def _orders_open() -> None:
        send_message_to_websocket('orders', subscribe_orders_status())

    if orders_callback is not None:
        connect_websocket(_orders_dispatch, _orders_open, 'orders')


def unsign_ticker(ticker: str, unsubscriptions: list[str] | None = None) -> None:
    """
    Cancela assinaturas de mercado para o ticker.

    Se ``unsubscriptions`` for ``None``, envia ``UnsubscribeQuote``, ``UnsubscribeBook``
    e ``UnsubscribeAggregateBook``. Não encerra o socket.
    """
    sym = ticker.strip().upper()
    if unsubscriptions is None:
        keys = list(_UNSUBSCRIBE_BUILDERS.keys())
    else:
        keys = list(unsubscriptions)
        for k in keys:
            if k not in _UNSUBSCRIBE_BUILDERS:
                raise ValueError(f'Cancelamento inválido: {k!r}')

    if not is_websocket_connected('marketdata'):
        raise WebSocketNotConnectedError(
            'WebSocket marketdata não está conectado; não é possível desassinar.'
        )

    for key in keys:
        send_message_to_websocket('marketdata', _UNSUBSCRIBE_BUILDERS[key](sym))


def unsign_orders_status() -> None:
    """Cancela assinatura de status de ordens (rota ``orders``)."""
    if not is_websocket_connected('orders'):
        raise WebSocketNotConnectedError(
            'WebSocket orders não está conectado; não é possível desassinar.'
        )
    send_message_to_websocket('orders', unsubscribe_orders_status())
