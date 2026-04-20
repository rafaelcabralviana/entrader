"""Protocolo de mensagens WebSocket Smart Trader (JSON + separador de registro).

Market Data ``/ws/v1/marketdata`` (documentação oficial):

- Limite: **1 conexão por API key** e **25 ativos por conexão**.
- URL exemplo (simulador):
  ``wss://variableincome-openapi-simulator.xpi.com.br:443/ws/v1/marketdata``
- Headers: ``Authorization: Bearer <access_token>`` (a doc às vezes traz typo
  ``Authorizaton``; o correto é ``Authorization``) e ``User-Agent`` obrigatório.

Ordens (status) ``/ws/v1/orders``:

- Limite: **1 conexão por API key** (socket dedicado; separado do marketdata).
- URL exemplo:
  ``wss://variableincome-openapi-simulator.xpi.com.br:443/ws/v1/orders``
- Mesmos headers (Bearer + User-Agent).
- Cliente → servidor: ``SubscribeOrdersStatus`` / ``UnsubscribeOrdersStatus`` com
  ``"type": 1``, ``"arguments": []`` (ver :func:`subscribe_orders_status`).
- Servidor → cliente: eventos com ``target`` (ex.: ``OrdensStatusUpdate``) e payload
  em ``arguments``; ``"type": 6`` = keep-alive (servidor ativo), sem ``target``.
"""

from __future__ import annotations

import json
from typing import Any, Final, Literal

RECORD_SEPARATOR = '\u001e'

DEFINE_PROTOCOL_MESSAGE: dict[str, Any] = {
    'protocol': 'json',
    'version': 1,
}

WebSocketTarget = Literal[
    'SubscribeQuote',
    'SubscribeBook',
    'SubscribeAggregateBook',
    'SubscribeOrdersStatus',
    'UnsubscribeQuote',
    'UnsubscribeBook',
    'UnsubscribeAggregateBook',
    'UnsubscribeOrdersStatus',
]

WebSocketResponseTarget = Literal[
    'Quote',
    'Book',
    'AggregateBook',
    'OrdensStatus',
    'OrdensStatusUpdate',
]

WebSocketRoute = Literal['marketdata', 'orders']

# Constantes de assinatura (mesmo valor que na API; estilo documentação smart_trader_client).
SubscribeQuote: Final = 'SubscribeQuote'
SubscribeBook: Final = 'SubscribeBook'
SubscribeAggregateBook: Final = 'SubscribeAggregateBook'
UnsubscribeQuote: Final = 'UnsubscribeQuote'
UnsubscribeBook: Final = 'UnsubscribeBook'
UnsubscribeAggregateBook: Final = 'UnsubscribeAggregateBook'


class WebSocketRequestMessage:
    """Formato de mensagem de pedido (cliente → servidor)."""

    def __init__(self, arguments: list[Any], target: WebSocketTarget, msg_type: int):
        self.arguments = arguments
        self.target = target
        self.type = msg_type

    def to_dict(self) -> dict[str, Any]:
        return {
            'arguments': self.arguments,
            'target': self.target,
            'type': self.type,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


def subscribe_quote(ticker: str) -> WebSocketRequestMessage:
    """Assinatura de cotação (Quote)."""
    return WebSocketRequestMessage([ticker], 'SubscribeQuote', 1)


def subscribe_book(ticker: str) -> WebSocketRequestMessage:
    """Assinatura de livro por ordem (Book)."""
    return WebSocketRequestMessage([ticker], 'SubscribeBook', 1)


def subscribe_aggregate_book(ticker: str) -> WebSocketRequestMessage:
    """Assinatura de livro agregado por preço (AggregateBook)."""
    return WebSocketRequestMessage([ticker], 'SubscribeAggregateBook', 1)


def unsubscribe_quote(ticker: str) -> WebSocketRequestMessage:
    return WebSocketRequestMessage([ticker], 'UnsubscribeQuote', 1)


def unsubscribe_book(ticker: str) -> WebSocketRequestMessage:
    return WebSocketRequestMessage([ticker], 'UnsubscribeBook', 1)


def unsubscribe_aggregate_book(ticker: str) -> WebSocketRequestMessage:
    return WebSocketRequestMessage([ticker], 'UnsubscribeAggregateBook', 1)


def subscribe_orders_status() -> WebSocketRequestMessage:
    """Assina atualizações de status de ordens (rota ``orders``)."""
    return WebSocketRequestMessage([], 'SubscribeOrdersStatus', 1)


def unsubscribe_orders_status() -> WebSocketRequestMessage:
    return WebSocketRequestMessage([], 'UnsubscribeOrdersStatus', 1)
