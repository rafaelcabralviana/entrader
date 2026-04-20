class ClearxpWebsocketError(Exception):
    """Erro base do cliente WebSocket Clear/Smart Trader."""


class WebSocketNotConnectedError(ClearxpWebsocketError):
    """Não há conexão ativa para a rota informada."""


class WebSocketSendError(ClearxpWebsocketError):
    """Falha ao enviar mensagem pelo WebSocket."""
