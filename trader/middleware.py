from __future__ import annotations

from trader.environment import (
    get_session_environment,
    set_current_environment,
)


class TradingEnvironmentMiddleware:
    """
    Define o ambiente ativo por requisição a partir da sessão.

    Assim, qualquer serviço que leia `trader.environment.get_current_environment()`
    passa a operar no contexto correto (simulador/real), sem duplicar fluxo de negócio.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        set_current_environment(get_session_environment(request))
        return self.get_response(request)
