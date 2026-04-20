"""
Utilitários compartilhados pelos testes do app api_auth.

- env: valores de ambiente fictícios (nunca credenciais reais).
- mocks: construção de respostas HTTP e patches comuns.
"""

from api_auth.tests.support.env import default_api_auth_env
from api_auth.tests.support.mocks import auth_success_payload, mock_auth_post_response

__all__ = [
    'auth_success_payload',
    'default_api_auth_env',
    'mock_auth_post_response',
]

