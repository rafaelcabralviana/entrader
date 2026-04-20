"""Mocks e payloads HTTP reutilizáveis para autenticação."""

from __future__ import annotations

from unittest.mock import MagicMock


def auth_success_payload(
    *,
    access_token: str = 'test-access-token',
    expires_in: int = 120,
) -> dict[str, object]:
    """Corpo JSON típico de sucesso do endpoint de auth."""
    return {
        'access_token': access_token,
        'expires_in': expires_in,
    }


def mock_auth_post_response(
    *,
    status_code: int = 200,
    json_body: dict | None = None,
) -> MagicMock:
    """Monta o objeto retornado por ``requests.post`` nos testes."""
    response = MagicMock()
    response.status_code = status_code
    if json_body is not None:
        response.json.return_value = json_body
    return response
