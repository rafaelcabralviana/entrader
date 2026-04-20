"""Variáveis de ambiente padronizadas para testes (valores fictícios)."""

from __future__ import annotations

from typing import Any


def default_api_auth_env(**overrides: Any) -> dict[str, str]:
    """
    Retorna o conjunto mínimo de variáveis para o serviço de auth em testes.

    Use ``**overrides`` para simular cenários (ex.: omitir uma chave removendo
    após o merge, ou sobrescrever um valor).
    """
    base: dict[str, str] = {
        'SMART_TRADER_SUBSCRIPTION_KEY': 'test-subscription-key',
        'SMART_TRADER_API_KEY': 'test-api-key',
        'SMART_TRADER_API_SECRET': 'test-api-secret',
        'SMART_TRADER_USER_AGENT': 'Smart-Trader-Tests/1.0',
        'SMART_TRADER_AUTH_URL': 'https://example.test/smart-trader/auth',
    }
    base.update({k: str(v) for k, v in overrides.items()})
    return base
