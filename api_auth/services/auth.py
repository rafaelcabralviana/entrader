from __future__ import annotations

import logging

import requests
from django.core.cache import cache

from api_auth import config
from api_auth.exceptions import (
    SmartTraderAuthError,
    SmartTraderConfigurationError,
)
from trader.environment import get_current_environment

logger = logging.getLogger(__name__)

_TOKEN_BUFFER_SECONDS = 60
_DEFAULT_EXPIRES_IN = 3600


def _cache_key() -> str:
    return f'api_auth:access_token:{get_current_environment()}'


def _load_credentials() -> tuple[str, str, str, str, str]:
    try:
        return (
            config.auth_url(),
            config.subscription_key(),
            config.api_key(),
            config.api_secret(),
            config.user_agent(),
        )
    except ValueError as exc:
        raise SmartTraderConfigurationError(str(exc)) from exc


def get_access_token(*, force_refresh: bool = False) -> str:
    """
    Retorna o access_token da Smart Trader API, usando cache enquanto válido.
    Não registra o token em logs.
    """
    if not force_refresh:
        cached = cache.get(_cache_key())
        if isinstance(cached, str) and cached:
            return cached

    auth_url, subscription_key, api_key, api_secret, user_agent = _load_credentials()

    headers = {
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': subscription_key,
        'User-Agent': user_agent,
    }
    payload = {
        'API_KEY': api_key,
        'API_SECRET': api_secret,
    }

    try:
        response = requests.post(
            auth_url,
            headers=headers,
            json=payload,
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.warning('Falha de rede ao autenticar na Smart Trader API.')
        raise SmartTraderAuthError('Falha de comunicação com o serviço de autenticação.') from exc

    if response.status_code != 200:
        logger.warning(
            'Autenticação Smart Trader rejeitada: status=%s',
            response.status_code,
        )
        raise SmartTraderAuthError(
            'Não foi possível obter o token de acesso. Verifique credenciais e ambiente.'
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise SmartTraderAuthError('Resposta de autenticação inválida.') from exc

    token = data.get('access_token')
    if not token or not isinstance(token, str):
        raise SmartTraderAuthError('Resposta de autenticação sem access_token.')

    expires_in = data.get('expires_in')
    if expires_in is None:
        ttl = _DEFAULT_EXPIRES_IN - _TOKEN_BUFFER_SECONDS
    else:
        try:
            ttl = max(30, int(expires_in) - _TOKEN_BUFFER_SECONDS)
        except (TypeError, ValueError):
            ttl = _DEFAULT_EXPIRES_IN - _TOKEN_BUFFER_SECONDS

    cache.set(_cache_key(), token, ttl)
    return token


def clear_token_cache() -> None:
    """Remove o token em cache (útil em testes ou após revogação)."""
    cache.delete(_cache_key())
