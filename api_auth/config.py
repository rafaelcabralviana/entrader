"""Configuração lida do ambiente (Smart Trader / Clear).

Variáveis relevantes: ``SMART_TRADER_SUBSCRIPTION_KEY``, ``SMART_TRADER_API_KEY``,
``SMART_TRADER_API_SECRET``, ``SMART_TRADER_USER_AGENT``, ``SMART_TRADER_AUTH_URL``,
``SMART_TRADER_API_BASE_URL`` (REST, default simulador), chave RSA para assinatura.
"""
from __future__ import annotations

import base64
import binascii
import os
from pathlib import Path

from trader.environment import (
    ENV_REAL,
    ENV_SIMULATOR,
    get_current_environment,
)

DEFAULT_AUTH_URL = (
    'https://api-parceiros.xpi.com.br/variableincome-openapi-auth/v1/auth'
)
DEFAULT_API_BASE_URL_SIMULATOR = 'https://variableincome-openapi-simulator.xpi.com.br/api'
DEFAULT_API_BASE_URL_REAL = 'https://variableincome-openapi.xpi.com.br/api'
DEFAULT_USER_AGENT = 'Smart-Trader-API Devs-Clear'

# Raiz do repositório (pasta que contém ``manage.py``) — para resolver PEM relativo ao .env
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        return default
    return value.strip()


def _env_by_environment(
    *,
    suffix: str,
    simulator_default: str | None = None,
    real_default: str | None = None,
) -> str | None:
    env = get_current_environment()
    if env == ENV_REAL:
        return _env(f'SMART_TRADER_REAL_{suffix}', real_default)
    return _env(f'SMART_TRADER_SIM_{suffix}', simulator_default)


def subscription_key() -> str:
    key = _env_by_environment(suffix='SUBSCRIPTION_KEY') or _env('SMART_TRADER_SUBSCRIPTION_KEY')
    if not key:
        raise ValueError('SMART_TRADER_SUBSCRIPTION_KEY não configurada.')
    return key


def api_key() -> str:
    key = _env_by_environment(suffix='API_KEY') or _env('SMART_TRADER_API_KEY')
    if not key:
        raise ValueError('SMART_TRADER_API_KEY não configurada.')
    return key


def api_secret() -> str:
    secret = _env_by_environment(suffix='API_SECRET') or _env('SMART_TRADER_API_SECRET')
    if not secret:
        raise ValueError('SMART_TRADER_API_SECRET não configurada.')
    return secret


def user_agent() -> str:
    return (
        _env_by_environment(
            suffix='USER_AGENT',
            simulator_default=DEFAULT_USER_AGENT,
            real_default=DEFAULT_USER_AGENT,
        )
        or _env('SMART_TRADER_USER_AGENT', DEFAULT_USER_AGENT)
        or DEFAULT_USER_AGENT
    )


def auth_url() -> str:
    return (
        _env_by_environment(
            suffix='AUTH_URL',
            simulator_default=DEFAULT_AUTH_URL,
            real_default=DEFAULT_AUTH_URL,
        )
        or _env('SMART_TRADER_AUTH_URL', DEFAULT_AUTH_URL)
        or DEFAULT_AUTH_URL
    )


def api_base_url() -> str:
    """URL base REST (ex.: ``.../api``) para market data, ordens, etc."""
    return (
        _env_by_environment(
            suffix='API_BASE_URL',
            simulator_default=DEFAULT_API_BASE_URL_SIMULATOR,
            real_default=DEFAULT_API_BASE_URL_REAL,
        )
        or _env('SMART_TRADER_API_BASE_URL', DEFAULT_API_BASE_URL_SIMULATOR)
        or DEFAULT_API_BASE_URL_SIMULATOR
    )


def private_rsa_pem_bytes() -> bytes:
    """
    PEM da chave privada RSA (conteúdo bruto, não Base64 do PEM).

    Fontes (use uma): ``SMART_TRADER_PRIVATE_RSA_PEM_B64`` (PEM em Base64,
    uma linha) ou ``SMART_TRADER_PRIVATE_RSA_PATH`` (caminho do ``.pem``).

    Se o caminho for **relativo**, ele é resolvido a partir da **raiz do projeto**
    (pasta do ``manage.py``), não do diretório de trabalho atual do processo.
    """
    b64 = _env_by_environment(suffix='PRIVATE_RSA_PEM_B64') or _env('SMART_TRADER_PRIVATE_RSA_PEM_B64')
    if b64:
        try:
            raw = base64.b64decode(b64.strip(), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(
                'SMART_TRADER_PRIVATE_RSA_PEM_B64 inválido (esperado PEM codificado em Base64).'
            ) from exc
        if not raw.strip():
            raise ValueError('SMART_TRADER_PRIVATE_RSA_PEM_B64 está vazio.')
        return raw

    path = _env_by_environment(suffix='PRIVATE_RSA_PATH') or _env('SMART_TRADER_PRIVATE_RSA_PATH')
    if path:
        pem_path = Path(path).expanduser()
        if not pem_path.is_absolute():
            pem_path = (_PROJECT_ROOT / pem_path).resolve()
        if not pem_path.is_file():
            raise ValueError(
                f'Arquivo de chave RSA não encontrado: {pem_path} '
                f'(raiz do projeto: {_PROJECT_ROOT})'
            )
        return pem_path.read_bytes()

    raise ValueError(
        'Configure SMART_TRADER_PRIVATE_RSA_PATH ou SMART_TRADER_PRIVATE_RSA_PEM_B64 '
        'para gerar BODY_SIGNATURE.'
    )
