from __future__ import annotations

import os
from contextvars import ContextVar

ENV_SIMULATOR = 'simulator'
ENV_REAL = 'real'
VALID_ENVIRONMENTS: frozenset[str] = frozenset({ENV_SIMULATOR, ENV_REAL})

SESSION_KEY = 'trader_environment'
_current_env: ContextVar[str] = ContextVar('trader_environment', default=ENV_SIMULATOR)


def normalize_environment(value: str | None) -> str:
    raw = (value or '').strip().lower()
    if raw in VALID_ENVIRONMENTS:
        return raw
    return ENV_SIMULATOR


def default_environment() -> str:
    return normalize_environment(os.environ.get('SMART_TRADER_ENVIRONMENT', ENV_SIMULATOR))


def get_current_environment() -> str:
    return normalize_environment(_current_env.get(default_environment()))


def set_current_environment(value: str | None) -> None:
    _current_env.set(normalize_environment(value))


def get_session_environment(request) -> str:
    """
    Ambiente escolhido na sessão.

    Se o usuário nunca aplicou o seletor (chave ausente), usa ``SMART_TRADER_ENVIRONMENT``
    do processo (via :func:`default_environment`), em vez de forçar simulador.
    Assim o histórico local e a API ficam alinhados ao modo real quando o .env é real.
    """
    session = getattr(request, 'session', None)
    if session is None:
        return default_environment()
    raw = session.get(SESSION_KEY)
    if raw is None:
        return default_environment()
    return normalize_environment(raw)


def set_session_environment(request, value: str | None) -> str:
    env = normalize_environment(value)
    request.session[SESSION_KEY] = env
    return env


def environment_label(env: str) -> str:
    return 'REAL' if normalize_environment(env) == ENV_REAL else 'SIMULADOR'


def order_api_mode_label() -> str:
    """Rótulo REAL ou SIMULADOR alinhado a :func:`get_current_environment` (envio à API)."""
    return environment_label(get_current_environment())
