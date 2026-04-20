"""Variáveis globais de template."""

from django.conf import settings

from trader.environment import (
    ENV_REAL,
    ENV_SIMULATOR,
    environment_label,
    get_session_environment,
)
from trader.panel_context import collateral_custody_context_for_template_request


def public_branding(request):
    return {
        'public_site_name': getattr(settings, 'PUBLIC_SITE_NAME', 'Privado'),
    }


def trading_environment(request):
    env = get_session_environment(request)
    return {
        'trading_environment': env,
        'trading_environment_label': environment_label(env),
        'trading_environment_is_real': env == ENV_REAL,
        'trading_environment_is_simulator': env == ENV_SIMULATOR,
    }


def collateral_custody_lists(request):
    """Garantias/custódia no layout (cache ~30s; em miss consulta a API no servidor)."""
    if not request.user.is_authenticated:
        return {}
    return collateral_custody_context_for_template_request(request)
