"""
Sincroniza a simulação de mercado da sessão HTTP para ``AutomationMarketSimPreference``
(tarefas Celery e estratégias sem acesso à sessão).
"""

from __future__ import annotations

from datetime import date

from trader.automacoes.simulation import get_automation_market_simulation
from trader.environment import ENV_SIMULATOR, get_session_environment, normalize_environment
from trader.models import AutomationMarketSimPreference


def sync_automation_sim_preference_from_request(request) -> None:
    """Chamar em views com ``request`` autenticado (ex.: GET ``/automacoes/``)."""
    user = getattr(request, 'user', None)
    if user is None or not getattr(user, 'is_authenticated', False):
        return
    env = get_session_environment(request)
    _sync_for_user_session(user, env, request)


def sync_automation_sim_preference_after_sim_post(request) -> None:
    """Chamar após ``set_automation_market_simulation`` no POST da simulação."""
    user = getattr(request, 'user', None)
    if user is None or not getattr(user, 'is_authenticated', False):
        return
    env = get_session_environment(request)
    _sync_for_user_session(user, env, request)


def clear_automation_sim_preference_for_user(user, trading_environment: str) -> None:
    """Desliga o espelho DB para o par utilizador/ambiente."""
    env = normalize_environment(trading_environment)
    AutomationMarketSimPreference.objects.filter(user=user, trading_environment=env).update(
        enabled=False,
        session_date=None,
        sim_ticker='',
        replay_until=None,
    )


def _sync_for_user_session(user, env: str, request) -> None:
    env = normalize_environment(env)
    if env != ENV_SIMULATOR:
        AutomationMarketSimPreference.objects.filter(user=user, trading_environment=env).update(
            enabled=False,
            session_date=None,
            sim_ticker='',
            replay_until=None,
        )
        return
    sim = get_automation_market_simulation(request)
    if sim.get('effective'):
        sd: date | None = sim.get('session_date')
        sym = (sim.get('sim_ticker') or '').strip().upper()
        AutomationMarketSimPreference.objects.update_or_create(
            user=user,
            trading_environment=env,
            defaults={
                'enabled': True,
                'session_date': sd,
                'sim_ticker': sym,
            },
        )
    else:
        AutomationMarketSimPreference.objects.filter(user=user, trading_environment=env).update(
            enabled=False,
            session_date=None,
            sim_ticker='',
            replay_until=None,
        )
