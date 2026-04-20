"""
Gestão de perfis de execução de automações.
"""

from __future__ import annotations

from django.utils import timezone

from trader.environment import ENV_SIMULATOR, normalize_environment
from trader.models import AutomationExecutionProfile


def get_or_create_default_profile(user, trading_environment: str) -> AutomationExecutionProfile:
    env = normalize_environment(trading_environment)
    p, _ = AutomationExecutionProfile.objects.get_or_create(
        user=user,
        trading_environment=env,
        name='Tempo_Real',
        defaults={
            'mode': AutomationExecutionProfile.Mode.REAL_TIME,
            'is_active': True,
            'is_system_default': True,
        },
    )
    if not AutomationExecutionProfile.objects.filter(
        user=user,
        trading_environment=env,
        is_active=True,
    ).exists():
        p.is_active = True
        p.save(update_fields=['is_active', 'updated_at'])
    return p


def list_profiles(user, trading_environment: str) -> list[AutomationExecutionProfile]:
    env = normalize_environment(trading_environment)
    get_or_create_default_profile(user, env)
    return list(
        AutomationExecutionProfile.objects.filter(
            user=user,
            trading_environment=env,
        ).order_by('-is_active', '-is_system_default', 'name')
    )


def resolve_active_profile(user, trading_environment: str) -> AutomationExecutionProfile:
    env = normalize_environment(trading_environment)
    p = (
        AutomationExecutionProfile.objects.filter(
            user=user,
            trading_environment=env,
            is_active=True,
        )
        .order_by('-updated_at')
        .first()
    )
    if p is not None:
        return p
    return get_or_create_default_profile(user, env)


def set_active_profile(user, trading_environment: str, profile_id: int) -> AutomationExecutionProfile:
    env = normalize_environment(trading_environment)
    target = AutomationExecutionProfile.objects.filter(
        id=profile_id,
        user=user,
        trading_environment=env,
    ).first()
    if target is None:
        return resolve_active_profile(user, env)
    AutomationExecutionProfile.objects.filter(
        user=user,
        trading_environment=env,
        is_active=True,
    ).exclude(id=target.id).update(is_active=False)
    if not target.is_active:
        target.is_active = True
        target.save(update_fields=['is_active', 'updated_at'])
    return target


def create_sim_profile(user, trading_environment: str, *, name: str, sim_ticker: str, session_date):
    env = normalize_environment(trading_environment)
    mode = (
        AutomationExecutionProfile.Mode.SIMULATION
        if env == ENV_SIMULATOR
        else AutomationExecutionProfile.Mode.REAL_TIME
    )
    nm = (name or '').strip()[:64] or 'Simulação'
    p = AutomationExecutionProfile.objects.create(
        user=user,
        trading_environment=env,
        name=nm,
        mode=mode,
        sim_ticker=(sim_ticker or '').strip().upper(),
        session_date=session_date,
        is_active=False,
        is_system_default=False,
    )
    return p


def start_profile_runtime(profile: AutomationExecutionProfile, *, clear_cursor: bool = True) -> None:
    profile.execution_started_at = timezone.now()
    if clear_cursor:
        profile.last_runtime_cursor_at = None
    profile.save(update_fields=['execution_started_at', 'last_runtime_cursor_at', 'updated_at'])

