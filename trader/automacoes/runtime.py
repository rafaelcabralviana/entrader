from __future__ import annotations

from trader.environment import normalize_environment
from trader.models import AutomationRuntimePreference


def _normalize_max_open_operations(v: int | None) -> int:
    try:
        n = int(v or 1)
    except (TypeError, ValueError):
        n = 1
    return max(1, min(n, 10))


def runtime_enabled(user, trading_environment: str | None) -> bool:
    if user is None or not trading_environment:
        return True
    env = normalize_environment(trading_environment)
    row = AutomationRuntimePreference.objects.filter(
        user=user,
        trading_environment=env,
    ).first()
    if row is None:
        return True
    return bool(row.enabled)


def runtime_enabled_map(user_ids: list[int], trading_environment: str) -> dict[int, bool]:
    env = normalize_environment(trading_environment)
    out = {int(uid): True for uid in user_ids}
    rows = AutomationRuntimePreference.objects.filter(
        user_id__in=user_ids,
        trading_environment=env,
    ).values_list('user_id', 'enabled')
    for uid, enabled in rows:
        out[int(uid)] = bool(enabled)
    return out


def runtime_max_open_operations(user, trading_environment: str | None) -> int:
    if user is None or not trading_environment:
        return 1
    env = normalize_environment(trading_environment)
    row = AutomationRuntimePreference.objects.filter(
        user=user,
        trading_environment=env,
    ).first()
    if row is None:
        return 1
    return _normalize_max_open_operations(getattr(row, 'max_open_operations', 1))


# Cada «operação» das estratégias day trade usa até 2 contratos/ações por entrada.
_UNITS_PER_STRATEGY_OPERATION = 2


def runtime_max_position_units(user, trading_environment: str | None) -> int:
    """
    Teto de quantidade total (soma de quantity_open) por ticker/ambiente,
    derivado de max_open_operations × unidades por operação.
    Evita acumular ex.: 12 vendido numa única perna quando o limite era 1 operação.
    """
    mo = runtime_max_open_operations(user, trading_environment)
    return max(1, mo * _UNITS_PER_STRATEGY_OPERATION)


def set_runtime_enabled(
    user,
    trading_environment: str,
    *,
    enabled: bool,
    max_open_operations: int | None = None,
) -> AutomationRuntimePreference | None:
    if user is None:
        return None
    env = normalize_environment(trading_environment)
    defaults = {'enabled': bool(enabled)}
    if max_open_operations is not None:
        defaults['max_open_operations'] = _normalize_max_open_operations(max_open_operations)
    row, _ = AutomationRuntimePreference.objects.update_or_create(
        user=user,
        trading_environment=env,
        defaults=defaults,
    )
    return row
