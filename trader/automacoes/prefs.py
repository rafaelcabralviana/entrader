"""
Leitura e gravação de toggles de automação por usuário e ambiente (simulador/real).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.http import QueryDict
from django.db.models import Q

from trader.automacoes.strategies import AUTOMATION_STRATEGIES, AUTOMATION_STRATEGY_KEYS
from trader.environment import normalize_environment, strategy_toggle_storage_environment
from trader.models import AutomationStrategyToggle, AutomationTrailingStopPreference


def _resolve_trailing_pref_row(user, env: str, *, execution_profile=None):
    base = AutomationTrailingStopPreference.objects.filter(
        user=user,
        trading_environment=env,
    )
    if execution_profile is None:
        return base.filter(execution_profile__isnull=True).first()
    exact = base.filter(execution_profile=execution_profile).first()
    if exact is not None:
        return exact
    return base.filter(execution_profile__isnull=True).first()


def trailing_stop_adjustment_enabled(
    user,
    trading_environment: str | None,
    *,
    execution_profile=None,
) -> bool:
    """Ajuste automático de stop (trailing) após bracket; predefinição: ligado."""
    if user is None or not trading_environment:
        return True
    env = normalize_environment(trading_environment)
    row = _resolve_trailing_pref_row(user, env, execution_profile=execution_profile)
    if row is None:
        return True
    return bool(row.adjustment_enabled)


def save_trailing_stop_adjustment_from_post(
    user,
    trading_environment: str,
    post: QueryDict,
    *,
    execution_profile=None,
) -> None:
    if user is None:
        return
    env = normalize_environment(trading_environment)
    AutomationTrailingStopPreference.objects.update_or_create(
        user=user,
        trading_environment=env,
        execution_profile=execution_profile,
        defaults={'adjustment_enabled': post.get('automation_trailing_stop_adjustment') == 'on'},
    )


def _resolve_toggle_row(user, env: str, strategy_key: str, *, execution_profile=None):
    se = strategy_toggle_storage_environment(env)
    base = AutomationStrategyToggle.objects.filter(
        user=user,
        strategy_key=strategy_key,
        trading_environment=se,
    )
    if execution_profile is None:
        return base.filter(execution_profile__isnull=True).first()
    exact = base.filter(execution_profile=execution_profile).first()
    if exact is not None:
        return exact
    return base.filter(execution_profile__isnull=True).first()


def get_strategy_enabled_map(user, trading_environment: str, *, execution_profile=None) -> dict[str, bool]:
    """Mapa ``strategy_key -> enabled`` para o ambiente; padrão ``False`` se não houver linha."""
    env = normalize_environment(trading_environment)
    se = strategy_toggle_storage_environment(env)
    base = AutomationStrategyToggle.objects.filter(
        user=user,
        trading_environment=se,
        strategy_key__in=AUTOMATION_STRATEGY_KEYS,
    )
    if execution_profile is None:
        rows = base.filter(execution_profile__isnull=True).values_list('strategy_key', 'enabled')
        db_map = {k: bool(v) for k, v in rows}
    else:
        rows = base.filter(
            Q(execution_profile=execution_profile) | Q(execution_profile__isnull=True)
        ).values_list('strategy_key', 'enabled', 'execution_profile_id')
        db_map: dict[str, bool] = {}
        # Primeiro aplica legado (null), depois sobrescreve com o perfil ativo.
        for k, v, pid in rows:
            if pid is None and k not in db_map:
                db_map[k] = bool(v)
        for k, v, pid in rows:
            if pid is not None:
                db_map[k] = bool(v)
    return {s['key']: db_map.get(s['key'], False) for s in AUTOMATION_STRATEGIES}


def save_strategy_toggles_from_post(
    user,
    trading_environment: str,
    post: QueryDict,
    *,
    execution_profile=None,
) -> None:
    """Interpreta checkboxes ``strategy_<key>=on`` e persiste para o ambiente indicado."""
    env = normalize_environment(trading_environment)
    se = strategy_toggle_storage_environment(env)
    for s in AUTOMATION_STRATEGIES:
        key = s['key']
        enabled = post.get(f'strategy_{key}') == 'on'
        is_active = str(s.get('automation_role') or 'active').lower() == 'active'
        execute_orders = is_active and (post.get(f'strategy_exec_{key}') == 'on')
        existing = _resolve_toggle_row(user, env, key, execution_profile=execution_profile)
        merge: dict[str, str] = {}
        if existing and isinstance(getattr(existing, 'params_json', None), dict):
            merge = {str(k): str(v) for k, v in existing.params_json.items()}
        if key == 'teste_limite_preco_34':
            raw_lim = (post.get('strategy_param_teste_limite_preco_34_threshold') or '').strip()
            if raw_lim:
                try:
                    lim = Decimal(raw_lim.replace(',', '.'))
                    if lim > 0:
                        merge['threshold'] = f'{lim:.4f}'
                except (InvalidOperation, ValueError):
                    pass
            else:
                merge.pop('threshold', None)
        if key in ('tendencia_mercado', 'tendencia_mercado_ativa'):
            raw_bars = (post.get(f'strategy_param_{key}_analysis_bars') or '').strip()
            if raw_bars:
                try:
                    ib = int(raw_bars)
                    if ib >= 12:
                        merge['analysis_bars'] = str(min(200, ib))
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('analysis_bars', None)
        if key == 'tendencia_mercado_ativa':
            raw_vp = (post.get('strategy_param_tendencia_mercado_ativa_trend_vote_prob_min') or '').strip()
            if raw_vp:
                try:
                    vp = float(raw_vp.replace(',', '.'))
                    if vp > 0:
                        merge['trend_vote_prob_min'] = f'{max(0.0, min(vp, 1.0)):.4f}'.rstrip('0').rstrip('.')
                    else:
                        merge['trend_vote_prob_min'] = '0'
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('trend_vote_prob_min', None)
            merge.pop('min_confirmations', None)
            raw_vk = (post.get('strategy_param_tendencia_mercado_ativa_trend_vote_k') or '').strip()
            if raw_vk:
                try:
                    vk = int(raw_vk)
                    if 3 <= vk <= 15:
                        merge['trend_vote_k'] = str(vk)
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('trend_vote_k', None)
            raw_gh = (post.get('strategy_param_tendencia_mercado_ativa_trend_group_hits_required') or '').strip()
            if raw_gh:
                try:
                    gh = int(raw_gh)
                    if gh >= 1:
                        merge['trend_group_hits_required'] = str(min(gh, 15))
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('trend_group_hits_required', None)
            raw_st = (post.get('strategy_param_tendencia_mercado_ativa_score_threshold') or '').strip()
            if raw_st:
                try:
                    stf = float(raw_st.replace(',', '.'))
                    if 0.05 <= stf <= 0.95:
                        merge['score_threshold'] = f'{stf:.6f}'.rstrip('0').rstrip('.')
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('score_threshold', None)
            raw_ms = (post.get('strategy_param_tendencia_mercado_ativa_max_silence_sec') or '').strip()
            if raw_ms:
                try:
                    ms = float(raw_ms.replace(',', '.'))
                    if ms <= 0:
                        merge['max_silence_sec'] = '0'
                    elif ms >= 60:
                        merge['max_silence_sec'] = str(int(min(ms, 7 * 86400)))
                except (TypeError, ValueError):
                    pass
            else:
                merge.pop('max_silence_sec', None)
        # Mesmo critério de leitura (``get_strategy_enabled_map`` / ``_resolve_toggle_row``):
        # Replay partilha armazenamento com Simulador.
        AutomationStrategyToggle.objects.update_or_create(
            user=user,
            strategy_key=key,
            trading_environment=se,
            execution_profile=execution_profile,
            defaults={
                'enabled': enabled,
                'execute_orders': execute_orders,
                'params_json': merge,
            },
        )
    save_trailing_stop_adjustment_from_post(
        user, env, post, execution_profile=execution_profile
    )


def is_strategy_enabled(
    user,
    strategy_key: str,
    trading_environment: str | None,
    *,
    execution_profile=None,
) -> bool:
    """API para tasks/robô: consulta se a estratégia está ligada para o usuário e ambiente."""
    sk = (strategy_key or '').strip()
    if sk not in AUTOMATION_STRATEGY_KEYS:
        return False
    env = normalize_environment(trading_environment)
    row = _resolve_toggle_row(user, env, sk, execution_profile=execution_profile)
    return bool(getattr(row, 'enabled', False))


def get_strategy_execute_orders_map(
    user,
    trading_environment: str,
    *,
    execution_profile=None,
) -> dict[str, bool]:
    env = normalize_environment(trading_environment)
    out: dict[str, bool] = {}
    for s in AUTOMATION_STRATEGIES:
        key = s['key']
        role = str(s.get('automation_role') or 'active').strip().lower()
        row = _resolve_toggle_row(user, env, key, execution_profile=execution_profile)
        out[key] = bool(getattr(row, 'execute_orders', False)) if role == 'active' else False
    return out


def strategy_execute_orders_enabled(
    user,
    strategy_key: str,
    trading_environment: str | None,
    *,
    execution_profile=None,
) -> bool:
    sk = (strategy_key or '').strip()
    if sk not in AUTOMATION_STRATEGY_KEYS:
        return False
    env = normalize_environment(trading_environment)
    row = _resolve_toggle_row(user, env, sk, execution_profile=execution_profile)
    return bool(getattr(row, 'execute_orders', False))


def get_strategy_params(
    user,
    strategy_key: str,
    trading_environment: str | None,
    *,
    execution_profile=None,
) -> dict[str, str]:
    sk = (strategy_key or '').strip()
    if sk not in AUTOMATION_STRATEGY_KEYS:
        return {}
    env = normalize_environment(trading_environment)
    row = _resolve_toggle_row(user, env, sk, execution_profile=execution_profile)
    raw = getattr(row, 'params_json', None) if row is not None else None
    return raw if isinstance(raw, dict) else {}
