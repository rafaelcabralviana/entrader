"""
Estratégia ativa: usa a mesma leitura de tendência da passiva e, quando forte o suficiente,
pode enviar operação de curto prazo (bracket + trailing), se permitido pelo ambiente e toggles.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

from django.conf import settings
from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone as dj_tz

from trader.automacoes.prefs import (
    get_strategy_params,
    is_strategy_enabled,
    strategy_execute_orders_enabled,
    trailing_stop_adjustment_enabled,
)
from trader.automacoes.profiles import resolve_active_profile
from trader.automacoes.strategy_registry import register_celery_tick, register_evaluator
from trader.automacoes.tendencia_ativa_execution import (
    execute_trend_ativa_bracket,
    execute_trend_ativa_bracket_replay_shadow,
)
from trader.automacoes.execution_guard import (
    count_open_positions,
    has_open_position_for_ticker,
    total_open_quantity_for_ticker,
    try_consume_order_slot_for_round,
)
from trader.automacoes.runtime import runtime_max_open_operations, runtime_max_position_units
from trader.automacoes.bracket_volume_levels import protective_lvn_stop_mid
from trader.automacoes.bracket_width import apply_bracket_distance_multipliers
from trader.automacoes.leafar_vp import compute_volume_profile
from trader.automacoes.universal_bracket_trailing import (
    BRACKET_LANE_REPLAY_SHADOW,
    BRACKET_LANE_STANDARD,
    try_trend_ativa_trailing_stop_update,
)
from trader.automacoes.trend_core import (
    SCORE_THRESHOLD,
    classify_trend,
    range_of_window,
    trend_vote_probability_last_k,
)
from trader.environment import ENV_REAL, ENV_SIMULATOR, order_api_mode_label
from trader.models import AutomationThought, AutomationTriggerMarker
from trader.trading_system.contracts.context import ObservationContext

logger = logging.getLogger(__name__)

# Anti-spam entre disparos (só afecta a **ativa**); valor baixo = mais tentativas de ordem.
_COOLDOWN_SEC = 25

# Cache: último bracket executado por lado (para bloquear inversão Buy↔Sell em segundos).
_BRACKET_LAST_KEY = 'tendencia_ativa:last_bracket:v1'


def _opposite_cooldown_sec() -> int:
    """
    Segundos mínimos entre uma entrada executada e uma entrada no **sentido oposto**
    (evita fechar comprado com venda em ticks consecutivos — o P/L pequeno é isso, não o SL).

    0 = desliga. Predef.: 120 s. Env ou ``TRADER_TENDENCIA_ATIVA_OPPOSITE_COOLDOWN_SEC``.
    """
    raw = getattr(settings, 'TRADER_TENDENCIA_ATIVA_OPPOSITE_COOLDOWN_SEC', None)
    if raw is not None and str(raw).strip() != '':
        try:
            v = int(float(str(raw).replace(',', '.')))
            return max(0, min(v, 7200))
        except (TypeError, ValueError):
            pass
    return 120


def _last_bracket_cache_key(
    env: str,
    user,
    profile,
    sym: str,
    *,
    replay_sim: bool,
) -> str:
    uid = int(getattr(user, 'pk', 0) or 0)
    pid = int(getattr(profile, 'id', 0) or 0)
    lane = 'replay' if replay_sim else 'live'
    return f'{_BRACKET_LAST_KEY}:{env}:{uid}:{pid}:{sym}:{lane}'


def _read_last_bracket(ck: str) -> tuple[str | None, datetime | None]:
    raw = cache.get(ck)
    if not raw or not isinstance(raw, str):
        return None, None
    try:
        o = json.loads(raw)
        side = str(o.get('side') or '').strip()
        ts_raw = o.get('ts')
        if not side or not ts_raw:
            return None, None
        ts = datetime.fromisoformat(str(ts_raw))
        if dj_tz.is_naive(ts):
            ts = dj_tz.make_aware(ts, dj_tz.get_current_timezone())
        return side, ts
    except Exception:
        return None, None


def _record_last_bracket_execution(
    env: str,
    user,
    profile,
    sym: str,
    side: str,
    *,
    replay_sim: bool,
) -> None:
    ck = _last_bracket_cache_key(env, user, profile, sym, replay_sim=replay_sim)
    cache.set(
        ck,
        json.dumps({'side': side, 'ts': dj_tz.now().isoformat()}),
        timeout=48 * 3600,
    )


def _opposite_bracket_blocked(
    env: str,
    user,
    profile,
    sym: str,
    new_side: str,
    *,
    replay_sim: bool,
) -> tuple[bool, str]:
    """(blocked, mensagem curta). Bloqueia só Buy vs Sell alternados dentro do cooldown."""
    sec = _opposite_cooldown_sec()
    if sec <= 0:
        return False, ''
    ck = _last_bracket_cache_key(env, user, profile, sym, replay_sim=replay_sim)
    old_side, ts = _read_last_bracket(ck)
    if not old_side or old_side == new_side:
        return False, ''
    if ts is None:
        return False, ''
    delta = (dj_tz.now() - ts).total_seconds()
    if delta >= sec - 1e-6:
        return False, ''
    remain = max(0.0, sec - delta)
    return True, (
        f'inversão {old_side}→{new_side} bloqueada por {remain:.0f}s '
        f'(mín. {sec:.0f}s após entrada — não é SL curto, é trocar de lado cedo demais)'
    )

# Limiar opcional (modal/settings). Sem valor = **None** → mesmo critério da passiva no gráfico (0,20).
_MIN_EXEC_SCORE_FLOOR = 0.03  # chão ao interpretar limiar vindo do modal/settings

# Últimas N análises (prefixos): predefinição se não vier do modal/settings.
_DEFAULT_TREND_VOTE_K = 5
_MIN_VOTE_K = 3
_MAX_VOTE_K = 15
# Mínimo de prefixos alinhados ao sinal (1 = dispara com o sinal actual + votos coerentes).
_DEFAULT_GROUP_HITS_REQUIRED = 1

# Probabilidade mínima opcional (0 = só o «grupo» decide). Valor >0 exige também essa fração de votos == want.
_DEFAULT_TREND_VOTE_PROB_MIN = 0.0

# Silêncio sem marcação passiva: predefinição **desligada** (None) para não bloquear disparos em teste.
# Defina no modal ou ``TRADER_TENDENCIA_ATIVA_MAX_SILENCE_SEC`` para voltar a filtrar.
_DEFAULT_MAX_SILENCE_WITHOUT_PASSIVE_MARKER_SEC: float | None = None


def _max_silence_sec(params: dict[str, Any] | None) -> float | None:
    raw = (params or {}).get('max_silence_sec')
    if raw is not None and str(raw).strip() != '':
        try:
            v = float(str(raw).replace(',', '.'))
            if v <= 0:
                return None
            return max(60.0, min(v, 7 * 86400.0))
        except (TypeError, ValueError):
            pass
    env = getattr(settings, 'TRADER_TENDENCIA_ATIVA_MAX_SILENCE_SEC', None)
    if env is not None and str(env).strip() != '':
        try:
            ev = float(env)
            if ev <= 0:
                return None
            return max(60.0, min(ev, 7 * 86400.0))
        except (TypeError, ValueError):
            pass
    if _DEFAULT_MAX_SILENCE_WITHOUT_PASSIVE_MARKER_SEC is None:
        return None
    return float(_DEFAULT_MAX_SILENCE_WITHOUT_PASSIVE_MARKER_SEC)


def _ref_datetime(ctx: ObservationContext) -> dj_tz.datetime:
    t = ctx.captured_at
    if t is None:
        return dj_tz.now()
    if dj_tz.is_naive(t):
        return dj_tz.make_aware(t, dj_tz.get_current_timezone())
    return t


def _seconds_since_last_passive_trend_marker(
    user,
    env: str,
    sym: str,
    profile,
    *,
    ref: dj_tz.datetime,
) -> float | None:
    """
    Idade da última :class:`AutomationTriggerMarker` da passiva ``tendencia_mercado``.
    ``None`` = nunca houve marcação (não bloqueia por silêncio — início de sessão / sem histórico).
    """
    if user is None:
        return None
    qs = AutomationTriggerMarker.objects.filter(
        user_id=user.pk,
        trading_environment=env,
        ticker=sym,
        strategy_key='tendencia_mercado',
    )
    if profile is not None:
        qs = qs.filter(Q(execution_profile=profile) | Q(execution_profile__isnull=True))
    else:
        qs = qs.filter(execution_profile__isnull=True)
    row = qs.order_by('-marker_at').only('marker_at').first()
    if row is None:
        return None
    mt = row.marker_at
    if dj_tz.is_naive(mt):
        mt = dj_tz.make_aware(mt, dj_tz.get_current_timezone())
    delta = (ref - mt).total_seconds()
    return max(0.0, float(delta))


def _execution_score_threshold(params: dict[str, Any] | None) -> float | None:
    """
    ``None`` = usar o limiar da passiva (``SCORE_THRESHOLD``, 0,20), alinhado às setas do gráfico.
    """
    raw = (params or {}).get('score_threshold')
    if raw is not None and str(raw).strip() != '':
        try:
            v = float(str(raw).replace(',', '.'))
            return max(_MIN_EXEC_SCORE_FLOOR, min(v, 0.95))
        except (TypeError, ValueError):
            pass
    env = getattr(settings, 'TRADER_TENDENCIA_ATIVA_SCORE_THRESHOLD', None)
    if env is not None and str(env).strip() != '':
        try:
            return max(_MIN_EXEC_SCORE_FLOOR, min(float(env), 0.95))
        except (TypeError, ValueError):
            pass
    return None


def _vote_k_from_params(params: dict[str, Any] | None) -> int:
    raw = (params or {}).get('trend_vote_k')
    if raw is not None and str(raw).strip() != '':
        try:
            k = int(str(raw).strip())
            return max(_MIN_VOTE_K, min(k, _MAX_VOTE_K))
        except (TypeError, ValueError):
            pass
    env = getattr(settings, 'TRADER_TENDENCIA_ATIVA_VOTE_K', None)
    if env is not None and str(env).strip() != '':
        try:
            return max(_MIN_VOTE_K, min(int(env), _MAX_VOTE_K))
        except (TypeError, ValueError):
            pass
    return int(_DEFAULT_TREND_VOTE_K)


def _group_hits_required_from_params(params: dict[str, Any] | None, vote_k: int) -> int:
    """
    Quantas das últimas N análises (prefixos) têm de coincidir com o sinal actual (Alta/Baixa).
    Predef.: 1 em N; aumente no modal para exigir mais confirmações.
    """
    raw = (params or {}).get('trend_group_hits_required')
    if raw is not None and str(raw).strip() != '':
        try:
            n = int(str(raw).strip())
            return max(1, min(n, vote_k))
        except (TypeError, ValueError):
            pass
    env = getattr(settings, 'TRADER_TENDENCIA_ATIVA_GROUP_HITS_REQUIRED', None)
    if env is not None and str(env).strip() != '':
        try:
            return max(1, min(int(env), vote_k))
        except (TypeError, ValueError):
            pass
    return max(1, min(int(_DEFAULT_GROUP_HITS_REQUIRED), vote_k))


def _vote_prob_min_from_params(params: dict[str, Any] | None) -> float:
    raw = (params or {}).get('trend_vote_prob_min')
    if raw is not None and str(raw).strip() != '':
        try:
            v = float(str(raw).replace(',', '.'))
            return max(0.0, min(v, 1.0))
        except (TypeError, ValueError):
            pass
    env = getattr(settings, 'TRADER_TENDENCIA_ATIVA_VOTE_PROB_MIN', None)
    if env is not None and str(env).strip() != '':
        try:
            return max(0.0, min(float(env), 1.0))
        except (TypeError, ValueError):
            pass
    return float(_DEFAULT_TREND_VOTE_PROB_MIN)


def _send_orders_enabled() -> bool:
    return bool(getattr(settings, 'TRADER_TENDENCIA_ATIVA_SEND_ORDERS', False))


def _profile_runtime_started(profile) -> bool:
    """Ordens só depois de «Iniciar execução» no perfil (``execution_started_at``)."""
    return profile is not None and getattr(profile, 'execution_started_at', None) is not None


def _require_profile_started_for_orders() -> bool:
    """``TRADER_TENDENCIA_ATIVA_REQUIRE_PROFILE_STARTED=True`` exige botão Iniciar (predef.: False)."""
    return bool(getattr(settings, 'TRADER_TENDENCIA_ATIVA_REQUIRE_PROFILE_STARTED', False))


def _quantity() -> int:
    return 2


def _tp_sl_from_range(side: str, last: float, rng: float) -> tuple[float, float]:
    """TP e SL proporcionais à amplitude da janela (curto prazo)."""
    if rng <= 1e-12:
        tick = 0.01 if last < 1000 else 0.05
        rng = tick * 80
    tp_frac = float(getattr(settings, 'TRADER_TENDENCIA_ATIVA_TP_FRAC', 0.35))
    sl_frac = float(getattr(settings, 'TRADER_TENDENCIA_ATIVA_SL_FRAC', 0.12))
    if side == 'Buy':
        return last + rng * tp_frac, last - rng * sl_frac
    return last - rng * tp_frac, last + rng * sl_frac


def _price_tick(last: float) -> float:
    return 0.05 if last >= 1000 else 0.01


def _sl_stop_limit_order_prices(side: str, last: float, stop_loss: float) -> tuple[float, float]:
    """Gatilho e preço da ordem SL (igual :mod:`trader.automacoes.tendencia_ativa_execution`)."""
    tick = 0.01 if last < 1000 else 0.05
    exit_side = 'Sell' if side == 'Buy' else 'Buy'
    trig = round(float(stop_loss), 6)
    if exit_side == 'Sell':
        order_px = round(trig - tick, 6)
    else:
        order_px = round(trig + tick, 6)
    return trig, order_px


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    """Só mensagens vêm do celery_tick para evitar duplicar."""
    return None


def run_tendencia_ativa_for_context(ctx: ObservationContext, user, env: str) -> None:
    if not bool(getattr(settings, 'TRADER_TENDENCIA_ATIVA_ENABLED', True)):
        return
    raw = ctx.extra.get('candles')
    candles = raw if isinstance(raw, list) else []
    if len(candles) < 12:
        return
    sym = (ctx.ticker or '').strip().upper()
    profile = resolve_active_profile(user, env) if user is not None else None
    params = get_strategy_params(user, 'tendencia_mercado_ativa', env, execution_profile=profile)
    vote_k = _vote_k_from_params(params)
    # N análises = remove até N-1 barras do fim → precisamos de 12 + (N-1) velas.
    if len(candles) < 12 + vote_k - 1:
        return
    sig_th = _execution_score_threshold(params)
    label, w_used, score = classify_trend(candles, params, score_threshold=sig_th)
    for_live = ctx.data_source == 'live_tail'
    data_label = ctx.data_source or ctx.mode
    env_n = str(env).strip().lower()
    replay_sim = env_n == ENV_SIMULATOR and ctx.data_source == 'session_replay'
    bracket_lane = BRACKET_LANE_REPLAY_SHADOW if replay_sim else BRACKET_LANE_STANDARD

    try:
        last = float(candles[-1]['close'])
    except (TypeError, ValueError, KeyError, IndexError):
        return

    run_in_simulator = env_n == ENV_SIMULATOR
    strategy_on = is_strategy_enabled(
        user,
        'tendencia_mercado_ativa',
        env,
        execution_profile=profile,
    ) and strategy_execute_orders_enabled(
        user,
        'tendencia_mercado_ativa',
        env,
        execution_profile=profile,
    )
    send_allowed = (run_in_simulator or _send_orders_enabled()) and strategy_on
    runtime_started = (
        _profile_runtime_started(profile)
        if _require_profile_started_for_orders()
        else True
    )
    # Ambiente real: só ``live_tail``. Simulador: replay de dia **ou** ao vivo (testes deixam de ficar mudos).
    if env_n == ENV_REAL:
        can_exec = bool(send_allowed and runtime_started and for_live)
    else:
        can_exec = bool(send_allowed and runtime_started)

    if trailing_stop_adjustment_enabled(user, env, execution_profile=profile):
        trail_msg = try_trend_ativa_trailing_stop_update(
            sym, last, bracket_lane=bracket_lane
        )
        if trail_msg:
            try:
                from trader.automacoes.thoughts import record_automation_thought

                record_automation_thought(
                    user,
                    env,
                    f'Tendência ativa {sym}: {trail_msg}',
                    source='tendencia_mercado_ativa',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=profile,
                )
            except Exception:
                logger.exception('tendencia_ativa thought trail')

    if label not in ('Alta', 'Baixa'):
        return

    prob_min = _vote_prob_min_from_params(params)
    hits_need = _group_hits_required_from_params(params, vote_k)
    vote_p, vote_labels = trend_vote_probability_last_k(
        candles,
        params,
        want=label,
        k=vote_k,
        score_threshold=sig_th,
    )
    if not vote_labels:
        return
    hits = sum(1 for lab in vote_labels if lab == label)
    effective_need = min(hits_need, len(vote_labels))
    if hits < effective_need:
        return
    if prob_min > 0 and vote_p + 1e-12 < prob_min:
        return
    vote_compact = '/'.join((v or '?')[0] for v in vote_labels)

    ref = _ref_datetime(ctx)
    max_sil = _max_silence_sec(params)
    age_sec = _seconds_since_last_passive_trend_marker(user, env, sym, profile, ref=ref)
    silence_note = ''
    if max_sil is not None and max_sil > 0:
        if age_sec is not None and age_sec > max_sil:
            sk_notice = f'tendencia_ativa:silence_notice:{env}:{sym}:{getattr(user, "pk", 0)}'
            if cache.add(sk_notice, '1', timeout=120):
                try:
                    from trader.automacoes.thoughts import record_automation_thought

                    record_automation_thought(
                        user,
                        env,
                        f'Tendência ativa [{data_label} · {sym}]: há ~{age_sec:.0f}s sem marcação '
                        f'da passiva no gráfico (limite {max_sil:.0f}s). Ordem não disparada.',
                        source='tendencia_mercado_ativa',
                        kind=AutomationThought.Kind.NOTICE,
                        execution_profile=profile,
                    )
                except Exception:
                    logger.exception('tendencia_ativa silence thought')
            return
        if age_sec is not None:
            silence_note = (
                f' | Últ. marcação passiva: há {age_sec:.0f}s (máx. silêncio {max_sil:.0f}s)'
            )
        else:
            silence_note = ' | Sem marcação passiva anterior (silêncio não bloqueia)'

    side = 'Buy' if label == 'Alta' else 'Sell'
    blocked, block_msg = _opposite_bracket_blocked(
        env, user, profile, sym, side, replay_sim=replay_sim
    )
    if blocked:
        try:
            from trader.automacoes.thoughts import record_automation_thought

            record_automation_thought(
                user,
                env,
                (
                    f'Tendência ativa [{data_label} · {sym}]: {block_msg}. '
                    f'Prejuízo miúdo ao «virar» não é stop curto — é fechar comprado ao abrir venda. '
                    f'Cooldown: TRADER_TENDENCIA_ATIVA_OPPOSITE_COOLDOWN_SEC (0=desliga).'
                )[:3900],
                source='tendencia_mercado_ativa',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=profile,
            )
        except Exception:
            logger.exception('tendencia_ativa opposite cooldown thought')
        return

    rng = range_of_window(candles, w_used)
    tp, sl = _tp_sl_from_range(side, last, rng)
    sl, tp = apply_bracket_distance_multipliers(side, last, sl, tp)
    tick = _price_tick(last)
    try:
        nb = int(getattr(settings, 'TRADER_LEAFAR_VP_BINS', 24))
    except (TypeError, ValueError):
        nb = 24
    nb = max(8, min(64, nb))
    vp = compute_volume_profile(candles, num_bins=nb)
    if vp is not None:
        edges, vols = vp
        min_stop_dist = max(tick * 28.0, abs(tp - last) * 0.92)
        lvn = protective_lvn_stop_mid(
            edges,
            vols,
            last=last,
            side=str(side).lower(),
            min_distance=min_stop_dist * 0.92,
        )
        if lvn is not None:
            if str(side).lower() == 'sell':
                sl = max(float(sl), float(lvn))
            else:
                sl = min(float(sl), float(lvn))
    sl_trig, sl_order_px = _sl_stop_limit_order_prices(side, last, sl)
    exec_note = ''
    th_show = f'{float(sig_th):.3f}' if sig_th is not None else f'padrão={SCORE_THRESHOLD:.2f}'
    if not can_exec:
        if not runtime_started:
            exec_note = (
                ' | Ordens: inicie a execução no perfil (botão Iniciar) ou defina '
                'TRADER_TENDENCIA_ATIVA_REQUIRE_PROFILE_STARTED=False.'
            )
        elif env_n == ENV_REAL and not for_live:
            exec_note = ' | Ordens no real: só com cauda ao vivo (não em replay de sessão).'
        elif not send_allowed:
            if not is_strategy_enabled(
                user, 'tendencia_mercado_ativa', env, execution_profile=profile
            ):
                exec_note = ' | Ordens: estratégia desligada no ambiente.'
            elif not strategy_execute_orders_enabled(
                user, 'tendencia_mercado_ativa', env, execution_profile=profile
            ):
                exec_note = ' | Ordens: marque «executar ordem» na tendência ativa.'
            else:
                exec_note = ' | Ordens: desligadas no servidor (ex.: TRADER_TENDENCIA_ATIVA_SEND_ORDERS).'
    prob_extra = f' · p≥{prob_min:.0%}' if prob_min > 0 else ''
    api_lbl = order_api_mode_label()
    envio_ok = bool(can_exec)
    status_envio = (
        'Ordens podem ir à API neste tick.'
        if envio_ok
        else 'Só aviso no painel — nenhuma ordem vai à API neste tick (motivo em «Ordens» abaixo).'
    )
    summary = (
        f'[API {api_lbl}] {sym} · {data_label} · {label} → {side} | {status_envio} '
        f'Força {score:+.2f} · limiar exec. {th_show} (base passiva {SCORE_THRESHOLD:.2f}) · '
        f'{w_used} velas · conf. {label} {hits}/{len(vote_labels)} (mín. {effective_need}) · '
        f'p {vote_p:.0%}{prob_extra} [{vote_compact}] · fechamento {last:.4f}. '
        f'Entrada a mercado (sem preço fixo). Proteção inicial gerida pelo trailing: '
        f'TP≈{tp:.4f} + SL stop-limit≈{sl_trig:.4f}/{sl_order_px:.4f} (gatilho/ordem). '
        f'Curto prazo; trailing assume TP/SL após executar.'
        f'{silence_note}{exec_note}'
    )
    try:
        from trader.automacoes.thoughts import record_automation_thought

        record_automation_thought(
            user,
            env,
            summary,
            source='tendencia_mercado_ativa',
            kind=AutomationThought.Kind.WARN,
            execution_profile=profile,
        )
    except Exception:
        logger.exception('tendencia_ativa thought')

    try:
        mk_at = _ref_datetime(ctx)
        marker_msg = (
            f'direction={side};entry={last:.4f};last={last:.4f};target={tp:.4f};sl={sl:.4f};'
            f'sl_trigger={sl_trig:.4f}'
        )
        AutomationTriggerMarker.objects.create(
            user=user,
            execution_profile=profile,
            trading_environment=env,
            ticker=sym,
            strategy_key='tendencia_mercado_ativa',
            marker_at=mk_at,
            price=last,
            message=marker_msg[:500],
        )
    except Exception:
        logger.exception('tendencia_ativa marker')

    if not can_exec:
        return

    lane = 'replay_shadow' if replay_sim else 'standard'
    if has_open_position_for_ticker(sym, position_lane=lane):
        try:
            from trader.automacoes.thoughts import record_automation_thought

            record_automation_thought(
                user,
                env,
                (
                    f'Tendência ativa [{data_label} · {sym}] bloqueada: já existe operação ativa '
                    f'({lane}). Feche/liquide antes de nova entrada.'
                )[:3900],
                source='tendencia_mercado_ativa',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=profile,
            )
        except Exception:
            logger.exception('tendencia_ativa thought open position block')
        return

    max_open_ops = runtime_max_open_operations(user, env)
    opened_now = count_open_positions(position_lane=lane)
    max_u = runtime_max_position_units(user, env)
    total_u = total_open_quantity_for_ticker(sym, position_lane=lane)
    if total_u >= max_u:
        try:
            from trader.automacoes.thoughts import record_automation_thought

            record_automation_thought(
                user,
                env,
                (
                    f'Tendência ativa [{data_label} · {sym}] pausada: quantidade em aberto ({total_u}) '
                    f'atingiu o teto ({max_u}).'
                )[:3900],
                source='tendencia_mercado_ativa',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=profile,
            )
        except Exception:
            logging.getLogger(__name__).exception('tendencia_ativa thought max qty block')
        return
    if opened_now >= max_open_ops:
        try:
            from trader.automacoes.thoughts import record_automation_thought

            record_automation_thought(
                user,
                env,
                (
                    f'Tendência ativa [{data_label} · {sym}] pausada: limite de operações abertas '
                    f'atingido ({opened_now}/{max_open_ops}). Foco no trailing até liquidar.'
                )[:3900],
                source='tendencia_mercado_ativa',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=profile,
            )
        except Exception:
            logger.exception('tendencia_ativa thought max open ops block')
        return

    slot_ok, slot_cur, slot_lim = try_consume_order_slot_for_round(
        user=user,
        trading_environment=env,
        execution_profile=profile,
        ctx=ctx,
        max_orders=1,
        strategy_key='tendencia_mercado_ativa',
    )
    if not slot_ok:
        try:
            from trader.automacoes.thoughts import record_automation_thought

            record_automation_thought(
                user,
                env,
                (
                    f'Tendência ativa [{data_label} · {sym}] bloqueada: limite de ordens por rodada '
                    f'atingido ({slot_cur}/{slot_lim}).'
                )[:3900],
                source='tendencia_mercado_ativa',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=profile,
            )
        except Exception:
            logger.exception('tendencia_ativa thought round slot block')
        return

    lock_k = f'tendencia_ativa:signal:{env}:{sym}:{ctx.session_date_iso or "live"}:{side}'
    if not cache.add(lock_k, '1', timeout=_COOLDOWN_SEC):
        return

    ok_exec = False
    if replay_sim:
        ok_exec = bool(
            execute_trend_ativa_bracket_replay_shadow(
                sym,
                side=side,
                last=last,
                take_profit=tp,
                stop_loss=sl,
                quantity=_quantity(),
                log_user=user,
                log_environment=env,
                log_execution_profile=profile,
                log_session_label=data_label,
            )
        )
    else:
        ok_exec = bool(
            execute_trend_ativa_bracket(
                sym,
                side=side,
                last=last,
                take_profit=tp,
                stop_loss=sl,
                quantity=_quantity(),
                log_user=user,
                log_environment=env,
                log_execution_profile=profile,
                log_session_label=data_label,
            )
        )
    if ok_exec:
        _record_last_bracket_execution(
            env, user, profile, sym, side, replay_sim=replay_sim
        )


register_evaluator('tendencia_mercado_ativa', evaluate)
register_celery_tick('tendencia_mercado_ativa', run_tendencia_ativa_for_context)
