"""
leafaR: detecção (VP), pensamentos e execução no Celery.

Chamado via ``register_celery_tick`` com ``ObservationContext`` montado pelo
:mod:`trader.automacoes.automation_engine` (ao vivo ou replay na mesma tabela).
"""

from __future__ import annotations

import logging
import json
from dataclasses import replace
from typing import Any

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone as dj_tz
from decimal import Decimal

from trader.automacoes.leafar_execution import (
    execute_leafar_bracket,
    execute_leafar_bracket_replay_shadow,
)
from trader.automacoes.execution_guard import (
    count_open_positions,
    has_open_position_for_ticker,
    release_order_slot_for_round,
    total_open_quantity_for_ticker,
    try_consume_order_slot_for_round,
)
from trader.automacoes.runtime import (
    runtime_max_open_operations,
    runtime_max_position_units,
)
from trader.automacoes.bracket_width import apply_bracket_distance_multipliers
from trader.automacoes.session_range_bracket import (
    adjust_tp_sl_to_session_extremes,
    session_high_low_from_candles,
)
from trader.automacoes.prefs import strategy_execute_orders_enabled, trailing_stop_adjustment_enabled
from trader.automacoes.trend_core import classify_trend
from trader.automacoes.universal_bracket_trailing import (
    BRACKET_LANE_REPLAY_SHADOW,
    BRACKET_LANE_STANDARD,
    state_cache_key,
    try_trailing_stop_update,
)
from trader.automacoes.bracket_volume_levels import protective_lvn_stop_mid
from trader.automacoes.leafar_vp import (
    compute_volume_profile,
    detect_leafar_signal,
    volume_profile_mountains,
)
from trader.automacoes.leafar_candles import (
    load_session_day_candles,
    parse_replay_until_iso,
    trim_candles_to_replay_until,
)
from trader.automacoes.profiles import resolve_active_profile
from trader.automacoes.thoughts import record_automation_thought
from trader.custody_simulator import record_bracket_execution_marker
from trader.environment import (
    ENV_REPLAY,
    ENV_SIMULATOR,
    get_current_environment,
    normalize_environment,
    order_api_mode_label,
)
from trader.models import AutomationThought, AutomationTriggerMarker, Position
from trader.trading_system.contracts.context import ObservationContext

logger = logging.getLogger(__name__)
_LEAFAR_OPEN_OP_KEY = 'leafar:open_op:v1'
_LEAFAR_LOG_THROTTLE_KEY = 'leafar:log_throttle:v1'


def _session_day_from_ctx(ctx: ObservationContext):
    iso = str(getattr(ctx, 'session_date_iso', '') or '').strip()
    if not iso:
        iso = str((ctx.extra or {}).get('session_day_iso') or '').strip() if isinstance(ctx.extra, dict) else ''
    if not iso:
        return dj_tz.now().astimezone().date()
    try:
        from datetime import date
        return date.fromisoformat(iso[:10])
    except Exception:
        return dj_tz.now().astimezone().date()


def _ensure_full_day_candles(
    ctx: ObservationContext,
    sym: str,
    candles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Defesa final: VP coerente com o modo (ao vivo vs replay).

    No **replay** com ``replay_until``, recarrega sempre de ``QuoteSnapshot`` com
    ``captured_at <= replay_until`` — não confiar só em ``candles_full_day`` vindo do
    buffer (evita decisões com dados «do futuro» relativamente ao cursor).
    """
    try:
        full_day_flag = bool((ctx.extra or {}).get('candles_full_day')) if isinstance(ctx.extra, dict) else False
    except Exception:
        full_day_flag = False
    try:
        iv_raw = getattr(settings, 'TRADER_LEAFAR_INTERVAL_SEC', 10)
        iv = max(1, min(int(iv_raw), 300))
    except (TypeError, ValueError):
        iv = 10
    session_day = _session_day_from_ctx(ctx)
    try:
        replay_until = parse_replay_until_iso(getattr(ctx, 'replay_until_iso', None))
    except Exception:
        replay_until = None
    data_source = getattr(ctx, 'data_source', None) or ''

    if str(data_source) == 'session_replay' and replay_until is not None:
        full = load_session_day_candles(
            sym,
            session_day,
            interval_sec=iv,
            replay_until=replay_until,
        )
        return full or trim_candles_to_replay_until(candles or [], replay_until)

    if full_day_flag and candles:
        if replay_until is not None:
            return trim_candles_to_replay_until(candles, replay_until)
        return candles

    full = load_session_day_candles(
        sym,
        session_day,
        interval_sec=iv,
        replay_until=replay_until,
    )
    if replay_until is not None and full:
        full = trim_candles_to_replay_until(full, replay_until)
    return full or candles


def _price_tick(price: float) -> float:
    if price >= 1000:
        return 5.0
    if price >= 100:
        return 0.05
    return 0.01


def _best_volume_magnet_price(
    edges: list[float],
    vols: list[float],
    *,
    last: float,
    side: str,
) -> float | None:
    """
    Preço médio do bin com **maior volume** na direção da reversão:
    - venda (preço acima do eixo): íman **abaixo** do último (liquidez / POC visual);
    - compra: íman **acima** do último.
    """
    n = len(vols)
    if n < 1 or len(edges) != n + 1:
        return None
    best_v = -1.0
    best_mid: float | None = None
    for i in range(n):
        mid = (edges[i] + edges[i + 1]) / 2.0
        v = float(vols[i])
        if side == 'sell':
            if mid < last - 1e-9 and v > best_v:
                best_v = v
                best_mid = mid
        else:
            if mid > last + 1e-9 and v > best_v:
                best_v = v
                best_mid = mid
    return best_mid


def _major_volume_price(candles: list[dict[str, Any]]) -> float | None:
    """
    Preço médio do bin #1 de volume, compatível com o VP visual do gráfico:
    - fonte histórica: cauda de até 500 candles;
    - faixa de preço para binning: janela visível (cauda de 120 candles),
      com closes históricos fora da faixa sendo "clipados" para os bins de borda.
    """
    if not candles:
        return None
    nb = _leafar_detection_kwargs()['num_bins']
    hist = candles[-500:] if len(candles) > 500 else candles
    vis = candles[-120:] if len(candles) > 120 else candles
    lows: list[float] = []
    highs: list[float] = []
    for c in vis:
        try:
            lo = float(c.get('low'))
            hi = float(c.get('high'))
        except (TypeError, ValueError):
            continue
        if hi < lo:
            lo, hi = hi, lo
        lows.append(lo)
        highs.append(hi)
    if not lows or not highs:
        return None
    p_min = min(lows)
    p_max = max(highs)
    if not (p_max > p_min):
        p_max = p_min + 1e-6
    span = p_max - p_min
    edges = [p_min + i * span / nb for i in range(nb + 1)]
    vols = [0.0] * nb
    for c in hist:
        try:
            cl = float(c.get('close'))
            v = float(c.get('volume') or 0.0)
        except (TypeError, ValueError):
            continue
        if v <= 0:
            continue
        idx = int((cl - p_min) / span * nb)
        idx = max(0, min(nb - 1, idx))
        vols[idx] += v
    if not vols:
        return None
    try:
        idx = max(range(len(vols)), key=lambda i: float(vols[i]))
    except ValueError:
        return None
    if idx < 0 or idx + 1 >= len(edges):
        return None
    return float((edges[idx] + edges[idx + 1]) / 2.0)


def _adjust_levels_to_day_volume(sig, candles: list[dict[str, Any]]) -> tuple[float, float]:
    """
    Alvo (gain): nível de maior volume no VP **na direção do trade** (íman — como o #1 no gráfico).
    Stop (perda): lado protetor (acima na venda, abaixo na compra), com folga mínima vs. distância ao alvo.
    Usa o mesmo ``num_bins`` da detecção para coerência com o gráfico.
    """
    target = float(sig.take_profit)
    stop = float(sig.stop_loss)
    last = float(sig.last)
    side = str(sig.side).lower()
    nb = _leafar_detection_kwargs()['num_bins']
    vp = compute_volume_profile(candles, num_bins=nb)
    if vp is not None:
        edges, vols = vp
        magnet = _best_volume_magnet_price(edges, vols, last=last, side=side)
        if magnet is not None:
            target = float(magnet)
        elif vols:
            poc_i = max(range(len(vols)), key=lambda i: vols[i])
            poc_mid = float((edges[poc_i] + edges[poc_i + 1]) / 2.0)
            if side == 'sell' and poc_mid < last:
                target = poc_mid
            elif side == 'buy' and poc_mid > last:
                target = poc_mid
            else:
                mountains = volume_profile_mountains(
                    edges,
                    vols,
                    max_mountains=8,
                    min_relative_peak=0.05,
                    min_bin_separation=1,
                )
                if mountains:
                    if side == 'sell':
                        cands = [(px, vol) for px, vol in mountains if px < last - 1e-9]
                    else:
                        cands = [(px, vol) for px, vol in mountains if px > last + 1e-9]
                    if cands:
                        cands.sort(key=lambda it: it[1], reverse=True)
                        target = float(cands[0][0])

    tick = _price_tick(last)
    dist_target = abs(last - target)
    # Folga mínima maior + ancora opcional em LVN (menor volume) no lado do stop.
    min_stop_dist = max(tick * 28.0, dist_target * 0.92)
    if side == 'sell':
        if target >= last:
            target = float(sig.take_profit)
        if target >= last:
            target = last - tick * 10
        stop = max(float(stop), last + min_stop_dist)
    else:
        if target <= last:
            target = float(sig.take_profit)
        if target <= last:
            target = last + tick * 10
        stop = min(float(stop), last - min_stop_dist)
    if vp is not None:
        edges, vols = vp
        lvn = protective_lvn_stop_mid(
            edges,
            vols,
            last=last,
            side=side,
            min_distance=min_stop_dist * 0.92,
        )
        if lvn is not None:
            if side == 'sell':
                stop = max(float(stop), float(lvn))
            else:
                stop = min(float(stop), float(lvn))
    bounds = session_high_low_from_candles(candles)
    if bounds is not None:
        d_hi, d_lo = bounds
        target, stop = adjust_tp_sl_to_session_extremes(
            side, last, target, stop, d_hi, d_lo, tick
        )
    return target, stop


def _leafar_summary(sym: str, sig, data_label: str) -> str:
    is_buy = str(sig.side).lower() == 'buy'
    signal = 'COMPRA' if is_buy else 'VENDA'
    direction = 'alta' if is_buy else 'baixa'
    return (
        f'leafaR [{data_label} · {sym}] | Sinal: {signal} ({direction}) | '
        f'Agora: {sig.last:.4f} | Alvo (POC): {sig.take_profit:.4f} | '
        f'Stop: {sig.stop_loss:.4f}. {sig.reason}'
    )


def _leafar_enabled() -> bool:
    return bool(getattr(settings, 'TRADER_LEAFAR_ENABLED', True))


def _quantity(user, env: str) -> int:
    """
    Quantidade por entrada; limitada pelo máx. operações abertas (robô) e por ``TRADER_LEAFAR_QUANTITY``.
    """
    try:
        q = int(getattr(settings, 'TRADER_LEAFAR_QUANTITY', 1))
    except (TypeError, ValueError):
        q = 1
    q = max(1, q)
    mo = runtime_max_open_operations(user, env)
    return max(1, min(q, mo))


def _cooldown_sec() -> int:
    try:
        return int(getattr(settings, 'TRADER_LEAFAR_COOLDOWN_SEC', 240))
    except (TypeError, ValueError):
        return 240


def _leafar_detection_kwargs() -> dict:
    """Parâmetros aproximados configuráveis (``settings``); defaults mais permissivos."""

    def _i(name: str, default: int) -> int:
        try:
            return int(getattr(settings, name, default))
        except (TypeError, ValueError):
            return default

    def _f(name: str, default: float) -> float:
        try:
            return float(getattr(settings, name, default))
        except (TypeError, ValueError):
            return default

    return {
        # Alinhado ao VP visual do gráfico (default 24 bins no painel).
        'num_bins': max(8, min(64, _i('TRADER_LEAFAR_VP_BINS', 24))),
        'min_bins_from_poc': max(0, _i('TRADER_LEAFAR_MIN_BINS_FROM_POC', 1)),
        'low_corridor_ratio': max(0.05, min(0.95, _f('TRADER_LEAFAR_VP_CORRIDOR_RATIO', 0.38))),
        'min_candles': max(20, _i('TRADER_LEAFAR_MIN_CANDLES', 42)),
        'trend_window': max(3, min(20, _i('TRADER_LEAFAR_TREND_WINDOW', 7))),
        'trend_min_frac': max(0.35, min(0.95, _f('TRADER_LEAFAR_TREND_MIN_FRAC', 0.48))),
        'min_price_sep_frac': max(0.0, min(0.08, _f('TRADER_LEAFAR_MIN_PRICE_SEP_FRAC', 0.006))),
        'session_local_sep_frac': max(0.05, min(0.55, _f('TRADER_LEAFAR_SESSION_LOCAL_SEP_FRAC', 0.22))),
        # Filtros de cautela (defaults moderados para manter frequência viável).
        'poc_stability_bars': max(1, min(8, _i('TRADER_LEAFAR_POC_STABILITY_BARS', 2))),
        'poc_dominance_ratio': max(1.0, min(2.2, _f('TRADER_LEAFAR_POC_DOMINANCE_RATIO', 1.08))),
        'persistence_bars': max(1, min(8, _i('TRADER_LEAFAR_PERSISTENCE_BARS', 2))),
        'min_recent_range_ticks': max(0, min(120, _i('TRADER_LEAFAR_MIN_RECENT_RANGE_TICKS', 8))),
        'min_session_minutes': max(0, min(120, _i('TRADER_LEAFAR_MIN_SESSION_MINUTES', 18))),
    }


def _leafar_trend_bias_enabled() -> bool:
    return bool(getattr(settings, 'TRADER_LEAFAR_TREND_BIAS_ENABLED', True))


def _leafar_trend_bias_min_score() -> float:
    try:
        return float(getattr(settings, 'TRADER_LEAFAR_TREND_BIAS_MIN_SCORE', 0.12))
    except (TypeError, ValueError):
        return 0.12


def _override_side_with_hvn_anchor(sig, passive_context: dict[str, Any] | None):
    """
    Quando houver HVN passivo dominante, usa esse nível também para decidir o lado.
    """
    if not isinstance(passive_context, dict):
        return sig, ''
    mts = passive_context.get('hvn_mountains')
    if not isinstance(mts, list) or not mts:
        return sig, ''
    parsed: list[tuple[float, float]] = []
    for item in mts:
        if not isinstance(item, dict):
            continue
        try:
            px = float(item.get('price'))
            vol = float(item.get('volume'))
        except (TypeError, ValueError):
            continue
        if px <= 0 or vol <= 0:
            continue
        parsed.append((px, vol))
    if not parsed:
        return sig, ''
    parsed.sort(key=lambda it: it[1], reverse=True)
    major_px, major_vol = parsed[0]
    last = float(sig.last)
    if abs(major_px - last) <= (_price_tick(last) * 0.75):
        return sig, ''
    forced_side = 'Sell' if major_px < last else 'Buy'
    if str(sig.side).lower() == forced_side.lower():
        return sig, ''
    new_sig = replace(sig, side=forced_side, take_profit=float(major_px))
    note = (
        f'HVN passivo dominante forçou lado: {forced_side.upper()} '
        f'(HVN#1≈{major_px:.4f}, vol≈{major_vol:.0f}, last≈{last:.4f}).'
    )
    return new_sig, note


def _override_side_with_day_hvn(sig, *, hvn1_price: float | None):
    """
    Regra principal da leafaR:
    - preço acima do HVN #1 do dia => SELL
    - preço abaixo do HVN #1 do dia => BUY
    """
    if hvn1_price is None:
        return sig, ''
    last = float(sig.last)
    hvn1 = float(hvn1_price)
    if abs(hvn1 - last) <= (_price_tick(last) * 0.75):
        return sig, ''
    forced_side = 'Sell' if hvn1 < last else 'Buy'
    if str(sig.side).lower() == forced_side.lower() and abs(float(sig.take_profit) - hvn1) <= 1e-9:
        return sig, ''
    new_sig = replace(sig, side=forced_side, take_profit=hvn1, poc=hvn1)
    note = (
        f'HVN#1 do dia forçou sinal: {forced_side.upper()} '
        f'(hvn1≈{hvn1:.4f}, last≈{last:.4f}).'
    )
    return new_sig, note


def _leafar_open_op_cache_key(env: str, sym: str, lane: str) -> str:
    env_n = (env or '').strip().lower() or 'simulator'
    return f'{_LEAFAR_OPEN_OP_KEY}:{env_n}:{sym}:{lane}'


def _leafar_track_open_operation(
    *,
    env: str,
    sym: str,
    lane: str,
    market_order_id: str | None,
) -> None:
    op = str(market_order_id or '').strip()
    if not op:
        return
    cache.set(_leafar_open_op_cache_key(env, sym, lane), op, timeout=12 * 3600)


def _leafar_get_open_operation(env: str, sym: str, lane: str) -> str | None:
    raw = cache.get(_leafar_open_op_cache_key(env, sym, lane))
    op = str(raw or '').strip()
    return op or None


def _leafar_clear_open_operation(env: str, sym: str, lane: str) -> None:
    cache.delete(_leafar_open_op_cache_key(env, sym, lane))


def _leafar_operation_state_alive(env: str, sym: str, lane: str, open_op: str | None) -> bool:
    """
    Valida se o lock da operação ainda representa uma operação realmente aberta.
    """
    op = str(open_op or '').strip()
    if not op:
        return False
    env_n = str(env or '').strip().lower() or 'simulator'
    lane_n = (lane or '').strip() or Position.Lane.STANDARD
    raw = cache.get(state_cache_key(sym, bracket_lane=lane_n, trading_environment=env_n))
    if not isinstance(raw, str) or not raw.strip():
        return False
    try:
        st = json.loads(raw)
    except Exception:
        return False
    if not isinstance(st, dict):
        return False
    if bool(st.get('force_close_done')):
        return False
    st_op = str(st.get('operation_id') or st.get('market_order_id') or '').strip()
    if not st_op or st_op != op:
        return False
    return has_open_position_for_ticker(sym, position_lane=lane_n)


def _leafar_log_allowed(
    *,
    env: str,
    sym: str,
    data_label: str,
    category: str,
    signature: str,
    timeout_sec: int = 18,
) -> bool:
    ck = (
        f'{_LEAFAR_LOG_THROTTLE_KEY}:{(env or "").strip().lower()}:'
        f'{sym}:{(data_label or "").strip()}:{category}:{signature}'
    )
    return bool(cache.add(ck, '1', timeout=max(4, int(timeout_sec))))


def _leafar_try_heal_stale_open_position(env: str, sym: str, lane: str) -> int:
    """
    Se houver posição ativa no ledger, mas sem lock da leafaR e sem estado de bracket,
    trata como resíduo e fecha localmente para evitar bloqueio fantasma.
    """
    env_n = str(env or '').strip().lower() or 'simulator'
    lane_n = (lane or '').strip() or Position.Lane.STANDARD
    br_raw = cache.get(state_cache_key(sym, bracket_lane=lane_n, trading_environment=env_n))
    if isinstance(br_raw, str) and br_raw.strip():
        return 0
    qs = Position.objects.filter(
        ticker=sym,
        trading_environment=env_n,
        position_lane=lane_n,
        is_active=True,
        closed_at__isnull=True,
        quantity_open__gt=Decimal('0.000001'),
    )
    if not qs.exists():
        return 0
    now = dj_tz.now()
    changed = 0
    for p in qs:
        p.is_active = False
        p.closed_at = now
        p.quantity_open = Decimal('0')
        p.save(update_fields=['is_active', 'closed_at', 'quantity_open', 'updated_at'])
        changed += 1
    if changed > 0:
        _leafar_clear_open_operation(env_n, sym, lane_n)
    return changed


def _process_signal(
    ctx: ObservationContext,
    sym: str,
    candles: list[dict[str, Any]],
    user,
    env: str,
    *,
    for_live_tail: bool,
    lock_suffix: str,
    data_label: str,
    can_execute_orders: bool,
) -> None:
    if user is None:
        return
    candles = _ensure_full_day_candles(ctx, sym, candles or [])
    if not candles:
        return
    execution_profile = resolve_active_profile(user, env) if user is not None else None
    try:
        last = float(candles[-1]['close'])
    except (TypeError, ValueError, KeyError, IndexError):
        return
    replay_sim = (not for_live_tail) and normalize_environment(env) == ENV_REPLAY
    trail_lane = BRACKET_LANE_REPLAY_SHADOW if replay_sim else BRACKET_LANE_STANDARD
    lane = 'replay_shadow' if replay_sim else 'standard'
    if trailing_stop_adjustment_enabled(
        user, env, execution_profile=execution_profile
    ):
        trail_msg = try_trailing_stop_update(sym, last, bracket_lane=trail_lane)
        if trail_msg:
            try:
                record_automation_thought(
                    user, env, f'leafaR {sym}: {trail_msg}', source='leafar'
                )
            except Exception:
                logger.exception('leafar thought trail')

    sig = detect_leafar_signal(candles, **_leafar_detection_kwargs())
    if sig is None:
        return
    passive_ctx = (ctx.extra or {}).get('passive_context') if isinstance(ctx.extra, dict) else None
    major_vol_px = _major_volume_price(candles)
    sig, hvn_day_note = _override_side_with_day_hvn(sig, hvn1_price=major_vol_px)
    hvn_side_note = ''
    trend_label = ''
    trend_score = 0.0
    if _leafar_trend_bias_enabled():
        try:
            trend_label, _, trend_score = classify_trend(candles, params={})
        except Exception:
            trend_label = ''
            trend_score = 0.0
    is_buy = str(sig.side).strip().lower() == 'buy'
    aligns_trend = (is_buy and trend_label == 'Alta') or ((not is_buy) and trend_label == 'Baixa')
    opposes_trend = (is_buy and trend_label == 'Baixa') or ((not is_buy) and trend_label == 'Alta')
    trend_score_abs = abs(float(trend_score))
    trend_block = bool(
        _leafar_trend_bias_enabled()
        and opposes_trend
        and trend_score_abs >= max(0.05, _leafar_trend_bias_min_score())
    )
    target_adj, stop_adj = _adjust_levels_to_day_volume(sig, candles)
    # Estratégia principal: alvo no HVN #1 do dia.
    if major_vol_px is not None and abs(float(major_vol_px)) > 0:
        target_adj = float(major_vol_px)
    hvn_note = (
        f'HVN #1 dia: alvo≈{float(major_vol_px):.4f}.'
        if major_vol_px is not None
        else ''
    )
    stop_adj, target_adj = apply_bracket_distance_multipliers(
        sig.side, float(sig.last), float(stop_adj), float(target_adj)
    )

    # Se já houver operação aberta (ou lock da operação anterior), não gera novo "sinal"
    # para evitar spam de compra/venda enquanto a estratégia aguarda liquidação.
    healed = _leafar_try_heal_stale_open_position(env, sym, lane)
    if healed > 0:
        try:
            record_automation_thought(
                user,
                env,
                (
                    f'leafaR [{data_label} · {sym}] limpeza automática: removido bloqueio de '
                    f'operação ativa residual ({healed}).'
                )[:3900],
                source='leafar',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=execution_profile,
            )
        except Exception:
            logger.exception('leafar thought stale heal')
    open_op = _leafar_get_open_operation(env, sym, lane)
    if open_op and not _leafar_operation_state_alive(env, sym, lane, open_op):
        _leafar_clear_open_operation(env, sym, lane)
        open_op = None
    if open_op:
        if _leafar_log_allowed(
            env=env,
            sym=sym,
            data_label=data_label,
            category='open_op_lock',
            signature=open_op,
            timeout_sec=20,
        ):
            try:
                record_automation_thought(
                    user,
                    env,
                    (
                        f'leafaR [{data_label} · {sym}] aguardando liquidação da operação anterior '
                        f'(id={open_op}). Nova entrada bloqueada.'
                    )[:3900],
                    source='leafar',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=execution_profile,
                )
            except Exception:
                logger.exception('leafar thought previous op lock')
        return
    if has_open_position_for_ticker(sym, position_lane=lane):
        healed_after_block = _leafar_try_heal_stale_open_position(env, sym, lane)
        if healed_after_block > 0 and not has_open_position_for_ticker(sym, position_lane=lane):
            if _leafar_log_allowed(
                env=env,
                sym=sym,
                data_label=data_label,
                category='open_position_heal_after_block',
                signature=str(healed_after_block),
                timeout_sec=18,
            ):
                try:
                    record_automation_thought(
                        user,
                        env,
                        (
                            f'leafaR [{data_label} · {sym}] limpou bloqueio órfão de posição '
                            f'ativa ({healed_after_block}) e liberou nova entrada.'
                        )[:3900],
                        source='leafar',
                        kind=AutomationThought.Kind.NOTICE,
                        execution_profile=execution_profile,
                    )
                except Exception:
                    logger.exception('leafar thought open position post-heal')
            # posição fantasma removida; segue fluxo normal sem bloquear
        else:
            if _leafar_log_allowed(
                env=env,
                sym=sym,
                data_label=data_label,
                category='open_position_block',
                signature=lane,
                timeout_sec=20,
            ):
                try:
                    record_automation_thought(
                        user,
                        env,
                        (
                            f'leafaR [{data_label} · {sym}] bloqueado: já existe operação ativa '
                            f'({lane}). Feche/liquide antes de nova entrada.'
                        )[:3900],
                        source='leafar',
                        kind=AutomationThought.Kind.NOTICE,
                        execution_profile=execution_profile,
                    )
                except Exception:
                    logger.exception('leafar thought open position block')
            return

    max_open_ops = runtime_max_open_operations(user, env)
    opened_now = count_open_positions(position_lane=lane)
    max_u = runtime_max_position_units(user, env)
    total_u = total_open_quantity_for_ticker(sym, position_lane=lane)
    if total_u >= max_u:
        if _leafar_log_allowed(
            env=env,
            sym=sym,
            data_label=data_label,
            category='max_qty_block',
            signature=f'{total_u}/{max_u}:{lane}',
            timeout_sec=20,
        ):
            try:
                record_automation_thought(
                    user,
                    env,
                    (
                        f'leafaR [{data_label} · {sym}] pausada: quantidade em aberto ({total_u}) '
                        f'atingiu o teto ({max_u}) para este limite. Nada de novas entradas até reduzir.'
                    )[:3900],
                    source='leafar',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=execution_profile,
                )
            except Exception:
                logger.exception('leafar thought max qty block')
        return
    if opened_now >= max_open_ops:
        if _leafar_log_allowed(
            env=env,
            sym=sym,
            data_label=data_label,
            category='max_open_ops_block',
            signature=f'{opened_now}/{max_open_ops}:{lane}',
            timeout_sec=20,
        ):
            try:
                record_automation_thought(
                    user,
                    env,
                    (
                        f'leafaR [{data_label} · {sym}] pausada: limite de operações abertas '
                        f'atingido ({opened_now}/{max_open_ops}). Foco no trailing até liquidar.'
                    )[:3900],
                    source='leafar',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=execution_profile,
                )
            except Exception:
                logger.exception('leafar thought max open ops block')
        return

    summary = _leafar_summary(sym, sig, data_label)
    summary = summary.replace(
        f'Alvo (POC): {sig.take_profit:.4f} | Stop: {sig.stop_loss:.4f}',
        f'Alvo (VP dia): {target_adj:.4f} | Stop: {stop_adj:.4f}',
    )
    if trend_label:
        trend_tag = f'Tendência: {trend_label} ({trend_score:+.2f})'
        trend_weight = 'alinhada' if aligns_trend else ('contra' if opposes_trend else 'neutra')
        summary = f'{summary} | {trend_tag} | Peso tendência: {trend_weight}.'
    if hvn_note:
        summary = f'{summary} | {hvn_note}.'
    if hvn_day_note:
        summary = f'{summary} | {hvn_day_note}'
    if hvn_side_note:
        summary = f'{summary} | {hvn_side_note}'
    sig_signature = (
        f'{str(sig.side).upper()}:{float(sig.last):.3f}:{float(target_adj):.3f}:{float(stop_adj):.3f}'
    )
    if _leafar_log_allowed(
        env=env,
        sym=sym,
        data_label=data_label,
        category='signal',
        signature=sig_signature,
        timeout_sec=20,
    ):
        try:
            record_automation_thought(
                user,
                env,
                summary,
                source='leafar',
                kind=AutomationThought.Kind.WARN,
                execution_profile=execution_profile,
            )
        except Exception:
            logger.exception('leafar thought signal')
        try:
            major_vol_s = f'{float(major_vol_px):.4f}' if major_vol_px is not None else ''
            marker_msg = (
                f'direction={sig.side};entry={sig.last:.4f};last={sig.last:.4f};target={target_adj:.4f};'
                f'poc={sig.poc:.4f};sl={stop_adj:.4f};vol_major={major_vol_s}'
            )
            AutomationTriggerMarker.objects.create(
                user=user,
                execution_profile=execution_profile,
                trading_environment=env,
                ticker=sym,
                strategy_key='leafar',
                marker_at=(ctx.captured_at or dj_tz.now()),
                price=float(sig.last),
                message=marker_msg[:500],
            )
        except Exception:
            logger.exception('leafar marker signal')

    if not can_execute_orders:
        try:
            record_automation_thought(
                user,
                env,
                (
                    f'leafaR [{data_label} · {sym}] sinal detectado, '
                    'mas envio de ordem está desmarcado no flag "executar ordem".'
                )[:3900],
                source='leafar',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=execution_profile,
            )
        except Exception:
            logger.exception('leafar thought blocked execute')
        return
    if trend_block:
        if _leafar_log_allowed(
            env=env,
            sym=sym,
            data_label=data_label,
            category='trend_block',
            signature=f'{trend_label}:{float(trend_score):.2f}',
            timeout_sec=24,
        ):
            try:
                record_automation_thought(
                    user,
                    env,
                    (
                        f'leafaR [{data_label} · {sym}] envio bloqueado: tendência de mercado '
                        f'forte contra o sinal ({trend_label} {trend_score:+.2f}).'
                    )[:3900],
                    source='leafar',
                    kind=AutomationThought.Kind.NOTICE,
                    execution_profile=execution_profile,
                )
            except Exception:
                logger.exception('leafar thought trend block')
        return

    event_tag = ''
    if isinstance(candles, list) and candles:
        last_c = candles[-1] if isinstance(candles[-1], dict) else {}
        event_tag = str(last_c.get('bucket_start') or '').strip()
    if not event_tag:
        event_tag = str(ctx.replay_until_iso or ctx.captured_at or '').strip()
    if not event_tag:
        event_tag = 'event'
    lock_k = f'leafar:signal_lock:{env}:{sym}:{lock_suffix}:{event_tag}'
    cooldown = _cooldown_sec()
    if not for_live_tail:
        # Em replay: no máximo 1 por chamada/evento, sem bloquear sequência de frames.
        cooldown = min(cooldown, 2)
    if not cache.add(lock_k, '1', timeout=max(1, cooldown)):
        return

    slot_ok, slot_cur, slot_lim = try_consume_order_slot_for_round(
        user=user,
        trading_environment=env,
        execution_profile=execution_profile,
        ctx=ctx,
        max_orders=1,
        strategy_key='leafar',
    )
    if not slot_ok:
        try:
            record_automation_thought(
                user,
                env,
                (
                    f'leafaR [{data_label} · {sym}] bloqueado: limite de ordens por rodada '
                    f'atingido ({slot_cur}/{slot_lim}).'
                )[:3900],
                source='leafar',
                kind=AutomationThought.Kind.NOTICE,
                execution_profile=execution_profile,
            )
        except Exception:
            logger.exception('leafar thought round slot block')
        return

    slot_consumed = True
    qty = _quantity(user, env)
    sig_exec = replace(sig, take_profit=target_adj, stop_loss=stop_adj)
    api_lbl = order_api_mode_label()
    try:
        if replay_sim:
            res = execute_leafar_bracket_replay_shadow(
                sym,
                sig_exec,
                quantity=qty,
                data_label=data_label,
                log_user=user,
                env=env,
                execution_profile=execution_profile,
            )
        else:
            res = execute_leafar_bracket(sym, sig_exec, quantity=qty, user=user)
        detail = ' | '.join(res.messages)
        if replay_sim:
            head = (
                f'Bracket simulado (replay · leafaR · {data_label} · {sym}) '
                if getattr(res, 'ok', False)
                else f'[replay] leafaR [{data_label}] {sym}: ok={res.ok}. '
            )
        else:
            head = (
                f'Ordens enviadas [API {api_lbl} · leafaR · {data_label} · {sym}] '
                if getattr(res, 'ok', False)
                else f'[API {api_lbl}] leafaR [{data_label}] {sym}: execução ok={res.ok}. '
            )
        record_automation_thought(
            user,
            env,
            f'{head}{detail}'[:4000],
            source='leafar',
            kind=AutomationThought.Kind.NOTICE if getattr(res, 'ok', False) else AutomationThought.Kind.WARN,
            execution_profile=execution_profile,
        )
        if slot_consumed and not getattr(res, 'ok', False):
            try:
                release_order_slot_for_round(
                    user=user,
                    trading_environment=env,
                    execution_profile=execution_profile,
                    ctx=ctx,
                    strategy_key='leafar',
                )
            except Exception:
                logger.exception('leafar release slot after failed execution')
        if getattr(res, 'ok', False):
            rid = str((res.market_resp or {}).get('Id') or (res.market_resp or {}).get('id') or '').strip()
            _leafar_track_open_operation(
                env=env,
                sym=sym,
                lane=lane,
                market_order_id=rid or None,
            )
        if (
            getattr(res, 'ok', False)
            and get_current_environment() == ENV_SIMULATOR
            and not replay_sim
        ):
            try:
                record_bracket_execution_marker(
                    ticker=sym,
                    side=sig_exec.side,
                    quantity=qty,
                    last=sig_exec.last,
                    strategy_source='leafar',
                    log_session_label=data_label,
                    market_order_id=(
                        str((res.market_resp or {}).get('Id') or (res.market_resp or {}).get('id') or '')
                        .strip()
                        or None
                    ),
                )
            except Exception:
                logger.exception('leafar custody marker')
    except Exception as exc:
        if slot_consumed:
            try:
                release_order_slot_for_round(
                    user=user,
                    trading_environment=env,
                    execution_profile=execution_profile,
                    ctx=ctx,
                    strategy_key='leafar',
                )
            except Exception:
                logger.exception('leafar release slot after exception')
        logger.warning('leafar execute %s', exc)
        try:
            record_automation_thought(
                user,
                env,
                f'[API {api_lbl}] leafaR [{data_label}] {sym}: falha ao enviar ordens: {exc}',
                source='leafar',
                kind=AutomationThought.Kind.WARN,
                execution_profile=execution_profile,
            )
        except Exception:
            pass


def run_leafar_for_context(ctx: ObservationContext, user, env: str) -> None:
    """Hook Celery: usa ``ctx.extra['candles']`` e ``ctx.data_source``."""
    if not _leafar_enabled():
        return
    raw = ctx.extra.get('candles')
    candles = raw if isinstance(raw, list) else []
    if not candles:
        return
    sym = (ctx.ticker or '').strip().upper()
    for_live = ctx.data_source == 'live_tail'
    label = ctx.data_source or ctx.mode
    profile = resolve_active_profile(user, env) if user is not None else None
    live_target = (getattr(profile, 'live_ticker', '') or '').strip().upper()
    # Defesa extra: no ao vivo, leafaR só processa o ticker explicitamente selecionado.
    if for_live:
        if not live_target:
            return
        if live_target != sym:
            return
    prof_id = int(getattr(profile, 'id', 0) or 0)
    started = getattr(profile, 'execution_started_at', None)
    started_tag = f':st{int(started.timestamp())}' if started is not None else ''
    lock_suf = ('live' if for_live else f'sim:{ctx.session_date_iso or ""}') + f':p{prof_id}{started_tag}'
    send_profile = strategy_execute_orders_enabled(
        user,
        'leafar',
        env,
        execution_profile=profile,
    )
    # Compatibilidade: se a flag estiver no legado (sem perfil), também permite envio.
    send_legacy = strategy_execute_orders_enabled(
        user,
        'leafar',
        env,
        execution_profile=None,
    )
    send = bool(send_profile or send_legacy)
    # Replay no simulador: bracket fictício (preço da vela); ao vivo: API.
    can_exec = send and (for_live or ctx.data_source == 'session_replay')
    _process_signal(
        ctx,
        sym,
        candles,
        user,
        env,
        for_live_tail=for_live,
        lock_suffix=lock_suf,
        data_label=str(label),
        can_execute_orders=can_exec,
    )
