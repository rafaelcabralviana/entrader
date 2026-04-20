"""
Estratégia passiva: classifica tendência (Alta / Baixa / Lateralizado) a partir das velas
do mesmo conjunto usado pelo motor e pelo gráfico compacto.
"""

from __future__ import annotations

import statistics
from typing import Any, Optional

from django.core.cache import cache

from trader.automacoes.prefs import get_strategy_params
from trader.automacoes.profiles import resolve_active_profile
from trader.automacoes.strategy_registry import register_evaluator
from trader.automacoes.trend_core import classify_trend
from trader.trading_system.contracts.context import ObservationContext

_THROTTLE_SEC = 48
_MIN_BARS = 12


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _reversal_chance_pct(candles: list[dict[str, Any]], score: float) -> int:
    """
    Heurística local de reversão usando apenas a janela já observada (sem look-ahead).
    """
    try:
        closes = [float(c['close']) for c in candles if isinstance(c, dict)]
        highs = [float(c['high']) for c in candles if isinstance(c, dict)]
        lows = [float(c['low']) for c in candles if isinstance(c, dict)]
    except (TypeError, ValueError, KeyError):
        return 50
    if len(closes) < 6 or not highs or not lows:
        return 50
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    non_zero = [d for d in deltas if abs(d) > 1e-12]
    sign_changes = 0
    for i in range(1, len(non_zero)):
        if non_zero[i - 1] * non_zero[i] < 0:
            sign_changes += 1
    chop = sign_changes / float(max(1, len(non_zero) - 1))
    rng = max(highs) - min(lows)
    if rng <= 1e-12:
        stretch = 0.0
    else:
        mean_c = statistics.mean(closes[-8:])
        stretch = abs(closes[-1] - mean_c) / rng
    base = 1.0 - _clamp(abs(score) / 0.8, 0.0, 1.0)
    chance = (0.55 * base) + (0.25 * _clamp(chop, 0.0, 1.0)) + (0.20 * _clamp(stretch * 2.2, 0.0, 1.0))
    return int(round(_clamp(chance, 0.05, 0.95) * 100.0))


def _throttle_ok(
    user: Any,
    env: str,
    ticker: str,
    session_key: str,
    label: str,
    candles: list,
    *,
    sec: int = _THROTTLE_SEC,
) -> bool:
    uid = getattr(user, 'id', 0) or 0
    fp = '0'
    if isinstance(candles, list) and candles:
        last = candles[-1]
        try:
            fp = f"{len(candles)}:{float(last.get('close', 0)):.4f}:{label}"
        except (TypeError, ValueError):
            fp = f"{len(candles)}:{label}"
    ck = f'automation:tend_merc:{uid}:{env}:{ticker}:{session_key}:{fp}'
    return bool(cache.add(ck, '1', timeout=sec))


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    raw = ctx.extra.get('candles')
    if not isinstance(raw, list) or len(raw) < _MIN_BARS:
        return None
    env = (ctx.trading_environment or '').strip() or 'simulator'
    profile = resolve_active_profile(user, env)
    params = get_strategy_params(user, 'tendencia_mercado', env, execution_profile=profile)
    label, w_used, score = classify_trend(raw, params)
    sym = (ctx.ticker or '').strip().upper() or '—'
    sk = ctx.session_date_iso or 'live'
    if not _throttle_ok(user, env, sym, sk, label, raw):
        return None
    ds = ctx.data_source or ctx.mode
    try:
        last_c = float(raw[-1]['close'])
        last_s = f'{last_c:.2f}'
    except (TypeError, ValueError, KeyError, IndexError):
        last_s = '—'
    rev_pct = _reversal_chance_pct(raw[-w_used:], score)
    return (
        f'Tendência de mercado [{ds} · {sym}] | Direção: {label} | '
        f'Força: {score:+.2f} | Janela: {w_used} barras | Último fecho: {last_s}. '
        f'Reversão (estimada): {rev_pct}%. '
        f'Heurística com dados já observados; não é recomendação de investimento.'
    )


register_evaluator('tendencia_mercado', evaluate)
