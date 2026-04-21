"""
Estratégia passiva: destaca as principais «montanhas» do perfil de volume (máximos locais
em cada faixa de preço), alinhado ao mesmo VP OHLC que o leafaR (bins configuráveis).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from django.conf import settings
from django.core.cache import cache

from trader.automacoes.leafar_candles import (
    load_session_day_candles,
    parse_replay_until_iso,
    trim_candles_to_replay_until,
)
from trader.automacoes.leafar_vp import compute_volume_profile, volume_profile_mountains
from trader.automacoes.strategy_registry import register_evaluator
from trader.trading_system.contracts.context import ObservationContext

_THROTTLE_SEC = 52
_MIN_BARS = 16


def _vp_bins() -> int:
    try:
        v = int(getattr(settings, 'TRADER_LEAFAR_VP_BINS', 24))
    except (TypeError, ValueError):
        v = 24
    return max(8, min(64, v))


def _fmt_price(p: float) -> str:
    if p >= 1000:
        return f'{p:.2f}'
    if p >= 100:
        return f'{p:.3f}'
    return f'{p:.4f}'


def _fmt_mountains_line(mountains: list[tuple[float, float]]) -> str:
    parts: list[str] = []
    for px, vol in mountains:
        q = int(round(vol))
        parts.append(f'({_fmt_price(px)} - {q})')
    return 'Vol. Montanhas: ' + '  '.join(parts)


def _throttle_ok(
    user: Any,
    env: str,
    ticker: str,
    session_key: str,
    signature: str,
    *,
    sec: int = _THROTTLE_SEC,
) -> bool:
    uid = getattr(user, 'id', 0) or 0
    ck = f'automation:vp_mount:{uid}:{env}:{ticker}:{session_key}:{signature}'
    return bool(cache.add(ck, '1', timeout=sec))


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    raw = ctx.extra.get('candles')
    if not isinstance(raw, list) or len(raw) < _MIN_BARS:
        return None
    candles = raw
    try:
        is_full_day = bool((ctx.extra or {}).get('candles_full_day')) if isinstance(ctx.extra, dict) else False
    except Exception:
        is_full_day = False

    day_iso = str(ctx.session_date_iso or (ctx.extra or {}).get('session_day_iso') or '').strip()
    try:
        session_day = date.fromisoformat(day_iso[:10]) if day_iso else None
    except Exception:
        session_day = None
    replay_until = parse_replay_until_iso(getattr(ctx, 'replay_until_iso', None))
    # Replay com cursor: recarregar sempre (``candles_full_day`` sozinho não garante anti look-ahead).
    needs_reload = (not is_full_day) or (replay_until is not None)
    if needs_reload and session_day is not None:
        try:
            iv = max(1, min(int(getattr(settings, 'TRADER_LEAFAR_INTERVAL_SEC', 10)), 300))
        except (TypeError, ValueError):
            iv = 10
        full = load_session_day_candles(
            (ctx.ticker or '').strip().upper(),
            session_day,
            interval_sec=iv,
            replay_until=replay_until,
        )
        if replay_until is not None and isinstance(full, list):
            full = trim_candles_to_replay_until(full, replay_until)
        if isinstance(full, list) and len(full) >= _MIN_BARS:
            candles = full
    bins = _vp_bins()
    vp = compute_volume_profile(candles, num_bins=bins)
    if vp is None:
        return None
    edges, vols = vp
    mountains = volume_profile_mountains(edges, vols, max_mountains=3)
    if not mountains:
        return None
    sig = '|'.join(f'{p:.5f}:{int(round(v))}' for p, v in mountains)
    env = (ctx.trading_environment or '').strip() or 'simulator'
    sym = (ctx.ticker or '').strip().upper() or '—'
    sk = ctx.session_date_iso or 'live'
    if not _throttle_ok(user, env, sym, sk, sig):
        return None
    ds = ctx.data_source or ctx.mode
    line = _fmt_mountains_line(mountains)
    return (
        f'Perfil de volume (montanhas) [{ds} · {sym}] | {line}. '
        f'VP ≈ {bins} bins (OHLC, mesmo critério leafaR). Não é recomendação de investimento.'
    )


register_evaluator('perfil_volume_montanhas', evaluate)
