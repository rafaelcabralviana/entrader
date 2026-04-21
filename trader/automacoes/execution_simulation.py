from __future__ import annotations

import random
import time
from dataclasses import dataclass

from django.conf import settings

from trader.environment import ENV_REAL, ENV_REPLAY, normalize_environment


@dataclass
class SimulatedFillResult:
    filled: bool
    price: float
    reason: str = ''


def _bool_setting(name: str, default: bool) -> bool:
    raw = getattr(settings, name, default)
    if isinstance(raw, bool):
        return raw
    s = str(raw or '').strip().lower()
    if s in ('1', 'true', 'yes', 'on'):
        return True
    if s in ('0', 'false', 'no', 'off'):
        return False
    return bool(default)


def _float_setting(name: str, default: float) -> float:
    raw = getattr(settings, name, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _int_setting(name: str, default: int) -> int:
    raw = getattr(settings, name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def simulate_non_real_fill(
    *,
    trading_environment: str,
    side: str,
    reference_price: float,
    is_exit: bool,
) -> SimulatedFillResult:
    env = normalize_environment(trading_environment)
    ref = max(0.01, float(reference_price or 0.01))
    if env in (ENV_REAL, ENV_REPLAY):
        # Replay precisa ficar colado ao relógio/candle virtual para evitar
        # atraso visual e "entrada fantasma" por slippage/latência artificial.
        return SimulatedFillResult(filled=True, price=ref)
    if not _bool_setting('TRADER_SIM_EXECUTION_ENABLED', True):
        return SimulatedFillResult(filled=True, price=ref)

    min_ms = max(0, _int_setting('TRADER_SIM_LATENCY_MIN_MS', 80))
    max_ms = max(min_ms, _int_setting('TRADER_SIM_LATENCY_MAX_MS', 450))
    if max_ms > 0:
        time.sleep(random.uniform(min_ms, max_ms) / 1000.0)

    fill_pct = _float_setting(
        'TRADER_SIM_EXIT_FILL_PROB_PCT' if is_exit else 'TRADER_SIM_ENTRY_FILL_PROB_PCT',
        100.0,
    )
    fill_pct = max(0.0, min(100.0, fill_pct))
    if random.random() > (fill_pct / 100.0):
        return SimulatedFillResult(
            filled=False,
            price=ref,
            reason='Ordem não executada na janela simulada (timing/liquidez).',
        )

    base_bps = max(0.0, _float_setting('TRADER_SIM_SLIPPAGE_BPS', 4.0))
    max_bps = max(base_bps, _float_setting('TRADER_SIM_MAX_SLIPPAGE_BPS', 25.0))
    slip_bps = random.uniform(base_bps, max_bps)
    side_n = str(side or '').strip().upper()
    is_buy = side_n in ('BUY', 'COMPRA')
    sign = 1.0 if is_buy else -1.0
    px = ref * (1.0 + sign * (slip_bps / 10000.0))
    return SimulatedFillResult(filled=True, price=max(0.01, px))
