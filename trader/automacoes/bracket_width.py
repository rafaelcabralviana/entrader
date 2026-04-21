"""
Alargamento global de SL/TP em brackets (leafaR, tendência ativa, etc.).

O motor de trailing em ``universal_bracket_trailing`` só **aperta** o stop a favor;
a «faixa» inicial vem das estratégias. Estes multiplicadores alongam distâncias em
relação ao último preço (env ou Django settings; SL predef. ×2,0).
"""

from __future__ import annotations

import os
from typing import Any

from django.conf import settings


def _cfg_float(
    env_key: str,
    default: float,
    *,
    lo: float,
    hi: float,
    settings_attr: str | None = None,
) -> float:
    raw = os.environ.get(env_key)
    if raw is None or not str(raw).strip():
        if settings_attr:
            raw = getattr(settings, settings_attr, None)
    if raw is not None and str(raw).strip() != '':
        try:
            v = float(str(raw).replace(',', '.'))
            return max(lo, min(hi, v))
        except (TypeError, ValueError):
            pass
    return default


def bracket_sl_distance_mult() -> float:
    """Distância entrada→SL multiplicada por este factor (1.0 = sem mudança). Predef.: 2.0."""
    return _cfg_float(
        'TRADER_BRACKET_SL_DISTANCE_MULT',
        2.0,
        lo=1.0,
        hi=8.0,
        settings_attr='TRADER_BRACKET_SL_DISTANCE_MULT',
    )


def bracket_tp_distance_mult() -> float:
    """Distância entrada→TP multiplicada por este factor. Predef.: 4.0."""
    return _cfg_float(
        'TRADER_BRACKET_TP_DISTANCE_MULT',
        4.0,
        lo=1.0,
        hi=8.0,
        settings_attr='TRADER_BRACKET_TP_DISTANCE_MULT',
    )


def _cfg_int(
    env_key: str,
    default: int,
    *,
    lo: int,
    hi: int,
    settings_attr: str | None = None,
) -> int:
    raw = os.environ.get(env_key)
    if raw is None or not str(raw).strip():
        if settings_attr:
            raw = getattr(settings, settings_attr, None)
    if raw is not None and str(raw).strip() != '':
        try:
            v = int(float(str(raw).replace(',', '.')))
            return max(lo, min(hi, v))
        except (TypeError, ValueError):
            pass
    return default


def trailing_min_favorable_ticks() -> int:
    """
    Ticks de preço que o mercado tem de ir **a favor** da posição antes de
    permitir o primeiro (e qualquer) ajuste de trailing.

    Compra: máximo acima da entrada; venda: mínimo abaixo da entrada.
    **0** desliga o filtro (comportamento antigo, mais agressivo).
    Predef.: 16 (~R$ 0,16 em ativo tick 0,01).
    """
    return _cfg_int(
        'TRADER_TRAILING_MIN_FAVORABLE_TICKS',
        16,
        lo=0,
        hi=200,
        settings_attr='TRADER_TRAILING_MIN_FAVORABLE_TICKS',
    )


def trailing_protective_floor_ticks() -> int:
    """
    Após o MFE, limita o quanto o trailing pode **apertar** o gatilho em relação à entrada.

    Compra (SL venda abaixo): não sobe o gatilho acima de ``entrada − ticks`` (mantém folga).
    Venda (SL compra acima): não baixa o gatilho abaixo de ``entrada + ticks``.
    **0** desliga. Predef.: 10.
    """
    return _cfg_int(
        'TRADER_TRAILING_PROTECTION_FLOOR_TICKS',
        10,
        lo=0,
        hi=120,
        settings_attr='TRADER_TRAILING_PROTECTION_FLOOR_TICKS',
    )


def trailing_stop_tick_steps() -> float:
    """
    Ticks de preço por passo ao acompanhar o stop (``try_trailing_stop_update``).

    Maior = cada ajuste deixa o gatilho mais longe do extremo favorável (menos agressivo).
    Predef.: 12.0 (antes fixo 4.0).
    """
    return _cfg_float(
        'TRADER_TRAILING_STOP_TICKS',
        12.0,
        lo=1.0,
        hi=40.0,
        settings_attr='TRADER_TRAILING_STOP_TICKS',
    )


def trailing_lock_profit_arm_pct() -> float:
    """
    Lucro **máximo** a favor (vs entrada) para **armar** o travamento de lucro mínimo.

    Ex.: 0,03 = após ~+3 % no extremo favorável, o SL passa a garantir pelo menos o
    floor definido em ``TRADER_TRAILING_LOCK_PROFIT_FLOOR_PCT``. **0** desliga.
    """
    return _cfg_float(
        'TRADER_TRAILING_LOCK_PROFIT_ARM_PCT',
        0.03,
        lo=0.0,
        hi=0.5,
        settings_attr='TRADER_TRAILING_LOCK_PROFIT_ARM_PCT',
    )


def trailing_lock_profit_floor_pct() -> float:
    """
    Lucro mínimo **garantido** (fração sobre a entrada) depois de armado o travamento.

    Compra: gatilho SL não fica abaixo de ``entrada × (1 + floor)``.
    Venda: gatilho de cobertura não fica acima de ``entrada × (1 − floor)``.
    Predef.: 0,01 (1 %). Só actua se ``ARM_PCT`` > 0 e o movimento a favor o atingiu.
    """
    return _cfg_float(
        'TRADER_TRAILING_LOCK_PROFIT_FLOOR_PCT',
        0.01,
        lo=0.0,
        hi=0.25,
        settings_attr='TRADER_TRAILING_LOCK_PROFIT_FLOOR_PCT',
    )


def trailing_tp_peak_follow_ticks() -> float:
    """
    Ticks atrás do pico (compra) ou à frente do vale (venda) para ir **subindo**
    (ou ajustando) o preço limite de TP. **0** desliga o rastreio do TP.

    Predef.: 6,0.
    """
    return _cfg_float(
        'TRADER_TRAILING_TP_FOLLOW_PEAK_TICKS',
        6.0,
        lo=0.0,
        hi=40.0,
        settings_attr='TRADER_TRAILING_TP_FOLLOW_PEAK_TICKS',
    )


def trailing_breakeven_arm_ticks() -> int:
    """
    Ticks a favor para obrigar o SL a ultrapassar a entrada (break-even+).

    Predef.: 18. **0** desliga.
    """
    return _cfg_int(
        'TRADER_TRAILING_BREAKEVEN_ARM_TICKS',
        18,
        lo=0,
        hi=300,
        settings_attr='TRADER_TRAILING_BREAKEVEN_ARM_TICKS',
    )


def trailing_breakeven_offset_ticks() -> int:
    """
    Offset (ticks) acima/abaixo da entrada quando o break-even é armado.

    Compra: SL >= entrada + offset. Venda: SL <= entrada - offset.
    Predef.: 3. **0** = exatamente na entrada.
    """
    return _cfg_int(
        'TRADER_TRAILING_BREAKEVEN_OFFSET_TICKS',
        3,
        lo=0,
        hi=80,
        settings_attr='TRADER_TRAILING_BREAKEVEN_OFFSET_TICKS',
    )


def trailing_relax_pullback_ticks() -> int:
    """
    Pullback mínimo (em ticks) para permitir afrouxar SL de forma controlada.

    Predef.: 14. **0** desliga afrouxamento por pullback.
    """
    return _cfg_int(
        'TRADER_TRAILING_RELAX_PULLBACK_TICKS',
        14,
        lo=0,
        hi=200,
        settings_attr='TRADER_TRAILING_RELAX_PULLBACK_TICKS',
    )


def trailing_relax_max_ticks() -> int:
    """
    Máximo de afrouxamento por ajuste (em ticks) quando há pullback.

    Limita o quanto o SL pode "abrir" para não soltar demais o risco.
    Predef.: 5. **0** desliga afrouxamento.
    """
    return _cfg_int(
        'TRADER_TRAILING_RELAX_MAX_TICKS',
        5,
        lo=0,
        hi=80,
        settings_attr='TRADER_TRAILING_RELAX_MAX_TICKS',
    )


def apply_bracket_distance_multipliers(
    side: str,
    last: float,
    stop_loss: float,
    take_profit: float,
) -> tuple[float, float]:
    """
    Alonga SL e TP a partir de ``last`` mantendo o lado da operação.

    Compra: SL abaixo do último, TP acima. Venda: o inverso.
    """
    sl_m = bracket_sl_distance_mult()
    tp_m = bracket_tp_distance_mult()
    last_f = float(last)
    sl0 = float(stop_loss)
    tp0 = float(take_profit)
    s = (side or '').strip().lower()
    if s == 'buy':
        risk = max(last_f - sl0, 1e-9)
        rew = max(tp0 - last_f, 1e-9)
        return last_f - risk * sl_m, last_f + rew * tp_m
    if s == 'sell':
        risk = max(sl0 - last_f, 1e-9)
        rew = max(last_f - tp0, 1e-9)
        return last_f + risk * sl_m, last_f - rew * tp_m
    return sl0, tp0


__all__ = [
    'apply_bracket_distance_multipliers',
    'bracket_sl_distance_mult',
    'bracket_tp_distance_mult',
    'trailing_lock_profit_arm_pct',
    'trailing_lock_profit_floor_pct',
    'trailing_min_favorable_ticks',
    'trailing_protective_floor_ticks',
    'trailing_breakeven_arm_ticks',
    'trailing_breakeven_offset_ticks',
    'trailing_relax_pullback_ticks',
    'trailing_relax_max_ticks',
    'trailing_stop_tick_steps',
    'trailing_tp_peak_follow_ticks',
]
