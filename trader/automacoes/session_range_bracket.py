"""
Referência de TP/SL à amplitude do dia (máx./mín. da sessão nas velas disponíveis).

Não fixa ordens exatamente na máxima/mínima: aplica folgas e frações do intervalo
``R = máx − mín`` e da posição do último preço dentro desse intervalo.
"""

from __future__ import annotations

from typing import Any


def session_high_low_from_candles(candles: list[dict[str, Any]]) -> tuple[float, float] | None:
    """Retorna (máxima, mínima) do conjunto de velas; ``None`` se não houver OHLC válido."""
    if not candles:
        return None
    hs: list[float] = []
    ls: list[float] = []
    for c in candles:
        try:
            hs.append(float(c['high']))
            ls.append(float(c['low']))
        except (KeyError, TypeError, ValueError):
            continue
    if not hs:
        return None
    return max(hs), min(ls)


def adjust_tp_sl_to_session_extremes(
    side: str,
    last: float,
    take_profit: float,
    stop_loss: float,
    day_hi: float,
    day_lo: float,
    tick: float,
) -> tuple[float, float]:
    """
    Ajusta alvo e stop tendo em conta a máxima/mínima do dia.

    - Compra: TP não fica acima da máxima (com folga); stop relaciona-se com a distância
      entre o último e a mínima do dia.
    - Venda: simétrico (TP não abaixo da mínima com folga; stop em relação à máxima).
    """
    tp = float(take_profit)
    sl = float(stop_loss)
    lf = float(last)
    hi = float(day_hi)
    lo = float(day_lo)
    t = max(float(tick), 1e-12)
    R = hi - lo
    if R < t * 4:
        return tp, sl

    pad = max(t * 4.0, R * 0.007)
    s = str(side).strip().lower()

    if s == 'buy':
        dist_to_low = max(0.0, lf - lo)
        # TP: não além da máxima do dia (com folga), mantendo acima do último.
        cap = hi - pad
        if cap > lf + t * 2 and tp > cap:
            tp = cap
        # Stop: fração do espaço entre mínima do dia e o último (não colado ao último só por ticks).
        depth = max(t * 8.0, min(dist_to_low * 0.32, R * 0.24))
        sl_ref = lf - depth
        if sl_ref < lo - t * 2:
            sl_ref = lo - max(t * 3.0, R * 0.012)
        if sl_ref < sl:
            sl = sl_ref
    elif s == 'sell':
        dist_to_high = max(0.0, hi - lf)
        # TP: não além da mínima do dia (com folga), mantendo abaixo do último.
        floor = lo + pad
        if floor < lf - t * 2 and tp < floor:
            tp = floor
        depth = max(t * 8.0, min(dist_to_high * 0.32, R * 0.24))
        sl_ref = lf + depth
        if sl_ref > hi + t * 2:
            sl_ref = hi + max(t * 3.0, R * 0.012)
        if sl_ref > sl:
            sl = sl_ref
    return tp, sl
