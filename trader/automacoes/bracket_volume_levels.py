"""
Níveis de bracket a partir do perfil de volume (VP).

``protective_lvn_stop_mid`` escolhe, no lado **protetor** do último, o bin com
**menor volume** entre os que estão a pelo menos ``min_distance`` do preço —
LVN «longe o suficiente» para reduzir stops curtos demais em zona congestionada.
"""

from __future__ import annotations


def protective_lvn_stop_mid(
    edges: list[float],
    vols: list[float],
    *,
    last: float,
    side: str,
    min_distance: float,
) -> float | None:
    """
    Preço médio do bin (lado stop) com menor volume, só bins a ``min_distance``
    ou mais do ``last``. Compra: bins abaixo do último; venda: acima.

    Empate em volume: fica o bin **mais afastado** do último.
    """
    n = len(vols)
    if n < 2 or len(edges) != n + 1:
        return None
    md = max(float(min_distance), 1e-9)
    s = (side or '').strip().lower()
    cands: list[tuple[float, float]] = []
    for i in range(n):
        mid = (float(edges[i]) + float(edges[i + 1])) / 2.0
        v = float(vols[i])
        if s == 'buy':
            if last - mid >= md - 1e-12:
                cands.append((mid, v))
        elif s == 'sell':
            if mid - last >= md - 1e-12:
                cands.append((mid, v))
    if not cands:
        return None
    cands.sort(key=lambda t: (t[1], -abs(last - t[0])))
    best_v = cands[0][1]
    tol = max(best_v * 0.08, 1e-9)
    same = [t for t in cands if t[1] <= best_v + tol]
    return max(same, key=lambda t: abs(last - t[0]))[0]


__all__ = ['protective_lvn_stop_mid']
