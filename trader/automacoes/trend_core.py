"""
Núcleo compartilhado: classificação de tendência e janela de análise configurável.
"""

from __future__ import annotations

import statistics
from typing import Any

_LABEL_ALTA = 'Alta'
_LABEL_BAIXA = 'Baixa'
_LABEL_LAT = 'Lateralizado'

_MIN_BARS = 12

# Mesmo critério de ``classify_trend`` (Alta/Baixa vs lateral).
SCORE_THRESHOLD = 0.2


def _coerce_window(candles: list[dict[str, Any]], w: int) -> list[dict[str, Any]]:
    if w < 1 or not candles:
        return []
    return candles[-w:]


def _linear_slope(ys: list[float]) -> float:
    n = len(ys)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs) or 1e-12
    return num / den


def resolve_analysis_window(n: int, params: dict[str, Any] | None) -> int:
    """
    Padrão original: ``min(48, max(12, n // 3))``.
    Com ``params['analysis_bars']`` inteiro: usa esse valor limitado a [12, min(200, n)].
    """
    if n < 1:
        return _MIN_BARS
    if not params:
        return min(48, max(_MIN_BARS, n // 3))
    raw = params.get('analysis_bars')
    if raw is None or str(raw).strip() == '':
        return min(48, max(_MIN_BARS, n // 3))
    try:
        w = int(str(raw).strip())
        return min(max(w, _MIN_BARS), min(200, n))
    except (TypeError, ValueError):
        return min(48, max(_MIN_BARS, n // 3))


def classify_trend(
    candles: list[dict[str, Any]],
    params: dict[str, Any] | None,
    *,
    score_threshold: float | None = None,
) -> tuple[str, int, float]:
    """Retorna (rótulo, janela W usada, score).

    ``score_threshold`` opcional (ex.: estratégia ativa mais sensível); omisso = ``SCORE_THRESHOLD`` (0,2).
    """
    th = float(SCORE_THRESHOLD if score_threshold is None else score_threshold)
    th = max(0.01, min(th, 0.99))
    n = len(candles)
    w = resolve_analysis_window(n, params)
    win = _coerce_window(candles, w)
    try:
        ys = [float(c['close']) for c in win]
        highs = [float(c['high']) for c in win]
        lows = [float(c['low']) for c in win]
    except (TypeError, ValueError, KeyError):
        return (_LABEL_LAT, w, 0.0)
    if len(ys) < _MIN_BARS:
        return (_LABEL_LAT, w, 0.0)
    hi, lo = max(highs), min(lows)
    rng = hi - lo
    slope = _linear_slope(ys)
    q = max(2, len(ys) // 4)
    first = statistics.mean(ys[:q])
    last = statistics.mean(ys[-q:])
    if rng > 1e-9:
        reg_n = (slope * max(1, len(ys) - 1)) / rng
        fm_n = (last - first) / rng
        score = (reg_n + fm_n) / 2.0
    else:
        score = 0.0
    if score > th:
        return (_LABEL_ALTA, w, score)
    if score < -th:
        return (_LABEL_BAIXA, w, score)
    return (_LABEL_LAT, w, score)


def count_consecutive_trend_confirmations(
    candles: list[dict[str, Any]],
    params: dict[str, Any] | None,
    *,
    expected: str,
    score_threshold: float | None = None,
) -> int:
    """
    Quantas **últimas** classificações de tendência seguidas coincidem com ``expected``:
    compara ``classify_trend`` no fecho completo, depois ao remover 1, 2, … barras do fim.
    Ex.: contagem 3 = as três últimas leituras (neste sentido) são Alta ou Baixa conforme o sinal.
    Utilitário legado (confirmações consecutivas); a ativa usa ``trend_group_qualifies``.
    """
    n = len(candles)
    if n < _MIN_BARS or expected not in (_LABEL_ALTA, _LABEL_BAIXA):
        return 0
    cnt = 0
    for trim in range(0, n - _MIN_BARS + 1):
        sub = candles[: n - trim] if trim else candles
        if len(sub) < _MIN_BARS:
            break
        lab, _, _ = classify_trend(sub, params, score_threshold=score_threshold)
        if lab == expected:
            cnt += 1
        else:
            break
    return cnt


def trend_vote_probability_last_k(
    candles: list[dict[str, Any]],
    params: dict[str, Any] | None,
    *,
    want: str,
    k: int = 5,
    score_threshold: float | None = None,
) -> tuple[float, list[str]]:
    """
    Últimas **k** análises: ``classify_trend`` em ``candles``, ``candles[:-1]``, … até ``k-1``
    barras removidas do fim (a mais recente primeiro na lista devolvida).

    Devolve ``(probabilidade, rótulos)`` onde a probabilidade é a fracção de
    ocorrências de ``want`` (``Alta`` ou ``Baixa``) entre as corridas válidas.
    """
    n = len(candles)
    if n < _MIN_BARS or want not in (_LABEL_ALTA, _LABEL_BAIXA) or k < 1:
        return 0.0, []
    labels: list[str] = []
    max_trim = min(k, n - _MIN_BARS + 1)
    for trim in range(0, max_trim):
        sub = candles[: n - trim] if trim else candles
        if len(sub) < _MIN_BARS:
            break
        lab, _, _ = classify_trend(sub, params, score_threshold=score_threshold)
        labels.append(lab)
    if not labels:
        return 0.0, []
    hits = sum(1 for lab in labels if lab == want)
    return hits / float(len(labels)), labels


def trend_group_qualifies(labels_newest_first: list[str], want: str) -> bool:
    """
    Decide se o «grupo» das últimas análises (rótulos de ``trend_vote_probability_last_k``,
    índice 0 = mais recente) autoriza ordem alinhada a ``want`` (``Alta`` ou ``Baixa``).

    Heurística alinhada a consolidação + direção: exige contexto lateral ou várias
    confirmações; bloqueia o caso «só dois sinais seguidos» sem nenhum lateral na janela
    (spike frágil). Aceita falhas pontuais noutros padrões.
    """
    if not labels_newest_first or want not in (_LABEL_ALTA, _LABEL_BAIXA):
        return False
    chrono = list(reversed(labels_newest_first))  # mais antigo → mais recente
    if chrono[-1] != want:
        return False
    lat = sum(1 for x in chrono if x == _LABEL_LAT)
    hits = sum(1 for x in chrono if x == want)
    n = len(chrono)
    if (
        n >= 2
        and chrono[-1] == want
        and chrono[-2] == want
        and lat == 0
        and hits <= 2
    ):
        return False
    if hits >= 3:
        return True
    if lat >= 2 and chrono[-1] == want and (n < 2 or chrono[-2] == _LABEL_LAT):
        return True
    if hits >= 2 and lat >= 1:
        return True
    return False


def range_of_window(candles: list[dict[str, Any]], w: int) -> float:
    win = _coerce_window(candles, w)
    if not win:
        return 0.0
    try:
        highs = [float(c['high']) for c in win]
        lows = [float(c['low']) for c in win]
        return max(highs) - min(lows)
    except (TypeError, ValueError, KeyError):
        return 0.0
