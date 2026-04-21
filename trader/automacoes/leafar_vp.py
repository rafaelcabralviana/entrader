"""
Perfil de volume (VP) simplificado e detecção da configuração leafaR.

Distribui o volume de cada candle pelos bins de preço proporcionalmente à
sobreposição [low, high] do candle com o bin (semelhante ao histograma VP do painel).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from trader.automacoes.session_range_bracket import session_high_low_from_candles
from trader.smart_trader_limits import extract_bmf_base

Side = Literal['Buy', 'Sell']


@dataclass(frozen=True)
class LeafarSignal:
    side: Side
    last: float
    poc: float
    stop_loss: float
    take_profit: float
    poc_volume: float
    corridor_max_vol: float
    reason: str


def _last_close(candles: list[dict[str, Any]]) -> float | None:
    if not candles:
        return None
    c = candles[-1]
    v = c.get('close')
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ohlc(c: dict[str, Any]) -> tuple[float, float, float] | None:
    try:
        lo = float(c['low'])
        hi = float(c['high'])
        cl = float(c['close'])
        return lo, hi, cl
    except (KeyError, TypeError, ValueError):
        return None


def _volume_profile_peak_bins(vols: list[float]) -> list[int]:
    """
    Bins que formam «montanhas» no histograma: máximos locais (plateau → um pico ao centro).

    Não são simplesmente os maiores valores globais: exige queda à direita (ou planalto no fim
    com subida antes) para separar vales entre montanhas.
    """
    n = len(vols)
    if n < 2:
        return []
    peaks: list[int] = []

    def _append_peak(lo: int, hi: int) -> None:
        if vols[lo] == vols[hi]:
            peaks.append((lo + hi) // 2)
        else:
            peaks.append(hi)

    if n >= 3:
        i = 1
        while i < n - 1:
            if vols[i] <= vols[i - 1]:
                i += 1
                continue
            j = i
            while j + 1 < n and vols[j + 1] >= vols[j]:
                j += 1
            if j + 1 < n and vols[j + 1] < vols[j]:
                _append_peak(i, j)
                i = j + 1
                continue
            if j == n - 1:
                mono_from_start = i <= 1 and all(
                    vols[t] < vols[t + 1] for t in range(i, j) if t + 1 <= j
                )
                if vols[i] == vols[j]:
                    _append_peak(i, j)
                elif not mono_from_start:
                    _append_peak(i, j)
                i = j + 1
                continue
            i += 1

    if vols[0] > vols[1]:
        peaks.append(0)
    if n >= 2 and vols[n - 1] > vols[n - 2]:
        peaks.append(n - 1)

    peaks.sort()
    dedup: list[int] = []
    for p in peaks:
        if dedup and p == dedup[-1]:
            continue
        dedup.append(p)
    return dedup


def volume_profile_mountains(
    edges: list[float],
    vols: list[float],
    *,
    max_mountains: int = 3,
    min_relative_peak: float = 0.07,
    min_bin_separation: int = 2,
) -> list[tuple[float, float]]:
    """
    Até ``max_mountains`` picos principais: preço médio do bin e volume no pico.

    Filtra picos fracos (``min_relative_peak`` × máximo do VP) e aplica separação mínima em
    número de bins para não colapsar dois ombros da mesma elevação.
    """
    n = len(vols)
    if n < 2 or len(edges) != n + 1:
        return []
    vmax = max(vols) or 0.0
    if vmax <= 0:
        return []
    floor_v = max(vmax * float(min_relative_peak), 1e-12)

    def _mid_price(bin_i: int) -> float:
        return (edges[bin_i] + edges[bin_i + 1]) / 2.0

    cand_bins = [b for b in _volume_profile_peak_bins(vols) if vols[b] >= floor_v]
    if not cand_bins:
        poc_i = max(range(n), key=lambda i: vols[i])
        return [(_mid_price(poc_i), float(vols[poc_i]))]

    scored = sorted(((vols[b], b) for b in cand_bins), reverse=True)
    chosen: list[int] = []
    for _, b in scored:
        if all(abs(b - c) >= int(min_bin_separation) for c in chosen):
            chosen.append(b)
        if len(chosen) >= int(max_mountains):
            break
    if not chosen:
        poc_i = max(range(n), key=lambda i: vols[i])
        return [(_mid_price(poc_i), float(vols[poc_i]))]

    out_bins = sorted(chosen, key=lambda bi: _mid_price(bi), reverse=True)
    return [(_mid_price(bi), float(vols[bi])) for bi in out_bins]


def compute_volume_profile(
    candles: list[dict[str, Any]],
    *,
    num_bins: int = 24,
) -> tuple[list[float], list[float]] | None:
    """
    Retorna (edges, volumes) com len(edges) == num_bins + 1 e len(volumes) == num_bins.
    """
    if num_bins < 4 or not candles:
        return None
    lows: list[float] = []
    highs: list[float] = []
    for c in candles:
        t = _ohlc(c)
        if t is None:
            continue
        lo, hi, _ = t
        if hi < lo:
            lo, hi = hi, lo
        lows.append(lo)
        highs.append(hi)
    if not lows:
        return None
    p_min = min(lows)
    p_max = max(highs)
    if p_max <= p_min:
        p_max = p_min + 1e-6
    edges = [p_min + i * (p_max - p_min) / num_bins for i in range(num_bins + 1)]
    vols = [0.0] * num_bins
    span = p_max - p_min
    for c in candles:
        t = _ohlc(c)
        if t is None:
            continue
        _, _, cl = t
        try:
            v = float(c.get('volume') or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v <= 0:
            continue
        # Alinhado ao gráfico: volume inteiro no bin do CLOSE da vela.
        idx = int((float(cl) - p_min) / span * num_bins)
        idx = max(0, min(num_bins - 1, idx))
        vols[idx] += v
    return edges, vols


def _bin_index(price: float, edges: list[float], num_bins: int) -> int:
    if price <= edges[0]:
        return 0
    if price >= edges[-1]:
        return num_bins - 1
    span = edges[-1] - edges[0]
    if span <= 0:
        return 0
    idx = int((price - edges[0]) / span * num_bins)
    return max(0, min(num_bins - 1, idx))


def _trend_direction_fraction(closes: list[float], *, window: int, direction: Literal['down', 'up']) -> float:
    """Fracção aproximada de passos a favor (ex.: 0.6 = 60% dos degraus são descidas)."""
    if len(closes) < window + 1 or window < 2:
        return 0.0
    tail = closes[-window:]
    prev = closes[-(window + 1) : -1]
    if direction == 'down':
        return sum(1 for a, b in zip(prev, tail) if b < a) / float(window)
    return sum(1 for a, b in zip(prev, tail) if b > a) / float(window)


def _corridor_aggregate(vols_slice: list[float]) -> float:
    """Reduz sensibilidade a um único bin espigado no corredor (aproximação)."""
    if not vols_slice:
        return 0.0
    if len(vols_slice) == 1:
        return float(vols_slice[0])
    s = sorted(vols_slice, reverse=True)
    return float(s[0]) * 0.65 + float(s[1]) * 0.35


def _price_tick(price: float, *, ticker: str | None = None) -> float:
    base = extract_bmf_base(str(ticker or '').strip().upper()) if ticker else None
    if base in ('WIN', 'IND'):
        return 5.0
    if base in ('WDO', 'DOL'):
        return 0.5
    if base == 'BIT':
        return 5.0
    if price >= 1000:
        return 5.0
    if price >= 100:
        return 0.05
    if price >= 10:
        return 0.01
    return 0.01


def _session_range_and_edge_room(
    candles: list[dict[str, Any]],
    last: float,
    tick: float,
    *,
    vp_hi: float,
    vp_lo: float,
) -> tuple[float, float, float, float]:
    """
    Retorna ``(session_hi, session_lo, R, edge_room)``.

    ``R`` = máxima − mínima do dia (velas); ``edge_room`` = distância do último à borda
    mais próxima (mínima ou máxima), com piso em ticks — base para «perto/longe» vs formação.
    """
    bounds = session_high_low_from_candles(candles)
    if bounds is None:
        s_hi, s_lo = float(vp_hi), float(vp_lo)
    else:
        s_hi, s_lo = float(bounds[0]), float(bounds[1])
    if s_hi < s_lo:
        s_hi, s_lo = s_lo, s_hi
    t = max(float(tick), 1e-12)
    R = max(s_hi - s_lo, t * 4.0)
    below = max(0.0, last - s_lo)
    above = max(0.0, s_hi - last)
    edge_room = max(t * 8.0, min(below, above))
    return s_hi, s_lo, R, edge_room


def _min_sep_abs_session(
    *,
    R: float,
    edge_room: float,
    min_price_sep_frac: float,
    session_local_sep_frac: float,
) -> float:
    """
    Separação mínima preço↔POC: maior entre fração da amplitude do dia e fração do «espaço»
    até à borda mais próxima (preço encostado num extremo → escala local).
    """
    weak = max(0.004, float(min_price_sep_frac) * 0.8)
    loc = max(0.02, min(0.55, float(session_local_sep_frac)))
    return max(weak * float(R), loc * float(edge_room))


def _candle_dt(c: dict[str, Any]) -> datetime | None:
    raw = c.get('bucket_start') or c.get('label') or c.get('captured_at')
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def detect_leafar_signal(
    candles: list[dict[str, Any]],
    *,
    ticker: str | None = None,
    num_bins: int = 24,
    min_bins_from_poc: int = 1,
    low_corridor_ratio: float = 0.38,
    min_candles: int = 28,
    trend_window: int = 7,
    trend_min_frac: float = 0.5,
    min_price_sep_frac: float = 0.006,
    session_local_sep_frac: float = 0.22,
    poc_stability_bars: int = 2,
    poc_dominance_ratio: float = 1.08,
    persistence_bars: int = 2,
    min_recent_range_ticks: int = 8,
    min_session_minutes: int = 18,
) -> LeafarSignal | None:
    """
    Estratégia leafaR (modo simplificado):
    - identifica o nível de maior volume (POC/#1);
    - se preço atual está abaixo desse nível -> BUY buscando o nível;
    - se preço atual está acima desse nível -> SELL buscando o nível.

    A separação preço↔POC considera a **amplitude do dia** (máx./mín. nas velas) e a
    distância do último preço à **borda da sessão mais próxima**, para definir o mínimo
    aceitável («perto» demais do POC = sem sinal).
    """
    # Cautela de arranque: exige amostra mínima real (antes usava min_candles/3).
    if len(candles) < max(6, int(min_candles)):
        return None
    msm = max(0, int(min_session_minutes))
    if msm > 0 and len(candles) >= 2:
        d0 = _candle_dt(candles[0])
        d1 = _candle_dt(candles[-1])
        if d0 is not None and d1 is not None:
            elapsed_min = (d1 - d0).total_seconds() / 60.0
            if elapsed_min < float(msm) - 1e-12:
                return None
    vp = compute_volume_profile(candles, num_bins=num_bins)
    if vp is None:
        return None
    edges, vols = vp
    n = len(vols)
    if n < 4:
        return None
    poc_i = max(range(n), key=lambda i: vols[i])
    poc_vol = float(vols[poc_i] or 0.0)
    if poc_vol <= 0:
        return None
    if n >= 2 and poc_dominance_ratio > 1.0:
        top_two = sorted((float(v or 0.0) for v in vols), reverse=True)[:2]
        if len(top_two) >= 2 and top_two[1] > 1e-12:
            dominance = top_two[0] / top_two[1]
            if dominance < float(poc_dominance_ratio):
                return None
    poc = (edges[poc_i] + edges[poc_i + 1]) / 2.0
    last = _last_close(candles)
    if last is None:
        return None

    price_i = _bin_index(last, edges, n)
    sep_bins = abs(price_i - poc_i)
    vp_span = max(edges[-1] - edges[0], 1e-12)
    sep_price = abs(last - poc) / vp_span
    tick = _price_tick(last, ticker=ticker)
    _, _, R, edge_room = _session_range_and_edge_room(
        candles, last, tick, vp_hi=edges[-1], vp_lo=edges[0]
    )
    min_sep_abs = _min_sep_abs_session(
        R=R,
        edge_room=edge_room,
        min_price_sep_frac=min_price_sep_frac,
        session_local_sep_frac=session_local_sep_frac,
    )
    sep_abs = abs(last - poc)
    sep_vs_rng = sep_abs / max(R, 1e-12)
    sep_vs_edge = sep_abs / max(edge_room, 1e-12)

    # Afastamento mínimo vs amplitude do dia e vs borda mais próxima (formaçao «longe» do POC).
    if sep_abs < min_sep_abs:
        return None
    # Colado ao POC no grid do VP: poucos bins e ainda «fraco» em termos do próprio histograma.
    weak_bins = max(1, int(min_bins_from_poc))
    weak_sep = max(0.004, float(min_price_sep_frac) * 0.8)
    if sep_bins <= weak_bins and sep_price <= weak_sep:
        return None

    # Persistência mínima: evita disparo em 1 tick de afastamento pontual.
    pb = max(1, int(persistence_bars))
    if pb > 1 and len(candles) >= pb:
        recent_closes: list[float] = []
        for c in candles[-pb:]:
            try:
                recent_closes.append(float(c.get('close')))
            except (TypeError, ValueError):
                recent_closes = []
                break
        if len(recent_closes) == pb:
            if last < poc and not all(px < poc for px in recent_closes):
                return None
            if last > poc and not all(px > poc for px in recent_closes):
                return None

    # Comprime ruído lateral: se range recente for muito curto, não entra.
    mrt = max(0, int(min_recent_range_ticks))
    if mrt > 0:
        hw = max(8, min(22, int(trend_window) * 2))
        seg = candles[-hw:] if len(candles) >= hw else candles
        highs: list[float] = []
        lows: list[float] = []
        for c in seg:
            try:
                highs.append(float(c['high']))
                lows.append(float(c['low']))
            except (KeyError, TypeError, ValueError):
                continue
        if highs and lows:
            recent_range = max(highs) - min(lows)
            if recent_range < (_price_tick(last, ticker=ticker) * float(mrt)):
                return None

    # Estabilidade do POC: exige o bin dominante consistente nas últimas barras.
    psb = max(1, int(poc_stability_bars))
    if psb > 1 and len(candles) >= max(16, psb + 2):
        stable_ok = True
        for back in range(1, psb):
            end = len(candles) - back
            if end < max(16, min_candles // 2):
                break
            vp_prev = compute_volume_profile(candles[:end], num_bins=num_bins)
            if vp_prev is None:
                stable_ok = False
                break
            _, prev_vols = vp_prev
            if not prev_vols:
                stable_ok = False
                break
            prev_idx = max(range(len(prev_vols)), key=lambda i: prev_vols[i])
            if prev_idx != poc_i:
                stable_ok = False
                break
        if not stable_ok:
            return None

    # Direção determinística:
    # preço abaixo do #1 => Buy ; preço acima do #1 => Sell
    prefer_side = 'Buy' if last < poc else 'Sell'

    recent_lows: list[float] = []
    recent_highs: list[float] = []
    for c in candles[-18:]:
        t = _ohlc(c)
        if t:
            recent_lows.append(t[0])
            recent_highs.append(t[1])

    if prefer_side == 'Buy':
        target_px = float(poc)
        sl = (min(recent_lows) if recent_lows else last) - tick * 2
        if sl >= last - tick * 0.5:
            sl = last - tick * 3
        return LeafarSignal(
            side='Buy',
            last=last,
            poc=poc,
            stop_loss=sl,
            take_profit=target_px,
            poc_volume=poc_vol,
            corridor_max_vol=0.0,
            reason=(
                f'Compra direta: preço abaixo do volume forte (#1≈{poc:.4f}, vol≈{poc_vol:.0f}); '
                f'alvo no próprio #1 do VP dia. sep_rng={sep_vs_rng:.4f} sep_vs_borda={sep_vs_edge:.4f} '
                f'(mín. abs {min_sep_abs:.4f}; R={R:.4f})'
            ),
        )

    target_px = float(poc)
    sl = (max(recent_highs) if recent_highs else last) + tick * 2
    if sl <= last + tick * 0.5:
        sl = last + tick * 3
    return LeafarSignal(
        side='Sell',
        last=last,
        poc=poc,
        stop_loss=sl,
        take_profit=target_px,
        poc_volume=poc_vol,
        corridor_max_vol=0.0,
        reason=(
            f'Venda direta: preço acima do volume forte (#1≈{poc:.4f}, vol≈{poc_vol:.0f}); '
            f'alvo no próprio #1 do VP dia. sep_rng={sep_vs_rng:.4f} sep_vs_borda={sep_vs_edge:.4f} '
            f'(mín. abs {min_sep_abs:.4f}; R={R:.4f})'
        ),
    )
