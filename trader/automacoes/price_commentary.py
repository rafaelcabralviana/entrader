"""
Comentários heurísticos sobre o preço intradiário a partir **apenas** de candles já
existentes (``QuoteSnapshot`` agregados — ao vivo ou dia simulado até o instante actual).

Não usa dados futuros nem valores inventados; é estatística descritiva simples + rótulos
de «forma» aproximada (não substitui análise técnica formal).
"""

from __future__ import annotations

import math
from typing import Any


def _nums(candles: list[dict[str, Any]]) -> tuple[list[float], list[float], list[float], list[float]] | None:
    """Retorna (opens, highs, lows, closes) alinhados."""
    o, h, l, c = [], [], [], []
    for x in candles:
        try:
            o.append(float(x['open']))
            h.append(float(x['high']))
            l.append(float(x['low']))
            c.append(float(x['close']))
        except (KeyError, TypeError, ValueError):
            return None
    if len(c) < 5:
        return None
    return o, h, l, c


def build_intraday_price_commentary(
    candles: list[dict[str, Any]],
    *,
    lookback_tail: int = 10,
) -> str | None:
    """
    Gera um parágrafo curto em português (BRT) com leitura aproximada da «forma» do dia.

    ``lookback_tail`` — últimos N fechos para micro-tendência vs. abertura da sessão nos candles.
    """
    nums = _nums(candles)
    if nums is None:
        return None
    opens, highs, lows, closes = nums
    n = len(closes)
    session_open = opens[0]
    session_high = max(highs)
    session_low = min(lows)
    last = closes[-1]
    span = session_high - session_low
    if session_open == 0:
        return None
    if span <= 0:
        return (
            f'Faixa intradiária muito estreita (último {last:.4f}, amostra com {n} candles já gravados). '
            f'Movimento neutro ou dados com preço constante na agregação. '
            f'(Leitura heurística — não é recomendação de investimento.)'
        )

    # Variação desde a primeira barra agregada (proxy de «abertura» intradiária visível).
    varia_pct = (last - session_open) / abs(session_open) * 100.0
    pos_pct = (last - session_low) / span * 100.0  # 0 = mínima, 100 = máxima da amostra

    # Oscilação relativa ao último (proxy de «tamanho» do dia em %).
    amplitude_pct = span / abs(last) * 100.0 if last else 0.0

    tail = min(lookback_tail, n)
    tail_first = closes[-tail]
    micro_drift_pct = (last - tail_first) / abs(tail_first) * 100.0 if tail_first else 0.0

    # Desvio padrão simples dos últimos retornos bar-a-bar (últimos min(tail,n-1) passos).
    rets: list[float] = []
    for i in range(max(1, n - tail), n):
        a, b = closes[i - 1], closes[i]
        if a:
            rets.append((b - a) / abs(a) * 100.0)
    vol_rets = 0.0
    if len(rets) >= 2:
        m = sum(rets) / len(rets)
        vol_rets = math.sqrt(sum((x - m) ** 2 for x in rets) / (len(rets) - 1))

    # Rótulo de tendência global (sessão).
    if varia_pct > 0.12:
        tend = 'viés de alta na sessão'
    elif varia_pct < -0.12:
        tend = 'viés de baixa na sessão'
    else:
        tend = 'variação modesta em torno da abertura visível'

    # Micro na cauda.
    if micro_drift_pct > 0.06:
        micro = f'na cauda recente ({tail} barras) o preço empurra para cima (~{micro_drift_pct:+.2f}%).'
    elif micro_drift_pct < -0.06:
        micro = f'na cauda recente ({tail} barras) o preço cede (~{micro_drift_pct:+.2f}%).'
    else:
        micro = f'na cauda recente ({tail} barras) o movimento é contido (~{micro_drift_pct:+.2f}%).'

    # «Forma» heurística.
    forma: str
    if amplitude_pct < 0.25 and vol_rets < 0.04:
        forma = 'Perfil compatível com **consolidação** (amplitude baixa e retornos pouco dispersos).'
    elif pos_pct >= 88 and varia_pct > 0.08:
        forma = 'Formação sugestiva de **teste/extensão na zona das máximas** da amostra intradiária.'
    elif pos_pct <= 12 and varia_pct < -0.08:
        forma = 'Formação sugestiva de **pressão na zona das mínimas** da amostra intradiária.'
    elif vol_rets > 0.07:
        forma = 'Perfil **oscilante** (retornos bar-a-bar mais dispersos — menos direccional).'
    else:
        forma = 'Perfil **intermédio**: nem extremo da faixa nem consolidação muito fechada.'

    # Estatísticas compactas.
    stats = (
        f'Estatística (aprox.): último {last:.4f}; mín./máx. da amostra {session_low:.4f} / {session_high:.4f}; '
        f'Δ vs. primeira barra {varia_pct:+.2f}%; posição na faixa do dia ~{pos_pct:.0f}%; '
        f'amplitude ~{amplitude_pct:.2f}% do último; vol. retornos recentes ~{vol_rets:.3f} pp.'
    )

    return (
        f'{tend.capitalize()}. {micro} {forma} {stats} '
        f'(Leitura heurística sobre {n} candles já gravados — não é recomendação de investimento.)'
    )
