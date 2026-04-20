"""
Estratégia leve: comentário descritivo do preço ao longo do dia (só dados já existentes).
"""

from __future__ import annotations

from typing import Any, Optional

from django.core.cache import cache

from trader.automacoes.price_commentary import build_intraday_price_commentary
from trader.automacoes.strategy_registry import register_evaluator
from trader.trading_system.contracts.context import ObservationContext

_UP3_THRESHOLD = 3.0


def _pct_gain_vs_first_bar(candles: list[dict[str, Any]]) -> float | None:
    """
    Variação percentual do último fecho face à **abertura da primeira barra** da série
    (o mesmo «session open» visível que o comentário heurístico usa).
    """
    if not isinstance(candles, list) or len(candles) < 1:
        return None
    first, last = candles[0], candles[-1]
    try:
        o0 = float(first.get('open', first.get('close')))
        c_last = float(last.get('close'))
    except (TypeError, ValueError):
        return None
    if o0 == 0:
        return None
    return (c_last - o0) / abs(o0) * 100.0


def _pct_gain_vs_session_low(candles: list[dict[str, Any]]) -> float | None:
    """
    Variação do último fecho face à **mínima dos lows** de todas as barras carregadas.

    No replay / cauda recente a «primeira barra» pode já abrir alto (janela a partir das 9h),
    escondendo um movimento de 33→34 visto no gráfico do dia inteiro; a mínima da série
    aproxima melhor esse «fundo visível» dentro do conjunto enviado ao motor.
    """
    if not isinstance(candles, list) or len(candles) < 1:
        return None
    try:
        lows = [float(c['low']) for c in candles if isinstance(c, dict)]
        c_last = float(candles[-1]['close'])
    except (TypeError, ValueError, KeyError):
        return None
    if not lows:
        return None
    lo = min(lows)
    if lo <= 0:
        return None
    return (c_last - lo) / abs(lo) * 100.0


def _up3_alert_fragments(
    pct_vs_open: float | None,
    pct_vs_low: float | None,
) -> str:
    th = _UP3_THRESHOLD
    parts: list[str] = []
    if pct_vs_open is not None and pct_vs_open + 1e-9 >= th:
        parts.append(f'+{pct_vs_open:.2f}% face à 1ª abertura da série')
    if pct_vs_low is not None and pct_vs_low + 1e-9 >= th:
        parts.append(f'+{pct_vs_low:.2f}% face à mínima intradiária da série carregada')
    if not parts:
        return ''
    return f'[Alta ≥{th:.0f}%] {"; ".join(parts)}. '


def _throttle_ok(
    user: Any,
    env: str,
    ticker: str,
    session_key: str,
    candles: list,
    *,
    sec: int = 55,
) -> bool:
    """
    Evita spam: um comentário por janela ``sec`` s para o mesmo «instante» da série
    (nº de candles + último fecho). Quando o replay ou o mercado avançam, a chave muda
    e pode sair outro comentário antes de expirar a janela anterior.
    """
    uid = getattr(user, 'id', 0) or 0
    fp = '0'
    if isinstance(candles, list) and candles:
        last = candles[-1]
        try:
            fp = f"{len(candles)}:{float(last.get('close', 0)):.6f}"
        except (TypeError, ValueError):
            fp = str(len(candles))
    ck = f'automation:preco_coment:{uid}:{env}:{ticker}:{session_key}:{fp}'
    return bool(cache.add(ck, '1', timeout=sec))


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    raw = ctx.extra.get('candles')
    if not isinstance(raw, list) or len(raw) < 5:
        return None
    text = build_intraday_price_commentary(raw)
    if not text:
        return None
    env = (ctx.trading_environment or '').strip() or 'simulator'
    sym = (ctx.ticker or '').strip().upper() or '—'
    sk = ctx.session_date_iso or 'live'
    if not _throttle_ok(user, env, sym, sk, raw):
        return None
    ds = ctx.data_source or ctx.mode
    po = _pct_gain_vs_first_bar(raw)
    pl = _pct_gain_vs_session_low(raw)
    alert = _up3_alert_fragments(po, pl)
    return f'Comentário intradiário [{ds} · {sym}] | {alert}{text}'


register_evaluator('comentario_preco_intradia', evaluate)
