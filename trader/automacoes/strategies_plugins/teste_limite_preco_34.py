"""
Estratégia de teste: alerta quando o preço efectivo atinge 34,11 ou mais.

O painel «Último» do gráfico usa o **fecho da última vela** (mesma série agregada
no motor a partir de ``QuoteSnapshot``); priorizamos esse valor em relação ao
``quote`` bruto do último snapshot, que pode estar vazio ou desfasado.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

from django.core.cache import cache

from trader.automacoes.prefs import get_strategy_params
from trader.automacoes.profiles import resolve_active_profile
from trader.automacoes.strategy_registry import register_evaluator
from trader.trading_system.contracts.context import ObservationContext

_THRESHOLD = 34.11
_THROTTLE_SEC = 45


def _coerce_float(v: Any) -> float | None:
    """Aceita número, ``Decimal`` e strings com ``.`` ou ``,`` como decimal."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(' ', '').replace('\u00a0', '')
        if not s:
            return None
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
        elif ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '')
        try:
            return float(s)
        except ValueError:
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _last_from_quote(q: dict[str, Any]) -> float | None:
    if not isinstance(q, dict):
        return None
    for key in (
        'lastPrice',
        'LastPrice',
        'last_price',
        'tradePrice',
        'close',
        'Close',
        'last',
        'Last',
        'price',
        'Price',
    ):
        v = q.get(key)
        if v is None or v == '':
            continue
        p = _coerce_float(v)
        if p is not None:
            return p
    return None


def _last_from_candles(extra: dict[str, Any]) -> float | None:
    raw = extra.get('candles') if isinstance(extra, dict) else None
    if not isinstance(raw, list) or not raw:
        return None
    last = raw[-1]
    if not isinstance(last, dict):
        return None
    for key in ('close', 'Close', 'last', 'Last'):
        if key not in last:
            continue
        p = _coerce_float(last.get(key))
        if p is not None:
            return p
    return None


def _effective_price(ctx: ObservationContext) -> tuple[float | None, str]:
    # Igual ao compacto do gráfico: «Último» = fecho da última vela (``candles_volume.html``).
    c = _last_from_candles(ctx.extra)
    if c is not None:
        return c, 'último fecho (velas)'
    q = _last_from_quote(ctx.quote)
    if q is not None:
        return q, 'cotação'
    return None, ''


def _crossed_up_from_candles(ctx: ObservationContext, threshold: float) -> tuple[bool | None, float | None]:
    raw = ctx.extra.get('candles') if isinstance(ctx.extra, dict) else None
    if not isinstance(raw, list) or not raw:
        return None, None
    if len(raw) < 2:
        last = _last_from_candles(ctx.extra)
        return (last is not None and last >= threshold), last
    prev = raw[-2] if isinstance(raw[-2], dict) else {}
    cur = raw[-1] if isinstance(raw[-1], dict) else {}
    p_prev = _coerce_float(prev.get('close'))
    p_cur = _coerce_float(cur.get('close'))
    if p_prev is None or p_cur is None:
        return None, p_cur
    return (p_prev < threshold <= p_cur), p_cur


def _last_candle_bucket(ctx: ObservationContext) -> str:
    raw = ctx.extra.get('candles') if isinstance(ctx.extra, dict) else None
    if not isinstance(raw, list) or not raw:
        return ''
    last = raw[-1]
    if not isinstance(last, dict):
        return ''
    return str(last.get('bucket_start') or '').strip()


def _throttle_ok(user: Any, env: str, ticker: str, price: float, event_key: str = '') -> bool:
    uid = getattr(user, 'id', 0) or 0
    sym = (ticker or '').strip().upper() or '—'
    fp = f'{price:.4f}'
    ek = (event_key or '').strip() or fp
    ck = f'automation:teste3411:{uid}:{env}:{sym}:{ek}'
    return bool(cache.add(ck, '1', timeout=_THROTTLE_SEC))


def _state_key(user: Any, env: str, ticker: str, session_key: str) -> str:
    uid = getattr(user, 'id', 0) or 0
    sym = (ticker or '').strip().upper() or '—'
    sk = (session_key or '').strip() or 'live'
    return f'automation:teste3411:state:{uid}:{env}:{sym}:{sk}'


def _was_above(user: Any, env: str, ticker: str, session_key: str) -> bool:
    return bool(cache.get(_state_key(user, env, ticker, session_key)))


def _set_above(user: Any, env: str, ticker: str, session_key: str, *, above: bool) -> None:
    key = _state_key(user, env, ticker, session_key)
    if above:
        cache.set(key, '1', timeout=24 * 3600)
    else:
        cache.delete(key)


def _last_fire_bucket_key(user: Any, env: str, ticker: str, session_key: str) -> str:
    uid = getattr(user, 'id', 0) or 0
    sym = (ticker or '').strip().upper() or '—'
    sk = (session_key or '').strip() or 'live'
    return f'automation:teste3411:last_fire_bucket:{uid}:{env}:{sym}:{sk}'


def _already_fired_for_bucket(user: Any, env: str, ticker: str, session_key: str, bucket: str) -> bool:
    if not bucket:
        return False
    return str(cache.get(_last_fire_bucket_key(user, env, ticker, session_key)) or '') == bucket


def _mark_fired_bucket(user: Any, env: str, ticker: str, session_key: str, bucket: str) -> None:
    if not bucket:
        return
    cache.set(
        _last_fire_bucket_key(user, env, ticker, session_key),
        bucket,
        timeout=24 * 3600,
    )


def _clear_fired_bucket(user: Any, env: str, ticker: str, session_key: str) -> None:
    cache.delete(_last_fire_bucket_key(user, env, ticker, session_key))


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    env = (ctx.trading_environment or '').strip() or 'simulator'
    sym = (ctx.ticker or '').strip().upper() or '—'
    profile = resolve_active_profile(user, env)
    cfg = get_strategy_params(
        user,
        'teste_limite_preco_34',
        env,
        execution_profile=profile,
    )
    threshold = _coerce_float((cfg or {}).get('threshold')) if isinstance(cfg, dict) else None
    if threshold is None or threshold <= 0:
        threshold = _THRESHOLD
    crossed, crossed_price = _crossed_up_from_candles(ctx, threshold)
    session_key = (ctx.session_date_iso or 'live').strip() or 'live'
    bucket = _last_candle_bucket(ctx)
    if crossed is not None:
        # Se voltou abaixo no candle atual, rearma para permitir novo disparo no próximo cruzamento.
        cur_price = _last_from_candles(ctx.extra)
        if cur_price is not None and cur_price < threshold:
            _clear_fired_bucket(user, env, sym, session_key)
        if crossed is not True or crossed_price is None:
            return None
        # Evita poluição dentro do mesmo bucket (close 34.11 -> 34.12 -> 34.13...).
        if _already_fired_for_bucket(user, env, sym, session_key, bucket):
            return None
        price = crossed_price
        origin = 'último fecho (velas)'
        _mark_fired_bucket(user, env, sym, session_key, bucket)
    else:
        # Fallback sem candles válidos: usa estado em cache para evitar repetição contínua.
        price, origin = _effective_price(ctx)
        if price is None:
            return None
        now_above = price >= threshold
        prev_above = _was_above(user, env, sym, session_key)
        if not now_above:
            if prev_above:
                _set_above(user, env, sym, session_key, above=False)
            return None
        if prev_above:
            return None
        _set_above(user, env, sym, session_key, above=True)
    evk = bucket or f'{session_key}:{price:.4f}'
    if not _throttle_ok(user, env, sym, price, event_key=evk):
        return None
    ds = ctx.data_source or ctx.mode
    return (
        f'Teste 34,11 [{ds} · {sym}] | Agora: {price:.4f} ({origin}) | '
        f'Limite: {threshold:.2f} | Evento: cruzou para cima.'
    )


register_evaluator('teste_limite_preco_34', evaluate)
