from __future__ import annotations

from typing import Any
from decimal import Decimal

from django.core.cache import cache
from django.db.models import Q, Sum
from django.utils import timezone as dj_tz

from trader.environment import get_current_environment, normalize_environment
from trader.models import Position

_MIN_EFFECTIVE_OPEN_QTY = Decimal('0.000001')
_MIN_EFFECTIVE_OPEN_PRICE = Decimal('0.000001')


def has_open_position_for_ticker(
    ticker: str,
    *,
    trading_environment: str | None = None,
    position_lane: str = Position.Lane.STANDARD,
) -> bool:
    sym = (ticker or '').strip().upper()
    if not sym:
        return False
    env = (
        normalize_environment(trading_environment)
        if trading_environment is not None
        else get_current_environment()
    )
    # Considera posição aberta por estado/quantidade e preço médio válido.
    # Linhas antigas com preço médio zerado são tratadas como resíduos inválidos.
    return Position.objects.filter(
        ticker=sym,
        trading_environment=env,
        position_lane=position_lane,
        is_active=True,
    ).filter(
        Q(closed_at__isnull=True),
        Q(quantity_open__gt=_MIN_EFFECTIVE_OPEN_QTY),
        Q(avg_open_price__gt=_MIN_EFFECTIVE_OPEN_PRICE),
    ).exists()


def total_open_quantity_for_ticker(
    ticker: str,
    *,
    trading_environment: str | None = None,
    position_lane: str = Position.Lane.STANDARD,
) -> Decimal:
    """
    Soma ``quantity_open`` das posições ativas no ticker/lane (evita limite só por contagem de linhas).
    """
    sym = (ticker or '').strip().upper()
    if not sym:
        return Decimal('0')
    env = (
        normalize_environment(trading_environment)
        if trading_environment is not None
        else get_current_environment()
    )
    agg = (
        Position.objects.filter(
            ticker=sym,
            trading_environment=env,
            position_lane=position_lane,
            is_active=True,
            closed_at__isnull=True,
            quantity_open__gt=_MIN_EFFECTIVE_OPEN_QTY,
            avg_open_price__gt=_MIN_EFFECTIVE_OPEN_PRICE,
        ).aggregate(total=Sum('quantity_open'))
    )
    raw = agg.get('total')
    if raw is None:
        return Decimal('0')
    try:
        return Decimal(str(raw))
    except Exception:
        return Decimal('0')


def try_acquire_market_entry_lock(
    *,
    user_id: int,
    ticker: str,
    trading_environment: str,
    position_lane: str,
    ttl_sec: int = 25,
) -> bool:
    """
    Evita corrida entre ticks Celery: duas entradas quase simultâneas passando ``has_open_position``.
    """
    sym = (ticker or '').strip().upper()
    env = (trading_environment or '').strip().lower() or 'simulator'
    lane = (position_lane or Position.Lane.STANDARD).strip() or Position.Lane.STANDARD
    uid = max(0, int(user_id or 0))
    key = f'automation:mkt_entry_lock:v1:{env}:{uid}:{sym}:{lane}'
    return cache.add(key, '1', timeout=max(5, int(ttl_sec)))


def release_market_entry_lock(
    *,
    user_id: int,
    ticker: str,
    trading_environment: str,
    position_lane: str,
) -> None:
    sym = (ticker or '').strip().upper()
    env = (trading_environment or '').strip().lower() or 'simulator'
    lane = (position_lane or Position.Lane.STANDARD).strip() or Position.Lane.STANDARD
    uid = max(0, int(user_id or 0))
    key = f'automation:mkt_entry_lock:v1:{env}:{uid}:{sym}:{lane}'
    try:
        cache.delete(key)
    except Exception:
        pass


def count_open_positions(
    *,
    position_lane: str | None = None,
) -> int:
    env = get_current_environment()
    qs = Position.objects.filter(
        trading_environment=env,
        is_active=True,
        closed_at__isnull=True,
        quantity_open__gt=_MIN_EFFECTIVE_OPEN_QTY,
        avg_open_price__gt=_MIN_EFFECTIVE_OPEN_PRICE,
    )
    if position_lane:
        qs = qs.filter(position_lane=position_lane)
    return int(qs.count())


def _round_id_from_context(ctx: Any) -> str:
    try:
        extra = getattr(ctx, 'extra', None)
        if isinstance(extra, dict):
            candles = extra.get('candles')
            if isinstance(candles, list) and candles:
                last = candles[-1] if isinstance(candles[-1], dict) else {}
                bid = str(last.get('bucket_start') or '').strip()
                if bid:
                    return bid[:19]
        rui = str(getattr(ctx, 'replay_until_iso', '') or '').strip()
        if rui:
            return rui[:19]
        cap = getattr(ctx, 'captured_at', None)
        if cap is not None:
            return cap.isoformat()[:19]
    except Exception:
        pass
    return dj_tz.now().isoformat()[:19]


def _order_slot_cache_key(
    *,
    user: Any,
    trading_environment: str,
    execution_profile: Any,
    ctx: Any,
    strategy_key: str | None = None,
) -> str:
    uid = int(getattr(user, 'id', 0) or 0)
    pid = int(getattr(execution_profile, 'id', 0) or 0)
    env = str(trading_environment or '').strip().lower() or 'simulator'
    rid = _round_id_from_context(ctx)
    sk = str(strategy_key or '').strip().lower() or 'generic'
    return f'automation:order_slots:v1:{uid}:{env}:{pid}:{sk}:{rid}'


def try_consume_order_slot_for_round(
    *,
    user: Any,
    trading_environment: str,
    execution_profile: Any,
    ctx: Any,
    max_orders: int = 2,
    strategy_key: str | None = None,
) -> tuple[bool, int, int]:
    ck = _order_slot_cache_key(
        user=user,
        trading_environment=trading_environment,
        execution_profile=execution_profile,
        ctx=ctx,
        strategy_key=strategy_key,
    )
    limit = max(1, int(max_orders))
    current_raw = cache.get(ck)
    try:
        current = int(current_raw or 0)
    except (TypeError, ValueError):
        current = 0
    if current >= limit:
        return False, current, limit
    current += 1
    cache.set(ck, current, timeout=180)
    return True, current, limit


def release_order_slot_for_round(
    *,
    user: Any,
    trading_environment: str,
    execution_profile: Any,
    ctx: Any,
    strategy_key: str | None = None,
) -> int:
    """
    Devolve 1 slot consumido da rodada (mínimo 0).
    """
    ck = _order_slot_cache_key(
        user=user,
        trading_environment=trading_environment,
        execution_profile=execution_profile,
        ctx=ctx,
        strategy_key=strategy_key,
    )
    current_raw = cache.get(ck)
    try:
        current = int(current_raw or 0)
    except (TypeError, ValueError):
        current = 0
    current = max(0, current - 1)
    cache.set(ck, current, timeout=180)
    return current
