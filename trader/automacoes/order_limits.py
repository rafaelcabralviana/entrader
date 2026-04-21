from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from zoneinfo import ZoneInfo

from django.core.cache import cache
from django.utils import timezone


_TZ_BRT = ZoneInfo('America/Sao_Paulo')

_BMF_DAILY_ORDER_LIMITS = {
    'BIT': 10,
    'WDO': 10,
    'DOL': 4,
    'WIN': 10,
    'IND': 4,
}
_BMF_TICKET_LIMITS = {
    'BIT': 5,
    'WDO': 5,
    'DOL': 5,
    'WIN': 5,
    'IND': 5,
}
_BOV_DAILY_ORDER_LIMIT = 10
_BOV_TICKET_LIMIT = 500


def ticker_root(ticker: str) -> str:
    sym = (ticker or '').strip().upper()
    if not sym:
        return ''
    for root in ('BIT', 'WDO', 'DOL', 'WIN', 'IND'):
        if sym.startswith(root):
            return root
    return sym


def daily_order_limit_for_ticker(ticker: str) -> int:
    root = ticker_root(ticker)
    if root in _BMF_DAILY_ORDER_LIMITS:
        return int(_BMF_DAILY_ORDER_LIMITS[root])
    return _BOV_DAILY_ORDER_LIMIT


def ticket_limit_for_ticker(ticker: str) -> int:
    root = ticker_root(ticker)
    if root in _BMF_TICKET_LIMITS:
        return int(_BMF_TICKET_LIMITS[root])
    return _BOV_TICKET_LIMIT


def clamp_quantity_to_ticket_limit(ticker: str, quantity: int) -> int:
    q = max(1, int(quantity))
    return min(q, ticket_limit_for_ticker(ticker))


def _ttl_until_day_end_brt() -> int:
    now = timezone.now().astimezone(_TZ_BRT)
    day_end = (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
    sec = int((day_end - now).total_seconds())
    return max(60, sec)


def _day_key() -> str:
    return timezone.now().astimezone(_TZ_BRT).date().isoformat()


def _budget_cache_key(*, user_id: int, trading_environment: str, ticker: str) -> str:
    return (
        f'automation:daily_orders:v1:{_day_key()}:'
        f'{int(user_id)}:{str(trading_environment).strip().lower()}:{ticker_root(ticker)}'
    )


@dataclass
class DailyBudgetDecision:
    ok: bool
    reason: str
    used: int
    limit: int
    required_weight: float


def try_consume_daily_order_budget(
    *,
    user_id: int,
    trading_environment: str,
    ticker: str,
    strategy_weight: float,
    user_daily_limit: int,
) -> DailyBudgetDecision:
    limit = max(1, min(int(user_daily_limit), daily_order_limit_for_ticker(ticker)))
    ck = _budget_cache_key(
        user_id=int(user_id),
        trading_environment=trading_environment,
        ticker=ticker,
    )
    raw = cache.get(ck)
    try:
        used = int(raw or 0)
    except (TypeError, ValueError):
        used = 0
    if used >= limit:
        return DailyBudgetDecision(
            ok=False,
            reason=f'limite diário atingido ({used}/{limit})',
            used=used,
            limit=limit,
            required_weight=1.0,
        )
    progress = (used / float(limit)) if limit > 0 else 1.0
    required = min(0.95, 0.18 + (0.62 * progress))
    w = max(0.0, min(1.0, float(strategy_weight)))
    if w + 1e-12 < required:
        return DailyBudgetDecision(
            ok=False,
            reason=f'prioridade baixa para orçamento restante ({used}/{limit})',
            used=used,
            limit=limit,
            required_weight=required,
        )
    used += 1
    cache.set(ck, used, timeout=_ttl_until_day_end_brt())
    return DailyBudgetDecision(
        ok=True,
        reason='ok',
        used=used,
        limit=limit,
        required_weight=required,
    )


def release_daily_order_budget(
    *,
    user_id: int,
    trading_environment: str,
    ticker: str,
) -> int:
    ck = _budget_cache_key(
        user_id=int(user_id),
        trading_environment=trading_environment,
        ticker=ticker,
    )
    raw = cache.get(ck)
    try:
        used = int(raw or 0)
    except (TypeError, ValueError):
        used = 0
    used = max(0, used - 1)
    cache.set(ck, used, timeout=_ttl_until_day_end_brt())
    return used
