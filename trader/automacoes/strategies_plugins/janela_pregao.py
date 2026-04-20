from __future__ import annotations

from typing import Any, Optional

from django.core.cache import cache

from trader.automacoes.strategy_registry import register_evaluator
from trader.panel_context import quote_status_is_end_of_day
from trader.trading_system.contracts.context import ObservationContext


def evaluate(ctx: ObservationContext, user: Any) -> Optional[str]:
    """
    Usa a mesma ``quote`` injectada no contexto (ao vivo ou instante do replay em BD).
    """
    q = ctx.quote
    if not q:
        return None
    if quote_status_is_end_of_day(q):
        uid = getattr(user, 'id', None) or 0
        ck = f'automation:janela_eod:{uid}:{ctx.trading_environment}'
        if not cache.add(ck, '1', timeout=3600):
            return None
        ds = ctx.data_source or ctx.mode
        return (
            f'janela_pregao: pregão encerrado nos dados ({ds} · {ctx.ticker}). '
            f'Evite enviar ordens automáticas até à próxima sessão.'
        )
    return None


register_evaluator('janela_pregao', evaluate)
