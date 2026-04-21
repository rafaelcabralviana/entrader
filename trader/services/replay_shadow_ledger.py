"""Ledger local ``replay_shadow`` (ambiente Replay — ordens fictícias, sem corretora)."""

from __future__ import annotations

from django.db import transaction

from trader.models import ClosedOperation, Position, PositionLiquidation


def delete_replay_shadow_ledger() -> dict[str, int]:
    """
    Apaga posições ``replay_shadow``, liquidações e ``ClosedOperation`` associados.

    Retorna contagens **antes** do delete (útil para mensagens).
    """
    qs = Position.objects.filter(
        trading_environment=Position.TradingEnvironment.REPLAY,
        position_lane=Position.Lane.REPLAY_SHADOW,
    )
    n_pos = qs.count()
    n_liq = PositionLiquidation.objects.filter(position__in=qs).count()
    n_co = ClosedOperation.objects.filter(position__in=qs).count()
    with transaction.atomic():
        PositionLiquidation.objects.filter(position__in=qs).delete()
        ClosedOperation.objects.filter(position__in=qs).delete()
        qs.delete()
    return {'positions': n_pos, 'liquidations': n_liq, 'closed_operations': n_co}


def replay_shadow_ledger_stats() -> dict[str, int]:
    """Contagens actuais (sem apagar)."""
    qs = Position.objects.filter(
        trading_environment=Position.TradingEnvironment.REPLAY,
        position_lane=Position.Lane.REPLAY_SHADOW,
    )
    return {
        'positions': qs.count(),
        'liquidations': PositionLiquidation.objects.filter(position__in=qs).count(),
        'closed_operations': ClosedOperation.objects.filter(position__in=qs).count(),
    }


__all__ = ['delete_replay_shadow_ledger', 'replay_shadow_ledger_stats']
