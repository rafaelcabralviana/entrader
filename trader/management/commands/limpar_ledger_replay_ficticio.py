"""Apaga posições e PnL do ledger ``replay_shadow`` (simulador, replay fictício)."""

from __future__ import annotations

from django.core.management.base import BaseCommand

from trader.panel_context import invalidate_collateral_custody_cache
from trader.services.replay_shadow_ledger import (
    delete_replay_shadow_ledger,
    replay_shadow_ledger_stats,
)


class Command(BaseCommand):
    help = (
        'Remove do banco todas as posições ``replay_shadow`` (abertas ou fechadas), '
        'liquidações associadas e registos ``ClosedOperation``. Não altera o ledger da API.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Só mostra contagens; não apaga.',
        )

    def handle(self, *args, **options):
        dry = bool(options.get('dry_run'))
        stats = replay_shadow_ledger_stats()
        n_pos = stats['positions']
        n_liq = stats['liquidations']
        n_co = stats['closed_operations']
        self.stdout.write(
            f'Ledger replay fictício: {n_pos} posição(ões), {n_liq} liquidação(ões), '
            f'{n_co} operação(ões) encerrada(s).'
        )
        if dry:
            self.stdout.write(self.style.WARNING('Dry-run: nada foi apagado.'))
            return
        delete_replay_shadow_ledger()
        try:
            invalidate_collateral_custody_cache()
        except Exception:
            pass
        self.stdout.write(
            self.style.SUCCESS(
                f'Ledger replay fictício limpo: {n_pos} posição(ões), {n_liq} liquidação(ões), '
                f'{n_co} registo(s) de PnL encerrado(s).'
            )
        )
