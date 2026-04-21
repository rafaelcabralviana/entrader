"""
Dispara o motor de automações em sequência temporal sobre ``QuoteSnapshot`` já gravados
(mesmo critério que o replay por instante no simulador).

Exemplos::

    python manage.py stream_replay_motor --user 1 --ticker PETR4 --date 2025-04-17
    python manage.py stream_replay_motor --user 1 --ticker PETR4 --date 2025-04-17 --pace 0.5 --max 2000

Requisitos: simulador, runtime de automações activo, estratégias activas e perfil de
simulação com «execução iniciada» (``execution_started_at``), como no painel de simulação.
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import date as date_cls
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from trader.environment import set_current_environment
from trader.services.replay_stream_motor import stream_session_replay_ticks


class Command(BaseCommand):
    help = (
        'Percorre QuoteSnapshot do dia em ordem e chama o motor de replay a cada instante '
        '(teste de estratégias como fluxo temporal).'
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument('--user', type=int, required=True, help='ID do utilizador Django.')
        parser.add_argument('--ticker', required=True, help='Símbolo (ex.: PETR4).')
        parser.add_argument(
            '--date',
            dest='session_date',
            required=True,
            help='Dia da sessão YYYY-MM-DD (America/Sao_Paulo, alinhado ao gráfico).',
        )
        parser.add_argument(
            '--pace',
            type=float,
            default=1.0,
            help='Segundos entre cada instante (0 = sem pausa entre ticks).',
        )
        parser.add_argument(
            '--max',
            dest='max_snapshots',
            type=int,
            default=None,
            help='Teto de snapshots a processar (default: TRADER_REPLAY_STREAM_MAX_SNAPSHOTS ou 5000).',
        )
        parser.add_argument(
            '--env',
            choices=('real', 'simulator'),
            default='simulator',
            help='Contexto API (o motor de replay de sessão só corre no simulador).',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options.get('env'):
            set_current_environment(options['env'])
            self.stdout.write(self.style.NOTICE(f'Ambiente API desta execução: {options["env"]}'))

        raw = str(options.get('session_date') or '').strip()[:10]
        try:
            sd = date_cls.fromisoformat(raw)
        except ValueError as exc:
            raise CommandError(f'Data inválida: {raw!r}') from exc

        uid = int(options['user'])
        sym = str(options['ticker']).strip().upper()
        pace = float(options['pace'])
        mx = options.get('max_snapshots')

        out = stream_session_replay_ticks(
            user_id=uid,
            ticker=sym,
            session_day=sd,
            pace_sec=pace,
            max_snapshots=mx,
        )
        if not out.get('ok'):
            raise CommandError(str(out))
        self.stdout.write(self.style.SUCCESS(str(out)))
