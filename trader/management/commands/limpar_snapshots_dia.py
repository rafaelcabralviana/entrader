"""
Apaga **QuoteSnapshot** (e opcionalmente **BookSnapshot**) de um dia inteiro,
usando ``captured_at`` em **America/Sao_Paulo** (meia-noite → meia-noite).

Por padrão remove **todos** os tickers desse dia. Exige ``--confirm`` para não apagar sem querer.

Na raiz do projeto::

    python manage.py limpar_snapshots_dia --data 2026-04-18 --confirm
    python manage.py limpar_snapshots_dia --data 2026-04-18 --ticker PETR4 --confirm
    python manage.py limpar_snapshots_dia --data 2026-04-18 --livro --confirm
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import date, datetime, time as time_cls, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from zoneinfo import ZoneInfo

from trader.models import BookSnapshot, QuoteSnapshot

_TZ_SP = ZoneInfo('America/Sao_Paulo')


class Command(BaseCommand):
    help = 'Apaga snapshots (quote e opcionalmente livro) de um dia inteiro em BRT.'

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            '--data',
            required=True,
            help='Dia ISO (YYYY-MM-DD) em calendário BRT.',
        )
        parser.add_argument(
            '--ticker',
            default='',
            help='Se informado, apaga só esse ticker (case-insensitive). Vazio = todos.',
        )
        parser.add_argument(
            '--livro',
            action='store_true',
            help='Apaga também BookSnapshot no mesmo intervalo.',
        )
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Obrigatório para executar o DELETE.',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if not options['confirm']:
            raise CommandError(
                'Para apagar de verdade, passe --confirm (proteção contra apagar sem querer).'
            )

        raw = str(options['data'] or '').strip()
        try:
            session_day = date.fromisoformat(raw)
        except ValueError as e:
            raise CommandError(f'--data inválida: {raw!r}') from e

        sym = (options.get('ticker') or '').strip().upper()

        day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
        day_end = day_start + timedelta(days=1)

        qqs = QuoteSnapshot.objects.filter(
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        )
        bqs = BookSnapshot.objects.filter(
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        )
        if sym:
            qqs = qqs.filter(ticker__iexact=sym)
            bqs = bqs.filter(ticker__iexact=sym)

        nq = qqs.count()
        nb = bqs.count() if options['livro'] else 0

        dq, _ = qqs.delete()
        self.stdout.write(self.style.WARNING(f'QuoteSnapshot removidos: {dq} (contagem pré-delete: {nq}).'))

        if options['livro']:
            db, _ = bqs.delete()
            self.stdout.write(self.style.WARNING(f'BookSnapshot removidos: {db} (contagem pré-delete: {nb}).'))

        escopo = f'ticker={sym}' if sym else 'TODOS os tickers'
        self.stdout.write(
            self.style.SUCCESS(
                f'OK — dia {session_day.isoformat()} BRT ({escopo}), '
                f'intervalo {day_start.isoformat()} .. {day_end.isoformat()}.'
            )
        )
