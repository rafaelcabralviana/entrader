"""
Uma única coleta pontual na API (sem Celery / sem standby).

- **Cotação:** GET ``/v1/marketdata/quote`` (:func:`~trader.services.marketdata.fetch_quote`).
- **Livro:** mesma ordem do worker — WebSocket ``SubscribeBook`` quando habilitado,
  senão REST ``/v1/marketdata/book`` (:func:`~trader.tasks._fetch_book_for_snapshot`).

Ambiente da API: variável ``SMART_TRADER_ENVIRONMENT`` ou flag ``--env``.

Exemplos::

    python manage.py coletar_snapshot_uma_vez PETR4
    python manage.py coletar_snapshot_uma_vez --tickers PETR4,VALE3
    python manage.py coletar_snapshot_uma_vez --env real WINJ26
    python manage.py coletar_snapshot_uma_vez --no-book PETR4
"""

from __future__ import annotations

import os
from argparse import ArgumentParser
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from trader.environment import set_current_environment
from trader.services.marketdata import fetch_quote
from trader.services.quote_history import save_book_snapshot, save_quote_snapshot
from trader.tasks import _fetch_book_for_snapshot


class Command(BaseCommand):
    help = 'Grava um QuoteSnapshot (e opcionalmente BookSnapshot) por ticker, uma vez, via API real.'

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            'tickers',
            nargs='*',
            help='Símbolos (ex.: PETR4 WINJ26). Se vazio, usa TRADER_WATCH_TICKERS ou PETR4.',
        )
        parser.add_argument(
            '--tickers',
            dest='tickers_csv',
            default='',
            help='Lista CSV alternativa (ex.: PETR4,VALE3). Ignora posicionais se não-vazio.',
        )
        parser.add_argument(
            '--env',
            choices=('real', 'simulator'),
            default=None,
            help='Ambiente Smart Trader para esta execução (default: SMART_TRADER_ENVIRONMENT / simulador).',
        )
        parser.add_argument(
            '--no-book',
            action='store_true',
            help='Não tenta gravar BookSnapshot (só quote).',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        raw_csv = (options.get('tickers_csv') or '').strip()
        if raw_csv:
            syms = [t.strip().upper() for t in raw_csv.split(',') if t.strip()]
        else:
            pos = options.get('tickers') or ()
            if pos:
                syms = [str(t).strip().upper() for t in pos if str(t).strip()]
            else:
                env_list = os.environ.get('TRADER_WATCH_TICKERS', 'PETR4')
                syms = [t.strip().upper() for t in env_list.split(',') if t.strip()]
        if not syms:
            raise CommandError('Informe ao menos um ticker.')

        if options.get('env'):
            set_current_environment(options['env'])
            self.stdout.write(self.style.NOTICE(f'Ambiente API desta execução: {options["env"]}'))

        no_book = bool(options.get('no_book'))
        for sym in syms:
            try:
                quote = fetch_quote(sym, use_cache=False)
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f'{sym} quote: {exc}'))
                continue
            snap = save_quote_snapshot(sym, quote)
            if snap is None:
                self.stderr.write(self.style.WARNING(f'{sym}: quote não gravado (payload inválido).'))
                continue
            self.stdout.write(
                self.style.SUCCESS(
                    f'{sym}: QuoteSnapshot id={snap.pk} status={(quote or {}).get("status")!r}'
                )
            )
            if no_book:
                continue
            book = _fetch_book_for_snapshot(sym)
            if not isinstance(book, dict):
                self.stdout.write(self.style.WARNING(f'{sym}: sem livro (WS/REST).'))
                continue
            bs = save_book_snapshot(sym, book)
            if bs is not None:
                self.stdout.write(self.style.SUCCESS(f'{sym}: BookSnapshot id={bs.pk}'))
            else:
                self.stdout.write(self.style.WARNING(f'{sym}: livro não gravado.'))
