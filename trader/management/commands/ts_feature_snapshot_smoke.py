"""Grava um FeatureSnapshot minimo a partir do ultimo QuoteSnapshot (smoke test)."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand
from django.utils import timezone

from trader.models import FeatureSnapshot
from trader.trading_system.data.readers import latest_quote_snapshot, quote_dict_from_row
from trader.trading_system.features.engine import FeatureEngine


class Command(BaseCommand):
    help = 'Cria um FeatureSnapshot de teste para o ticker indicado.'

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument('ticker', nargs='?', default='WIN', type=str)

    def handle(self, *args: Any, **options: Any) -> None:
        ticker = (options.get('ticker') or 'WIN').strip().upper()
        row = latest_quote_snapshot(ticker)
        if row is None:
            self.stderr.write(self.style.WARNING(f'Sem QuoteSnapshot para {ticker}.'))
            return
        dt = row.captured_at or timezone.now()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
        as_of_ms = int(dt.timestamp() * 1000)
        eng = FeatureEngine()
        fv = eng.run(ticker=ticker, as_of_ts_ms=as_of_ms)
        FeatureSnapshot.objects.create(
            ticker=fv.ticker,
            as_of_ts_ms=fv.as_of_ts_ms,
            schema_version=fv.schema_version,
            regime=fv.regime,
            features=fv.features,
            source_quote=row,
        )
        self.stdout.write(self.style.SUCCESS(f'FeatureSnapshot criado para {ticker} @ {as_of_ms}.'))
