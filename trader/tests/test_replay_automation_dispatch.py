"""Motor de estratégias acoplado ao instante do replay de sessão."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase

from trader.automacoes.automation_engine import run_automation_session_replay_now
from trader.environment import ENV_SIMULATOR
from trader.models import AutomationStrategyToggle, AutomationThought, QuoteSnapshot


class ReplayAutomationDispatchTests(TestCase):
    def setUp(self):
        super().setUp()
        cache.clear()
        self.user = User.objects.create_user('replay_u1', 'replay_u1@test.com', 'x' * 12)
        AutomationStrategyToggle.objects.create(
            user=self.user,
            strategy_key='teste_limite_preco_34',
            trading_environment=ENV_SIMULATOR,
            enabled=True,
        )
        self.tz = ZoneInfo('America/Sao_Paulo')
        self.session_day = date(2025, 4, 17)
        self.base = datetime(2025, 4, 17, 10, 0, 0, tzinfo=self.tz)
        self.sym = 'XX34'
        prices = [33.0, 33.5, 34.0, 34.09, 34.14, 34.2]
        for i, lp in enumerate(prices):
            row = QuoteSnapshot.objects.create(
                ticker=self.sym,
                quote_data={'lastPrice': lp, 'lastQuantity': 1},
            )
            QuoteSnapshot.objects.filter(pk=row.pk).update(
                captured_at=self.base + timedelta(seconds=i)
            )

    def tearDown(self):
        cache.clear()
        super().tearDown()

    def test_replay_now_sem_disparo_antes_do_limite(self):
        AutomationThought.objects.all().delete()
        run_automation_session_replay_now(
            self.user,
            session_day=self.session_day,
            sim_ticker=self.sym,
            replay_until=self.base + timedelta(seconds=3),
        )
        self.assertFalse(
            AutomationThought.objects.filter(
                user=self.user, source='teste_limite_preco_34'
            ).exists()
        )

    def test_replay_now_dispara_no_instante_que_ultrapassa_limite(self):
        AutomationThought.objects.all().delete()
        run_automation_session_replay_now(
            self.user,
            session_day=self.session_day,
            sim_ticker=self.sym,
            replay_until=self.base + timedelta(seconds=4),
        )
        t = AutomationThought.objects.filter(
            user=self.user, source='teste_limite_preco_34'
        ).first()
        self.assertIsNotNone(t)
        assert t is not None
        self.assertIn('34.11', t.message)
        self.assertEqual(t.kind, AutomationThought.Kind.WARN)
