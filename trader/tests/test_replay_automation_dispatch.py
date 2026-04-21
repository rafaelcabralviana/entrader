"""Motor de estratégias acoplado ao instante do replay de sessão."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase

from trader.automacoes.automation_engine import run_automation_session_replay_now
from trader.automacoes.profiles import start_profile_runtime
from trader.environment import ENV_REPLAY, ENV_SIMULATOR
from trader.models import (
    AutomationExecutionProfile,
    AutomationStrategyToggle,
    AutomationThought,
    QuoteSnapshot,
)


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
            trading_environment=ENV_REPLAY,
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
            trading_environment=ENV_REPLAY,
        )
        t = AutomationThought.objects.filter(
            user=self.user, source='teste_limite_preco_34'
        ).first()
        self.assertIsNotNone(t)
        assert t is not None
        self.assertIn('34.11', t.message)
        self.assertEqual(t.kind, AutomationThought.Kind.WARN)

    def test_replay_now_force_ignores_simulation_cursor_guard(self):
        AutomationThought.objects.all().delete()
        AutomationExecutionProfile.objects.filter(user=self.user).update(is_active=False)
        sim_p = AutomationExecutionProfile.objects.create(
            user=self.user,
            trading_environment=ENV_REPLAY,
            name='ReplayStreamTest',
            mode=AutomationExecutionProfile.Mode.SIMULATION,
            sim_ticker=self.sym,
            session_date=self.session_day,
            is_active=True,
            is_system_default=False,
        )
        start_profile_runtime(sim_p, clear_cursor=True)
        sim_p.last_runtime_cursor_at = self.base + timedelta(seconds=10)
        sim_p.save(update_fields=['last_runtime_cursor_at', 'updated_at'])

        run_automation_session_replay_now(
            self.user,
            session_day=self.session_day,
            sim_ticker=self.sym,
            replay_until=self.base + timedelta(seconds=4),
            trading_environment=ENV_REPLAY,
        )
        self.assertFalse(
            AutomationThought.objects.filter(
                user=self.user, source='teste_limite_preco_34'
            ).exists()
        )

        AutomationThought.objects.all().delete()
        run_automation_session_replay_now(
            self.user,
            session_day=self.session_day,
            sim_ticker=self.sym,
            replay_until=self.base + timedelta(seconds=4),
            trading_environment=ENV_REPLAY,
            force=True,
        )
        self.assertTrue(
            AutomationThought.objects.filter(
                user=self.user, source='teste_limite_preco_34'
            ).exists()
        )
