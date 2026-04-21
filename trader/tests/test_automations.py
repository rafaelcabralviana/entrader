from django.contrib.auth.models import User
from django.test import Client, TestCase

from trader.environment import ENV_REAL, ENV_REPLAY, ENV_SIMULATOR
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from trader.automacoes.simulation import (
    SESSION_KEY_SIM_DATE,
    SESSION_KEY_SIM_ENABLED,
    SESSION_KEY_SIM_TICKER,
)
from trader.automacoes.thoughts import parse_calendar_day_brt, record_automation_thought
from trader.market_defaults import default_primary_ticker
from decimal import Decimal

from trader.models import (
    AutomationExecutionProfile,
    AutomationRuntimePreference,
    AutomationMarketSimPreference,
    AutomationStrategyToggle,
    AutomationThought,
    ClosedOperation,
    Position,
    QuoteSnapshot,
    WatchedTicker,
)


class AutomationsDashboardTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('auto_u1', 'auto_u1@test.com', 'x' * 12)
        self.client = Client()
        self.client.login(username='auto_u1', password='x' * 12)

    def test_dashboard_requires_login(self):
        c = Client()
        r = c.get('/automacoes/')
        self.assertEqual(r.status_code, 302)

    def test_dashboard_renders(self):
        r = self.client.get('/automacoes/')
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, 'id="automation-logs"')
        self.assertContains(r, 'Logs</h2>')
        self.assertContains(r, '/automacoes/logs/')
        self.assertContains(r, 'sidebar-estrategias')
        self.assertContains(r, 'sidebar-simulacao')
        self.assertContains(r, 'Simulador')
        self.assertContains(r, 'Salvar estrategias')
        self.assertContains(r, 'class="strat-modal-open"')
        self.assertContains(r, 'strat-modal-leafar')
        self.assertContains(r, 'automation-passive-marquee-inner')
        self.assertContains(r, 'Passivas')
        self.assertContains(r, 'Ativas')
        self.assertContains(r, 'strat-modal-tendencia_mercado')
        self.assertContains(r, 'id="automacoes-log-hide-system"')
        self.assertContains(r, 'id="automacoes-log-only-warn"')

    def test_save_strategies_persists(self):
        self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_strategies',
                'strategy_stop_percentual_book': 'on',
            },
        )
        row = AutomationStrategyToggle.objects.get(
            user=self.user,
            strategy_key='stop_percentual_book',
            trading_environment=ENV_SIMULATOR,
        )
        self.assertTrue(row.enabled)

    def test_save_strategies_in_replay_uses_simulator_storage(self):
        """Replay partilha toggles com Simulador; a BD deve usar trading_environment=simulator."""
        session = self.client.session
        session['trader_environment'] = ENV_REPLAY
        session.save()
        self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_strategies',
                'strategy_stop_percentual_book': 'on',
            },
        )
        row = AutomationStrategyToggle.objects.get(
            user=self.user,
            strategy_key='stop_percentual_book',
            trading_environment=ENV_SIMULATOR,
        )
        self.assertTrue(row.enabled)

    def test_runtime_toggle_saves_live_ticker_per_environment(self):
        WatchedTicker.objects.create(ticker='PETR4', enabled=True)
        self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_runtime_toggle',
                'runtime_environment': ENV_SIMULATOR,
                'robot_enabled': '0',
                'runtime_action': 'save_all',
                'max_open_operations': '2',
                'automation_live_ticker': 'PETR4',
            },
        )
        profile = AutomationExecutionProfile.objects.get(
            user=self.user,
            trading_environment=ENV_SIMULATOR,
            is_active=True,
        )
        self.assertEqual(profile.live_ticker, 'PETR4')

    def test_state_json(self):
        r = self.client.get('/automacoes/estado.json')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['environment'], 'simulator')
        self.assertIn('robot_enabled', data)
        self.assertIn('strategies', data)
        self.assertIn('stop_percentual_book', data['strategies'])
        self.assertIn('teste_limite_preco_34', data['strategies'])
        self.assertIn('tendencia_mercado', data['strategies'])
        self.assertIn('perfil_volume_montanhas', data['strategies'])

    def test_runtime_toggle_persists_by_environment(self):
        r = self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_runtime_toggle',
                'runtime_environment': ENV_SIMULATOR,
                'robot_enabled': '0',
            },
        )
        self.assertEqual(r.status_code, 302)
        row = AutomationRuntimePreference.objects.get(
            user=self.user,
            trading_environment=ENV_SIMULATOR,
        )
        self.assertFalse(row.enabled)
        data = self.client.get('/automacoes/estado.json').json()
        self.assertFalse(data['robot_enabled'])

    def test_runtime_toggle_real_does_not_change_simulator(self):
        AutomationRuntimePreference.objects.create(
            user=self.user,
            trading_environment=ENV_SIMULATOR,
            enabled=True,
        )
        self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_runtime_toggle',
                'runtime_environment': ENV_REAL,
                'robot_enabled': '0',
            },
        )
        sim = AutomationRuntimePreference.objects.get(
            user=self.user,
            trading_environment=ENV_SIMULATOR,
        )
        real = AutomationRuntimePreference.objects.get(
            user=self.user,
            trading_environment=ENV_REAL,
        )
        self.assertTrue(sim.enabled)
        self.assertFalse(real.enabled)

    def test_pensamentos_json_includes_passive_flags(self):
        t0 = record_automation_thought(
            self.user, ENV_SIMULATOR, 'Linha activa', source='leafar'
        ).id
        record_automation_thought(
            self.user, ENV_SIMULATOR, 'Linha passiva', source='comentario_preco_intradia'
        )
        r = self.client.get(f'/automacoes/pensamentos.json?since={t0}')
        self.assertEqual(r.status_code, 200)
        rows = r.json().get('thoughts') or []
        passive = [x for x in rows if x.get('source') == 'comentario_preco_intradia']
        self.assertTrue(passive)
        self.assertTrue(passive[0].get('is_passive'))
        self.assertIn('strategy_title', passive[0])

    def test_save_strategies_records_thought(self):
        self.client.post(
            '/automacoes/',
            {
                'form_name': 'automation_strategies',
                'strategy_janela_pregao': 'on',
            },
        )
        self.assertTrue(
            AutomationThought.objects.filter(user=self.user, source='estrategias').exists()
        )

    def test_logs_day_page_lists_thoughts(self):
        record_automation_thought(
            self.user, ENV_SIMULATOR, 'Evento de teste no histórico', source='test_hist'
        )
        r = self.client.get('/automacoes/logs/')
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, 'Logs por dia')
        self.assertContains(r, 'Evento de teste no histórico')
        self.assertContains(r, 'test_hist')

    def test_logs_day_requires_login(self):
        c = Client()
        r = c.get('/automacoes/logs/')
        self.assertEqual(r.status_code, 302)

    def test_clear_thoughts_clears_replay_shadow_ledger(self):
        """POST limpar-logs no Replay apaga PnL/posições do ledger fictício."""
        from django.utils import timezone as dj_tz

        from trader.environment import ENV_REPLAY

        now = dj_tz.now()
        p = Position.objects.create(
            ticker='XX99',
            trading_environment=Position.TradingEnvironment.REPLAY,
            position_lane=Position.Lane.REPLAY_SHADOW,
            side=Position.Side.LONG,
            quantity_open=Decimal('100'),
            avg_open_price=Decimal('10'),
            opened_at=now,
            is_active=False,
            closed_at=now,
        )
        ClosedOperation.objects.create(
            position=p,
            pnl_type=ClosedOperation.PnLType.ESTIMATED,
            gross_pnl=Decimal('50'),
            fees=Decimal('0'),
            net_pnl=Decimal('50'),
            closed_at=now,
        )
        self.assertTrue(Position.objects.filter(pk=p.pk).exists())
        r = self.client.post(
            '/automacoes/limpar-logs/',
            {'next': '/automacoes/', 'env': 'replay'},
        )
        self.assertEqual(r.status_code, 302)
        self.assertFalse(Position.objects.filter(ticker='XX99').exists())
        self.assertFalse(ClosedOperation.objects.filter(position__ticker='XX99').exists())

    def test_replay_shadow_ledger_clear_without_clearing_logs(self):
        """POST dedicado limpa só replay_shadow; exige sessão Replay."""
        from django.utils import timezone as dj_tz

        now = dj_tz.now()
        p = Position.objects.create(
            ticker='YY88',
            trading_environment=Position.TradingEnvironment.REPLAY,
            position_lane=Position.Lane.REPLAY_SHADOW,
            side=Position.Side.SHORT,
            quantity_open=Decimal('10'),
            avg_open_price=Decimal('20'),
            opened_at=now,
            is_active=True,
        )
        session = self.client.session
        session['trader_environment'] = ENV_REPLAY
        session.save()
        r = self.client.post(
            '/automacoes/limpar-ledger-replay-ficticio/',
            {'next': '/automacoes/'},
        )
        self.assertEqual(r.status_code, 302)
        self.assertFalse(Position.objects.filter(pk=p.pk).exists())

    def test_replay_shadow_ledger_clear_rejects_non_replay_session(self):
        from django.utils import timezone as dj_tz

        now = dj_tz.now()
        p = Position.objects.create(
            ticker='ZZ77',
            trading_environment=Position.TradingEnvironment.REPLAY,
            position_lane=Position.Lane.REPLAY_SHADOW,
            side=Position.Side.LONG,
            quantity_open=Decimal('1'),
            avg_open_price=Decimal('1'),
            opened_at=now,
            is_active=True,
        )
        session = self.client.session
        session['trader_environment'] = ENV_SIMULATOR
        session.save()
        r = self.client.post(
            '/automacoes/limpar-ledger-replay-ficticio/',
            {'next': '/automacoes/'},
        )
        self.assertEqual(r.status_code, 302)
        self.assertTrue(Position.objects.filter(pk=p.pk).exists())

    def test_parse_calendar_day_brt_invalid_falls_back(self):
        d = parse_calendar_day_brt('not-a-date')
        self.assertIsNotNone(d.year)

    def test_thoughts_json_since_zero_empty(self):
        r = self.client.get('/automacoes/pensamentos.json?since=0')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['thoughts'], [])

    def test_thoughts_json_incremental(self):
        t1 = record_automation_thought(
            self.user, ENV_SIMULATOR, 'Primeiro', source='test'
        )
        t2 = record_automation_thought(
            self.user, ENV_SIMULATOR, 'Segundo', source='test'
        )
        r = self.client.get(f'/automacoes/pensamentos.json?since={t1.id}')
        self.assertEqual(r.status_code, 200)
        rows = r.json()['thoughts']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['id'], t2.id)

    def test_market_simulation_post_stores_session(self):
        sym = default_primary_ticker()
        sp = ZoneInfo('America/Sao_Paulo')
        dt = datetime(2025, 3, 10, 12, 0, 0, tzinfo=sp)
        row = QuoteSnapshot.objects.create(
            ticker=sym,
            quote_data={'lastPrice': 100.0, 'lastQuantity': 1},
        )
        # captured_at tem auto_now_add — fixar instante para o teste de datas.
        QuoteSnapshot.objects.filter(pk=row.pk).update(captured_at=dt)
        self.client.post(
            '/automacoes/simulacao-mercado/',
            {
                'next': '/automacoes/',
                'sim_enabled': 'on',
                'sim_ticker': sym,
                'session_date': '2025-03-10',
            },
        )
        sess = self.client.session
        self.assertTrue(sess.get(SESSION_KEY_SIM_ENABLED))
        self.assertEqual(sess.get(SESSION_KEY_SIM_DATE), '2025-03-10')
        self.assertEqual(sess.get(SESSION_KEY_SIM_TICKER), sym)
        pref = AutomationMarketSimPreference.objects.filter(
            user=self.user, trading_environment=ENV_SIMULATOR
        ).first()
        self.assertIsNotNone(pref)
        self.assertTrue(pref.enabled)
        self.assertEqual(pref.sim_ticker, sym)
        self.assertEqual(pref.session_date.isoformat(), '2025-03-10')

    def test_market_simulation_clear_on_real_env_switch(self):
        AutomationMarketSimPreference.objects.create(
            user=self.user,
            trading_environment=ENV_SIMULATOR,
            enabled=True,
            session_date=date(2025, 1, 2),
            sim_ticker='WINJ26',
            replay_until=datetime(2025, 1, 2, 15, 0, 0, tzinfo=ZoneInfo('America/Sao_Paulo')),
        )
        s = self.client.session
        s[SESSION_KEY_SIM_ENABLED] = True
        s[SESSION_KEY_SIM_DATE] = '2025-01-02'
        s[SESSION_KEY_SIM_TICKER] = 'WINJ26'
        s.save()
        self.client.post(
            '/ambiente/selecionar/',
            {
                'next': '/automacoes/',
                'environment': 'real',
            },
        )
        s = self.client.session
        self.assertIsNone(s.get(SESSION_KEY_SIM_ENABLED))
        self.assertIsNone(s.get(SESSION_KEY_SIM_DATE))
        self.assertIsNone(s.get(SESSION_KEY_SIM_TICKER))
        pref = AutomationMarketSimPreference.objects.get(
            user=self.user, trading_environment=ENV_SIMULATOR
        )
        self.assertFalse(pref.enabled)
        self.assertEqual(pref.sim_ticker, '')
        self.assertIsNone(pref.replay_until)

    def test_sim_replay_cursor_updates_preference(self):
        sym = default_primary_ticker()
        sp = ZoneInfo('America/Sao_Paulo')
        dt = datetime(2025, 3, 10, 10, 0, 0, tzinfo=sp)
        row = QuoteSnapshot.objects.create(
            ticker=sym,
            quote_data={'lastPrice': 100.0, 'lastQuantity': 1},
        )
        QuoteSnapshot.objects.filter(pk=row.pk).update(captured_at=dt)
        self.client.post(
            '/automacoes/simulacao-mercado/',
            {
                'next': '/automacoes/',
                'sim_enabled': 'on',
                'sim_ticker': sym,
                'session_date': '2025-03-10',
            },
        )
        iso = dt.isoformat()
        r = self.client.post('/automacoes/sim-replay-cursor/', {'replay_until': iso})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get('ok'))
        pref = AutomationMarketSimPreference.objects.get(
            user=self.user, trading_environment=ENV_SIMULATOR
        )
        self.assertIsNotNone(pref.replay_until)

    def test_state_json_includes_market_simulation(self):
        r = self.client.get('/automacoes/estado.json')
        data = r.json()
        self.assertIn('market_simulation', data)
        self.assertTrue(data['market_simulation']['available'])

    def test_replay_day_json_paginated(self):
        sym = 'ZZRP'
        sp = ZoneInfo('America/Sao_Paulo')
        base = datetime(2025, 3, 10, 10, 0, 0, tzinfo=sp)
        for i in range(5):
            row = QuoteSnapshot.objects.create(
                ticker=sym,
                quote_data={'lastPrice': 10.0 + i, 'lastQuantity': 1},
            )
            QuoteSnapshot.objects.filter(pk=row.pk).update(captured_at=base + timedelta(seconds=i))
        self.client.post(
            '/automacoes/simulacao-mercado/',
            {
                'next': '/automacoes/',
                'sim_enabled': 'on',
                'sim_ticker': sym,
                'session_date': '2025-03-10',
            },
        )
        r1 = self.client.get(f'/automacoes/replay-dia.json?ticker={sym}&offset=0&limit=2')
        self.assertEqual(r1.status_code, 200)
        d1 = r1.json()
        self.assertEqual(len(d1['frames']), 2)
        self.assertEqual(d1['meta']['total'], 5)
        self.assertTrue(d1['meta']['has_more'])
        r2 = self.client.get(f'/automacoes/replay-dia.json?ticker={sym}&offset=2&limit=2')
        self.assertEqual(r2.status_code, 200)
        d2 = r2.json()
        self.assertEqual(len(d2['frames']), 2)
        self.assertTrue(d2['meta']['has_more'])
        r3 = self.client.get(f'/automacoes/replay-dia.json?ticker={sym}&offset=4&limit=2')
        self.assertEqual(r3.status_code, 200)
        d3 = r3.json()
        self.assertEqual(len(d3['frames']), 1)
        self.assertFalse(d3['meta']['has_more'])
