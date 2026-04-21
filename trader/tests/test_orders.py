"""Testes do cliente REST de ordens (mocks de rede e assinatura)."""

from unittest.mock import MagicMock, patch

from django.contrib.auth.models import User
from django.test import SimpleTestCase, TestCase

from trader.models import QuoteSnapshot, WatchedTicker
from trader.panel_context import (
    _order_is_cancellable,
    _order_status_raw,
    order_column_heading_pt,
    quote_live_allows_automation_orders,
    quote_status_is_end_of_day,
    tabular_from_api_payload,
)


class QuoteStatusEndOfDayTests(SimpleTestCase):
    def test_detects_endofday_variants(self):
        self.assertTrue(quote_status_is_end_of_day({'status': 'endofday'}))
        self.assertTrue(quote_status_is_end_of_day({'status': 'EndOfDay'}))
        self.assertTrue(quote_status_is_end_of_day({'Status': 'END_OF_DAY'}))

    def test_other_status_not_eod(self):
        self.assertFalse(quote_status_is_end_of_day({'status': 'Trading'}))
        self.assertFalse(quote_status_is_end_of_day(None))
        self.assertFalse(quote_status_is_end_of_day({}))


class QuoteLiveAllowsAutomationOrdersTests(SimpleTestCase):
    def test_trading_and_missing_status_allow(self):
        self.assertTrue(quote_live_allows_automation_orders({'status': 'Trading'}))
        self.assertTrue(quote_live_allows_automation_orders({'Status': 'TRADING'}))
        self.assertTrue(quote_live_allows_automation_orders({}))
        self.assertTrue(quote_live_allows_automation_orders({'price': 100}))
        self.assertTrue(quote_live_allows_automation_orders(None))

    def test_endofday_and_non_trading_block(self):
        self.assertFalse(quote_live_allows_automation_orders({'status': 'EndOfDay'}))
        self.assertFalse(quote_live_allows_automation_orders({'status': 'PreTrading'}))
        self.assertFalse(quote_live_allows_automation_orders({'status': 'AfterTrading'}))


class OrderColumnHeadingTests(SimpleTestCase):
    def test_translates_api_keys_to_pt(self):
        self.assertEqual(order_column_heading_pt('id'), 'ID')
        self.assertEqual(order_column_heading_pt('status'), 'Status')
        self.assertEqual(order_column_heading_pt('message'), 'Mensagem')
        self.assertEqual(order_column_heading_pt('module'), 'Módulo')
        self.assertEqual(order_column_heading_pt('type'), 'Tipo')
        self.assertEqual(order_column_heading_pt('ticker'), 'Ativo')
        self.assertEqual(order_column_heading_pt('side'), 'Lado')
        self.assertEqual(order_column_heading_pt('quantity'), 'Quantidade')
        self.assertEqual(order_column_heading_pt('price'), 'Preço')
        self.assertEqual(order_column_heading_pt('timeInForce'), 'Validade (TIF)')
        self.assertEqual(order_column_heading_pt('stop'), 'Stop')
        self.assertEqual(order_column_heading_pt('averagePrice'), 'Preço médio')
        self.assertEqual(order_column_heading_pt('openQuantity'), 'Qtd. em aberto')
        self.assertEqual(order_column_heading_pt('executedQuantity'), 'Qtd. executada')
        self.assertEqual(order_column_heading_pt('received'), 'Recebida em')


class OrderCancelButtonStatusTests(SimpleTestCase):
    """Prioridade orderStatus vs status; estados finais sem botão Cancelar."""

    def test_prefers_order_status_over_generic_status(self):
        self.assertEqual(
            _order_status_raw(
                {'status': 'Filled', 'orderStatus': 'Working'},
            ),
            'Working',
        )

    def test_cancellable_when_working_or_new(self):
        self.assertTrue(_order_is_cancellable({'orderStatus': 'Working'}))
        self.assertTrue(_order_is_cancellable({'orderStatus': 'New'}))
        self.assertTrue(_order_is_cancellable({'orderStatus': 'PartiallyFilled'}))

    def test_not_cancellable_when_terminal(self):
        self.assertFalse(_order_is_cancellable({'orderStatus': 'Filled'}))
        self.assertFalse(_order_is_cancellable({'orderStatus': 'Canceled'}))
        self.assertFalse(_order_is_cancellable({'orderStatus': 'PendingCancel'}))

    def test_unknown_status_allows_cancel_button(self):
        self.assertTrue(_order_is_cancellable({}))


class OrdersClientTests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        self._patches = [
            patch('trader.services.orders._respect_1_per_second'),
            patch('trader.services.orders._respect_sliding'),
            patch('trader.services.orders.get_access_token', return_value='tok'),
            patch(
                'trader.services.orders.api_config.subscription_key',
                return_value='sub',
            ),
            patch('trader.services.orders.api_config.user_agent', return_value='UA'),
            patch(
                'trader.services.orders.api_config.api_base_url',
                return_value='https://example.test/api',
            ),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()
        super().tearDown()

    @patch('trader.services.orders.requests.get')
    def test_fetch_orders_get(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=b'[]', json=lambda: [])
        from trader.services.orders import fetch_orders

        fetch_orders()
        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        self.assertTrue(url.endswith('/v1/orders'))

    @patch('trader.services.orders.requests.get')
    def test_fetch_orders_history_params(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import fetch_orders_history

        fetch_orders_history('2026-01-01', '2026-01-31', page_index=1, page_size=50)
        kwargs = mock_get.call_args[1]
        self.assertEqual(
            kwargs['params'],
            {'from': '2026-01-01', 'to': '2026-01-31', 'pageIndex': 1, 'pageSize': 50},
        )

    @patch('trader.services.orders.requests.post')
    def test_post_cancel_uses_Id_param(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import post_cancel_order

        post_cancel_order('ord-1')
        kwargs = mock_post.call_args[1]
        self.assertEqual(kwargs['params'], {'Id': 'ord-1'})

    @patch('trader.services.orders.requests.post')
    def test_post_cancel_retries_with_lowercase_id(self, mock_post):
        resp_400 = MagicMock(status_code=400, content=b'{"message":"bad request"}', text='{"message":"bad request"}')
        resp_400.json = lambda: {"message": "bad request"}
        resp_200 = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        mock_post.side_effect = [resp_400, resp_200]
        from trader.services.orders import post_cancel_order

        post_cancel_order('ord-2')
        self.assertEqual(mock_post.call_count, 2)
        first_params = mock_post.call_args_list[0][1]['params']
        second_params = mock_post.call_args_list[1][1]['params']
        self.assertEqual(first_params, {'Id': 'ord-2'})
        self.assertEqual(second_params, {'id': 'ord-2'})

    @patch('trader.services.orders.get_current_environment', return_value='real')
    @patch('trader.services.orders.generate_body_signature', return_value='SIG')
    @patch('trader.services.orders.requests.post')
    def test_post_send_market_signature_and_body(self, mock_post, _sig, _env):
        mock_post.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import post_send_market_order

        body = {
            'Module': 'DayTrade',
            'Ticker': 'PETR4',
            'Side': 'Buy',
            'Quantity': 100,
            'TimeInForce': 'Day',
        }
        post_send_market_order(body)
        kwargs = mock_post.call_args[1]
        self.assertEqual(kwargs['headers']['BODY_SIGNATURE'], 'SIG')
        self.assertIn(
            '"Ticker":"PETR4"',
            kwargs['data'].decode('utf-8'),
        )
        self.assertTrue(mock_post.call_args[0][0].endswith('/v1/orders/send/market'))

    @patch('trader.services.orders.get_current_environment', return_value='simulator')
    @patch('trader.services.orders.generate_body_signature', return_value='SIG')
    @patch('trader.services.orders.requests.post')
    def test_post_send_market_simulator_calls_setup_filled_first(self, mock_post, _sig, _env):
        mock_post.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import post_send_market_order

        body = {
            'Module': 'DayTrade',
            'Ticker': 'PETR4',
            'Side': 'Buy',
            'Quantity': 1,
            'TimeInForce': 'Day',
        }
        post_send_market_order(body)
        self.assertEqual(mock_post.call_count, 2)
        first_url = mock_post.call_args_list[0][0][0]
        second_url = mock_post.call_args_list[1][0][0]
        self.assertTrue(first_url.endswith('/v1/setup/orders'))
        self.assertTrue(second_url.endswith('/v1/orders/send/market'))
        setup_headers = mock_post.call_args_list[0][1]['headers']
        self.assertNotIn('BODY_SIGNATURE', setup_headers)

    @patch('trader.services.orders.generate_body_signature', return_value='SIG')
    @patch('trader.services.orders.requests.post')
    def test_post_replace_market_query_id(self, mock_post, _sig):
        mock_post.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import post_replace_market_order

        post_replace_market_order('abc', {'Quantity': 10, 'TimeInForce': 'Day'})
        kwargs = mock_post.call_args[1]
        self.assertEqual(kwargs['params'], {'id': 'abc'})

    @patch('trader.services.orders.requests.post')
    def test_post_simulator_setup_orders_body(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, content=b'{}', json=lambda: {})
        from trader.services.orders import post_simulator_setup_orders

        post_simulator_setup_orders('filled')
        kwargs = mock_post.call_args[1]
        self.assertNotIn('BODY_SIGNATURE', kwargs['headers'])
        self.assertIn('"orderStatus":"filled"', kwargs['data'].decode('utf-8'))
        self.assertTrue(mock_post.call_args[0][0].endswith('/v1/setup/orders'))

    def test_post_simulator_setup_orders_rejects_non_api_status(self):
        from trader.services.orders import post_simulator_setup_orders

        with self.assertRaises(ValueError):
            post_simulator_setup_orders('new')

    def test_post_simulator_setup_orders_invalid(self):
        from trader.services.orders import post_simulator_setup_orders

        with self.assertRaises(ValueError) as ctx:
            post_simulator_setup_orders('not-a-status')
        self.assertIn('order_status deve ser', str(ctx.exception))


class OrderStatusBadgeContextTests(SimpleTestCase):
    @patch('trader.panel_context.fetch_orders_cached')
    def test_status_column_gets_open_badge(self, mock_fetch):
        mock_fetch.return_value = [
            {
                'Id': 'a1',
                'orderStatus': 'New',
                'received': '2026-01-01T12:00:00+00:00',
            },
        ]
        from trader.panel_context import build_orders_context

        ctx = build_orders_context()
        cells = ctx['orders_table_display'][0][0]
        row_html = ''.join(str(c) for c in cells)
        self.assertIn('ord-st-open', row_html)
        self.assertIn('○', row_html)


class OrdersViewTests(TestCase):
    """Rotas do painel: / exige login; /painel/ público (travado), ordens exige login."""

    def test_cancel_order_post_requires_login(self):
        r = self.client.post('/ordens/cancelar/', {'order_id': 'x'})
        self.assertEqual(r.status_code, 302)
        self.assertIn('/entrar/', r.url or '')

    @patch('trader.views.post_cancel_order')
    def test_cancel_order_post_calls_api(self, mock_cancel):
        User.objects.create_user('cancel_u', password='secret-test-co')
        self.client.login(username='cancel_u', password='secret-test-co')
        self.client.get('/')
        mock_cancel.return_value = {}
        r = self.client.post(
            '/ordens/cancelar/',
            {'order_id': 'ord-xyz', 'next': '/#ordens'},
        )
        self.assertEqual(r.status_code, 302)
        mock_cancel.assert_called_once_with('ord-xyz')

    @patch('trader.views.build_orders_context')
    @patch('trader.views.post_cancel_order')
    def test_cancel_order_ajax_returns_orders_html(self, mock_cancel, mock_ctx):
        User.objects.create_user('cancel_ajax_u', password='secret-cancel-1')
        self.client.login(username='cancel_ajax_u', password='secret-cancel-1')
        mock_cancel.return_value = {}
        mock_ctx.return_value = {
            'orders_error': None,
            'order_column_keys': ['id'],
            'order_column_labels': ['ID'],
            'orders_table_display': [(['ord-xyz'], 'ord-xyz', True)],
            'order_table_rows': [],
            'order_cancel_ids': [],
            'orders_raw_json': None,
        }
        r = self.client.post(
            '/ordens/cancelar/',
            {
                'order_id': 'ord-xyz',
                'next': '/#ordens',
                'dashboard_home': '1',
            },
            HTTP_X_REQUESTED_WITH='XMLHttpRequest',
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertTrue(data.get('ok'))
        self.assertIn('orders_html', data)
        self.assertIn('ord-xyz', data['orders_html'])
        mock_cancel.assert_called_once_with('ord-xyz')
        mock_ctx.assert_called_once()

    def test_root_redirects_anonymous_to_login(self):
        r = self.client.get('/')
        self.assertEqual(r.status_code, 302)
        self.assertIn('/entrar/', r.url or '')

    def test_painel_200_anonymous(self):
        r = self.client.get('/painel/')
        self.assertEqual(r.status_code, 200)

    def test_ordens_redirects_if_not_logged_in(self):
        r = self.client.get('/ordens/')
        self.assertEqual(r.status_code, 302)
        self.assertIn('/entrar/', r.url or '')

    def test_boleta_auto_stop_suggest_requires_login(self):
        r = self.client.get('/automacoes/boleta/auto-stop.json?ticker=PETR4')
        self.assertEqual(r.status_code, 302)

    @patch('trader.automacoes.boleta.build_market_context_local')
    def test_boleta_auto_stop_suggest_json_buy(self, mock_ctx):
        User.objects.create_user('as_u', password='secret-as-1')
        self.client.login(username='as_u', password='secret-as-1')
        mock_ctx.return_value = {
            'ticker': 'PETR4',
            'quote': {'lastPrice': 100.0},
            'book': None,
        }
        r = self.client.get(
            '/automacoes/boleta/auto-stop.json'
            '?ticker=PETR4&side=Buy&pct=1&basis=last&order_delta_pct=0'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertTrue(data.get('ok'))
        self.assertEqual(data.get('ref_price'), '100')
        self.assertEqual(data.get('stop_trigger'), '101')
        self.assertEqual(data.get('stop_order'), '101')

    def test_orders_panel_fragment_requires_login(self):
        r = self.client.get('/ordens/painel-parcial/')
        self.assertEqual(r.status_code, 302)

    def test_collateral_custody_fragment_requires_login(self):
        r = self.client.get('/painel/garantias-custodia.html')
        self.assertEqual(r.status_code, 302)

    @patch('trader.views.build_collateral_custody_context')
    def test_collateral_custody_fragment_200(self, mock_cc):
        User.objects.create_user('strip_u', password='secret-strip-1')
        self.client.login(username='strip_u', password='secret-strip-1')
        mock_cc.return_value = {
            'api_collateral_display': {'empty': True},
            'api_custody_display': {'empty': True},
        }
        r = self.client.get('/painel/garantias-custodia.html')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Garantias', r.content)
        mock_cc.assert_called_once()

    @patch('trader.views.build_orders_context')
    def test_orders_panel_fragment_200(self, mock_ctx):
        User.objects.create_user('frag_u', password='secret-frag-1')
        self.client.login(username='frag_u', password='secret-frag-1')
        mock_ctx.return_value = {'orders_error': None, 'order_column_keys': []}
        r = self.client.get('/ordens/painel-parcial/')
        self.assertEqual(r.status_code, 200)
        mock_ctx.assert_called_once()

    def test_save_watch_tickers_updates_db_list(self):
        User.objects.create_user('watch_u', password='secret-watch-1')
        self.client.login(username='watch_u', password='secret-watch-1')
        WatchedTicker.objects.create(ticker='PETR4', enabled=True)
        WatchedTicker.objects.create(ticker='VALE3', enabled=True)

        r = self.client.post(
            '/market/watch-tickers-save/',
            {
                'watch_tickers': 'PETR4,ITUB4',
                'next': '/mercado/',
            },
        )
        self.assertEqual(r.status_code, 302)

        enabled = list(
            WatchedTicker.objects.filter(enabled=True)
            .order_by('ticker')
            .values_list('ticker', flat=True)
        )
        self.assertEqual(enabled, ['ITUB4', 'PETR4'])

    @patch('trader.views.build_orders_context')
    @patch('trader.views.run_order_test_form')
    def test_home_boleta_ajax_returns_orders_html_in_json(self, mock_run, mock_ctx):
        User.objects.create_user('ajax_u', password='secret-ajax-1')
        self.client.login(username='ajax_u', password='secret-ajax-1')
        mock_run.return_value = ({}, '{"orderId": "x"}', None)
        mock_ctx.return_value = {
            'orders_error': None,
            'order_column_keys': ['id'],
            'order_column_labels': ['ID'],
            'orders_table_display': [(['oid-1'], 'c1', True)],
            'order_table_rows': [],
            'order_cancel_ids': [],
            'orders_raw_json': None,
        }
        r = self.client.post(
            '/painel/',
            {
                'setup': 'filled',
                'order_type': 'market',
                'order_ticker': 'PETR4',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
            },
            HTTP_X_REQUESTED_WITH='XMLHttpRequest',
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertTrue(data.get('ok'))
        self.assertIn('orders_html', data)
        self.assertIn('oid-1', data['orders_html'])
        session = self.client.session
        self.assertEqual(session.get('last_sent_order_id'), 'x')
        mock_run.assert_called_once()
        mock_ctx.assert_called_once()

    def test_market_snapshot_json_redirects_if_not_logged_in(self):
        r = self.client.get('/mercado/snapshot.json?ticker=PETR4')
        self.assertEqual(r.status_code, 302)

    @patch('trader.views.build_market_context_local')
    def test_market_snapshot_json_200_when_logged_in(self, mock_ctx):
        User.objects.create_user('snap_u', password='secret-test-1')
        self.client.login(username='snap_u', password='secret-test-1')
        mock_ctx.return_value = {
            'ticker': 'PETR4',
            'errors': {},
            'details': {'ticker': 'PETR4'},
            'quote': {'ticker': 'PETR4', 'lastPrice': 10.0, 'dateTime': '2026-04-15T00:28:43-03:00'},
            'book': None,
            'aggregate_book': None,
            'agg_bids': [],
            'agg_asks': [],
            'chart_payload': None,
            'operation_hints': {
                'mercado': 'BOVESPA',
                'base_bmf': '—',
                'limite_ordens_dia': 10,
                'limite_boleta': 500,
            },
        }
        r = self.client.get('/mercado/snapshot.json?ticker=PETR4')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['ticker'], 'PETR4')
        self.assertEqual(data['poll_ms'], 2200)
        self.assertTrue(data.get('live_poll_active', False))
        self.assertIn('quote_latency_ms', data)
        self.assertEqual(QuoteSnapshot.objects.filter(ticker='PETR4').count(), 0)
        mock_ctx.assert_called_once_with('PETR4')

    @patch('trader.views.build_market_context_local')
    def test_market_snapshot_live_poll_false_on_endofday(self, mock_ctx):
        User.objects.create_user('snap_eod', password='secret-test-2')
        self.client.login(username='snap_eod', password='secret-test-2')
        mock_ctx.return_value = {
            'ticker': 'WINZ25',
            'errors': {},
            'details': None,
            'quote': {'status': 'EndOfDay', 'lastPrice': 100},
            'book': None,
            'aggregate_book': None,
            'agg_bids': [],
            'agg_asks': [],
            'chart_payload': None,
            'operation_hints': {
                'mercado': 'BMF',
                'base_bmf': 'WIN',
                'limite_ordens_dia': 10,
                'limite_boleta': 5,
            },
        }
        r = self.client.get('/mercado/snapshot.json?ticker=WINZ25')
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()['live_poll_active'])

    @patch('trader.views.build_market_context_local')
    def test_market_snapshot_live_poll_false_on_404_errors(self, mock_ctx):
        User.objects.create_user('snap_404', password='secret-test-3')
        self.client.login(username='snap_404', password='secret-test-3')
        mock_ctx.return_value = {
            'ticker': 'WINZ25',
            'errors': {'quote': 'A API retornou status 404 em /quote.'},
            'details': None,
            'quote': None,
            'book': None,
            'aggregate_book': None,
            'agg_bids': [],
            'agg_asks': [],
            'chart_payload': None,
            'operation_hints': {
                'mercado': 'BMF',
                'base_bmf': 'WIN',
                'limite_ordens_dia': 10,
                'limite_boleta': 5,
            },
        }
        r = self.client.get('/mercado/snapshot.json?ticker=WINZ25')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertFalse(data['live_poll_active'])
        self.assertEqual(data.get('pause_reason'), 'status_404')

    def test_quote_history_json_requires_login(self):
        r = self.client.get('/mercado/quote-history.json?ticker=PETR4')
        self.assertEqual(r.status_code, 302)

    def test_quote_history_json_returns_saved_quote_fields(self):
        User.objects.create_user('qh_u', password='secret-qh-1')
        self.client.login(username='qh_u', password='secret-qh-1')
        QuoteSnapshot.objects.create(
            ticker='PETR4',
            quote_data={'ticker': 'PETR4', 'lastPrice': 10.5, 'status': 'Trading'},
        )
        r = self.client.get('/mercado/quote-history.json?ticker=PETR4&limit=10')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['ticker'], 'PETR4')
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['items'][0]['quote_data']['lastPrice'], 10.5)

    def test_quote_candles_json_requires_login(self):
        r = self.client.get('/mercado/candles.json?ticker=PETR4&interval=1m')
        self.assertEqual(r.status_code, 302)

    def test_quote_candles_json_returns_aggregated_ohlcv(self):
        User.objects.create_user('qc_u', password='secret-qc-1')
        self.client.login(username='qc_u', password='secret-qc-1')
        QuoteSnapshot.objects.create(
            ticker='PETR4',
            quote_data={'ticker': 'PETR4', 'lastPrice': 10.0, 'lastQuantity': 100},
        )
        QuoteSnapshot.objects.create(
            ticker='PETR4',
            quote_data={'ticker': 'PETR4', 'lastPrice': 11.0, 'lastQuantity': 50},
        )
        r = self.client.get('/mercado/candles.json?ticker=PETR4&interval=1m&limit=10')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['ticker'], 'PETR4')
        self.assertTrue(data.get('live_poll_active', False))
        self.assertGreaterEqual(data['count'], 1)
        c = data['candles'][-1]
        self.assertIn('open', c)
        self.assertIn('high', c)
        self.assertIn('low', c)
        self.assertIn('close', c)
        self.assertIn('volume', c)

        r2 = self.client.get('/mercado/candles.json?ticker=PETR4&interval=10s&limit=10')
        self.assertEqual(r2.status_code, 200)
        data2 = r2.json()
        self.assertEqual(data2['interval_sec'], 10)
        c2 = data2['candles'][-1]
        self.assertIn('label', c2)
        self.assertGreaterEqual(str(c2['label']).count(':'), 2)

    def test_quote_candles_json_accepts_lastprice_alias(self):
        """Snapshots só com LastPrice (sem lastPrice) ainda geram candles."""
        User.objects.create_user('qc_lp', password='secret-qc-lp')
        self.client.login(username='qc_lp', password='secret-qc-lp')
        QuoteSnapshot.objects.create(
            ticker='LP99',
            quote_data={'ticker': 'LP99', 'LastPrice': 55.5, 'lastQuantity': 1},
        )
        r = self.client.get('/mercado/candles.json?ticker=LP99&interval=1m&limit=10')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertGreaterEqual(data['count'], 1)
        self.assertEqual(data['candles'][-1]['close'], 55.5)

    def test_quote_candles_session_date_tail_when_snapshot_cap_exceeded(self):
        """Com muitos snapshots num dia, o GET deve agregar a cauda (até replay/fim), não só a manhã."""
        User.objects.create_user('qc_tail', password='secret-qc-tail')
        self.client.login(username='qc_tail', password='secret-qc-tail')
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        sp = ZoneInfo('America/Sao_Paulo')
        day = datetime(2024, 6, 15, 10, 0, 0, tzinfo=sp)
        objs = []
        for i in range(4500):
            objs.append(
                QuoteSnapshot(
                    ticker='ZZTAIL',
                    captured_at=day + timedelta(seconds=i),
                    quote_data={
                        'ticker': 'ZZTAIL',
                        'lastPrice': float(i + 1),
                        'lastQuantity': 1.0,
                    },
                )
            )
        QuoteSnapshot.objects.bulk_create(objs, batch_size=1500)
        r = self.client.get(
            '/mercado/candles.json?ticker=ZZTAIL&interval=1m&limit=120&session_date=2024-06-15'
        )
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertGreater(len(data['candles']), 0, data)
        self.assertEqual(
            data['candles'][-1]['close'],
            4500.0,
            'Último candle deve refletir o último snapshot do dia (cauda), não o corte da manhã.',
        )

    def test_quote_candles_json_filters_by_session_date(self):
        User.objects.create_user('qc_u2', password='secret-qc-2')
        self.client.login(username='qc_u2', password='secret-qc-2')
        from datetime import datetime
        from zoneinfo import ZoneInfo

        sp = ZoneInfo('America/Sao_Paulo')
        d_old = datetime(2024, 3, 10, 15, 0, 0, tzinfo=sp)
        d_new = datetime(2024, 3, 11, 15, 0, 0, tzinfo=sp)
        q1 = QuoteSnapshot.objects.create(
            ticker='ZZ11',
            quote_data={'ticker': 'ZZ11', 'lastPrice': 1.0, 'lastQuantity': 1},
        )
        QuoteSnapshot.objects.filter(pk=q1.pk).update(captured_at=d_old)
        q2 = QuoteSnapshot.objects.create(
            ticker='ZZ11',
            quote_data={'ticker': 'ZZ11', 'lastPrice': 2.0, 'lastQuantity': 1},
        )
        QuoteSnapshot.objects.filter(pk=q2.pk).update(captured_at=d_new)
        r = self.client.get('/mercado/candles.json?ticker=ZZ11&interval=1m&limit=50&session_date=2024-03-10')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['session_date'], '2024-03-10')
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['candles'][-1]['close'], 1.0)
        self.assertFalse(data.get('live_poll_active', True))

    def test_quote_candles_json_live_poll_false_when_last_quote_endofday(self):
        User.objects.create_user('qc_eod', password='secret-qc-eod')
        self.client.login(username='qc_eod', password='secret-qc-eod')
        QuoteSnapshot.objects.create(
            ticker='PETR4',
            quote_data={
                'ticker': 'PETR4',
                'lastPrice': 10.0,
                'lastQuantity': 100,
                'status': 'EndOfDay',
            },
        )
        r = self.client.get('/mercado/candles.json?ticker=PETR4&interval=1m&limit=10')
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()['live_poll_active'])

    def test_quote_candles_session_dates_json_lists_days_with_data(self):
        User.objects.create_user('qc_u3', password='secret-qc-3')
        self.client.login(username='qc_u3', password='secret-qc-3')
        from datetime import datetime
        from zoneinfo import ZoneInfo

        sp = ZoneInfo('America/Sao_Paulo')
        d_a = datetime(2024, 6, 1, 10, 0, 0, tzinfo=sp)
        d_b = datetime(2024, 6, 2, 10, 0, 0, tzinfo=sp)
        qa = QuoteSnapshot.objects.create(
            ticker='ZZ22',
            quote_data={'ticker': 'ZZ22', 'lastPrice': 1.0, 'lastQuantity': 1},
        )
        QuoteSnapshot.objects.filter(pk=qa.pk).update(captured_at=d_a)
        qb = QuoteSnapshot.objects.create(
            ticker='ZZ22',
            quote_data={'ticker': 'ZZ22', 'lastPrice': 2.0, 'lastQuantity': 1},
        )
        QuoteSnapshot.objects.filter(pk=qb.pk).update(captured_at=d_b)
        r = self.client.get('/mercado/candles-session-dates.json?ticker=ZZ22')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data['ticker'], 'ZZ22')
        self.assertIn('2024-06-01', data['dates'])
        self.assertIn('2024-06-02', data['dates'])


class CeleryCollectQuotesTests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        from django.core import cache as django_cache
        from unittest.mock import patch

        django_cache.cache.clear()
        # Lock/standby usam Redis em produção; nos testes força fallback ao cache Django.
        self._patch_broker_redis = patch('trader.tasks._broker_redis', return_value=None)
        self._patch_broker_redis.start()
        self.addCleanup(self._patch_broker_redis.stop)

    @patch('trader.automacoes.automation_engine.run_automation_after_quote_collect')
    @patch('trader.tasks.save_book_snapshot')
    @patch('trader.tasks._fetch_book_for_snapshot')
    @patch('trader.tasks.save_quote_snapshot')
    @patch('trader.tasks.fetch_quote')
    @patch('trader.tasks.WatchedTicker')
    @patch('trader.tasks.settings')
    def test_collect_watch_quotes_enabled(
        self,
        mock_settings,
        mock_watched,
        mock_fetch_quote,
        mock_save_quote,
        mock_fetch_book_for_snapshot,
        mock_save_book,
        _mock_auto,
    ):
        mock_settings.TRADER_WATCH_ENABLED = True
        mock_settings.TRADER_QUOTE_SAVE_BRT_WINDOW_ENABLED = False
        mock_settings.TRADER_WATCH_TICKERS = []
        mock_watched.objects.filter.return_value.values_list.return_value = ['PETR4', 'VALE3']
        mock_fetch_quote.return_value = {'ticker': 'PETR4', 'lastPrice': 10.0}
        mock_fetch_book_for_snapshot.return_value = {'ticker': 'PETR4', 'bids': [], 'asks': []}
        from trader.tasks import collect_watch_quotes

        out = collect_watch_quotes()
        self.assertTrue(out['enabled'])
        self.assertEqual(out['saved'], 2)
        self.assertEqual(mock_fetch_quote.call_count, 2)
        self.assertEqual(mock_save_quote.call_count, 2)
        self.assertEqual(mock_fetch_book_for_snapshot.call_count, 2)
        self.assertEqual(mock_save_book.call_count, 2)

    @patch('trader.automacoes.automation_engine.run_automation_after_quote_collect')
    @patch('trader.tasks.save_book_snapshot')
    @patch('trader.tasks._fetch_book_for_snapshot')
    @patch('trader.tasks.save_quote_snapshot')
    @patch('trader.tasks.fetch_quote')
    @patch('trader.tasks.WatchedTicker')
    @patch('trader.tasks.settings')
    def test_collect_watch_quotes_end_of_day_standby(
        self,
        mock_settings,
        mock_watched,
        mock_fetch_quote,
        mock_save_quote,
        mock_fetch_book_for_snapshot,
        mock_save_book,
        _mock_auto,
    ):
        import time

        from django.core.cache import cache

        from trader.tasks import _WATCH_STANDBY_UNTIL_KEY, collect_watch_quotes

        mock_settings.TRADER_WATCH_ENABLED = True
        mock_settings.TRADER_QUOTE_SAVE_BRT_WINDOW_ENABLED = False
        mock_settings.TRADER_WATCH_STANDBY_ENABLED = True
        mock_settings.TRADER_WATCH_TICKERS = []
        mock_watched.objects.filter.return_value.values_list.return_value = ['PETR4', 'VALE3']
        mock_fetch_quote.return_value = {'status': 'EndOfDay', 'lastPrice': 10.0}
        mock_save_quote.return_value = object()

        out = collect_watch_quotes()
        self.assertTrue(out.get('market_standby'))
        self.assertEqual(mock_fetch_quote.call_count, 1)
        mock_fetch_book_for_snapshot.assert_not_called()
        mock_save_book.assert_not_called()
        _mock_auto.assert_called_once()
        raw = cache.get(_WATCH_STANDBY_UNTIL_KEY)
        self.assertIsInstance(raw, (int, float))
        self.assertGreater(float(raw), time.time())

    @patch('trader.tasks._watch_list_tickers', return_value=[])
    @patch('trader.tasks.fetch_quote')
    @patch('trader.tasks.settings')
    def test_collect_watch_quotes_standby_short_circuit(
        self, mock_settings, mock_fetch_quote, _mock_watch_tickers
    ):
        import time

        from django.core.cache import cache

        from trader.tasks import _WATCH_STANDBY_UNTIL_KEY, collect_watch_quotes

        mock_settings.TRADER_WATCH_ENABLED = True
        mock_settings.TRADER_QUOTE_SAVE_BRT_WINDOW_ENABLED = False
        mock_settings.TRADER_WATCH_STANDBY_ENABLED = True
        cache.set(_WATCH_STANDBY_UNTIL_KEY, time.time() + 3600.0)

        out = collect_watch_quotes()
        self.assertTrue(out.get('market_standby'))
        self.assertEqual(out.get('saved'), 0)
        mock_fetch_quote.assert_not_called()
        _mock_watch_tickers.assert_called_once()

    @patch('trader.tasks.WatchedTicker')
    @patch('trader.tasks.settings')
    def test_collect_watch_quotes_disabled(self, mock_settings, _mock_watched):
        mock_settings.TRADER_WATCH_ENABLED = False
        mock_settings.TRADER_WATCH_TICKERS = ['PETR4']
        from trader.tasks import collect_watch_quotes

        out = collect_watch_quotes()
        self.assertFalse(out['enabled'])


class TabularPayloadTests(SimpleTestCase):
    def test_list_of_dicts(self):
        r = tabular_from_api_payload([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
        self.assertEqual(r['keys'], ['a', 'b'])
        self.assertEqual(r['key_labels'], ['A', 'B'])
        self.assertEqual(len(r['rows']), 2)

    def test_empty_list(self):
        r = tabular_from_api_payload([])
        self.assertTrue(r.get('empty'))

    def test_items_wrapped(self):
        r = tabular_from_api_payload({'items': [{'x': 1}]})
        self.assertEqual(r['keys'], ['x'])
        self.assertEqual(r['key_labels'], ['X'])
