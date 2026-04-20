"""View de envio de ordem de teste."""

from unittest.mock import patch

from django.core.cache import cache
from django.contrib.auth.models import User
from django.test import Client, RequestFactory, TestCase

from api_auth.exceptions import SmartTraderConfigurationError


class SendOrderTestViewTests(TestCase):
    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user('t', password='p-test-88')
        self.client = Client()

    def test_redirects_when_anonymous(self):
        r = self.client.get('/envio-teste/')
        self.assertEqual(r.status_code, 302)
        self.assertIn('/entrar/', r.url or '')

    def test_get_200_when_logged_in(self):
        self.client.force_login(self.user)
        r = self.client.get('/envio-teste/')
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, 'Envio de ordem')

    @patch('trader.services.orders.post_send_market_order', return_value={'ok': True})
    @patch('trader.services.orders.post_simulator_setup_orders', return_value={})
    def test_post_sends_market(self, _setup, _send):
        self.client.force_login(self.user)
        r = self.client.post(
            '/envio-teste/',
            {
                'setup': 'filled',
                'order_type': 'market',
                'ticker': 'WINJ26',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
            },
        )
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, 'Resposta da API')
        _send.assert_called_once()

    @patch('trader.services.orders.post_send_limited_order')
    @patch('trader.services.orders.post_send_market_order', return_value={'ok': True})
    @patch('trader.services.orders.post_simulator_setup_orders', return_value={})
    def test_filled_forces_market_despite_limited_in_form(self, mock_setup, mock_mkt, mock_limited):
        """Com simulação filled/rejected a boleta envia a mercado (limitada + preço 1 ficava new)."""
        from trader.panel_context import run_order_test_form

        rf = RequestFactory()
        req = rf.post(
            '/envio-teste/',
            {
                'setup': 'filled',
                'order_type': 'limited',
                'price': '1',
                'ticker': 'WINJ26',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
            },
        )
        _defaults, _rj, err = run_order_test_form(req)
        self.assertIsNone(err)
        mock_setup.assert_called_once_with('filled')
        mock_mkt.assert_called_once()
        mock_limited.assert_not_called()


class RunOrderTestFormOpenLimitedTests(TestCase):
    """Preset open_limited: sem /setup/orders + limitada com preço fora do mercado."""

    def setUp(self):
        cache.clear()

    @patch('trader.services.orders.post_send_limited_order', return_value={'orderId': '1'})
    @patch('trader.services.orders.post_simulator_setup_orders')
    def test_open_limited_skips_setup_and_sends_limited(self, mock_setup, mock_limited):
        from trader.panel_context import run_order_test_form

        rf = RequestFactory()
        req = rf.post(
            '/envio-teste/',
            {
                'setup': 'open_limited',
                'order_type': 'market',
                'ticker': 'WINJ26',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
                'price': '',
            },
        )
        _defaults, rj, err = run_order_test_form(req)
        mock_setup.assert_not_called()
        mock_limited.assert_called_once()
        body = mock_limited.call_args[0][0]
        self.assertEqual(body['Price'], 1.0)
        self.assertIsNone(err)


class RunOrderTestFormMessageTests(TestCase):
    """Mensagem amigável quando RSA não está no .env."""

    def setUp(self):
        cache.clear()

    def test_configuration_error_returns_hint_not_stack(self):
        from trader.panel_context import run_order_test_form

        rf = RequestFactory()
        req = rf.post(
            '/envio-teste/',
            {
                'setup': 'filled',
                'order_type': 'market',
                'ticker': 'WINJ26',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
            },
        )

        with (
            patch(
                'trader.services.orders.post_send_market_order',
                side_effect=SmartTraderConfigurationError('no pem'),
            ),
            patch('trader.services.orders.post_simulator_setup_orders', return_value={}),
        ):
            _defaults, _rj, err = run_order_test_form(req)

        self.assertIsNotNone(err)
        self.assertIn('SMART_TRADER_PRIVATE_RSA', err)
        self.assertIn('BODY_SIGNATURE', err)


class RunOrderTestFormTickerNotAllowedCacheTests(TestCase):
    def setUp(self):
        cache.clear()

    def test_second_try_is_blocked_after_ticker_not_allowed(self):
        from trader.panel_context import run_order_test_form

        rf = RequestFactory()
        req = rf.post(
            '/envio-teste/',
            {
                'setup': 'filled',
                'order_type': 'market',
                'ticker': 'WINJ26',
                'side': 'Buy',
                'quantity': '1',
                'tif': 'Day',
            },
        )

        with (
            patch('trader.services.orders.post_simulator_setup_orders', return_value={}),
            patch(
                'trader.services.orders.post_send_market_order',
                side_effect=ValueError(
                    'A API de ordens retornou status 403. '
                    'TICKER_NOT_ALLOWED: O ticker não é permitido para este módulo.'
                ),
            ) as mock_send,
        ):
            _defaults, _rj, err1 = run_order_test_form(req)
            self.assertIsNotNone(err1)
            self.assertIn('TICKER_NOT_ALLOWED', err1)
            self.assertEqual(mock_send.call_count, 1)

            _defaults, _rj, err2 = run_order_test_form(req)
            self.assertIsNotNone(err2)
            self.assertIn('bloqueado temporariamente', err2)
            self.assertEqual(mock_send.call_count, 1)
