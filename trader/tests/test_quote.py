from unittest.mock import MagicMock, patch

from django.contrib.auth.models import User
from django.test import Client, TestCase, override_settings


@override_settings(
    CACHES={
        'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'}
    }
)
class FetchQuoteTests(TestCase):
    def setUp(self):
        super().setUp()
        self._rl = patch('trader.services.marketdata._respect_rate_limit')
        self._rlq = patch('trader.services.marketdata._respect_quote_rate_limit')
        self._rl.start()
        self._rlq.start()

    def tearDown(self):
        self._rl.stop()
        self._rlq.stop()
        from django.core.cache import cache

        cache.clear()
        super().tearDown()

    @patch('trader.services.marketdata.requests.get')
    def test_fetch_quote_parses_json(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {'lastPrice': 1.23})

        from trader.services.quote import fetch_quote

        data = fetch_quote('PETR4', use_cache=False)
        self.assertEqual(data['lastPrice'], 1.23)

    def test_ohlc_payload(self):
        from trader.services.marketdata import ohlc_bar_chart_payload

        p = ohlc_bar_chart_payload(
            {'open': 10, 'high': 12, 'low': 9, 'close': None, 'lastPrice': 11}
        )
        self.assertIsNotNone(p)
        self.assertEqual(len(p['values']), 4)
        self.assertEqual(p['values'][-1], 11.0)

    @patch('trader.services.marketdata.requests.get')
    def test_fetch_aggregate_book_parses_json(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {'bids': [{'price': 100, 'quantity': 5}], 'asks': []},
        )
        from trader.services.marketdata import fetch_aggregate_book

        data = fetch_aggregate_book('WINJ26', use_cache=False)
        self.assertEqual(len(data['bids']), 1)
        self.assertEqual(data['bids'][0]['price'], 100)


class MarketQuoteViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('viewer', password='pw-test-99')
        self.client = Client()
        self._rl = patch('trader.services.marketdata._respect_rate_limit')
        self._rlq = patch('trader.services.marketdata._respect_quote_rate_limit')
        self._rl.start()
        self._rlq.start()

    def tearDown(self):
        self._rl.stop()
        self._rlq.stop()
        super().tearDown()

    @patch('trader.panel_context.fetch_aggregate_book')
    @patch('trader.panel_context.fetch_book')
    @patch('trader.panel_context.fetch_ticker_details')
    @patch('trader.panel_context.fetch_quote')
    def test_mercado_renders_anonymous(self, mock_q, mock_d, mock_b, mock_agg):
        mock_q.return_value = {
            'lastPrice': 30.5,
            'ticker': 'VALE3',
            'open': 30,
            'high': 31,
            'low': 29,
            'close': 30.5,
        }
        mock_d.return_value = {'ticker': 'VALE3', 'asset': 'Vale', 'type': 'Stock'}
        mock_b.return_value = {'bids': [{'position': 1, 'price': 30.4, 'quantity': 100, 'broker': '1'}], 'asks': []}
        mock_agg.return_value = {'bids': [{'price': 30.4, 'quantity': 200}], 'asks': []}
        self.client.force_login(self.user)
        r = self.client.get('/mercado/', {'ticker': 'VALE3'})
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, 'VALE3')
        self.assertContains(r, '30.5')
        self.assertContains(r, 'Detalhes')
        self.assertContains(r, 'Aggregate Book')
