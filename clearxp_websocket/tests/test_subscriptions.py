from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from clearxp_websocket import config
from clearxp_websocket.exceptions import WebSocketNotConnectedError
from clearxp_websocket.subscriptions import (
    initialize_websocket,
    sign_ticker,
    unsign_orders_status,
    unsign_ticker,
)


class InitializeWebsocketTests(SimpleTestCase):
    def tearDown(self):
        config.set_ws_base_url_override(None)
        super().tearDown()

    def test_sets_base_url_override(self):
        initialize_websocket('wss://example.test/ws/v1', version=1)
        self.assertEqual(config.ws_base_url(), 'wss://example.test/ws/v1')


class SignTickerTests(SimpleTestCase):
    @patch('clearxp_websocket.subscriptions.connect_websocket')
    def test_sign_ticker_market_and_orders(self, mock_connect):
        sign_ticker('PETR4', None, lambda t, m: None, lambda t, m: None)
        self.assertEqual(mock_connect.call_count, 2)
        routes = [mock_connect.call_args_list[i][0][2] for i in range(2)]
        self.assertIn('marketdata', routes)
        self.assertIn('orders', routes)

    @patch('clearxp_websocket.subscriptions.connect_websocket')
    def test_sign_ticker_orders_only(self, mock_connect):
        sign_ticker('PETR4', [], None, lambda t, m: None)
        mock_connect.assert_called_once()
        self.assertEqual(mock_connect.call_args[0][2], 'orders')

    def test_invalid_subscription_raises(self):
        with self.assertRaises(ValueError):
            sign_ticker('PETR4', ['SubscribeInvalid'], lambda t, m: None, None)


class UnsignTickerTests(SimpleTestCase):
    @patch('clearxp_websocket.subscriptions.send_message_to_websocket')
    @patch('clearxp_websocket.subscriptions.is_websocket_connected', return_value=True)
    def test_unsign_all_sends_three(self, _mock_connected, mock_send):
        unsign_ticker('VALE3', None)
        self.assertEqual(mock_send.call_count, 3)

    @patch('clearxp_websocket.subscriptions.is_websocket_connected', return_value=False)
    def test_unsign_raises_when_offline(self, _mock):
        with self.assertRaises(WebSocketNotConnectedError):
            unsign_ticker('VALE3')

    @patch('clearxp_websocket.subscriptions.send_message_to_websocket')
    @patch('clearxp_websocket.subscriptions.is_websocket_connected', return_value=True)
    def test_unsign_specific(self, _mock_conn, mock_send):
        unsign_ticker('PETR4', ['UnsubscribeQuote'])
        mock_send.assert_called_once()


class UnsignOrdersTests(SimpleTestCase):
    @patch('clearxp_websocket.subscriptions.send_message_to_websocket')
    @patch('clearxp_websocket.subscriptions.is_websocket_connected', return_value=True)
    def test_unsign_orders_status(self, _mock_c, mock_send):
        unsign_orders_status()
        mock_send.assert_called_once()

    @patch('clearxp_websocket.subscriptions.is_websocket_connected', return_value=False)
    def test_unsign_orders_raises(self, _mock):
        with self.assertRaises(WebSocketNotConnectedError):
            unsign_orders_status()
