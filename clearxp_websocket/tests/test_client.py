import json
import time
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from clearxp_websocket.exceptions import WebSocketNotConnectedError, WebSocketSendError
from clearxp_websocket.protocol import (
    RECORD_SEPARATOR,
    WebSocketRequestMessage,
    subscribe_aggregate_book,
    subscribe_quote,
)
from clearxp_websocket.services import client as client_mod


class WebSocketProtocolTests(SimpleTestCase):
    def test_request_message_to_dict(self):
        msg = WebSocketRequestMessage(
            arguments=['PETR4'],
            target='SubscribeQuote',
            msg_type=1,
        )
        self.assertEqual(
            msg.to_dict(),
            {'arguments': ['PETR4'], 'target': 'SubscribeQuote', 'type': 1},
        )

    def test_subscribe_quote_factory(self):
        msg = subscribe_quote('XPBR31')
        self.assertEqual(
            msg.to_dict(),
            {'arguments': ['XPBR31'], 'target': 'SubscribeQuote', 'type': 1},
        )

    def test_subscribe_aggregate_book_matches_doc(self):
        msg = subscribe_aggregate_book('XPBR31')
        self.assertEqual(
            msg.to_dict(),
            {
                'arguments': ['XPBR31'],
                'target': 'SubscribeAggregateBook',
                'type': 1,
            },
        )


class WebSocketClientTests(SimpleTestCase):
    def tearDown(self):
        with client_mod._connections_lock:
            client_mod._ws_connections.clear()
        super().tearDown()

    @patch('clearxp_websocket.services.client.websocket.WebSocketApp')
    @patch('clearxp_websocket.services.client.get_access_token', return_value='fake-token')
    @patch('clearxp_websocket.services.client.get_user_agent', return_value='Test-UA/1')
    def test_connect_websocket_starts_thread_and_registers(
        self, _mock_ua, _mock_token, mock_ws_app_cls
    ):
        fake_ws = MagicMock()
        mock_ws_app_cls.return_value = fake_ws

        def on_msg(s: str) -> None:
            pass

        def on_open_cb() -> None:
            pass

        client_mod.connect_websocket(on_msg, on_open_cb, 'marketdata')
        time.sleep(0.05)

        mock_ws_app_cls.assert_called_once()
        call_kw = mock_ws_app_cls.call_args[1]
        self.assertIn('on_message', call_kw)
        self.assertIn('on_open', call_kw)
        fake_ws.run_forever.assert_called_once()
        with client_mod._connections_lock:
            self.assertIs(client_mod._ws_connections.get('marketdata'), fake_ws)

    def test_send_not_connected_raises(self):
        with self.assertRaises(WebSocketNotConnectedError):
            client_mod.send_message_to_websocket('marketdata', {'x': 1})

    def test_send_appends_record_separator(self):
        ws = MagicMock()
        ws.sock = MagicMock()
        ws.sock.connected = True
        with client_mod._connections_lock:
            client_mod._ws_connections['marketdata'] = ws

        client_mod.send_message_to_websocket('marketdata', {'a': 1})
        ws.send.assert_called_once()
        (payload,), _ = ws.send.call_args
        self.assertTrue(payload.endswith(RECORD_SEPARATOR))
        body = payload[: -len(RECORD_SEPARATOR)]
        self.assertEqual(json.loads(body), {'a': 1})

    def test_send_request_message_uses_to_json(self):
        ws = MagicMock()
        ws.sock = MagicMock()
        ws.sock.connected = True
        with client_mod._connections_lock:
            client_mod._ws_connections['orders'] = ws

        msg = WebSocketRequestMessage([], 'SubscribeOrdersStatus', 1)
        client_mod.send_message_to_websocket('orders', msg)
        (payload,), _ = ws.send.call_args
        body = payload[: -len(RECORD_SEPARATOR)]
        self.assertEqual(
            json.loads(body),
            {'arguments': [], 'target': 'SubscribeOrdersStatus', 'type': 1},
        )

    def test_send_failure_raises_send_error(self):
        ws = MagicMock()
        ws.sock = MagicMock()
        ws.sock.connected = True
        ws.send.side_effect = OSError('boom')
        with client_mod._connections_lock:
            client_mod._ws_connections['marketdata'] = ws

        with self.assertRaises(WebSocketSendError):
            client_mod.send_message_to_websocket('marketdata', {})

    @patch('clearxp_websocket.services.client.websocket.WebSocketApp')
    def test_second_connect_skips_new_socket_when_already_connected(self, mock_cls):
        ws = MagicMock()
        ws.sock = MagicMock()
        ws.sock.connected = True
        with client_mod._connections_lock:
            client_mod._ws_connections['marketdata'] = ws
        opens = {'n': 0}

        def on_open():
            opens['n'] += 1

        client_mod.connect_websocket(lambda m: None, on_open, 'marketdata')
        mock_cls.assert_not_called()
        self.assertEqual(opens['n'], 1)

    def test_is_websocket_connected_respects_bool_true_only(self):
        ws = MagicMock()
        ws.sock = MagicMock()
        ws.sock.connected = True
        with client_mod._connections_lock:
            client_mod._ws_connections['marketdata'] = ws
        self.assertTrue(client_mod.is_websocket_connected('marketdata'))
        ws.sock.connected = False
        self.assertFalse(client_mod.is_websocket_connected('marketdata'))
