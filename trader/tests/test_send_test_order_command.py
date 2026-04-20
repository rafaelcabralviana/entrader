"""Comando send_test_order (dry-run e fluxo com mocks)."""

from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import SimpleTestCase


class SendTestOrderCommandTests(SimpleTestCase):
    def test_dry_run_prints_json_no_http(self):
        out = StringIO()
        call_command(
            'send_test_order',
            '--dry-run',
            stdout=out,
        )
        text = out.getvalue()
        self.assertIn('Dry-run', text)
        self.assertIn('send/limited', text)
        self.assertIn('WINJ26', text)

    def test_dry_run_no_setup_skips_setup_line(self):
        out = StringIO()
        call_command(
            'send_test_order',
            '--dry-run',
            '--no-setup',
            stdout=out,
        )
        self.assertNotIn('/v1/setup/orders', out.getvalue())

    @patch('trader.management.commands.send_test_order.post_send_limited_order')
    @patch('trader.management.commands.send_test_order.post_simulator_setup_orders')
    def test_default_open_limited_skips_setup_sends_limited(self, mock_setup, mock_send):
        mock_send.return_value = {'orderId': 'x'}
        out = StringIO()
        call_command('send_test_order', stdout=out)
        mock_setup.assert_not_called()
        mock_send.assert_called_once()
        body = mock_send.call_args[0][0]
        self.assertEqual(body['Module'], 'DayTrade')
        self.assertEqual(body['Ticker'], 'WINJ26')
        self.assertEqual(body['Price'], 1.0)
