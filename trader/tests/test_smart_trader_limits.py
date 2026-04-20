from django.test import SimpleTestCase

from trader.smart_trader_limits import (
    DAILY_ORDER_LIMIT_BOV,
    OPERATION_LIMIT_RULES,
    daily_order_limit_for_ticker,
    extract_bmf_base,
    ticket_limit_for_ticker,
)


class SmartTraderLimitsTests(SimpleTestCase):
    def test_bmf_bases(self):
        self.assertEqual(extract_bmf_base('WINJ26'), 'WIN')
        self.assertEqual(extract_bmf_base('wdoapr25'), 'WDO')
        self.assertEqual(extract_bmf_base('PETR4'), None)

    def test_daily_limits(self):
        self.assertEqual(daily_order_limit_for_ticker('WINJ26'), 10)
        self.assertEqual(daily_order_limit_for_ticker('DOLF25'), 4)
        self.assertEqual(daily_order_limit_for_ticker('VALE3'), DAILY_ORDER_LIMIT_BOV)

    def test_ticket_limits(self):
        self.assertEqual(ticket_limit_for_ticker('INDZ24'), 5)
        self.assertEqual(ticket_limit_for_ticker('ITUB4'), 500)

    def test_operation_matrix(self):
        self.assertEqual(OPERATION_LIMIT_RULES['send_order'], (True, True))
        self.assertEqual(OPERATION_LIMIT_RULES['cancel_order'], (False, False))
        self.assertEqual(OPERATION_LIMIT_RULES['replace_order'], (False, True))
