"""Helpers e registro local após envio de ordem (histórico interno)."""

from decimal import Decimal

from django.test import TestCase

from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)


class LocalHistoryHelpersTests(TestCase):
    def test_market_always_records(self):
        self.assertTrue(should_record_local_history('market', {}))
        self.assertTrue(should_record_local_history('market', {'ok': True}))

    def test_limited_open_order_skips(self):
        self.assertFalse(should_record_local_history('limited', {'orderId': '1'}))

    def test_limited_filled_records(self):
        self.assertTrue(
            should_record_local_history('limited', {'orderStatus': 'Filled'})
        )
        self.assertTrue(
            should_record_local_history('limited', {'executedQuantity': 1})
        )

    def test_infer_price_body_then_resp(self):
        self.assertEqual(
            infer_execution_price({'Price': 10.5}, {}),
            Decimal('10.5'),
        )
        self.assertEqual(
            infer_execution_price(
                {},
                {'averagePrice': 123.45},
            ),
            Decimal('123.45'),
        )


class RegisterTradeExecutionChainTests(TestCase):
    def test_buy_then_sell_closes_with_closed_operation(self):
        register_trade_execution(
            ticker='TEST9',
            side='Buy',
            quantity=10,
            price=Decimal('100'),
            source='t',
        )
        register_trade_execution(
            ticker='TEST9',
            side='Sell',
            quantity=10,
            price=Decimal('101'),
            source='t',
        )
        from trader.models import ClosedOperation, Position

        self.assertEqual(Position.objects.filter(ticker='TEST9').count(), 1)
        p = Position.objects.get(ticker='TEST9')
        self.assertFalse(p.is_active)
        self.assertTrue(ClosedOperation.objects.filter(position=p).exists())
