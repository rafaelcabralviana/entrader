"""Multiplicadores globais de bracket (SL/TP)."""

from django.test import SimpleTestCase

from trader.automacoes.bracket_width import apply_bracket_distance_multipliers


class BracketWidthTests(SimpleTestCase):
    def test_buy_widens_sl_down_and_tp_up(self):
        last = 100.0
        sl, tp = apply_bracket_distance_multipliers('Buy', last, 98.0, 102.0)
        self.assertLess(sl, 98.0)
        self.assertGreater(tp, 102.0)

    def test_sell_widens_sl_up_and_tp_down(self):
        last = 100.0
        sl, tp = apply_bracket_distance_multipliers('Sell', last, 102.0, 98.0)
        self.assertGreater(sl, 102.0)
        self.assertLess(tp, 98.0)
