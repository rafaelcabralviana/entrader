"""Stops ancorados em bins de menor volume (lado protetor)."""

from django.test import SimpleTestCase

from trader.automacoes.bracket_volume_levels import protective_lvn_stop_mid


class ProtectiveLvnTests(SimpleTestCase):
    def test_buy_picks_lowest_volume_below_min_distance(self):
        edges = [0.0, 10.0, 20.0, 30.0, 40.0]
        vols = [100.0, 50.0, 10.0, 80.0]
        last = 35.0
        out = protective_lvn_stop_mid(edges, vols, last=last, side='buy', min_distance=5.0)
        self.assertIsNotNone(out)
        self.assertAlmostEqual(out, 25.0, places=6)

    def test_sell_protective_side(self):
        edges = [0.0, 10.0, 20.0, 30.0, 40.0]
        vols = [10.0, 200.0, 5.0, 200.0]
        last = 8.0
        out = protective_lvn_stop_mid(edges, vols, last=last, side='sell', min_distance=3.0)
        self.assertIsNotNone(out)
        self.assertAlmostEqual(out, 25.0, places=6)
