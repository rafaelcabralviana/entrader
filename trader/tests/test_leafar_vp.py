from django.test import SimpleTestCase

from trader.automacoes.leafar_vp import (
    LeafarSignal,
    compute_volume_profile,
    detect_leafar_signal,
    volume_profile_mountains,
)


class LeafarVpTests(SimpleTestCase):
    def test_compute_volume_profile_basic(self):
        candles = [
            {'open': 10, 'high': 11, 'low': 9, 'close': 10, 'volume': 100},
            {'open': 10, 'high': 10.5, 'low': 9.5, 'close': 10, 'volume': 50},
        ]
        vp = compute_volume_profile(candles, num_bins=8)
        self.assertIsNotNone(vp)
        edges, vols = vp
        self.assertEqual(len(vols), 8)
        self.assertEqual(len(edges), 9)
        self.assertGreater(sum(vols), 0)

    def test_detect_long_mean_reversion(self):
        candles: list[dict] = []
        base = 100.0
        for _ in range(38):
            p = base + 0.005
            candles.append(
                {
                    'open': p,
                    'high': p + 0.004,
                    'low': p - 0.004,
                    'close': p,
                    'volume': 8000.0,
                }
            )
        p = base
        for i in range(14):
            p = base - 0.35 * (i + 1)
            candles.append(
                {
                    'open': p + 0.01,
                    'high': p + 0.02,
                    'low': p - 0.03,
                    'close': p - 0.02,
                    'volume': 5.0,
                }
            )
        sig = detect_leafar_signal(
            candles,
            num_bins=24,
            min_candles=20,
            low_corridor_ratio=0.35,
        )
        self.assertIsInstance(sig, LeafarSignal)
        assert sig is not None
        self.assertEqual(sig.side, 'Buy')
        self.assertGreater(sig.poc, sig.last)
        self.assertLess(sig.stop_loss, sig.last)

    def test_volume_profile_mountains_three_hills(self):
        """Três máximos locais separados — não confundir com só os 3 maiores bins globais."""
        v = [1.0, 3.0, 1.0, 8.0, 1.0, 4.0, 1.0]
        edges = [float(i) for i in range(len(v) + 1)]
        m = volume_profile_mountains(
            edges,
            v,
            max_mountains=3,
            min_relative_peak=0.05,
            min_bin_separation=1,
        )
        self.assertEqual(len(m), 3)
        prices = [p for p, _ in m]
        self.assertGreater(prices[0], prices[1])
        self.assertGreater(prices[1], prices[2])
        by_price = {round(p, 6): round(vol, 6) for p, vol in m}
        self.assertEqual(by_price[3.5], 8.0)
        self.assertEqual(by_price[1.5], 3.0)
        self.assertEqual(by_price[5.5], 4.0)

    def test_volume_profile_mountains_subpeak_near_main(self):
        """Dois máximos locais próximos: separação mínima em bins mantém o de maior volume."""
        v = [1.0, 11.0, 9.0, 10.0, 1.0, 1.0]
        edges = [float(i) for i in range(len(v) + 1)]
        m = volume_profile_mountains(
            edges,
            v,
            max_mountains=3,
            min_relative_peak=0.05,
            min_bin_separation=3,
        )
        self.assertEqual(len(m), 1)
        self.assertAlmostEqual(m[0][0], 1.5)
        self.assertAlmostEqual(m[0][1], 11.0)
