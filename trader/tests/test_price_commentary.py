from django.test import SimpleTestCase

from trader.automacoes.price_commentary import build_intraday_price_commentary


class PriceCommentaryTests(SimpleTestCase):
    def test_build_returns_paragraph(self):
        candles = []
        p = 10.0
        for i in range(12):
            p += 0.02 * (1 if i % 3 else -1)
            candles.append(
                {
                    'open': p,
                    'high': p + 0.03,
                    'low': p - 0.03,
                    'close': p + 0.01,
                    'volume': 100.0,
                }
            )
        out = build_intraday_price_commentary(candles)
        self.assertIsNotNone(out)
        assert out is not None
        self.assertIn('Estatística', out)
        self.assertIn('heurística', out.lower())
