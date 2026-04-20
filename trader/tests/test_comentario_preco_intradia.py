from __future__ import annotations

from types import SimpleNamespace

from django.core.cache import cache
from django.test import SimpleTestCase

from trader.automacoes.strategies_plugins.comentario_preco_intradia import (
    _pct_gain_vs_first_bar,
    _pct_gain_vs_session_low,
    evaluate,
)
from trader.trading_system.contracts.context import ObservationContext


def _candles_series(first_open: float, last_close: float, n: int = 6) -> list[dict]:
    """Série monótona crescente da primeira abertura até o último fecho desejado."""
    if n < 5:
        n = 5
    out: list[dict] = []
    for i in range(n - 1):
        t = i / max(1, n - 2)
        o = first_open + (last_close - first_open) * t * 0.85
        c = o + 0.08
        out.append(
            {
                'open': round(o, 4),
                'high': round(c + 0.15, 4),
                'low': round(o - 0.05, 4),
                'close': round(c, 4),
                'volume': 100.0,
            }
        )
    o_last = first_open + (last_close - first_open) * 0.92
    out.append(
        {
            'open': round(o_last, 4),
            'high': round(last_close + 0.2, 4),
            'low': round(o_last - 0.05, 4),
            'close': round(last_close, 4),
            'volume': 120.0,
        }
    )
    return out


class ComentarioPrecoUp3Tests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        cache.clear()

    def tearDown(self):
        cache.clear()
        super().tearDown()

    def test_pct_gain_vs_session_low(self):
        candles = [
            {'open': 34.0, 'high': 34.1, 'low': 33.0, 'close': 34.05, 'volume': 10.0},
            {'open': 34.05, 'high': 34.2, 'low': 33.95, 'close': 34.1, 'volume': 10.0},
            {'open': 34.1, 'high': 34.55, 'low': 34.05, 'close': 34.48, 'volume': 10.0},
            {'open': 34.48, 'high': 34.5, 'low': 34.4, 'close': 34.45, 'volume': 10.0},
            {'open': 34.45, 'high': 34.52, 'low': 34.42, 'close': 34.5, 'volume': 10.0},
        ]
        pl = _pct_gain_vs_session_low(candles)
        self.assertIsNotNone(pl)
        assert pl is not None
        self.assertGreater(pl, 4.4)

    def test_pct_gain_vs_first_bar_none_and_value(self):
        self.assertIsNone(_pct_gain_vs_first_bar([]))
        self.assertIsNone(_pct_gain_vs_first_bar('x'))  # type: ignore[arg-type]
        pct = _pct_gain_vs_first_bar(
            [
                {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.2, 'volume': 1.0},
                {'open': 100.2, 'high': 105.0, 'low': 100.0, 'close': 103.5, 'volume': 1.0},
            ]
        )
        self.assertIsNotNone(pct)
        assert pct is not None
        self.assertAlmostEqual(pct, 3.5, places=5)

    def test_evaluate_includes_up3_alert_from_session_low(self):
        user = SimpleNamespace(id=44)
        candles = [
            {'open': 34.0, 'high': 34.1, 'low': 33.0, 'close': 34.05, 'volume': 10.0},
            {'open': 34.05, 'high': 34.2, 'low': 33.95, 'close': 34.08, 'volume': 10.0},
            {'open': 34.08, 'high': 34.25, 'low': 33.98, 'close': 34.1, 'volume': 10.0},
            {'open': 34.1, 'high': 34.3, 'low': 34.0, 'close': 34.12, 'volume': 10.0},
            {'open': 34.12, 'high': 34.35, 'low': 34.05, 'close': 34.2, 'volume': 10.0},
            {'open': 34.2, 'high': 34.55, 'low': 34.15, 'close': 34.48, 'volume': 10.0},
        ]
        ctx = ObservationContext(
            mode='session_day',
            ticker='PETR4',
            trading_environment='simulator',
            captured_at=None,
            session_date_iso='2026-04-17',
            data_source='session_replay',
            extra={'candles': candles},
        )
        msg = evaluate(ctx, user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('[Alta ≥3%]', msg)
        self.assertIn('mínima', msg)

    def test_evaluate_includes_up3_alert(self):
        user = SimpleNamespace(id=42)
        candles = _candles_series(100.0, 103.6, n=8)
        self.assertGreaterEqual(_pct_gain_vs_first_bar(candles) or 0, 3.0)
        ctx = ObservationContext(
            mode='session_day',
            ticker='PETR4',
            trading_environment='simulator',
            captured_at=None,
            session_date_iso='2026-04-17',
            data_source='session_replay',
            extra={'candles': candles},
        )
        msg = evaluate(ctx, user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('[Alta ≥3%]', msg)
        self.assertIn('3.', msg)
        self.assertIn('primeira barra', msg)

    def test_evaluate_no_up3_alert_below_threshold(self):
        user = SimpleNamespace(id=43)
        candles = _candles_series(100.0, 102.5, n=8)
        self.assertLess(_pct_gain_vs_first_bar(candles) or 999, 3.0)
        ctx = ObservationContext(
            mode='session_day',
            ticker='PETR4',
            trading_environment='simulator',
            captured_at=None,
            session_date_iso='2026-04-17',
            data_source='session_replay',
            extra={'candles': candles},
        )
        msg = evaluate(ctx, user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertNotIn('[Alta ≥3%]', msg)
