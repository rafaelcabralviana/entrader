from datetime import datetime
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase

from trader.automacoes.strategies_plugins.perfil_volume_montanhas import (
    _fmt_mountains_line,
    evaluate,
)
from trader.trading_system.contracts.context import ObservationContext

_BRT = ZoneInfo('America/Sao_Paulo')


class PerfilVolumeMontanhasFmtTests(SimpleTestCase):
    def test_fmt_mountains_line(self):
        s = _fmt_mountains_line([(203.125, 1500.4), (201.5, 9000.0)])
        self.assertIn('Vol. Montanhas:', s)
        self.assertIn('(203.125 - 1500)', s)
        self.assertIn('(201.500 - 9000)', s)


class PerfilVolumeMontanhasEvaluateTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('vp_m_u1', 'vp_m_u1@test.com', 'x' * 12)
        cache.clear()

    def _candles(self, n: int):
        out: list[dict] = []
        base = 200.0
        for i in range(n):
            p = base + (i % 7) * 0.02
            out.append(
                {
                    'open': p,
                    'high': p + 0.15,
                    'low': p - 0.15,
                    'close': p + 0.01,
                    'volume': 100.0 + float(i % 5) * 20.0,
                }
            )
        return out

    def test_evaluate_returns_message_with_vol_montanhas(self):
        candles = self._candles(40)
        ctx = ObservationContext(
            mode='live',
            ticker='TEST',
            trading_environment='simulator',
            captured_at=datetime.now(tz=_BRT),
            data_source='live_tail',
            session_date_iso='2026-04-19',
            extra={'candles': candles},
        )
        msg = evaluate(ctx, self.user)
        self.assertIsInstance(msg, str)
        assert msg is not None
        self.assertIn('perfil_volume_montanhas', msg)
        self.assertIn('Vol. Montanhas:', msg)
        self.assertIn(' - ', msg)

    def test_evaluate_throttle_second_call_none(self):
        candles = self._candles(40)
        ctx = ObservationContext(
            mode='live',
            ticker='TEST',
            trading_environment='simulator',
            captured_at=datetime.now(tz=_BRT),
            data_source='live_tail',
            session_date_iso='2026-04-19',
            extra={'candles': candles},
        )
        m1 = evaluate(ctx, self.user)
        m2 = evaluate(ctx, self.user)
        self.assertIsNotNone(m1)
        self.assertIsNone(m2)
