from __future__ import annotations

from types import SimpleNamespace

from django.core.cache import cache
from django.test import SimpleTestCase

from trader.automacoes.strategies_plugins.teste_limite_preco_34 import evaluate
from trader.trading_system.contracts.context import ObservationContext


def _ctx(
    *,
    quote: dict | None = None,
    candles: list | None = None,
    last: float | None = None,
) -> ObservationContext:
    q = dict(quote) if quote is not None else {}
    if last is not None:
        q['lastPrice'] = last
    extra: dict = {}
    if candles is not None:
        extra['candles'] = candles
    return ObservationContext(
        mode='live',
        ticker='TEST',
        trading_environment='simulator',
        captured_at=None,
        quote=q,
        data_source='live_tail',
        extra=extra,
    )


class TesteLimitePreco34Tests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        cache.clear()

    def tearDown(self):
        cache.clear()
        super().tearDown()

    def test_sem_mensagem_abaixo_do_limite(self):
        user = SimpleNamespace(id=901)
        self.assertIsNone(evaluate(_ctx(last=33.99), user))
        self.assertIsNone(evaluate(_ctx(last=34.0), user))
        self.assertIsNone(evaluate(_ctx(last=34.10), user))
        self.assertIsNone(evaluate(_ctx(quote={}, candles=[]), user))

    def test_mensagem_quando_atinge_34_11_exato(self):
        user = SimpleNamespace(id=902)
        msg = evaluate(_ctx(last=34.11), user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('teste_limite_preco_34', msg)
        self.assertIn('34.1100', msg)
        self.assertIn('cotação', msg)
        self.assertIn('atingiu ou ultrapassou 34.11', msg)

    def test_mensagem_quando_ultrapassa_34_11_pela_cotacao(self):
        user = SimpleNamespace(id=905)
        msg = evaluate(_ctx(last=34.12), user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('34.1200', msg)
        self.assertIn('atingiu ou ultrapassou 34.11', msg)

    def test_prefere_fecho_vela_como_no_grafico_compacto(self):
        """Cotação bruta baixa mas fecho da última vela (o «Último» no ecrã) acima do limiar."""
        user = SimpleNamespace(id=906)
        candles = [
            {'open': 33.0, 'high': 33.5, 'low': 32.9, 'close': 33.1, 'volume': 1.0},
            {'open': 33.1, 'high': 34.2, 'low': 33.0, 'close': 34.15, 'volume': 1.0},
        ]
        msg = evaluate(_ctx(last=33.0, candles=candles), user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('velas', msg)
        self.assertIn('34.1500', msg)

    def test_cotacao_string_com_virgula_decimal(self):
        user = SimpleNamespace(id=907)
        msg = evaluate(_ctx(quote={'lastPrice': '34,15'}, last=None), user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('34.1500', msg)

    def test_fallback_ultimo_fecho_velas_sem_cotacao(self):
        user = SimpleNamespace(id=903)
        candles = [
            {'open': 30.0, 'high': 30.5, 'low': 29.9, 'close': 30.1, 'volume': 1.0},
            {'open': 30.1, 'high': 35.0, 'low': 30.0, 'close': 34.5, 'volume': 1.0},
        ]
        msg = evaluate(_ctx(quote={}, candles=candles), user)
        self.assertIsNotNone(msg)
        assert msg is not None
        self.assertIn('velas', msg)
        self.assertIn('34.5000', msg)

    def test_throttle_repete_mesmo_preco(self):
        user = SimpleNamespace(id=904)
        ctx = _ctx(last=40.0)
        m1 = evaluate(ctx, user)
        self.assertIsNotNone(m1)
        m2 = evaluate(ctx, user)
        self.assertIsNone(m2)

    def test_rearma_so_apos_voltar_abaixo_do_limite(self):
        user = SimpleNamespace(id=908)
        self.assertIsNotNone(evaluate(_ctx(last=34.20), user))
        # Permaneceu acima: não deve repetir.
        self.assertIsNone(evaluate(_ctx(last=34.30), user))
        self.assertIsNone(evaluate(_ctx(last=34.50), user))
        # Caiu abaixo: rearma gatilho.
        self.assertIsNone(evaluate(_ctx(last=34.00), user))
        # Novo cruzamento acima: dispara novamente.
        self.assertIsNotNone(evaluate(_ctx(last=34.12), user))

    def test_candles_dispara_so_no_cruzamento(self):
        user = SimpleNamespace(id=909)
        c1 = [
            {
                'open': 34.00,
                'high': 34.10,
                'low': 33.99,
                'close': 34.10,
                'volume': 1.0,
                'bucket_start': '2026-04-17T09:00:00-03:00',
            },
            {
                'open': 34.10,
                'high': 34.12,
                'low': 34.08,
                'close': 34.11,
                'volume': 1.0,
                'bucket_start': '2026-04-17T09:00:10-03:00',
            },
        ]
        self.assertIsNotNone(evaluate(_ctx(candles=c1), user))
        c2 = [
            {
                'open': 34.10,
                'high': 34.12,
                'low': 34.08,
                'close': 34.10,
                'volume': 1.0,
                'bucket_start': '2026-04-17T09:00:00-03:00',
            },
            {
                'open': 34.11,
                'high': 34.20,
                'low': 34.10,
                'close': 34.18,
                'volume': 1.0,
                'bucket_start': '2026-04-17T09:00:10-03:00',
            },
        ]
        self.assertIsNone(evaluate(_ctx(candles=c2), user))
