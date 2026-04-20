"""Testes do enriquecimento de custódia com cotação e P&amp;L."""

from unittest.mock import patch

from django.test import SimpleTestCase

from trader.custody_enrichment import (
    enrich_custody_payload,
    mark_price_from_quote,
    prepare_custody_payload,
)


class CustodyEnrichmentTests(SimpleTestCase):
    def test_mark_price_from_quote(self):
        self.assertEqual(mark_price_from_quote({'lastPrice': 1.5}), 1.5)
        self.assertEqual(mark_price_from_quote({'LastPrice': '2'}), 2.0)
        self.assertIsNone(mark_price_from_quote({}))

    @patch('trader.custody_enrichment.fetch_quote')
    def test_pnl_long_positive(self, mock_quote):
        mock_quote.return_value = {'lastPrice': 11.0, 'status': 'Trading'}
        rows = [{'ticker': 'PETR4', 'availableQuantity': 100, 'averagePrice': 10.0}]
        enriched, meta = enrich_custody_payload(rows)
        self.assertEqual(meta['row_pnl_classes'], ['pos'])
        self.assertIn('100,00', enriched[0]['pnlBrl'])  # (11-10)*100
        self.assertEqual(enriched[0]['sessionStatus'], 'Em pregão')

    @patch('trader.custody_enrichment.fetch_quote')
    def test_pnl_short_negative(self, mock_quote):
        mock_quote.return_value = {'lastPrice': 11.0, 'status': 'Trading'}
        rows = [{'ticker': 'PETR4', 'availableQuantity': -100, 'averagePrice': 10.0}]
        enriched, meta = enrich_custody_payload(rows)
        self.assertEqual(meta['row_pnl_classes'], ['neg'])
        self.assertIn('100,00', enriched[0]['pnlBrl'])

    @patch('trader.custody_enrichment.fetch_quote')
    def test_same_ticker_cached_once(self, mock_quote):
        mock_quote.return_value = {'lastPrice': 5.0, 'status': 'Trading'}
        rows = [
            {'ticker': 'X', 'availableQuantity': 1, 'averagePrice': 4.0},
            {'ticker': 'X', 'availableQuantity': 2, 'averagePrice': 4.0},
        ]
        enrich_custody_payload(rows)
        self.assertEqual(mock_quote.call_count, 1)

    def test_prepare_wrapped_dict(self):
        payload = {'items': [{'ticker': 'AB', 'availableQuantity': 1, 'averagePrice': 10.0}]}
        with patch('trader.custody_enrichment.fetch_quote') as mq:
            mq.return_value = {'lastPrice': 12.0, 'status': 'Trading'}
            out, meta = prepare_custody_payload(payload)
        self.assertIn('items', out)
        self.assertTrue(meta.get('row_pnl_classes'))
