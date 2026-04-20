"""
Enriquecimento da custódia com cotação (GET /v1/marketdata/quote) e resultado estimado.

Resultado (R$) = (preço de mercado − preço médio) × quantidade (com sinal: compra +, venda −).
Resultado (%) = resultado / (|preço médio| × |quantidade|) × 100 quando o denominador > 0.

Uso típico day trade: posição e resultado são atualizados conforme custódia e cotação (REST).
"""

from __future__ import annotations

import logging
from typing import Any

from trader.services.marketdata import fetch_quote

logger = logging.getLogger(__name__)
_LAST_MARK_PRICE_BY_TICKER: dict[str, float] = {}


_TICKER_KEYS = ('ticker', 'Ticker', 'symbol', 'Symbol')
_QTY_KEYS = (
    'availableQuantity',
    'AvailableQuantity',
    'quantity',
    'Quantity',
    'positionQuantity',
    'PositionQuantity',
    'totalQuantity',
    'TotalQuantity',
)
_AVG_KEYS = (
    'averagePrice',
    'AveragePrice',
    'avgPrice',
    'AvgPrice',
    'averageCost',
    'AverageCost',
    'avgCost',
    'AvgCost',
    'costPrice',
    'CostPrice',
)

_QUOTE_PRICE_KEYS = (
    'lastPrice',
    'LastPrice',
    'close',
    'Close',
    'price',
    'Price',
)


def _row_get_ci(row: dict[str, Any], *candidates: str) -> Any:
    """Primeiro valor encontrado com chave case-insensitive."""
    if not row:
        return None
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for c in candidates:
        k = c.lower()
        if k in lower_map:
            return lower_map[k]
    return None


def _parse_float_any(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().replace(' ', '')
    if not s:
        return None
    # Aceita formatos:
    # - BR: 1.234,56
    # - EN: 1,234.56
    # - simples: 1234.56 / 1234,56
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    else:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_ticker(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip().upper()
    return s or None


def mark_price_from_quote(q: dict[str, Any]) -> float | None:
    """Último preço utilizável da cotação."""
    for k in _QUOTE_PRICE_KEYS:
        v = q.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _latest_local_quote_for_ticker(sym: str) -> dict[str, Any] | None:
    """Última cotação local (QuoteSnapshot) para fallback de preço."""
    try:
        from trader.models import QuoteSnapshot

        row = (
            QuoteSnapshot.objects.filter(ticker=sym)
            .order_by('-captured_at')
            .values('quote_data')
            .first()
        )
    except Exception:
        logger.warning('Fallback local quote falhou ticker=%s', sym)
        return None
    q = (row or {}).get('quote_data')
    return q if isinstance(q, dict) else None


def _session_label_pt(quote: dict[str, Any]) -> str:
    from trader.panel_context import quote_status_is_end_of_day

    if quote_status_is_end_of_day(quote):
        return 'Pós-pregão'
    st = quote.get('status') or quote.get('Status')
    if st:
        s = str(st).strip()
        if s.lower() in ('trading',):
            return 'Em pregão'
        return s
    return 'Em pregão'


def _format_brl_br(value: float) -> str:
    neg = value < 0
    x = abs(value)
    s = f'{x:,.2f}'
    left, right = s.split('.')
    left = left.replace(',', '.')
    return ('−' if neg else '') + f'{left},{right}'


def _format_pct(value: float) -> str:
    neg = value < 0
    x = abs(value)
    return ('−' if neg else '') + f'{x:.2f}%'


def enrich_custody_payload(payload: Any) -> tuple[Any, dict[str, Any]]:
    """
    Se ``payload`` for lista de dicts (custódia), acrescenta colunas de mercado e resultado.
    Caso contrário devolve ``(payload, {})``.
    """
    if not isinstance(payload, list) or not payload:
        return payload, {}
    if not all(isinstance(x, dict) for x in payload):
        return payload, {}

    quotes_by_ticker: dict[str, dict[str, Any]] = {}

    def quote_for(sym: str) -> dict[str, Any] | None:
        if sym in quotes_by_ticker:
            cached = quotes_by_ticker[sym]
            return cached if cached else None
        try:
            # P&amp;L de custódia depende da cotação atual; usar cache pode "travar"
            # a atualização por alguns segundos.
            q = fetch_quote(sym, use_cache=False)
        except Exception:
            logger.warning('Cotação custódia ticker=%s', sym)
            q = _latest_local_quote_for_ticker(sym)
            quotes_by_ticker[sym] = q or {}
            return q
        quotes_by_ticker[sym] = q
        return q

    enriched: list[dict[str, Any]] = []
    row_pnl_classes: list[str] = []
    total_net_value = 0.0
    total_pnl_brl = 0.0
    has_total_net_value = False
    has_total_pnl = False

    for row in payload:
        base = dict(row)
        ticker = _normalize_ticker(_row_get_ci(base, *_TICKER_KEYS))
        qty = _parse_float_any(_row_get_ci(base, *_QTY_KEYS))
        avg = _parse_float_any(_row_get_ci(base, *_AVG_KEYS))

        if not ticker or qty is None or abs(qty) < 1e-12:
            base['markPrice'] = '—'
            base['pnlBrl'] = '—'
            base['pnlPct'] = '—'
            base['sessionStatus'] = '—'
            row_pnl_classes.append('neutral')
            enriched.append(base)
            continue

        q = quote_for(ticker)
        mark = mark_price_from_quote(q or {})
        if mark is None:
            mark = _LAST_MARK_PRICE_BY_TICKER.get(ticker)
        if mark is not None:
            _LAST_MARK_PRICE_BY_TICKER[ticker] = mark

        if mark is None:
            base['markPrice'] = '—'
            base['pnlBrl'] = '—'
            base['pnlPct'] = '—'
            base['sessionStatus'] = '—'
            row_pnl_classes.append('neutral')
            enriched.append(base)
            continue

        if avg is None:
            base['markPrice'] = _format_brl_br(mark)
            base['pnlBrl'] = '—'
            base['pnlPct'] = '—'
            base['sessionStatus'] = _session_label_pt(q) if q else 'Último preço local'
            row_pnl_classes.append('neutral')
            enriched.append(base)
            continue

        pnl_brl = (mark - avg) * qty
        denom = abs(avg) * abs(qty)
        pnl_pct_val = (pnl_brl / denom * 100.0) if denom > 1e-12 else None
        net_value_brl = mark * qty
        total_net_value += net_value_brl
        has_total_net_value = True
        total_pnl_brl += pnl_brl
        has_total_pnl = True

        if pnl_brl > 1e-9:
            cls = 'pos'
        elif pnl_brl < -1e-9:
            cls = 'neg'
        else:
            cls = 'neutral'
        row_pnl_classes.append(cls)

        base['markPrice'] = _format_brl_br(mark)
        base['pnlBrl'] = _format_brl_br(pnl_brl)
        base['pnlPct'] = _format_pct(pnl_pct_val) if pnl_pct_val is not None else '—'
        base['sessionStatus'] = _session_label_pt(q) if q else 'Último preço local'
        enriched.append(base)

    meta = {
        'row_pnl_classes': row_pnl_classes,
        'pnl_column_key': 'pnlBrl',
    }
    if has_total_net_value:
        meta['total_net_value_brl'] = _format_brl_br(total_net_value)
    if has_total_pnl:
        meta['total_pnl_brl'] = _format_brl_br(total_pnl_brl)
    return enriched, meta


def prepare_custody_payload(payload: Any) -> tuple[Any, dict[str, Any]]:
    """
    Aceita lista de posições ou objeto com lista em ``items``/``data``/etc.
    Devolve estrutura pronta para :func:`tabular_from_api_payload` e metadados de P&amp;L.
    """
    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        return enrich_custody_payload(payload)
    if isinstance(payload, dict):
        for alt in (
            'items',
            'Items',
            'data',
            'Data',
            'results',
            'Results',
            'custody',
            'Custody',
        ):
            inner = payload.get(alt)
            if isinstance(inner, list) and inner and all(isinstance(x, dict) for x in inner):
                enriched, meta = enrich_custody_payload(inner)
                out = dict(payload)
                out[alt] = enriched
                return out, meta
    return payload, {}


def apply_custody_enrichment_meta(
    tabular: dict[str, Any], meta: dict[str, Any]
) -> dict[str, Any]:
    """Acrescenta índice da coluna de P&amp;L e classes por linha ao dict tabular."""
    if not meta or 'keys' not in tabular:
        return tabular
    key = meta.get('pnl_column_key') or 'pnlBrl'
    keys = tabular['keys']
    if key in keys:
        tabular['pnl_column_index'] = keys.index(key)
    else:
        tabular['pnl_column_index'] = None
    tabular['row_pnl_classes'] = meta.get('row_pnl_classes') or []
    tabular['total_net_value_brl'] = meta.get('total_net_value_brl')
    tabular['total_pnl_brl'] = meta.get('total_pnl_brl')
    return tabular
