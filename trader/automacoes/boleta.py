"""
Automações de preenchimento da boleta (stop automático a partir de % e preço de referência).
Usa os mesmos snapshots locais que /mercado/snapshot.json (QuoteSnapshot/BookSnapshot).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from trader.market_defaults import default_primary_ticker
from trader.panel_context import build_market_context_local, resolve_daytrade_base_ticker

_BASIS_CHOICES = frozenset({'last', 'bid', 'ask', 'mid'})


def _parse_decimal(raw: object) -> Decimal | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, (int, float)):
        return Decimal(str(raw))
    s = str(raw).strip().replace(' ', '')
    if not s:
        return None
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    else:
        s = s.replace(',', '.')
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _book_best_prices(book: dict | None) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(book, dict):
        return None, None
    bids = book.get('bids') or book.get('Bids') or []
    asks = book.get('asks') or book.get('Asks') or []
    bid_p = None
    ask_p = None
    if isinstance(bids, list) and bids:
        top = bids[0]
        if isinstance(top, dict):
            bid_p = _parse_decimal(top.get('price', top.get('Price')))
    if isinstance(asks, list) and asks:
        top = asks[0]
        if isinstance(top, dict):
            ask_p = _parse_decimal(top.get('price', top.get('Price')))
    return bid_p, ask_p


def reference_price_for_basis(
    quote: dict | None, book: dict | None, basis: str
) -> tuple[Decimal | None, str]:
    """
    Retorna (preço, rótulo da origem).
    Ordem de fallback: conforme basis; depois último, meio do spread, bid, ask.
    """
    bkey = (basis or 'last').strip().lower()
    if bkey not in _BASIS_CHOICES:
        bkey = 'last'

    q = quote if isinstance(quote, dict) else {}
    last = _parse_decimal(
        q.get('lastPrice') or q.get('LastPrice') or q.get('last_price')
    )
    bid_p, ask_p = _book_best_prices(book)

    if bkey == 'last' and last is not None:
        return last, 'lastPrice'
    if bkey == 'bid' and bid_p is not None:
        return bid_p, 'bestBid'
    if bkey == 'ask' and ask_p is not None:
        return ask_p, 'bestAsk'
    if bkey == 'mid' and bid_p is not None and ask_p is not None:
        return (bid_p + ask_p) / Decimal('2'), 'mid(bid/ask)'

    if last is not None:
        return last, 'lastPrice(fallback)'
    if bid_p is not None and ask_p is not None:
        return (bid_p + ask_p) / Decimal('2'), 'mid(fallback)'
    if bid_p is not None:
        return bid_p, 'bestBid(fallback)'
    if ask_p is not None:
        return ask_p, 'bestAsk(fallback)'
    return None, 'none'


def _fmt_price(d: Decimal) -> str:
    s = format(d, 'f')
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s if s else '0'


def compute_stop_trigger_and_order(
    ref: Decimal,
    side: str,
    pct: Decimal,
    order_delta_pct: Decimal,
) -> tuple[Decimal, Decimal]:
    """
    Compra stop-limit (disparo acima da ref); venda stop-limit (disparo abaixo).
    Ordem limitada: ajuste opcional em % sobre o disparo (compra +delta, venda -delta).
    """
    s = (side or 'Buy').strip().upper()
    p = pct / Decimal('100')
    if s == 'SELL':
        trig = ref * (Decimal('1') - p)
    else:
        trig = ref * (Decimal('1') + p)
    dlt = order_delta_pct / Decimal('100')
    if s == 'SELL':
        ord_p = trig * (Decimal('1') - dlt)
    else:
        ord_p = trig * (Decimal('1') + dlt)
    return trig, ord_p


@login_required
@require_GET
def boleta_auto_stop_suggest(request):
    """
    GET: ticker, side (Buy/Sell), pct (ex.: 0.5), basis (last|bid|ask|mid),
    order_delta_pct (opcional, default 0 — mesma faixa para disparo e ordem).
    """
    raw_ticker = (request.GET.get('ticker') or default_primary_ticker()).strip().upper()
    ticker = resolve_daytrade_base_ticker(request, raw_ticker)
    side = (request.GET.get('side') or 'Buy').strip()
    basis = (request.GET.get('basis') or 'last').strip().lower()

    try:
        pct = Decimal(str(request.GET.get('pct') or '0.5'))
    except InvalidOperation:
        pct = Decimal('0.5')
    if pct < 0:
        pct = Decimal('0')
    if pct > 100:
        pct = Decimal('100')

    try:
        od = Decimal(str(request.GET.get('order_delta_pct') or '0'))
    except InvalidOperation:
        od = Decimal('0')
    if od < 0:
        od = Decimal('0')
    if od > 100:
        od = Decimal('100')

    ctx = build_market_context_local(ticker)
    quote = ctx.get('quote')
    book = ctx.get('book')
    ref, ref_label = reference_price_for_basis(quote, book, basis)
    if ref is None:
        return JsonResponse(
            {
                'ok': False,
                'error': 'Sem preço de referência (snapshot vazio). Aguarde cotação ou ajuste o ticker.',
                'ticker': ctx.get('ticker') or ticker,
            },
            status=200,
        )

    trig, ord_p = compute_stop_trigger_and_order(ref, side, pct, od)
    return JsonResponse(
        {
            'ok': True,
            'ticker': ctx.get('ticker') or ticker,
            'side': side,
            'basis': basis,
            'ref_label': ref_label,
            'ref_price': _fmt_price(ref),
            'stop_trigger': _fmt_price(trig),
            'stop_order': _fmt_price(ord_p),
            'pct': str(pct),
            'order_delta_pct': str(od),
        }
    )
