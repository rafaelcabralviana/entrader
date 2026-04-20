"""Reexporta funções de market data REST (compatibilidade com imports antigos)."""

from trader.services.marketdata import (
    fetch_aggregate_book,
    fetch_book,
    fetch_quote,
    fetch_ticker_details,
)

__all__ = [
    'fetch_aggregate_book',
    'fetch_book',
    'fetch_quote',
    'fetch_ticker_details',
]
