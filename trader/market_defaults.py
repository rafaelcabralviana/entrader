"""
Defaults do painel focados em **day trade BMF**: mini Índice (WIN) e mini Dólar (WDO).

Ajuste vencimento (letra do mês + ano) conforme o contrato **ativo** na B3 / simulador.
Variáveis de ambiente opcionais (sobrescrevem os exemplos abaixo):

- ``ENTRADE_DEFAULT_WIN_TICKER`` — ex.: ``WINJ26``
- ``ENTRADE_DEFAULT_WDO_TICKER`` — ex.: ``WDOF26``
"""

import os

# Contratos exemplo — troque pelo vencimento negociado no dia.
_DEFAULT_WIN = 'WINJ26'
_DEFAULT_WDO = 'WDOF26'


def _env_ticker(name: str, fallback: str) -> str:
    v = os.environ.get(name, '').strip().upper()
    return v or fallback


def default_daytrade_win_ticker() -> str:
    """Mini Ibovespa (WIN) — ticker padrão ao abrir o Mercado."""
    return _env_ticker('ENTRADE_DEFAULT_WIN_TICKER', _DEFAULT_WIN)


def default_daytrade_wdo_ticker() -> str:
    """Mini dólar (WDO) — segundo atalho principal."""
    return _env_ticker('ENTRADE_DEFAULT_WDO_TICKER', _DEFAULT_WDO)


def default_primary_ticker() -> str:
    """Ticker inicial da página Mercado (mini índice por padrão)."""
    return default_daytrade_win_ticker()


def default_ticker_suggestions_daytrade() -> tuple[str, ...]:
    """Atalhos principais: WIN + WDO."""
    return (default_daytrade_win_ticker(), default_daytrade_wdo_ticker())


# Ações (BOVESPA) — referência; limites de boleta agregados (500).
DEFAULT_TICKER_SUGGESTIONS_BOV: tuple[str, ...] = (
    'PETR4',
    'VALE3',
    'ITUB4',
    'BBDC4',
)


def default_ticker_suggestions_equities() -> tuple[str, ...]:
    return DEFAULT_TICKER_SUGGESTIONS_BOV


def default_ticker_suggestions() -> tuple[str, ...]:
    """Ordem: day trade (BMF) primeiro, depois exemplos de ações."""
    return default_ticker_suggestions_daytrade() + default_ticker_suggestions_equities()
