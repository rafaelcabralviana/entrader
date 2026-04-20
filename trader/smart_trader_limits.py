"""
Limites de negociação da Smart Trader API (referência oficial).

- Resetam no pregão do dia útil seguinte.
- **BMF:** limites **por ativo** (código-base, ex.: ``WIN``, ``WDO``).
- **BOVESPA:** **sem** segregação por ativo — todos os papéis compartilham o mesmo
  limite diário de ordens e o mesmo limite de boleta.
- Os valores **não** podem ser alterados pelo usuário (aplicação automática na API).

Fonte: documentação Clear / Smart Trader — Limites de Negociação.
"""

from __future__ import annotations

from typing import Literal

# --- Limite de ordem (quantidade máxima de ordens enviadas por dia) ---

DAILY_ORDER_LIMIT_BMF: dict[str, int] = {
    'BIT': 10,
    'WDO': 10,
    'DOL': 4,
    'WIN': 10,
    'IND': 4,
}

DAILY_ORDER_LIMIT_BOV: int = 10

# --- Limite de boleta (quantidade máxima por única ordem) ---

TICKET_LIMIT_BMF: dict[str, int] = {
    'BIT': 5,
    'WDO': 5,
    'DOL': 5,
    'WIN': 5,
    'IND': 5,
}

TICKET_LIMIT_BOV: int = 500

# Prefixos BMF na ordem de verificação (evita ``D`` engolir ``DOL``, etc.).
_BMF_BASE_PREFIXES: tuple[str, ...] = ('WDO', 'WIN', 'IND', 'DOL', 'BIT')

OperationKind = Literal['send_order', 'cancel_order', 'replace_order']

# (conta para limite diário de ordens?, conta para limite de boleta?)
OPERATION_LIMIT_RULES: dict[OperationKind, tuple[bool, bool]] = {
    'send_order': (True, True),
    'cancel_order': (False, False),
    'replace_order': (False, True),
}


def extract_bmf_base(ticker: str) -> str | None:
    """
    Retorna o código-base BMF se o ticker parecer futuro/termo (prefixo conhecido).

    Ex.: ``WINJ26`` → ``WIN``. ``PETR4`` → ``None`` (tratado como BOV).
    """
    t = ticker.strip().upper()
    for base in _BMF_BASE_PREFIXES:
        if t.startswith(base):
            return base
    return None


def daily_order_limit_for_ticker(ticker: str) -> int:
    """Limite diário de **envio** de ordens para o papel (BMF por base; BOV único)."""
    base = extract_bmf_base(ticker)
    if base is not None:
        return DAILY_ORDER_LIMIT_BMF[base]
    return DAILY_ORDER_LIMIT_BOV


def ticket_limit_for_ticker(ticker: str) -> int:
    """Limite máximo de quantidade/contratos em **uma** ordem (boleta)."""
    base = extract_bmf_base(ticker)
    if base is not None:
        return TICKET_LIMIT_BMF[base]
    return TICKET_LIMIT_BOV


def applies_daily_order_limit(operation: OperationKind) -> bool:
    """Se o tipo de operação conta para o limite diário de ordens."""
    return OPERATION_LIMIT_RULES[operation][0]


def applies_ticket_limit(operation: OperationKind) -> bool:
    """Se o tipo de operação está sujeito ao limite de boleta."""
    return OPERATION_LIMIT_RULES[operation][1]
