"""
Persistência de posições, liquidações e PnL estimado a partir de execuções locais.

Usado após envio bem-sucedido de ordem (boleta/liquidação) para alimentar o histórico interno.
"""

from __future__ import annotations

import logging
import random
import time
from decimal import Decimal
from typing import Any

from django.db import connection, transaction
from django.db.utils import OperationalError
from django.utils import timezone

from trader.environment import get_current_environment, normalize_environment
from trader.models import ClosedOperation, Position, PositionLiquidation

logger = logging.getLogger(__name__)

_DEFAULT_LANE = Position.Lane.STANDARD

# Replay + liquidação + Celery: SQLite ainda pode falhar com "database is locked" (WAL ajuda, não elimina).
_SQLITE_LOCK_RETRIES = 55
_SQLITE_LOCK_BASE_DELAY_SEC = 0.02
_SQLITE_LOCK_DELAY_CAP_SEC = 1.25

_FILL_PRICE_KEYS = (
    'averagePrice',
    'AveragePrice',
    'averageExecutedPrice',
    'AverageExecutedPrice',
    'executionPrice',
    'ExecutionPrice',
    'executedPrice',
    'ExecutedPrice',
    'lastExecutedPrice',
    'LastExecutedPrice',
    'price',
    'Price',
)


def _to_dec(v) -> Decimal:
    return Decimal(str(v))


def _safe_open_price(px: Decimal) -> Decimal:
    """
    Garante preço de abertura positivo para não gerar posição "fantasma" no guard.
    """
    return px if px > 0 else Decimal('0.01')


def _side_norm(side: str) -> str:
    s = str(side or '').strip().upper()
    if s in ('BUY', 'COMPRA'):
        return 'BUY'
    if s in ('SELL', 'VENDA'):
        return 'SELL'
    return s


def _try_decimal(v: Any) -> Decimal | None:
    if v is None or v == '':
        return None
    try:
        d = Decimal(str(v).strip().replace(',', '.'))
        return d
    except (ArithmeticError, ValueError, TypeError):
        return None


def infer_execution_price(body: dict[str, Any] | None, resp: Any) -> Decimal | None:
    """
    Tenta obter preço de execução a partir do corpo enviado e/ou da resposta JSON da API.
    """
    b = body or {}
    for key in ('Price', 'StopOrderPrice', 'price', 'stopOrderPrice'):
        if key in b and b[key] is not None:
            d = _try_decimal(b[key])
            if d is not None and d != 0:
                return d

    def _from_mapping(m: dict[str, Any], depth: int) -> Decimal | None:
        if depth > 3:
            return None
        for k in _FILL_PRICE_KEYS:
            if k in m:
                d = _try_decimal(m.get(k))
                if d is not None:
                    return d
        for nest in ('order', 'Order', 'data', 'Data', 'result', 'Result'):
            sub = m.get(nest)
            if isinstance(sub, dict):
                d = _from_mapping(sub, depth + 1)
                if d is not None:
                    return d
        return None

    if isinstance(resp, dict):
        d = _from_mapping(resp, 0)
        if d is not None:
            return d
    return None


def should_record_local_history(order_kind: str, resp: Any) -> bool:
    """
    Ordens a mercado: após sucesso HTTP, assume execução para o histórico local (simulador).

    Limitada / stop-limit: só registra se a resposta indicar execução (filled / qtd. executada),
    para não criar posição quando a ordem ficou só em aberto (ex.: open_limited).
    """
    k = (order_kind or '').strip().lower()
    if k == 'market':
        return True
    if not isinstance(resp, dict):
        return False
    status = (
        resp.get('orderStatus')
        or resp.get('OrderStatus')
        or resp.get('status')
        or resp.get('Status')
        or ''
    )
    st = str(status).strip().lower()
    if st in ('filled', 'partiallyfilled', 'partially_filled'):
        return True
    for qkey in (
        'executedQuantity',
        'ExecutedQuantity',
        'filledQuantity',
        'FilledQuantity',
        'executedQty',
    ):
        raw = resp.get(qkey)
        if raw is None:
            continue
        try:
            if float(raw) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _position_signed_qty(active: Position) -> Decimal:
    q = active.quantity_open
    if active.side == Position.Side.LONG:
        return q
    return -q


def _gross_pnl_on_close(active: Position) -> Decimal:
    liqs = list(active.liquidations.all().values('quantity', 'price'))
    gross = Decimal('0')
    avg = active.avg_open_price
    for row in liqs:
        q = _to_dec(row['quantity'])
        px = _to_dec(row['price'])
        if active.side == Position.Side.LONG:
            gross += (px - avg) * q
        else:
            gross += (avg - px) * q
    return gross


def register_trade_execution(
    *,
    ticker: str,
    side: str,
    quantity,
    price=None,
    source: str = '',
    trading_environment: str | None = None,
    position_lane: str | None = None,
) -> Position | None:
    """
    Atualiza posição aberta, registra liquidações parciais/totais e fecha com ClosedOperation quando zera.

    ``price`` ausente usa preço médio atual na liquidação (fallback).
    ``trading_environment`` ausente usa :func:`trader.environment.get_current_environment` (sessão/middleware).
    ``position_lane`` — ``standard`` (ledger alinhado à API) ou ``replay_shadow`` (só replay, preço das velas).
    """
    last_exc: OperationalError | None = None
    for attempt in range(_SQLITE_LOCK_RETRIES):
        try:
            return _register_trade_execution_atomic(
                ticker=ticker,
                side=side,
                quantity=quantity,
                price=price,
                source=source,
                trading_environment=trading_environment,
                position_lane=position_lane,
            )
        except OperationalError as exc:
            msg = str(exc).lower()
            if 'database is locked' not in msg:
                raise
            last_exc = exc
            if attempt >= _SQLITE_LOCK_RETRIES - 1:
                logger.warning(
                    'register_trade_execution: database is locked após %d tentativas (%s)',
                    _SQLITE_LOCK_RETRIES,
                    ticker,
                )
                raise
            delay = min(
                _SQLITE_LOCK_DELAY_CAP_SEC,
                _SQLITE_LOCK_BASE_DELAY_SEC * (2 ** min(attempt, 16)) + random.random() * 0.04,
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc
    return None


@transaction.atomic
def _register_trade_execution_atomic(
    *,
    ticker: str,
    side: str,
    quantity,
    price=None,
    source: str = '',
    trading_environment: str | None = None,
    position_lane: str | None = None,
) -> Position | None:
    """
    Implementação transacional de :func:`register_trade_execution` (com retry no wrapper).
    """
    now = timezone.now()
    env = normalize_environment(
        trading_environment if trading_environment is not None else get_current_environment()
    )
    lane = (position_lane or _DEFAULT_LANE).strip()
    if lane not in (Position.Lane.STANDARD, Position.Lane.REPLAY_SHADOW):
        lane = Position.Lane.STANDARD
    t = (ticker or '').strip().upper()
    qty_abs = abs(_to_dec(quantity))
    if not t or qty_abs <= 0:
        raise ValueError('Ticker ou quantidade inválidos para histórico.')

    is_buy = _side_norm(side) == 'BUY'
    trade_signed = qty_abs if is_buy else -qty_abs
    exec_price = _to_dec(price) if price is not None else Decimal('0')

    # No SQLite, ``select_for_update`` aumenta contenção com replay/Celery; o transacional já serializa escrita.
    pos_qs = Position.objects.active().filter(
        ticker=t, trading_environment=env, position_lane=lane
    ).order_by('-opened_at')
    if connection.vendor != 'sqlite':
        pos_qs = pos_qs.select_for_update()
    active = pos_qs.first()

    liq_dir = (
        PositionLiquidation.Direction.BUY if is_buy else PositionLiquidation.Direction.SELL
    )

    if active is None:
        side_enum = Position.Side.LONG if trade_signed > 0 else Position.Side.SHORT
        open_px = _safe_open_price(exec_price)
        return Position.objects.create(
            ticker=t,
            trading_environment=env,
            position_lane=lane,
            side=side_enum,
            quantity_open=abs(trade_signed),
            avg_open_price=open_px,
            opened_at=now,
            is_active=True,
        )

    pos_signed = _position_signed_qty(active)

    # Mesmo sentido: aumenta posição e recalcula PM.
    if pos_signed * trade_signed > 0:
        new_abs = abs(pos_signed) + abs(trade_signed)
        if exec_price != 0 and new_abs > 0:
            weighted = (active.avg_open_price * abs(pos_signed)) + (exec_price * abs(trade_signed))
            active.avg_open_price = weighted / new_abs
        active.quantity_open = new_abs
        active.save(update_fields=['quantity_open', 'avg_open_price', 'updated_at'])
        return active

    # Sentido oposto: reduz, zera ou inverte.
    remaining_signed = pos_signed + trade_signed
    close_amt = min(abs(pos_signed), abs(trade_signed))
    px = exec_price if exec_price != 0 else active.avg_open_price

    mode = (
        PositionLiquidation.LiquidationMode.FULL
        if close_amt >= abs(pos_signed)
        else PositionLiquidation.LiquidationMode.PARTIAL
    )
    PositionLiquidation.objects.create(
        position=active,
        mode=mode,
        direction=liq_dir,
        quantity=close_amt,
        price=px,
        executed_at=now,
    )

    if remaining_signed == 0:
        gross = _gross_pnl_on_close(active)
        active.quantity_open = Decimal('0')
        active.is_active = False
        active.closed_at = now
        active.save(update_fields=['quantity_open', 'is_active', 'closed_at', 'updated_at'])
        ClosedOperation.objects.update_or_create(
            position=active,
            defaults={
                'pnl_type': ClosedOperation.PnLType.ESTIMATED,
                'gross_pnl': gross,
                'fees': Decimal('0'),
                'net_pnl': gross,
                'notes': f'Fechamento ({source or "execução"})',
                'closed_at': now,
            },
        )
        return active

    if pos_signed * remaining_signed > 0:
        active.quantity_open = abs(remaining_signed)
        active.save(update_fields=['quantity_open', 'updated_at'])
        return active

    # Cruzou zero: encerra posição antiga e abre nova no remanescente.
    gross = _gross_pnl_on_close(active)
    active.quantity_open = Decimal('0')
    active.is_active = False
    active.closed_at = now
    active.save(update_fields=['quantity_open', 'is_active', 'closed_at', 'updated_at'])
    ClosedOperation.objects.update_or_create(
        position=active,
        defaults={
            'pnl_type': ClosedOperation.PnLType.ESTIMATED,
            'gross_pnl': gross,
            'fees': Decimal('0'),
            'net_pnl': gross,
            'notes': f'Fechamento parcial por inversão ({source or "execução"})',
            'closed_at': now,
        },
    )

    excess = abs(remaining_signed)
    new_side = Position.Side.LONG if remaining_signed > 0 else Position.Side.SHORT
    new_px = _safe_open_price(px)
    return Position.objects.create(
        ticker=t,
        trading_environment=env,
        position_lane=lane,
        side=new_side,
        quantity_open=excess,
        avg_open_price=new_px,
        opened_at=now,
        is_active=True,
    )
