"""Separação visual no simulador: marcações de execução replay vs tempo real (API).

A custódia ``GET /v1/custody`` é única na conta simulada; este módulo agrega
:class:`~trader.models.TradeMarker` com ``metadata.custody_channel`` em ``replay`` ou ``live``.
"""

from __future__ import annotations

import logging
from typing import Any

from django.utils import timezone as dj_tz

from trader.environment import ENV_SIMULATOR, get_current_environment

logger = logging.getLogger(__name__)

# Valores gravados em TradeMarker.metadata["custody_channel"]
SIMULATOR_CUSTODY_REPLAY = 'replay'
SIMULATOR_CUSTODY_LIVE = 'live'

# ``TradeMarker.source`` (chave interna) → texto curto para a tabela + explicação no ``title``.
_MARKER_SOURCE_PT: dict[str, tuple[str, str]] = {
    'tendencia_mercado_ativa': (
        'Tendência ativa',
        'Robô «tendência de mercado ativa» (curto prazo).',
    ),
    'leafar': (
        'leafaR',
        'Robô leafaR (volume / VP).',
    ),
    'order_test_form': (
        'Boleta de teste',
        'Você enviou pela boleta de ordem de teste no painel.',
    ),
    'liquidate_single': (
        'Liquidação',
        'Botão «Liquidar ativo» na custódia.',
    ),
    'liquidate_all': (
        'Liquidação (todos)',
        'Botão «Liquidar todos» na custódia.',
    ),
    'send_test_order_cmd': (
        'Comando CLI',
        'Comando ``send_test_order`` no terminal.',
    ),
}


def trade_marker_source_labels(raw: str | None) -> tuple[str, str]:
    """(rótulo curto, texto longo para tooltip / acessibilidade)."""
    key = (raw or '').strip()
    if not key:
        return '—', ''
    hit = _MARKER_SOURCE_PT.get(key)
    if hit:
        return hit
    return (key[:28] + ('…' if len(key) > 28 else ''), f'Identificador interno: {key}')


def session_label_is_replay(session_label: str | None) -> bool:
    s = (session_label or '').strip().lower()
    if not s:
        return False
    return 'replay' in s or s in ('session_replay', 'replay_day')


def classify_simulator_custody_channel(metadata: dict[str, Any] | None) -> str:
    m = metadata if isinstance(metadata, dict) else {}
    ch = str(m.get('custody_channel') or m.get('simulator_custody') or '').strip().lower()
    if ch in (SIMULATOR_CUSTODY_REPLAY, 'replay_automation', 'session_replay'):
        return SIMULATOR_CUSTODY_REPLAY
    if ch in (SIMULATOR_CUSTODY_LIVE, 'api_boleta', 'simulator_api_live', 'api_manual'):
        return SIMULATOR_CUSTODY_LIVE
    ds = str(m.get('data_source') or '').lower()
    if 'replay' in ds:
        return SIMULATOR_CUSTODY_REPLAY
    if ds == 'live_tail':
        return SIMULATOR_CUSTODY_LIVE
    # Marcações antigas (boleta / liquidação sem canal): tratar como uso direto da API.
    return SIMULATOR_CUSTODY_LIVE


def record_bracket_execution_marker(
    *,
    ticker: str,
    side: str,
    quantity: int,
    last: float | None,
    strategy_source: str,
    log_session_label: str | None,
    market_order_id: str | None = None,
) -> None:
    """Grava TradeMarker para separar replay vs ao vivo na custódia do simulador."""
    if get_current_environment() != ENV_SIMULATOR:
        return
    try:
        from trader.services.trade_markers import record_trade_marker

        ch = (
            SIMULATOR_CUSTODY_REPLAY
            if session_label_is_replay(log_session_label)
            else SIMULATOR_CUSTODY_LIVE
        )
        record_trade_marker(
            ticker=ticker,
            side=side,
            quantity=quantity,
            price=last,
            source=strategy_source,
            metadata={
                'custody_channel': ch,
                'data_source': (log_session_label or '').strip(),
                'market_order_id': (market_order_id or '').strip(),
            },
        )
    except Exception:
        logger.exception('record_bracket_execution_marker %s', ticker)


def build_simulator_custody_activity_display(*, per_channel_limit: int = 40) -> dict[str, Any]:
    """Linhas recentes por canal (só leitura; não substitui GET /custody)."""
    from trader.models import TradeMarker

    replay_rows: list[dict[str, Any]] = []
    live_rows: list[dict[str, Any]] = []
    scanned = 0
    max_scan = max(200, per_channel_limit * 15)
    for tm in TradeMarker.objects.order_by('-marker_at').iterator():
        scanned += 1
        if scanned > max_scan:
            break
        if len(replay_rows) >= per_channel_limit and len(live_rows) >= per_channel_limit:
            break
        ch = classify_simulator_custody_channel(tm.metadata)
        cap_short, cap_long = trade_marker_source_labels(tm.source)
        row = {
            'at': dj_tz.localtime(tm.marker_at).strftime('%d/%m %H:%M:%S'),
            'ticker': tm.ticker,
            'side': tm.side,
            'qty': str(tm.quantity),
            'price': '—' if tm.price is None else f'{tm.price:.4f}',
            'registo': cap_short,
            'registo_title': cap_long or (tm.source or '').strip(),
        }
        if ch == SIMULATOR_CUSTODY_REPLAY:
            if len(replay_rows) < per_channel_limit:
                replay_rows.append(row)
        else:
            if len(live_rows) < per_channel_limit:
                live_rows.append(row)
    return {
        'replay_rows': replay_rows,
        'live_rows': live_rows,
        'replay_empty': len(replay_rows) == 0,
        'live_empty': len(live_rows) == 0,
    }
