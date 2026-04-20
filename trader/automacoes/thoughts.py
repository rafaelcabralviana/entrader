"""
Registro e leitura de “pensamentos” da automação (log auditável por usuário e ambiente).
Tasks e análises futuras devem chamar :func:`record_automation_thought`.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.db.models import Q
from django.utils import timezone

from trader.automacoes.strategies import is_passive_strategy, strategy_by_key
from trader.environment import normalize_environment
from trader.models import AutomationThought

_TZ_BRT = ZoneInfo('America/Sao_Paulo')


def record_automation_thought(
    user,
    trading_environment: str,
    message: str,
    *,
    source: str = '',
    kind: str = AutomationThought.Kind.INFO,
    execution_profile=None,
) -> AutomationThought:
    """Persiste uma linha de log. ``source`` identifica o módulo (ex.: ``analise:book``, ``estrategia``)."""
    env = normalize_environment(trading_environment)
    src = (source or '').strip()[:96]
    msg = (message or '').strip()
    if not msg:
        msg = '(vazio)'
    valid_kinds = {c.value for c in AutomationThought.Kind}
    k = kind if kind in valid_kinds else AutomationThought.Kind.INFO
    return AutomationThought.objects.create(
        user=user,
        trading_environment=env,
        execution_profile=execution_profile,
        message=msg,
        source=src,
        kind=k,
    )


def fetch_thoughts_for_poll(
    user,
    trading_environment: str,
    *,
    since_id: int | None = None,
    execution_profile=None,
    limit_initial: int = 120,
    limit_poll: int = 400,
) -> list[AutomationThought]:
    """
    ``since_id`` None ou 0: últimas ``limit_initial`` linhas, ordem cronológica (mais antiga primeiro).
    ``since_id`` > 0: linhas com id maior (novas desde o último poll), até ``limit_poll``.
    """
    env = normalize_environment(trading_environment)
    base = AutomationThought.objects.filter(user=user, trading_environment=env)
    if execution_profile is not None:
        base = base.filter(
            Q(execution_profile=execution_profile) | Q(execution_profile__isnull=True)
        )
    sid = int(since_id or 0)
    if sid > 0:
        return list(base.filter(id__gt=sid).order_by('id')[:limit_poll])
    rows = list(base.order_by('-id')[: max(1, limit_initial)])
    rows.reverse()
    return rows


def parse_calendar_day_brt(raw: str | None) -> date:
    """
    Interpreta ``YYYY-MM-DD``; se inválido ou vazio, retorna a data atual em BRT.
    """
    today = timezone.now().astimezone(_TZ_BRT).date()
    if raw is None or not str(raw).strip():
        return today
    try:
        return date.fromisoformat(str(raw).strip()[:10])
    except ValueError:
        return today


def calendar_day_bounds_brt(day: date) -> tuple[datetime, datetime]:
    """Intervalo [início, fim) do dia civil em America/Sao_Paulo (consciente)."""
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=_TZ_BRT)
    end = start + timedelta(days=1)
    return start, end


def thought_to_dict(row: AutomationThought) -> dict:
    ts = row.created_at
    if timezone.is_naive(ts):
        ts = timezone.make_aware(ts, timezone.get_current_timezone())
    ts_brt = ts.astimezone(_TZ_BRT)
    src = row.source or ''
    passive = is_passive_strategy(src)
    st = strategy_by_key(src)
    kind_out = row.kind
    # Compatibilidade com registros antigos da leafaR que eram salvos como INFO.
    if src == 'leafar' and 'sinal ' in (row.message or '').lower():
        kind_out = AutomationThought.Kind.WARN
    return {
        'id': row.id,
        'created_at': ts.isoformat(),
        'label_time': ts_brt.strftime('%d/%m %H:%M:%S'),
        'label_datetime': ts_brt.strftime('%d/%m/%Y %H:%M:%S'),
        'source': src,
        'kind': kind_out,
        'message': row.message,
        'is_passive': passive,
        'strategy_title': (st.get('title') if st else '') or '',
    }


def passive_insight_cards_from_thoughts(thought_dicts: list[dict]) -> list[dict[str, str]]:
    """
    Uma entrada por estratégia passiva: mantém a última linha (ordem cronológica crescente em ``thought_dicts``).
    """
    latest: dict[str, dict] = {}
    for t in thought_dicts:
        if not isinstance(t, dict) or not t.get('is_passive'):
            continue
        src = (t.get('source') or '').strip()
        if not src:
            continue
        latest[src] = t
    cards: list[dict[str, str]] = []
    for src in sorted(latest.keys()):
        row = latest[src]
        title = (row.get('strategy_title') or src).strip()
        msg = (row.get('message') or '').strip()
        accent = ''
        if src == 'tendencia_mercado':
            if 'Tendência Alta' in msg:
                accent = 'up'
            elif 'Tendência Baixa' in msg:
                accent = 'down'
            else:
                accent = 'flat'
        cards.append(
            {
                'source': src,
                'title': title,
                'message': msg,
                'label_time': (row.get('label_time') or '').strip(),
                'accent': accent,
            }
        )
    return cards
