"""
Replay «em série» dos instantes de ``QuoteSnapshot`` para o mesmo pipeline do ao vivo.

Cada tick chama :func:`~trader.automacoes.automation_engine.run_automation_session_replay_now`
com ``replay_until`` igual ao ``captured_at`` do snapshot e ``force=True``, para não ficar
preso ao cursor monotónico ao avançar no tempo (voltar ou reprocessar o dia).
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone as dj_tz

from trader.automacoes.automation_engine import run_automation_session_replay_now
from trader.automacoes.runtime import runtime_enabled
from trader.environment import ENV_REPLAY, set_current_environment
from trader.models import QuoteSnapshot

logger = logging.getLogger(__name__)
_TZ_SP = ZoneInfo('America/Sao_Paulo')


def stream_session_replay_ticks(
    *,
    user_id: int,
    ticker: str,
    session_day: date,
    pace_sec: float = 1.0,
    max_snapshots: int | None = None,
) -> dict[str, Any]:
    """
    Percorre ``QuoteSnapshot`` do dia em ordem e dispara o motor de estratégias a cada instante.

    Requer utilizador com runtime activo no simulador e estratégias activas (igual ao replay
    manual). O perfil de simulação deve ter ``execution_started_at`` preenchido (como no UI).
    """
    User = get_user_model()
    user = User.objects.filter(id=int(user_id), is_active=True).first()
    if user is None:
        return {'ok': False, 'error': 'user_not_found'}
    if not runtime_enabled(user, ENV_REPLAY):
        return {'ok': False, 'error': 'runtime_disabled'}
    sym = (ticker or '').strip().upper()
    if not sym:
        return {'ok': False, 'error': 'ticker'}
    try:
        pace = float(pace_sec)
    except (TypeError, ValueError):
        pace = 1.0
    if pace < 0:
        pace = 0.0
    default_cap = int(getattr(settings, 'TRADER_REPLAY_STREAM_MAX_SNAPSHOTS', 5000) or 5000)
    try:
        cap = int(max_snapshots) if max_snapshots is not None else default_cap
    except (TypeError, ValueError):
        cap = default_cap
    cap = max(1, min(cap, 100_000))

    day_start = datetime.combine(session_day, dtime.min, tzinfo=_TZ_SP)
    day_end = day_start + timedelta(days=1)
    set_current_environment(ENV_REPLAY)

    qs = (
        QuoteSnapshot.objects.filter(
            ticker=sym,
            captured_at__gte=day_start,
            captured_at__lt=day_end,
        )
        .order_by('captured_at')
        .values_list('captured_at', flat=True)[:cap]
    )
    n = 0
    last_cap: datetime | None = None
    for cap_dt in qs.iterator(chunk_size=256):
        if not isinstance(cap_dt, datetime):
            continue
        if dj_tz.is_naive(cap_dt):
            cap_dt = dj_tz.make_aware(cap_dt, _TZ_SP)
        else:
            cap_dt = cap_dt.astimezone(_TZ_SP)
        last_cap = cap_dt
        try:
            run_automation_session_replay_now(
                user,
                session_day=session_day,
                sim_ticker=sym,
                replay_until=cap_dt,
                trading_environment=ENV_REPLAY,
                force=True,
            )
        except Exception:
            logger.exception(
                'stream_session_replay_ticks tick user_id=%s ticker=%s', user_id, sym
            )
        n += 1
        if pace > 0:
            time.sleep(pace)
    return {
        'ok': True,
        'ticks': n,
        'last_captured_at': last_cap.isoformat() if last_cap else None,
        'ticker': sym,
        'session_day': session_day.isoformat(),
    }
