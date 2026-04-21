"""
Simulação de mercado por dia (simulador) e sessão de Replay (snapshots locais por dia).

O estado fica na sessão Django; em ambiente real é ignorado e limpo ao trocar para REAL.
Simulador e Replay usam chaves de sessão distintas para não misturar o dia activo.
"""

from __future__ import annotations

from datetime import date

from trader.environment import ENV_REAL, ENV_REPLAY, ENV_SIMULATOR, get_session_environment

SESSION_KEY_SIM_ENABLED = 'trader_automation_mkt_sim_enabled'
SESSION_KEY_SIM_DATE = 'trader_automation_mkt_sim_date'
SESSION_KEY_SIM_TICKER = 'trader_automation_mkt_sim_ticker'

SESSION_KEY_REPLAY_ENABLED = 'trader_automation_mkt_replay_enabled'
SESSION_KEY_REPLAY_DATE = 'trader_automation_mkt_replay_date'
SESSION_KEY_REPLAY_TICKER = 'trader_automation_mkt_replay_ticker'


def _parse_iso_date(raw: str | None) -> date | None:
    s = (raw or '').strip()[:10]
    if not s or len(s) != 10:
        return None
    try:
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    except ValueError:
        return None


def _session_keys_for_env(env: str) -> tuple[str, str, str]:
    if env == ENV_REPLAY:
        return (SESSION_KEY_REPLAY_ENABLED, SESSION_KEY_REPLAY_DATE, SESSION_KEY_REPLAY_TICKER)
    return (SESSION_KEY_SIM_ENABLED, SESSION_KEY_SIM_DATE, SESSION_KEY_SIM_TICKER)


def clear_automation_market_simulation(request) -> None:
    """Remove apenas as flags de simulação do simulador (não mexe na sessão de Replay)."""
    for k in (SESSION_KEY_SIM_ENABLED, SESSION_KEY_SIM_DATE, SESSION_KEY_SIM_TICKER):
        request.session.pop(k, None)


def clear_replay_market_session(request) -> None:
    """Remove flags da sessão de dia usada no ambiente Replay."""
    for k in (SESSION_KEY_REPLAY_ENABLED, SESSION_KEY_REPLAY_DATE, SESSION_KEY_REPLAY_TICKER):
        request.session.pop(k, None)


def clear_all_market_day_sessions(request) -> None:
    """Usado ao mudar para REAL: limpa simulador e replay na sessão."""
    clear_automation_market_simulation(request)
    clear_replay_market_session(request)


def get_automation_market_simulation(request) -> dict:
    """
    Retorna estado da simulação / sessão de dia para templates e APIs.

    ``effective`` é True no simulador ou em Replay, com opção ligada e data válida.
    """
    env = get_session_environment(request)
    out: dict = {
        'environment': env,
        'enabled_flag': False,
        'session_date': None,
        'session_date_iso': '',
        'sim_ticker': '',
        'effective': False,
        'label_br': '',
    }
    if env == ENV_REAL:
        return out
    if env not in (ENV_SIMULATOR, ENV_REPLAY):
        return out
    k_en, k_dt, k_sym = _session_keys_for_env(env)
    out['enabled_flag'] = bool(request.session.get(k_en))
    raw = (request.session.get(k_dt) or '').strip()
    sd = _parse_iso_date(raw)
    if sd:
        out['session_date'] = sd
        out['session_date_iso'] = sd.isoformat()
        out['label_br'] = sd.strftime('%d/%m/%Y')
    raw_sym = (request.session.get(k_sym) or '').strip().upper()
    if raw_sym:
        out['sim_ticker'] = raw_sym
    out['effective'] = bool(
        out['enabled_flag']
        and out['session_date'] is not None
        and bool(out['sim_ticker'])
    )
    return out


def set_automation_market_simulation(
    request,
    *,
    enabled: bool,
    session_date_iso: str | None,
    sim_ticker: str | None = None,
) -> dict:
    """
    Grava simulação na sessão (simulador ou Replay, conforme o ambiente activo na sessão).
    Data ou ticker inválidos não alteram o estado anterior.
    """
    env = get_session_environment(request)
    if env not in (ENV_SIMULATOR, ENV_REPLAY):
        return get_automation_market_simulation(request)
    k_en, k_dt, k_sym = _session_keys_for_env(env)
    if not enabled:
        request.session.pop(k_en, None)
        request.session.pop(k_dt, None)
        request.session.pop(k_sym, None)
        request.session.modified = True
        return get_automation_market_simulation(request)
    raw = (session_date_iso or '').strip()
    sd = _parse_iso_date(raw)
    sym = (sim_ticker or '').strip().upper()
    if sd is None or not sym:
        return get_automation_market_simulation(request)
    request.session[k_en] = True
    request.session[k_dt] = sd.isoformat()
    request.session[k_sym] = sym
    request.session.modified = True
    return get_automation_market_simulation(request)
