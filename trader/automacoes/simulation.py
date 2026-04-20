"""
Simulação de mercado por dia (somente ambiente simulador): usa snapshots salvos como se fossem «ao vivo».

O estado fica na sessão Django; em ambiente real é ignorado e limpo ao trocar para REAL.
"""

from __future__ import annotations

from datetime import date

from trader.environment import ENV_SIMULATOR, get_session_environment

SESSION_KEY_SIM_ENABLED = 'trader_automation_mkt_sim_enabled'
SESSION_KEY_SIM_DATE = 'trader_automation_mkt_sim_date'
SESSION_KEY_SIM_TICKER = 'trader_automation_mkt_sim_ticker'


def _parse_iso_date(raw: str | None) -> date | None:
    s = (raw or '').strip()[:10]
    if not s or len(s) != 10:
        return None
    try:
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    except ValueError:
        return None


def clear_automation_market_simulation(request) -> None:
    """Remove flags de simulação da sessão."""
    request.session.pop(SESSION_KEY_SIM_ENABLED, None)
    request.session.pop(SESSION_KEY_SIM_DATE, None)
    request.session.pop(SESSION_KEY_SIM_TICKER, None)


def get_automation_market_simulation(request) -> dict:
    """
    Retorna estado da simulação para templates e APIs.

    ``effective`` é True só em simulador, com opção ligada e data válida.
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
    if env != ENV_SIMULATOR:
        return out
    out['enabled_flag'] = bool(request.session.get(SESSION_KEY_SIM_ENABLED))
    raw = (request.session.get(SESSION_KEY_SIM_DATE) or '').strip()
    sd = _parse_iso_date(raw)
    if sd:
        out['session_date'] = sd
        out['session_date_iso'] = sd.isoformat()
        out['label_br'] = sd.strftime('%d/%m/%Y')
    raw_sym = (request.session.get(SESSION_KEY_SIM_TICKER) or '').strip().upper()
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
    Grava simulação na sessão (apenas faz sentido em simulador; chamar só após checar ambiente).
    Data ou ticker inválidos não alteram o estado anterior.
    """
    if not enabled:
        clear_automation_market_simulation(request)
        request.session.modified = True
        return get_automation_market_simulation(request)
    raw = (session_date_iso or '').strip()
    sd = _parse_iso_date(raw)
    sym = (sim_ticker or '').strip().upper()
    if sd is None or not sym:
        return get_automation_market_simulation(request)
    request.session[SESSION_KEY_SIM_ENABLED] = True
    request.session[SESSION_KEY_SIM_DATE] = sd.isoformat()
    request.session[SESSION_KEY_SIM_TICKER] = sym
    request.session.modified = True
    return get_automation_market_simulation(request)
