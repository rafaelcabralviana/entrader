from django.urls import path

from trader import views
from trader.automacoes.boleta import boleta_auto_stop_suggest

app_name = 'trader'

urlpatterns = [
    path('', views.panel_hub, name='panel_hub'),
    path('painel/', views.home, name='home'),
    path('automacoes/', views.automations_dashboard, name='automations_dashboard'),
    path(
        'automacoes/logs/',
        views.automations_logs_day,
        name='automations_logs_day',
    ),
    path(
        'automacoes/simulacao-mercado/',
        views.automation_market_simulation,
        name='automation_market_simulation',
    ),
    path('automacoes/estado.json', views.automations_state_json, name='automations_state_json'),
    path(
        'automacoes/pensamentos.json',
        views.automation_thoughts_json,
        name='automation_thoughts_json',
    ),
    path(
        'automacoes/limpar-logs/',
        views.automation_clear_thoughts,
        name='automation_clear_thoughts',
    ),
    path(
        'automacoes/limpar-ledger-replay-ficticio/',
        views.automation_replay_shadow_ledger_clear,
        name='automation_replay_shadow_ledger_clear',
    ),
    path(
        'automacoes/perfis/selecionar/',
        views.automation_profile_select,
        name='automation_profile_select',
    ),
    path(
        'automacoes/perfis/criar/',
        views.automation_profile_create,
        name='automation_profile_create',
    ),
    path(
        'automacoes/perfis/iniciar/',
        views.automation_profile_start,
        name='automation_profile_start',
    ),
    path(
        'automacoes/sim-replay-cursor/',
        views.automation_sim_replay_cursor,
        name='automation_sim_replay_cursor',
    ),
    path(
        'automacoes/replay-dia.json',
        views.automation_replay_day_json,
        name='automation_replay_day_json',
    ),
    path(
        'automacoes/replay-stream/',
        views.automation_replay_stream_start,
        name='automation_replay_stream_start',
    ),
    path('ambiente/selecionar/', views.set_trading_environment, name='set_trading_environment'),
    path('mercado/snapshot.json', views.market_snapshot_json, name='market_snapshot_json'),
    path(
        'automacoes/boleta/auto-stop.json',
        boleta_auto_stop_suggest,
        name='boleta_auto_stop_suggest',
    ),
    path('mercado/quote-history.json', views.quote_history_json, name='quote_history_json'),
    path('mercado/candles.json', views.quote_candles_json, name='quote_candles_json'),
    path(
        'mercado/candles-session-dates.json',
        views.quote_candles_session_dates_json,
        name='quote_candles_session_dates_json',
    ),
    path('mercado/', views.market_quote, name='market_quote'),
    path('ordens/cancelar/', views.cancel_order, name='cancel_order'),
    path('market/daytrade-candidates-save/', views.save_daytrade_candidates, name='save_daytrade_candidates'),
    path('market/watch-tickers-save/', views.save_watch_tickers, name='save_watch_tickers'),
    path('ordens/liquidar-ativo/', views.liquidate_single_asset, name='liquidate_single_asset'),
    path('ordens/liquidar-todos/', views.liquidate_all_assets, name='liquidate_all_assets'),
    path('ordens/painel-parcial/', views.orders_panel_fragment, name='orders_panel_fragment'),
    path(
        'painel/garantias-custodia.html',
        views.collateral_custody_fragment,
        name='collateral_custody_fragment',
    ),
    path('ordens/', views.orders_intraday, name='orders_intraday'),
    path('envio-teste/', views.send_order_test, name='send_order_test'),
    path('testes/celery/', views.celery_tests, name='celery_tests'),
    path('testes/celery.json', views.celery_tests_json, name='celery_tests_json'),
    path('liquidacoes/historico/', views.liquidation_history, name='liquidation_history'),
]
