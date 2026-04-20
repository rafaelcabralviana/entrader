def load_all() -> None:
    from importlib import import_module

    import_module('trader.automacoes.strategies_plugins.stop_percentual_book')
    import_module('trader.automacoes.strategies_plugins.janela_pregao')
    import_module('trader.automacoes.strategies_plugins.ts_signals_stub')
    import_module('trader.automacoes.strategies_plugins.ts_risk_stub')
    import_module('trader.automacoes.strategies_plugins.leafar')
    import_module('trader.automacoes.strategies_plugins.comentario_preco_intradia')
    import_module('trader.automacoes.strategies_plugins.teste_limite_preco_34')
    import_module('trader.automacoes.strategies_plugins.tendencia_mercado')
    import_module('trader.automacoes.strategies_plugins.tendencia_mercado_ativa')
    import_module('trader.automacoes.strategies_plugins.perfil_volume_montanhas')
