"""
Catálogo de estratégias de automação (chaves estáveis para persistência e código).

Novas entradas: ``key``, ``title``, ``description``, ``group``; opcionalmente
``automation_role`` = ``active`` (dispara ou condiciona operações) ou ``passive`` (só informa);
``celery_scope`` = ``once`` (um contexto por tick, ticker primário ao vivo ou replay)
ou ``per_ticker`` (repete para cada símbolo do watch); opcionalmente ``config_items``
(lista de ``{"name": "...", "detail": "..."}``) para o modal de ajuda na barra lateral.
Registar ``evaluate`` e, se precisar de efeitos no worker, ``register_celery_tick`` em ``strategies_plugins``.
"""

from __future__ import annotations

from typing import Any, TypedDict


class _StrategyDefBase(TypedDict):
    key: str
    title: str
    description: str
    group: str


class _StrategyDefOptional(TypedDict, total=False):
    celery_scope: str
    config_items: list[dict[str, str]]
    automation_role: str  # 'active' — operações / condiciona envio; 'passive' — só informa


class StrategyDef(_StrategyDefBase, _StrategyDefOptional):
    """Catálogo em código; campos opcionais em ``_StrategyDefOptional``."""


AUTOMATION_STRATEGIES: list[StrategyDef] = [
    {
        'key': 'stop_percentual_book',
        'group': 'legacy',
        'automation_role': 'active',
        'title': 'Stop por percentual (referência book/último)',
        'description': (
            'Usa a mesma ideia de preço de referência da boleta (último, bid, ask, meio) '
            'para definir ou ajustar stops automaticamente. Respeita o ambiente ativo '
            '(Simulador ou Real) ao executar.'
        ),
        'config_items': [
            {
                'name': '(estado atual)',
                'detail': (
                    'O avaliador está ligado ao motor, mas a lógica de percentual sobre book/último '
                    'ainda não expõe variáveis TRADER_* neste repositório; configure stops manuais ou '
                    'boleta automática conforme a documentação interna.'
                ),
            },
        ],
    },
    {
        'key': 'janela_pregao',
        'group': 'legacy',
        'automation_role': 'active',
        'title': 'Operar só na janela do pregão',
        'description': (
            'Restringe envio de ordens automáticas ao horário de pregão detectado nos dados '
            '(ex.: status da quote). Útil antes de ligar execução real.'
        ),
        'config_items': [
            {
                'name': 'Comportamento em fim de pregão',
                'detail': (
                    'Quando a quote indica pregão encerrado, regista um pensamento no log (no máximo '
                    'uma vez por hora por utilizador e ambiente) a avisar para não enviar ordens até à próxima sessão.'
                ),
            },
        ],
    },
    {
        'key': 'comentario_preco_intradia',
        'group': 'legacy',
        'automation_role': 'passive',
        'title': 'Comentário de preço (intradiário)',
        'description': (
            'Gera texto de leitura aproximada da «forma» do preço (tendência, posição na faixa, '
            'consolidação/oscilação) usando só candles já gravados no dia — ao vivo ou replay. '
            'Limite de frequência no log (~1/min por ticker). Não é recomendação de investimento.'
        ),
        'config_items': [
            {
                'name': 'Alerta de alta ≥3%',
                'detail': (
                    'Acrescenta `[Alta ≥3%]` se o último fecho subir ≥3% face à **1ª abertura** da série '
                    '**ou** face à **mínima (lows)** das barras enviadas ao motor (útil no replay quando '
                    'a janela recente já abre alto mas o dia teve fundos mais baixos). O motor passa a '
                    'receber até ~3200 barras/dia com 1ª barra preservada na truncagem.'
                ),
            },
            {
                'name': 'Frequência',
                'detail': (
                    'Anti-spam em cache (~55 s) por utilizador, ambiente, ticker, sessão e «instante» da série '
                    '(número de candles + último fecho); quando o replay ou o mercado mudam esse instante, '
                    'pode sair novo comentário antes de expirar a janela.'
                ),
            },
        ],
    },
    {
        'key': 'leafar',
        'group': 'legacy',
        'automation_role': 'active',
        'title': 'leafaR — perfil de volume (mean reversion)',
        'description': (
            'Detecta acúmulo de volume (POC) afastado do preço com «corredor» de baixo volume; '
            'opera reversão à média (compra ou venda), TP no POC, SL na extremidade com baixo volume. '
            'Execução e ajuste de stop (trailing) acionam no worker de cotações (Celery); '
            'o trailing é centralizado em ``universal_bracket_trailing`` e pode ser desligado '
            'no painel («Ajuste automático de stop»). '
            'Só lê ``QuoteSnapshot`` (o que o Celery grava): ao vivo e simulação = **todo o dia civil em BRT** '
            'até ao instante actual (no replay, ``replay_until`` alinhado ao scrubber). A cada ciclo do watch reavalia com critérios '
            'aproximados (ajuste TRADER_LEAFAR_VP_CORRIDOR_RATIO, TRADER_LEAFAR_TREND_MIN_FRAC, '
            'TRADER_LEAFAR_TREND_WINDOW, TRADER_LEAFAR_MIN_PRICE_SEP_FRAC, TRADER_LEAFAR_SESSION_LOCAL_SEP_FRAC, '
            'TRADER_LEAFAR_MIN_CANDLES, '
            'TRADER_LEAFAR_VP_BINS). Ordens: TRADER_LEAFAR_SEND_ORDERS. No **replay** do simulador o bracket é '
            'fictício (preço da vela, ledger ``replay_shadow``); ao vivo usa a API.'
        ),
        'celery_scope': 'per_ticker',
        'config_items': [
            {
                'name': 'TRADER_LEAFAR_ENABLED',
                'detail': 'Liga ou desliga o processamento leafaR no worker (predefinição: True).',
            },
            {
                'name': 'TRADER_LEAFAR_SEND_ORDERS',
                'detail': (
                    'Obsoleto: o envio é controlado pelo checkbox «executar ordem» da estratégia '
                    'e pelo botão global do robô por ambiente (Real/Simulador).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_EXEC_ON_SESSION_REPLAY',
                'detail': 'Obsoleto: o replay no simulador usa sempre bracket fictício (sem API). Ignorado.',
            },
            {
                'name': 'TRADER_LEAFAR_QUANTITY',
                'detail': 'Quantidade por entrada (inteiro, limitado entre 1 e 100).',
            },
            {
                'name': 'TRADER_LEAFAR_COOLDOWN_SEC',
                'detail': 'Segundos de bloqueio entre sinais iguais por símbolo/sessão (cache).',
            },
            {
                'name': 'TRADER_BRACKET_SL / TP / trailing (STOP, MFE, lock lucro, TP)',
                'detail': (
                    'Globais para **todas** as estratégias com bracket: após o cálculo do sinal, alarga SL e TP '
                    'em relação ao último (predef.: SL ×2,0 e TP ×4,0; limites 1–8). '
                    '``TRADER_TRAILING_STOP_TICKS``: passo (predef. 12). '
                    '``TRADER_TRAILING_MIN_FAVORABLE_TICKS``: ticks a favor antes de apertar (predef. 16; 0=desliga). '
                    '``TRADER_TRAILING_PROTECTION_FLOOR_TICKS``: folga mínima entrada↔gatilho (predef. 10; 0=desliga). '
                    '``TRADER_TRAILING_LOCK_PROFIT_ARM_PCT`` / ``TRADER_TRAILING_LOCK_PROFIT_FLOOR_PCT``: após lucro '
                    'máximo a favor ≥ arm (predef. 3 %), garante lucro mínimo floor (predef. 1 %) no SL. '
                    '``TRADER_TRAILING_TP_FOLLOW_PEAK_TICKS``: TP limite acompanha pico/vale (predef. 6; 0=desliga). '
                    'Definir no .env ou em ``settings``.'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_VP_BINS',
                'detail': 'Número de bins do perfil de volume (8–64, predefinição 24).',
            },
            {
                'name': 'TRADER_LEAFAR_MIN_BINS_FROM_POC',
                'detail': 'Bins mínimos entre preço e POC para considerar afastamento.',
            },
            {
                'name': 'TRADER_LEAFAR_VP_CORRIDOR_RATIO',
                'detail': 'Rácio do «corredor» de baixo volume em torno do POC (aprox. 0,05–0,95).',
            },
            {
                'name': 'TRADER_LEAFAR_MIN_CANDLES',
                'detail': 'Mínimo de candles na amostra antes de avaliar (≥20; predefinição 42).',
            },
            {
                'name': 'TRADER_LEAFAR_MIN_SESSION_MINUTES',
                'detail': (
                    'Minutos mínimos decorridos desde a 1ª barra da sessão para permitir sinal '
                    '(0 desliga; predefinição 18). Evita entrada cedo demais no abrir do pregão.'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_TREND_WINDOW',
                'detail': 'Janela (em barras) para medir tendência aproximada (3–20).',
            },
            {
                'name': 'TRADER_LEAFAR_TREND_BIAS_ENABLED / MIN_SCORE',
                'detail': (
                    'Peso da tendência de mercado na execução: quando ativo, sinal Buy favorece tendência Alta '
                    'e Sell favorece Baixa. Se a tendência forte vier contra o sinal (|score| ≥ MIN_SCORE), '
                    'a leafaR bloqueia envio de ordem e registra aviso.'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_TREND_MIN_FRAC',
                'detail': 'Fração mínima de alinhamento para considerar tendência (0,35–0,95; predefinição 0,48).',
            },
            {
                'name': 'TRADER_LEAFAR_MIN_PRICE_SEP_FRAC',
                'detail': (
                    'Fração base da separação preço–POC sobre a **amplitude do dia** (máx.−mín. nas velas); '
                    'combinada com TRADER_LEAFAR_SESSION_LOCAL_SEP_FRAC (borda mais próxima).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_SESSION_LOCAL_SEP_FRAC',
                'detail': (
                    'Fração mínima |preço−POC| sobre a distância do último à **borda da sessão mais próxima** '
                    '(com piso em ticks). Junto com MIN_PRICE_SEP_FRAC define o mínimo absoluto aceite — '
                    '«perto/longe» da formação em relação ao dia. Predef. 0,22 (0,05–0,55).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_POC_STABILITY_BARS',
                'detail': (
                    'Confirma estabilidade do POC (#1) nas últimas barras antes de sinalizar '
                    '(1 desliga; padrão 2, cautela moderada).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_POC_DOMINANCE_RATIO',
                'detail': (
                    'Relação mínima vol(#1)/vol(#2) do VP para aceitar sinal '
                    '(padrão 1,08; maior = mais seletivo).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_PERSISTENCE_BARS',
                'detail': (
                    'Barras seguidas com preço do mesmo lado do POC antes de entrar '
                    '(1 desliga; padrão 2).'
                ),
            },
            {
                'name': 'TRADER_LEAFAR_MIN_RECENT_RANGE_TICKS',
                'detail': (
                    'Range mínimo recente (em ticks) para evitar entradas em lateral estreita '
                    '(0 desliga; padrão 8).'
                ),
            },
        ],
    },
    {
        'key': 'teste_limite_preco_34',
        'group': 'legacy',
        'automation_role': 'passive',
        'title': 'Teste — limiar 34,11',
        'description': (
            'Validação: regista no log quando o preço atinge 34,11 ou mais. '
            'Usa primeiro o fecho da última vela agregada no motor (o mesmo «Último» '
            'do gráfico compacto); se não houver velas, usa o último snapshot de cotação. '
            'Não é recomendação de investimento.'
        ),
        'config_items': [
            {
                'name': 'Limiar',
                'detail': (
                    'Dispara com preço ≥ 34,11. No log aparece como alerta (laranja). '
                    'Anti-spam ~45 s por preço/ticker. No painel Automações use «Só alertas» para filtrar.'
                ),
            },
        ],
    },
    {
        'key': 'tendencia_mercado',
        'group': 'legacy',
        'automation_role': 'passive',
        'title': 'Tendência de mercado',
        'description': (
            'Classifica a tendência **Alta**, **Baixa** ou **Lateralizado** com base nas mesmas '
            'velas OHLC que alimentam o gráfico e o motor (série do dia até ao instante actual ou '
            'até ao replay). Usa inclinação aproximada dos fechos e a amplitude da janela recente; '
            'não é recomendação de investimento.'
        ),
        'config_items': [
            {
                'name': 'Janela de candles',
                'detail': (
                    'No modal pode fixar o nº de velas da análise (12–200). Vazio = padrão original '
                    'automático (equivalente ao critério clássico min(48, max(12, n/3)) quando há barras suficientes).'
                ),
            },
            {
                'name': 'Dados',
                'detail': (
                    'Mesmo conjunto de candles agregados que o painel compacto / motor (intervalo '
                    'TRADER_LEAFAR_INTERVAL_SEC). Com poucas barras o resultado tende a «Lateralizado».'
                ),
            },
            {
                'name': 'Frequência',
                'detail': (
                    'Anti-spam em cache (~48 s) por utilizador, ambiente, ticker, sessão e estado '
                    '(nº de velas, último fecho e rótulo de tendência).'
                ),
            },
        ],
    },
    {
        'key': 'tendencia_mercado_ativa',
        'group': 'legacy',
        'automation_role': 'active',
        'title': 'Tendência de mercado — ativa (curto prazo)',
        'description': (
            'Usa a **mesma leitura** da tendência passiva (Alta/Baixa/Lateral). Quando a força do '
            'sinal é suficiente, pode lançar operação de **curto prazo** com TP e SL proporcionais à '
            'amplitude da janela. O stop de proteção **acompanha** o preço (trailing) para tentar '
            'garantir lucro parcial — como aproximar o stop de 10→11 quando o preço já subiu de 10 '
            'em direção ao alvo. Respeita Simulador/Real e o checkbox «executar ordem». '
            'Não é recomendação de investimento.'
        ),
        # Cada ticker da coleta (antes «once» só no 1.º da lista — ordens não batiam com o gráfico).
        'celery_scope': 'per_ticker',
        'config_items': [
            {
                'name': 'Janela de candles',
                'detail': (
                    'No modal, defina quantas velas usar na análise (mín. 12). Vazio = padrão automático '
                    'igual à estratégia passiva (min(48, max(12, n/3))).'
                ),
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_ENABLED',
                'detail': 'Liga/desliga o processamento (predefinição: True).',
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_SEND_ORDERS',
                'detail': 'Se True, permite envio de ordens reais/simulador quando «executar ordem» está marcado.',
            },
            {
                'name': 'Replay / perfil «Iniciar»',
                'detail': (
                    '**Real:** ordens só com ``live_tail``. **Simulador ao vivo:** API. **Simulador em ``session_replay``:** '
                    'bracket fictício ao preço da vela (ledger ``replay_shadow``), sem envio à corretora. '
                    '``TRADER_TENDENCIA_ATIVA_REQUIRE_PROFILE_STARTED``: True exige «Iniciar»; predefinição: **False**.'
                ),
            },
            {
                'name': 'Celery / tickers',
                'detail': (
                    'A ativa usa ``celery_scope: per_ticker``: corre em **cada** ativo da coleta de cotações, '
                    'não só no primeiro da lista.'
                ),
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_SCORE_THRESHOLD / modal',
                'detail': (
                    'Sem valor no modal e sem setting: a ativa usa o **mesmo** limiar da passiva (0,20), alinhado às setas. '
                    'Defina um número mais baixo só para entradas mais agressivas que o gráfico.'
                ),
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_MAX_SILENCE_SEC',
                'detail': (
                    'Tempo máximo (segundos) **sem** nova marcação da passiva no gráfico; acima disso a ativa '
                    'não dispara ordem. Predef.: 900. **0** desliga. Também no modal «Máx. silêncio».'
                ),
            },
            {
                'name': 'N análises / mín. conferências (modal ou settings)',
                'detail': (
                    'Predefinição no motor: **N=5**, **M=1** (ajustável no modal). '
                    '``trend_vote_k`` (3–15), ``trend_group_hits_required`` (1–N). '
                    'Override: ``TRADER_TENDENCIA_ATIVA_VOTE_K``, ``TRADER_TENDENCIA_ATIVA_GROUP_HITS_REQUIRED``.'
                ),
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_TP_FRAC / SL_FRAC',
                'detail': 'Frações da amplitude da janela para TP e SL (predef.: 0,35 e 0,12).',
            },
            {
                'name': 'TRADER_TENDENCIA_ATIVA_OPPOSITE_COOLDOWN_SEC',
                'detail': (
                    'Segundos mínimos entre uma entrada **Buy** e uma **Sell** (ou o inverso) no mesmo ativo. '
                    'Evita «virar» a posição em ticks seguidos — o P/L pequeno nesse caso **não** é SL curto. '
                    'Predef.: **120**; **0** desliga.'
                ),
            },
            {
                'name': 'TRADER_BRACKET_SL / TP / trailing (ticks)',
                'detail': (
                    'Após TP_FRAC/SL_FRAC, alarga distâncias ao último (predef.: SL ×2,0, TP ×4,0). '
                    'Trailing: ``TRADER_TRAILING_STOP_TICKS``, ``TRADER_TRAILING_MIN_FAVORABLE_TICKS`` (16), '
                    '``TRADER_TRAILING_PROTECTION_FLOOR_TICKS`` (10). Env ou ``settings``.'
                ),
            },
        ],
    },
    {
        'key': 'perfil_volume_montanhas',
        'group': 'legacy',
        'automation_role': 'passive',
        'title': 'Perfil de volume — montanhas (HVN)',
        'description': (
            'Identifica até **três** picos de volume por **máximos locais** no histograma de preço '
            '(«montanhas» separadas por vales), não apenas os três maiores valores globais se caírem '
            'no mesmo maciço. Usa o mesmo VP por sobreposição OHLC que o leafaR '
            '(``TRADER_LEAFAR_VP_BINS``). Mostra preço médio do bin e volume arredondado; '
            'não é recomendação de investimento.'
        ),
        'config_items': [
            {
                'name': 'Dados',
                'detail': (
                    'Candles agregados do motor / gráfico compacto; bins = TRADER_LEAFAR_VP_BINS (8–64).'
                ),
            },
            {
                'name': 'Frequência',
                'detail': (
                    'Anti-spam ~52 s por utilizador, ambiente, ticker, sessão e assinatura das '
                    'três montanhas (preço+volume); alteração no perfil gera nova linha.'
                ),
            },
        ],
    },
    {
        'key': 'ts_signals_stub',
        'group': 'institutional',
        'automation_role': 'active',
        'title': 'Motor de sinal (stub)',
        'description': 'Placeholder institucional; convive com outras estrategias activas.',
        'config_items': [],
    },
    {
        'key': 'ts_risk_stub',
        'group': 'institutional',
        'automation_role': 'active',
        'title': 'Motor de risco (stub)',
        'description': 'Placeholder de risco; convive em simultaneo com as demais chaves.',
        'config_items': [],
    },
]

AUTOMATION_STRATEGY_KEYS: frozenset[str] = frozenset(s['key'] for s in AUTOMATION_STRATEGIES)

PASSIVE_STRATEGY_KEYS: frozenset[str] = frozenset(
    s['key'] for s in AUTOMATION_STRATEGIES if s.get('automation_role') == 'passive'
)


def strategy_display_dict(
    strategy: StrategyDef,
    *,
    enabled: bool,
    execute_orders: bool = False,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Mescla o catálogo com estado de UI (checkbox) e ``config_items`` sempre lista."""
    role = str(strategy.get('automation_role') or 'active').strip().lower()
    if role not in ('active', 'passive'):
        role = 'active'
    return {
        **strategy,
        'automation_role': role,
        'enabled': enabled,
        'execute_orders': bool(execute_orders),
        'params': params or {},
        'config_items': list(strategy.get('config_items') or []),
    }


def is_passive_strategy(key: str) -> bool:
    return (key or '').strip() in PASSIVE_STRATEGY_KEYS


def strategy_by_key(key: str) -> StrategyDef | None:
    k = (key or '').strip()
    for s in AUTOMATION_STRATEGIES:
        if s['key'] == k:
            return s
    return None


def strategy_celery_scope(key: str) -> str:
    """
    ``once`` — um contexto por utilizador/tick (ticker primário ao vivo ou sessão replay).
    ``per_ticker`` — repete para cada símbolo da ronda do watch (ex.: leafaR).
    """
    s = strategy_by_key(key)
    if not s:
        return 'once'
    raw = s.get('celery_scope') if isinstance(s, dict) else None
    v = str(raw or 'once').strip().lower()
    return v if v in ('once', 'per_ticker') else 'once'


def validate_strategy_keys(keys: Any) -> list[str]:
    """Filtra chaves desconhecidas; retorna lista de chaves válidas."""
    if not isinstance(keys, (list, tuple, set, frozenset)):
        return []
    out: list[str] = []
    for x in keys:
        sk = str(x).strip()
        if sk in AUTOMATION_STRATEGY_KEYS and sk not in out:
            out.append(sk)
    return out
