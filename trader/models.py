from django.conf import settings
from django.db import models
from django.utils import timezone as django_timezone


class PositionQuerySet(models.QuerySet):
    def active(self):
        return self.filter(is_active=True)

    def closed(self):
        return self.filter(is_active=False)


class PositionManager(models.Manager.from_queryset(PositionQuerySet)):
    pass


class QuoteSnapshot(models.Model):
    ticker = models.CharField(max_length=16, db_index=True)
    # default=now (não auto_now_add): no admin você define o pregão; auto_now_add forçava «agora» no save.
    captured_at = models.DateTimeField(default=django_timezone.now, db_index=True)
    quote_data = models.JSONField()
    quote_event_at = models.DateTimeField(null=True, blank=True)
    latency_ms = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ['-captured_at']
        indexes = [
            models.Index(fields=['ticker', '-captured_at']),
        ]

    def __str__(self) -> str:
        return f'{self.ticker} @ {self.captured_at.isoformat()}'


class BookSnapshot(models.Model):
    ticker = models.CharField(max_length=16, db_index=True)
    captured_at = models.DateTimeField(default=django_timezone.now, db_index=True)
    book_data = models.JSONField()

    class Meta:
        ordering = ['-captured_at']
        indexes = [
            models.Index(fields=['ticker', '-captured_at']),
        ]

    def __str__(self) -> str:
        return f'{self.ticker} (book) @ {self.captured_at.isoformat()}'


class FeatureSnapshot(models.Model):
    ticker = models.CharField(max_length=16, db_index=True)
    as_of_ts_ms = models.BigIntegerField(db_index=True)
    schema_version = models.PositiveSmallIntegerField(default=1)
    regime = models.CharField(max_length=32, blank=True, db_index=True)
    features = models.JSONField(default=dict)
    source_quote = models.ForeignKey(
        'QuoteSnapshot',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='feature_snapshots',
    )
    source_book = models.ForeignKey(
        'BookSnapshot',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='feature_snapshots',
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['ticker', 'as_of_ts_ms']),
        ]
        verbose_name = 'Snapshot de features'
        verbose_name_plural = 'Snapshots de features'

    def __str__(self) -> str:
        return f'{self.ticker} features @ {self.as_of_ts_ms}'


class WatchedTicker(models.Model):
    """Tickers monitorados pelo Celery para coleta contínua de quote."""

    ticker = models.CharField(max_length=16, unique=True, db_index=True)
    enabled = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['ticker']
        verbose_name = 'Ticker monitorado'
        verbose_name_plural = 'Tickers monitorados'

    def save(self, *args, **kwargs):
        self.ticker = (self.ticker or '').strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.ticker


class Position(models.Model):
    class Side(models.TextChoices):
        LONG = 'LONG', 'Comprado'
        SHORT = 'SHORT', 'Vendido'

    class TradingEnvironment(models.TextChoices):
        SIMULATOR = 'simulator', 'Simulador'
        REAL = 'real', 'Real'
        REPLAY = 'replay', 'Replay'

    class Lane(models.TextChoices):
        """Ledger local: custódia API vs replay (preços das velas, sem corretora)."""

        STANDARD = 'standard', 'API / simulador (custódia)'
        REPLAY_SHADOW = 'replay_shadow', 'Replay fictício (sessão)'

    ticker = models.CharField(max_length=16, db_index=True)
    trading_environment = models.CharField(
        max_length=16,
        choices=TradingEnvironment.choices,
        default=TradingEnvironment.SIMULATOR,
        db_index=True,
    )
    position_lane = models.CharField(
        max_length=24,
        choices=Lane.choices,
        default=Lane.STANDARD,
        db_index=True,
    )
    side = models.CharField(max_length=5, choices=Side.choices, db_index=True)
    quantity_open = models.DecimalField(max_digits=18, decimal_places=6)
    avg_open_price = models.DecimalField(max_digits=18, decimal_places=6)
    opened_at = models.DateTimeField(db_index=True)
    closed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    objects = PositionManager()

    class Meta:
        ordering = ['-opened_at']
        indexes = [
            models.Index(fields=['ticker', 'is_active']),
            models.Index(
                fields=['ticker', 'is_active', 'trading_environment'],
                name='trader_pos_ticker_act_env_idx',
            ),
            models.Index(
                fields=['ticker', 'is_active', 'trading_environment', 'position_lane'],
                name='trader_pos_env_lane_act_idx',
            ),
            models.Index(fields=['side', 'is_active']),
        ]
        verbose_name = 'Posição'
        verbose_name_plural = 'Posições'

    def save(self, *args, **kwargs):
        self.ticker = (self.ticker or '').strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        status = 'aberta' if self.is_active else 'encerrada'
        return f'{self.ticker} {self.side} ({status})'

    @property
    def liquidation_direction(self) -> str:
        if self.side == self.Side.LONG:
            return PositionLiquidation.Direction.SELL
        return PositionLiquidation.Direction.BUY


class PositionLiquidation(models.Model):
    class LiquidationMode(models.TextChoices):
        FULL = 'FULL', 'Zeragem total'
        PARTIAL = 'PARTIAL', 'Zeragem parcial'

    class Direction(models.TextChoices):
        BUY = 'BUY', 'Compra'
        SELL = 'SELL', 'Venda'

    position = models.ForeignKey(
        Position,
        on_delete=models.PROTECT,
        related_name='liquidations',
    )
    mode = models.CharField(max_length=8, choices=LiquidationMode.choices, db_index=True)
    direction = models.CharField(max_length=4, choices=Direction.choices)
    quantity = models.DecimalField(max_digits=18, decimal_places=6)
    price = models.DecimalField(max_digits=18, decimal_places=6)
    executed_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-executed_at']
        indexes = [
            models.Index(fields=['mode', 'executed_at']),
            models.Index(fields=['direction', 'executed_at']),
        ]
        verbose_name = 'Liquidação de posição'
        verbose_name_plural = 'Liquidações de posição'

    def __str__(self) -> str:
        return f'{self.position.ticker} {self.direction} {self.quantity}'


class ClosedOperation(models.Model):
    class PnLType(models.TextChoices):
        REALIZED = 'REALIZED', 'Realizado'
        ESTIMATED = 'ESTIMATED', 'Estimado'

    position = models.OneToOneField(
        Position,
        on_delete=models.PROTECT,
        related_name='closed_operation',
    )
    pnl_type = models.CharField(max_length=9, choices=PnLType.choices, db_index=True)
    gross_pnl = models.DecimalField(max_digits=18, decimal_places=6)
    fees = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    net_pnl = models.DecimalField(max_digits=18, decimal_places=6)
    notes = models.TextField(blank=True)
    closed_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-closed_at']
        indexes = [
            models.Index(fields=['pnl_type', 'closed_at']),
            models.Index(fields=['net_pnl', 'closed_at']),
        ]
        verbose_name = 'Operação finalizada'
        verbose_name_plural = 'Operações finalizadas'

    def __str__(self) -> str:
        return f'{self.position.ticker} | PnL líquido: {self.net_pnl}'


class TradeMarker(models.Model):
    class Side(models.TextChoices):
        BUY = 'BUY', 'Compra'
        SELL = 'SELL', 'Venda'

    ticker = models.CharField(max_length=16, db_index=True)
    side = models.CharField(max_length=4, choices=Side.choices, db_index=True)
    quantity = models.DecimalField(max_digits=18, decimal_places=6)
    price = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    marker_at = models.DateTimeField(db_index=True)
    source = models.CharField(max_length=32, blank=True, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-marker_at']
        indexes = [
            models.Index(fields=['ticker', 'marker_at']),
            models.Index(fields=['side', 'marker_at']),
        ]
        verbose_name = 'Marcação de trade'
        verbose_name_plural = 'Marcações de trade'

    def save(self, *args, **kwargs):
        self.ticker = (self.ticker or '').strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f'{self.ticker} {self.side} @ {self.marker_at.isoformat()}'


class AutomationStrategyToggle(models.Model):
    """
    Preferência por usuário: estratégia ativa ou não, separada por ambiente (simulador/real),
    alinhado a ``Position.trading_environment``.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_strategy_toggles',
    )
    execution_profile = models.ForeignKey(
        'AutomationExecutionProfile',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name='strategy_toggles',
    )
    strategy_key = models.CharField(max_length=64, db_index=True)
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    enabled = models.BooleanField(default=False)
    execute_orders = models.BooleanField(default=False)
    params_json = models.JSONField(default=dict, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'strategy_key', 'trading_environment', 'execution_profile'],
                name='trader_automation_strat_user_key_env_profile_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['user', 'trading_environment', 'enabled']),
        ]
        verbose_name = 'Estratégia de automação (toggle)'
        verbose_name_plural = 'Estratégias de automação (toggles)'

    def __str__(self) -> str:
        return f'{self.user_id} {self.strategy_key} ({self.trading_environment})'


class AutomationTrailingStopPreference(models.Model):
    """
    Preferência global (por utilizador, ambiente e perfil de execução) para o ajuste
    automático de stop (trailing) após o bracket definido pelas estratégias.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_trailing_stop_preferences',
    )
    execution_profile = models.ForeignKey(
        'AutomationExecutionProfile',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name='trailing_stop_preferences',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    adjustment_enabled = models.BooleanField(
        default=True,
        help_text='Se falso, não se aplica trailing ao stop-limit após entrada (leafaR, tendência ativa, etc.).',
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'trading_environment', 'execution_profile'],
                name='trader_auto_trailing_pref_user_env_profile_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['user', 'trading_environment']),
        ]
        verbose_name = 'Preferência de trailing stop (automações)'
        verbose_name_plural = 'Preferências de trailing stop (automações)'

    def __str__(self) -> str:
        return f'{self.user_id} trailing={self.adjustment_enabled} ({self.trading_environment})'


class AutomationThought(models.Model):
    """
    Linha de “pensamento” / log da automação (análises, decisões, eventos) por usuário e ambiente.
    """

    class Kind(models.TextChoices):
        INFO = 'info', 'Info'
        NOTICE = 'notice', 'Aviso'
        WARN = 'warn', 'Alerta'
        DEBUG = 'debug', 'Debug'

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_thoughts',
    )
    execution_profile = models.ForeignKey(
        'AutomationExecutionProfile',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='thoughts',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    message = models.TextField()
    source = models.CharField(max_length=96, blank=True, db_index=True)
    kind = models.CharField(
        max_length=16,
        choices=Kind.choices,
        default=Kind.INFO,
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'trading_environment', '-created_at']),
            models.Index(fields=['user', 'trading_environment', 'id']),
            models.Index(fields=['execution_profile', 'id']),
        ]
        verbose_name = 'Pensamento (log de automação)'
        verbose_name_plural = 'Pensamentos (logs de automação)'

    def __str__(self) -> str:
        return f'{self.user_id} #{self.pk} {self.created_at.isoformat()}'


class AutomationMarketSimPreference(models.Model):
    """
    Espelho da simulação de mercado (pregão salvo) para o worker Celery.

    A UI continua a usar a sessão; ao activar/desactivar a simulação ou abrir
    ``/automacoes/``, o estado é sincronizado aqui para estratégias poderem
    ler snapshots do dia escolhido sem depender da sessão HTTP.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_market_sim_preferences',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    enabled = models.BooleanField(default=False, db_index=True)
    session_date = models.DateField(null=True, blank=True)
    sim_ticker = models.CharField(max_length=16, blank=True)
    replay_until = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Instante máximo dos snapshots incluídos (scrubber de replay); vazio = dia completo.',
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'trading_environment'],
                name='trader_automation_sim_pref_user_env_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['user', 'trading_environment', 'enabled']),
        ]
        verbose_name = 'Preferência de simulação de mercado (worker)'
        verbose_name_plural = 'Preferências de simulação de mercado (worker)'

    def __str__(self) -> str:
        return f'{self.user_id} sim={self.enabled} {self.sim_ticker} {self.session_date}'


class AutomationRuntimePreference(models.Model):
    """
    Liga/desliga o robô de automações por utilizador e ambiente.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_runtime_preferences',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    enabled = models.BooleanField(default=True, db_index=True)
    max_open_operations = models.PositiveSmallIntegerField(
        default=1,
        help_text='Máximo de operações abertas simultâneas para novas entradas das estratégias ativas.',
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'trading_environment'],
                name='trader_auto_runtime_pref_user_env_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['user', 'trading_environment', 'enabled']),
        ]
        verbose_name = 'Preferência de execução do robô'
        verbose_name_plural = 'Preferências de execução do robô'

    def __str__(self) -> str:
        return f'{self.user_id} runtime={self.enabled} ({self.trading_environment})'


class AutomationExecutionProfile(models.Model):
    """
    Perfil de execução de automações por utilizador e ambiente.

    ``Tempo_Real`` é o perfil base; perfis de simulação guardam ticker/dia/replay.
    """

    class Mode(models.TextChoices):
        REAL_TIME = 'real_time', 'Tempo real'
        SIMULATION = 'simulation', 'Simulação'

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_execution_profiles',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    name = models.CharField(max_length=64)
    mode = models.CharField(
        max_length=16,
        choices=Mode.choices,
        default=Mode.REAL_TIME,
        db_index=True,
    )
    is_active = models.BooleanField(default=False, db_index=True)
    is_system_default = models.BooleanField(default=False, db_index=True)
    live_ticker = models.CharField(max_length=16, blank=True)
    sim_ticker = models.CharField(max_length=16, blank=True)
    session_date = models.DateField(null=True, blank=True)
    replay_until = models.DateTimeField(null=True, blank=True)
    execution_started_at = models.DateTimeField(null=True, blank=True)
    last_runtime_cursor_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'trading_environment', 'name'],
                name='trader_auto_exec_profile_user_env_name_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['user', 'trading_environment', 'is_active']),
            models.Index(fields=['user', 'trading_environment', 'mode']),
        ]
        verbose_name = 'Perfil de execução (automações)'
        verbose_name_plural = 'Perfis de execução (automações)'

    def save(self, *args, **kwargs):
        self.name = (self.name or '').strip()[:64] or 'Perfil'
        self.live_ticker = (self.live_ticker or '').strip().upper()
        self.sim_ticker = (self.sim_ticker or '').strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f'{self.user_id} {self.trading_environment} {self.name}'


class AutomationTriggerMarker(models.Model):
    """
    Marcação visual de disparo de estratégia para render no gráfico.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='automation_trigger_markers',
    )
    execution_profile = models.ForeignKey(
        AutomationExecutionProfile,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='trigger_markers',
    )
    trading_environment = models.CharField(
        max_length=16,
        choices=Position.TradingEnvironment.choices,
        db_index=True,
    )
    ticker = models.CharField(max_length=16, db_index=True)
    strategy_key = models.CharField(max_length=96, db_index=True)
    marker_at = models.DateTimeField(db_index=True)
    price = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-marker_at']
        indexes = [
            models.Index(fields=['user', 'trading_environment', 'ticker', 'marker_at']),
            models.Index(fields=['execution_profile', 'ticker', 'marker_at']),
        ]
        verbose_name = 'Marcação de disparo (automações)'
        verbose_name_plural = 'Marcações de disparo (automações)'

    def save(self, *args, **kwargs):
        self.ticker = (self.ticker or '').strip().upper()
        self.strategy_key = (self.strategy_key or '').strip()[:96]
        super().save(*args, **kwargs)
