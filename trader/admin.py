from django.contrib import admin, messages
from django.contrib.admin import DateFieldListFilter
from django.utils import timezone
from zoneinfo import ZoneInfo

from trader.services.quote_history import _parse_quote_event_datetime
from trader.models import (
    AutomationStrategyToggle,
    AutomationThought,
    FeatureSnapshot,
    QuoteSnapshot,
    BookSnapshot,
    WatchedTicker,
    Position,
    PositionLiquidation,
    ClosedOperation,
    TradeMarker,
)


@admin.register(QuoteSnapshot)
class QuoteSnapshotAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'captured_at', 'quote_event_at', 'latency_ms')
    list_filter = (
        'ticker',
        ('captured_at', DateFieldListFilter),
    )
    date_hierarchy = 'captured_at'
    search_fields = ('ticker',)
    ordering = ('-captured_at',)

    _TZ_SP = ZoneInfo('America/Sao_Paulo')

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        fld = super().formfield_for_dbfield(db_field, request, **kwargs)
        if db_field.name == 'captured_at' and fld is not None:
            fld.help_text = (
                'Define em qual DIA o gráfico de candles agrupa este ponto (calendário BRT). '
                'Alinhe ao pregão desejado; o dateTime dentro de «Quote data» não substitui este campo.'
            )
        if db_field.name == 'quote_data' and fld is not None:
            fld.help_text = (
                'lastPrice, OHLC, status, etc. O eixo temporal do gráfico vem de «Captured at», '
                'não dos timestamps dentro deste JSON.'
            )
        return fld

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        q = obj.quote_data
        if not isinstance(q, dict):
            return
        evt = _parse_quote_event_datetime(q)
        cap = obj.captured_at
        if evt is None or cap is None:
            return
        if timezone.is_naive(evt):
            evt = timezone.make_aware(evt, timezone.get_current_timezone())
        if timezone.is_naive(cap):
            cap = timezone.make_aware(cap, timezone.get_current_timezone())
        d_evt = evt.astimezone(self._TZ_SP).date()
        d_cap = cap.astimezone(self._TZ_SP).date()
        if d_cap != d_evt:
            self.message_user(
                request,
                (
                    f'Data BRT de «Captured at» ({d_cap.isoformat()}) difere da data BRT do evento '
                    f'em dateTime/tradeDateTime ({d_evt.isoformat()}). O gráfico filtra por «Captured at»; '
                    f'no select «Dia» use {d_cap.isoformat()} (ou corrija «Captured at»).'
                ),
                level=messages.WARNING,
            )


@admin.register(WatchedTicker)
class WatchedTickerAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'enabled', 'updated_at')
    list_filter = ('enabled',)
    search_fields = ('ticker',)
    ordering = ('ticker',)


@admin.register(BookSnapshot)
class BookSnapshotAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'captured_at')
    list_filter = (
        'ticker',
        ('captured_at', DateFieldListFilter),
    )
    date_hierarchy = 'captured_at'
    search_fields = ('ticker',)
    ordering = ('-captured_at',)


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    list_display = (
        'ticker',
        'trading_environment',
        'position_lane',
        'side',
        'quantity_open',
        'avg_open_price',
        'is_active',
        'opened_at',
        'closed_at',
    )
    list_filter = (
        'side',
        'is_active',
        'trading_environment',
        'position_lane',
        ('opened_at', DateFieldListFilter),
    )
    date_hierarchy = 'opened_at'
    search_fields = ('ticker',)
    ordering = ('-opened_at',)


@admin.register(PositionLiquidation)
class PositionLiquidationAdmin(admin.ModelAdmin):
    list_display = ('position', 'mode', 'direction', 'quantity', 'price', 'executed_at')
    list_filter = (
        'mode',
        'direction',
        ('executed_at', DateFieldListFilter),
    )
    date_hierarchy = 'executed_at'
    search_fields = ('position__ticker',)
    ordering = ('-executed_at',)


@admin.register(ClosedOperation)
class ClosedOperationAdmin(admin.ModelAdmin):
    list_display = ('position', 'pnl_type', 'gross_pnl', 'fees', 'net_pnl', 'closed_at')
    list_filter = (
        'pnl_type',
        ('closed_at', DateFieldListFilter),
    )
    date_hierarchy = 'closed_at'
    search_fields = ('position__ticker',)
    ordering = ('-closed_at',)


@admin.register(AutomationThought)
class AutomationThoughtAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'trading_environment', 'kind', 'source', 'created_at')
    list_filter = (
        'trading_environment',
        'kind',
        'source',
        ('created_at', DateFieldListFilter),
    )
    date_hierarchy = 'created_at'
    search_fields = ('message', 'source', 'user__username')
    ordering = ('-created_at',)
    readonly_fields = ('created_at',)


@admin.register(AutomationStrategyToggle)
class AutomationStrategyToggleAdmin(admin.ModelAdmin):
    list_display = ('user', 'strategy_key', 'trading_environment', 'enabled', 'updated_at')
    list_filter = ('trading_environment', 'enabled', 'strategy_key')
    search_fields = ('user__username', 'strategy_key')
    ordering = ('-updated_at',)


@admin.register(FeatureSnapshot)
class FeatureSnapshotAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'as_of_ts_ms', 'schema_version', 'regime', 'created_at')
    list_filter = ('ticker', 'schema_version', ('created_at', DateFieldListFilter))
    search_fields = ('ticker', 'regime')
    ordering = ('-created_at',)
    readonly_fields = ('created_at',)


@admin.register(TradeMarker)
class TradeMarkerAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'side', 'quantity', 'price', 'marker_at', 'source')
    list_filter = (
        'side',
        'source',
        ('marker_at', DateFieldListFilter),
    )
    date_hierarchy = 'marker_at'
    search_fields = ('ticker',)
    ordering = ('-marker_at',)
