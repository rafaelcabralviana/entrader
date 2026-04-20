from __future__ import annotations

# Enumerações alinhadas à documentação Smart Trader API.

# OrderModule
ORDER_MODULE_DAY_TRADE = 'DayTrade'
ORDER_MODULE_SWING_TRADE = 'SwingTrade'
ORDER_MODULES: frozenset[str] = frozenset(
    {ORDER_MODULE_DAY_TRADE, ORDER_MODULE_SWING_TRADE}
)

# OrderType (rótulos canônicos da API)
ORDER_TYPE_MARKET = 'Market'
ORDER_TYPE_LIMIT = 'Limit'
ORDER_TYPE_STOP_LIMIT = 'StopLimit'

# Valores internos usados na boleta/comando (compatibilidade)
ORDER_TYPE_MARKET_INTERNAL = 'market'
ORDER_TYPE_LIMIT_INTERNAL = 'limited'
ORDER_TYPE_STOP_LIMIT_INTERNAL = 'stop-limit'
ORDER_TYPES_INTERNAL: frozenset[str] = frozenset(
    {
        ORDER_TYPE_MARKET_INTERNAL,
        ORDER_TYPE_LIMIT_INTERNAL,
        ORDER_TYPE_STOP_LIMIT_INTERNAL,
    }
)

# OrderSide
ORDER_SIDE_BUY = 'Buy'
ORDER_SIDE_SELL = 'Sell'
ORDER_SIDES: frozenset[str] = frozenset({ORDER_SIDE_BUY, ORDER_SIDE_SELL})

# OrderTimeInForce
ORDER_TIF_DAY = 'Day'
ORDER_TIF_IOC = 'ImmediateOrCancel'
ORDER_TIF_FOK = 'FillOrKill'
ORDER_TIME_IN_FORCE_VALUES: frozenset[str] = frozenset(
    {ORDER_TIF_DAY, ORDER_TIF_IOC, ORDER_TIF_FOK}
)
