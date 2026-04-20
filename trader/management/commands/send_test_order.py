"""
Envio de ordem de teste no **simulador** (Smart Trader).

Requer ``SMART_TRADER_API_BASE_URL`` apontando para o simulador, credenciais da API
e chave RSA para ``BODY_SIGNATURE``. Opcionalmente chama ``POST /v1/setup/orders``
antes do envio. Use ``open_limited`` para ordem em aberto (sem setup + limitada).

Exemplos::

    python manage.py send_test_order
    python manage.py send_test_order --ticker WDOF26 --side Sell --quantity 1
    python manage.py send_test_order --order-type limited --price 150000
    python manage.py send_test_order --no-setup
    python manage.py send_test_order --dry-run
"""

from __future__ import annotations

import json
import logging

from django.core.management.base import BaseCommand, CommandError

from trader.market_defaults import default_daytrade_win_ticker
from trader.order_enums import (
    ORDER_MODULE_DAY_TRADE,
    ORDER_SIDES,
    ORDER_TIME_IN_FORCE_VALUES,
    ORDER_TIF_DAY,
    ORDER_TYPE_LIMIT_INTERNAL,
    ORDER_TYPE_MARKET_INTERNAL,
    ORDER_TYPES_INTERNAL,
)
from trader.panel_context import ORDER_TEST_SETUP_OPEN_LIMITED
from trader.services.operations_history import (
    infer_execution_price,
    register_trade_execution,
    should_record_local_history,
)
from trader.services.orders import (
    SIMULATOR_SETUP_ORDER_STATUSES,
    post_send_limited_order,
    post_send_market_order,
    post_send_stop_limit_order,
    post_simulator_setup_orders,
)

logger = logging.getLogger(__name__)

_SEND_TEST_SETUP_CHOICES = tuple(
    sorted({ORDER_TEST_SETUP_OPEN_LIMITED, *SIMULATOR_SETUP_ORDER_STATUSES})
)


class Command(BaseCommand):
    help = (
        'Envia uma ordem de teste no ambiente simulado (configura setup + POST de ordem).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--setup',
            choices=_SEND_TEST_SETUP_CHOICES,
            default=ORDER_TEST_SETUP_OPEN_LIMITED,
            help=(
                'filled/rejected = POST /v1/setup/orders. '
                'open_limited = sem setup, ordem limitada com preço que não executa (testar cancelar).'
            ),
        )
        parser.add_argument(
            '--no-setup',
            action='store_true',
            help='Não chama /v1/setup/orders antes do envio.',
        )
        parser.add_argument(
            '--order-type',
            choices=tuple(sorted(ORDER_TYPES_INTERNAL)),
            default=ORDER_TYPE_MARKET_INTERNAL,
            dest='order_type',
            help='Tipo de ordem (default: market).',
        )
        parser.add_argument(
            '--ticker',
            default=None,
            help='Ticker BMF/ação (default: ENTRADE_DEFAULT_WIN_TICKER ou WINJ26).',
        )
        parser.add_argument(
            '--side',
            choices=tuple(sorted(ORDER_SIDES)),
            default='Buy',
            help='Lado da ordem.',
        )
        parser.add_argument(
            '--quantity',
            type=int,
            default=1,
            help='Quantidade (default: 1).',
        )
        parser.add_argument(
            '--time-in-force',
            choices=tuple(sorted(ORDER_TIME_IN_FORCE_VALUES)),
            default=ORDER_TIF_DAY,
            dest='tif',
            help='Time in force (default: Day). Ex.: Day, ImmediateOrCancel, FillOrKill.',
        )
        parser.add_argument(
            '--price',
            type=float,
            default=None,
            help='Preço limite (obrigatório para --order-type limited).',
        )
        parser.add_argument(
            '--stop-trigger',
            type=float,
            default=None,
            dest='stop_trigger',
            help='StopTriggerPrice (obrigatório para stop-limit).',
        )
        parser.add_argument(
            '--stop-order',
            type=float,
            default=None,
            dest='stop_order',
            help='StopOrderPrice (obrigatório para stop-limit).',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Só imprime o corpo e o fluxo; não chama a API.',
        )

    def handle(self, *args, **options):
        ticker = (options['ticker'] or default_daytrade_win_ticker()).strip().upper()
        setup_mode = options['setup']
        if setup_mode == ORDER_TEST_SETUP_OPEN_LIMITED:
            options['no_setup'] = True
            order_type = ORDER_TYPE_LIMIT_INTERNAL
            if options['price'] is None:
                options['price'] = (
                    1.0 if options['side'] == 'Buy' else 999999999.0
                )
        else:
            order_type = options['order_type']
        qty = options['quantity']
        if qty < 1:
            raise CommandError('--quantity deve ser >= 1.')

        if order_type == 'limited' and options['price'] is None:
            raise CommandError('--price é obrigatório para --order-type limited.')
        if order_type == 'stop-limit' and (
            options['stop_trigger'] is None or options['stop_order'] is None
        ):
            raise CommandError(
                '--stop-trigger e --stop-order são obrigatórios para stop-limit.'
            )

        body_market = {
            'Module': ORDER_MODULE_DAY_TRADE,
            'Ticker': ticker,
            'Side': options['side'],
            'Quantity': qty,
            'TimeInForce': options['tif'],
        }

        if order_type == ORDER_TYPE_MARKET_INTERNAL:
            payload = body_market
            send_fn = post_send_market_order
            endpoint_label = 'POST /v1/orders/send/market'
        elif order_type == ORDER_TYPE_LIMIT_INTERNAL:
            payload = {**body_market, 'Price': float(options['price'])}
            send_fn = post_send_limited_order
            endpoint_label = 'POST /v1/orders/send/limited'
        else:
            payload = {
                **body_market,
                'StopTriggerPrice': float(options['stop_trigger']),
                'StopOrderPrice': float(options['stop_order']),
            }
            send_fn = post_send_stop_limit_order
            endpoint_label = 'POST /v1/orders/send/stop-limit'

        if options['dry_run']:
            self.stdout.write(self.style.WARNING('Dry-run (nenhuma chamada HTTP).'))
            if not options['no_setup']:
                self.stdout.write(
                    f'[setup] POST /v1/setup/orders orderStatus={setup_mode}'
                )
            self.stdout.write(endpoint_label)
            self.stdout.write(json.dumps(payload, indent=2, ensure_ascii=False))
            return

        explicit_sim_setup = False
        if not options['no_setup']:
            self.stdout.write(
                f'Configurando simulador: orderStatus={setup_mode} ...'
            )
            try:
                setup_resp = post_simulator_setup_orders(setup_mode)
                explicit_sim_setup = setup_mode in SIMULATOR_SETUP_ORDER_STATUSES
                self.stdout.write(
                    self.style.SUCCESS(f'Setup OK: {json.dumps(setup_resp, ensure_ascii=False)}')
                )
            except Exception as exc:
                raise CommandError(f'Falha no setup do simulador: {exc}') from exc

        try:
            if send_fn is post_send_market_order:
                resp = post_send_market_order(
                    payload,
                    skip_simulator_auto_filled=explicit_sim_setup,
                )
            else:
                resp = send_fn(payload)
        except Exception as exc:
            raise CommandError(f'Falha ao enviar ordem: {exc}') from exc

        order_kind = (
            'market'
            if order_type == ORDER_TYPE_MARKET_INTERNAL
            else (
                'limited'
                if order_type == ORDER_TYPE_LIMIT_INTERNAL
                else 'stop-limit'
            )
        )
        hist_price = infer_execution_price(payload, resp)
        if should_record_local_history(order_kind, resp):
            try:
                register_trade_execution(
                    ticker=ticker,
                    side=options['side'],
                    quantity=qty,
                    price=hist_price,
                    source='send_test_order_cmd',
                )
            except Exception:
                logger.exception('register_trade_execution send_test_order_cmd')

        self.stdout.write(self.style.SUCCESS('Resposta da API:'))
        self.stdout.write(json.dumps(resp, indent=2, ensure_ascii=False))
