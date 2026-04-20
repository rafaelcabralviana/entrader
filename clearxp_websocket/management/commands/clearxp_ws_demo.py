import json
import time

from django.core.management.base import BaseCommand

from clearxp_websocket.protocol import WebSocketRequestMessage, subscribe_quote
from clearxp_websocket.services.client import connect_websocket, send_message_to_websocket


class Command(BaseCommand):
    help = (
        'Conecta ao WebSocket Smart Trader (demo). Use fora do runserver. '
        'Ctrl+C encerra antes do tempo.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--route',
            choices=('marketdata', 'orders'),
            default='marketdata',
            help='Rota WebSocket (default: marketdata).',
        )
        parser.add_argument(
            '--ticker',
            default='WINJ26',
            help='Ativo para SubscribeQuote (padrão: mini índice WIN; route=marketdata).',
        )
        parser.add_argument(
            '--seconds',
            type=int,
            default=120,
            help='Duração em segundos antes de sair (default: 120).',
        )

    def handle(self, *args, **options):
        route = options['route']
        ticker = options['ticker']
        seconds = options['seconds']

        def on_message(raw: str) -> None:
            try:
                data = json.loads(raw)
                if data.get('type') == 6:
                    self.stdout.write('[msg] keep-alive (type=6)')
                    return
                target = data.get('target', '')
                self.stdout.write(f'[msg] target={target}')
            except json.JSONDecodeError:
                self.stdout.write(f'[msg] (não-JSON) {raw[:200]}')

        def on_open() -> None:
            if route == 'marketdata':
                msg = subscribe_quote(ticker)
            else:
                msg = WebSocketRequestMessage(
                    arguments=[],
                    target='SubscribeOrdersStatus',
                    msg_type=1,
                )
            send_message_to_websocket(route, msg)
            self.stdout.write(
                self.style.SUCCESS(f'Subscrição enviada route={route!r}.')
            )

        connect_websocket(on_message, on_open, route)
        self.stdout.write(
            f'Escutando por {seconds}s (route={route}). Ctrl+C para parar antes.'
        )
        try:
            time.sleep(seconds)
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Interrompido pelo usuário.'))
