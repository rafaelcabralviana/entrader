from django.core.management.base import BaseCommand

from api_auth.exceptions import SmartTraderAuthError, SmartTraderConfigurationError
from api_auth.services.auth import get_access_token


class Command(BaseCommand):
    help = (
        'Verifica se o .env permite obter access_token na API de autenticação '
        '(não exibe o token).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--refresh',
            action='store_true',
            help='Ignora cache e solicita um token novo.',
        )

    def handle(self, *args, **options):
        force = options['refresh']
        try:
            get_access_token(force_refresh=force)
        except SmartTraderConfigurationError as exc:
            self.stderr.write(self.style.ERROR(f'Configuração: {exc}'))
            raise SystemExit(1) from exc
        except SmartTraderAuthError as exc:
            self.stderr.write(self.style.ERROR(f'Autenticação: {exc}'))
            raise SystemExit(2) from exc

        self.stdout.write(
            self.style.SUCCESS('Conectado: token obtido com sucesso (credenciais válidas).')
        )
