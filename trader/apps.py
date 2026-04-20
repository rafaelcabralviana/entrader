import logging

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class TraderConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'trader'
    verbose_name = 'Trader'

    def ready(self) -> None:
        try:
            from trader.automacoes.strategies_plugins import load_all

            load_all()
        except Exception:
            logger.exception('trader.apps: falha ao carregar strategies_plugins')

        # SQLite: leituras em paralelo com escritas (Celery) sem bloquear tanto o ficheiro.
        from django.db.backends.signals import connection_created

        def _sqlite_pragmas(sender, connection, **kwargs):
            if connection.vendor != 'sqlite':
                return
            try:
                from django.conf import settings

                opts = (settings.DATABASES.get('default') or {}).get('OPTIONS') or {}
                timeout_sec = float(opts.get('timeout', 90))
                busy_ms = max(5000, int(timeout_sec * 1000))
                with connection.cursor() as cursor:
                    cursor.execute('PRAGMA journal_mode=WAL;')
                    cursor.execute('PRAGMA synchronous=NORMAL;')
                    cursor.execute('PRAGMA busy_timeout=%d;' % busy_ms)
            except Exception:
                pass

        connection_created.connect(_sqlite_pragmas, dispatch_uid='entrade_sqlite_wal')
