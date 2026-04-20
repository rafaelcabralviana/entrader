"""Gunicorn — ENTRADE (mesma porta 8000 que o Nginx usa no proxy_pass)."""
import multiprocessing
import os

chdir = '/home/APICLEAR/ENTRADE'

wsgi_module = 'ENTRADE.wsgi:application'

bind = '127.0.0.1:8000'

# Com SQLite, muitos workers sync disputam o mesmo ficheiro → fila de locks e páginas
# a “carregar para sempre” até esgotar workers/timeout. Cap por defeito; com Postgres
# pode subir (ex.: GUNICORN_WORKERS=9 no ambiente do systemd).
_cpu = max(1, multiprocessing.cpu_count())
_formula = _cpu * 2 + 1
_default_workers = max(2, min(4, _formula))
try:
    workers = int(os.environ.get('GUNICORN_WORKERS', str(_default_workers)))
except ValueError:
    workers = _default_workers
workers = max(1, workers)
worker_class = 'sync'
timeout = 120

os.makedirs('/home/APICLEAR/ENTRADE/logs', exist_ok=True)
accesslog = '/home/APICLEAR/ENTRADE/logs/gunicorn_access.log'
errorlog = '/home/APICLEAR/ENTRADE/logs/gunicorn_error.log'
loglevel = 'info'

daemon = False
preload_app = True
max_requests = 1000
max_requests_jitter = 50


def post_fork(server, worker):
    try:
        from django.db import connections

        for conn in connections.all():
            conn.close()
    except Exception as e:
        server.log.warning(
            'Erro ao reinicializar conexões do banco no worker %s: %s',
            worker.pid,
            e,
        )
