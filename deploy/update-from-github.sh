#!/usr/bin/env bash
# Atualiza o ENTRADE a partir do GitHub e reinicia serviços.
# Uso: sudo bash deploy/update-from-github.sh
# Requer: clone em /home/APICLEAR/ENTRADE, remote origin, .venv e .env no servidor.
set -euo pipefail

ROOT="/home/APICLEAR/ENTRADE"
cd "$ROOT"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "Execute com sudo (reinício dos serviços systemd)." >&2
  exit 1
fi

if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
  echo "Aviso: há alterações locais não commitadas. Faça commit, stash ou descarte antes do pull." >&2
  git status -s
  exit 1
fi

echo ">>> git pull"
git pull --ff-only origin main

echo ">>> pip install"
"$ROOT/.venv/bin/pip" install -r "$ROOT/requirements.txt"

echo ">>> migrate"
"$ROOT/.venv/bin/python" "$ROOT/manage.py" migrate --noinput

echo ">>> collectstatic"
"$ROOT/.venv/bin/python" "$ROOT/manage.py" collectstatic --noinput

echo ">>> reiniciar serviços"
systemctl restart gunicorn-entrade.service
systemctl restart celery-entrade-worker.service 2>/dev/null || true
systemctl restart celery-entrade-beat.service 2>/dev/null || true

echo "OK. Estado:"
systemctl is-active gunicorn-entrade.service || true
