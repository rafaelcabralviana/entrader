#!/usr/bin/env bash
# Alterna qual app Django fica em 127.0.0.1:8000 (Nginx continua em proxy_pass fixo).
# Uso: sudo vps-django-active projetoalkha | entrade
set -euo pipefail

ACTIVE_LINK="/etc/nginx/snippets/django-active-static-media.conf"
SNIPPET_ALKHA="/etc/nginx/snippets/django-static-media-projetoalkha.conf"
SNIPPET_ENTRADE="/etc/nginx/snippets/django-static-media-entrade.conf"

usage() {
    echo "Uso: sudo $0 projetoalkha | entrade" >&2
    echo "  projetoalkha  — Gunicorn em /home/ProjetoAlkha (serviço gunicorn-projetoalkha)" >&2
    echo "  entrade       — Gunicorn em /home/APICLEAR/ENTRADE (serviço gunicorn-entrade)" >&2
    exit 1
}

if [[ "${EUID:-0}" -ne 0 ]]; then
    echo "Execute como root (sudo)." >&2
    exit 1
fi

case "${1:-}" in
    projetoalkha|alkha)
        TARGET="projetoalkha"
        SNIPPET="$SNIPPET_ALKHA"
        SVC_ACTIVE="gunicorn-projetoalkha.service"
        SVC_OTHER="gunicorn-entrade.service"
        ;;
    entrade)
        TARGET="entrade"
        SNIPPET="$SNIPPET_ENTRADE"
        SVC_ACTIVE="gunicorn-entrade.service"
        SVC_OTHER="gunicorn-projetoalkha.service"
        ;;
    *)
        usage
        ;;
esac

if [[ ! -f "$SNIPPET" ]]; then
    echo "Arquivo ausente: $SNIPPET (copie deploy/nginx/ para /etc/nginx/snippets/)." >&2
    exit 1
fi

# Serviço legado (antes da divisão em dois units)
systemctl stop gunicorn.service 2>/dev/null || true
systemctl disable gunicorn.service 2>/dev/null || true

systemctl stop "$SVC_OTHER" 2>/dev/null || true
ln -sfn "$SNIPPET" "$ACTIVE_LINK"
nginx -t
if systemctl is-active --quiet nginx 2>/dev/null; then
    systemctl reload nginx
else
    systemctl start nginx
fi
systemctl enable "$SVC_ACTIVE"
systemctl restart "$SVC_ACTIVE"

CELERY_W=celery-entrade-worker.service
CELERY_B=celery-entrade-beat.service
case "$TARGET" in
  entrade)
    if [[ -f /etc/systemd/system/$CELERY_W ]]; then
      systemctl daemon-reload 2>/dev/null || true
      systemctl enable "$CELERY_W" "$CELERY_B" 2>/dev/null || true
      systemctl restart "$CELERY_W" "$CELERY_B" 2>/dev/null || true
      echo "Celery worker + beat reiniciados (broker Redis padrão: 127.0.0.1:6379)."
    else
      echo "Dica: copie deploy/systemd/celery-entrade-*.service para /etc/systemd/system/ e: systemctl daemon-reload && systemctl enable --now celery-entrade-worker celery-entrade-beat"
    fi
    ;;
  projetoalkha)
    systemctl stop "$CELERY_W" "$CELERY_B" 2>/dev/null || true
    systemctl disable "$CELERY_W" "$CELERY_B" 2>/dev/null || true
    echo "Celery ENTRADE parado (outro app na porta 8000)."
    ;;
esac

echo "App ativo: $TARGET (porta 8000 → Nginx). Serviço: $SVC_ACTIVE"
