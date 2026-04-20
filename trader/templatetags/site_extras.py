"""Tags de template que leem settings (funcionam mesmo sem context processor)."""

from datetime import datetime
from zoneinfo import ZoneInfo

from django import template
from django.conf import settings

register = template.Library()
_TZ_BRT = ZoneInfo('America/Sao_Paulo')


@register.simple_tag
def site_brand_name() -> str:
    return getattr(settings, 'PUBLIC_SITE_NAME', 'Privado') or 'Privado'


@register.simple_tag
def site_brand_initial() -> str:
    name = getattr(settings, 'PUBLIC_SITE_NAME', 'Privado') or 'Privado'
    return name[0] if name else '·'


@register.simple_tag(takes_context=True)
def sidebar_title(context) -> str:
    request = context.get('request')
    if request is not None and getattr(request.user, 'is_authenticated', False):
        return getattr(settings, 'SESSION_APP_TITLE', 'Painel') or 'Painel'
    return getattr(settings, 'PUBLIC_SITE_NAME', 'Privado') or 'Privado'


@register.simple_tag(takes_context=True)
def sidebar_initial(context) -> str:
    name = sidebar_title(context)
    return name[0] if name else '·'


@register.filter
def index_at(sequence, i):
    """Acesso por índice em template: ``lista|index_at:forloop.counter0``."""
    if sequence is None:
        return ''
    try:
        idx = int(i)
        return sequence[idx]
    except (TypeError, ValueError, IndexError, KeyError):
        return ''


@register.filter
def zip_min(a, b):
    """
    Emparelha dois iteráveis pelo menor tamanho.

    Uso: ``bids|zip_min:asks`` retorna uma lista de dicts:
    ``[{ 'bid': <item>, 'ask': <item> }, ...]``.
    """
    if a is None or b is None:
        return []
    try:
        la = list(a)
        lb = list(b)
    except TypeError:
        return []
    n = min(len(la), len(lb))
    out = []
    for i in range(n):
        out.append({'bid': la[i], 'ask': lb[i]})
    return out


@register.filter
def brt_datetime(value):
    """
    Converte datetime/ISO para horário de Brasília.
    Exibição: dd/mm/yyyy HH:MM:SS
    """
    if value is None:
        return ''
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return ''
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return value
    else:
        return value
    if dt.tzinfo is None:
        return dt.strftime('%d/%m/%Y %H:%M:%S')
    return dt.astimezone(_TZ_BRT).strftime('%d/%m/%Y %H:%M:%S')
