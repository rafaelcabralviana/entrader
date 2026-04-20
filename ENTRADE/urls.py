"""
URL configuration for ENTRADE project.

- Painel web e futuras rotas de UI: app ``trader`` em ``/``.
- Autenticação API: ``api_auth`` em ``/api/...`` (ex.: ``/api/auth/status/``).
"""
from django.conf import settings
from django.contrib import admin
from django.urls import include, path

admin.site.site_header = getattr(settings, 'PUBLIC_SITE_NAME', 'Privado')
admin.site.site_title = getattr(settings, 'PUBLIC_SITE_NAME', 'Privado')
admin.site.index_title = 'Painel'

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('api_auth.urls')),
    path('', include('accounts.urls')),
    path('', include('trader.urls')),
]
