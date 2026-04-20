"""
Casos base para testes de autenticação do app ``api_auth`` (API da corretora).

Uso típico:
- Serviço (sem banco): herde ``AuthServiceTestCase``.
- HTTP (views + cliente Django): herde ``AuthHttpTestCase``.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase, override_settings

from api_auth.services import auth as auth_service
from api_auth.tests.support.env import default_api_auth_env

_ISOLATED_CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
    }
}


@override_settings(CACHES=_ISOLATED_CACHES)
class AuthServiceTestCase(SimpleTestCase):
    """
    Garante cache limpo e variáveis de ambiente da integração fictícias.

    Sobrescreva ``api_auth_env()`` se precisar de um dicionário diferente.
    """

    def api_auth_env(self) -> dict[str, str]:
        return default_api_auth_env()

    def setUp(self) -> None:
        super().setUp()
        self._clear_auth_state()
        self._env_patcher = patch.dict(os.environ, self.api_auth_env(), clear=False)
        self._env_patcher.start()

    def tearDown(self) -> None:
        self._env_patcher.stop()
        self._clear_auth_state()
        super().tearDown()

    @staticmethod
    def _clear_auth_state() -> None:
        cache.clear()
        auth_service.clear_token_cache()


class AuthHttpTestCase(TestCase):
    """Testes de rotas HTTP relacionadas à autenticação (staff, JSON, etc.)."""

    auth_status_path = '/api/auth/status/'

    def setUp(self) -> None:
        super().setUp()
        self.staff_user = User.objects.create_user(
            username='staff_tester',
            password='test-password-secure-1',
            is_staff=True,
        )

    def login_as_staff(self) -> None:
        self.client.login(username='staff_tester', password='test-password-secure-1')
