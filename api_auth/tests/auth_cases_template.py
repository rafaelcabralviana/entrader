"""
TEMPLATE — organização sugerida para novos testes de autenticação
==================================================================

1. Copie trechos deste arquivo para um novo módulo ``test_auth_<area>.py``
   (nome começando com ``test_`` para o Django descobrir os casos).

2. Testes do **serviço** (sem banco): herde ``AuthServiceTestCase`` em
   ``api_auth.tests.base`` e use ``patch`` em
   ``api_auth.services.auth.requests.post``.

3. Testes **HTTP** (views, permissões): herde ``AuthHttpTestCase`` e use
   ``self.client``; mocks em ``api_auth.views.get_access_token`` quando
   não quiser chamar a API real.

4. Dados fictícios de ambiente: ``default_api_auth_env()`` em
   ``api_auth.tests.support.env``.

5. Respostas JSON: ``auth_success_payload`` e ``mock_auth_post_response`` em
   ``api_auth.tests.support.mocks``.

Exemplo mínimo (serviço)
------------------------

.. code-block:: python

    from unittest.mock import patch

    from api_auth.services import auth as auth_service
    from api_auth.tests.base import AuthServiceTestCase
    from api_auth.tests.support.mocks import auth_success_payload, mock_auth_post_response


    class MinhaFeatureAuthTests(AuthServiceTestCase):
        @patch('api_auth.services.auth.requests.post')
        def test_renovacao_quando_api_retorna_novo_token(self, mock_post):
            mock_post.return_value = mock_auth_post_response(
                json_body=auth_success_payload(access_token='novo', expires_in=300),
            )
            self.assertEqual(auth_service.get_access_token(force_refresh=True), 'novo')

Exemplo mínimo (HTTP)
---------------------

.. code-block:: python

    from unittest.mock import patch

    from api_auth.tests.base import AuthHttpTestCase


    class MinhasRotasAuthTests(AuthHttpTestCase):
        @patch('api_auth.views.get_access_token')
        def test_staff_ve_status_ok(self, mock_get_token):
            mock_get_token.return_value = 'token-opaco'
            self.login_as_staff()
            r = self.client.get(self.auth_status_path)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json().get('status'), 'ok')
"""
