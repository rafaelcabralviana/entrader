"""Testes HTTP das rotas de autenticação (views)."""

from unittest.mock import patch

from api_auth.tests.base import AuthHttpTestCase


class AuthStatusViewTests(AuthHttpTestCase):
    @patch('api_auth.views.get_access_token')
    def test_staff_receives_ok_without_token_in_body(self, mock_token):
        mock_token.return_value = 'super-secret-token'
        self.login_as_staff()
        response = self.client.get(self.auth_status_path)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get('status'), 'ok')
        self.assertNotIn('super-secret-token', response.content.decode())

    def test_anonymous_redirects_to_login(self):
        response = self.client.get(self.auth_status_path)
        self.assertEqual(response.status_code, 302)
