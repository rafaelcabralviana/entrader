"""Testes do serviço ``get_access_token`` (HTTP mockado, sem rede)."""

import os
from unittest.mock import patch

from api_auth.exceptions import SmartTraderAuthError, SmartTraderConfigurationError
from api_auth.services import auth as auth_service
from api_auth.tests.base import AuthServiceTestCase
from api_auth.tests.support.mocks import auth_success_payload, mock_auth_post_response


class GetAccessTokenTests(AuthServiceTestCase):
    @patch('api_auth.services.auth.requests.post')
    def test_obtains_token_and_caches(self, mock_post):
        mock_post.return_value = mock_auth_post_response(
            json_body=auth_success_payload(
                access_token='token-abc',
                expires_in=120,
            ),
        )

        token = auth_service.get_access_token()
        self.assertEqual(token, 'token-abc')
        mock_post.assert_called_once()

        mock_post.reset_mock()
        token_again = auth_service.get_access_token()
        self.assertEqual(token_again, 'token-abc')
        mock_post.assert_not_called()

    @patch('api_auth.services.auth.requests.post')
    def test_force_refresh_bypasses_cache(self, mock_post):
        mock_response = mock_auth_post_response(
            json_body=auth_success_payload(access_token='first', expires_in=600),
        )
        mock_post.return_value = mock_response

        auth_service.get_access_token()
        mock_response.json.return_value = auth_success_payload(
            access_token='second',
            expires_in=600,
        )
        token = auth_service.get_access_token(force_refresh=True)
        self.assertEqual(token, 'second')
        self.assertEqual(mock_post.call_count, 2)

    @patch('api_auth.services.auth.requests.post')
    def test_http_error_raises(self, mock_post):
        mock_post.return_value = mock_auth_post_response(status_code=401)

        with self.assertRaises(SmartTraderAuthError):
            auth_service.get_access_token()

    @patch('api_auth.services.auth.requests.post')
    def test_missing_token_in_body_raises(self, mock_post):
        mock_post.return_value = mock_auth_post_response(json_body={})

        with self.assertRaises(SmartTraderAuthError):
            auth_service.get_access_token()

    def test_missing_env_raises_configuration_error(self):
        self._env_patcher.stop()
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SmartTraderConfigurationError):
                auth_service.get_access_token()
