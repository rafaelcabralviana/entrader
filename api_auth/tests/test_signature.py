import base64
import os
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from api_auth.exceptions import SmartTraderConfigurationError, SmartTraderSignatureError
from api_auth.services.signature import generate_body_signature
from api_auth.tests.base import AuthServiceTestCase
from api_auth.tests.support.env import default_api_auth_env


def _pem_b64() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.b64encode(pem).decode('ascii')


class GenerateBodySignatureTests(AuthServiceTestCase):
    def api_auth_env(self):
        env = super().api_auth_env()
        env['SMART_TRADER_PRIVATE_RSA_PEM_B64'] = _pem_b64()
        return env

    def test_sign_dict_returns_base64(self):
        sig = generate_body_signature({'symbol': 'PETR4', 'quantity': 1})
        self.assertTrue(sig)
        self.assertEqual(len(base64.b64decode(sig, validate=True)), 256)

    def test_same_body_same_signature(self):
        body = {'a': 1, 'b': 2}
        self.assertEqual(generate_body_signature(body), generate_body_signature(body))

    def test_string_body(self):
        raw = '{"x":1}'
        sig = generate_body_signature(raw)
        self.assertTrue(sig)

    def test_missing_key_raises(self):
        self._env_patcher.stop()
        with patch.dict(os.environ, default_api_auth_env(), clear=True):
            with self.assertRaises(SmartTraderConfigurationError):
                generate_body_signature({'k': 'v'})

    def test_invalid_pem_raises_signature_error(self):
        self._env_patcher.stop()
        bad_b64 = base64.b64encode(b'not a pem').decode('ascii')
        merged = {**default_api_auth_env(), 'SMART_TRADER_PRIVATE_RSA_PEM_B64': bad_b64}
        with patch.dict(os.environ, merged, clear=True):
            with self.assertRaises(SmartTraderSignatureError):
                generate_body_signature({'k': 'v'})
