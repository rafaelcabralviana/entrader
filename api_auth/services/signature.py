"""
Assinatura digital BODY_SIGNATURE (RSA-SHA256, PKCS1v15), conforme Smart Trader API.

O corpo assinado deve ser **exatamente** o mesmo byte string enviado no POST:
se usar ``dict``, a serialização JSON deve coincidir com o corpo da requisição.
Prefira passar ``body`` já como ``str`` quando o cliente montar o JSON manualmente.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from api_auth import config
from api_auth.exceptions import SmartTraderConfigurationError, SmartTraderSignatureError

logger = logging.getLogger(__name__)


def _body_string(body: str | dict | list[Any]) -> str:
    if isinstance(body, str):
        return body
    return json.dumps(body)


def generate_body_signature(body: str | dict | list[Any]) -> str:
    """
    Gera o valor do header ``BODY_SIGNATURE`` (Base64 da assinatura RSA-SHA256).

    Alinhado à documentação: ``PKCS1v15`` + ``SHA256`` sobre os bytes UTF-8 do corpo.
    """
    try:
        pem = config.private_rsa_pem_bytes()
    except ValueError as exc:
        raise SmartTraderConfigurationError(str(exc)) from exc

    body_string = _body_string(body)

    try:
        private_key = load_pem_private_key(pem, password=None)
        signature = private_key.sign(
            body_string.encode('utf-8'),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    except Exception as exc:
        logger.warning('Falha ao assinar corpo com chave RSA.')
        raise SmartTraderSignatureError('Não foi possível gerar BODY_SIGNATURE.') from exc

    return base64.b64encode(signature).decode('ascii')


def sign_body(body: str | dict | list[Any]) -> str:
    """Alias legível; mesmo comportamento que :func:`generate_body_signature`."""
    return generate_body_signature(body)
