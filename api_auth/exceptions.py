class SmartTraderError(Exception):
    """Erro base da integração Smart Trader."""


class SmartTraderConfigurationError(SmartTraderError):
    """Configuração ausente ou inválida (variáveis de ambiente)."""


class SmartTraderAuthError(SmartTraderError):
    """Falha ao obter ou renovar o token de acesso."""


class SmartTraderSignatureError(SmartTraderError):
    """Falha ao gerar BODY_SIGNATURE (RSA-SHA256)."""
