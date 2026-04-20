"""
Pacote de testes do ``api_auth``.

Estrutura
---------

``base.py``
    Casos base: ``AuthServiceTestCase``, ``AuthHttpTestCase``.

``support/``
    Dados fictícios de ambiente e mocks HTTP reutilizáveis.

``test_auth_service.py``
    Serviço de token (cache, erros, configuração).

``test_auth_http.py``
    Views e fluxo HTTP (permissões, corpo da resposta).

``auth_cases_template.py``
    Referência para copiar ao adicionar novos cenários (não é suite de testes).
"""
