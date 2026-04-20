"""
Camadas do sistema de trading (dados → features → sinal → risco → execução → log → aprendizado).

A fonte canónica de mercado no ENTRADE são ``QuoteSnapshot`` e ``BookSnapshot``;
este pacote agrega leitura, features derivadas e orquestração sem duplicar raw.
"""
