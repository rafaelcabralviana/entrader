"""Regras de «grupo» para disparo da tendência ativa (trend_core.trend_group_qualifies)."""

from django.test import SimpleTestCase

from trader.automacoes.trend_core import trend_group_qualifies


class TrendGroupQualifiesTests(SimpleTestCase):
    def test_bloqueia_dois_iguais_sem_lateral(self):
        labels = ['Alta', 'Alta', 'Baixa', 'Baixa', 'Baixa']
        self.assertIs(trend_group_qualifies(labels, 'Alta'), False)

    def test_tres_alta_sem_lateral_passa(self):
        labels = ['Alta', 'Alta', 'Alta', 'Baixa', 'Baixa']
        self.assertIs(trend_group_qualifies(labels, 'Alta'), True)

    def test_rompimento_apos_lateral(self):
        labels = ['Alta', 'Lateralizado', 'Lateralizado', 'Lateralizado', 'Lateralizado']
        self.assertIs(trend_group_qualifies(labels, 'Alta'), True)

    def test_dois_baixa_com_lateral_no_meio(self):
        labels = ['Baixa', 'Baixa', 'Lateralizado', 'Alta', 'Lateralizado']
        self.assertIs(trend_group_qualifies(labels, 'Baixa'), True)

    def test_ultimo_nao_alinha(self):
        labels = ['Lateralizado', 'Alta', 'Alta', 'Alta', 'Alta']
        self.assertIs(trend_group_qualifies(labels, 'Baixa'), False)
