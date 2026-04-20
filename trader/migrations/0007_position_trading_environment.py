# Generated manually for ENTRADE

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0006_trademarker'),
    ]

    operations = [
        migrations.AddField(
            model_name='position',
            name='trading_environment',
            field=models.CharField(
                choices=[('simulator', 'Simulador'), ('real', 'Real')],
                db_index=True,
                default='simulator',
                max_length=16,
            ),
        ),
        migrations.AddIndex(
            model_name='position',
            index=models.Index(
                fields=['ticker', 'is_active', 'trading_environment'],
                name='trader_pos_ticker_act_env_idx',
            ),
        ),
    ]
