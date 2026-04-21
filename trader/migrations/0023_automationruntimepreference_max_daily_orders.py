from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0022_trading_environment_replay_and_shadow_migration'),
    ]

    operations = [
        migrations.AddField(
            model_name='automationruntimepreference',
            name='max_daily_orders',
            field=models.PositiveSmallIntegerField(
                default=10,
                help_text='Máximo de ordens enviadas por dia para o ativo (respeita teto da corretora).',
            ),
        ),
    ]
