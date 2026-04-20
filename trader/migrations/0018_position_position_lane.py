from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0017_automation_trailing_stop_preference'),
    ]

    operations = [
        migrations.AddField(
            model_name='position',
            name='position_lane',
            field=models.CharField(
                choices=[
                    ('standard', 'API / simulador (custódia)'),
                    ('replay_shadow', 'Replay fictício (sessão)'),
                ],
                db_index=True,
                default='standard',
                max_length=24,
            ),
        ),
        migrations.AddIndex(
            model_name='position',
            index=models.Index(
                fields=['ticker', 'is_active', 'trading_environment', 'position_lane'],
                name='trader_pos_env_lane_act_idx',
            ),
        ),
    ]
