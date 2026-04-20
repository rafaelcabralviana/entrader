from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0016_strategy_toggle_exec_and_params'),
    ]

    operations = [
        migrations.CreateModel(
            name='AutomationTrailingStopPreference',
            fields=[
                (
                    'id',
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name='ID',
                    ),
                ),
                (
                    'trading_environment',
                    models.CharField(
                        choices=[('simulator', 'Simulador'), ('real', 'Real')],
                        db_index=True,
                        max_length=16,
                    ),
                ),
                ('adjustment_enabled', models.BooleanField(default=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                (
                    'execution_profile',
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='trailing_stop_preferences',
                        to='trader.automationexecutionprofile',
                    ),
                ),
                (
                    'user',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='automation_trailing_stop_preferences',
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                'verbose_name': 'Preferência de trailing stop (automações)',
                'verbose_name_plural': 'Preferências de trailing stop (automações)',
            },
        ),
        migrations.AddConstraint(
            model_name='automationtrailingstoppreference',
            constraint=models.UniqueConstraint(
                fields=('user', 'trading_environment', 'execution_profile'),
                name='trader_auto_trailing_pref_user_env_profile_uniq',
            ),
        ),
        migrations.AddIndex(
            model_name='automationtrailingstoppreference',
            index=models.Index(fields=['user', 'trading_environment'], name='trader_auto__user_id_7a8b2c_idx'),
        ),
    ]
