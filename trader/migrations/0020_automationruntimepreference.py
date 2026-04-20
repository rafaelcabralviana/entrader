from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0019_automationexecutionprofile_live_ticker'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='AutomationRuntimePreference',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                (
                    'trading_environment',
                    models.CharField(
                        choices=[('simulator', 'Simulador'), ('real', 'Real')],
                        db_index=True,
                        max_length=16,
                    ),
                ),
                ('enabled', models.BooleanField(db_index=True, default=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                (
                    'user',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='automation_runtime_preferences',
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                'verbose_name': 'Preferência de execução do robô',
                'verbose_name_plural': 'Preferências de execução do robô',
                'indexes': [models.Index(fields=['user', 'trading_environment', 'enabled'], name='trader_auto_user_id_476d2f_idx')],
                'constraints': [
                    models.UniqueConstraint(
                        fields=('user', 'trading_environment'),
                        name='trader_auto_runtime_pref_user_env_uniq',
                    )
                ],
            },
        ),
    ]
