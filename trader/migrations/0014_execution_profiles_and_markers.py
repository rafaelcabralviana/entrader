from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


def _backfill_profiles(apps, schema_editor):
    User = apps.get_model('auth', 'User')
    AutomationExecutionProfile = apps.get_model('trader', 'AutomationExecutionProfile')
    AutomationStrategyToggle = apps.get_model('trader', 'AutomationStrategyToggle')
    AutomationThought = apps.get_model('trader', 'AutomationThought')

    user_ids = set(
        AutomationStrategyToggle.objects.values_list('user_id', flat=True).distinct()
    ) | set(AutomationThought.objects.values_list('user_id', flat=True).distinct())
    user_ids = {int(uid) for uid in user_ids if uid}
    envs = ('simulator', 'real')

    profile_map: dict[tuple[int, str], int] = {}
    for uid in user_ids:
        if not User.objects.filter(id=uid).exists():
            continue
        for env in envs:
            p, _ = AutomationExecutionProfile.objects.get_or_create(
                user_id=uid,
                trading_environment=env,
                name='Tempo_Real',
                defaults={
                    'mode': 'real_time',
                    'is_active': True,
                    'is_system_default': True,
                },
            )
            profile_map[(uid, env)] = int(p.id)

    for (uid, env), pid in profile_map.items():
        AutomationStrategyToggle.objects.filter(
            user_id=uid,
            trading_environment=env,
            execution_profile__isnull=True,
        ).update(execution_profile_id=pid)
        AutomationThought.objects.filter(
            user_id=uid,
            trading_environment=env,
            execution_profile__isnull=True,
        ).update(execution_profile_id=pid)


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0013_automation_sim_replay_until'),
    ]

    operations = [
        migrations.CreateModel(
            name='AutomationExecutionProfile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('trading_environment', models.CharField(choices=[('simulator', 'Simulador'), ('real', 'Real')], db_index=True, max_length=16)),
                ('name', models.CharField(max_length=64)),
                ('mode', models.CharField(choices=[('real_time', 'Tempo real'), ('simulation', 'Simulação')], db_index=True, default='real_time', max_length=16)),
                ('is_active', models.BooleanField(db_index=True, default=False)),
                ('is_system_default', models.BooleanField(db_index=True, default=False)),
                ('sim_ticker', models.CharField(blank=True, max_length=16)),
                ('session_date', models.DateField(blank=True, null=True)),
                ('replay_until', models.DateTimeField(blank=True, null=True)),
                ('execution_started_at', models.DateTimeField(blank=True, null=True)),
                ('last_runtime_cursor_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='automation_execution_profiles', to='auth.user')),
            ],
            options={
                'verbose_name': 'Perfil de execução (automações)',
                'verbose_name_plural': 'Perfis de execução (automações)',
            },
        ),
        migrations.AddField(
            model_name='automationstrategytoggle',
            name='execution_profile',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='strategy_toggles', to='trader.automationexecutionprofile'),
        ),
        migrations.AddField(
            model_name='automationthought',
            name='execution_profile',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='thoughts', to='trader.automationexecutionprofile'),
        ),
        migrations.CreateModel(
            name='AutomationTriggerMarker',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('trading_environment', models.CharField(choices=[('simulator', 'Simulador'), ('real', 'Real')], db_index=True, max_length=16)),
                ('ticker', models.CharField(db_index=True, max_length=16)),
                ('strategy_key', models.CharField(db_index=True, max_length=96)),
                ('marker_at', models.DateTimeField(db_index=True)),
                ('price', models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ('message', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('execution_profile', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='trigger_markers', to='trader.automationexecutionprofile')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='automation_trigger_markers', to='auth.user')),
            ],
            options={
                'verbose_name': 'Marcação de disparo (automações)',
                'verbose_name_plural': 'Marcações de disparo (automações)',
                'ordering': ['-marker_at'],
            },
        ),
        migrations.AddIndex(
            model_name='automationexecutionprofile',
            index=models.Index(fields=['user', 'trading_environment', 'is_active'], name='trader_auto_user_id_1adf0f_idx'),
        ),
        migrations.AddIndex(
            model_name='automationexecutionprofile',
            index=models.Index(fields=['user', 'trading_environment', 'mode'], name='trader_auto_user_id_416057_idx'),
        ),
        migrations.AddConstraint(
            model_name='automationexecutionprofile',
            constraint=models.UniqueConstraint(fields=('user', 'trading_environment', 'name'), name='trader_auto_exec_profile_user_env_name_uniq'),
        ),
        migrations.AddIndex(
            model_name='automationthought',
            index=models.Index(fields=['execution_profile', 'id'], name='trader_auto_executi_1a6015_idx'),
        ),
        migrations.AddConstraint(
            model_name='automationstrategytoggle',
            constraint=models.UniqueConstraint(fields=('user', 'strategy_key', 'trading_environment', 'execution_profile'), name='trader_automation_strat_user_key_env_profile_uniq'),
        ),
        migrations.RemoveConstraint(
            model_name='automationstrategytoggle',
            name='trader_automation_strat_user_key_env_uniq',
        ),
        migrations.AddIndex(
            model_name='automationtriggermarker',
            index=models.Index(fields=['user', 'trading_environment', 'ticker', 'marker_at'], name='trader_auto_user_id_c64061_idx'),
        ),
        migrations.AddIndex(
            model_name='automationtriggermarker',
            index=models.Index(fields=['execution_profile', 'ticker', 'marker_at'], name='trader_auto_executi_d9456d_idx'),
        ),
        migrations.RunPython(_backfill_profiles, migrations.RunPython.noop),
    ]
