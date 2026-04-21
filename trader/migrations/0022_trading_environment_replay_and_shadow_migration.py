# Generated manually — valor ``replay`` + migração ledger fictício para o ambiente Replay.

from __future__ import annotations

from django.db import migrations


def forwards_migrate_replay_shadow_to_replay_env(apps, schema_editor):
    Position = apps.get_model('trader', 'Position')
    db_alias = schema_editor.connection.alias
    Position.objects.using(db_alias).filter(
        position_lane='replay_shadow',
        trading_environment='simulator',
    ).update(trading_environment='replay')


def backwards_noop(apps, schema_editor):
    Position = apps.get_model('trader', 'Position')
    db_alias = schema_editor.connection.alias
    Position.objects.using(db_alias).filter(
        position_lane='replay_shadow',
        trading_environment='replay',
    ).update(trading_environment='simulator')


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0021_automationruntimepreference_max_open_operations'),
    ]

    operations = [
        migrations.RunPython(forwards_migrate_replay_shadow_to_replay_env, backwards_noop),
    ]
