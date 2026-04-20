from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0015_rename_trader_auto_user_id_1adf0f_idx_trader_auto_user_id_8e1b0a_idx_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='automationstrategytoggle',
            name='execute_orders',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='automationstrategytoggle',
            name='params_json',
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
