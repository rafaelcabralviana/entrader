from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('trader', '0020_automationruntimepreference'),
    ]

    operations = [
        migrations.AddField(
            model_name='automationruntimepreference',
            name='max_open_operations',
            field=models.PositiveSmallIntegerField(
                default=1,
                help_text='Máximo de operações abertas simultâneas para novas entradas das estratégias ativas.',
            ),
        ),
    ]

