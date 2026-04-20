from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trader', '0018_position_position_lane'),
    ]

    operations = [
        migrations.AddField(
            model_name='automationexecutionprofile',
            name='live_ticker',
            field=models.CharField(blank=True, max_length=16),
        ),
    ]
