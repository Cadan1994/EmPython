# Generated by Django 4.2.8 on 2024-03-05 16:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('appetd', '0011_rename_tabela_selecttablesdataextraction_table'),
    ]

    operations = [
        migrations.AlterField(
            model_name='selecttablesdataextraction',
            name='table',
            field=models.IntegerField(verbose_name='Tabela'),
        ),
    ]
