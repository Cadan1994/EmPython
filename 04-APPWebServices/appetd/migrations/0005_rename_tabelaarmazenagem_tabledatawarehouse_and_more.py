# Generated by Django 4.2.8 on 2024-02-21 12:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('appetd', '0004_rename_tabelasarmazenagens_tabelaarmazenagem'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='TabelaArmazenagem',
            new_name='TableDataWarehouse',
        ),
        migrations.AlterModelOptions(
            name='tabledatawarehouse',
            options={'ordering': ['id'], 'verbose_name_plural': 'Tabelas de Armazenagens'},
        ),
    ]
