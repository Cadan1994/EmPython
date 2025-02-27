# Generated by Django 4.2.4 on 2023-11-24 17:41

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Equipamento',
            fields=[
                ('equipamento_id', models.AutoField(primary_key=True, serialize=False)),
                ('modelo', models.CharField(max_length=50)),
                ('marca', models.CharField(max_length=50, null=True)),
                ('serial', models.CharField(max_length=50)),
                ('imei1', models.CharField(max_length=50)),
                ('imei2', models.CharField(blank=True, max_length=50, null=True)),
                ('processador', models.CharField(blank=True, max_length=50, null=True)),
                ('memoria', models.CharField(blank=True, max_length=50, null=True)),
                ('descricao', models.TextField(blank=True, null=True, verbose_name='Descrição')),
                ('status', models.CharField(blank=True, max_length=50, null=True)),
                ('valor', models.CharField(blank=True, max_length=255)),
                ('total_equipamentos', models.IntegerField()),
                ('data_entrega', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Data de cadastro')),
            ],
            options={
                'verbose_name': 'Equipamento',
                'verbose_name_plural': 'Equipamentos',
            },
        ),
        migrations.CreateModel(
            name='Tipo',
            fields=[
                ('tipo_id', models.AutoField(primary_key=True, serialize=False)),
                ('nome_tipo', models.CharField(max_length=100, null=True)),
                ('descricao', models.TextField(blank=True, verbose_name='Descrição')),
                ('data', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Data de cadastro')),
            ],
        ),
        migrations.CreateModel(
            name='Responsavel',
            fields=[
                ('responsavel_id', models.AutoField(primary_key=True, serialize=False)),
                ('nome', models.CharField(blank=True, max_length=255)),
                ('rg', models.CharField(max_length=10, verbose_name='RG')),
                ('cpf', models.CharField(max_length=11, verbose_name='CPF')),
                ('telefone_celular', models.CharField(blank=True, max_length=15, verbose_name='Telefone')),
                ('data_entrega', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Data de cadastro')),
                ('departamento', models.CharField(max_length=50)),
                ('funcao', models.CharField(blank=True, max_length=50, null=True, verbose_name='Função')),
                ('equipamento_responsavel', models.ManyToManyField(related_name='responsaveis', to='app_devicehub.equipamento')),
            ],
            options={
                'verbose_name': 'Responsável',
                'verbose_name_plural': 'Responsáveis',
            },
        ),
        migrations.CreateModel(
            name='Manutencao',
            fields=[
                ('manutencao_id', models.AutoField(primary_key=True, serialize=False)),
                ('descricao', models.TextField(blank=True, verbose_name='Descrição')),
                ('data_manutencao', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Data de Manutenção')),
                ('equipamento_manutencao', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='app_devicehub.equipamento', verbose_name='Equipamento em manutenção')),
                ('responsavel_manutencao', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='app_devicehub.responsavel', verbose_name='Responsável pelo equipamento')),
            ],
            options={
                'verbose_name': 'Manutenção',
            },
        ),
        migrations.AddField(
            model_name='equipamento',
            name='tipo',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='app_devicehub.tipo'),
        ),
        migrations.CreateModel(
            name='Devolucao',
            fields=[
                ('devolucao_id', models.AutoField(primary_key=True, serialize=False)),
                ('descricao', models.TextField(blank=True, verbose_name='Descrição')),
                ('data_devolucao', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Data de Devolução')),
                ('ultimo_equipamento', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='app_devicehub.equipamento', verbose_name='Antigo dispositivo')),
                ('ultimo_responsavel', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='app_devicehub.responsavel', verbose_name='Antigo responsável')),
            ],
            options={
                'verbose_name': 'Devolução',
            },
        ),
    ]
