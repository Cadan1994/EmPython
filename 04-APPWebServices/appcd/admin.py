from django.contrib import admin
from . import models

@admin.register(models.Equipamento)
class EquipamentosAdmin(admin.ModelAdmin):
    list_display = 'equipamento_id', 'serial', 'imei1',
    ordering = ('equipamento_id', 'serial', 'imei1')

@admin.register(models.Responsavel)
class ResponsavelAdmin(admin.ModelAdmin):
    list_display = 'responsavel_id', 'cpf', 'nome'
    ordering = ('responsavel_id', 'cpf', 'nome')

@admin.register(models.Devolucao)
class DevolucaoAdmin(admin.ModelAdmin):
    list_display = 'devolucao_id', 'ultimo_responsavel'
    ordering = ('devolucao_id', 'ultimo_responsavel')

'''@admin.register(models.Departamento)
class DepartamentoAdmin(admin.ModelAdmin):
    list_display = 'departamento_id', 'nome_departamento'
    ordering = ('departamento_id', 'nome_departamento')'''

@admin.register(models.Tipo)
class TipoAdmin(admin.ModelAdmin):
    list_display = 'tipo_id', 'nome_tipo'
    ordering = ('tipo_id', 'nome_tipo')

@admin.register(models.Manutencao)
class ManutencaoAdmin(admin.ModelAdmin):
    list_display = 'manutencao_id', 'responsavel_manutencao',
    ordering = ('manutencao_id', 'responsavel_manutencao')

