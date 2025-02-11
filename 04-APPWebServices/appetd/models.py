from django.db import models


class TableDataWarehouse(models.Model):
    """
    TABELAS DE ARMAZENAGEM DE DADOS
    --------------------------------------------------------------------------------------------------------------------
    Objetive: Cadastrar das tabelas para o 'DATA WAREHOUSE'
    Create: HILSON SANTOS
    Data: 16/12/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    type = (('D', 'Dimensão'), ('F', 'Fato'))
    status = (('A', 'Ativo'), ('I', 'Inativo'))
    name = models.CharField(max_length=30, verbose_name='Nome')
    description = models.TextField(verbose_name='Descrição')
    createscript = models.TextField(verbose_name='Script de Criação')
    type = models.CharField(max_length=1, choices=type, verbose_name='Tipo')
    status = models.CharField(max_length=1, choices=status, verbose_name='Status')
    alteruser = models.IntegerField(verbose_name='Usuário Alteração')
    datealter = models.DateField(auto_now=True, verbose_name='Data Alteração')

    class Meta:
        verbose_name_plural = 'Tabelas'
        ordering = ['id']

    def __str__(self):
        return self.description


class SelectTablesDataExtraction(models.Model):
    """
    TABELAS PARA SELECIONAR OS DADOS PARA EXTRAÇÃO
    --------------------------------------------------------------------------------------------------------------------
    Objetive: Cadastrar as tabelas e os selects que serão para a extração de dados no ERP
    Create: HILSON SANTOS
    Date: 16/12/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    type = (('D', 'Dimensão'), ('F', 'Fato'))
    status = (('A', 'Ativo'), ('I', 'Inativo'))
    partial = (('N', 'Não'), ('S', 'Sim'))
    table = models.IntegerField(verbose_name='Tabela')
    name = models.CharField(max_length=30, verbose_name='Nome')
    description = models.TextField(verbose_name='Descrição')
    selectinitial = models.TextField(verbose_name='Select Inicial')
    selectpartial = models.TextField(verbose_name='Select Parcial')
    partial = models.CharField(max_length=1, choices=partial, verbose_name='Executa select parcial?')
    type = models.CharField(max_length=1, choices=type, verbose_name='Tipo')
    status = models.CharField(max_length=1, choices=status, verbose_name='Status')
    alteruser = models.IntegerField(verbose_name='Usuário Alteração')
    datealter = models.DateField(auto_now=True, verbose_name='Data Alteração')

    class Meta:
        verbose_name_plural = 'Selects'
        ordering = ['id']

    def __str__(self):
        return self.description