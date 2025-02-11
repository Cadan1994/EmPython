from django.db import models
from django.utils import timezone

class Tipo(models.Model):
    tipo_id = models.AutoField(primary_key=True)
    nome_tipo = models.CharField(max_length=100, null=True)
    descricao = models.TextField(blank=True, verbose_name='Descrição')
    data = models.DateTimeField(default=timezone.now, verbose_name='Data de cadastro')

    def __str__(self) -> str:
        return f'{self.nome_tipo}'

'''class Departamento_Funcao(models.Model):
    class Meta:
        verbose_name = 'Departamento/Função'

    departamento_funcao_id = models.AutoField(primary_key=True)
    nome = models.CharField(max_length=100, null=True, verbose_name='Nome do Departamento e função')
    data = models.DateField(auto_now=True, verbose_name='Data de cadastro')

    def __str__(self) -> str:
        return f'{self.nome}'''


class Equipamento(models.Model):
    class Meta:
        verbose_name = 'Equipamento'
        verbose_name_plural = 'Equipamentos'
        unique_together = ['imei1', 'imei2', 'serial']

    STATUS_CHOICES = (
        ("A", "Ativo"),
        ("I", "Inativo"),
        ("N", "Nenhuma das opções")
    )

    equipamento_id = models.AutoField(primary_key=True)
    #responsavel = models.ForeignKey(Responsavel, on_delete=models.CASCADE, null=True,blank=True)
    modelo = models.CharField(max_length=50)
    marca = models.CharField(max_length=50, null=True)
    tipo = models.ForeignKey(Tipo, on_delete=models.SET_NULL, null=True)
    serial = models.CharField(max_length=50)
    imei1 = models.CharField(max_length=50)
    imei2 = models.CharField(max_length=50, blank=True, null=True)
    processador = models.CharField(max_length=50, blank=True, null=True)
    memoria = models.CharField(max_length=50, blank=True, null=True)
    descricao = models.TextField(blank=True, null=True, verbose_name='Descrição')
    status = models.CharField(max_length=1, choices=STATUS_CHOICES, blank=True, null=True)
    valor = models.CharField(max_length=255, blank=True)
    total_equipamentos = models.IntegerField()
    data_entrega = models.DateTimeField(default=timezone.now, verbose_name='Data de cadastro')

    def __str__(self) -> str:
        return f'{self.serial}'

class Responsavel(models.Model):
    class Meta:
        verbose_name = 'Responsável'
        verbose_name_plural = 'Responsáveis'
        unique_together = ['rg', 'cpf'] #campos que não pode se repedir

    responsavel_id = models.AutoField(primary_key=True)
    nome = models.CharField(max_length=255, blank=True, verbose_name='Nome Completo')
    rg = models.CharField(max_length=50, verbose_name='RG')
    cpf = models.CharField(max_length=50, verbose_name='CPF')
    telefone_celular = models.CharField(max_length=50, verbose_name='Telefone', blank=True)
    data_entrega = models.DateTimeField(default=timezone.now, verbose_name='Data de cadastro')
    #equipamento_responsavel = models.ForeignKey(Equipamento, on_delete=models.SET_NULL, null=True, verbose_name='Dispositivo')
    equipamento_responsavel = models.ManyToManyField('Equipamento', related_name='responsaveis')
    departamento = models.CharField(max_length=50)
    funcao = models.CharField(max_length=50, blank=True, null=True, verbose_name='Função')

    def __str__(self) -> str:
        return f'{self.nome}'

    def obter_info_equipamentos(self):
        info_equipamentos = []

        for equipamento in self.equipamento_responsavel.all():
            info_equipamento = {
                'equipamento_id': equipamento.equipamento_id,
                'valor': equipamento.valor,
                'imei1': equipamento.imei1,
                'imei2': equipamento.imei2,
                'modelo': equipamento.modelo,
                'marca': equipamento.marca,
                'tipo': equipamento.tipo,
            }
            info_equipamentos.append(info_equipamento)

        return info_equipamentos

class Devolucao(models.Model):
    class Meta:
        verbose_name = 'Devolução'
    devolucao_id = models.AutoField(primary_key=True)
    ultimo_equipamento = models.ForeignKey(Equipamento, on_delete=models.SET_NULL, null=True, verbose_name='Antigo dispositivo')
    ultimo_responsavel = models.ForeignKey(Responsavel, on_delete=models.SET_NULL, null=True,verbose_name='Antigo responsável')
    descricao = models.TextField(blank=True, verbose_name='Descrição')
    data_devolucao = models.DateTimeField(default=timezone.now, verbose_name='Data de cadastro')

    def __str__(self) -> str:
        return f'{self.devolucao_id}'

    def obter_info_responsavel(self):
        if self.ultimo_responsavel:
            info_responsavel = {
                'responsavel_id': self.ultimo_responsavel.responsavel_id,
                'nome': self.ultimo_responsavel.nome,
                'cpf': self.ultimo_responsavel.cpf,
                'rg': self.ultimo_responsavel.rg,
                'departamento': self.ultimo_responsavel.departamento,
                'funcao': self.ultimo_responsavel.funcao,
            }
            return info_responsavel
        return {'erro': 'Responsável não definido'}

    def obter_info_equipamento(self):
        if self.ultimo_equipamento:
            info_equipamento = {
                'imei1': self.ultimo_equipamento.imei1,
                'imei2': self.ultimo_equipamento.imei2,
                'modelo': self.ultimo_equipamento.modelo,
                'marca': self.ultimo_equipamento.marca,
                'tipo': self.ultimo_equipamento.tipo,
            }
            return info_equipamento
        return {'erro': 'Equipamento não definido'}

class Manutencao(models.Model):
    class Meta:
        verbose_name = 'Manutenção'

    manutencao_id = models.AutoField(primary_key=True)
    equipamento_manutencao = models.ForeignKey(Equipamento, on_delete=models.SET_NULL, null=True, verbose_name='Equipamento em manutenção')
    responsavel_manutencao = models.ForeignKey(Responsavel, on_delete=models.SET_NULL, null=True,verbose_name='Responsável pelo equipamento')
    descricao = models.TextField(blank=True, verbose_name='Descrição')
    data_manutencao = models.DateTimeField(default=timezone.now, verbose_name='Data de cadastro')

    def __str__(self) -> str:
        return f'{self.manutencao_id}'



