from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.db.models import Count
from appcd.models import Equipamento, Tipo, Responsavel, Manutencao, Devolucao


@login_required
def home(request):
    return render(request, 'paginas/home.html')

'''########################## Dashboard ##################################################'''

@login_required
def gerenciamento(request):
    # caixas com os valores
    total_equipamentos = Equipamento.objects.count()
    total_funcionarios = Responsavel.objects.count()
    total_devolucao = Devolucao.objects.count()

    #Gráfico de pizza - Quantidade por tipo de dispositivo
    tipos = Tipo.objects.all()
    total_por_tipo = {}
    for tipo in tipos:
        quantidade = Equipamento.objects.filter(tipo=tipo).count()
        total_por_tipo[tipo.nome_tipo] = quantidade

    #Tabela - Quantidade por funções
    funcoes_mais_comuns = Responsavel.objects.values('funcao').annotate(
        total_tipos=Count('equipamento_responsavel__tipo')).order_by('-total_tipos')[:10]

    #Tabela - Devoluções por descrição
    total_devolucoes = Devolucao.objects.count()
    devolucoes = Devolucao.objects.values('descricao').annotate(total=Count('descricao')).order_by('-total')[:5]

    return render(request, 'paginas/dashboard.html', {
        'total_equipamentos': total_equipamentos,
        'total_funcionarios': total_funcionarios,
        'total_devolucao': total_devolucao,
        'total_por_tipo': total_por_tipo,
        'funcoes_mais_comuns': funcoes_mais_comuns,
        'total_devolucoes': total_devolucoes,
        'devolucoes': devolucoes
    })

#Gráfico de pizza - Quantidade de dispositivos ativo e inativo
def quantidade_dispositivos(request):
    dispositivos_ativos = Equipamento.objects.filter(status='A').count()
    dispositivos_inativos = Equipamento.objects.filter(status='I').count()

    return JsonResponse({
        'ativos': dispositivos_ativos,
        'inativos': dispositivos_inativos
    })

def info_devolucao(request):
    total_devolucoes = Devolucao.objects.count()

    devolucoes = Devolucao.objects.values('descricao').annotate(total=Count('descricao')).order_by('-total')[:5]

    tabela_devolucoes = [{'descricao': dev['descricao'], 'total': dev['total']} for dev in devolucoes]

    return JsonResponse({
        'total_devolucoes': total_devolucoes,
        'devolucoes': tabela_devolucoes
    })

def dados_departamentos(request):
    departamentos_com_dispositivos = Responsavel.objects.values('departamento').annotate(
        total_dispositivos=Count('equipamento_responsavel')).order_by('departamento')

    dados = {
        'departamentos': [item['departamento'] for item in departamentos_com_dispositivos],
        'quantidade_dispositivos': [item['total_dispositivos'] for item in departamentos_com_dispositivos]
    }

    return JsonResponse(dados)



