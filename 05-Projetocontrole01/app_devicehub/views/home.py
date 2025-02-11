from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.db.models import Count
from app_devicehub.models import Equipamento, Tipo, Responsavel, Manutencao, Devolucao


@login_required
def home(request):
    return render(request, 'paginas/home.html')

'''########################## Dashboard ##################################################'''

'''1 - Equipamentos:'''
@login_required
def gerenciamento(request):
    return render(request, 'paginas/dashboard.html')

"""QUANTIDADE TOTAL DE TODOS OS DISPOSITIVOS"""
def retorna_total_equipamentos(request):
    total_equipamentos = Equipamento.objects.count()
    if request.method == "GET":
        return JsonResponse({'total_equipamentos': total_equipamentos})


"""QUANTIDADE DE CADA TIPO DE DISPOSITIVO"""
def retorna_total_por_tipo(request):
    tipos = Tipo.objects.all()
    total_por_tipo = {}
    for tipo in tipos:
        quantidade = Equipamento.objects.filter(tipo=tipo).count()
        total_por_tipo[tipo.nome_tipo] = quantidade
    return JsonResponse({'total_por_tipo': total_por_tipo})


"""AONDE SE ENCONTRA OS DISPOSITIVOS E SUA MAIOR QUENTIDADE"""
def funcoes_com_mais_tipos(request):
    funcoes_mais_comuns = Responsavel.objects.values('funcao').annotate(total_tipos=Count('equipamento_responsavel__tipo')).order_by('-total_tipos')[:5]
    return JsonResponse({'funcoes_mais_comuns': list(funcoes_mais_comuns)})

def departamentos_com_mais_tipos(request):
    departamentos_mais_comuns = Responsavel.objects.values('departamento').annotate(total_tipos=Count('equipamento_responsavel__tipo')).order_by('-total_tipos')[:5]
    return JsonResponse({'departamentos_mais_comuns': list(departamentos_mais_comuns)})



"""TOTAL DA MANUTENÇÃO E O MOTIVO O PORQUE TÁ EM MANUTENÇÃO"""
def info_manutencao(request):
    total_equipamentos_manutencao = Manutencao.objects.count()

    equipamentos_manutencao = Manutencao.objects.values('descricao').annotate(total=Count('descricao')).order_by('-total')[:5]

    return JsonResponse({
        'total_equipamentos_manutencao': total_equipamentos_manutencao,
        'equipamentos_manutencao': list(equipamentos_manutencao)
    })

"""TOTAL DE DEVOLUÇÕES E O MOTIVO DA DEVOLUÇÃO"""
def info_devolucao(request):
    total_devolucoes = Devolucao.objects.count()

    devolucoes = Devolucao.objects.values('descricao').annotate(total=Count('descricao')).order_by('-total')[:5]

    return JsonResponse({
        'total_devolucoes': total_devolucoes,
        'devolucoes': list(devolucoes)
    })



