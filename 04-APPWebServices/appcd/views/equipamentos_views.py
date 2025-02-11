from django.db.models import Q
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from appcd.models import Equipamento, Responsavel, Manutencao, Devolucao
from django.http import Http404


'''############### Acesso aos Equipamentos (listar, ver, campo de pesquisa) ##############################'''
@login_required
def listar_equipamento(request):
    equipamentos = Equipamento.objects.all().order_by('equipamento_id')
    context = {'equipamentos': equipamentos}
    print(context)
    return render(request,'paginas/dispositivos.html', context)

@login_required
def ver_equipamento(request, equipamento_id):
    verequipamentos = Equipamento.objects.filter(pk=equipamento_id).first()

    if verequipamentos is None:
        raise Http404

    context = {'verequipamentos': verequipamentos}
    print(context)
    return render(request,'paginas/unico.html', context)

@login_required
def search(request):
    search_value = request.GET.get('q', '').strip()

    if search_value == '':
        return redirect('appcd:home')

    context = {}  # Inicializa o dicionário de contexto vazio

    equipamentos = Equipamento.objects.filter(
        Q(marca__icontains=search_value) |
        Q(modelo__icontains=search_value) |
        Q(serial__icontains=search_value) |
        Q(imei1__icontains=search_value) |
        Q(imei2__icontains=search_value)
    ).order_by('equipamento_id')

    responsaveis = Responsavel.objects.filter(
        Q(nome__icontains=search_value) |
        Q(rg__icontains=search_value) |
        Q(cpf__icontains=search_value)
    ).order_by('responsavel_id')

    manutencoes = Manutencao.objects.filter(
        Q(responsavel_manutencao__nome__icontains=search_value) |
        Q(equipamento_manutencao__modelo__icontains=search_value)
    ).order_by('manutencao_id')

    devolucoes = Devolucao.objects.filter(
        Q(ultimo_responsavel__nome__icontains=search_value) |
        Q(ultimo_equipamento__modelo__icontains=search_value)
    ).order_by('devolucao_id')

    if equipamentos.exists():
        template = 'paginas/dispositivos.html'
        context['equipamentos'] = equipamentos
        context['search_value'] = search_value

    elif responsaveis.exists():
        template = 'paginas/funcionarios.html'
        context['responsaveis'] = responsaveis

    elif manutencoes.exists():
        template = 'paginas/manutencoes.html'
        context['manutencoes'] = manutencoes

    elif devolucoes.exists():
        template = 'paginas/devolucoes.html'
        context['devolucoes'] = devolucoes

    else:
        template = 'paginas/home.html'

    return render(request, template, context)