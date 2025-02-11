from django.db.models import Q
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from app_devicehub.models import Equipamento, Responsavel, Manutencao, Devolucao
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
        return redirect('app_devicehub:home')

    # Filtrando Equipamentos
    equipamentos = Equipamento.objects \
        .filter(
            Q(marca__icontains=search_value) |
            Q(modelo__icontains=search_value) |
            Q(serial__icontains=search_value) |
            Q(imei1__icontains=search_value) |
            Q(imei2__icontains=search_value)
        ) \
        .order_by('equipamento_id')

    # Filtrando Responsáveis
    responsaveis = Responsavel.objects \
        .filter(
            Q(nome__icontains=search_value) |
            Q(rg__icontains=search_value) |
            Q(cpf__icontains=search_value)
        ) \
        .order_by('responsavel_id')

    # Filtrando Manutenções
    manutencoes = Manutencao.objects \
        .filter(
            Q(descricao__icontains=search_value)
        ) \
        .order_by('manutencao_id')

    # Filtrando Devoluções
    devolucoes = Devolucao.objects \
        .filter(
            Q(descricao__icontains=search_value)
        ) \
        .order_by('devolucao_id')

    context = {
        'equipamentos': equipamentos,
        'responsaveis': responsaveis,
        'manutencoes': manutencoes,
        'devolucoes': devolucoes,
    }

    return render(
        request,
        'paginas/dispositivos.html',
        context
    )