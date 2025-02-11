from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from app_devicehub.models import *
from django.http import Http404

'''############### Acesso aos Equipamentos (listar, ver, campo de pesquisa) ##############################'''
@login_required
def listar_devolucao(request):
    devolucoes = Devolucao.objects.all().order_by('devolucao_id')
    context = {'devolucoes': devolucoes}
    print(context)
    return render(request,'paginas/devolucao.html', context)
@login_required
def ver_devolucao(request, devolucao_id):
    verdevolucao = Devolucao.objects.filter(pk=devolucao_id).first()

    if verdevolucao is None:
        raise Http404

    context = {'verdevolucao': verdevolucao}
    print(context)
    return render(request,'paginas/devolucao_unico.html', context)