from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import render
from appcd.models import Manutencao


'''############### Acesso aos Responsaveis (listar, ver, campo de pesquisa) ##############################'''
@login_required
def listar_manutencao(request):
    manutencoes = Manutencao.objects.all().order_by('manutencao_id')
    context = {'manutencoes': manutencoes}
    print(context)
    return render(request,'paginas/manutencao.html', context)
@login_required
def ver_manutencao(request, manutencao_id):
    vermanutencao = Manutencao.objects.filter(pk=manutencao_id).first()

    if vermanutencao is None:
        raise Http404

    context = {'vermanutencao': vermanutencao}
    print(context)
    return render(request,'paginas/manutencao_unico.html', context)
