from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import render
from appcd.models import Responsavel


'''############### Acesso aos Responsaveis (listar, ver, campo de pesquisa) ##############################'''
@login_required
def listar_responsavel(request):
    responsaveis = Responsavel.objects.all().order_by('responsavel_id')
    context = {'responsaveis': responsaveis}
    print(context)
    return render(request,'paginas/funcionarios.html', context)
@login_required
def ver_responsaveis(request, responsavel_id):
    verresponsaveis = Responsavel.objects.filter(pk=responsavel_id).first()

    if verresponsaveis is None:
        raise Http404

    context = {'verresponsaveis': verresponsaveis}
    print(context)
    return render(request,'paginas/funcionarios_unico.html', context)



