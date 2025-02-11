from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import render
from app_devicehub.models import Tipo


'''############### Acesso aos Tipos (listar, ver, campo de pesquisa) ##############################'''
@login_required
def listar_tipo(request):
    tipos = Tipo.objects.all().order_by('tipo_id')
    context = {'tipos': tipos}
    print(context)
    return render(request,'paginas/tipo.html', context)

@login_required
def ver_tipo(request, tipo_id):
    vertipo = Tipo.objects.filter(pk=tipo_id).first()

    if vertipo is None:
        raise Http404

    context = {'vertipo': vertipo}
    print(context)
    return render(request,'paginas/tipo_unico.html', context)