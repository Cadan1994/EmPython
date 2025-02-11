from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from app_devicehub.forms import DevolucaoForm
from app_devicehub.models import Devolucao
from django.contrib import messages

'''############### Acesso ao form (CRUD)  ##############################'''
@login_required
def novo_cadastro_devolucao(request):
    form_action = reverse('app_devicehub:novo_cadastro_devolucao')

    if request.method == 'POST':
        form = DevolucaoForm(request.POST)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           devolucao = form.save()
           messages.success(request, 'Devolução Concluída!')
           return redirect('app_devicehub:atualizar_devolucao', devolucao_id=devolucao.pk)


        return render(request, 'paginas/criardevolucao.html', context)

    context = {
        'form': DevolucaoForm(),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criardevolucao.html', context)

@login_required
def atualizar_devolucao(request, devolucao_id):
    devolucao = get_object_or_404(Devolucao, pk=devolucao_id)

    form_action = reverse('app_devicehub:atualizar_devolucao', args=(devolucao_id,))

    if request.method == 'POST':
        form = DevolucaoForm(request.POST, instance=devolucao)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           devolucao = form.save()
           messages.success(request, 'Atualização concluída')
           return redirect('app_devicehub:atualizar_devolucao', devolucao_id=devolucao.pk)


        return render(request, 'paginas/criardevolucao.html', context)

    context = {
        'form': DevolucaoForm(instance=devolucao),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criardevolucao.html', context)
@login_required
def excluir_devolucao(request, devolucao_id):
    devolucao = get_object_or_404(Devolucao, pk=devolucao_id)

    devolucao.delete()
    messages.success(request, 'Você excluiu essa devolução')
    return redirect('app_devicehub:devolucao')