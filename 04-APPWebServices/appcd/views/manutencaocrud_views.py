from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from appcd.forms import ManutencaoForm
from appcd.models import Manutencao
from django.contrib import messages

'''############### Acesso ao form (CRUD)  ##############################'''
@login_required
def novo_cadastro_manutencao(request):
    form_action = reverse('appcd:novo_cadastro_manutencao')

    if request.method == 'POST':
        form = ManutencaoForm(request.POST)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           manutencao = form.save()
           messages.success(request, 'Manutenção concluída')
           return redirect('appcd:atualizar_manutencao', manutencao_id=manutencao.pk)


        return render(request, 'paginas/criarmanutencao.html', context)

    context = {
        'form': ManutencaoForm(),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarmanutencao.html', context)

@login_required
def atualizar_manutencao(request, manutencao_id):
    manutencao = get_object_or_404(Manutencao, pk=manutencao_id)

    form_action = reverse('appcd:atualizar_manutencao', args=(manutencao_id,))

    if request.method == 'POST':
        form = ManutencaoForm(request.POST, instance=manutencao)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           manutencao = form.save()
           messages.success(request, 'Atualização concluída')
           return redirect('appcd:atualizar_manutencao', manutencao_id=manutencao.pk)


        return render(request, 'paginas/criarmanutencao.html', context)

    context = {
        'form': ManutencaoForm(instance=manutencao),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarmanutencao.html', context)
@login_required
def excluir_manutencao(request, manutencao_id):
    manutencao = get_object_or_404(Manutencao, pk=manutencao_id)

    manutencao.delete()
    messages.success(request, 'Você excluiu essa manutenção!')
    return redirect('appcd:manutencao')