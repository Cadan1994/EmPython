from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from appcd.forms import EquipamentoForm
from appcd.models import Equipamento
from django.contrib import messages

'''############### Acesso ao form (CRUD)  ##############################'''
@login_required
def novo_cadastro_equipamento(request):
    form_action = reverse('appcd:novo_cadastro_equipamento')

    if request.method == 'POST':
        form = EquipamentoForm(request.POST)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           dispositivo = form.save()
           messages.success(request, 'Equipamento cadastrado!')
           return redirect('appcd:atualizar_equipamento', equipamento_id=dispositivo.pk)


        return render(request, 'paginas/criarequipamento.html', context)

    context = {
        'form': EquipamentoForm(),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarequipamento.html', context)

@login_required
def atualizar_equipamento(request, equipamento_id):
    dispositivo = get_object_or_404(Equipamento, pk=equipamento_id)

    form_action = reverse('appcd:atualizar_equipamento', args=(equipamento_id,))

    if request.method == 'POST':
        form = EquipamentoForm(request.POST, instance=dispositivo)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           dispositivo = form.save()
           messages.success(request, 'Atualização concluída')
           return redirect('appcd:atualizar_equipamento', equipamento_id=dispositivo.pk)


        return render(request, 'paginas/criarequipamento.html', context)

    context = {
        'form': EquipamentoForm(instance=dispositivo),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarequipamento.html', context)

@login_required
def excluir_equipamento(request, equipamento_id):
    dispositivo = get_object_or_404(Equipamento, pk=equipamento_id)

    dispositivo.delete()
    messages.success(request, 'Você excluiu esse equipamento!')
    return redirect('appcd:dispositivos')