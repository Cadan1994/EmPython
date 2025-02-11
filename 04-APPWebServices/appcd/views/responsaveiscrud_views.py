from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from appcd.models import Responsavel
from appcd.forms import ResponsavelForm
from django.contrib import messages

'''############### Acesso ao form (CRUD)  ##############################'''
@login_required
def novo_cadastro_responsavel(request):
    form_action = reverse('appcd:novo_cadastro_responsavel')

    if request.method == 'POST':
        form = ResponsavelForm(request.POST)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           responsavel = form.save()
           messages.success(request, 'Funcionário cadastrado!')
           return redirect('appcd:atualizar_responsavel', responsavel_id=responsavel.pk)


        return render(request, 'paginas/criarresponsavel.html', context)

    context = {
        'form': ResponsavelForm(),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarresponsavel.html', context)

@login_required
def atualizar_responsavel(request, responsavel_id):
    responsavel = get_object_or_404(Responsavel, pk=responsavel_id)

    form_action = reverse('appcd:atualizar_responsavel', args=(responsavel_id,))

    if request.method == 'POST':
        form = ResponsavelForm(request.POST, instance=responsavel)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           responsavel = form.save()
           messages.success(request, 'Atualização concluída')
           return redirect('appcd:atualizar_responsavel', responsavel_id=responsavel.pk)


        return render(request, 'paginas/criarresponsavel.html', context)

    context = {
        'form': ResponsavelForm(instance=responsavel),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criarresponsavel.html', context)

@login_required
def excluir_responsavel(request, responsavel_id):
    responsavel = get_object_or_404(Responsavel, pk=responsavel_id)

    responsavel.delete()
    messages.success(request, 'Você exluiu esse Funcionário!')
    return redirect('appcd:responsaveis')