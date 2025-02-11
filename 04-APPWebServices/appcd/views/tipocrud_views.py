from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from appcd.models import Tipo
from appcd.forms import TipoForm
from django.contrib import messages

'''############### Acesso ao form (CRUD)  ##############################'''
@login_required
def novo_cadastro_tipo(request):
    form_action = reverse('appcd:novo_cadastro_tipo')

    if request.method == 'POST':
        form = TipoForm(request.POST)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           tipo = form.save()
           messages.success(request, 'Tipo cadastrado')
           return redirect('appcd:atualizar_tipo', tipo_id=tipo.pk)


        return render(request, 'paginas/criartipo.html', context)

    context = {
        'form': TipoForm(),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criartipo.html', context)

@login_required
def atualizar_tipo(request, tipo_id):
    tipo = get_object_or_404(Tipo, pk=tipo_id)

    form_action = reverse('appcd:atualizar_tipo', args=(tipo_id,))

    if request.method == 'POST':
        form = TipoForm(request.POST, instance=tipo)
        context = {
            'form': form,
            'form_action': form_action,
        }
        print(context)

        if form.is_valid():
           tipo = form.save()
           messages.success(request, 'Atualização concluída')
           return redirect('appcd:atualizar_tipo', tipo_id=tipo.pk)


        return render(request, 'paginas/criartipo.html', context)

    context = {
        'form': TipoForm(instance=tipo),
        'form_action': form_action,
    }
    print(context)
    return render(request, 'paginas/criartipo.html', context)

@login_required
def excluir_tipo(request, tipo_id):
    tipo = get_object_or_404(Tipo, pk=tipo_id)

    tipo.delete()
    messages.success(request, 'Você excluiu esse Tipo!')
    return redirect('appcd:tipo')