from django.shortcuts import redirect, render
from app_devicehub.forms import RegisterForm, RegisterUpdateForm
from django.contrib.auth.forms import AuthenticationForm
from django.contrib import auth
from django.contrib import messages

def register(request):
    form = RegisterForm

    if request.method == 'POST':
        form = RegisterForm(request.POST)

        if form.is_valid():
            form.save()
            messages.success(request, 'Usuário registrado')
            return redirect('app_devicehub:login')

    return render(
        request, 'paginas/criarUser.html',
        {
            'form': form
        }
    )
def usuario_atualizar(request):
    form = RegisterUpdateForm(instance=request.user)
    
    if request.method != 'POST':
       return render(
          request, 'paginas/User_atualizar.html',
          {
              'form': form
          }
       )
       
    form = RegisterUpdateForm(data=request.POST, instance=request.user)

    if not form.is_valid():
        return render(
            request, 'paginas/User_atualizar.html',
            {
                'form': form
            }
        )
    form.save()
    return redirect('app_devicehub:usuario_atualizar')

def login_admin(request):
    form = AuthenticationForm(request)

    if request.method == 'POST':
        form = AuthenticationForm(request, data=request.POST)

        if form.is_valid():
            user = form.get_user()
            auth.login(request, user)
            messages.success(request, 'Logado com sucesso!')
            return redirect('app_devicehub:home')
        else:
            messages.error(request, 'Login inválido')
    return render(
        request, 'paginas/login1.html',
        {
            'form': form
        }
    )

def sair(request):
    auth.logout(request)
    return redirect('app_devicehub:login')
