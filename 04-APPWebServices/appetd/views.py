import re
import requests as req
import hashlib
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.db import transaction
from .models import *
from .models import SelectTablesDataExtraction


class Users(object):
    """
    USUÁRIOS
    """
    def __init__(self):
        self.GET = None
        self.POST = None
        self.session = None

    def login(self):
        """
        CHAMA TELA DE LOGIN
        ----------------------------------------------------------------------------------------------------------------
        Objective: CHAMAR A TELA DE LOGIN E AUTOMATICAMENTE DELETA O USUÁRIO DO SESSION.
        Create: HILSON SANTOS
        Data: 13/12/2024
        @return: CHAMA O TEMPLANTE "appetd-login.html".
        ----------------------------------------------------------------------------------------------------------------
        """
        self.session.flush()
        return render(self, template_name='appetd-login.html')

    def authentication(self):
        """
        AUTENTICAÇÃO DO USUÁRIO
        ----------------------------------------------------------------------------------------------------------------
        Objective: VERIFICAR SE O USUÁRIO PODE TER ACESSO AO SISTEMA.
        Create: HILSON SANTOS
        Date: 13/12/2024
        @return: CASO TRUE CHAMA A TELA PRINCIPAL DO SISTEMA "HOME", CASO CONTRÁRIO PERMACESSE NA TELA DE LOGIN.
        """
        username = self.GET.get('username')
        userpassword = self.GET.get('userpassword')

        if len(username.strip()) == 0 and len(userpassword.strip()) == 0:
            messages.info(self, message="Atenção, usuário e senha não informada!")
            return redirect(to="/appetd")
        # Se o usuário não estiver informado, envia uma mensagem.
        elif len(username.strip()) == 0:
            messages.info(self, message="Atenção, usuário não informado!")
            return redirect(to="/appetd")
        # Se a senha não estiver informado, envia uma mensagem.
        elif len(userpassword.strip()) == 0:
            messages.info(self, message="Atenção, senha não informado!")
            return redirect(to="/appetd")
        else:
            try:
                # Recebe os dados do usuário logado.
                user = User.objects.get(username=username)

                """userpassword = hashlib.md5(username.encode()).hexdigest().upper()"""

                # Recebe a informação do campo se está ativo.
                isactive = user.is_active

                # Se o usuário estiver inativo, envia uma mensagem.
                if not isactive:
                    messages.info(self, message="Atenção, usuário está inativo!")
                    return redirect("/appetd")
                else:
                    # Verifica as credenciais do usuário.
                    authentication = authenticate(username=username, password=userpassword)

                    # Se autenticidade for OK, entra no sistema, caso contrário, envia uma mensagem.
                    if authentication is not None:
                        self.session["username"] = username
                        return redirect("/appetd/home")
                    else:
                        messages.info(self, message="Atenção, senha inválida!")
                        return redirect("/appetd")
            except Exception as message:
                message = "Atenção, usuário informado não existe!"
                messages.info(self, message=message)
                return redirect("/appetd")

    def logoff(self):
        """
        SAIR
        ----------------------------------------------------------------------------------------------------------------
        Objetive: Retornar a tela de login.
        Create: HILSON SANTOS
        Date: 16/12/2024
        @return: Chama a templante "apppix-login.html"
        """
        self.session.flush()
        return redirect(to="/appetd")


class Home(object):
    """
    TELA PRINCIPAL
    --------------------------------------------------------------------------------------------------------------------
    Create: HILSON SANTOS
    Date: 12/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    def __init__(self):
        pass

    def home(self):
        """
        TELA PRINCIPAL DO SISTEMA
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        Create: HILSON SANTOS
        Date: 13/12/2024
        @return: Chama a templante "apppix-home.html" se true ou "apppix-login.html"
        ----------------------------------------------------------------------------------------------------------------
        """
        if self.session.get("username"):
            return render(self, template_name="appetd-home.html")
        else:
            messages.info(self, message="Você precisa fazer o login!")
            return redirect(to="/appetd")


"""
class Tabelas(object):
    def __init__(self):
        self.GET = None
        self.POST = None
        self.session = None

    def cadlist(self):
        if not self.session.get('usuario_id'):
            message = 'Precisa logar novamente no sistema.'
            return render(self, 'appetd-login.html', {'send': True, 'message': message})
        else:
            table_datawarehouse = TableDataWarehouse.objects.all()
            data = {'register': table_datawarehouse}
            print(data)
            return render(self, 'appetd-criar_tabelas.html', data)
    
    def cadinsert(self):
        try:
            # Inicia uma transção
            with transaction.atomic():
                var_name = self.POST.get('name')
                var_description = self.POST.get('description')
                var_script = self.POST.get('script')
                var_type = self.POST.get('type')
                var_status = self.POST.get('status')
                var_user = self.session.get('usuario_id')

                if var_status is None:
                    var_status = 'A'

                table_datawarehouse = TableDataWarehouse(
                    name=var_name.upper(),
                    description=var_description.upper(),
                    createscript=var_script.upper(),
                    type=var_type,
                    status=var_status,
                    alteruser=var_user
                )

                # Salva os dados no banco de dados
                table_datawarehouse.save()
                message = 'Registro salvo com sucesso.'
            return redirect('/appetd/tabelas/')
        except Exception as erro:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {erro}'
            return redirect('/appetd/tabelas/')

    def cadupdate(self):
        try:
            # Inicia uma transção
            with transaction.atomic():
                var_id = self.POST.get('id')
                var_name = self.POST.get('name')
                var_description = self.POST.get('description')
                var_script = self.POST.get('createscript')
                var_type = self.POST.get('type')
                var_status = self.POST.get('status')
                var_user = self.session.get('usuario_id')
                # Supondo que você tenha um objeto existente que deseja modificar
                table_datawarehouse = TableDataWarehouse.objects.get(pk=var_id)
                # Modificar os campos conforme necessário
                table_datawarehouse.name = var_name.upper()
                table_datawarehouse.description = var_description.upper()
                table_datawarehouse.createscript = var_script.upper()
                table_datawarehouse.type = var_type
                table_datawarehouse.status = var_status
                table_datawarehouse.alteruser = var_user
                # Salva as alterações no banco de dados
                table_datawarehouse.save()

            return redirect('/appetd/tabelas/')
        except Exception as erro:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {erro}'
            return redirect('/appetd/tabelas/')

    def caddelete(self):
        try:
            # Inicia uma transção
            with transaction.atomic():
                var_id = self.POST.get('id')
                # Supondo que você tenha um objeto existente que deseja modificar
                table_datawarehouse = TableDataWarehouse.objects.get(pk=var_id)
                # Salva as alterações no banco de dados
                table_datawarehouse.delete()

            return redirect('/appetd/tabelas/')
        except Exception as e:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {e}'
            return redirect('/appetd/tabelas/')


class Selects(object):
    def __init__(self):
        self.GET = None
        self.POST = None
        self.session = None

    def cadlist(self):
        if not self.session.get('usuario_id'):
            message = 'Precisa logar novamente no sistema'
            return render(self, 'login.html', {'send': True, 'message': message})
        else:
            table_datawarehouse = TableDataWarehouse.objects.all()
            selects_tablesdataextraction = SelectTablesDataExtraction.objects.all()
            data = {
                'register': selects_tablesdataextraction,
                'table': table_datawarehouse
            }

            return render(self, 'appetd-criar_selects.html', data)

    def cadinsert(self):
        try:
            # Inícia uma transação
            with transaction.atomic():
                var_name = self.POST.get('name')
                var_description = self.POST.get('description')
                var_type = self.POST.get('type')
                var_status = self.POST.get('status')
                var_selectini = self.POST.get('selectinitial')
                var_selectpar = self.POST.get('selectpartial')
                var_selectnoyes = self.POST.get('selectnoyes')
                var_user = self.session.get('usuario_id')
                tableid = self.POST.get('table')
                padrao = re.compile('(.*?)-')
                resultado = padrao.match(tableid)
                var_table = resultado.group(1)

                # Se var_status for null recebe "A"
                if var_status is None:
                    var_status = 'A'

                # Se var_selectnoyes for null recebe "S"
                if var_selectnoyes is None:
                    var_selectnoyes = 'S'

                # Adiciona as variáveis aos compos para inserir os dados
                selects_tablesdataextraction = SelectTablesDataExtraction(
                    table=var_table,
                    name=var_name.upper(),
                    description=var_description.upper(),
                    selectinitial=var_selectini.upper(),
                    selectpartial=var_selectpar.upper(),
                    partial=var_selectnoyes,
                    type=var_type,
                    status=var_status,
                    alteruser=var_user
                )

                # Salva os dados no banco de dados
                selects_tablesdataextraction.save()
                message = 'Registro salvo com sucesso.'

            return redirect('/appetd/selects/')
        except Exception as erro:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {erro}'

    def cadupdate(self):
        try:
            # Inícia uma transação
            with transaction.atomic():
                var_id = self.POST.get('id')
                var_name = self.POST.get('name')
                var_description = self.POST.get('description')
                var_type = self.POST.get('type')
                var_status = self.POST.get('status')
                var_selectini = self.POST.get('selectinitial')
                var_selectpar = self.POST.get('selectpartial')
                var_selectnoyes = self.POST.get('selectnoyes')
                var_user = self.session.get('usuario_id')
                tableid = self.POST.get('table')
                padrao = re.compile('(.*?)-')
                resultado = padrao.match(tableid)
                var_table = resultado.group(1)

                # Se var_status for null recebe "A"
                if var_status is None:
                    var_status = 'A'

                # Se var_selectnoyes for null recebe "S"
                if var_selectnoyes is None:
                    var_selectnoyes = 'S'

                # Supondo que você tenha um objeto existente que deseja modificar
                selects_tablesdataextraction = SelectTablesDataExtraction.objects.get(pk=var_id)
                # Modificar os campos conforme necessário
                selects_tablesdataextraction.table = var_table
                selects_tablesdataextraction.name = var_name.upper()
                selects_tablesdataextraction.description = var_description.upper()
                selects_tablesdataextraction.selectinitial = var_selectini.upper()
                selects_tablesdataextraction.selectpartial = var_selectpar.upper()
                selects_tablesdataextraction.selectnoyes = var_selectnoyes
                selects_tablesdataextraction.type = var_type
                selects_tablesdataextraction.status = var_status
                selects_tablesdataextraction.alteruser = var_user
                # Salva as alterações no banco de dados
                selects_tablesdataextraction.save()

            return redirect('/appetd/selects/')
        except Exception as erro:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {erro}'

    def caddelete(self):
        try:
            # Inicia uma transção
            with transaction.atomic():
                var_id = self.POST.get('id')
                # Supondo que você tenha um objeto existente que deseja modificar
                selects_tablesdataextraction = SelectTablesDataExtraction.objects.get(pk=var_id)
                # Salva as alterações no banco de dados
                selects_tablesdataextraction.delete()

            return redirect('/appetd/selects/')
        except Exception as e:
            # Se ocorreu uma exceção, realiza o rollback manualmente
            transaction.set_rollback(True)
            message = f'Erro: {e}'
            return redirect('/appetd/selects/')


def settings(request):
    return render(request, 'appetd-configuracoes.html')
"""