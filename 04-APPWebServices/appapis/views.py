import requests
import pandas as pd
import cx_Oracle as ora
import jwt
import json
import hashlib
import os
import tempfile
import datetime
import locale
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpRequest, HttpResponse, JsonResponse
from http.server import BaseHTTPRequestHandler
from cryptography.fernet import Fernet
from requests.auth import HTTPBasicAuth
from fpdf import FPDF
from .connectdb import ConnectDatabase
from .querys import *


# variável recebe o caminho que vai ser armazenado a chave e o token #
TEMPORATY_DIR = tempfile.gettempdir()


class ParametersService(object):
    """
    API LISTAR OS PARÂMETROS
    --------------------------------------------------------------------------------------------------------------------
    Objective: Selecionar os parãmetros
    Create: HILSON SANTOS
    Date: 11/11/2024
    """
    @staticmethod
    def select_data_table_parameters():
        """
        PARÂMETROS DE DADOS
        --------------------------------------------------------------------------------------------------------------------
        Objective: Selecionar os dados da tabela "CADAN_PIXPARAMENTROS"
        Create: HILSON SANTOS
        Date: 06/11/2024
        @return: JSON com os dados da tabela
        --------------------------------------------------------------------------------------------------------------------
        """
        try:
            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            query = \
                """
                SELECT
                    LOWER(id) 
                    AS id,
                    valor
                FROM implantacao.cadan_pixparametros
                WHERE 1=1
                """
            cur.execute(query)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            dfdata = pd.DataFrame.from_records(data=data, columns=columns)
            dfdata = dfdata.astype(str)
            data_dictionary = dict(
                zip(
                    list(dfdata['ID']),
                    list(dfdata["VALOR"])
                )
            )

            return JsonResponse(data_dictionary)
        except ora.DatabaseError as Error:
            message = {"message": Error}
            return JsonRespose(message)


class TokenService(object):
    """
    API DE GERAR O TOKEN COM AS CREDENCIAIS
    --------------------------------------------------------------------------------------------------------------------
    Objective: É de gerar e criptografar o "token" no diretório TEMP do usuário
    create: HILSON SANTOS
    @date: 30/10/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    def __init__(self, usercompany, username, userpassword):
        self.usercompany = usercompany
        self.username = username
        self.userpassword = userpassword

    @csrf_exempt
    def create_token(self):
        """
        ESSA FUNÇÃO TEM O OBJETIVO DE VERIFICA AS CREDENCIAIS DO USUÁRIO PARA CONSUMIR AS API'S
        ----------------------------------------------------------------------------------------------------------------
        create: Hilson Santos
        date: 30/10/2024
        params request: USEREMPRESA, USERNAME e USERPASSWORD do usuário
        @return: Dicionário de dados do “TOKEN”
        ----------------------------------------------------------------------------------------------------------------
        """
        # As variáveis recebem os seguintes parâmetros informados: id, login e password #
        usercompany = self.usercompany
        username = self.username
        userpassword = self.userpassword

        # A variável recebe a instância da classe #
        oracle = ConnectDatabase()

        # Se conectar com o ORACLE e executa a query "consulta" #
        conn = oracle.conn_oracle()
        cur = conn.cursor()

        # Executa a query "SELECT" de cadastro de usuários #
        query = \
        f"""
        SELECT 
            userid, 
            userempresa, 
            userlogin, 
            usernome, 
            usersenha 
        FROM implantacao.cadan_pixusuarios
        WHERE 1=1
        AND userempresa = '{usercompany}'
        AND userlogin = '{username}'
        AND usersenha = '{hashlib.md5(userpassword.encode()).hexdigest().upper()}'
        """
        cur.execute(query)

        # A variável recebe os nomes dos campos #
        columns = [row[0] for row in cur.description]

        # A variável recebe os dados #
        data = cur.fetchall()

        # A variável recebe o DATAFRAME do cadastro de usuário #
        dfusuario = pd.DataFrame.from_records(data=data, columns=columns)
        dfusuario = dfusuario.astype(str)

        # Se a quantidade de usuário for 0 com os filtros informado
        if len(dfusuario) == 0:
            # A variável receber uma mensagem no formato JSON para ser enviado ao usuário #
            message = JsonResponse({
                "status_code": "401",
                "error": "invalid_credentials",
                "message": "As credenciais fornecidas não correspondem a nenhum usuário de acesso conhecido pelo sistema."
            })

            # A variável recebe o conteúdo do corpo da resposta em bytes #
            response_content = message.content

            # A variável recebe a conversão de bytes em strings #
            response_decode = response_content.decode(encoding="utf-8")

            # A variável recebe uma lista com o JSON em string convertido em formato JSON #
            response_json = json.loads(response_decode)

            return JsonResponse(response_json)
        else:
            # A variável recebe o usuário do DATAFRAME em string no formato JSON #
            dfusuario = dfusuario.to_json(orient='records')

            # A variável recebe uma lista com o JSON em string convertido em formato JSON #
            jsonusuarioloads = json.loads(dfusuario)

            # A variável recebe o JSON de uma lista #
            jsonusuario = jsonusuarioloads[0]

            # As variáveis recebe os dados do usuário: USERNAME, USERSENHA
            user_login = jsonusuario["USERNOME"]
            user_password = jsonusuario["USERSENHA"]
            user_file = hashlib.md5(user_login.encode()).hexdigest()

            # A variável receber o caminho do diretório temp do usuário do windows #
            file_path = f"{TEMPORATY_DIR}/{user_file}.tmp"
            file_path_key = f"{TEMPORATY_DIR}/{user_file}.key"

            # Se exitir usuário executa o bloco #
            shalogin = hashlib.sha256(user_login.encode()).hexdigest()
            shapassword = hashlib.sha256(user_password.encode()).hexdigest()

            # Verifica se o arquivo existe se não existir cria uma chave ou carrega uma chave existente
            if not os.path.isfile(file_path):
                # A variável recebe a chave criada e guarda no arquivo #
                key = Fernet.generate_key()
                with open(file_path_key, "w") as write_file:
                    write_file.write(key.decode())
                    write_file.close()
            else:
                with open(file_path_key, "r") as write_file:
                    # A variável carrega uma chave existente #
                    key = write_file.read()
                    write_file.close()

            # A variável recebe a chave #
            fernet = Fernet(key)

            # Se não exitir o arquivo do token ou se o arquivo do token estiver em branco #
            if not os.path.isfile(file_path) or os.path.getsize(file_path) == 0:
                # A variável recebe o USERNAME e USERSENHA criptografado no hash sha256 #
                secret_key = shalogin + shapassword

                # É o corpo da mensagem que carrega os dados que uma aplicação envia ou recebe de outra, geralmente no #
                # formato JSON ou XML. #
                payload = {
                    "sub": usercompany,
                    "iss": username,
                    "iat": datetime.datetime.utcnow()
                }

                # É a informações sobre o algoritmo de criptografia e o tipo de token. #
                headers = {
                    "alg": "HS256",
                    "typ": "JWT"
                }

                # A variável recebe a geração do token criptografado #
                token = jwt.encode(payload=payload, key=secret_key, algorithm="HS256", headers=headers)

                # A variável recebe o token criptografado em bytes #
                encrypted_token = fernet.encrypt(token.encode())

                # Se o arquivo não existir se não altera o arquivo com o token gerado.
                if not os.path.isfile(file_path):
                    with open(file_path, "w") as write_file:
                        write_file.write(str(encrypted_token.decode("utf-8")))
                        write_file.close()
                elif os.path.getsize(file_path) == 0:
                    with open(file_path, "a") as write_file:
                        write_file.write(str(encrypted_token.decode("utf-8")))
                        write_file.close()

                # A variável recebe o resultado da validação do token #
                token_validation = TokenService.validate_token(token=token, login=user_login, password=user_password)

                # A variável recebe o conteúdo do corpo da resposta em bytes #
                response_content = token_validation.content

                # A variável recebe a conversão de bytes em strings #
                response_decode = response_content.decode(encoding="utf-8")

                # A variável recebe uma lista com o JSON em string convertido em formato JSON #
                response_json = json.loads(response_decode)

                return JsonResponse(response_json)
            else:
                # Abre o arquivo do token como leitura e atribui a variável "token" #
                with open(file_path, 'r') as write_file:
                    token = write_file.read()

                # A variável recebe o valor do token do arquivo lido #
                token_encode = token

                # A variável recebe o token decriptografado #
                decrypted_token = fernet.decrypt(token_encode).decode(encoding="utf-8")

                # A variável recebe o resultado da validação do token #
                token_validation = TokenService.validate_token(token=decrypted_token, login=user_login, password=user_password)

                # A variável recebe o conteúdo do corpo da resposta em bytes #
                response_content = token_validation.content

                # A variável recebe a conversão de bytes em strings #
                response_decode = response_content.decode(encoding="utf-8")

                # A variável recebe uma lista com o JSON em string convertido em formato JSON #
                response_json = json.loads(response_decode)

                # A variável recebe o status code no filtrada no JSON #
                status_code = response_json["status_code"]

                # Se o status code for diferente de 200 a variável "erro" recebe a descrição do erro se o arquivo existir #
                # o arquivo é deletado #
                if int(status_code) != 200:
                    erro = response_json["error"]
                    if erro == "expired_signature_error":
                        if os.path.exists(file_path):
                            os.remove(file_path)

                    # A variável recebe o USERNAME e USERSENHA criptografado no hash sha256 #
                    secret_key = shalogin + shapassword

                    # É o corpo da mensagem que carrega os dados que uma aplicação envia ou recebe de outra, geralmente no formato JSON ou XML. #
                    payload = {
                        "sub": usercompany,
                        "iss": username,
                        "iat": datetime.datetime.utcnow()
                    }

                    # É a informações sobre o algoritmo de criptografia e o tipo de token.
                    headers = {
                        "alg": "HS256",
                        "typ": "JWT"
                    }

                    # A variável recebe a geração do token criptografado #
                    token = jwt.encode(payload=payload, key=secret_key, algorithm="HS256", headers=headers)

                    # A variável recebe o token criptografado em bytes #
                    encrypted_token = fernet.encrypt(token.encode())

                    # Cria um arquivo com o token armazenado #
                    with open(file_path, "w") as write_file:
                        write_file.write(str(encrypted_token.decode("utf-8")))
                        write_file.close()

                    # Abre o arquivo do token como leitura e atribui a variável "token" #
                    with open(file_path, 'r') as write_file:
                        token = write_file.read()

                    # A variável recebe o valor do token do arquivo lido #
                    token_encode = token

                    # A variável recebe o token decriptografado #
                    decrypted_token = fernet.decrypt(token_encode).decode(encoding="utf-8")

                    # A variável recebe o resultado da validação do token #
                    token_validation = TokenService.validate_token(token=decrypted_token, login=user_login, password=user_password)

                    # A variável recebe o conteúdo do corpo da resposta em bytes #
                    response_content = token_validation.content

                    # A variável recebe a conversão de bytes em strings #
                    response_decode = response_content.decode(encoding="utf-8")

                    # A variável recebe uma lista com o JSON em string convertido em formato JSON #
                    response_json = json.loads(response_decode)

                return JsonResponse(response_json)

    @staticmethod
    def validate_token(token, login, password):
        """
        ESSA FUNÇÃO TEM O OBJETIVO DE VALIDAR O “TOKEN” DO USUÁRIO ATRAVÉS DO NOME E SENHA
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        create: Hilson Santos
        date: 30/10/2024
        params: TOKEN, LOGIN e PASSWORD do usuário logado
        @return: O resultado no formato JSON com os dados
        ----------------------------------------------------------------------------------------------------------------
        """
        try:
            # As variáveis recebem o USERNAME e USERSENHA do usuário #
            shalogin = hashlib.sha256(login.encode()).hexdigest()
            shapassword = hashlib.sha256(password.encode()).hexdigest()

            # A variável recebe o USERNAME e USERSENHA criptografado no hash sha256 #
            secret_key = shalogin + shapassword

            # Decodifica o token, que normalmente contém informações do usuário ou dados de sessão. #
            jwt.decode(jwt=token, key=secret_key, algorithms=["HS256"])

            # A variável recebe a saída que será um dicionário com os dados do payload JWT #
            decoded_token = dict(
                status_code=str("200"),
                acess_token=str(token),
                secret_key=str(secret_key),
                login=str(shalogin),
                password=str(shapassword),
                message="Seu token foi gerado com sucesso."
            )
        except jwt.ExpiredSignatureError:
            decoded_token = {
                "status_code": "401",
                "error": "expired_signature_error",
                "message": "Seu token de sessão expirou. Por favor, atualize o token."
            }
        except jwt.InvalidSignatureError:
            decoded_token = {
                "status_code": "401",
                "error": "invalid_signature_error",
                "message": "O token fornecido tem uma assinatura invalida."
            }
        except jwt.InvalidKeyError:
            decoded_token = {
                "status_code": "401",
                "error": "invalid_key_error",
                "message": "A chave de acesso ou token fornecido não corresponde ao esperado pelo sistema."
            }
        except jwt.DecodeError:
            decoded_token = {
                "status_code": "401",
                "error": "invalid_key_error",
                "message": "Falha na verificacao da assinatura."
            }

        return JsonResponse(decoded_token)

    def access_token(self):
        usercompany = self.GET.get("usercompany")
        username = self.GET.get("username")
        userpassword = self.GET.get("userpassword")

        token = TokenService(usercompany=usercompany, username=username, userpassword=userpassword)
        validate_token = token.create_token()
        response_content = validate_token.content
        response_json = json.loads(response_content)
        return JsonResponse(response_json)


class AutomaticUserService(object):
    """
    API DO SERVIÇO AUTOMÁTICO PARA IMPORTAÇÃO DOS USUÁRIOS
    --------------------------------------------------------------------------------------------------------------------
    Objective: É de inserir os usuários cadastrados na tabela "GE_USUARIOS" para a tabela "CADAN_PIXUSUARIOS
    create: HILSON SANTOS
    date: 05/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    @staticmethod
    def import_users():
        """
        ESSA FUNÇÃO IMPORTA OS USUARIOS
        ----------------------------------------------------------------------------------------------------------------
        Objective: É responsável de importar os usuários cadastrados na tabela "GE_USUARIOS" para "CADAN_PIXUSUARIOS"
        Create: HILSON SANTOS
        Date: 05/11/2024
        @return: Uma mensagem e envia para o arquivo de log "appapis.log"
        ----------------------------------------------------------------------------------------------------------------
        """
        datetime_now = datetime.datetime.now()
        datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
        file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as write_file:
                write_file.close()

        oracle = ConnectDatabase()
        conn = oracle.conn_oracle()
        cur = conn.cursor()
        try:
            cur.execute(GE_USUARIOS)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            if len(data) != 0:
                dfusuarios = pd.DataFrame.from_records(data=data, columns=columns)
                dfusuarios.columns = dfusuarios.columns.str.lower()
                dfusuarios = dfusuarios.astype(str)
                data_dictionary = dfusuarios.to_dict("records")
                data_dumps = json.dumps(data_dictionary)
                data_dict = {"data": json.loads(data_dumps)}
                for record in data_dict["data"]:
                    userid = record.get("sequsuario")
                    userempresa = record.get("nroempresa")
                    userlogin = record.get("codusuario")
                    usernome = record.get("nome")
                    senhapadrao = "cadan@123"
                    usersenha = hashlib.md5(senhapadrao.encode()).hexdigest().upper()
                    value = \
                        f"""
                        ( 
                        '{str(userid)}',
                        '{str(userempresa)}',
                        '{str(userlogin)}',
                        '{str(usernome)}',
                        '{str(usersenha)}',
                        SYSDATE,
                        SYSDATE
                        )
                        """

                    insert = \
                        f"""
                         INSERT INTO implantacao.cadan_pixusuarios(
                            userid, 
                            userempresa, 
                            userlogin, 
                            usernome, 
                            usersenha,
                            userdtacadastro,
                            userdtaimportacao
                          )
                         VALUES {value}
                         """
                    cur.execute(insert)
                conn.commit()
                cur.close()
                message = f"{datetime_format}: Tabela PIXUSUARIOS - Processo realizado com sucesso.\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
            else:
                message = f"{datetime_format}: Tabela PIXUSUARIOS - A solicitação foi bem-sucedida, mas não há pedidos a serem importados.\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
        except ora.DatabaseError as Error:
            conn.rollback()
            cur.close()
            message = f"{datetime_format}: {Error}\n"
            with open(file_path, "a") as write_file:
                write_file.write(str(message))
                write_file.close()

    def query_user(self):
        usercompany = self.GET.get("usercompany")
        username = self.GET.get("username")
        userpassword = self.GET.get("userpassword")
        oracle = ConnectDatabase()
        conn = oracle.conn_oracle()
        cur = conn.cursor()
        try:
            query = \
                f"""
                SELECT 
                    userid, 
                    userempresa, 
                    userlogin, 
                    usernome, 
                    usersenha 
                FROM implantacao.cadan_pixusuarios	
                WHERE userempresa = '{usercompany}' 
                AND userlogin = '{username}' 
                AND usersenha = '{userpassword}' 
                """
            cur.execute(query)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            dfusuarios = pd.DataFrame.from_records(data=data, columns=columns)
            dfusuarios.columns = dfusuarios.columns.str.lower()
            dfusuarios = dfusuarios.astype(str)
            data_dictionary = dfusuarios.to_dict("records")
            data_dumps = json.dumps(data_dictionary)
            data_dict = {"data": json.loads(data_dumps)}
            size_data = len(data_dict["data"])
            if size_data != 0:
                message = data_dict
            else:
                message = {"Mensagem": "Usuário não localizado!"}
            cur.close()
            return JsonResponse(message)
        except ora.DatabaseError as Error:
            cur.close()
            return JsonResponse({"Erro": f"{Error}"})


class AutomaticOrderService(object):
    """
    API DO SERVIÇO AUTOMÁTICO DA IMPORTAÇÃO DOS PEDIDOS
    --------------------------------------------------------------------------------------------------------------------
    Objective: Importar pedidos da tabela "MAD_PEDVENDA" para "CADAN_PIXPEDIDOS" e consultar os PIX enviados ao banco
               com status "CONCLUIDO" pelo TXID.
    create: HILSON SANTOS
    date:  01/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    @staticmethod
    def import_orders():
        """
        IMPORTAR PEDIDOS
        ----------------------------------------------------------------------------------------------------------------
        Objective: Importar os pedidos em análise da tabela "MAD_PEDVENDA" para "CADAN_PIXPEDIDOS"
        Create: HILSON SANTOS
        Date: 01/11/2024
        @return: Uma mensagem e envia para o arquivo de log "appapis.log"
        """
        datetime_now = datetime.datetime.now()
        datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
        file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as write_file:
                write_file.close()

        oracle = ConnectDatabase()
        conn = oracle.conn_oracle()
        cur = conn.cursor()
        try:
            cur.execute(MAD_PEDVENDA)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            if len(data) != 0:
                dfpedidos = pd.DataFrame.from_records(data=data, columns=columns)
                dfpedidos.columns = dfpedidos.columns.str.lower()
                dfpedidos = dfpedidos.astype(str)
                data_dictionary = dfpedidos.to_dict("records")
                data_dumps = json.dumps(data_dictionary)
                data_disc = {"data": json.loads(data_dumps)}
                for record in data_disc["data"]:
                    pedempresa = record.get("nroempresa")
                    pednumero = record.get("nropedvenda")
                    pedcliente = record.get("seqpessoa")
                    pedrepresentante = record.get("nrorepresentante")
                    pedentregaretira = record.get("indentregaretira")
                    pedsituacao = record.get("situacaoped")
                    pedvalor = record.get("vlrpedido")
                    peddtainclusao = record.get("dtainclusao")
                    peddtafaturamento = record.get("dtabasefaturamento")
                    value = \
                        f"""
                        ( 
                        '{str(pedempresa)}',
                        '{str(pednumero)}',
                        '{str(pedcliente)}',
                        '{str(pedrepresentante)}',
                        '{str(pedentregaretira)}',
                        '{str(pedsituacao)}',
                        '{str(pedvalor)}',
                        TO_TIMESTAMP('{str(peddtainclusao)}','YYYY-MM-DD HH24:MI:SS'),
                        TO_TIMESTAMP('{str(peddtafaturamento)}','YYYY-MM-DD HH24:MI:SS'),
                        SYSDATE
                        )
                        """

                    insert = \
                        f"""
                         INSERT INTO implantacao.cadan_pixpedidos(
                            nroempresa, 
                            nropedvenda, 
                            seqpessoa, 
                            nrorepresentante, 
                            indentregaretira,
                            situacaoped, 
                            vlrpedido,
                            dtainclusao,
                            dtabasefaturamento,
                            dtaimportacao
                          )
                         VALUES {value}
                         """
                    cur.execute(insert)
                conn.commit()
                cur.close()
                message = f"{datetime_format}: Tabela PIXPEDIDOS - Processo realizado com sucesso.\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
            else:
                message = f"{datetime_format}: Tabela PIXPEDIDOS - A solicitação foi bem-sucedida, mas não há pedidos a serem importados.\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
        except ora.DatabaseError as Error:
            conn.rollback()
            cur.close()
            message = f"{datetime_format}: {Error}\n"
            with open(file_path, "a") as write_file:
                write_file.write(str(message))
                write_file.close()

    @staticmethod
    def consult_orders():
        """
        CONSULTAR PEDIDOS
        ----------------------------------------------------------------------------------------------------------------
        Objetive: consultar os PIX enviados ao banco com status "ATIVA" pelo TXID.
        Create: HILSON SANTOS
        Date: 07/11/2024
        @return: Uma mensagem e envia para o arquivo de log "appapis.log"
        ----------------------------------------------------------------------------------------------------------------
        """
        datetime_now = datetime.datetime.now()
        datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
        file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
        pixparameters = ParametersService.select_data_table_parameters()
        pixparameters_content = pixparameters.content
        pixparameters_json = json.loads(pixparameters_content)
        url_token = pixparameters_json["bbrasil_url_token"]
        url_apipix = pixparameters_json["bbrasil_url_apipix"]
        client_id = pixparameters_json["bbrasil_client_id"]
        client_secret = pixparameters_json["bbrasil_client_secret"]
        authorization = pixparameters_json["bbrasil_authorization"]
        gw_dev_app_key = pixparameters_json["bbrasil_gw_dev_app_key"]
        params = {
            "grant_type": "client_credentials",
            "scope": "cob.read cob.write cobv.read cobv.write pix.read pix.write"
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"{authorization}"

        }
        response = requests.post(
            url=url_token,
            headers=headers,
            params=params,
            auth=HTTPBasicAuth(username=f"{client_id}", password=f"{client_secret}"),
            timeout=10
        )
        if response.status_code == 200:
            response_json = response.json()
            token = response_json["access_token"]

            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            query = \
                """
                SELECT 
                    DISTINCT
                    'txid' as id,
                    txid
                FROM implantacao.cadan_pixpedidos 
                WHERE 1=1
                AND situacaoped = 'E'
                """
            cur.execute(query)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            if len(data) != 0:
                dftxid = pd.DataFrame.from_records(data=data, columns=columns)
                dftxid.columns = dftxid.columns.str.lower()
                dftxid = dftxid.to_dict('records')

                if dftxid[0]["txid"] is not None:
                    list_txid = list()

                    for txid in dftxid:
                        list_txid.append(txid)

                    headers = {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {token}"
                    }
                    list_pix = list()
                    for txid in list_txid:
                        params = {
                            "gw-dev-app-key": f"{gw_dev_app_key}"
                        }
                        valuetxid = txid["txid"]
                        response = requests.get(url=f"{url_apipix}/cob/{valuetxid}", headers=headers, params=params)
                        response_json = response.json()
                        response_pix = response_json["pix"]
                        response_size = len(response_pix)
                        if response_size != 0:
                            list_pix.append(response_pix[0])

                    list_pix_size = len(list_pix)
                    if list_pix_size != 0:
                        try:
                            # ESSE LOOP É RESPONSÁVEL DE ALTERAR AS TABELAS "MAD_PEDVENDA" E "CADAN_PIXPEDIDOS" #
                            for record in list_pix:
                                txid = record["txid"]
                                endToEndId = record["endToEndId"]
                                datetime_str = str(record["horario"][:20])+"000Z"
                                convert_datetime = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                                horario = convert_datetime.strftime("%Y-%m-%d %H:%M:%S")

                                # REALIZA ALTERAÇÃO NA TABELA CADAN_PIXPEDIDOS #
                                query = \
                                    f"""
                                    UPDATE implantacao.cadan_pixpedidos
                                    SET situacaoped = 'L'
                                    WHERE 1=1
                                    AND txid = '{txid}'
                                    """
                                cur.execute(query)

                                # REALIZA ALTERAÇÃO NA TABELA CADAN_PIXTRANSAÇÕES #
                                query = \
                                    f"""
                                    UPDATE implantacao.cadan_pixtransacoes
                                    SET pixstatus = 'R',
                                        pixdtaconsulta = SYSDATE,
                                        pixdtarecebimento = TO_TIMESTAMP('{horario}', 'YYYY-MM-DD HH24:MI:SS'),
                                        pixe2eid = '{endToEndId}'
                                    WHERE 1=1
                                    AND pixtxid = '{txid}'
                                    """
                                cur.execute(query)

                                # REALIZA ALTERAÇÃO NA TABELA MAD_CRITICAPEDVENDA #
                                query = \
                                f"""
                                UPDATE implantacao.mad_criticapedvenda
                                SET situacaocritica = 'L',
                                    dtaliberacao = SYSDATE,
                                    usuliberacao = 'APPPIX'
                                WHERE nropedvenda IN (SELECT nropedvenda 
                                                     FROM implantacao.cadan_pixpedidos 
                                                     WHERE txid = '{txid}')
                                """
                                cur.execute(query)

                            conn.commit()
                            message = f"{datetime_format}: Tabela PIXPEDIDOS - Processo realizado com sucesso.\n"
                            with open(file_path, "a") as write_file:
                                write_file.write(str(message))
                                write_file.close()
                        except ora.DatabaseError as Error:
                            conn.rollback()
                            message = f"{datetime_format}: {Error}\n"
                            with open(file_path, "a") as write_file:
                                write_file.write(str(message))
                                write_file.close()
            cur.close()

    @staticmethod
    def check_invoiced_orders():
        """
        CONSULTA PEDIDOS FATURADOS
        ----------------------------------------------------------------------------------------------------------------
        Objetive: Gerar um comprovante em PDF do PIX recebido.
        Create: HILSON SANTOS
        Date: 09/12/2024
        @return:
        ----------------------------------------------------------------------------------------------------------------
        """
        datetime_now = datetime.datetime.now()
        datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
        file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
        dirqrcodes = "QRCodes"
        dirdownloads = os.path.join(os.path.expanduser("~"), "Downloads")
        if not os.path.exists(f"{dirdownloads}/{dirqrcodes}"):
            os.mkdir(f"{dirdownloads}/{dirqrcodes}")
        oracle = ConnectDatabase()
        conn = oracle.conn_oracle()
        cur = conn.cursor()
        query = \
            """
            SELECT 
                DISTINCT
                'txid' as id,
                txid
            FROM implantacao.cadan_pixpedidos a 
            JOIN implantacao.mfl_doctofiscal b ON b.nropedidovenda = a.nropedvenda
            JOIN implantacao.ge_pessoa c ON c.seqpessoa = b.seqpessoa	
            JOIN implantacao.cadan_pixtransacoes d ON d.pixempresa = a.nroempresa AND d.pixtxid = a.txid
            WHERE 1=1
            AND a.docimpresso = 'N'
            """
        cur.execute(query)
        columns = [row[0] for row in cur.description]
        data = cur.fetchall()
        if len(data) != 0:
            dftxid = pd.DataFrame.from_records(data=data, columns=columns)
            dftxid.columns = dftxid.columns.str.lower()
            dftxid = dftxid.to_dict('records')
            list_txid_size = len(dftxid)
            if list_txid_size != 0:
                try:
                    # ESSE LOOP É RESPONSÁVEL GERAR O DOCUMENTO DE RETORNO DO PIX E DE ALTERAR AS TABELAS "CADAN_PIXPEDIDOS" #
                    for record in dftxid:
                        txid = record["txid"]
                        query = \
                            f"""
                            SELECT 
                                DISTINCT
                                a.seqpessoa,
                                a.nropedvenda,
                                LISTAGG(b.numerodf, ', ') WITHIN GROUP (ORDER BY b.numerodf) AS nronumeronf,
                                c.nomerazao,
                                d.pixtxid,
                                d.pixvalor,
                                d.pixe2eid,
                                TO_CHAR(d.pixdtaenvio, 'DD/MM/YYYY HH24:MI:SS') AS pixdtaenvio,
                                TO_CHAR(d.pixdtarecebimento, 'DD/MM/YYYY HH24:MI:SS') AS pixdtarecebimento
                            FROM implantacao.cadan_pixpedidos a 
                            JOIN implantacao.mfl_doctofiscal b ON b.nropedidovenda = a.nropedvenda
                            JOIN implantacao.ge_pessoa c ON c.seqpessoa = b.seqpessoa	
                            JOIN implantacao.cadan_pixtransacoes d ON d.pixempresa = a.nroempresa AND d.pixtxid = a.txid
                            WHERE 1=1
                            AND a.docimpresso = 'N'
                            AND a.txid = '{txid}'
                            GROUP BY a.seqpessoa, a.nropedvenda, c.nomerazao, d.pixtxid, d.pixvalor, d.pixe2eid, d.pixdtaenvio, d.pixdtarecebimento
                            ORDER BY 1 ASC 
                            """
                        cur.execute(query)
                        columns = [row[0] for row in cur.description]
                        data = cur.fetchall()
                        dfpedidos = pd.DataFrame.from_records(data=data, columns=columns)
                        dfpedidos.columns = dfpedidos.columns.str.lower()
                        dfpedidos = dfpedidos.astype(str)
                        data_dictionary = dfpedidos.to_dict("records")
                        data_dumps = json.dumps(data_dictionary)
                        data_disc = {
                            "data": json.loads(data_dumps)
                        }
                        datapix = data_disc["data"][0]
                        custonercode = datapix["seqpessoa"]
                        custonername = datapix["nomerazao"]
                        order = datapix["nropedvenda"]
                        invoices = datapix["nronumeronf"]
                        pixvalue = datapix["pixvalor"]
                        value = float(pixvalue)
                        pixvalue = locale.format_string("%.2f", value, grouping=True)
                        dateshipping = datapix["pixdtaenvio"]
                        datereceipt = datapix["pixdtarecebimento"]
                        documentnumber = datapix["pixtxid"]
                        receiptnumber = datapix["pixe2eid"]
                        dirimage = os.getcwd()
                        imgcompanylogo = os.path.join(dirimage, "logo-bbrasil.png")
                        pdf = FPDF(orientation="P", unit="mm", format="A4")
                        pdf.add_page()
                        pdf.image(imgcompanylogo, x=2.0, y=2.0, link="", type="", w=30, h=0)
                        fontname = "times"
                        space_title = 35.0
                        space_information = 71.5
    
                        # Nome do cliente
                        pdf.set_xy(x=space_title, y=5.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=16.0)
                        pdf.cell(w=0, h=0, txt="BANCO DO BRASIL", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_title, y=11.0)
                        pdf.set_font(family=f"{fontname}", style="", size=14.0)
                        pdf.cell(w=0, h=0, txt="COMPROVANTE RECEBIMENTO PIX", border=0, ln=0, align='L', fill=False, link="")
                        fontname = "courier"
                        fontsize1 = 10.0
                        
                        pdf.set_xy(x=space_title, y=17.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize1)
                        pdf.cell(w=0, h=0, txt="Cliente.........:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=17.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize1)
                        pdf.cell(w=0, h=0, txt=f"{custonercode}-{custonername}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=space_title, y=23.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize1)
                        pdf.cell(w=0, h=0, txt="Pedido..........:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=23.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize1)
                        pdf.cell(w=0, h=0, txt=f"{order}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=space_title, y=29.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize1)
                        pdf.cell(w=0, h=0, txt="Notas Fiscais...:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=29.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize1)
                        pdf.cell(w=0, h=0, txt=f"{invoices}", border=0, ln=0, align='L', fill=False, link="")
                
                        x_initial = 2.0
                        x_end = 208.0
                        y_actual = pdf.get_y() + 5.0
                        pdf.line(x_initial, y_actual, x_end, y_actual)
                  
                        fontsize2 = 12.00
                        space_information = 74.0
                        pdf.set_xy(x=2.0, y=40.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"VALOR DO PIX R$............:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=40.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"{pixvalue}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=2.0, y=46.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"DATA DO ENVIO..............:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=46.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"{dateshipping}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=2.0, y=52.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"DATA DO RECEBIMENTO........:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=52.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"{datereceipt}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=2.0, y=58.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"IDENTIFICAÇÃO DOCUMENTO....:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=58.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"{documentnumber}", border=0, ln=0, align='L', fill=False, link="")
                        
                        pdf.set_xy(x=2.0, y=64.0)
                        pdf.set_font(family=f"{fontname}", style="B", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"IDENTIFICAÇÃO RETORNO......:", border=0, ln=0, align='L', fill=False, link="")
                        pdf.set_xy(x=space_information, y=64.0)
                        pdf.set_font(family=f"{fontname}", style="", size=fontsize2)
                        pdf.cell(w=0, h=0, txt=f"{receiptnumber}", border=0, ln=0, align='L', fill=False, link="")
                        pdf.output(os.path.join(f"{dirdownloads}/{dirqrcodes}/", f"DOC{documentnumber}.pdf"))
    
                        # REALIZA ALTERAÇÃO NA TABELA CADAN_PIXPEDIDOS #
                        query = \
                            f"""
                            UPDATE implantacao.cadan_pixpedidos
                            SET situacaoped = 'F', docimpresso = 'S' 
                            WHERE 1=1
                            AND txid = '{txid}'
                            """
                        cur.execute(query)
                    conn.commit()
                    message = f"{datetime_format}: Tabela PIXPEDIDOS - Processo realizado com sucesso.\n"
                    with open(file_path, "a") as write_file:
                        write_file.write(str(message))
                        write_file.close()
                except ora.DatabaseError as Error:
                    conn.rollback()
                    message = f"{datetime_format}: {Error}\n"
                    print(message)
                    with open(file_path, "a") as write_file:
                        write_file.write(str(message))
                        write_file.close()
        cur.close()


class OrderService(object):
    """
    API SERVIÇOS COM OS PEDIDOS IMPORTADOS
    --------------------------------------------------------------------------------------------------------------------
    Objective: É de listar os pedidos importados pelo processo "ServicoPedidoAutomatico"  para  o controle dos PIX dos
               pedidos enviados ao banco que estão armazenados na tabela "CADAN_PIXPEDIDOS"
    create: HISON SANTOS
    date:  05/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """

    def select_customers_orders(self):
        """
        SELECIONAR CLIENTES DOS PEDIDOS
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        Create: HILSON SANTOS
        Date: 18/11/2024
        @return:
        ----------------------------------------------------------------------------------------------------------------
        """
        usercompany = self.GET.get("usercompany")
        username = self.GET.get("username")
        userpassword = self.GET.get("userpassword")

        token = TokenService(usercompany=usercompany, username=username, userpassword=userpassword)
        validate_token = token.create_token()
        response_content = validate_token.content
        response_json = json.loads(response_content)
        status_code = response_json["status_code"]

        if int(status_code) == 200:
            datetime_now = datetime.datetime.now()
            datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
            file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
            if not os.path.isfile(file_path):
                with open(file_path, "w") as write_file:
                    write_file.close()

            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            try:
                query = \
                    """
                    SELECT
                        DISTINCT
                        a.seqpessoa,
                        INITCAP(b.nomerazao) nomerazao,
                        /* QUANTIDADE EM ANALISE */
                        NVL((SELECT COUNT(c.nropedvenda)
                        FROM implantacao.cadan_pixpedidos c
                        INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                        WHERE 1=1
                        AND c.situacaoped = 'A'
                        AND c.seqpessoa = a.seqpessoa
                        GROUP BY c.seqpessoa),0) AS qtdanalise,
                        /* QUANTIDADE ENVIADO */
                        NVL((SELECT COUNT(c.nropedvenda)
                        FROM implantacao.cadan_pixpedidos c
                        INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                        WHERE 1=1
                        AND c.situacaoped = 'E'
                        AND c.seqpessoa = a.seqpessoa
                        GROUP BY c.seqpessoa),0) AS qtdenviado,
                        /* QUANTIDADE LIBERADO */
                        NVL((SELECT COUNT(c.nropedvenda)
                        FROM implantacao.cadan_pixpedidos c
                        INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                        WHERE 1=1
                        AND c.situacaoped = 'L'
                        AND c.seqpessoa = a.seqpessoa
                        GROUP BY c.seqpessoa),0) AS qtdliberado,
                        /* VALOR TOTAL */
                        NVL((SELECT SUM(c.vlrpedido)
                        FROM implantacao.cadan_pixpedidos c
                        INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                        WHERE 1=1
                        AND c.situacaoped IN ('A', 'E', 'L')
                        AND c.seqpessoa = a.seqpessoa
                        GROUP BY c.seqpessoa),0) AS pedvalor
                    FROM implantacao.cadan_pixpedidos a
                    INNER JOIN implantacao.ge_pessoa b ON b.seqpessoa = a.seqpessoa
                    WHERE 1=1
                    AND a.situacaoped IN ('A', 'E', 'L')
                    GROUP BY a.seqpessoa, b.nomerazao
                    HAVING NVL((SELECT COUNT(c.nropedvenda)
                                FROM implantacao.cadan_pixpedidos c
                                INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                                WHERE 1=1
                                AND c.situacaoped IN ('A', 'E', 'L')
                                AND c.seqpessoa = a.seqpessoa
                                GROUP BY c.seqpessoa),0) > 0
                    ORDER BY 2 ASC
                    """
                cur.execute(query)
                columns = [row[0] for row in cur.description]
                data = cur.fetchall()
                dfpedidos = pd.DataFrame.from_records(data=data, columns=columns)
                dfpedidos.columns = dfpedidos.columns.str.lower()
                dfpedidos = dfpedidos.astype(str)
                data_dictionary = dfpedidos.to_dict("records")
                data_dumps = json.dumps(data_dictionary)
                data_disc = {
                    "date": datetime_format,
                    "table": "implantacao.cadan_pixpedidos",
                    "user": username,
                    "data": json.loads(data_dumps)
                }
                return JsonResponse(data_disc)
            except ora.DatabaseError as Error:
                conn.rollback()
                cur.close()
                message = f"{datetime_format}: {Error}\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
        else:
            response = response_json
            return JsonResponse(response)

    def select_orders_pays(self):
        """
        SELECIONAR PEDIDOS PARA PAGAMENTO
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        Create: HILSON SANTOS
        Date: 19/11/2024
        @return:
        ----------------------------------------------------------------------------------------------------------------
        """
        usercompany = self.GET.get("usercompany")
        username = self.GET.get("username")
        userpassword = self.GET.get("userpassword")
        codecustomer = self.GET.get("codecustomer")

        token = TokenService(usercompany=usercompany, username=username, userpassword=userpassword)
        validate_token = token.create_token()
        response_content = validate_token.content
        response_json = json.loads(response_content)
        status_code = response_json["status_code"]

        if int(status_code) == 200:
            datetime_now = datetime.datetime.now()
            datetime_format = datetime_now.strftime("%d/%m/%Y %H:%M:%S")
            file_path = f"{TEMPORATY_DIR}/appapis_log.tmp"
            if not os.path.isfile(file_path):
                with open(file_path, "w") as write_file:
                    write_file.close()

            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            try:
                query = \
                    f"""
                    SELECT
                        a.situacaoped,
                        a.nropedvenda,
                        b.nrorepresentante,
                        INITCAP(c.nomerazao) AS nomerazao,
                        a.vlrpedido
                    FROM implantacao.cadan_pixpedidos a
                    INNER JOIN implantacao.mad_representante b ON b.nrorepresentante = a.nrorepresentante
                    INNER JOIN implantacao.ge_pessoa c ON c.seqpessoa = b.seqpessoa
                    WHERE 1=1
                    AND a.situacaoped IN ('A', 'E', 'L')
                    AND a.seqpessoa = {codecustomer}
                    ORDER BY a.situacaoped ASC
                    """
                cur.execute(query)
                columns = [row[0] for row in cur.description]
                data = cur.fetchall()
                dfpedidos = pd.DataFrame.from_records(data=data, columns=columns)
                dfpedidos.columns = dfpedidos.columns.str.lower()
                dfpedidos = dfpedidos.astype(str)
                data_dictionary = dfpedidos.to_dict("records")
                data_dumps = json.dumps(data_dictionary)
                data_disc = {
                    "date": datetime_format,
                    "table": "implantacao.cadan_pixpedidos",
                    "user": username,
                    "data": json.loads(data_dumps)
                }
                return JsonResponse(data_disc)
            except ora.DatabaseError as Error:
                conn.rollback()
                cur.close()
                message = f"{datetime_format}: {Error}\n"
                with open(file_path, "a") as write_file:
                    write_file.write(str(message))
                    write_file.close()
        else:
            response = response_json
            return JsonResponse(response)
