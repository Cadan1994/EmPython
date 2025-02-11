import os
import pandas as pd
import requests
import json
import hashlib
import locale
import cx_Oracle as ora
from django.contrib import messages
from django.shortcuts import render, redirect
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from pathlib import Path
from fpdf import FPDF
from .connectdb import ConnectDatabase
from .querys import *
from .gqrcode import GerarQrCodePix

customercode = 0
customername = None


def select_data_table_parameters():
    """
    PARÂMETROS DE DADOS
    --------------------------------------------------------------------------------------------------------------------
    @Objective: Selecionar os dados da tabela "CADAN_PIXPARAMENTROS"
    @Create: HILSON SANTOS
    @Date: 06/11/2024
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


class Login(object):
    """
    TELA DE LOGIN DO USUÁRIO
    --------------------------------------------------------------------------------------------------------------------
    Create: HILSON SANTOS
    Date: 12/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    def __init__(self):
        self.GET = None
        self.POST = None
        self.session = None

    def login(self):
        """
        CHAMA A TELA DE LOGIN
        ----------------------------------------------------------------------------------------------------------------
        Objective: Digitar as credenciais do usuário.
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: A página para realizar o login no sistema
        ----------------------------------------------------------------------------------------------------------------
        """
        self.session.flush()
        template_name = "apppix-login.html"
        return render(self, template_name=template_name)

    def authentication(self):
        """
        AUTENTICAÇÃO DO USUÁRIO
        ----------------------------------------------------------------------------------------------------------------
        Objetive: Verificar as credenciais do usuário para dar permissão a acessar o sistema.
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: A página para realizar o login no sistema
        ----------------------------------------------------------------------------------------------------------------
        """
        usercompany = self.GET.get("usercampany")
        username = self.GET.get("username")
        userpassword = self.GET.get("userpassword")

        if len(username) == 0 and len(userpassword) == 0:
            messages.info(self, message="Usuário e senha não informada!")
            return redirect(to="/apppix/")
        elif len(username) == 0:
            messages.info(self, message="Usuário não informada!")
            return redirect(to="/apppix/")

        elif len(userpassword) == 0:
            messages.info(self, message="Senha não informada!")
            return redirect(to="/apppix/")
        else:
            headers = {
                "Content-Type": "application/json"
            }
            params = {
                "usercompany": usercompany,
                "username": username,
                "userpassword": userpassword
            }
            response = requests.get(
                url="http://127.0.0.1:8000/cadan/api/v1/authorization/token/",
                headers=headers,
                params=params
            )
            response_json = response.json()
            status_code = response_json["status_code"]
            message = response_json["message"]
            if int(status_code) == 200:
                self.session["usercompany"] = usercompany
                self.session["username"] = username
                self.session["userpassword"] = userpassword
                return redirect(to="/apppix/home/")
            else:
                messages.info(self, message=message)
                return redirect(to="/apppix/")

    def login_select(self):
        """
        SELECIONAR USUÁRIO
        ----------------------------------------------------------------------------------------------------------------
        Objetive: Selecionar o usuário para realizar alteração da senha.
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: Chanada do templante.
        ----------------------------------------------------------------------------------------------------------------
        """
        usercompany = self.session["usercompany"]
        username = self.session.get("username")
        oracle = ConnectDatabase()
        conn = oracle.conn_oracle()
        cur = conn.cursor()
        query = \
            f"""
            SELECT 
                userid,
                userempresa,
                userlogin 
            FROM implantacao.cadan_pixusuarios
            WHERE 1=1
            AND userempresa = {usercompany}
            AND userlogin = '{username}'               
            """
        cur.execute(query)
        columns = [row[0] for row in cur.description]
        data = cur.fetchall()
        if len(data) == 0:
            messages.info(self, message="Login não localizado para alterar senha!")
            return redirect(to="/apppix/")
        else:
            dfusuario = pd.DataFrame.from_records(data=data, columns=columns)
            dfusuario.columns = dfusuario.columns.str.lower()
            dfusuario = dfusuario.astype(str)
            data_dictionary = dfusuario.to_dict("records")
            data_dumps = json.dumps(data_dictionary)
            data_disc = {
                "data": json.loads(data_dumps)
            }
            data_json = data_disc["data"][0]
            template_name = "apppix-login-update.html"
            context = {
                "user": {
                    "userid": data_json["userid"],
                    "usercompany": data_json["userempresa"],
                    "username": data_json["userlogin"]
                }
            }
            return render(self, template_name=template_name, context=context)

    def login_update_password(self):
        """
        ALTERAR SENHA DO USUÁRIO
        ----------------------------------------------------------------------------------------------------------------
        Objetive: Realiza alteração da senha do usuário.
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: O templante principal
        """
        if self.method == "POST":
            usercompany = self.session["usercompany"]
            username = self.session.get("username")
            userpassword = self.POST.get("userpassword")
            userpasswordmd5 = hashlib.md5(userpassword.encode()).hexdigest().upper()
            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            try:
                query = \
                    f"""
                    UPDATE implantacao.cadan_pixusuarios
                    SET usersenha = '{userpasswordmd5}'
                    WHERE 1=1
                    AND userempresa = {usercompany}
                    AND userlogin = '{username}'               
                    """
                cur.execute(query)
                conn.commit()
            except ora.DatabaseError as Error:
                conn.rollback()
            cur.close()
        return redirect(to="/apppix/home/")


class Home(object):
    """
    TELA PRINCIPAL
    --------------------------------------------------------------------------------------------------------------------
    Create: HILSON SANTOS
    Date: 12/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    def __init__(self):
        self.session.get("username")

    def home(self):
        """
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: Chama a templante "apppix-home.html" se true ou "apppix-login.html"
        ----------------------------------------------------------------------------------------------------------------
        """
        if self.session.get("username"):
            return render(self, template_name="apppix-home.html")
        else:
            messages.info(self, message="Você precisa fazer o login!")
            return redirect(to="/apppix/")

    def logoff(self):
        """
        ----------------------------------------------------------------------------------------------------------------
        Objective:
        Create: HILSON SANTOS
        Date: 12/11/2024
        @return: Chama a templante "apppix-login.html" se true
        ---------------------------------------------------------------------------------------------------------------
        """
        self.session.flush()
        return redirect(to="/apppix/")


class OperationPix(object):
    """
    ENVIO DE PIX
    --------------------------------------------------------------------------------------------------------------------
    Objetive: Realizar operações com PIX do banco do brasil
    Create: HILSON SANTOS
    Date: 06/11/2024
    --------------------------------------------------------------------------------------------------------------------
    """
    def __init__(self):
        pixparameters = select_data_table_parameters()
        pixparameters_content = pixparameters.content
        pixparameters_json = json.loads(pixparameters_content)
        self.__client_id = pixparameters_json["bbrasil_client_id"]
        self.__client_secret = pixparameters_json["bbrasil_client_secret"]
        self.__authorization = pixparameters_json['bbrasil_authorization']
        self.__key_pix = pixparameters_json["bbrasil_chave_pix"]
        self.__urltoken = pixparameters_json["bbrasil_url_token"]
        self.__urlapipix = pixparameters_json["bbrasil_url_apipix"]
        self.__gwdevappkey = pixparameters_json['bbrasil_gw_dev_app_key']
        self.__params = {"gw-dev-app-key": self.__gwdevappkey}

    def request_token(self):
        """
        PEGAR O TOKEN
        ----------------------------------------------------------------------------------------------------------------
        Objective: Solicitar o token de acesso das API'S
        Create: HILSON SANTOS
        Date: 06/11/2024
        @return: Token
        ----------------------------------------------------------------------------------------------------------------
        """
        header = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"{self.__authorization}"
        }
        params = {
            "grant_type": "client_credentials",
            "scope": "cob.read cob.write cobv.read cobv.write pix.read pix.write"
        }
        response = requests.post(
            url=f"{self.__urltoken}",
            auth=(f"{self.__client_id}", f"{self.__client_secret}"),
            headers=header,
            params=params
        )
        token = response.json()
        return token["access_token"]

    def pix_manager_brasil_custumers(self):
        """
        GERENCIAMENTO DOS CLIENTES
        ----------------------------------------------------------------------------------------------------------------
        Objective: Selecionar os clientes que efetuaram pedidos para pagamento via PIX referente banco BRASIL
        Create: HILSON SANTOS
        Date: 19/11/2024
        @return: Chama a página "apppix-gpix.html"
        ----------------------------------------------------------------------------------------------------------------
        """
        if self.session.get("username"):
            usercompany = self.session["usercompany"]
            username = self.session["username"]
            userpassword = self.session["userpassword"]
            headers = {
                "Content-Type": "application/json"
            }
            params = {
                "usercompany": usercompany,
                "username": username,
                "userpassword": userpassword
            }
            url_customer = "http://127.0.0.1:8000/cadan/api/v1/SelecionarClientesPedidos/"
            response = requests.get(url=url_customer, headers=headers, params=params)
            json_customer = response.json()

            context = {
                "banco": {"codigo": "001"},
                "clientes": json_customer["data"]
            }
            return render(self, template_name="apppix-gpix.html", context=context)
        else:
            messages.info(self, message="Você precisa fazer o login!")
            return redirect("/apppix/")

    def pix_manager_brasil_orders(self):
        """
        GERENCIAMENTO DOS PEDIDOS POR CLIENTE
        ----------------------------------------------------------------------------------------------------------------
        Objective: Selecionar os pedidos por cliente para pagamento via PIX
        Create: HILSON SANTOS
        Date: 20/11/2024
        @return: Chama a página "apppix-gpix-pedidos.html"
        """
        usercompany = self.session["usercompany"]
        username = self.session["username"]
        userpassword = self.session["userpassword"]
        global customercode
        customercode = self.GET.get("selectedCustomerCode")
        global customername
        customername = self.GET.get("selectedCustomerName")
        headers = {
            "Content-Type": "application/json"
        }
        params = {
            "usercompany": usercompany,
            "username": username,
            "userpassword": userpassword,
            "codecustomer": customercode
        }

        url_orders = "http://127.0.0.1:8000/cadan/api/v1/SelecionarPedidosPagar/"
        response = requests.get(url=url_orders, headers=headers, params=params)
        json_orders = response.json()

        context = {
            "banco": {"codigo": "001"},
            "cliente": {"codigo": customercode, "nome": customername},
            "pedidos": json_orders["data"],
        }
        return render(self, template_name="apppix-gpix-pedidos.html", context=context)

    def send_orders_immediate_collection(self):
        """
        ENVIA PIX COBRANÇA IMEDIATA
        ----------------------------------------------------------------------------------------------------------------
        Objective: Enviar os pedidos selecionados pelo usuário
        Create: HILSON SANTOS
        Date: 22/11/2024
        @param: type_custumer, inscription,  namecompany, ordernumber, ordervalue, totalordervalue, txid)
        @return: O QR code para ser enviado ao cliente
        ----------------------------------------------------------------------------------------------------------------
        """
        global customercode
        global customername
        ordernumber = self.POST.get("selectedOrderNumber")
        parametros = select_data_table_parameters()
        parameters_content = parametros.content
        parameters_json = json.loads(parameters_content)
        urlapipix = parameters_json["bbrasil_url_apipix"]
        pixkey = parameters_json["bbrasil_chave_pix"]
        gwdevappkey = parameters_json["bbrasil_gw_dev_app_key"]
        locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")

        if len(ordernumber) == 0:
            messages.info(self, message="0")
            return redirect(to=f"/apppix/brasil-pix-orders/?selectedCustomerCode={customercode}&selectedCustomerName={customername}")
        else:
            oracle = ConnectDatabase()
            conn = oracle.conn_oracle()
            cur = conn.cursor()
            query = \
                f"""
                SELECT
                    DISTINCT
                    a.seqpessoa,
                    NVL((SELECT COUNT(c.nropedvenda)
                    FROM implantacao.cadan_pixpedidos c
                    INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                    WHERE 1=1
                    AND c.situacaoped IN ('A', 'E', 'L')
                    AND c.seqpessoa = a.seqpessoa
                    GROUP BY c.seqpessoa),0) pedquantidade
                FROM implantacao.cadan_pixpedidos a
                INNER JOIN implantacao.ge_pessoa b ON b.seqpessoa = a.seqpessoa
                WHERE 1=1
                AND a.situacaoped IN ('A', 'E', 'L')
                AND a.seqpessoa = {customercode}
                GROUP BY a.seqpessoa, b.nomerazao
                HAVING NVL((SELECT COUNT(c.nropedvenda)
                            FROM implantacao.cadan_pixpedidos c
                            INNER JOIN implantacao.ge_pessoa d ON d.seqpessoa = c.seqpessoa
                            WHERE 1=1
                            AND c.situacaoped IN ('A', 'E', 'L')
                            AND c.seqpessoa = a.seqpessoa
                            GROUP BY c.seqpessoa),0) > 0                
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
            pedquantidade = int(data_disc["data"][0]["pedquantidade"])
            if pedquantidade == 0:
                messages.info(self, message="1")
            else:
                messages.info(self, message="2")
                query = \
                    f"""
                    SELECT
                        DISTINCT
                        a.nroempresa,
                        a.seqpessoa,
                        b.nomerazao,
                        b.cidade,
                        b.fisicajuridica,	
                        b.nrocgccpf||b.digcgccpf AS cpfcnpj,	 
                        LISTAGG(a.nropedvenda, ', ') WITHIN GROUP (ORDER BY a.nropedvenda) AS pedidonumeros,
                        LISTAGG(a.vlrpedido, ', ') WITHIN GROUP (ORDER BY a.vlrpedido) AS pedidovalores,
                        SUM(a.vlrpedido) AS vlrpedido,	 
                        a.nroempresa||
                        LPAD(b.nrocgccpf||b.digcgccpf,15, '0')||
                        LPAD(a.seqpessoa, 6, '0')||
                        'L'||
                        LPAD(c.nropedvenda, 12, '0') AS nrotxid
                    FROM implantacao.cadan_pixpedidos a
                    INNER JOIN implantacao.ge_pessoa b 
                    ON b.seqpessoa=a.seqpessoa	 
                    INNER JOIN (SELECT 
                                    nroempresa, 
                                    seqpessoa, 
                                    MIN(nropedvenda) nropedvenda
                                FROM implantacao.cadan_pixpedidos
                                WHERE 1=1
                                AND seqpessoa = {customercode}
                                AND nropedvenda IN ({ordernumber})
                                GROUP BY nroempresa, seqpessoa) c 
                    ON c.nroempresa = a.nroempresa 
                    AND c.seqpessoa = a.seqpessoa
                    LEFT JOIN implantacao.cadan_pixpedidos e 
                    ON e.nroempresa = a.nroempresa 
                    AND e.nropedvenda = a.nropedvenda 
                    AND e.seqpessoa = a.seqpessoa
                    WHERE 1=1
                    AND a.seqpessoa = {customercode}
                    AND a.nropedvenda IN ({ordernumber})
                    GROUP BY a.nroempresa, 
                             a.seqpessoa, 
                             b.nomerazao,
                             b.cidade, 
                             b.fisicajuridica, 
                             b.nrocgccpf, 
                             b.digcgccpf, 
                             c.nropedvenda            
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
                data = data_disc["data"][0]
                companycode = data["nroempresa"]
                customercode = data["seqpessoa"]
                type_customer = data["fisicajuridica"]
                inscription = data["cpfcnpj"]
                namecustomer = data["nomerazao"]
                customercity = data["cidade"]
                totalordervalue = data["vlrpedido"]
                ordernumber = data["pedidonumeros"]
                listordernumber = [int(num.strip()) for num in ordernumber.split(",")]
                ordervalue = data["pedidovalores"]
                txid = data["nrotxid"]

                type_inscription = "cnpj"
                if type_customer == "F":
                    type_inscription = "cpf"

                payload = {
                    "calendario": {
                        "expiracao": 86400
                    },
                    "devedor": {
                        f"{type_inscription}": f"{inscription}",
                        "nome": f"{namecustomer}"
                    },
                    "valor": {
                        "original": f"{totalordervalue}",
                        "modalidadeAlteracao": 0
                    },
                    "chave": f"{pixkey}",
                    "solicitacaoPagador": "Compra realizada.",
                    "infoAdicionais": [
                        {
                            "nome": f"{ordernumber}",
                            "valor": f"{ordervalue}"
                        }
                    ]
                }
                token_dict = OperationPix()
                token = token_dict.request_token()
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                }
                params = {
                    "gw-dev-app-key": f"{gwdevappkey}"
                }
                response = requests.put(
                    url=f"{urlapipix}/cob/{txid}",
                    json=payload,
                    headers=headers,
                    params=params
                )
                statuscode = response.status_code
                if statuscode in [200, 201]:
                    response_json = response.json()
                    pixqrcode = response_json["pixCopiaECola"]

                    # CRIA UMA PASTA NO DIRETÓRIO DOWNLOADS
                    dirqrcodes = "QRCodes"
                    dirdownloads = os.path.join(os.path.expanduser("~"), "Downloads")
                    if not os.path.exists(f"{dirdownloads}/{dirqrcodes}"):
                        os.mkdir(f"{dirdownloads}/{dirqrcodes}")

                    dirqrcode = os.getcwd()
                    gerarqrcode = GerarQrCodePix(
                        nome=namecustomer,
                        chavepix=pixkey,
                        valor=totalordervalue,
                        cidade=customercity,
                        txid=txid,
                        diretorio=dirqrcode
                    ).gerar_payload()

                    imgqrcode = os.path.join(dirqrcode, "CadanQRCodePix.png")
                    imgcompanylogo = os.path.join(dirqrcode, "CadanLogo.png")

                    # FORMULÁRIO EM PDF DO QRCODE PARA PAGAMENTO
                    pdf = FPDF(orientation="P", unit="mm", format="A4")
                    pdf.add_page()
                    pdf.image(imgcompanylogo, x=0.5, y=0.5, link="", type="", w=30, h=0)
                    pdf.image(imgqrcode, x=30.5, y=0.5, link="", type="", w=35, h=35)
                    fontname = "Courier"
                    fontsize = 10.0
                    space_title = 65.0
                    space_information = 100.0
                    # Nome do cliente
                    pdf.set_xy(x=space_title, y=8.5)
                    pdf.set_font(family=f"{fontname}", style="B", size=fontsize)
                    pdf.cell(w=0, h=0, txt="CLIENTE........:", border=0, ln=0, align='L', fill=False, link="")
                    pdf.set_xy(x=space_information, y=8.5)
                    pdf.set_font(family=f"{fontname}", style="", size=fontsize)
                    pdf.cell(w=0, h=0, txt=f"{namecustomer}", border=0, ln=0, align='L', fill=False, link="")
                    # Números dos pedidos
                    pdf.set_xy(x=space_title, y=14.5)
                    pdf.set_font(family=f"{fontname}", style="B", size=fontsize)
                    pdf.cell(w=0, h=0, txt="PEDIDOS nº.....:", border=0, ln=0, align='L', fill=False, link="")
                    pdf.set_xy(x=space_information, y=14.5)
                    pdf.set_font(family=f"{fontname}", style="", size=fontsize)
                    pdf.cell(w=0, h=0, txt=f"{ordernumber}", border=0, ln=0, align='L', fill=False, link="")
                    # Valores dos pedidos
                    pdf.set_xy(x=space_title, y=20.5)
                    pdf.set_font(family=f"{fontname}", style="B", size=fontsize)
                    pdf.cell(w=0, h=0, txt="PEDIDOS R$.....:", border=0, ln=0, align='L', fill=False, link="")
                    pdf.set_xy(x=space_information, y=20.5)
                    pdf.set_font(family=f"{fontname}", style="", size=fontsize)
                    pdf.cell(w=0, h=0, txt=f"{ordervalue}", border=0, ln=0, align='L', fill=False, link="")
                    # Valor total à pagar
                    value = float(totalordervalue)
                    totalamountpay = locale.format_string("%.2f", value, grouping=True)
                    pdf.set_xy(x=space_title, y=26.5)
                    pdf.set_font(family=f"{fontname}", style="B", size=fontsize)
                    pdf.cell(w=0, h=0, txt="TOTAL PAGAR R$.:", border=0, ln=0, align='L', fill=False, link="")
                    pdf.set_xy(x=space_information, y=26.5)
                    pdf.set_font(family=f"{fontname}", style="", size=fontsize)
                    pdf.cell(w=0, h=0, txt=f"{totalamountpay}", border=0, ln=0, align='L', fill=False, link="")
                    # Link do QR Code
                    pdf.set_xy(x=0.5, y=40.0)
                    pdf.set_font(family=f"{fontname}", style="", size=10)
                    qrcodelink = gerarqrcode
                    pdf.multi_cell(w=0, h=3.5, txt=f"{qrcodelink}", border=0, align='L', fill=False)
                    # Desenha uma linha abaixo do link do QR Code
                    x_initial = 2.0
                    x_end = 208.0
                    y_actual = pdf.get_y() + 2.0
                    pdf.line(x_initial, y_actual, x_end, y_actual)

                    page_quantity = len(listordernumber)
                    first_page = 1

                    for order in listordernumber:
                        totalvalue = 0
                        if page_quantity != 0:
                            if first_page == 1:
                                pdf.set_xy(x=2.0, y=58.0)
                            else:
                                pdf.add_page()
                                pdf.set_xy(x=2.0, y=20)
                        else:
                            pdf.set_xy(x=2.0, y=58.0)

                        pdf.set_font(family=f"{fontname}", style="B", size=12)
                        pdf.cell(
                            w=208,
                            h=0,
                            txt=f"«» DETALHAMENTO DO PEDIDO NÚMERO: {order} «»",
                            border=0,
                            ln=0,
                            align='C',
                            fill=False,
                            link=""
                        )

                        if first_page == 1:
                            yline = 65.00
                        else:
                            yline = 28.0

                        pdf.set_xy(x=0.5, y=yline)
                        pdf.set_font(family=f"{fontname}", style="B", size=10)
                        pdf.cell(w=12, h=0, txt="ITEM", border=0, ln=0, align='C', fill=False, link="")

                        pdf.set_xy(x=12.5, y=yline)
                        pdf.set_font(family=f"{fontname}", style="B", size=10)
                        pdf.cell(w=160.0, h=0, txt="CÓDIGO/DESCRIÇÃO", border=0, ln=0, align='L', fill=False, link="")

                        pdf.set_xy(x=160.5, y=yline)
                        pdf.set_font(family=f"{fontname}", style="B", size=10)
                        pdf.cell(w=25, h=0, txt="QTDE.", border=0, ln=0, align='C', fill=False, link="")

                        pdf.set_xy(x=180.0, y=yline)
                        pdf.set_font(family=f"{fontname}", style="B", size=10)
                        pdf.cell(w=25, h=0, txt="Valor R$", border=0, ln=0, align='R', fill=False, link="")

                        query = \
                            f"""
                            SELECT
                                ROW_NUMBER() OVER (ORDER BY b.seqpedvendaitem) 
                                AS seqpedvendaitem,
                                b.seqproduto||'«»'||INITCAP(c.desccompleta)
                                AS desccompleta,
                                (b.qtdatendida / b.qtdembalagem)
                                AS qtdatendida,
                                SUM(
                                    ((b.qtdatendida / b.qtdembalagem) * 
                                    CASE 
                                    WHEN b.vlrembtabpromoc = 0 
                                    THEN b.vlrembtabpreco 
                                    ELSE b.vlrembtabpromoc 
                                    END) + 
                                    b.vlrtoticmsst
                                ) 
                                AS vlratendido
                            FROM implantacao.cadan_pixpedidos a
                            INNER JOIN implantacao.mad_pedvendaitem b 
                            ON b.nroempresa = a.nroempresa 
                            AND b.nropedvenda = a.nropedvenda
                            AND b.qtdatendida != 0
                            INNER JOIN implantacao.map_produto c 
                            ON c.seqproduto = b.seqproduto 
                            WHERE 1=1
                            AND a.seqpessoa = {customercode}
                            AND a.nropedvenda IN {order}
                            GROUP BY  b.seqpedvendaitem, b.seqproduto, b.qtdatendida, b.qtdembalagem, c.desccompleta	
                            ORDER BY b.seqpedvendaitem ASC
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
                        data = data_disc["data"]

                        if first_page == 1:
                            yline = 70.00
                        else:
                            yline = 33.0

                        for index in range(0, len(data)):
                            item = data[index]["seqpedvendaitem"]
                            product = data[index]["desccompleta"]
                            amount = data[index]["qtdatendida"]
                            value = data[index]["vlratendido"]
                            value_float = float(value)
                            value = locale.format_string("%.2f", value_float, grouping=True)

                            totalvalue = totalvalue + value_float

                            pdf.set_xy(x=0.5, y=yline)
                            pdf.set_font(family=f"{fontname}", style="", size=10)
                            pdf.cell(w=12, h=0, txt=item, border=0, ln=0, align='C', fill=False, link="")

                            pdf.set_xy(x=12.5, y=yline)
                            pdf.set_font(family=f"{fontname}", style="", size=10)
                            pdf.cell(w=160.0, h=0, txt=product, border=0, ln=0, align='L', fill=False, link="")

                            pdf.set_xy(x=160.5, y=yline)
                            pdf.set_font(family=f"{fontname}", style="", size=10)
                            pdf.cell(w=25, h=0, txt=amount, border=0, ln=0, align='C', fill=False, link="")

                            pdf.set_xy(x=180.0, y=yline)
                            pdf.set_font(family=f"{fontname}", style="", size=10)
                            pdf.cell(w=25, h=0, txt=value, border=0, ln=0, align='R', fill=False, link="")

                            if first_page == 1:
                                yline += 6.0
                            else:
                                yline += 5.6

                        # Desenha uma linha abaixo do link do QR Code
                        x_initial = 2.0
                        x_end = 208.0
                        y_actual = pdf.get_y() + 5.0
                        pdf.line(x_initial, y_actual, x_end, y_actual)

                        totalvalue = locale.format_string("%.2f", totalvalue, grouping=True)

                        pdf.set_xy(x=0, y=yline + 3)
                        pdf.set_font(family=f"{fontname}", style="B", size=10)
                        pdf.cell(w=205, h=0, txt=f"TOTAL » {totalvalue}", border=0, ln=0, align='R', fill=False, link="")

                        page_quantity -= 1
                        first_page += 1

                    # Salva o arquivo no diretório /downloads/QRCodes/
                    pdf.output(os.path.join(f"{dirdownloads}/{dirqrcodes}/", f"{txid}.pdf"))

                    try:
                        value = \
                            f"""
                            ( 
                            '{str(companycode)}',
                            '{str(customercode)}',
                            '{str(txid)}',
                            '{str(pixqrcode)}',
                            '{str(totalordervalue)}',
                            'A',
                            SYSDATE
                            )
                            """

                        insert = \
                            f"""
                             INSERT INTO implantacao.cadan_pixtransacoes(
                                pixempresa, 
                                pixpessoa, 
                                pixtxid, 
                                pixqrcode, 
                                pixvalor,
                                pixstatus, 
                                pixdtaenvio
                              )
                             VALUES {value}
                             """
                        cur.execute(insert)

                        update = \
                            f"""
                            UPDATE implantacao.cadan_pixpedidos 
                            SET situacaoped = 'E',
                                txid = '{txid}' 
                            WHERE 1=1
                            AND nroempresa = {companycode}
                            AND seqpessoa = {customercode}
                            AND nropedvenda IN ({ordernumber})
                            """
                        cur.execute(update)
                        conn.commit()
                    except ora.DatabaseError as Error:
                        conn.rollback()
                        messages.info(self, message=f"{Error}")
                        print(Error)
                    cur.close()
                else:
                    cur.close()
                    messages.info(self, message="0")

                messages.info(self, message="2")
            return redirect(to=f"/apppix/brasil-pix-orders/?selectedCustomerCode={customercode}&selectedCustomerName={customername}")
