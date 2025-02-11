import requests
from requests.auth import HTTPBasicAuth
from data_queries import *
from gerarqrcode import GerarQrCodePix


parametros = consulta_tabelaparametros()
authorization = parametros['AUTHORIZATION']['VALOR']
client_id = parametros['CLIENT_ID']['VALOR']
client_secret = parametros['CLIENT_SECRET']['VALOR']
chave_pix = parametros['CHAVE_PIX']['VALOR']
gw_dev_app_key = parametros['GW_DEV_APP_KEY']['VALOR']
gw_app_key = "95cad3f03fd9013a9d15005056825665"
url_token = parametros['URL_TOKEN']['VALOR']
url_cobranca_imediata = parametros['URL_APIPIX']['VALOR']
url_simulacao_pagamento = "https://api.hm.bb.com.br/testes-portal-desenvolvedor/v1/boletos-pix/pagar"


class BearerAuth(object):
    def __init__(self):
        self.token = None
        self.data = {
            'grant_type': 'client_credentials',
            'scope': 'cob.read cob.write cobv.read cobv.write pix.read pix.write'
        }
        self.header = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"{authorization}"
        }

    def get_acess_token(self):
        data = self.data
        header = self.header
        resposta = requests.post(
            url=f"{url_token}",
            data=data,
            auth=HTTPBasicAuth(username=f"{client_id}", password=f"{client_secret}"),
            headers=header
        )
        self.token = resposta.json()
        return f"Bearer {self.token['access_token']}"


class ApiPixCobrancaImediata(object):
    def __init__(self):
        bearerauth = BearerAuth()
        self.token = bearerauth.get_acess_token()
        self.gwdevappkey = f"{gw_dev_app_key}"
        self.headers = {"Content-Type": "application/json", "Authorization": f"{self.token}"}
        self.params = {"gw-dev-app-key": f"{self.gwdevappkey}"}
        self.url = None
        self.txid = None

    def cobranca_definida_usuario(self):
        """ CRIA UMA COBRANÇA IMEDIATA, COM TXID DEFINIDO PELO USUÁRIO RECEBEDOR """
        self.url = f"{url_cobranca_imediata}/cob"
        txid = "1052817578000139050452L000000000000"
        payload = {
            "calendario": {
                "expiracao": 1728000
            },
            "devedor": {
                "cnpj": "052817578000139",
                "nome": "AGL CHURRASCARIA LTDA"
            },
            "valor": {
                "original": "1949.78"
            },
            "chave": "9e881f18-cc66-4fc7-8f2c-a795dbb2bfc1",
            "solitacaoPagador": "Compra realizada.",
            "infoAdicionais": [
                {
                "nome": "Pedidos: 4446773, 4446774",
                "valor": "1949.78"
                }
            ]
        }
        response = requests.put(url=f"{self.url}/{txid}", data="", json=payload, headers=self.headers, params=self.params)
        response_json = response.json()

        #dicionario = dict(response_json)
        #print(dicionario["pixCopiaECola"])
        #gerarqrcode = GerarQrCodePix()
        #gerarqrcode.gerar_payload(dicionario["pixCopiaECola"])

        return print(response_json)

    def cobranca_definida_banco(self):
        """ CRIA UMA COBRANÇA IMEDIATA, COM TXID DEFINIDO PELO BANCO """
        self.url = f"{url_cobranca_imediata}/cob"
        payload = {
            "calendario": {
                "expiracao": 3600
            },
            "devedor": {
                "cnpj": "007635903000198",
                "nome": "CAFE E RESTAURANTE BOA VISTA LTDA"
            },
            "valor": {
                "original": "2763.94"
            },
            "chave": "9e881f18-cc66-4fc7-8f2c-a795dbb2bfc1",
            "solcnpjitacaoPagador": "Compra realizada.",
            "infoAdicionais": [
                {
                "nome": "Pedidos: 4446771, 4446772, 4446788, 4446789",
                "valor": "320.28, 546.29, 890.88, 1006.49"
                }
            ]
        }

        requisicao = requests.put(url=self.url, data="", json=payload, headers=self.headers, params=self.params)
        resposta = requisicao.status_code
        # dicionario = dict(resposta)
        # print(dicionario["pixCopiaECola"])
        # gerarqrcode = GerarQrCodePix()
        # gerarqrcode.gerar_qr_code(dicionario["pixCopiaECola"])

        return print(resposta)

    def consultar_cobranca_recebida(self):
        """ CONSULTA UMA LISTA DE COBRANÇAS IMEDIATAS """
        self.url = f"{url_cobranca_imediata}/cob/"
        self.txid = [
            "1002409562200019000601L000004446852"
        ]
        for txid in self.txid:
            params = {
                "gw-dev-app-key": f"{gw_dev_app_key}",
                "inicio": "2024-12-01T00:00:01Z",
                "fim": "2024-12-03T23:59:59Z"
            }
            requisicao = requests.get(url=f"{self.url}{txid}", headers=self.headers, params=params)
            resposta = requisicao.json()
            print(resposta) #["pixCopiaECola"]

    def simular_pagamento_pix(self):
        """ SIMULAÇÃO DE PAGAMENTO """
        self.url = f"{url_simulacao_pagamento}"
        params = {
            "gw-app-key": f"{gw_app_key}"
        }
        payload = {
            "pix": "00020101021226870014br.gov.bcb.pix2565qrcodepix-h.bb.com.br/pix/v2/0517c963-a43a-48b6-bd25-386ea18353a95204000053039865406809.405802BR5921PAPELARIA LEITE CUNHA6012RONDONOPOLIS62070503***6304ED16"
        }
        requisicao = requests.post(url=self.url, json=payload, headers=self.headers, params=params)
        resposta = requisicao.json()

        return print(resposta)

    def consultar_lista_pix_recebidos(self):
        """ CONSULTAR UM LISTA DE PIX RECEBIDOS """
        self.url = f"{url_cobranca_imediata}/pix"
        params = {
            "gw-dev-app-key": f"{gw_dev_app_key}",
            "inicio": "2024-11-08T00:00:01Z",
            "fim": "2024-11-11T23:59:59Z",
            "txid": "1003663226400015047609L000004446813"
        }

        requisicao = requests.get(url=self.url, headers=self.headers, params=params)
        resposta = requisicao.json()
        dicionario = dict(resposta)
        print(dicionario)

        return None


class ApiPixCobrancaVencimento(object):
    def __init__(self):
        bearerauth = BearerAuth()
        self.token = bearerauth.get_acess_token()
        self.gwdevappkey = f"{gw_dev_app_key}"
        self.headers = {"Content-Type": "application/json", "Authorization": f"{self.token}"}
        self.params = {"gw-dev-app-key": f"{self.gwdevappkey}"}
        self.url = None
        self.txid = None

    def cobranca_definida_usuario(self):
        """ CRIA UMA COBRANÇA IMEDIATA, COM TXID DEFINIDO PELO USUÁRIO RECEBEDOR """
        self.url = url_cobranca_vencimento
        txid = "CadanDistribuicao01Rem0000000000003"
        payload = {
            "calendario": {
                "dataDeVencimento": "2024-10-31"
            },
            "devedor": {
                "cnpj": "12345678901234",
                "nome": "Odorico Paraguacu"
            },
            "valor": {
                "original": "123.34"
            },
            "chave": f"{chave_pix_email}"
        }
        requisicao = requests.put(url=f"{self.url}/{txid}", data="", json=payload, headers=self.headers, params=self.params)
        resposta = requisicao.json()
        dicionario = dict(resposta)
        # print(dicionario["pixCopiaECola"])
        # gerarqrcode = GerarQrCodePix()
        # gerarqrcode.gerar_qr_code(dicionario["pixCopiaECola"])

        return print(dicionario)

    def consultar_cobranca_recebida(self):
        """ CONSULTA UMA LISTA DE COBRANÇAS IMEDIATAS """
        self.url = f"{url_cobranca_vencimento}"
        self.txid = "/CadanDistribuicao01Rem0000000002   "
        params = {
            "gw-dev-app-key": f"{gw_dev_app_key}",
            "inicio": "2024-10-25T00:00:01Z",
            "fim": "2024-10-28T23:59:59Z",
            "cpf": "",
            "cnpj": "",
            "locationPresente": "False",
            "status": "ATIVA",
            "paginacao.paginaAtual": 0,
            "paginacao.itensPorPagina": 100
        }
        requisicao = requests.get(url=f"{self.url}{self.txid}", headers=self.headers, params=params)
        resposta = requisicao.json()

        if len(self.txid) == 0:
            lista = resposta["cobs"]
            for reg in lista:
                print(reg)
        else:
           return print(resposta)