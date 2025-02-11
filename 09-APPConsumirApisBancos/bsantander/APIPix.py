import requests
from bsantander.URLApi import *
from bsantander.Credenciais import *


class BearerAuth(object):
    def __init__(self):
        self.token = None
        self.data = {
            "client_id": client_id,
            "client_secret": client_secret
        }
        self.header = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        self.params = {
            'grant_type': 'client_credentials'
        }

    def get_acess_token(self):
        """developer_application_key = '1684b6826718dfbf3323dd4b5847ddd7'"""

        data = self.data
        header = self.header
        params = self.params
        resposta = requests.post(
            url=f"{url_token}",
            data=data,
            headers=header,
            params=params
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
        pass