import os
from pathlib import Path
import cx_Oracle as Ora


class ConnectBD(object):
    # Parâmetros para conexão com Oracle
    ora_usuario = 'hsantos'
    ora_senha = 'H1s@ntos1969'
    ora_ipservidor = '192.168.168.206'
    ora_porta = '1521'
    ora_banco = 'orcltst'
    ora_client = 'oracle\instantclient_21_3'
    base_dir = Path(__file__).parent
    Ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

    def __init__(self):
        pass

    # Essa função cria uma conexão com o banco de dados Oracle
    @classmethod
    def conn_oracle(cls):
        ConnectBD()
        host = cls.ora_ipservidor
        port = cls.ora_porta
        base = cls.ora_banco
        user = cls.ora_usuario
        password = cls.ora_senha
        parametro = f"{user}/{password}@{host}:{port}/{base}"
        return Ora.connect(parametro)
