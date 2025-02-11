import os
import cx_Oracle as ora
import psutil
from pathlib import Path


class ConnectDataBase(object):
    """ Paramêtros para conexão com Oracle """
    """DEVPOL"""
    """o2T56TZ6x"""

    # Variáveis globais #
    ora_usuario = 'DEVPOL'
    ora_senha = 'o2T56TZ6x'
    ora_ipservidor = '192.168.168.200'
    ora_porta = '1521'
    ora_banco = 'orcl'
    # Caminho do client do oracle #
    ora_client = 'oracle\instantclient_21_3'
    # Pega o caminho onde se encontra o diretório oracle do client #
    base_dir = Path(__file__).cwd()
    # Verifica se o client do oracle está iniciado #
    for proc in psutil.process_iter(['pid', 'name']):
        if 'init_oracle_client' in proc.info['name']:
            ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

    def __init__(self):
        pass

    @classmethod
    def conn_oracle(cls):
        """
        Essa função cria uma conexão com o banco de dados Oracle
        :return: a conexão
        """
        host = cls.ora_ipservidor
        port = cls.ora_porta
        base = cls.ora_banco
        user = cls.ora_usuario
        password = cls.ora_senha
        parameter = f'{user}/{password}@{host}:{port}/{base}'
        return ora.connect(parameter)