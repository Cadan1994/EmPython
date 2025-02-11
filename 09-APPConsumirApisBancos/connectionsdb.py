import os
import cx_Oracle as ora
import psutil
from pathlib import Path


class ConnectionsDataBase(object):
    """
    Essa classe é utilizada para conexões com os seguintes bancos de dados:
    Oracle e PostgreSQL
    Criado por.....................: HILSON SANTOS
    Data da criação................: 29/10/2024
    """

    # Parâmetros para conexão com Oracle #
    ora_usuario = 'hsantos'
    ora_senha = 'H1s@ntos1969'
    ora_ipservidor = '192.168.168.206'
    ora_porta = '1521'
    ora_banco = 'orcltst'
    ora_client = 'oracle\instantclient_21_3'
    base_dir = Path(__file__).cwd()

    # Retorna um gerador produzindo uma instância de Processo para todos os processos em execução #
    for proc in psutil.process_iter(['pid', 'name']):
        if 'init_oracle_client' in proc.info['name']:
            ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

    # Parâmetros para conexão com PostgreSQL #
    pga_usuario = 'postgres'
    pga_senha = 'cfb5ce8c49'
    pga_ipservidor = '172.16.157.3'
    pga_porta = '2899'
    pga_banco = 'PBICadan'

    def __init__(self):
        pass

    @classmethod
    def conn_oracle(cls):
        """ Essa função cria uma conexão com o banco de dados Oracle """
        ConnectionsDataBase()
        host = cls.ora_ipservidor
        port = cls.ora_porta
        base = cls.ora_banco
        user = cls.ora_usuario
        password = cls.ora_senha
        parametros = f"{user}/{password}@{host}:{port}/{base}"
        return ora.connect(parametros)

    @classmethod
    def conn_postgresql(cls):
        """ Essa função cria uma conexão com o banco de dados PostgreSQL """
        ConnectionsDataBase()
        host = cls.pga_ipservidor
        port = cls.pga_porta
        base = cls.pga_banco
        user = cls.pga_usuario
        password = cls.pga_senha
        return pga.connect(dbname=base, user=user, password=password, host=host, port=port)