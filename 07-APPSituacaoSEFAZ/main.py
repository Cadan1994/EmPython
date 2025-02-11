import os
import xmltodict
import json
import cx_Oracle as ora
import pandas as pd
import numpy as np
import urllib3
import requests
from pathlib import Path
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from datetime import datetime, timedelta


class ConnectDatabase(object):
    # Parâmetros para conexão com Oracle #
    ora_usuario = 'hsantos'
    ora_senha = 'H1s@ntos1969'
    ora_ipservidor = '192.168.168.200'
    ora_porta = '1521'
    ora_banco = 'orcl'
    ora_client = 'oracle\instantclient_21_3'
    base_dir = Path(__file__).cwd()
    ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

    # Parâmetros para conexão com PostgreSQL #
    pga_usuario = 'powerbi'
    pga_senha = 'cfb5ce8c49'
    pga_ipservidor = '172.16.157.3'
    pga_porta = '2899'
    pga_banco = 'PBICadan'

    def __init__(self):
        pass

    @classmethod
    def conn_oracle(cls):
        # Essa função cria uma conexão com o banco de dados Oracle #
        ConnectDatabase()
        host = cls.ora_ipservidor
        port = cls.ora_porta
        base = cls.ora_banco
        user = cls.ora_usuario
        password = cls.ora_senha
        parametros = f"{user}/{password}@{host}:{port}/{base}"
        return ora.connect(parametros)


def situacao_sefaz(cnpj, uf, ie):
    certificado_path = Path(__file__).cwd()
    certificado_file = 'certificado.pfx'
    certificado = os.path.join(certificado_path, certificado_file)
    urllib3.disable_warnings()
    senha = 'cadan1998'
    homologacao = False

    comunicacao = ComunicacaoSefaz(uf, certificado, senha, homologacao)
    resposta = comunicacao.status_code(modelo='nfe')

    if resposta == 200:
        retorno_xml = comunicacao.consulta_cadastro(modelo='nfe', cnpj=cnpj)
        xml_dict = xmltodict.parse(retorno_xml.text)
        """print(json.dumps(xml_dict, indent=4))"""

        if uf == 'PE':
            wservice = 'soapenv'
            status_codigo = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['retConsCad']['infCons']['cStat']
        elif uf in ('PR', 'GO'):
            wservice = 'env'
            status_codigo = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['retConsCad']['infCons']['cStat']
        elif uf == 'MG':
            wservice = 'S'
            status_codigo = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['consultaCadastro4Result']['retConsCad']['infCons']['cStat']
        elif uf == 'MT':
            wservice = 'soapenv'
            status_codigo = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['consultaCadastroResult']['retConsCad']['infCons']['cStat']
        else:
            wservice = 'soap'
            status_codigo = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['retConsCad']['infCons']['cStat']

        if int(status_codigo) in [111, 112]:
            if uf == 'MG':
                status_situacao = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['consultaCadastro4Result']['retConsCad']['infCons']['infCad']
            elif uf == 'MT':
                status_situacao = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['consultaCadastroResult']['retConsCad']['infCons']['infCad']
            else:
                status_situacao = xml_dict[f'{wservice}:Envelope'][f'{wservice}:Body']['nfeResultMsg']['retConsCad']['infCons']['infCad']

            situacao = None

            if int(status_codigo) == 111:
                list_dict = list()
                list_dict.append(status_situacao)
                list_situacao = list_dict
                dicionario = dict(list_situacao[0])
                status_situacao = dicionario['cSit']
                if int(status_situacao) == 0:
                    situacao = 'Não Habilitado'
                else:
                    situacao = 'Habilitado'
            else:
                list_situacao = list(status_situacao)

            if len(list_situacao) > 1:
                for i in range(0, len(list_situacao)):
                    dicionario = dict(list_situacao[i])
                    valor = dicionario['IE']
                    if valor == f'{ie}':
                        status_situacao = dicionario['cSit']
                        if int(status_situacao) == 0:
                            situacao = 'Não Habilitado'
                        else:
                            situacao = 'Habilitado'
                        break
                    else:
                        status_situacao = 2
                        situacao = f'Cliente não encontrado na SEFAZ com IE: {ie}'

            if status_situacao == 0 or status_situacao == 2:
                print(f'Status: {status_codigo} - CNPJ: {cnpj} - UF: {uf} - Situação: {situacao}')

            return status_situacao
        elif int(status_codigo) == 259:
            status_situacao = 0
            situacao = 'Não Habilitado'
            print(f'Status: {status_codigo} - CNPJ: {cnpj} - UF: {uf} - Situação: {situacao}')
            return status_situacao


def consulta_cliente_sefaz():
    dh = datetime.now()
    dd = dh.strftime("%d")
    dm = dh.strftime("%m")
    da = dh.strftime("%Y")
    try:
        query = f"""
                SELECT
                    DISTINCT
                    LPAD(TO_CHAR(a.seqpessoa), 6, 0)												
                    AS seqpessoa,
                    LPAD(b.nrocgccpf, 12, '0')||LPAD(b.digcgccpf, 2, '0')		
                    AS nrocgccpf,
                    b.inscricaorg,
                    INITCAP(b.nomerazao)														 				
                    AS nomerazao,
                    b.uf
                FROM implantacao.mrl_cliente a
                INNER JOIN implantacao.ge_pessoa b 
                ON b.seqpessoa = a.seqpessoa	
                AND b.inscricaorg != 'ISENTO' 
                AND b.fisicajuridica = 'J'
                AND b.uf NOT IN ('PE')
                WHERE 1=1
                AND a.seqpessoa NOT IN (1, 22401)
                AND a.seqpessoa = 48305
                AND a.statuscliente = 'A'
                ORDER BY 1 ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        columns = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata = pd.DataFrame.from_records(data, columns=columns)

        if not os.path.isfile(f'ClientesSEFAZ{dd + dm + da}.txt'):
            write_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'w')
            write_file.close()

        if os.path.isfile(f'ClientesSEFAZ{dd + dm + da}.txt'):
            write_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'w')
            write_file.close()
            write_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'a')
            for index, reg in dfdata.iterrows():
                pessoa = reg.SEQPESSOA
                cnpj = reg.NROCGCCPF
                uf = reg.UF
                ie = reg.INSCRICAORG
                status = situacao_sefaz(cnpj, uf, ie)
                print(status)

                if status is None:
                    status = '0'

                if int(status) == 0:
                    situacao = 'Não Habilitado'
                    record = f'{str(pessoa)};{str(cnpj)};{str(uf)};{str(situacao)}'
                    write_file.write(f'{record}\n')
                elif int(status) == 2:
                    situacao = f'Cliente não encontrado na SEFAZ com IE: {ie}'
                    record = f'{str(pessoa)};{str(cnpj)};{str(uf)};{str(situacao)}'
                    write_file.write(f'{record}\n')

            write_file.close()

        heard = f'{columns[0]};{columns[1]};{columns[4]};SITUACAO'

        if not os.path.isfile(f'ClientesSEFAZ{dd + dm + da}.txt'):
            write_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'w')
            write_file.write(f'{heard}\n')
            write_file.close()

        if os.path.isfile(f'ClientesSEFAZ{dd + dm + da}.txt'):
            read_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'r')
            total_lines = len(read_file.readlines())
            write_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'a')

            if total_lines == 1:
                for index, reg in dfdata.iterrows():
                    pessoa = reg.SEQPESSOA
                    cnpj = reg.NROCGCCPF
                    uf = reg.UF
                    ie = reg.INSCRICAORG
                    record = f'{str(pessoa)};{str(cnpj)};{str(uf)};{str(situacao_sefaz(cnpj, uf, ie))}'
                    write_file.write(f'{record}\n')
            else:
                for index, reg in dfdata.iterrows():
                    pessoa = reg.SEQPESSOA
                    cnpj = reg.NROCGCCPF
                    uf = reg.UF
                    ie = reg.INSCRICAORG
                    record = f'{str(pessoa)};{str(cnpj)};{str(uf)};{str(situacao_sefaz(cnpj, uf, ie))}'
                    record_search = record[:7]
                    if not ler_arquivo_clientessefaz(record_search):
                        write_file.write(f'{record}\n')

    except ora.DatabaseError as Error:
        return HttpResponse(Error)


def ler_arquivo_clientessefaz(record):
    """
    Essa função ler os dos dados nos arquivos txt:
    @Arquivo: ClientesSEFAZNHabilitadoPE.txt
    @Arquivo: ClientesSEFAZNHabilitadoOU.txt
    @param record:
    @return:
    """
    dh = datetime.now()
    dd = dh.strftime("%d")
    dm = dh.strftime("%m")
    da = dh.strftime("%Y")
    result = False
    read_file = open(f'ClientesSEFAZ{dd + dm + da}.txt', 'r')
    i = 1
    records_list = read_file.readlines()
    records_list_size = len(records_list)

    while i <= records_list_size - 1:
        line = records_list[i]
        record_file = line[:7]
        i += 1
        if record == record_file:
            result = True
            break

    return result


def ler_arquivo_clientessefaz_estado(identificador):

    df_principal = pd.read_csv(filepath_or_buffer=f'ClientesSEFAZNHabilitado{identificador}.txt', sep=';', encoding='latin-1')
    df_dados = df_principal[['REGISTRO', 'CNPJ', 'UF']]
    conn = ConnectDatabase.conn_oracle()
    cur = conn.cursor()

    if not os.path.isfile('ClientesSEFAZ_NaoHabilitado.txt'):
        write_file = open('ClientesSEFAZ_NaoHabilitado.txt', 'w')
        write_file.close()

    if os.path.isfile('ClientesSEFAZ_NaoHabilitado.txt'):
        read_file = open('ClientesSEFAZ_NaoHabilitado.txt', 'a')

        for index, reg in df_dados.iterrows():
            seqpessoa = reg.REGISTRO
            cnpj = reg.CNPJ
            uf = reg.UF
            print(f'Registro: {seqpessoa} » UF: {uf} » CNPJ: {cnpj} ')
            query = f"""
                    SELECT count(*) quantidade 
                    FROM implantacao.fi_titulo a 
                    WHERE 1 = 1 AND a.obrigdireito = 'D' 
                    AND a.Abertoquitado = 'A' 
                    AND a.seqpessoa = {seqpessoa}
                    """
            cur.execute(query)
            colunas = [row[0] for row in cur.description]
            dados = cur.fetchall()
            df = pd.DataFrame.from_records(data=dados, columns=colunas)
            qtde = df['QUANTIDADE'].values[0]

            if int(qtde) == 0:
                read_file.write(f'{seqpessoa};{uf};{cnpj}\n')

    cur.close()


if __name__ == '__main__':
    #ler_arquivo_clientessefaz_estado('OU')
    #consulta_cliente_sefaz()
    situacao_sefaz(cnpj="32918895000134", uf="PE", ie="081918160")