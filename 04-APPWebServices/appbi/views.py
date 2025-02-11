import json
import os
import cx_Oracle as ora
import psycopg2 as pga
import pandas as pd
import numpy as np
import requests
import calendar
import xmltodict
import urllib3
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from datetime import datetime, timedelta
from pathlib import Path
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse


class ConnectDatabase(object):
    """ Parâmetros para conexão com Oracle """
    ora_usuario = 'hsantos'
    ora_senha = 'H1s@ntos1969'
    ora_ipservidor = '192.168.168.200'
    ora_porta = '1521'
    ora_banco = 'orcl'
    ora_client = 'oracle\instantclient_21_3'
    base_dir = Path(__file__).cwd()
    ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

    """ Parâmetros para conexão com PostgreSQL """
    pga_usuario = 'powerbi'
    pga_senha = 'cfb5ce8c49'
    pga_ipservidor = '172.16.157.3'
    pga_porta = '2899'
    pga_banco = 'PBICadan'

    def __init__(self):
        pass

    @classmethod
    def conn_oracle(cls):
        """ Essa função cria uma conexão com o banco de dados Oracle """
        ConnectDatabase()
        host = cls.ora_ipservidor
        port = cls.ora_porta
        base = cls.ora_banco
        user = cls.ora_usuario
        password = cls.ora_senha
        parametros = f"{user}/{password}@{host}:{port}/{base}"
        return ora.connect(parametros)

    @classmethod
    def conn_postgresql(cls):
        ConnectDatabase()
        host = cls.pga_ipservidor
        port = cls.pga_porta
        base = cls.pga_banco
        user = cls.pga_usuario
        password = cls.pga_senha
        return pga.connect(dbname=base, user=user, password=password, host=host, port=port)


# FUNÇÕES REFERENTE A EXECUÇÃO DO PAINEL01 » LOGÍSTICA #
########################################################################################################################
def painellog01_dashboard(request):
    return render(request, 'bilogistica/painel01.html')


def analise_log_painel01(request):
    """
    Essa função retorna a quantidade de pedidos de acordo com os seguintes dados:
    » Tipos ("ENTREGA", "RETIRA")
    » Data limite fatura
    » Status ("ANALISE", "CANCELADO", "LIBERADO", "SEPARAÇÃO")
    » Críticas ("COMERCIAL", "FINANCEIRO")
    @param request:
    @return:
    """
    try:
        query = """
                SELECT
                    DISTINCT 
                    a.nropedvenda,	
                    TO_DATE(a.dtainclusao) 
                    AS dtainclusao,
                    TO_DATE(a.dtainclusao + 8)
                    AS dtalimfatura,
                    DECODE(
                        a.indentregaretira,
                        'E', 'ENTREGA',
                        'R', 'RETIRA'
                    )
                    AS indentregaretira,
                    a.seqpessoa,
                    b.nomerazao,
                    DECODE(
                        a.situacaoped, 
                        'A', 'ANALISE', 
                        'C', 'CANCELADO', 
                        'D', 'DIGITACAO', 
                        'F', 'FATURADO', 
                        'L', 'LIBERADO', 
                        'P', 'PRE-SEPARACAO', 
                        'R', 'RETEIRIZACAO', 
                        'S', 'SEPARACAO', 
                        'W', 'SEPARADO'
                    )
                    AS situacaoped,
                    DECODE(
                        a.indcriticapedido, 
                        'F', 'FINANCEIRO', 
                        'B', 'COMERCIAL', 
                        '', 'COMERCIAL', 
                        'L', 'LIBERADO'
                    )
                    AS indcriticapedido,
                    a.motcancelamento,
                    ROUND(sysdate - a.dtainclusao, 0)
                    AS quantidadedia,
                    j.descrota
                FROM implantacao.mad_pedvenda     a,
                     implantacao.ge_pessoa        b,
                     implantacao.mad_segmento     d,
                     implantacao.mad_pedvendaitem e,
                     implantacao.map_famembalagem f,
                     implantacao.map_produto      g,
                     implantacao.mad_clienteend   h,
                     implantacao.mad_praca        i,
                     implantacao.mad_rota         j
                WHERE 1=1
                AND b.seqpessoa = a.seqpessoa
                AND h.seqpessoa = a.seqpessoa
                AND d.nrosegmento = a.nrosegmento
                AND e.nropedvenda = a.nropedvenda
                AND e.qtdembalagem = f.qtdembalagem
                AND e.seqproduto = g.seqproduto
                AND f.seqfamilia = g.seqfamilia
                AND h.seqpraca = i.seqpraca
                AND i.seqrota = j.seqrota
                AND a.nroempresa = 1
                AND a.situacaoped NOT IN ('F', 'D', 'R')
                AND ROUND(sysdate - a.dtainclusao, 0) <= 8
                GROUP BY a.nropedvenda, a.indentregaretira, a.nrocarga, a.seqpessoa,
                         b.nomerazao, b.fantasia, a.situacaoped, a.dtainclusao, a.dtaalteracao,
                         ROUND(sysdate - a.dtainclusao), a.dtabasefaturamento, a.nrosegmento,
                         d.descsegmento, a.dtalibcredped, j.descrota, a.motcancelamento,
                         a.obspedido, a.indcriticapedido, a.dtahorgeracaonf, a.dtainclusao,
                         a.dtageracaocarga, a.dtahorsituacaopedalt, a.usualteracao, a.usuaprovcredito,
                         e.qtdpedida, e.qtdatendida
                ORDER BY 3 ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata = pd.DataFrame.from_records(data, columns=colums)
        dfdata['DTAINCLUSAO'] = pd.to_datetime(dfdata['DTAINCLUSAO'], errors='coerce').dt.strftime('%d/%m/%Y')
        pd.set_option('display.precision', 2)

        # A variável receber o campo a ser filtrado
        dffilter = dfdata['SITUACAOPED'] != 'CANCELADO'
        dffilter_situacao = dfdata['SITUACAOPED'] == 'ANALISE'
        dffilter_critica = dfdata['INDCRITICAPEDIDO'] != 'LIBERADO'

        # A variável pega a quantidade total dos pedidos por tipo
        li_pedidostipo = list()
        df_pedidostipo = dfdata.loc[dffilter, ['INDENTREGARETIRA', 'NROPEDVENDA']]
        df_pedidostipo = df_pedidostipo[['INDENTREGARETIRA', 'NROPEDVENDA']].groupby('INDENTREGARETIRA').nunique().reset_index()
        df_pedidostipo = df_pedidostipo.rename(columns={'INDENTREGARETIRA': 'Descrição', 'NROPEDVENDA': 'Quant.'}).copy()
        df_pedidostipo.fillna(value=0, inplace=True)

        li_pedidostipo.append(df_pedidostipo.columns.tolist())
        for lista in df_pedidostipo.values:
            li_pedidostipo.append(lista.tolist())

        # A variável pega a quantidade total dos pedidos por data limite fatura
        li_pedidosdataslimfatura = list()
        df_pedidosdataslimfatura = dfdata.loc[dffilter, ['DTALIMFATURA', 'NROPEDVENDA', 'INDENTREGARETIRA']]
        filterentrega = df_pedidosdataslimfatura['INDENTREGARETIRA'] == 'ENTREGA'
        filterretira = df_pedidosdataslimfatura['INDENTREGARETIRA'] == 'RETIRA'
        df_pedidos_entrega = df_pedidosdataslimfatura.loc[filterentrega, ['DTALIMFATURA', 'NROPEDVENDA']].groupby('DTALIMFATURA').nunique().reset_index()
        df_pedidos_entrega = df_pedidos_entrega.rename(columns={'DTALIMFATURA': 'Data', 'NROPEDVENDA': 'Entrega'}).copy()
        df_pedidos_retira = df_pedidosdataslimfatura.loc[filterretira, ['DTALIMFATURA', 'NROPEDVENDA']].groupby('DTALIMFATURA').nunique().reset_index()
        df_pedidos_retira = df_pedidos_retira.rename(columns={'DTALIMFATURA': 'Data', 'NROPEDVENDA': 'Retira'}).copy()
        df_pedidosdataslimfatura = pd.merge(df_pedidos_entrega, df_pedidos_retira, left_on='Data', right_on='Data', how='outer').copy()
        df_pedidosdataslimfatura.fillna(value=0, inplace=True)
        df_pedidosdataslimfatura['Data'] = pd.to_datetime(df_pedidosdataslimfatura['Data'], errors='coerce').dt.strftime('%d/%m/%Y')

        li_pedidosdataslimfatura.append(df_pedidosdataslimfatura.columns.tolist())
        for lista in df_pedidosdataslimfatura.values:
            li_pedidosdataslimfatura.append(lista.tolist())

        # A variável pega a quantidade total dos pedidos por status
        li_pedidosstatus = list()
        df_pedidosstatus = dfdata.loc[dffilter, ['SITUACAOPED', 'NROPEDVENDA', 'INDENTREGARETIRA']]

        filterentrega = df_pedidosstatus['INDENTREGARETIRA'] == 'ENTREGA'
        filterretira = df_pedidosstatus['INDENTREGARETIRA'] == 'RETIRA'
        df_pedidos_entrega = df_pedidosstatus.loc[filterentrega, ['SITUACAOPED', 'NROPEDVENDA']].groupby('SITUACAOPED').nunique().reset_index()
        df_pedidos_entrega = df_pedidos_entrega.rename(columns={'SITUACAOPED': 'Descrição', 'NROPEDVENDA': 'Entrega'}).copy()
        df_pedidos_retira = df_pedidosstatus.loc[filterretira, ['SITUACAOPED', 'NROPEDVENDA']].groupby('SITUACAOPED').nunique().reset_index()
        df_pedidos_retira = df_pedidos_retira.rename(columns={'SITUACAOPED': 'Descrição', 'NROPEDVENDA': 'Retira'}).copy()
        df_pedidosstatus = pd.merge(df_pedidos_entrega, df_pedidos_retira, left_on='Descrição', right_on='Descrição', how='outer').copy()
        df_pedidosstatus.fillna(value=0, inplace=True)

        li_pedidosstatus.append(df_pedidosstatus.columns.tolist())
        for lista in df_pedidosstatus.values:
            li_pedidosstatus.append(lista.tolist())

        # A variável pega a quantidade total dos pedidos por crítica
        li_pedidoscriticas = list()
        df_pedidoscriticas = dfdata.loc[dffilter_situacao & dffilter_critica, ['INDCRITICAPEDIDO', 'NROPEDVENDA', 'INDENTREGARETIRA']]

        filterentrega = df_pedidoscriticas['INDENTREGARETIRA'] == 'ENTREGA'
        filterretira = df_pedidoscriticas['INDENTREGARETIRA'] == 'RETIRA'
        df_pedidos_entrega = df_pedidoscriticas.loc[filterentrega, ['INDCRITICAPEDIDO', 'NROPEDVENDA']].groupby('INDCRITICAPEDIDO').nunique().reset_index()
        df_pedidos_entrega = df_pedidos_entrega.rename(columns={'INDCRITICAPEDIDO': 'Descrição', 'NROPEDVENDA': 'Entrega'}).copy()
        df_pedidos_retira = df_pedidoscriticas.loc[filterretira, ['INDCRITICAPEDIDO', 'NROPEDVENDA']].groupby('INDCRITICAPEDIDO').nunique().reset_index()
        df_pedidos_retira = df_pedidos_retira.rename(columns={'INDCRITICAPEDIDO': 'Descrição', 'NROPEDVENDA': 'Retira'}).copy()
        df_pedidoscriticas = pd.merge(df_pedidos_entrega, df_pedidos_retira, left_on='Descrição', right_on='Descrição', how='outer').copy()
        df_pedidoscriticas.fillna(value=0, inplace=True)
        
        li_pedidoscriticas.append(df_pedidoscriticas.columns.tolist())
        for lista in df_pedidoscriticas.values:
            li_pedidoscriticas.append(lista.tolist())

        # A variável representa o dicionário de dados para gerar nos gráficos
        data = {
            'jsonpedidostipos': li_pedidostipo,
            'jsonpedidosdataslimfatura': li_pedidosdataslimfatura,
            'jsonpedidosstatus': li_pedidosstatus,
            'jsonpedidoscriticas': li_pedidoscriticas
        }

        return JsonResponse(data)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


# FUNÇÕES REFERENTE A EXECUÇÃO DO PAINEL02 » LOGÍSTICA #
########################################################################################################################
def painellog02_dashboard(request):
    return render(request, 'bilogistica/painel02.html')


def ler_arquivo_dados(record):
    """
    Essa função ler os parâmetros gravados nos arquivos txt:
    » IntervalosPedidosMesAno.txt
    @param record:
    @return:
    """
    dh = datetime.now()
    dm = dh.strftime("%m")
    da = dh.strftime("%Y")
    result = False
    read_file = open(f'IntervalosPedidos{dm + da}.txt', 'r')
    i = 1
    records_list = read_file.readlines()
    records_list_size = len(records_list)

    while i <= records_list_size - 1:
        line = records_list[i]
        record_file = line[:7] + line[28:][:2]
        i += 1
        if record == record_file:
            result = True
            break

    return result


def gerar_arquivo_dados():
    dh = datetime.now()
    dm = dh.strftime("%m")
    da = dh.strftime("%Y")
    hr = datetime.strptime(dh.strftime("%X"), "%H:%M:%S")
    hi = datetime.strptime("06:00:00", "%H:%M:%S")
    hf = datetime.strptime("20:00:00", "%H:%M:%S")
    try:
        query = """
                SELECT 
                    a.nropedvenda,
                    a.dtahorsituacaopedalt 
                    AS datahora,	 
                    CASE 
                    WHEN a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '06:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                                AND     TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '08:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    THEN '06 AS 09'
                    WHEN a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '09:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                                AND     TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '11:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    THEN '09 AS 12'
                    WHEN a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '12:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                                AND     TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '14:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    THEN '12 AS 15'
                    WHEN a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '15:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                                AND     TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '16:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    THEN '15 AS 17'
                    WHEN a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '17:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                                AND     TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || '19:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    THEN '17 AS 20'
                    ELSE ''
                    END 
                    AS intervalo,
                    a.indentregaretira,
                    SUM(NVL((b.qtdatendida / b.qtdembalagem) * b.vlrembinformado, 0))
                    AS pesobruto
                FROM implantacao.mad_pedvenda a
                INNER JOIN implantacao.mad_pedvendaitem b 
                ON b.nroempresa=a.nroempresa AND b.nropedvenda=a.nropedvenda AND b.qtdatendida > 0
                INNER JOIN implantacao.map_produto c ON c.seqproduto = b.seqproduto
                INNER JOIN implantacao.map_famembalagem d ON d.seqfamilia = c.seqfamilia AND d.qtdembalagem = b.qtdembalagem
                WHERE 1=1
                AND a.nroempresa = 1
                AND a.situacaoped = 'L'
                AND a.dtahorsituacaopedalt BETWEEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || ' 06:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                AND TO_DATE(TO_CHAR(SYSDATE, 'YYYY-MM-DD') || ' 19:59:59', 'YYYY-MM-DD HH24:MI:SS')
                GROUP BY a.nropedvenda,a.dtahorsituacaopedalt,a.indentregaretira 		
                ORDER BY 2 ASC
                """
        if (hr >= hi) and (hr <= hf):
            conn = ConnectDatabase.conn_oracle()
            cur = conn.cursor()
            cur.execute(query)
            columns = [row[0] for row in cur.description]
            data = cur.fetchall()
            dfdata = pd.DataFrame.from_records(data, columns=columns)

            heard = f'{columns[0]};{columns[1]};{columns[2]};{columns[3]};{columns[4]}'

            if not os.path.isfile(f'IntervalosPedidos{dm + da}.txt'):
                write_file = open(f'IntervalosPedidos{dm + da}.txt', 'w')
                write_file.write(f'{heard}\n')
                write_file.close()

            if os.path.isfile(f'IntervalosPedidos{dm + da}.txt'):
                read_file = open(f'IntervalosPedidos{dm + da}.txt', 'r')
                total_lines = len(read_file.readlines())
                write_file = open(f'IntervalosPedidos{dm + da}.txt', 'a')

                if total_lines == 1:
                    for index, reg in dfdata.iterrows():
                        record = f'{str(reg.NROPEDVENDA)};{str(reg.DATAHORA)};{str(reg.INTERVALO)};{reg.INDENTREGARETIRA};{reg.PESOBRUTO}'
                        write_file.write(f'{record}\n')
                else:
                    for index, reg in dfdata.iterrows():
                        record = f'{str(reg.NROPEDVENDA)};{str(reg.DATAHORA)};{str(reg.INTERVALO)};{reg.INDENTREGARETIRA};{reg.PESOBRUTO}'
                        record_search = record[:7] + record[28:][:2]
                        if not ler_arquivo_dados(record_search):
                            write_file.write(f'{record}\n')

    except ora.DatabaseError as Error:
        return HttpResponse(Error)


def ler_arquivo_clientessefaz(record):
    """
    Essa função ler os parâmetros gravados nos arquivos txt:
    » ClientesSEFAZddmmyyyy.txt
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


def gerar_clientes_sefaz():
    """
    Essa função gera arquivos txt:
    » ClientesSEFAZddmmyyyy.txt
    @return:
    """
    'dh = datetime.now()'
    'dd = dh.strftime("%d")'
    'dm = dh.strftime("%m")'
    'da = dh.strftime("%Y")'
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
                FROM implantacao.mad_pedvenda a
                INNER JOIN implantacao.ge_pessoa b 
                ON b.seqpessoa = a.seqpessoa	
                AND b.inscricaorg != 'ISENTO' 
                AND b.fisicajuridica = 'J'
                AND b.atividade NOT IN ('FORNECEDOR')
                AND b.seqpessoa NOT IN (1, 22401)
                WHERE 1=1
                AND a.nroempresa = 1
                AND a.situacaoped IN ('A', 'L', 'R', 'S')
                AND a.indentregaretira = 'E'
                ORDER BY 1 ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        "columns = [row[0] for row in cur.description]"
        "data = cur.fetchall()"
        "dfdata = pd.DataFrame.from_records(data, columns=columns)"
        """
        heard = f'{columns[0]};SITUACAO'

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
                    record = f'{str(pessoa)};{str(situacao_sefaz(cnpj, uf, ie))}'
                    write_file.write(f'{record}\n')
            else:
                for index, reg in dfdata.iterrows():
                    pessoa = reg.SEQPESSOA
                    cnpj = reg.NROCGCCPF
                    uf = reg.UF
                    ie = reg.INSCRICAORG
                    record = f'{str(pessoa)};{str(situacao_sefaz(cnpj, uf, ie))}'
                    record_search = record[:7]
                    if not ler_arquivo_clientessefaz(record_search):
                        write_file.write(f'{record}\n')
        """
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


def analise_log_painel02(request):
    dh = datetime.now()
    dm = dh.strftime("%m")
    da = dh.strftime("%Y")

    pd.set_option('display.precision', 2)

    dfdata = pd.read_csv(f'IntervalosPedidos{dm + da}.txt', sep=';')
    dfdata['DATAHORA'] = pd.to_datetime(dfdata['DATAHORA'], errors='coerce').dt.strftime('%Y-%m-%d')
    dfdata_entrega = (dfdata['DATAHORA'] == dh.strftime('%Y-%m-%d')) & (dfdata['INDENTREGARETIRA'] == 'E')
    dfdata_retira = (dfdata['DATAHORA'] == dh.strftime('%Y-%m-%d')) & (dfdata['INDENTREGARETIRA'] == 'R')

    # PEDIDOS ENTREGA
    df_dataentreda = dfdata.loc[dfdata_entrega, ['NROPEDVENDA', 'INTERVALO', 'PESOBRUTO']]

    li_pedidosentregaquantidade = list()
    pedidosentregaquantidade = df_dataentreda.groupby('INTERVALO')['NROPEDVENDA'].count().reset_index()
    li_pedidosentregaquantidade.append(pedidosentregaquantidade.columns.tolist())
    for lista in pedidosentregaquantidade.values:
        li_pedidosentregaquantidade.append(lista.tolist())

    li_pedidosentregapesobruto = list()
    pedidosentregapesobruto = df_dataentreda.groupby('INTERVALO')['PESOBRUTO'].sum().reset_index()
    li_pedidosentregapesobruto.append(pedidosentregapesobruto.columns.tolist())
    for lista in pedidosentregapesobruto.values:
        li_pedidosentregapesobruto.append(lista.tolist())

    # PEDIDOS RETIRA
    df_dataretira = dfdata.loc[dfdata_retira, ['NROPEDVENDA', 'INTERVALO', 'PESOBRUTO']]

    li_pedidosretiraquantidade = list()
    pedidosretiraquantidade = df_dataretira.groupby('INTERVALO')['NROPEDVENDA'].count().reset_index()
    li_pedidosretiraquantidade.append(pedidosretiraquantidade.columns.tolist())
    for lista in pedidosretiraquantidade.values:
        li_pedidosretiraquantidade.append(lista.tolist())

    li_pedidosretirapesobruto = list()
    pedidosretirapesobruto = df_dataretira.groupby('INTERVALO')['PESOBRUTO'].sum().reset_index()
    li_pedidosretirapesobruto.append(pedidosretirapesobruto.columns.tolist())
    for lista in pedidosretirapesobruto.values:
        li_pedidosretirapesobruto.append(lista.tolist())

    data = {
        'jsonpedidosentregaquantidade': li_pedidosentregaquantidade,
        'jsonpedidosretiraquantidade': li_pedidosretiraquantidade,
        'jsonpedidosentregapesobruto': li_pedidosentregapesobruto,
        'jsonpedidosretirapesobruto': li_pedidosretirapesobruto
    }

    return JsonResponse(data)


# FUNÇÕES REFERENTE A EXECUÇÃO DO PAINEL03 » LOGÍSTICA #
########################################################################################################################
def painellog03_dashboard(request):
    return render(request, 'bilogistica/painel03.html')


def analise_log_painel03(request):
    try:
        dh = datetime.now()
        dbf = dh - timedelta(days=9)
        dh = dh.strftime('%Y-%m-%d')
        dbf = dbf.strftime('%Y-%m-%d')
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()

        query = \
            """
            SELECT
                nrosegmento,
                INITCAP(descsegmento)
                AS descsegmento
            FROM implantacao.mad_segmento
            WHERE 1=1
            AND status = 'A'
            """
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_segmentos = pd.DataFrame.from_records(data, columns=colums)
        pd.set_option('display.precision', 2)

        query = \
            """
            SELECT
                DECODE(
                    a.situacaoped, 
                    'L', 'LIBERADO',
                    'R', 'LIBERADO',
                    'S', 'SEPARACAO'
                )
                AS situacaoped,
                a.nrosegmento,
                a.indentregaretira,
                TO_DATE(a.dtainclusao) 
                AS dtainclusao,	
                TO_DATE(a.dtabasefaturamento) 
                AS dtabasefaturamento,	
                TO_DATE(a.dtageracaocarga) 
                AS dtageracaocarga,
                SUM(NVL((d.pesobruto / b.qtdembalagem) * b.qtdatendida, 0))
                AS pesobruto
            FROM implantacao.mad_pedvenda a
            INNER JOIN implantacao.mad_pedvendaitem b 
            ON b.nroempresa=a.nroempresa AND b.nropedvenda=a.nropedvenda AND b.qtdatendida > 0
            INNER JOIN implantacao.map_produto c ON c.seqproduto = b.seqproduto
            INNER JOIN implantacao.map_famembalagem d ON d.seqfamilia = c.seqfamilia AND d.qtdembalagem = b.qtdembalagem
            WHERE 1=1
            AND a.nroempresa = 1
            AND a.situacaoped IN ('L', 'R', 'S')
            GROUP BY a.situacaoped,a.nrosegmento,a.indentregaretira,a.dtainclusao,a.dtabasefaturamento,a.dtageracaocarga
            """

        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_principal = pd.DataFrame.from_records(data, columns=colums)
        df_principal = df_principal.merge(df_segmentos, left_on='NROSEGMENTO', right_on='NROSEGMENTO', how='outer')
        df_principal.columns = [
            'Situacao',
            'CodSegmento',
            'IndEntregaRetira',
            'DataInclusao',
            'DataBaseFaturamento',
            'DataGeracaoCarga',
            'PesoBruto',
            'DesSegmento'
        ]

        pd.set_option('display.max_columns', None)
        pd.options.display.float_format = '{:.2f}'.format

        # APURAÇÃO DOS PEDIDOS LIBERADOS
        pesobrutolibretira = df_principal[(df_principal['Situacao'] == 'LIBERADO') &
                                          (df_principal['IndEntregaRetira'] == 'R') &
                                          ((df_principal['DataBaseFaturamento'] >= dbf) &
                                          (df_principal['DataBaseFaturamento'] <= dh))]['PesoBruto'].sum()

        pesobrutolibentrega = df_principal[(df_principal['Situacao'] == 'LIBERADO') &
                                           (df_principal['IndEntregaRetira'] == 'E') &
                                           ((df_principal['DataBaseFaturamento'] >= dbf) &
                                           (df_principal['DataBaseFaturamento'] <= dh))]['PesoBruto'].sum()

        totalpesobrutoliberado = pesobrutolibretira + pesobrutolibentrega

        df_pesobrutoliberado = df_principal[(df_principal['Situacao'] == 'LIBERADO') &
                                            ((df_principal['DataBaseFaturamento'] >= dbf) &
                                            (df_principal['DataBaseFaturamento'] <= dh))].groupby('DesSegmento')['PesoBruto'].sum().reset_index().copy()
        df_pesobrutoliberado.fillna(value=0, inplace=True)

        li_pesobrutoliberado = list()
        li_pesobrutoliberado.append(df_pesobrutoliberado.columns.tolist())
        for lista in df_pesobrutoliberado.values:
            li_pesobrutoliberado.append(lista.tolist())

        # APURAÇÃO DOS PEDIDOS SEPARAÇÃO
        pesobrutosepretira = df_principal[(df_principal['Situacao'] == 'SEPARACAO') &
                                          (df_principal['IndEntregaRetira'] == 'R') &
                                          (df_principal['DataGeracaoCarga'] == dh)]['PesoBruto'].sum()

        pesobrutosepentrega = df_principal[(df_principal['Situacao'] == 'SEPARACAO') &
                                           (df_principal['IndEntregaRetira'] == 'E') &
                                           (df_principal['DataGeracaoCarga'] == dh)]['PesoBruto'].sum()

        totalpesobrutoseparacao = pesobrutosepretira + pesobrutosepentrega

        df_pesobrutoseperacao = df_principal[(df_principal['Situacao'] == 'SEPARACAO') &
                                             (df_principal['DataGeracaoCarga'] == dh)].groupby('DesSegmento')['PesoBruto'].sum().reset_index().copy()
        df_pesobrutoseperacao.fillna(value=0, inplace=True)

        li_pesobrutoseparacao = list()
        li_pesobrutoseparacao.append(df_pesobrutoseperacao.columns.tolist())
        for lista in df_pesobrutoseperacao.values:
            li_pesobrutoseparacao.append(lista.tolist())

        data = {
            'pesobrutolibretira': pesobrutolibretira,
            'pesobrutolibentrega': pesobrutolibentrega,
            'pesobrutosepretira': pesobrutosepretira,
            'pesobrutosepentrega': pesobrutosepentrega,
            'totalpesobrutoliberado': totalpesobrutoliberado,
            'totalpesobrutoseparacao': totalpesobrutoseparacao,
            'jsonpesobrutoliberado': li_pesobrutoliberado,
            'jsonpesobrutoseparacao': li_pesobrutoseparacao,
        }
        return JsonResponse(data)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


# FUNÇÕES REFERENTE A EXECUÇÃO DO PAINEL01 » DIRETÓRIO #
########################################################################################################################
def analise_dir_painel01(request):
    try:
        query = """
                SELECT
                    a.nroempresa,
                    TO_DATE(a.dtavda)
                    AS dtavda,
                    a.codgeraloper,
                    ROUND(
                    SUM(
                    implantacao.fC5_AbcDistribLucratividade(
                    'L',
                    'L',
                    'N',
                    ROUND(a.vlritem,2),
                    'S',
                    a.vlricmsst,
                    a.vlrfcpst,
                    a.vlricmsstemporig,
                    g.uf,
                    a.ufpessoa,
                    'S',
                    j.vlrdescregra,
                    'N',
                    a.vlripiitem,
                    a.vlripidevolitem,
                    'N',
                    a.vlrdescforanf,
                    b.cmdiavlrnf-0,
                    b.cmdiaipi,
                    NVL(b.cmdiacredpis,0),
                    NVL(b.cmdiacredcofins,0),
                    b.cmdiaicmsst,
                    b.cmdiadespnf,
                    b.cmdiadespforanf,
                    b.cmdiadctoforanf,
                    'S',
                    c.propqtdprodutobase,
                    a.qtditem,
                    a.vlrembdescressarcst,
                    a.acmcompravenda,
                    a.pisitem,
                    a.cofinsitem,
                    DECODE(a.tipcgo,'S',b.qtdvda,NVL(b.qtddevol,b.qtdvda)),
                    (DECODE(a.tipcgo,'S',b.vlrimpostovda - NVL(b.vlripivda,0),NVL(((b.vlrimpostodevol / DECODE(NVL(b.qtddevol,0),0,1,b.qtddevol)) * a.qtddevolitem) - NVL(a.vlripidevolitem,0), 
                    b.vlrimpostovda - NVL( b.vlripivda,0)))),
                    'N',
                    a.vlrdespoperacionalitem,
                    b.vlrdespesavda,
                    'N',
                    NVL(b.vlrverbavdaacr,0),
                    b.qtdverbavda,
                    b.vlrverbavda - NVL(b.vlrverbavdaindevida,0),
                    'N',
                    NVL(a.vlrtotcomissaoitem,0),
                    a.vlrdevolitem,
                    a.vlrdevolicmsst,
                    a.dvlrfcpst,
                    a.qtddevolitem,
                    a.pisdevolitem,
                    a.cofinsdevolitem,
                    a.vlrdespoperacionalitemdevol,
                    a.vlrtotcomissaoitemdevol,
                    g.perirlucrat,
                    g.percslllucrat,
                    b.cmdiacredicms,
                    DECODE(a.icmsefetivoitem,0,a.ICMSITEM,a.icmsefetivoitem),
                    a.vlrfcpicms,
                    a.percpmf,
                    a.peroutroimposto,
                    DECODE(a.icmsefetivodevolitem,0,a.icmsdevolitem,a.icmsefetivodevolitem),
                    a.Dvlrfcpicms, 
                    CASE 
                    WHEN ('S') = 'N' 
                    THEN (NVL(b.cmdiavlrdescpistransf,0) 
                         +NVL(b.cmdiavlrdesccofinstransf,0)
                             +NVL(b.cmdiavlrdescicmstransf,0)
                             +NVL(b.cmdiavlrdescipitransf,0)
                             +NVL(b.cmdiavlrdesclucrotransf,0)
                             +NVL(b.cmdiavlrdescverbatransf,0))
                    ELSE 0
                    END, 
                    CASE 
                    WHEN h.utilacresccustprodrelac = 'S' and NVL(c.seqprodutobase,c.seqprodutobaseantigo) IS NOT NULL
                    THEN COALESCE(i.percacresccustorelacvig,NVL(implantacao.f_retacresccustorelacabc(a.seqproduto,a.dtavda),1))
                    ELSE 1 
                    END,
                    'N',
                    0,
                    0,
                    'S',
                    a.vlrdescmedalha,
                    'S',
                    a.vlrdescfornec,
                    a.vlrdescfornecdevol,
                    'N',
                    a.vlrfreteitemrateio,
                    a.vlrfreteitemrateiodev,
                    'S',
                    a.vlricmsstembutprod,
                    a.vlricmsstembutproddev,
                    a.vlrembdescressarcstdevol,
                    CASE 
                    WHEN 'N' = 'S' 
                    THEN NVL(a.vlrdescacordoverbapdv,0)
                    ELSE 0 
                    END,
                    NVL(b.cmdiacredipi,0),NVL(a.vlritemrateiocte,0),'N','C')) * 100 / 100, 2)
                    AS lucratividade, 
                    ROUND(SUM(a.vlritem - a.vlricmsst - a.vlrfcpst - a.vlrdevolitem + a.vlrdevolicmsst + a.dvlrfcpst), 2)
                    AS vendas,
                    ROUND(SUM((a.qtditem - a.qtddevolitem) / f.qtdembalagem * f.pesobruto), 2) 
                    AS pesobruto
                FROM implantacao.maxv_abcdistribbase a, 
                         implantacao.mrl_custodiafam b, 
                         implantacao.map_produto c, 
                         implantacao.map_produto d, 
                         implantacao.map_famdivisao e, 
                         implantacao.map_famembalagem f, 
                         implantacao.max_empresa g, 
                         implantacao.max_divisao h, 
                         implantacao.map_prodacresccustorelac i, 
                         implantacao.mrlv_descontoregra j 
                WHERE 1 = 1 
                AND a.seqproduto = c.seqproduto
                AND a.seqprodutocusto = d.seqproduto
                AND a.nrodivisao = e.nrodivisao
                AND a.seqproduto = i.seqproduto(+)
                AND a.dtavda = i.dtamovimentacao(+)
                AND a.dtavda = j.datafaturamento (+)
                AND a.nrodocto = j.numerodf (+)
                AND a.seriedocto = j.seriedf (+) 
                AND a.nroempresa = j.nroempresa (+) 	 
                AND DECODE(a.tiptabela,'S',a.cgoacmcompravenda,a.acmcompravenda) IN ('S', 'I')
                AND a.nrosegmento IN (1, 3, 4, 5, 6, 7, 8, 9, 10)
                AND a.codgeraloper IN (173, 201, 235, 307, 314, 401, 598)
                AND a.seqpessoa NOT IN (1, 22401)
                AND b.nroempresa = NVL(g.nroempcustoabc, g.nroempresa) 
                AND b.dtaentradasaida = a.dtavda
                AND b.seqfamilia = d.seqfamilia
                AND e.seqfamilia = c.seqfamilia
                AND e.nrodivisao = a.nrodivisao
                AND e.seqcomprador NOT IN (8, 11)		
                AND f.seqfamilia = c.seqfamilia AND f.qtdembalagem = 1 AND a.seqproduto = j.seqproduto (+) 
                AND g.nroempresa = a.nroempresa
                AND g.nrodivisao = h.nrodivisao
                AND a.dtavda BETWEEN TRUNC(ADD_MONTHS(SYSDATE, 0),'MM') AND TRUNC(SYSDATE)
                GROUP BY a.nroempresa, a.dtavda, a.codgeraloper
                ORDER BY a.dtavda ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata1 = pd.DataFrame.from_records(data, columns=colums)

        query = """
                SELECT
                    e.nroempresa,
                    round(sum(c.estqdeposito / k.qtdembalagem), 3) 
                    as estoquegeralqtd,
                    round(sum(((c.qtdreservadavda + c.qtdreservadareceb)) / k.qtdembalagem), 6) 
                    as estoquereservadoqtd,
                    round(sum(((estqdeposito) - (c.qtdreservadavda + c.qtdreservadareceb)) / k.qtdembalagem), 6) 
                    as estoquedisponivelqtd,
                    sum(nvl(c.preco,0) * ((c.estqdeposito))) 
                    as estoquegeralvlr
                FROM implantacao.map_produto a, implantacao.map_familia b,
                ( select y.seqproduto, y.nroempresa, y.seqcluster ,
                decode( ( estqloja + estqdeposito + estqalmoxarifado + nvl( estqterceiro, 0 ) ), 0, null, y.seqproduto ) seqprodutocomestq,
                decode( x.preco, 0, x.menorpreco, x.preco ) preco, x.menorpreco, x.maiorpreco, y.nrogondola, y.estqloja, y.estqdeposito, y.estqtroca, y.estqalmoxarifado, y.estqoutro, nvl( y.estqterceiro, 0 ) estqterceiro,
                y.qtdpendpedcompra, y.qtdpendpedexped, 
                y.qtdreservadavda, y.qtdreservadareceb, y.qtdreservadafixa, 
                y.medvdiapromoc, y.medvdiageral, y.medvdiaforapromoc, 
                y.cmultvlrnf, 
                y.cmultipi, 
                y.cmultcredicms, 
                y.cmulticmsst, 
                y.cmultdespnf, 
                y.cmultdespforanf, 
                y.cmultdctoforanf, 
                nvl( y.cmultimpostopresum, 0 ) cmultimpostopresum,
                nvl( y.cmultcredicmspresum, 0 ) cmultcredicmspresum,
                nvl( y.cmultcredicmsantecip, 0 ) cmultcredicmsantecip,
                nvl( y.cmultcusliquidoemp, 0 ) cmultcusliquidoemp,
                nvl( y.cmultcredicmsemp, 0 ) cmultcredicmsemp,
                nvl( y.cmultcredpisemp, 0 ) cmultcredpisemp,
                nvl( y.cmultcredcofinsemp, 0 ) cmultcredcofinsemp,
                nvl( y.cmultcredpis, 0 ) cmultcredpis,
                nvl( y.cmultcredcofins, 0 ) cmultcredcofins,
                y.statuscompra, x.statusvenda,
                trunc( sysdate ) - y.dtaultentrada diasultentrada, 
                nvl( y.nrosegproduto, e.nrosegmentoprinc ) nrosegproduto, 
                y.locentrada, y.locsaida,
                nvl( y.classeabastqtd, '**sem classificação**' ) classeabastqtd, 
                nvl( y.classeabastvlr, '**sem classificação**' ) classeabastvlr,
                nvl( y.cmultvlrcompror, 0 ) cmultvlrcompror,
                nvl( y.cmultvlrdescpistransf, 0 ) cmultvlrdescpistransf,
                nvl( y.cmultvlrdesccofinstransf, 0 ) cmultvlrdesccofinstransf,
                nvl( y.cmultvlrdescicmstransf, 0 ) cmultvlrdescicmstransf, 
                nvl( y.cmultvlrdesclucrotransf, 0 ) cmultvlrdesclucrotransf,
                nvl( y.cmultvlrdescipitransf, 0 ) cmultvlrdescipitransf,
                nvl( y.cmultvlrdescverbatransf, 0 ) cmultvlrdescverbatransf,
                nvl( y.cmultvlrdescdiferencatransf, 0 ) cmultvlrdescdiferencatransf,
                nvl( y.cmultcredipi, 0 ) cmultcredipi, 
                trunc( sysdate ) - y.dtaultentrcusto diasultentrcusto,
                ( nvl( y.cmultvlrdescpistransf, 0 ) + nvl( y.cmultvlrdesccofinstransf, 0 ) + nvl( y.cmultvlrdescicmstransf, 0 ) + nvl( y.cmultvlrdescipitransf, 0 )
                + nvl( y.cmultvlrdesclucrotransf, 0 ) + nvl( y.cmultvlrdescverbatransf, 0 ) + nvl( y.cmultvlrdescdiferencatransf, 0 ) ) vlrdesctransfcb,
                y.seqsensibilidade, 
                y.formaabastecimento, case when nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) < 0 
                then 0 
                else nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 )
                end custofiscalunit,
                case when nvl( ( nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) ) * y.estqempresa, 0 ) < 0 
                then 0
                else nvl( ( nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) ) * y.estqempresa, 0 ) 
                end custofiscaltotal,
                coalesce(
                (select ( custoa.qtdestqinicialemp + custoa.qtdentradaemp - custoa.qtdsaidaemp)
                from implantacao.mrl_custodiaemp custoa
                where custoa.seqproduto = y.seqproduto
                and custoa.nroempresa = y.nroempresa
                and custoa.dtaentradasaida = nvl( '', sysdate ) ),
                ( select ( custoa.qtdestqinicialemp + custoa.qtdentradaemp - custoa.qtdsaidaemp)
                from implantacao.mrl_custodiaemp custoa
                where custoa.seqproduto = y.seqproduto
                and custoa.nroempresa = y.nroempresa
                and custoa.dtaentradasaida = ( select max( custob.dtaentradasaida )
                from implantacao.mrl_custodiaemp custob
                where custob.seqproduto = custoa.seqproduto
                and custob.nroempresa = custoa.nroempresa
                and custob.dtaentradasaida <= nvl( '', sysdate ) ) ), 0) estqfiscalempresa,
                nvl( y.estqempresa, 0 ) estqempresa,
                sysdate dtaentradasaida, 
                nvl( y.cmultvlrdespfixa, 0 ) cmultvlrdespfixa,
                nvl( y.cmultvlrdescfixo, 0 ) cmultvlrdescfixo,
                nvl( y.cmultvlrdescresticmstransf, 0 ) cmultvlrdescresticmstransf,
                nvl( y.cmultverbacompra, 0 ) cmultverbacompra,
                nvl( y.cmultverbabonifincid, 0 ) cmultverbabonifincid,
                nvl( y.cmultverbabonifsemincid, 0 ) cmultverbabonifsemincid,
                nvl( y.cmultvlrdescverbatransfsellin, 0 ) cmultvlrdescverbatransfsellin,
                nvl( y.centrultvlrnf, 0 ) centrultvlrnf,
                nvl( y.centrultipi, 0 ) centrultipi,
                nvl( y.centrulticmsst, 0 ) centrulticmsst,
                nvl( y.centrultdespnf, 0 ) centrultdespnf,
                nvl( y.centrultdespforanf, 0 ) centrultdespforanf,
                nvl( y.centrultdctoforanf, 0 ) centrultdctoforanf,
                nvl( y.centrultcredicms, 0 ) centrultcredicms,
                nvl( y.centrultcredipi, 0 ) centrultcredipi,
                nvl( y.centrultcredpis, 0 ) centrultcredpis,
                nvl( y.centrultcredcofins, 0 ) centrultcredcofins,
                nvl( y.qentrultcusto, 0 ) qentrultcusto,
                y.indposicaocateg,
                nvl( y.cmultdctoforanfemp, 0 ) cmultdctoforanfemp,
                nvl( y.estqminimoloja, 0 ) qtdestoqueminimo, 
                nvl( y.estqmaximoloja, 0 ) qtdestoquemaximo,
                y.dtaultvenda dtaultvenda
                from ( select seqproduto, nroempresa, max( qtdembalagem ) qtdembalagemseg, 
                max( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) preco, 
                min( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) menorpreco,
                max( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) maiorpreco,
                decode(min(statusvenda), 'A', min(statusvenda), 'I') statusvenda
                from mrl_prodempseg
                where 1 = 1
                and nrosegmento in (1,6,5,4,9,7,10,8,3) group by seqproduto, nroempresa) x,
                mrl_produtoempresa y, max_empresa e 
                where e.nroempresa = y.nroempresa 
                and y.nroempresa = x.nroempresa and y.seqproduto = x.seqproduto
                and y.seqproduto in (select ff.seqproduto
                from map_produto ff
                where ff.seqfamilia not in (select seqfamilia
                from map_famdivisao
                where nrodivisao = '1'
                and seqcomprador in (8, 11)))) c, 
                implantacao.map_famdivisao d, implantacao.map_famembalagem k, implantacao.max_empresa e, implantacao.mad_parametro j3, 
                implantacao.max_divisao i2, implantacao.map_classifabc z2, implantacao.mad_famsegmento h, implantacao.map_regimetributacao rt, 
                implantacao.map_tributacaouf t3 , implantacao.mapv_piscofinstribut ss, implantacao.mad_segmento se, implantacao.map_prodacresccustorelac pr, 
                implantacao.map_famdivcateg fdc, implantacao.map_categoria cat, 
                (select a.seqfamilia, min(a.padraoembvenda) padraoembvenda 
                from implantacao.mad_segmento b,
                implantacao.mad_famsegmento a
                where a.status = 'A'
                and a.nrosegmento = b.nrosegmento
                and b.nrosegmento in (1,6,5,4,9,7,10,8,3)
                and b.nrodivisao = 1
                group by a.seqfamilia) sf 
                where a.seqproduto = c.seqproduto
                and b.seqfamilia = a.seqfamilia
                and d.seqfamilia = a.seqfamilia 
                and d.nrodivisao = e.nrodivisao
                and k.seqfamilia = d.seqfamilia 
                and k.qtdembalagem = nvl(sf.padraoembvenda, h.padraoembvenda)
                and e.nroempresa = c.nroempresa
                and j3.nroempresa = e.nroempresa 
                and i2.nrodivisao = e.nrodivisao 
                and i2.nrodivisao = d.nrodivisao
                and z2.nrosegmento = h.nrosegmento 
                and z2.classifcomercabc = h.classifcomercabc 
                and z2.nrosegmento = se.nrosegmento 
                and t3.nrotributacao = d.nrotributacao 
                and t3.ufempresa = nvl(e.ufformacaopreco, e.uf) 
                and t3.ufclientefornec = e.uf
                and t3.tiptributacao = decode(i2.tipdivisao, 'V', 'SN', 'SC') 
                and t3.nroregtributacao = nvl(e.nroregtributacao, 0) 
                and a.seqfamilia = fdc.seqfamilia
                and cat.nrodivisao = e.nrodivisao
                and fdc.seqcategoria = cat.seqcategoria
                and fdc.nrodivisao = cat.nrodivisao
                and b.seqfamilia = a.seqfamilia
                and cat.nivelhierarquia = 1
                and cat.statuscategor in ('A','F')
                and fdc.status = 'A'
                and cat.tipcategoria = 'M' 
                and c.seqproduto = pr.seqproduto(+) 
                and c.dtaentradasaida = pr.dtamovimentacao(+) 
                and ss.nroempresa = e.nroempresa
                and ss.nrotributacao = t3.nrotributacao 
                and ss.ufempresa = t3.ufempresa
                and ss.ufclientefornec = t3.ufclientefornec
                and ss.tiptributacao = t3.tiptributacao
                and ss.nroregtributacao = t3.nroregtributacao 
                and ss.seqfamilia = b.seqfamilia 
                and a.seqfamilia = sf.seqfamilia (+) 
                and h.seqfamilia = a.seqfamilia 
                and h.nrosegmento = e.nrosegmentoprinc 
                and t3.nroregtributacao = rt.nroregtributacao
                and c.statuscompra in ('A')
                and implantacao.fstatusvendaproduto(c.seqproduto, c.nroempresa, se.nrosegmento) = 'A'
                and d.seqcomprador not in (8, 11)
                and a.seqprodutobase is null 			 
                group by e.nroempresa
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata2 = pd.DataFrame.from_records(data, columns=colums)

        query = """
                SELECT
                    e.nroempresa,
                    round(sum(c.estqdeposito / k.qtdembalagem), 3) 
                    as estoquegeralqtd,
                    round(sum(((c.qtdreservadavda + c.qtdreservadareceb)) / k.qtdembalagem), 6) 
                    as estoquereservadoqtd,
                    round(sum(((estqdeposito) - (c.qtdreservadavda + c.qtdreservadareceb)) / k.qtdembalagem), 6) 
                    as estoquedisponivelqtd,
                    sum(nvl(c.preco,0) * ((c.estqdeposito))) 
                    as estoquegeralvlr
                FROM implantacao.map_produto a, implantacao.map_familia b,
                ( select y.seqproduto, y.nroempresa, y.seqcluster ,
                decode( ( estqloja + estqdeposito + estqalmoxarifado + nvl( estqterceiro, 0 ) ), 0, null, y.seqproduto ) seqprodutocomestq,
                decode( x.preco, 0, x.menorpreco, x.preco ) preco, x.menorpreco, x.maiorpreco, y.nrogondola, y.estqloja, y.estqdeposito, y.estqtroca, y.estqalmoxarifado, y.estqoutro, nvl( y.estqterceiro, 0 ) estqterceiro,
                y.qtdpendpedcompra, y.qtdpendpedexped, 
                y.qtdreservadavda, y.qtdreservadareceb, y.qtdreservadafixa, 
                y.medvdiapromoc, y.medvdiageral, y.medvdiaforapromoc, 
                y.cmultvlrnf, 
                y.cmultipi, 
                y.cmultcredicms, 
                y.cmulticmsst, 
                y.cmultdespnf, 
                y.cmultdespforanf, 
                y.cmultdctoforanf, 
                nvl( y.cmultimpostopresum, 0 ) cmultimpostopresum,
                nvl( y.cmultcredicmspresum, 0 ) cmultcredicmspresum,
                nvl( y.cmultcredicmsantecip, 0 ) cmultcredicmsantecip,
                nvl( y.cmultcusliquidoemp, 0 ) cmultcusliquidoemp,
                nvl( y.cmultcredicmsemp, 0 ) cmultcredicmsemp,
                nvl( y.cmultcredpisemp, 0 ) cmultcredpisemp,
                nvl( y.cmultcredcofinsemp, 0 ) cmultcredcofinsemp,
                nvl( y.cmultcredpis, 0 ) cmultcredpis,
                nvl( y.cmultcredcofins, 0 ) cmultcredcofins,
                y.statuscompra, x.statusvenda,
                trunc( sysdate ) - y.dtaultentrada diasultentrada, 
                nvl( y.nrosegproduto, e.nrosegmentoprinc ) nrosegproduto, 
                y.locentrada, y.locsaida,
                nvl( y.classeabastqtd, '**sem classificação**' ) classeabastqtd, 
                nvl( y.classeabastvlr, '**sem classificação**' ) classeabastvlr,
                nvl( y.cmultvlrcompror, 0 ) cmultvlrcompror,
                nvl( y.cmultvlrdescpistransf, 0 ) cmultvlrdescpistransf,
                nvl( y.cmultvlrdesccofinstransf, 0 ) cmultvlrdesccofinstransf,
                nvl( y.cmultvlrdescicmstransf, 0 ) cmultvlrdescicmstransf, 
                nvl( y.cmultvlrdesclucrotransf, 0 ) cmultvlrdesclucrotransf,
                nvl( y.cmultvlrdescipitransf, 0 ) cmultvlrdescipitransf,
                nvl( y.cmultvlrdescverbatransf, 0 ) cmultvlrdescverbatransf,
                nvl( y.cmultvlrdescdiferencatransf, 0 ) cmultvlrdescdiferencatransf,
                nvl( y.cmultcredipi, 0 ) cmultcredipi, 
                trunc( sysdate ) - y.dtaultentrcusto diasultentrcusto,
                ( nvl( y.cmultvlrdescpistransf, 0 ) + nvl( y.cmultvlrdesccofinstransf, 0 ) + nvl( y.cmultvlrdescicmstransf, 0 ) + nvl( y.cmultvlrdescipitransf, 0 )
                + nvl( y.cmultvlrdesclucrotransf, 0 ) + nvl( y.cmultvlrdescverbatransf, 0 ) + nvl( y.cmultvlrdescdiferencatransf, 0 ) ) vlrdesctransfcb,
                y.seqsensibilidade, 
                y.formaabastecimento, case when nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) < 0 
                then 0 
                else nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 )
                end custofiscalunit,
                case when nvl( ( nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) ) * y.estqempresa, 0 ) < 0 
                then 0
                else nvl( ( nvl( y.cmultcusliquidoemp, 0 ) - nvl( y.cmultdctoforanfemp, 0 ) ) * y.estqempresa, 0 ) 
                end custofiscaltotal,
                coalesce(
                (select ( custoa.qtdestqinicialemp + custoa.qtdentradaemp - custoa.qtdsaidaemp)
                from implantacao.mrl_custodiaemp custoa
                where custoa.seqproduto = y.seqproduto
                and custoa.nroempresa = y.nroempresa
                and custoa.dtaentradasaida = nvl( '', sysdate ) ),
                ( select ( custoa.qtdestqinicialemp + custoa.qtdentradaemp - custoa.qtdsaidaemp)
                from implantacao.mrl_custodiaemp custoa
                where custoa.seqproduto = y.seqproduto
                and custoa.nroempresa = y.nroempresa
                and custoa.dtaentradasaida = ( select max( custob.dtaentradasaida )
                from implantacao.mrl_custodiaemp custob
                where custob.seqproduto = custoa.seqproduto
                and custob.nroempresa = custoa.nroempresa
                and custob.dtaentradasaida <= nvl( '', sysdate ) ) ), 0) estqfiscalempresa,
                nvl( y.estqempresa, 0 ) estqempresa,
                sysdate dtaentradasaida, 
                nvl( y.cmultvlrdespfixa, 0 ) cmultvlrdespfixa,
                nvl( y.cmultvlrdescfixo, 0 ) cmultvlrdescfixo,
                nvl( y.cmultvlrdescresticmstransf, 0 ) cmultvlrdescresticmstransf,
                nvl( y.cmultverbacompra, 0 ) cmultverbacompra,
                nvl( y.cmultverbabonifincid, 0 ) cmultverbabonifincid,
                nvl( y.cmultverbabonifsemincid, 0 ) cmultverbabonifsemincid,
                nvl( y.cmultvlrdescverbatransfsellin, 0 ) cmultvlrdescverbatransfsellin,
                nvl( y.centrultvlrnf, 0 ) centrultvlrnf,
                nvl( y.centrultipi, 0 ) centrultipi,
                nvl( y.centrulticmsst, 0 ) centrulticmsst,
                nvl( y.centrultdespnf, 0 ) centrultdespnf,
                nvl( y.centrultdespforanf, 0 ) centrultdespforanf,
                nvl( y.centrultdctoforanf, 0 ) centrultdctoforanf,
                nvl( y.centrultcredicms, 0 ) centrultcredicms,
                nvl( y.centrultcredipi, 0 ) centrultcredipi,
                nvl( y.centrultcredpis, 0 ) centrultcredpis,
                nvl( y.centrultcredcofins, 0 ) centrultcredcofins,
                nvl( y.qentrultcusto, 0 ) qentrultcusto,
                y.indposicaocateg,
                nvl( y.cmultdctoforanfemp, 0 ) cmultdctoforanfemp,
                nvl( y.estqminimoloja, 0 ) qtdestoqueminimo, 
                nvl( y.estqmaximoloja, 0 ) qtdestoquemaximo,
                y.dtaultvenda dtaultvenda
                from ( select seqproduto, nroempresa, max( qtdembalagem ) qtdembalagemseg, 
                max( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) preco, 
                min( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) menorpreco,
                max( case when statusvenda = 'I' or decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) = 0 then null
                else ( decode( precovalidpromoc, 0, precovalidnormal, precovalidpromoc ) / qtdembalagem )
                end ) maiorpreco,
                decode(min(statusvenda), 'A', min(statusvenda), 'I') statusvenda
                from mrl_prodempseg
                where 1 = 1
                and nrosegmento in (1,6,5,4,9,7,10,8,3) group by seqproduto, nroempresa) x,
                mrl_produtoempresa y, max_empresa e 
                where e.nroempresa = y.nroempresa 
                and y.nroempresa = x.nroempresa and y.seqproduto = x.seqproduto
                and y.seqproduto in (select ff.seqproduto
                from map_produto ff
                where ff.seqfamilia not in (select seqfamilia
                from map_famdivisao
                where nrodivisao = '1'
                and seqcomprador in (8, 11)))) c, 
                implantacao.map_famdivisao d, implantacao.map_famembalagem k, implantacao.max_empresa e, implantacao.mad_parametro j3, 
                implantacao.max_divisao i2, implantacao.map_classifabc z2, implantacao.mad_famsegmento h, implantacao.map_regimetributacao rt, 
                implantacao.map_tributacaouf t3 , implantacao.mapv_piscofinstribut ss, implantacao.mad_segmento se, implantacao.map_prodacresccustorelac pr, 
                implantacao.map_famdivcateg fdc, implantacao.map_categoria cat, 
                (select a.seqfamilia, min(a.padraoembvenda) padraoembvenda 
                from implantacao.mad_segmento b,
                implantacao.mad_famsegmento a
                where a.status = 'A'
                and a.nrosegmento = b.nrosegmento
                and b.nrosegmento in (1,6,5,4,9,7,10,8,3)
                and b.nrodivisao = 1
                group by a.seqfamilia) sf 
                where a.seqproduto = c.seqproduto
                and b.seqfamilia = a.seqfamilia
                and d.seqfamilia = a.seqfamilia 
                and d.nrodivisao = e.nrodivisao
                and k.seqfamilia = d.seqfamilia 
                and k.qtdembalagem = nvl(sf.padraoembvenda, h.padraoembvenda)
                and e.nroempresa = c.nroempresa
                and j3.nroempresa = e.nroempresa 
                and i2.nrodivisao = e.nrodivisao 
                and i2.nrodivisao = d.nrodivisao
                and z2.nrosegmento = h.nrosegmento 
                and z2.classifcomercabc = h.classifcomercabc 
                and z2.nrosegmento = se.nrosegmento 
                and t3.nrotributacao = d.nrotributacao 
                and t3.ufempresa = nvl(e.ufformacaopreco, e.uf) 
                and t3.ufclientefornec = e.uf
                and t3.tiptributacao = decode(i2.tipdivisao, 'V', 'SN', 'SC') 
                and t3.nroregtributacao = nvl(e.nroregtributacao, 0) 
                and a.seqfamilia = fdc.seqfamilia
                and cat.nrodivisao = e.nrodivisao
                and fdc.seqcategoria = cat.seqcategoria
                and fdc.nrodivisao = cat.nrodivisao
                and b.seqfamilia = a.seqfamilia
                and cat.nivelhierarquia = 1
                and cat.statuscategor in ('A','F')
                and fdc.status = 'A'
                and cat.tipcategoria = 'M' 
                and c.seqproduto = pr.seqproduto(+) 
                and c.dtaentradasaida = pr.dtamovimentacao(+) 
                and ss.nroempresa = e.nroempresa
                and ss.nrotributacao = t3.nrotributacao 
                and ss.ufempresa = t3.ufempresa
                and ss.ufclientefornec = t3.ufclientefornec
                and ss.tiptributacao = t3.tiptributacao
                and ss.nroregtributacao = t3.nroregtributacao 
                and ss.seqfamilia = b.seqfamilia 
                and a.seqfamilia = sf.seqfamilia (+) 
                and h.seqfamilia = a.seqfamilia 
                and h.nrosegmento = e.nrosegmentoprinc 
                and t3.nroregtributacao = rt.nroregtributacao
                and c.statuscompra in ('A')
                and implantacao.fstatusvendaproduto(c.seqproduto, c.nroempresa, se.nrosegmento) = 'A'
                and d.seqcomprador not in (8, 11)
                and a.seqprodutobase is null 	
                and round( decode(C.MEDVDIAGERAL, 0, to_number(null), (ESTQLOJA + ESTQDEPOSITO + ESTQALMOXARIFADO + nvl(ESTQTERCEIRO, 0)) / C.MEDVDIAGERAL), 0) > '90'		 
                group by e.nroempresa
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata3 = pd.DataFrame.from_records(data, columns=colums)

        dh = datetime.now()
        da = int(dh.strftime('%Y'))
        dm = int(dh.strftime('%m'))
        dd = calendar.monthrange(da, dm)[1]

        dtatual = dh.strftime("%Y-%m-%d")

        primeiro_dia_mes = datetime(da, dm, day=1)
        ultimo_dia = int(datetime(da, dm, dd).strftime('%d'))
        ultimo_dia_vendas_fechada = int((dh - timedelta(days=1)).strftime('%d'))

        dias_uteis_mes = 0
        dias_uteis_venda = 0
        feriados = list()

        for i in range(ultimo_dia):
            dia = primeiro_dia_mes + timedelta(days=i)
            if dia.weekday() < 5 and dia not in feriados:
                dias_uteis_mes += 1

        for i in range(ultimo_dia_vendas_fechada):
            dia = primeiro_dia_mes + timedelta(days=i)
            if dia.weekday() < 5 and dia not in feriados:
                dias_uteis_venda += 1

        filtromes = dfdata1['DTAVDA'] < dtatual
        filtrodia = dfdata1['DTAVDA'] == dtatual
        cgodevolucoes = [173, 401]
        filtrocgo = dfdata1['CODGERALOPER'].isin(cgodevolucoes)

        # APURAÇÃO DOS DADOS CADAN MATRIZ
        filtroempresa = dfdata1['NROEMPRESA'] == 1
        vendaliquidamesmatriz = round(dfdata1[filtroempresa & filtromes]['VENDAS'].sum(), 2)
        vendaliquidadiamatriz = round(dfdata1[filtroempresa & filtrodia]['VENDAS'].sum(), 2)
        devolucoesmesmatriz = abs(round(dfdata1[filtroempresa & filtromes & filtrocgo]['VENDAS'].sum(), 2))
        devolucoesdiamatriz = abs(round(dfdata1[filtroempresa & filtrodia & filtrocgo]['VENDAS'].sum(), 2))
        pesobrutomesmatriz = round(dfdata1[filtroempresa & filtromes]['PESOBRUTO'].sum(), 2)
        pesobrutodiamatriz = round(dfdata1[filtroempresa & filtrodia]['PESOBRUTO'].sum(), 2)
        lucratividademes = round(dfdata1[filtroempresa & filtromes]['LUCRATIVIDADE'].sum(), 2)
        if vendaliquidamesmatriz == 0:
            margemlucromesmatriz = 0
        else:
            margemlucromesmatriz = round((lucratividademes * 100) / vendaliquidamesmatriz, 2)
        estoquegeralqtdmatriz = float(round(dfdata2[dfdata2['NROEMPRESA'] == 1]['ESTOQUEGERALQTD'].sum(), 0))
        estoquegeralvlrmatriz = round(dfdata2[dfdata2['NROEMPRESA'] == 1]['ESTOQUEGERALVLR'].sum(), 2)
        estoque90diasvlrmatriz = round(dfdata3[dfdata3['NROEMPRESA'] == 1]['ESTOQUEGERALVLR'].sum(), 2)
        tendenciasmatriz = round((vendaliquidamesmatriz / dias_uteis_venda) * dias_uteis_mes, 2)

        # APURAÇÃO DOS DADOS CADAN FILIAL
        filtroempresa = dfdata1['NROEMPRESA'] == 2
        vendaliquidamesfilial = round(dfdata1[filtroempresa & filtromes]['VENDAS'].sum(), 2)
        vendaliquidadiafilial = round(dfdata1[filtroempresa & filtrodia]['VENDAS'].sum(), 2)
        devolucoesmesfilial = abs(round(dfdata1[filtroempresa & filtromes & filtrocgo]['VENDAS'].sum(), 2))
        devolucoesdiafilial = abs(round(dfdata1[filtroempresa & filtrodia & filtrocgo]['VENDAS'].sum(), 2))
        pesobrutomesfilial = round(dfdata1[filtroempresa & filtromes]['PESOBRUTO'].sum(), 2)
        pesobrutodiafilial = round(dfdata1[filtroempresa & filtrodia]['PESOBRUTO'].sum(), 2)
        lucratividademes = round(dfdata1[filtroempresa & filtromes]['LUCRATIVIDADE'].sum(), 2)
        if vendaliquidamesfilial == 0:
            margemlucromesfilial = 0
        else:
            margemlucromesfilial = round((lucratividademes * 100) / vendaliquidamesfilial, 2)
        estoquegeralqtdfilial = float(round(dfdata2[dfdata2['NROEMPRESA'] == 2]['ESTOQUEGERALQTD'].sum(), 0))
        estoquegeralvlrfilial = round(dfdata2[dfdata2['NROEMPRESA'] == 2]['ESTOQUEGERALVLR'].sum(), 2)
        estoque90diasvlrfilial = round(dfdata3[dfdata3['NROEMPRESA'] == 2]['ESTOQUEGERALVLR'].sum(), 2)
        tendenciasfilial = round((vendaliquidamesfilial / dias_uteis_venda) * dias_uteis_mes, 2)

        dtamesini = (datetime(da, dm, day=1)).strftime("%d/%m/%Y")
        dtamesfin = (dh - timedelta(days=1)).strftime("%d/%m/%Y")
        dtatual = dh.strftime("%d/%m/%Y")

        data = {
            'DataInicial': dtamesini,
            'DataFinal': dtamesfin,
            'DataAtual': dtatual,
            'VendasMesMatriz': vendaliquidamesmatriz,
            'VendasMesFilial': vendaliquidamesfilial,
            'VendasDiaMatriz': vendaliquidadiamatriz,
            'VendasDiaFilial': vendaliquidadiafilial,
            'DevolucoesMesMatriz': devolucoesmesmatriz,
            'DevolucoesMesFilial': devolucoesmesfilial,
            'DevolucoesDiaMatriz': devolucoesdiamatriz,
            'DevolucoesDiaFilial': devolucoesdiafilial,
            'PesoBrutoMesMatriz': pesobrutomesmatriz,
            'PesoBrutoMesFilial': pesobrutomesfilial,
            'PesoBrutoDiaMatriz': pesobrutodiamatriz,
            'PesoBrutoDiaFilial': pesobrutodiafilial,
            'MargemLucroMesMatriz': margemlucromesmatriz,
            'MargemLucroMesFilial': margemlucromesfilial,
            'EstoqueGeralQtdMatriz': estoquegeralqtdmatriz,
            'EstoqueGeralQtdFilial': estoquegeralqtdfilial,
            'EstoqueGeralVlrMatriz': estoquegeralvlrmatriz,
            'EstoqueGeralVlrFilial': estoquegeralvlrfilial,
            'Estoque90DiasVlrMatriz': estoque90diasvlrmatriz,
            'Estoque90DiasVlrFilial': estoque90diasvlrfilial,
            'TendenciasMatriz': tendenciasmatriz,
            'TendenciasFilial': tendenciasfilial
        }

        return JsonResponse(data)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


def paineldir01_dashboard(request):
    return render(request, 'bidiretoria/painel01.html')


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
        comunicacao = ComunicacaoSefaz(uf, certificado, senha, homologacao)
        retorno_xml = comunicacao.consulta_cadastro(modelo='nfe', cnpj=cnpj)
        xml_dict = xmltodict.parse(retorno_xml.text)

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

            if int(status_codigo) == 111:
                list_dict = list()
                list_dict.append(status_situacao)
                list_situacao = list_dict
                dicionario = dict(list_situacao[0])
                status_situacao = dicionario['cSit']
            else:
                list_situacao = list(status_situacao)

            if len(list_situacao) > 1:
                for i in range(0, len(list_situacao)):
                    dicionario = dict(list_situacao[i])
                    valor = dicionario['IE']
                    if valor == f'{ie}':
                        status_situacao = dicionario['cSit']
                        break
                print(f'status: {status_codigo} - CNPJ: {cnpj} - UF: {uf}')
            return status_situacao


# FUNÇÕES REFERENTE A EXECUÇÃO DO DASHBOARD01 #
########################################################################################################################
def bilog01_dashboard(request):
    name_file = ['option-type', 'option-situation', 'option-criticize', 'option-route', 'option-date']
    for name in name_file:
        if not os.path.isfile(name+'.txt'):
            write_file = open(name+'.txt', 'x')
            write_file.close()

    var_type = request.GET.get('type')
    var_situation = request.GET.get('situation')
    var_criticize = request.GET.get('criticize')
    var_route = request.GET.get('route')
    var_date = request.GET.get('date')

    list_type = list()
    list_situation = list()
    list_criticize = list()
    list_route = list()
    list_date = list()

    if var_type is None:
        var_type = 'Todos'
        and_type = f"AND a.indentregaretira IN ('E', 'R')"
    else:
        if var_type == 'Todos':
            and_type = f"AND a.indentregaretira IN ('E', 'R')"
        elif var_type == 'Entrega':
            and_type = f"AND a.indentregaretira = 'E'"
        else:
            and_type = f"AND a.indentregaretira = 'R'"

    if var_situation is None:
        var_situation = 'Todos'
        and_situation = "AND a.situacaoped NOT IN ('F', 'D', 'R')"
    else:
        if var_situation == 'Todos':
            and_situation = "AND a.situacaoped NOT IN ('F', 'D', 'R')"
        elif var_situation == 'Analise':
            and_situation = "AND a.situacaoped = 'A'"
        elif var_situation == 'Cancelado':
            and_situation = "AND a.situacaoped = 'C'"
        elif var_situation == 'Digitacao':
            and_situation = "AND a.situacaoped = 'D'"
        elif var_situation == 'Faturado':
            and_situation = "AND a.situacaoped = 'F'"
        elif var_situation == 'Liberado':
            and_situation = "AND a.situacaoped = 'L'"
        elif var_situation == 'Pre-Separacao':
            and_situation = "AND a.situacaoped = 'P'"
        elif var_situation == 'Reterizacao':
            and_situation = "AND a.situacaoped = 'R'"
        elif var_situation == 'Separacao':
            and_situation = "AND a.situacaoped = 'S'"
        else:
            and_situation = "AND a.situacaoped = 'W'"

    if var_criticize is None:
        var_criticize = 'Todos'
        and_criticize = "AND (a.indcriticapedido IN ('B', 'F', 'L') OR(a.indcriticapedido IS NULL))"
    else:
        if var_criticize == 'Todos':
            and_criticize = "AND (a.indcriticapedido IN ('B', 'F', 'L') OR(a.indcriticapedido IS NULL))"
        elif var_criticize == 'Comercial':
            and_criticize = "AND a.indcriticapedido = 'C'"
        else:
            and_criticize = "AND a.indcriticapedido = 'F'"

    if var_route is None:
        var_route = 'Todos'
        and_route = "AND j.descrota IS NOT NULL"
    else:
        if var_route == 'Todos':
            and_route = "AND j.descrota IS NOT NULL"
        else:
            and_route = f"AND j.descrota = '{var_route.upper()}'"

    if var_date is None:
        var_date = 'Todos'
        and_date = "AND ROUND(sysdate - a.dtainclusao, 0) <= 8"
    else:
        if var_date == 'Todos':
            and_date = "AND ROUND(sysdate - a.dtainclusao, 0) <= 8"
        else:
            obj_date = str(var_date)
            obj_format = '%d/%m/%Y'
            var_datetime = datetime.strptime(obj_date, obj_format)
            var_date = var_datetime.strftime('%d-%B-%Y')
            and_date = f"AND TO_DATE(a.dtainclusao + 8) BETWEEN '{var_date}' AND '{var_date}'"

    write_file = open(name_file[0] + '.txt', 'w')
    write_file.write(var_type)
    write_file.close()

    write_file = open(name_file[1] + '.txt', 'w')
    write_file.write(var_situation)
    write_file.close()

    write_file = open(name_file[2] + '.txt', 'w')
    write_file.write(var_criticize)
    write_file.close()

    write_file = open(name_file[3] + '.txt', 'w')
    write_file.write(var_route)
    write_file.close()

    write_file = open(name_file[4] + '.txt', 'w')
    write_file.write(var_date)
    write_file.close()

    try:
        'dh = datetime.now()'
        'dd = dh.strftime("%d")'
        'dm = dh.strftime("%m")'
        'da = dh.strftime("%Y")'
        #dfclientesefaz = pd.read_csv(f'ClientesSEFAZ{dd + dm + da}.txt', sep=';')
        #dfclientesefaz['SEQPESSOA'] = dfclientesefaz['SEQPESSOA'].astype(int)

        query = f"""
                SELECT
                    DISTINCT 
                    DECODE(
                        a.indentregaretira,
                        'E', 'Entrega',
                        'R', 'Retira'
                    )
                    AS indentregaretira,
                    DECODE(
                        a.situacaoped, 
                        'A', 'Analise', 
                        'C', 'Cancelado', 
                        'D', 'Digitacao', 
                        'F', 'Faturado', 
                        'L', 'Liberado', 
                        'P', 'Pre-Separacao', 
                        'R', 'Reterizacao', 
                        'S', 'Separacao', 
                        'W', 'Separado'
                    )
                    AS situacaoped,
                    DECODE(
                        a.indcriticapedido, 
                        'F', 'Financeiro', 
                        'B', 'Comercial', 
                        '', 'Comercial', 
                        'L', 'Liberado'
                    )
                    AS indcriticapedido,
                    a.nropedvenda,
                    LPAD(TO_CHAR(a.seqpessoa), 6, 0) 
                    AS seqpessoa,
                    INITCAP(b.nomerazao)
                    AS nomerazao,
                    INITCAP(j.descrota)
                    AS descrota,
                    TO_DATE(a.dtainclusao + 8) 
                    AS dtalimfatuta
                FROM implantacao.mad_pedvenda     a,
                     implantacao.ge_pessoa        b,
                     implantacao.mad_segmento     d,
                     implantacao.mad_pedvendaitem e,
                     implantacao.map_famembalagem f,
                     implantacao.map_produto      g,
                     implantacao.mad_clienteend   h,
                     implantacao.mad_praca        i,
                     implantacao.mad_rota         j
                WHERE 1=1
                AND b.seqpessoa = a.seqpessoa
                AND h.seqpessoa = a.seqpessoa
                AND d.nrosegmento = a.nrosegmento
                AND e.nropedvenda = a.nropedvenda
                AND e.qtdembalagem = f.qtdembalagem
                AND e.seqproduto = g.seqproduto
                AND f.seqfamilia = g.seqfamilia
                AND h.seqpraca = i.seqpraca
                AND i.seqrota = j.seqrota
                AND a.nroempresa = 1
                {and_type}
                {and_situation}
                {and_criticize}
                {and_route}
                {and_date}
                AND ROUND(sysdate - a.dtainclusao, 0) <= 8
                ORDER BY TO_DATE(a.dtainclusao + 8) ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata = pd.DataFrame.from_records(data, columns=colums)
        dfdata['DTALIMFATUTA'] = pd.to_datetime(dfdata['DTALIMFATUTA'], errors='coerce').dt.strftime('%d/%m/%Y')
        dfdata['NOMERAZAO'] = dfdata['SEQPESSOA']+' » '+dfdata['NOMERAZAO']
        #dfdata['SEQPESSOA'] = dfdata['SEQPESSOA'].astype(int)
        #pd.set_option('display.max_columns', None)
        #dfdata = dfdata.merge(dfclientesefaz, left_on='SEQPESSOA', right_on='SEQPESSOA', how='inner').copy()
        #df_cliente_nh = dfdata.loc[dfdata['SITUACAO'] == 0, ['NOMERAZAO', 'NROPEDVENDA']]

        # FILTRO TIPOS
        if var_type == 'Todos':
            list_type.append('Todos')
            for data in dfdata['INDENTREGARETIRA'].unique():
                list_type.append(data)
        else:
            list_type.append(var_type)
            list_type.append('Todos')

        # FILTRO SITUAÇÕES
        if var_situation == 'Todos':
            list_situation.append('Todos')
            for data in dfdata['SITUACAOPED'].unique():
                list_situation.append(data)
        else:
            list_situation.append(var_situation)
            list_situation.append('Todos')

        # FILTRO CRÍTICAS
        if var_criticize == 'Todos':
            list_criticize.append('Todos')
            for data in dfdata['INDCRITICAPEDIDO'].unique():
                list_criticize.append(data)
        else:
            list_criticize.append(var_criticize)
            list_criticize.append('Todos')

        # FILTRO ROTAS
        if var_route == 'Todos':
            list_route.append('Todos')
            for data in dfdata['DESCROTA'].unique():
                list_route.append(data)
        else:
            list_route.append(var_route)
            list_route.append('Todos')

        # FILTRO DATAS
        if var_date == 'Todos':
            list_date.append('Todos')
            for data in dfdata['DTALIMFATUTA'].unique():
                list_date.append(data)
        else:
            obj_date = str(var_date)
            var_datetime = datetime.strptime(obj_date, '%d-%B-%Y')
            var_date = var_datetime.strftime('%d/%m/%Y')
            list_date.append(var_date)
            list_date.append('Todos')

        df_cliente = dfdata[['NOMERAZAO', 'NROPEDVENDA']].groupby('NOMERAZAO').nunique().reset_index()
        disc_clients = df_cliente.to_dict('records')

        #df_cliente_nh = df_cliente_nh[['NOMERAZAO', 'NROPEDVENDA']].groupby('NOMERAZAO').nunique().reset_index()
        #disc_clients_nh = df_cliente_nh.to_dict('records')

        df_routes = dfdata[['DESCROTA', 'NROPEDVENDA']].groupby('DESCROTA').nunique().reset_index()
        disc_routes = df_routes.to_dict('records')

        dictionary = {
            'type': list_type,
            'situation': list_situation,
            'criticize': list_criticize,
            'route': list_route,
            'date': list_date,
            'clients': disc_clients,
            #'clientsnh': disc_clients_nh,
            'routes': disc_routes,
        }

        return render(request, template_name='bilogistica/dashboard01.html', context=dictionary)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


def bilog01_processo(request):
    name_file = ['option-type', 'option-situation', 'option-criticize', 'option-route', 'option-date']

    var_readfile = open(name_file[0] + '.txt', 'r')
    var_type = var_readfile.read()

    if var_type == 'Todos':
        and_type = f"AND a.indentregaretira IN ('E', 'R')"
    elif var_type == 'Entrega':
        and_type = f"AND a.indentregaretira = 'E'"
    else:
        and_type = f"AND a.indentregaretira = 'R'"

    var_readfile = open(name_file[1] + '.txt', 'r')
    var_situation = var_readfile.read()

    if var_situation == 'Todos':
        and_situation = "AND a.situacaoped NOT IN ('F', 'D', 'R')"
    elif var_situation == 'Analise':
        and_situation = "AND a.situacaoped = 'A'"
    elif var_situation == 'Cancelado':
        and_situation = "AND a.situacaoped = 'C'"
    elif var_situation == 'Digitacao':
        and_situation = "AND a.situacaoped = 'D'"
    elif var_situation == 'Faturado':
        and_situation = "AND a.situacaoped = 'F'"
    elif var_situation == 'Liberado':
        and_situation = "AND a.situacaoped = 'L'"
    elif var_situation == 'Pre-Separacao':
        and_situation = "AND a.situacaoped = 'P'"
    elif var_situation == 'Reterizacao':
        and_situation = "AND a.situacaoped = 'R'"
    elif var_situation == 'Separacao':
        and_situation = "AND a.situacaoped = 'S'"
    else:
        and_situation = "AND a.situacaoped = 'W'"

    var_readfile = open(name_file[2] + '.txt', 'r')
    var_criticize = var_readfile.read()
    if var_criticize == 'Todos':
        and_criticize = "AND (a.indcriticapedido IN ('B', 'F', 'L') OR(a.indcriticapedido IS NULL))"
    elif var_criticize == 'Comercial':
        and_criticize = "AND a.indcriticapedido = 'C'"
    else:
        and_criticize = "AND a.indcriticapedido = 'F'"

    var_readfile = open(name_file[3] + '.txt', 'r')
    var_route = var_readfile.read()
    if var_route == 'Todos':
        and_route = "AND j.descrota IS NOT NULL"
    else:
        and_route = f"AND j.descrota LIKE '{var_route.upper()}%'"

    var_readfile = open(name_file[4] + '.txt', 'r')
    var_date = var_readfile.read()
    if var_date == 'Todos':
        and_date = "AND ROUND(sysdate - a.dtainclusao, 0) <= 8"
    else:
        and_date = f"AND TO_DATE(a.dtainclusao + 8) BETWEEN '{var_date}' AND '{var_date}'"

    try:
        query = f"""
                SELECT
                    DISTINCT 
                    a.nropedvenda,	
                    TO_DATE(a.dtainclusao) 
                    AS dtainclusao,
                    TO_DATE(a.dtainclusao + 8) 
                    AS dtalimfatuta,
                    DECODE(
                        a.indentregaretira,
                        'E', 'Entrega',
                        'R', 'Retira'
                    )
                    AS indentregaretira,
                    a.seqpessoa,
                    b.nomerazao,
                    DECODE(
                        a.situacaoped, 
                        'A', 'Analise', 
                        'C', 'Cancelado', 
                        'D', 'Digitacao', 
                        'F', 'Faturado', 
                        'L', 'Liberado', 
                        'P', 'Pre-Separacao', 
                        'R', 'Reterizacao', 
                        'S', 'Separacao', 
                        'W', 'Separado'
                    )
                    AS situacaoped,
                    DECODE(
                        a.indcriticapedido, 
                        'F', 'Financeiro', 
                        'B', 'Comercial', 
                        '', 'Comercial', 
                        'L', 'Liberado'
                    )
                    AS indcriticapedido,
                    a.motcancelamento,
                    ROUND(sysdate - a.dtainclusao)
                    AS quantidadedia,
                    INITCAP(j.descrota)
                    AS descrota
                FROM implantacao.mad_pedvenda     a,
                     implantacao.ge_pessoa        b,
                     implantacao.mad_segmento     d,
                     implantacao.mad_pedvendaitem e,
                     implantacao.map_famembalagem f,
                     implantacao.map_produto      g,
                     implantacao.mad_clienteend   h,
                     implantacao.mad_praca        i,
                     implantacao.mad_rota         j
                WHERE 1=1
                AND b.seqpessoa = a.seqpessoa
                AND h.seqpessoa = a.seqpessoa
                AND d.nrosegmento = a.nrosegmento
                AND e.nropedvenda = a.nropedvenda
                AND e.qtdembalagem = f.qtdembalagem
                AND e.seqproduto = g.seqproduto
                AND f.seqfamilia = g.seqfamilia
                AND h.seqpraca = i.seqpraca
                AND i.seqrota = j.seqrota
                AND a.nroempresa = 1
                {and_type}
                {and_situation}
                {and_criticize}
                {and_route}
                {and_date}
                AND ROUND(sysdate - a.dtainclusao, 0) <= 8
                GROUP BY a.nropedvenda, a.indentregaretira, a.nrocarga, a.seqpessoa,
                         b.nomerazao, b.fantasia, a.situacaoped, a.dtainclusao, a.dtaalteracao,
                         round(sysdate - a.dtainclusao), a.dtabasefaturamento, a.nrosegmento,
                         d.descsegmento, a.dtalibcredped, j.descrota, a.motcancelamento,
                         a.obspedido, a.indcriticapedido, a.dtahorgeracaonf, a.dtainclusao,
                         a.dtageracaocarga, a.dtahorsituacaopedalt, a.usualteracao, a.usuaprovcredito,
                         e.qtdpedida, e.qtdatendida
                ORDER BY 2 ASC
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        dfdata = pd.DataFrame.from_records(data, columns=colums)
        df_dtalimfatura = dfdata[['DTALIMFATUTA', 'NROPEDVENDA']].groupby('DTALIMFATUTA').nunique().reset_index()
        df_dtalimfatura['DTALIMFATUTA'] = pd.to_datetime(df_dtalimfatura['DTALIMFATUTA'], errors='coerce').dt.strftime('%d-%m-%Y')

        list_dtalimfatura_labels = list()
        list_dtalimfatura_values = list()

        for index, reg in df_dtalimfatura.iterrows():
            list_dtalimfatura_labels.append(reg.DTALIMFATUTA)
            list_dtalimfatura_values.append(reg.NROPEDVENDA)

        dictionary = {
            'dtafatura_labels': list_dtalimfatura_labels,
            'dtafatura_values': list_dtalimfatura_values,
        }

        return JsonResponse(dictionary)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


# FUNÇÕES REFERENTE A EXECUÇÃO DO PAINEL04 » LOGÍSTICA #
########################################################################################################################
def painellog04_dashboard(request):
    return render(request, 'bilogistica/painel04.html')


def analise_log_painel04(request):
    try:
        query = """
                SELECT 
                    codrua,
                    rua
                FROM implantacao.mlo_rua
                WHERE 1=1 
                AND nroempresa = 1
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_ruas = pd.DataFrame.from_records(data, columns=colums)
        df_ruas.fillna(value=0, inplace=True)

        query = """
                SELECT
                    especieendereco,
                    descespecie 
                FROM implantacao.mlo_especieendereco
                WHERE 1=1
                AND nroempresa = 1
                AND statusespecieendereco = 'A'
                """
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_salas = pd.DataFrame.from_records(data, columns=colums)
        df_salas.fillna(value=0, inplace=True)

        query = """
                SELECT			 
                    codrua,
                    nropredio,
                    nroapartamento,
                    TO_CHAR(nrosala) AS nrosala,
                    especieendereco, 
                    indterreoaereo,
                    seqpaleterf,
                    seqproduto,
                    qtdembalagem,
                    (qtdatual/qtdembalagem) AS qtdestqatual,
                    statusendereco,
                    dtaalteracao
                FROM implantacao.mlo_endereco
                WHERE 1=1
                AND nroempresa = 1
                AND codrua LIKE '0%'
                AND statusendereco != 'I'
                AND especieendereco = 'P'	
                --AND codrua = '014'
                ORDER BY 1,2,3,4 ASC
                """
        conn = ConnectOracle.conn_oracle()
        cur = conn.cursor()
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        pd.set_option('display.max_columns', None)
        df_enderecos = pd.DataFrame.from_records(data, columns=colums)
        df_enderecos['DTAALTERACAO'] = pd.to_datetime(df_enderecos['DTAALTERACAO'], errors='coerce').dt.strftime('%d/%m/%Y')
        df_enderecos.fillna(value=0, inplace=True)
        df_principal = pd.merge(df_ruas, df_enderecos, left_on='CODRUA', right_on='CODRUA', how='inner')
        df_principal = pd.merge(df_salas, df_principal, left_on='ESPECIEENDERECO', right_on='ESPECIEENDERECO', how='inner')
        df_principal['DESCESPECIE'] = df_principal['DESCESPECIE'] + ' ' + df_principal['NROSALA']
        df_principal = df_principal.groupby(['CODRUA', 'NROPREDIO', 'NROAPARTAMENTO', 'RUA', 'DESCESPECIE']).count().reset_index()
        df_ruas = df_principal['RUA']
        df_predios_rua01 = df_principal.loc[df_principal['CODRUA'] == 1, 'NROPREDIO']

        list_ruas = list()
        for rua in df_ruas.values:
            list_ruas.append(rua[:7].strip().capitalize())

        list_predios = list()
        for predio in df_predios_rua01.values:
            list_predios.append(predio)

        ruas = pd.Series(list_ruas).unique().tolist()
        predios = pd.Series(list_predios).unique().tolist()

        data = {
            'Ruas': ruas,
            'Predios': predios
        }



        return JsonResponse(data)
    except ora.DatabaseError as Error:
        return HttpResponse(Error)


# FUNÇÕES REFERENTE A EXECUÇÃO DO DASHBOARD » FINANCEIRO #
########################################################################################################################
def bifin01_dashboard(request):
    return render(request, 'bifinanceiro/dashboard01.html')


def bifin01_processo(request):
    pd.set_option('display.max_columns', None)
    try:
        conn = ConnectDatabase.conn_postgresql()
        cur = conn.cursor()

        query = """
                SELECT
                    "Documento.Id"      AS "DocumentoId",
                    "Especie.Id"        AS "EspecieId",
                    "Data Emissao"      AS "DtaEmissao",
                    "Titulo",
                    "Documento",
                    "Parcela",
                    "Aberto/Quitado"    AS "AbertoQuitado",
                    "Situacao",
                    "Original R$"       AS "TituloValor"
                FROM pbi."fatContasReceber"
                WHERE 1=1
                AND "Data Emissao" >= (date_trunc('year'::text, now()::timestamp without time zone) - '2 years'::interval) 
                AND "Data Emissao" <= (now() - '1 day'::interval)
                ORDER BY "Data Emissao" ASC
                """
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_contasreceber = pd.DataFrame.from_records(data, columns=colums)
        df_contasreceber = pd.to_datetime(df_contasreceber['DtaEmissao'], errors='coerce').dt.strftime('%d/%m/%Y')
        df_contasreceber.fillna(value=0, inplace=True)

        query = """
                SELECT
                    "Documento.Id"      AS "DocumentoId",
                    "Empresa.Id"        AS "EmpresaId",
                    "Cliente.Id"        AS "ClienteId",
                    "Data Inclusao"     AS "DtaInclusao",
                    "Nota Fiscal"       AS "NotaFiscal",
                    "Serie",
                    "Tipo",
                    "Valor"             AS "NotaFiscalValor"
                FROM pbi."fatPedidosFaturados"
                WHERE 1=1
                AND "Data Inclusao" >= (date_trunc('year'::text, now()::timestamp without time zone) - '2 years'::interval) 
                AND "Data Inclusao" <= (now() - '1 day'::interval)
                ORDER BY "Data Inclusao" ASC
                """
        cur.execute(query)
        colums = [row[0] for row in cur.description]
        data = cur.fetchall()
        df_pedidosfaturados = pd.DataFrame.from_records(data, columns=colums)
        df_pedidosfaturados = pd.to_datetime(df_pedidosfaturados['DtaInclusao'], errors='coerce').dt.strftime('%d/%m/%Y')
        df_pedidosfaturados.fillna(value=0, inplace=True)

        df_principal = pd.merge(df_pedidosfaturados, df_contasreceber, left_on='DocumentoId', right_on='DocumentoId', how='inner')
        df_notasfiscais = df_principal[['DtaInclusao', 'NotaFiscal', 'Serie', 'Tipo', 'NotaFiscalValor']]
        "df_titulos = df_principal[['DtaEmissao', 'Titulo', 'Documento', 'Parcela', 'AbertoQuitado', 'Situacao', 'TituloValor']]"

        list_notasfiscais = list()
        #list_notasfiscais.append(df_notasfiscais.columns.tolist())
        for value in df_notasfiscais.values:
            list_notasfiscais.append(value.tolist())

        print(list_notasfiscais[0])

        """data = {
            "NotasFiscais": list_notasfiscais[0],
        }"""

        return HttpResponse('')
    except ora.DatabaseError as Error:
        return HttpResponse(Error)

