""" PACOTES """
import pandas as pd
import datetime as dta
import cx_Oracle as ora
import psycopg2 as pga

""" FUNÇÃO PARA SE CONECTAR NO BANCO DE DADOS ORACLE """
ora.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_3")


def conect_oracle():
    oracle = ora.connect("hsantos/H1s@ntos1969@192.168.168.200:1521/orcl")
    return oracle


def conect_postgresql():
    """ FUNÇÃO PARA SE CONECTAR NO BANCO DE DADOS POSTGRESQL """
    postgre = pga.connect(
                host="172.16.157.3",
                port="2899",
                database="PBICadan",
                user="postgres",
                password="cfb5ce8c49"
              )
    return postgre


def create_table(query):
    """ FUNÇÃO PARA CRIAR TABELA DO POSTGRESQL """
    con = conect_postgresql()
    cur = con.cursor()
    try:
        cur.execute(query)
        con.commit()
    except (Exception, pga.DatabaseError) as Erro:
        print("Erro: %s" % Erro)
        con.rollback()
        cur.close()
        return 1
    cur.close()


def process_data(query):
    """ FUNÇÃO PARA INSERIR, ALTERAR E DELETAR DADOS NA TABELA DO POSTGRESQL """
    con = conect_postgresql()
    cur = con.cursor()
    try:
        cur.execute(query)
        con.commit()
    except (Exception, pga.DatabaseError) as Erro:
        print("Erro: %s" % Erro)
        con.rollback()
        cur.close()
        return 1
    cur.close()


def postgres_consulta(query):
    """ FUNÇÃO PARA CONSULTAR DADOS NO BANCO DE DADOS POSTGRES """
    con_postgres = conect_postgresql()
    tabela = con_postgres.cursor()
    tabela.execute(query)
    colunas = [row[0] for row in tabela.description]
    linhas = tabela.fetchall()
    df = pd.DataFrame.from_records(linhas, columns=colunas)
    return df


def carga_inicial():
    """ FUNÇÃO DE DATAS DE CARREGAMENTO """
    varanoini = dta.datetime.today().date().year - 4
    varanofin = dta.datetime.today().date().year + 1
    vanos = []
    for i in range(varanoini, varanofin):
        vanos.append(str(i))

    vmes = []
    for i in range(1, 13):
        vmes.append(str(i))

    vdia = []
    for i in range(1, 32):
        vdia.append(str(i))

    return vanos, vmes, vdia


def carga_parcial():
    vardataini = dta.datetime.today().date() - dta.timedelta(days=15)
    vardatafin = dta.datetime.today().date()
    vanoant = []
    vanoatu = []
    vmesesant = []
    vmesesatu = []
    vdiasant = []
    vdiasatu = []
    listacalendarioanoatual = []
    listacalendarioanoanterior = []
    listaresultado = []
    quantidademes = 0

    if vardataini.year == vardatafin.year:
        quantidadeano = 1
        if vardataini.month == vardatafin.month:
            quantidademes = 1
        else:
            quantidademes = len([vardataini.month, vardatafin.month])
    else:
        quantidadeano = len([vardataini.year, vardatafin.year])

    """ PEGA O  ANO ANTERIOR E ATUAL """
    if quantidadeano == 1:
        vanoatu.append(str(vardatafin.year))
    else:
        for i in range(vardataini.year, vardataini.year + 1):
            vanoant.append(str(i))
        for i in range(varDataFin.year, vardatafin.year + 1):
            vanoatu.append(str(i))

    """ PEGA OS MESES ANTERIOR E ATUAL """
    if quantidadeano == 1:
        if quantidademes == 1:
            vMesesAtu.append(str(vardatafin.month))
        else:
            for i in range(vardataini.month, vardatafin.month + 1):
                vmesesatu.append(str(i))
    else:
        if vardataini.year != vardatafin.year:
            for i in range(vardataini.month, 13):
                vmesesant.append(str(i))
            for i in range(1, vardatafin.month + 1):
                vmesesatu.append(str(i))

    """ PEGA OS DIAS ANTERIOR E ATUAL """
    if quantidadeano == 1:
        if quantidademes == 1:
            for i in range(vardataini.day, vardatafin.day + 1):
                vdiasatu.append(i)
        else:
            for i in range(1, 32):
                vdiasatu.append(i)
    else:
        for i in range(vardataini.day, 32):
            vdiasant.append(i)
        for i in range(1, vardatafin.day + 1):
            vdiasatu.append(i)

    if QuantidadeAno == 2:
        listacalendarioanoanterior.append(vAnoAnt)
        listacalendarioanoanterior.append(vMesesAnt)
        listacalendarioanoanterior.append(vDiasAnt)
        listacalendarioanoatual.append(vAnoAtu)
        listacalendarioanoatual.append(vMesesAtu)
        listacalendarioanoatual.append(vDiasAtu)

    if QuantidadeAno == 1:
        listaresultado.append(vAnoAtu)
        listaresultado.append(vMesesAtu)
        listaresultado.append(vDiasAtu)
    else:
        listaresultado.append(ListaCalendarioAnoAnterior)
        listaresultado.append(ListaCalendarioAnoAtual)

    del vanoant, vanoatu, vmesesant, vmesesatu, listacalendarioanoatual, listacalendarioanoanterior

    return listaresultado
