#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import datetime as dta
import md002Funcoes as fun
import md001CreateTableScripts as tbl
from pathlib import Path

# DataAtual = dta.datetime.today().date() - dta.timedelta(days=5)
DataAtual = dta.datetime.today().date()

Dia = str(DataAtual.day)
Mes = str(DataAtual.month)
Ano = str(DataAtual.year)
DataAtual = Dia.zfill(2) + "/" + Mes.zfill(2) + "/" + Ano.zfill(4)
dirtxt = "log"
base_dir = Path(__file__).cwd()
NomeArquivo = os.path.join(base_dir, dirtxt) + "\LogCarga" + Dia.zfill(2) + Mes.zfill(2) + Ano.zfill(4) + ".txt"

qtddia = 30


def etl_dimensoes():
    ###############################################################################################################
    # SE CONECTAR E BUSCA DE vDados NO BANCO DE vDados CONCINCO "ORACLE"
    ###############################################################################################################
    cOracle = fun.conect_oracle()

    try:
        ###############################################################################################################
        # CRIAS AS TABELAS, CASO N�O EXISTA
        ###############################################################################################################
        fun.create_table(tbl.CreateTable_GeEmpresa)
        fun.create_table(tbl.CreateTable_GePessoa)
        fun.create_table(tbl.CreateTable_GePessoaCadastro)
        fun.create_table(tbl.CreateTable_MrlCliente)
        fun.create_table(tbl.CreateTable_MadRepresentante)
        fun.create_table(tbl.CreateTable_MadClienteRepresentante)
        fun.create_table(tbl.CreateTable_MadSegmento)
        fun.create_table(tbl.CreateTable_MapMarca)
        fun.create_table(tbl.CreateTable_MaxDivisao)
        fun.create_table(tbl.CreateTable_MapFamilia)
        fun.create_table(tbl.CreateTable_MapFamiliaDivisao)
        fun.create_table(tbl.CreateTable_MapFamiliaFornecedor)
        fun.create_table(tbl.CreateTable_MaxCodigoGeralOperacao)
        fun.create_table(tbl.CreateTable_MadEquipe)
        fun.create_table(tbl.CreateTable_MapFamiliaEmbalagem)
        fun.create_table(tbl.CreateTable_MapProduto)
        fun.create_table(tbl.CreateTable_GeLogradouro)
        fun.create_table(tbl.CreateTable_GeBairro)
        fun.create_table(tbl.CreateTable_GeCidade)
        fun.create_table(tbl.CreateTable_MafFornecedor)
        fun.create_table(tbl.CreateTable_GeBanco)
        fun.create_table(tbl.CreateTable_FiEspecie)
        fun.create_table(tbl.CreateTable_Mrl_ProdutoStatusVenda)
        fun.create_table(tbl.CreateTable_Map_ProdutoStatusCompra)
        fun.create_table(tbl.CreateTable_Map_Secao)
        fun.create_table(tbl.CreateTable_Map_Categoria)
        fun.create_table(tbl.CreateTable_Map_SubCategoria)
        fun.create_table(tbl.CreateTable_ProdutosEmbalagemCompra)
        fun.create_table(tbl.CreateTable_ProdutosEmbalagemVenda)
        fun.create_table(tbl.CreateTable_RedesClientes)
        fun.create_table(tbl.CreateTable_Compradores)
        fun.create_table(tbl.CreateTable_DatasCritica)

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS GE_REDEPESSOA INNER JOIN GE_REDE
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("SELECT seqpessoa FROM pbi.carga_redesclientes")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        redesclientes = pd.DataFrame(dados, columns=colunas)
        total_reg = redesclientes["seqpessoa"].count()

        if total_reg == 0:
            select_redesclientes \
                = \
                """
                SELECT 
                    a.seqrede,
                    a.seqpessoa,
                    REPLACE(b.descricao, '''', '') AS descricao,
                    a.status,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao 
                FROM implantacao.ge_redepessoa a
                INNER JOIN implantacao.ge_rede b ON b.seqrede = a.seqrede	
                WHERE 1=1
                AND a.status = 'A'	
                ORDER BY a.seqpessoa ASC                
                """
        else:
            select_redesclientes \
                = \
                f"""
                SELECT 
                    a.seqrede,
                    a.seqpessoa,
                    REPLACE(b.descricao, '''', '') AS descricao,
                    a.status,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao 
                FROM implantacao.ge_redepessoa a
                INNER JOIN implantacao.ge_rede b ON b.seqrede = a.seqrede	
                WHERE 1=1
                AND a.status = 'A'
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                ORDER BY 2 ASC               
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_redesclientes)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        redesclientes = pd.DataFrame.from_records(dados, columns=colunas)

        print("Execultado processo na tabela de \"Cadastro Redes de Clientes\" ...")
        with open(NomeArquivo, "a") as arquivo:
            arquivo.write("Execultado processo na tabela de \"Cadastro Redes de Clientes\" ...\n")
        arquivo.close()

        campos = "(seqrede,seqpessoa,descricao,status,dtaalteracao)"

        for index, reg in redesclientes.iterrows():
            seqrede = str(reg.SEQREDE)
            seqpessoa = str(reg.SEQPESSOA)
            descricao = reg.DESCRICAO
            status = reg.STATUS
            dtaalteracao = str(reg.DTAALTERACAO)

            query = \
                f"""
                SELECT * FROM pbi.carga_redesclientes
                WHERE seqrede = {seqrede}
                AND seqpessoa = {seqpessoa}
                """
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                dados = "('" + seqrede + "','" + seqpessoa + "','" + descricao + "','" + status + "','" + dtaalteracao + "')"
                sql_insert \
                    = "INSERT INTO pbi.carga_redesclientes\n" + campos + "\nVALUES\n" + dados + "\n"
                fun.process_data(sql_insert)
            else:
                sql_update \
                    = \
                    "UPDATE pbi.carga_redesclientes " \
                    "SET " \
                    "descricao = '" + descricao + "'," \
                    "status = '" + status + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1 = 1 " \
                    "AND seqrede = '" + seqrede + "' " \
                    "AND seqpessoa = '" + seqpessoa + "'"
                fun.process_data(sql_update)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # CADASTRO DE DATAS CRÍTICAS
        # BUSCA DADOS DA TABELA MLO_ENDERECO INNER JOIN MLO_PALETEQTDE
        ###############################################################################################################
        cPostgres = fun.conect_postgresql()
        curpostgre = cPostgres.cursor()
        curpostgre.execute("select * from pbi.carga_datacritica")
        Col = [Nome[0] for Nome in curpostgre.description]
        Dados = curpostgre.fetchall()
        dfdatacritica = pd.DataFrame(Dados, columns=Col)
        TotalReg = len(dfdatacritica)

        if TotalReg == 0:
            select_mloendereco \
                = \
                """
                SELECT
                    a.nroempresa,
                    a.seqproduto,
                    f.embalagem||' '|| e.qtdembalagem AS embalagem,
                    SUM(b.qtdatual/e.qtdembalagem) AS qtdatual,
                    ((c.estqdeposito-c.qtdreservadavda)/e.qtdembalagem) AS qtdestoque,
                    b.dtavalidade,
                    TO_DATE(MAX(a.dtaalteracao)) AS dtaalteracao
                FROM implantacao.mlo_endereco a
                INNER JOIN implantacao.mlo_enderecoqtde b 
                ON b.seqendereco = a.seqendereco
                INNER JOIN implantacao.mrl_produtoempresa c 
                ON c.nroempresa = a.nroempresa AND c.seqproduto = a.seqproduto
                INNER JOIN implantacao.map_produto d 
                ON d.seqproduto = a.seqproduto
                INNER JOIN (SELECT DISTINCT seqfamilia, padraoembvenda AS qtdembalagem
                            FROM implantacao.mad_famsegmento
                            WHERE 1=1
                            AND nrosegmento NOT IN (2, 11, 50)) e 
                ON e.seqfamilia = d.seqfamilia 
                INNER JOIN implantacao.map_famembalagem f 
                ON f.seqfamilia = e.seqfamilia AND f.qtdembalagem = e.qtdembalagem
                WHERE 1=1
                AND a.especieendereco NOT IN ('L', 'V')	
                AND b.dtavalidade = (SELECT DISTINCT MIN(d.dtavalidade)
                                     FROM implantacao.mlo_endereco c
                                     INNER JOIN implantacao.mlo_enderecoqtde d on d.seqendereco = c.seqendereco
                                     WHERE 1=1
                                     AND c.especieendereco NOT IN ('L', 'V')	
                                     AND c.nroempresa = a.nroempresa
                                     AND c.seqproduto = a.seqproduto)
                GROUP BY a.nroempresa, a.seqproduto, b.dtavalidade, c.estqdeposito, c.qtdreservadavda, e.qtdembalagem, 
                         f.embalagem
                ORDER BY a.seqproduto ASC
                """
        else:
            select_mloendereco \
                = \
                f"""
                SELECT
                    a.nroempresa,
                    a.seqproduto,
                    f.embalagem||' '|| e.qtdembalagem AS embalagem,
                    SUM(b.qtdatual/e.qtdembalagem) AS qtdatual,
                    ((c.estqdeposito-c.qtdreservadavda)/e.qtdembalagem) AS qtdestoque,
                    b.dtavalidade,
                    TO_DATE(MAX(a.dtaalteracao)) AS dtaalteracao
                FROM implantacao.mlo_endereco a
                INNER JOIN implantacao.mlo_enderecoqtde b 
                ON b.seqendereco = a.seqendereco
                INNER JOIN implantacao.mrl_produtoempresa c 
                ON c.nroempresa = a.nroempresa AND c.seqproduto = a.seqproduto
                INNER JOIN implantacao.map_produto d 
                ON d.seqproduto = a.seqproduto
                INNER JOIN (SELECT DISTINCT seqfamilia, padraoembvenda AS qtdembalagem
                            FROM implantacao.mad_famsegmento
                            WHERE 1=1
                            AND nrosegmento NOT IN (2, 11, 50)) e 
                ON e.seqfamilia = d.seqfamilia 
                INNER JOIN implantacao.map_famembalagem f 
                ON f.seqfamilia = e.seqfamilia AND f.qtdembalagem = e.qtdembalagem
                WHERE 1=1
                AND a.especieendereco NOT IN ('L', 'V')	
                AND b.dtavalidade = (SELECT DISTINCT MIN(d.dtavalidade)
                                     FROM implantacao.mlo_endereco c
                                     INNER JOIN implantacao.mlo_enderecoqtde d on d.seqendereco = c.seqendereco
                                     WHERE 1=1
                                     AND c.especieendereco NOT IN ('L', 'V')	
                                     AND c.nroempresa = a.nroempresa
                                     AND c.seqproduto = a.seqproduto)
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                GROUP BY a.nroempresa, a.seqproduto, b.dtavalidade, c.estqdeposito, c.qtdreservadavda, e.qtdembalagem, 
                         f.embalagem
                ORDER BY a.seqproduto ASC
                """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(select_mloendereco)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        dfdatacritica = pd.DataFrame.from_records(Reg, columns=Col)

        print("Execultado processo na tabela de \"Cadastro Datas Críticas\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("Execultado processo na tabela de \"Cadastro Datas Críticas\" ...\n")
        Arquivo.close()

        Campos = "(nroempresa, seqproduto, embalagem, qtdatual, qtdestoque, dtavalidade, dtaalteracao)"

        for index, reg in dfdatacritica.iterrows():
            id_empresa = str(reg.NROEMPRESA)
            id_produto = str(reg.SEQPRODUTO)
            embalagem = str(reg.EMBALAGEM)
            qtdatual = str(reg.QTDATUAL)
            qtdestoque = str(reg.QTDESTOQUE)
            dtavalidade = str(reg.DTAVALIDADE)
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"SELECT * FROM pbi.carga_datacritica WHERE nroempresa = {id_empresa} AND seqproduto = {id_produto}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            Operacao = len(dados)

            if Operacao == 0:
                Dados = ("('" + id_empresa + "','" +
                                id_produto + "','" +
                                embalagem + "','" +
                                qtdatual + "','" +
                                qtdestoque + "','" +
                                dtavalidade + "','" +
                                dtaalteracao + "')")
                sqlInsert \
                    = "INSERT INTO pbi.carga_datacritica\n" + Campos + "\nVALUES\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate \
                    = \
                    "UPDATE pbi.carga_datacritica " \
                    "SET " \
                    "embalagem = '" + embalagem + "'," \
                    "qtdatual = '" + qtdatual + "'," \
                    "qtdestoque = '" + qtdestoque + "'," \
                    "dtavalidade = '" + dtavalidade + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1=1 " \
                    "AND nroempresa = '" + id_empresa + "' " \
                    "AND seqproduto = '" + id_produto + "'"
                fun.process_data(sqlupdate)

        curpostgre.close()
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAF_FORNECEDOR
        ###############################################################################################################
        cPostgres = fun.conect_postgresql()
        curpostgre = cPostgres.cursor()
        curpostgre.execute("select seqfornecedor from pbi.carga_Fornecedor")
        Col = [Nome[0] for Nome in curpostgre.description]
        Dados = curpostgre.fetchall()
        dfFornecedor = pd.DataFrame(Dados, columns=Col)
        TotalReg = dfFornecedor["seqfornecedor"].count()

        if TotalReg == 0:
            Select_MafFornecedor \
                = \
                """
                SELECT
                    DISTINCT
                    a.seqfornecedor,
                    CASE
                    WHEN a.tipfornecedor = 'D' THEN 'DISTRIBUIDOR'
                    WHEN a.tipfornecedor = 'I' THEN 'INDUSTRIA'
                    ELSE 'PRESTADOR SERVICO'
                    END 
                    tipfornecedor,
                    NVL(REGEXP_REPLACE(c.nomerazao, '''', ''),'NAO INFORMADO') nomerazao,
                    a.statusgeral
                FROM implantacao.maf_fornecedor a
                INNER JOIN implantacao.map_famfornec b ON b.seqfornecedor = a.seqfornecedor AND b.principal = 'S'
                INNER JOIN implantacao.ge_pessoa c ON c.seqpessoa = a.seqfornecedor
                WHERE 1=1
                ORDER BY a.seqfornecedor ASC
                """
        else:
            Select_MafFornecedor \
                = \
                f"""
                SELECT
                    DISTINCT
                    a.seqfornecedor,
                    CASE
                    WHEN a.tipfornecedor = 'D' THEN 'DISTRIBUIDOR'
                    WHEN a.tipfornecedor = 'I' THEN 'INDUSTRIA'
                    ELSE 'PRESTADOR SERVICO'
                    END 
                    tipfornecedor,
                    NVL(REGEXP_REPLACE(c.nomerazao, '''', ''),'NAO INFORMADO') nomerazao,
                    a.statusgeral
                FROM implantacao.maf_fornecedor a
                INNER JOIN implantacao.map_famfornec b ON b.seqfornecedor = a.seqfornecedor AND b.principal = 'S'
                INNER JOIN implantacao.ge_pessoa c ON c.seqpessoa = a.seqfornecedor
                WHERE 1=1
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                ORDER BY a.seqfornecedor ASC
                """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(Select_MafFornecedor)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        dfFornecedor = pd.DataFrame.from_records(Reg, columns=Col)

        print("Execultado processo na tabela de \"Cadastro Fornecedor\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("Execultado processo na tabela de \"Cadastro Fornecedor\" ...\n")
        Arquivo.close()

        Campos = "(seqfornecedor,tipfornecedor,nomerazaosocial,status)"

        for index, reg in dfFornecedor.iterrows():
            FornecedorId = str(reg.SEQFORNECEDOR)
            Tipo = reg.TIPFORNECEDOR.title()
            Nome = reg.NOMERAZAO.title()
            Status = reg.STATUSGERAL

            query = f"SELECT * FROM pbi.carga_Fornecedor WHERE seqfornecedor = {FornecedorId}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            Operacao = len(dados)

            if Operacao == 0:
                Dados = "('" + FornecedorId + "','" + Tipo + "','" + Nome + "','" + Status + "')"
                sqlInsert \
                    = "INSERT INTO pbi.carga_fornecedor\n" + Campos + "\nVALUES\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate \
                    = \
                    "UPDATE pbi.carga_fornecedor " \
                    "SET " \
                    "tipfornecedor = '" + Tipo + "'," \
                    "nomerazaosocial = '" + Nome + "'," \
                    "status = '" + Status + "' " \
                    "WHERE 1=1 " \
                    "AND seqfornecedor = '" + FornecedorId + "'"
                fun.process_data(sqlupdate)

        curpostgre.close()
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAF_FORNECDIVISAO INNER JOIN MAX_COMPRADOR
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("SELECT seqfornecedor FROM pbi.carga_compradores")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        compradores = pd.DataFrame(dados, columns=colunas)
        total_reg = compradores["seqfornecedor"].count()

        if total_reg == 0:
            select_compradores \
                = \
                """
                SELECT 
                    a.seqfornecedor, 
                    a.seqcomprador, 
                    b.comprador, 
                    b.apelido,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao 
                FROM implantacao.maf_fornecdivisao a
                INNER JOIN implantacao.max_comprador b 
                ON b.seqcomprador = a.seqcomprador	
                WHERE 1=1
                ORDER BY a.seqfornecedor ASC                
                """
        else:
            select_compradores \
                = \
                f"""
                SELECT 
                    a.seqfornecedor, 
                    a.seqcomprador, 
                    b.comprador, 
                    b.apelido,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao
                FROM implantacao.maf_fornecdivisao a
                INNER JOIN implantacao.max_comprador b 
                ON b.seqcomprador = a.seqcomprador	
                WHERE 1=1
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                ORDER BY 2 ASC               
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_compradores)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        compradores = pd.DataFrame.from_records(dados, columns=colunas)

        print("Execultado processo na tabela de \"Cadastro de Compradores\" ...")
        with open(NomeArquivo, "a") as arquivo:
            arquivo.write("Execultado processo na tabela de \"Cadastro de Compradores\" ...\n")
        arquivo.close()

        campos = "(seqfornecedor,seqcomprador,compradornome,compradorapelido,dtaalteracao)"

        for index, reg in compradores.iterrows():
            seqfornecedor = str(reg.SEQFORNECEDOR)
            seqcomprador = str(reg.SEQCOMPRADOR)
            compradornome = reg.COMPRADOR
            compradorapelido = reg.APELIDO
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"SELECT * FROM pbi.carga_compradores WHERE seqfornecedor = {seqfornecedor}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                dados = "('" + seqfornecedor + "','" + seqcomprador + "','" + compradornome + "','" + compradorapelido + "','" + dtaalteracao + "')"
                sql_insert = "insert into pbi.carga_compradores\n" + campos + "\nvalues\n" + dados + "\n"
                fun.process_data(sql_insert)
            else:
                sql_update \
                    = \
                    "update pbi.carga_compradores " \
                    "SET " \
                    "seqcomprador = '" + seqcomprador + "'," \
                    "compradornome = '" + compradornome + "'," \
                    "compradorapelido = '" + compradorapelido + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1 = 1 " \
                    "AND seqfornecedor = '" + seqfornecedor + "'"
                fun.process_data(sql_update)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MRL_PRODEMPSEG INNER JOIN MAP_PRODUTO
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("select seqproduto from pbi.carga_produtostatusvenda")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        dfProduto = pd.DataFrame(dados, columns=colunas)
        TotalReg = dfProduto["seqproduto"].count()

        if TotalReg == 0:
            Select_ProdutoStatusVendas \
                = \
                """
                SELECT 
                    DISTINCT 
                    a.seqproduto,
                    REPLACE(b.desccompleta,'''','') desccompleta,
                    REPLACE(b.descreduzida,'''','') descreduzida, 
                    a.statusvenda,
                    MAX(TO_DATE(NVL(a.dtaalteracao,'01-JAN-1994'))) dtaalteracao 
                FROM implantacao.mrl_prodempseg a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                WHERE a.statusvenda = 'A'
                GROUP BY a.seqproduto,b.desccompleta,b.descreduzida,a.statusvenda
                ORDER BY a.seqproduto ASC                
                """
        else:
            Select_ProdutoStatusVendas \
                = \
                f"""
                SELECT 
                    DISTINCT 
                    a.seqproduto,
                    REPLACE(b.desccompleta,'''','') desccompleta,
                    REPLACE(b.descreduzida,'''','') descreduzida, 
                    a.statusvenda,
                    MAX(TO_DATE(NVL(a.dtaalteracao,'01-JAN-1994'))) dtaalteracao 
                FROM implantacao.mrl_prodempseg a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                WHERE a.statusvenda = 'A'
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                GROUP BY a.seqproduto,b.desccompleta,b.descreduzida,a.statusvenda
                ORDER BY a.seqproduto ASC                
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(Select_ProdutoStatusVendas)
        dados = cursor.fetchall()
        colunasDB = [row[0] for row in cursor.description]
        dfProdutos = pd.DataFrame.from_records(dados, columns=colunasDB)

        print("Execultado processo na tabela de \"Cadastro Status Produtos Vendas\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("Execultado processo na tabela de \"Cadastro Status Produtos Vendas\" ...\n")
        Arquivo.close()

        Campos = "(seqproduto,desccompleta,descreduzida,status,dtaalteracao)"

        for index, reg in dfProdutos.iterrows():
            seqproduto = str(reg.SEQPRODUTO)
            desccompleta = reg.DESCCOMPLETA.title()
            descreduzida = reg.DESCREDUZIDA.title()
            status = reg.STATUSVENDA
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"SELECT * FROM pbi.carga_produtostatusvenda WHERE seqproduto = {seqproduto}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            Operacao = len(dados)

            if Operacao == 0:
                Dados = "('" + seqproduto + "','" + desccompleta + "','" + descreduzida + "','" + status + "','" + dtaalteracao + "')"
                sqlInsert \
                    = "insert into pbi.carga_produtostatusvenda\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate \
                    = \
                    "update pbi.carga_produtostatusvenda " \
                    "SET " \
                    "desccompleta = '" + desccompleta + "'," \
                    "descreduzida = '" + descreduzida + "'," \
                    "status = '" + status + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "where 1=1 " \
                    "and seqproduto = '" + seqproduto + "'"
                fun.process_data(sqlupdate)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_PRODUTO INNER JOIN MRL_PRODEMPSEG INNER JOIN MAP_FAMEMBALAGEM
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("SELECT seqproduto FROM pbi.carga_produtoembalagemvenda")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        produto_embalagem_venda = pd.DataFrame(dados, columns=colunas)
        total_reg = produto_embalagem_venda["seqproduto"].count()

        if total_reg == 0:
            select_produto_embalagem_venda \
                = \
                """
                SELECT 
                    DISTINCT
                    a.seqfamilia,
                    a.seqproduto,
                    TRUNC(b.padraoembvenda) AS qtdembalagem,
                    c.embalagem,
                    TO_DATE(a.dtahoralteracao) AS dtaalteracao 
                FROM implantacao.map_produto a
                INNER JOIN implantacao.mad_famsegmento b ON b.seqfamilia = a.seqfamilia AND b.nrosegmento IN (1,3,4,5,6,7,8,9,10) AND b.status = 'A'
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = a.seqfamilia AND c.qtdembalagem = b.padraoembvenda
                WHERE 1 = 1
                ORDER BY 2 ASC                
                """
        else:
            select_produto_embalagem_venda \
                = \
                f"""
                SELECT 
                    DISTINCT
                    a.seqfamilia,
                    a.seqproduto,
                    TRUNC(b.padraoembvenda) AS qtdembalagem,
                    c.embalagem,
                    TO_DATE(a.dtahoralteracao) AS dtaalteracao 
                FROM implantacao.map_produto a
                INNER JOIN implantacao.mad_famsegmento b ON b.seqfamilia = a.seqfamilia AND b.nrosegmento IN (1,3,4,5,6,7,8,9,10) AND b.status = 'A'
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = b.padraoembvenda
                WHERE 1 = 1
                AND TO_DATE(a.dtahoralteracao) >= TO_DATE(SYSDATE - {qtddia})
                ORDER BY 2 ASC               
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_produto_embalagem_venda)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        produto_embalagem_venda = pd.DataFrame.from_records(dados, columns=colunas)

        print("Execultado processo na tabela de \"Cadastro Produtos Embalagens Venda\" ...")
        with open(NomeArquivo, "a") as arquivo:
            arquivo.write("Execultado processo na tabela de \"Cadastro Produtos Embalagens Venda\" ...\n")
        arquivo.close()

        campos = "(seqfamilia,seqproduto,qtdembalagem,embalagem,dtaalteracao)"

        for index, reg in produto_embalagem_venda.iterrows():
            seqfamilia = str(reg.SEQFAMILIA)
            seqproduto = str(reg.SEQPRODUTO)
            qtdembalagem = str(reg.QTDEMBALAGEM)
            embalagem = reg.EMBALAGEM
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"SELECT * FROM pbi.carga_produtoembalagemvenda WHERE seqfamilia = {seqfamilia}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                dados = "('" + seqfamilia + "','" + seqproduto + "','" + qtdembalagem + "','" + embalagem + "','" + dtaalteracao + "')"
                sql_insert \
                    = "insert into pbi.carga_produtoembalagemvenda\n" + campos + "\nvalues\n" + dados + "\n"
                fun.process_data(sql_insert)
            else:
                sql_update \
                    = \
                    "update pbi.carga_produtoembalagemvenda " \
                    "SET " \
                    "qtdembalagem = '" + qtdembalagem + "'," \
                    "embalagem = '" + embalagem + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1 = 1 " \
                    "AND seqfamilia = '" + seqfamilia + "' " \
                    "AND seqproduto = '" + seqproduto + "'"
                fun.process_data(sql_update)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_PRODEMPRSTATUS INNER JOIN MAP_PRODUTO
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("select seqproduto from pbi.carga_produtostatuscompra")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        dfProduto = pd.DataFrame(dados, columns=colunas)
        TotalReg = dfProduto["seqproduto"].count()

        if TotalReg == 0:
            Select_ProdutoStatusCompras \
                = \
                """
                SELECT 
                    DISTINCT 
                    a.seqproduto,
                    REPLACE(b.desccompleta,'''','') desccompleta,
                    REPLACE(b.descreduzida,'''','') descreduzida, 
                    a.statuscompra,
                    MAX(TO_DATE(NVL(a.dtaalteracao,'01-JAN-1994'))) dtaalteracao 
                FROM implantacao.mrl_produtoempresa a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                WHERE a.statuscompra = 'A'
                GROUP BY a.seqproduto,b.desccompleta,b.descreduzida,a.statuscompra
                ORDER BY a.seqproduto ASC                
                """
        else:
            Select_ProdutoStatusCompras \
                = \
                f"""
                SELECT 
                    DISTINCT 
                    a.seqproduto,
                    REPLACE(b.desccompleta,'''','') desccompleta,
                    REPLACE(b.descreduzida,'''','') descreduzida, 
                    a.statuscompra,
                    MAX(TO_DATE(NVL(a.dtaalteracao,'01-JAN-1994'))) dtaalteracao 
                FROM implantacao.mrl_produtoempresa a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                WHERE a.statuscompra = 'A'
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - {qtddia})
                GROUP BY a.seqproduto,b.desccompleta,b.descreduzida,a.statuscompra
                ORDER BY a.seqproduto ASC                
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(Select_ProdutoStatusCompras)
        dados = cursor.fetchall()
        colunasDB = [row[0] for row in cursor.description]
        dfProdutos = pd.DataFrame.from_records(dados, columns=colunasDB)

        print("Execultado processo na tabela de \"Cadastro Status Produtos Compras\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("Execultado processo na tabela de \"Cadastro Status Produtos Compras\" ...\n")
        Arquivo.close()

        Campos = "(seqproduto,desccompleta,descreduzida,status,dtaalteracao)"

        for index, reg in dfProdutos.iterrows():
            seqproduto = str(reg.SEQPRODUTO)
            desccompleta = reg.DESCCOMPLETA.title()
            descreduzida = reg.DESCREDUZIDA.title()
            status = reg.STATUSCOMPRA
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"select seqproduto from pbi.carga_produtostatuscompra where seqproduto = {seqproduto}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            Operacao = len(dados)

            if Operacao == 0:
                Dados = "('" + seqproduto + "','" + desccompleta + "','" + descreduzida + "','" + status + "','" + dtaalteracao + "')"
                sqlInsert \
                    = "insert into pbi.carga_produtostatuscompra\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate \
                    = \
                    "update pbi.carga_produtostatuscompra " \
                    "SET " \
                    "desccompleta = '" + desccompleta + "'," \
                    "descreduzida = '" + descreduzida + "'," \
                    "status = '" + status + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "where 1=1 " \
                    "and seqproduto = '" + seqproduto + "'"
                fun.process_data(sqlupdate)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_PRODUTO INNER JOIN MAP_FAMDIVISAO INNER JOIN MAP_FAMEMBALAGEM
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("SELECT seqproduto FROM pbi.carga_produtoembalagemcompra")
        colunas = [col[0] for col in curpostgre.description]
        dados = curpostgre.fetchall()
        produto_embalagem_compra = pd.DataFrame(dados, columns=colunas)
        total_reg = produto_embalagem_compra["seqproduto"].count()

        if total_reg == 0:
            select_produto_embalagem_compra \
                = \
                """
                SELECT 
                    DISTINCT
                    a.seqfamilia,
                    a.seqproduto,
                    TRUNC(b.padraoembcompra) AS qtdembalagem,
                    c.embalagem,
                    TO_DATE(a.dtahoralteracao) AS dtaalteracao 
                FROM implantacao.map_produto a
                INNER JOIN implantacao.map_famdivisao b ON b.seqfamilia = a.seqfamilia
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = b.padraoembcompra
                WHERE 1 = 1
                ORDER BY 2 ASC                
                """
        else:
            select_produto_embalagem_compra \
                = \
                f"""
                SELECT 
                    DISTINCT
                    a.seqfamilia,
                    a.seqproduto,
                    TRUNC(b.padraoembcompra) AS qtdembalagem,
                    c.embalagem,
                    TO_DATE(a.dtahoralteracao) AS dtaalteracao 
                FROM implantacao.map_produto a
                INNER JOIN implantacao.map_famdivisao b ON b.seqfamilia = a.seqfamilia
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = b.padraoembcompra
                WHERE 1 = 1
                AND TO_DATE(a.dtahoralteracao) >= TO_DATE(SYSDATE - {qtddia})
                ORDER BY 2 ASC               
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_produto_embalagem_compra)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        produto_embalagem_compra = pd.DataFrame.from_records(dados, columns=colunas)

        print("Execultado processo na tabela de \"Cadastro Produtos Embalagens Compra\" ...")
        with open(NomeArquivo, "a") as arquivo:
            arquivo.write("Execultado processo na tabela de \"Cadastro Produtos Embalagens Compra\" ...\n")
        arquivo.close()

        campos = "(seqfamilia,seqproduto,qtdembalagem,embalagem,dtaalteracao)"

        for index, reg in produto_embalagem_compra.iterrows():
            seqfamilia = str(reg.SEQFAMILIA)
            seqproduto = str(reg.SEQPRODUTO)
            qtdembalagem = str(reg.QTDEMBALAGEM)
            embalagem = reg.EMBALAGEM
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"select seqfamilia from pbi.carga_produtoembalagemcompra where seqfamilia = {seqfamilia}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                dados = "('" + seqfamilia + "','" + seqproduto + "','" + qtdembalagem + "','" + embalagem + "','" + dtaalteracao + "')"
                sql_insert \
                    = "insert into pbi.carga_produtoembalagemcompra\n" + campos + "\nvalues\n" + dados + "\n"
                fun.process_data(sql_insert)
            else:
                sql_update \
                    = \
                    "update pbi.carga_produtoembalagemcompra " \
                    "SET " \
                    "qtdembalagem = '" + qtdembalagem + "'," \
                    "embalagem = '" + embalagem + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1 = 1 " \
                    "AND seqfamilia = '" + seqfamilia + "' " \
                    "AND seqproduto = '" + seqproduto + "'"
                fun.process_data(sql_update)

        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA FI_ESPECIE
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("select count(*) quantidade from pbi.carga_tituloespecie")
        dados = curpostgre.fetchall()
        colunas = [col[0] for col in curpostgre.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)
        TotalReg = df["quantidade"][0]

        if TotalReg == 0:
            query = \
                """
                SELECT 
                    seqespecie,
                    codespecie,
                    regexp_replace(descricao, '''', '') descricao,
                    regexp_replace(descreduzida, '''', '') descreduzida,
                    obrigdireito,
                    qtddiasatraso,
                    taxamaxjuros,
                    taxaminjuros,
                    permultaatraso,
                    tipoespecie,
                    observacao 
                FROM implantacao.fi_especie 
                WHERE 1=1
                ORDER BY seqespecie ASC
                """
        else:
            """
            D » direito
            O » obrigação
            """
            query = \
                f"""
                SELECT 
                    seqespecie,
                    codespecie,
                    regexp_replace(descricao, '''', '') descricao,
                    regexp_replace(descreduzida, '''', '') descreduzida,
                    obrigdireito,
                    qtddiasatraso,
                    taxamaxjuros,
                    taxaminjuros,
                    permultaatraso,
                    tipoespecie,
                    observacao 
                FROM implantacao.fi_especie 
                WHERE 1=1
                AND dtaalteracao >= ADD_MONTHS(TRUNC(SYSDATE,'DD'),0) - {qtddia}
                ORDER BY seqespecie ASC
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(query)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)

        print("Execultado processo na tabela de \"Cadastro Especie Titulo\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Especie Titulo\" ...\n")
            Arquivo.close()

        Campos = \
            """
            (
            seqespecie,
            codespecie,
            descricao,
            descreduzida,
            obrigdireito,
            qtddiasatraso,
            taxamaxjuros,
            taxaminjuros,
            permultaatraso,
            tipoespecie,
            observacao
            )
            """

        for index, reg in df.iterrows():
            seqespecie = str(reg.SEQESPECIE)
            codespecie = str(reg.CODESPECIE)
            descricao = str(reg.DESCRICAO)
            descreduzida = str(reg.DESCREDUZIDA)
            obrigdireito = str(reg.OBRIGDIREITO)
            qtddiasatraso = str(reg.QTDDIASATRASO)
            taxamaxjuros = str(reg.TAXAMAXJUROS)
            taxaminjuros = str(reg.TAXAMINJUROS)
            permultaatraso = str(reg.PERMULTAATRASO)
            tipoespecie = str(reg.TIPOESPECIE)
            observacao = str(reg.OBSERVACAO)

            query = \
                f"""
                SELECT * FROM pbi.carga_tituloespecie
                WHERE seqespecie = {seqespecie}
                """
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                Dados = "('" \
                        + seqespecie + "','" \
                        + codespecie + "','" \
                        + descricao + "','" \
                        + descreduzida + "','" \
                        + obrigdireito + "','" \
                        + qtddiasatraso + "','" \
                        + taxamaxjuros + "','" \
                        + taxaminjuros + "','" \
                        + permultaatraso + "','" \
                        + tipoespecie + "','" \
                        + observacao + "')"
                sqlInsert = "INSERT INTO pbi.carga_tituloespecie\n" + Campos + "\nVALUES\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate = "UPDATE pbi.carga_tituloespecie " \
                            "SET " \
                            "codespecie = '" + codespecie + "'," \
                            "descricao = '" + descricao + "'," \
                            "descreduzida = '" + descreduzida + "'," \
                            "obrigdireito = '" + obrigdireito + "'," \
                            "qtddiasatraso = '" + qtddiasatraso + "'," \
                            "taxamaxjuros = '" + taxamaxjuros + "'," \
                            "taxaminjuros = '" + taxaminjuros + "'," \
                            "permultaatraso = '" + permultaatraso + "'," \
                            "tipoespecie = '" + tipoespecie + "'," \
                            "observacao = '" + observacao + "' " \
                            "WHERE 1=1 " \
                            "AND seqespecie = '" + seqespecie + "'"

                fun.process_data(sqlupdate)
        curpostgre.close()
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_PRODUTO
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        curpostgre = pga_conexao.cursor()
        curpostgre.execute("SELECT COUNT(*) quantidade FROM pbi.carga_Produto")
        dados = curpostgre.fetchall()
        colunas = [col[0] for col in curpostgre.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)
        TotalReg = df["quantidade"][0]

        if TotalReg == 0:
            Select_Map_Produto \
                = \
                """
                SELECT
                    a.seqfamilia,
                    NVL(b.seqmarca,0) seqmarca,
                    NVL(d.seqsecao,0) seqsecao,
                    NVL(e.seqcategoria,0) seqcategoria,
                    NVL(f.seqsubcategoria,0) seqsubcategoria,
                    c.seqfornecedor,
                    a.seqproduto,
                    REGEXP_REPLACE(a.desccompleta, '''', '') desccompleta,
                    REGEXP_REPLACE(a.descreduzida, '''', '') descreduzida,
                    NVL(REGEXP_REPLACE(a.complemento, '''', ''),'NAO INFORMADO') complemento,
                    a.indprocfabricacao,
		            g.quantidade as qtdpalete,
		            g.dtavalidade as dtacritica,
                    TO_CHAR(a.dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    NVL(TO_CHAR(a.dtahoralteracao,'dd/mm/yyyy'),TO_CHAR(a.dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao 
                FROM implantacao.map_produto a
                INNER JOIN implantacao.map_familia b 
                ON b.seqfamilia = a.seqfamilia
                INNER JOIN implantacao.map_famfornec c 
                ON c.seqfamilia = b.seqfamilia AND c.principal = 'S'
                LEFT JOIN (select a.seqproduto,c.seqcategoria AS seqsecao
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 1 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') d 
                ON d.seqproduto = a.seqproduto
                LEFT JOIN (select a.seqproduto,c.seqcategoria AS seqcategoria
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia 
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 2 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') e 
                ON e.seqproduto = a.seqproduto
                LEFT JOIN (select a.seqproduto,c.seqcategoria AS seqsubcategoria
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia 
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 3 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') f 
                ON f.seqproduto = a.seqproduto 
                LEFT JOIN (select distinct a.seqproduto, sum(a.qtdatual/a.qtdembalagem) as quantidade, b.dtavalidade
                           from implantacao.mlo_endereco a
                           inner join implantacao.mlo_paleteqtde b on b.seqpaleterf = a.seqpaleterf
                           inner join implantacao.mrl_produtoempresa c on c.nroempresa = a.nroempresa and c.seqproduto = a.seqproduto 
                           where 1=1
                           and b.dtavalidade = (select distinct min(e.dtavalidade)
                                                from implantacao.mlo_endereco d
                                                inner join implantacao.mlo_paleteqtde e on e.seqpaleterf = d.seqpaleterf 
                                                where 1=1
                                                and d.seqproduto = a.seqproduto)
                           and a.seqproduto = a.seqproduto
                           group by a.seqproduto, b.dtavalidade) g 
                ON g.seqproduto = a.seqproduto 
                ORDER BY a.seqproduto ASC 
                """
        else:
            Select_Map_Produto \
                = \
                f"""
                select
                    a.seqfamilia,
                    nvl(b.seqmarca,0) seqmarca,
                    nvl(d.seqsecao,0) seqsecao,
                    nvl(e.seqcategoria,0) seqcategoria,
                    nvl(f.seqsubcategoria,0) seqsubcategoria,
                    c.seqfornecedor,
                    a.seqproduto,
                    regexp_replace(a.desccompleta, '''', '') desccompleta,
                    regexp_replace(a.descreduzida, '''', '') descreduzida,
                    nvl(regexp_replace(a.complemento, '''', ''),'NAO INFORMADO') complemento,
                    a.indprocfabricacao,
		            g.quantidade as qtdpalete,
		            g.dtavalidade as dtacritica,
                    to_char(a.dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    nvl(to_char(a.dtahoralteracao,'dd/mm/yyyy'),to_char(a.dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao 
                from implantacao.map_produto a
                inner join implantacao.map_familia b 
                on b.seqfamilia = a.seqfamilia
                inner join implantacao.map_famfornec c 
                on c.seqfamilia = b.seqfamilia and c.principal = 'S'
                left join (select a.seqproduto,c.seqcategoria AS seqsecao
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 1 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') d 
                on d.seqproduto = a.seqproduto
                left join (select a.seqproduto,c.seqcategoria AS seqcategoria
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia 
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 2 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') e 
                on e.seqproduto = a.seqproduto
                left join (select a.seqproduto,c.seqcategoria AS seqsubcategoria
                           from  implantacao.map_produto a
                           inner join implantacao.map_famdivisao b on b.seqfamilia = a.seqfamilia 
                           inner join implantacao.map_categoria c on c.nrodivisao = b.nrodivisao AND c.nivelhierarquia = 3 AND c.statuscategor = 'A'
                           inner join implantacao.map_famdivcateg d on d.seqfamilia = a.seqfamilia AND  d.seqcategoria = c.seqcategoria AND d.status = 'A') f 
                on f.seqproduto = a.seqproduto 
                left join (select distinct a.seqproduto, sum(a.qtdatual/a.qtdembalagem) as quantidade, b.dtavalidade
                           from implantacao.mlo_endereco a
                           inner join implantacao.mlo_paleteqtde b on b.seqpaleterf = a.seqpaleterf
                           inner join implantacao.mrl_produtoempresa c on c.nroempresa = a.nroempresa and c.seqproduto = a.seqproduto 
                           where 1=1
                           and b.dtavalidade = (select distinct min(e.dtavalidade)
                                                from implantacao.mlo_endereco d
                                                inner join implantacao.mlo_paleteqtde e on e.seqpaleterf = d.seqpaleterf 
                                                where 1=1
                                                and d.seqproduto = a.seqproduto)
                           and a.seqproduto = a.seqproduto
                           group by a.seqproduto, b.dtavalidade) g 
                on g.seqproduto = a.seqproduto 
                where 1=1
                and nvl(a.dtahoralteracao,a.dtahorinclusao) >= TO_DATE(SYSDATE - {qtddia})
                order by a.seqproduto asc
                """

        curoracle = cOracle.cursor()
        curoracle.execute(Select_Map_Produto)
        columns = [Reg[0] for Reg in curoracle.description]
        data = curoracle.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)

        print("Execultado processo na tabela de \"Cadastro Produto\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Produto\" ...\n")
            Arquivo.close()

        for index, reg in df.iterrows():
            seqfamilia = str(reg.SEQFAMILIA)
            seqmarca = str(reg.SEQMARCA)
            seqsecao = str(round(reg.SEQSECAO))
            seqcategoria = str(round(reg.SEQCATEGORIA))
            seqsubcategoria = str(round(reg.SEQSUBCATEGORIA))
            seqfornecedor = str(reg.SEQFORNECEDOR)
            seqproduto = str(reg.SEQPRODUTO)
            descompleta = reg.DESCCOMPLETA.title()
            desreduzida = reg.DESCREDUZIDA.title()
            complemento = reg.COMPLEMENTO.title()
            indprocfabricacao = reg.INDPROCFABRICACAO
            dtahorinclusao = reg.DTAHORINCLUSAO

            query = \
                f"""
                SELECT * FROM pbi.carga_Produto
                WHERE 1=1
                AND seqproduto = {seqproduto}
                """
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            operacao = len(dados)

            if operacao == 0:
                campos \
                    = \
                    """
                    (
                    seqfamilia,
                    seqmarca,
                    seqsecao,
                    seqcategoria,
                    seqsubcategoria,
                    seqfornecedor,
                    seqproduto,
                    descompleta,
                    desreduzida,
                    complemento,
                    indprocfabricacao,
                    dtahorinclusao
                    )
                    """
                values \
                    = \
                    "('" \
                    + seqfamilia + "','" \
                    + seqmarca + "','" \
                    + seqsecao + "','" \
                    + seqcategoria + "','" \
                    + seqsubcategoria + "','" \
                    + seqfornecedor + "','" \
                    + seqproduto + "','" \
                    + descompleta + "','" \
                    + desreduzida + "','" \
                    + complemento + "','" \
                    + indprocfabricacao + "','" \
                    + dtahorinclusao + \
                    "')"
                insert = "insert into pbi.carga_Produto\n" + campos + "\nvalues\n" + values + ""
                fun.process_data(insert)
            else:
                update \
                    = \
                    "update pbi.carga_Produto set " \
                    "seqfamilia         = '" + seqfamilia + "'," \
                    "seqmarca           = '" + seqmarca + "'," \
                    "seqsecao           = '" + seqsecao + "'," \
                    "seqcategoria       = '" + seqcategoria + "'," \
                    "seqsubcategoria    = '" + seqsubcategoria + "'," \
                    "seqfornecedor      = '" + seqfornecedor + "'," \
                    "seqproduto         = '" + seqproduto + "'," \
                    "descompleta        = '" + descompleta + "'," \
                    "desreduzida        = '" + desreduzida + "'," \
                    "complemento        = '" + complemento + "'," \
                    "indprocfabricacao  = '" + indprocfabricacao + "'," \
                    "dtahorinclusao     = '" + dtahorinclusao + "' " \
                    "where 1=1 " \
                    "and seqproduto = " + seqproduto + \
                    ""
                fun.process_data(update)
        curpostgre.close()
        curoracle.close()

    except Exception as erro:
        print("Error ao se conectar no banco de dados")
        print(erro)
    finally:
        if cOracle:
            cOracle.close()
