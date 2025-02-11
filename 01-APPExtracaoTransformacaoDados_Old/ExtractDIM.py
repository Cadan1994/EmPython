import pandas as pd
import datetime as dta
import md002Funcoes as fun
import md001CreateTableScripts as tbl

#DataAtual = dta.datetime.today().date() - dta.timedelta(days=5)
DataAtual = dta.datetime.today().date()

Dia = str(DataAtual.day)
Mes = str(DataAtual.month)
Ano = str(DataAtual.year)
DataAtual = Dia.zfill(2)+"/"+Mes.zfill(2)+"/"+Ano.zfill(4)
NomeArquivo = "LogCarga"+Dia.zfill(2)+Mes.zfill(2)+Ano.zfill(4)+".txt"


def etl_dimensoes():
    ###############################################################################################################
    # SE CONECTAR E BUSCA DE vDados NO BANCO DE vDados CONCINCO "ORACLE"
    ###############################################################################################################
    cOracle = fun.conect_oracle()

    try:
        ###############################################################################################################
        # CRIAS AS TABELAS, CASO NÃO EXISTA
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

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_CATEGORIA INNER JOIN MAX_DIVISAO » SEÇÕES
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        cursor = pga_conexao.cursor()
        cursor.execute("select seqsecao from pbi.carga_secoes")
        colunas = [col[0] for col in cursor.description]
        dados = cursor.fetchall()
        dfsecao = pd.DataFrame(dados, columns=colunas)
        totalreg = dfsecao["seqsecao"].count()

        if totalreg == 0:
            operacao = 1
            select_secoes \
                = \
                """
                SELECT
                    a.seqcategoria,
                    a.categoria,
                    a.statuscategor,
                    TO_DATE(a.datahoraalteracao) datahoraalteracao
                FROM implantacao.map_categoria a
                INNER JOIN implantacao.max_divisao b 
                ON b.nrodivisao = a.nrodivisao
                WHERE 1 = 1
                AND a.nivelhierarquia = 1
                AND a.statuscategor = 'A' 
                AND a.actfamilia = 'N'
                ORDER BY 1 ASC
                """
        else:
            operacao = 2
            select_secoes \
                = \
                """
                SELECT
                    a.seqcategoria,
                    a.categoria,
                    a.statuscategor,
                    TO_DATE(a.datahoraalteracao) datahoraalteracao
                FROM implantacao.map_categoria a
                INNER JOIN implantacao.max_divisao b 
                ON b.nrodivisao = a.nrodivisao
                WHERE 1 = 1
                AND a.nivelhierarquia = 1
                AND a.statuscategor = 'A' 
                AND a.actfamilia = 'N'
                AND a.datahoraalteracao >= TO_DATE(sysdate -5)
                ORDER BY 1 ASC
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_secoes)
        dados = cursor.fetchall()
        colunas = [col[0] for col in cursor.description]
        dfsecao = pd.DataFrame.from_records(dados, columns=colunas)
        totalreg = dfsecao["SEQCATEGORIA"].count()

        print("Execultado processo na tabela de \"Cadastro Seções\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Seções\" ...\n")
        Arquivo.close()

        campos = "(seqsecao,secao,status,dtaalteracao)"

        i = 0
        dados = ""
        parametro = 1000
        if totalreg < 1000:
            parametro = totalreg

        for index, reg in dfsecao.iterrows():
            seqsecao = str(round(reg.SEQCATEGORIA))
            secao = reg.CATEGORIA
            status = reg.STATUSCATEGOR
            dataalteracao = str(reg.DATAHORAALTERACAO)

            if i == 0:
                virgula = ""
            else:
                virgula = ","

            dados = dados + virgula + "('" + seqsecao + "','" + secao + "','" + status + "','" + dataalteracao + "')\n"

            i += 1

            if i == parametro:
                if (operacao == 1):
                    sqlinsert \
                        = "insert into pbi.carga_secoes\n" + campos + "\nvalues\n" + dados + "\n"
                    fun.process_data(sqlinsert)
                else:
                    sqlupdate \
                        = \
                        "update pbi.carga_secoes " \
                        "SET " \
                        "secao = '" + secao + "'," \
                                              "status = '" + status + "'," \
                                                                      "dtaalteracao = '" + dataalteracao + "' " \
                                                                                                           "WHERE 1=1 " \
                                                                                                           "AND seqsecao = '" + seqsecao + "'"
                    fun.process_data(sqlupdate)

                totalreg -= I

                if (totalreg < parametro):
                    parametro = totalreg
                I = 0
                dados = ""
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_CATEGORIA » CATEGORIAS
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        cursor = pga_conexao.cursor()
        cursor.execute("select seqcategoria from pbi.carga_categorias")
        colunas = [col[0] for col in cursor.description]
        dados = cursor.fetchall()
        dfcategorias = pd.DataFrame(dados, columns=colunas)
        totalreg = dfcategorias["seqcategoria"].count()

        if totalreg == 0:
            operacao = 1
            select_categorias \
                = \
                """
                SELECT
                    seqcategoria,
                    seqcategoriapai,
                    categoria,
                    statuscategor,
                    TO_DATE(datahoraalteracao) datahoraalteracao
                FROM implantacao.map_categoria
                WHERE 1 = 1
                AND nivelhierarquia = 2
                AND statuscategor = 'A'
                ORDER BY 1 ASC
                """
        else:
            operacao = 2
            select_categorias \
                = \
                """
                SELECT
                    seqcategoria,
                    seqcategoriapai,
                    categoria,
                    statuscategor,
                    TO_DATE(datahoraalteracao) datahoraalteracao
                FROM implantacao.map_categoria
                WHERE 1 = 1
                AND nivelhierarquia = 2
                AND statuscategor = 'A'
                AND datahoraalteracao >= TO_DATE(sysdate -5)
                ORDER BY 1 ASC
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_categorias)
        dados = cursor.fetchall()
        colunas = [col[0] for col in cursor.description]
        dfcategorias = pd.DataFrame.from_records(dados, columns=colunas)
        totalreg = dfcategorias["SEQCATEGORIA"].count()

        print("Execultado processo na tabela de \"Cadastro Categorias\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Categorias\" ...\n")
        Arquivo.close()

        campos = "(seqcategoria,seqsecao,categoria,status,dtaalteracao)"

        i = 0
        dados = ""
        parametro = 1000
        if totalreg < 1000:
            parametro = totalreg

        for index, reg in dfcategorias.iterrows():
            seqcategoria = str(round(reg.SEQCATEGORIA))
            seqsecao = str(round(reg.SEQCATEGORIAPAI))
            categoria = reg.CATEGORIA
            status = reg.STATUSCATEGOR
            dataalteracao = str(reg.DATAHORAALTERACAO)

            if i == 0:
                virgula = ""
            else:
                virgula = ","

            dados = dados + virgula + "('" + seqcategoria + "','" + seqsecao + "','" + categoria + "','" + status + "','" + dataalteracao + "')\n"

            i += 1

            if i == parametro:
                if operacao == 1:
                    sqlinsert \
                        = "insert into pbi.carga_categorias\n" + campos + "\nvalues\n" + dados + "\n"
                    fun.process_data(sqlinsert)
                else:
                    sqlupdate \
                        = \
                        "update pbi.carga_categorias " \
                        "SET " \
                        "seqsecao = '" + seqsecao + "'," \
                                                    "categoria = '" + categoria + "'," \
                                                                                  "status = '" + status + "'," \
                                                                                                          "dtaalteracao = '" + dataalteracao + "' " \
                                                                                                                                               "WHERE 1=1 " \
                                                                                                                                               "AND seqcategoria = '" + seqcategoria + "'"
                    fun.process_data(sqlupdate)
                totalreg -= I

                if totalreg < parametro:
                    parametro = totalreg
                I = 0
                dados = ""
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAP_CATEGORIA » SUBCATEGORIAS
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        cursor = pga_conexao.cursor()
        cursor.execute("select seqsubcategoria from pbi.carga_subcategorias")
        colunas = [col[0] for col in cursor.description]
        dados = cursor.fetchall()
        dfsubcategorias = pd.DataFrame(dados, columns=colunas)
        totalreg = dfsubcategorias["seqsubcategoria"].count()

        if totalreg == 0:
            operacao = 1
            select_subcategorias \
                = \
                """
                SELECT
                    c.seqcategoria                  AS  seqsubcategoria,
                    a.seqcategoria                  AS	seqsecao,
                    b.seqcategoria                  AS  seqcategoria,
                    c.categoria 				    AS 	subcategoria,
                    a.statuscategor,
                    TO_DATE(c.datahoraalteracao)    AS  datahoraalteracao
                FROM implantacao.map_categoria a
                INNER JOIN implantacao.map_categoria b 
                ON b.seqcategoriapai = a.seqcategoria AND b.nivelhierarquia = 2 AND b.statuscategor = 'A'
                INNER JOIN implantacao.map_categoria c 
                ON c.seqcategoriapai = b.seqcategoria AND c.nivelhierarquia = 3 AND c.statuscategor = 'A'
                WHERE 1 = 1
                AND a.nivelhierarquia = 1
                AND a.statuscategor = 'A'
                ORDER BY 1 ASC
                """
        else:
            operacao = 2
            select_subcategorias \
                = \
                """
                SELECT
                    c.seqcategoria                  AS  seqsubcategoria,
                    a.seqcategoria                  AS	seqsecao,
                    b.seqcategoria                  AS  seqcategoria,
                    c.categoria 				    AS 	subcategoria,
                    a.statuscategor,
                    TO_DATE(c.datahoraalteracao)    AS datahoraalteracao
                FROM implantacao.map_categoria a
                INNER JOIN implantacao.map_categoria b 
                ON b.seqcategoriapai = a.seqcategoria AND b.nivelhierarquia = 2 AND b.statuscategor = 'A'
                INNER JOIN implantacao.map_categoria c 
                ON c.seqcategoriapai = b.seqcategoria AND c.nivelhierarquia = 3 AND c.statuscategor = 'A'
                WHERE 1 = 1
                AND a.nivelhierarquia = 1
                AND a.statuscategor = 'A'
                AND c.datahoraalteracao >= TO_DATE(sysdate -5)
                ORDER BY 1 ASC
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_subcategorias)
        dados = cursor.fetchall()
        colunas = [col[0] for col in cursor.description]
        dfsubcategorias = pd.DataFrame.from_records(dados, columns=colunas)
        totalreg = dfsubcategorias["SEQSUBCATEGORIA"].count()

        print("Execultado processo na tabela de \"Cadastro SubCategorias\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro SubCategorias\" ...\n")
        Arquivo.close()

        campos = "(seqsubcategoria,seqcategoria,seqsecao,subcategoria,status,dtaalteracao)"

        i = 0
        dados = ""
        parametro = 1000
        if totalreg < 1000:
            parametro = totalreg

        for index, reg in dfsubcategorias.iterrows():
            seqsubcategoria = str(round(reg.SEQSUBCATEGORIA))
            seqsecao = str(round(reg.SEQSECAO))
            seqcategoria = str(round(reg.SEQCATEGORIA))
            subcategoria = reg.SUBCATEGORIA
            status = reg.STATUSCATEGOR
            dataalteracao = str(reg.DATAHORAALTERACAO)

            if i == 0:
                virgula = ""
            else:
                virgula = ","

            dados = dados + virgula + "('" + seqsubcategoria + "','" + seqcategoria + "','" + seqsecao + "','" + subcategoria + "','" + status + "','" + dataalteracao + "')\n"

            i += 1

            if i == parametro:
                if operacao == 1:
                    sqlinsert \
                        = "insert into pbi.carga_subcategorias\n" + campos + "\nvalues\n" + dados + "\n"
                    fun.process_data(sqlinsert)
                else:
                    sqlupdate \
                        = \
                        "update pbi.carga_subcategorias " \
                        "SET " \
                        "seqcategoria = '" + seqcategoria + "'," \
                                                            "seqsecao = '" + seqsecao + "'," \
                                                                                        "subcategoria = '" + subcategoria + "'," \
                                                                                                                            "status = '" + status + "'," \
                                                                                                                                                    "dtaalteracao = '" + dataalteracao + "' " \
                                                                                                                                                                                         "WHERE 1=1 " \
                                                                                                                                                                                         "AND seqsubcategoria = '" + seqsubcategoria + "'"
                    fun.process_data(sqlupdate)

                totalreg -= I

                if totalreg < parametro:
                    parametro = totalreg
                I = 0
                dados = ""
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA GE_PESSOACADASTRO
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_pessoacadastro"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_GePessoaCadastro \
                = \
                """
                select
                    seqpessoa,
                    situacaocredito,
                    nvl(limitecredito,0) limitecredito,
                    nvl(regexp_replace(obscredito, '''', ''),'NAO INFORMADA') obscredito,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),'01/01/1994') dtaalteracao
                from implantacao.ge_pessoacadastro
                nolock
                where 1=1
                order by seqpessoa asc
                """
        else:
            Select_GePessoaCadastro \
                = \
                """
                select
                    seqpessoa,
                    situacaocredito,
                    nvl(limitecredito,0) limitecredito,
                    nvl(regexp_replace(obscredito, '''', ''),'NAO INFORMADA') obscredito,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),'01/01/1994') dtaalteracao
                from implantacao.ge_pessoacadastro
                nolock
                where 1=1
                and dtaalteracao >= to_DATE(sysdate - 5)
                order by seqpessoa asc
                """

        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_GePessoaCadastro)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["SEQPESSOA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Situação\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Situação\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Pessoa Cadastro Situação\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Pessoa Cadastro Situação\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            seqpessoa = str(reg.SEQPESSOA)
            situacaocredito = reg.SITUACAOCREDITO
            limitecredito = str(reg.LIMITECREDITO)
            obscredito = reg.OBSCREDITO.title()

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_pessoacadastro " \
                "where 1=1 " \
                "and seqpessoa = " + seqpessoa + ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    seqpessoa,
                    situacaocredito,
                    limitecredito,
                    obscredito
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + seqpessoa + "','" \
                    + situacaocredito + "','" \
                    + limitecredito + "','" \
                    + obscredito + \
                    "')"
                vInsert = "insert into pbi.carga_pessoacadastro\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_pessoacadastro set \n" \
                    "situacaocredito = '" + situacaocredito + "'," \
                                                              "limitecredito = '" + limitecredito + "'," \
                                                                                                    "obscredito = '" + obscredito + "' " \
                                                                                                                                    "where 1=1 " \
                                                                                                                                    "and seqpessoa = " + seqpessoa + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MRL_CLIENTE
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_Cliente"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MrlCliente \
                = \
                """
                select
                    nroempresa,
                    seqpessoa,
                    nvl(pzopagtomaximo,0) pzopagtomaximo,
                    nvl(regexp_replace(observacao, '''', ''),'NAO INFORMADA') observacao,
                    to_char(dtacadastro,'dd/mm/yyyy') dtacadastro,
                    nvl(to_char(dtaativou,'dd/mm/yyyy'),'01/01/1899') dtaativou,
                    nvl(to_char(dtainativou,'dd/mm/yyyy'),'01/01/1899') dtainativou,
                    nvl(to_char(dtaultcompra,'dd/mm/yyyy'),'01/01/1899') dtaultcompra,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(dtacadastro,'dd/mm/yyyy')) dtaalteracao,
                    statuscliente
                from implantacao.mrl_cliente
                nolock
                where 1=1
                order by seqpessoa asc
                """
        else:
            Select_MrlCliente \
                = \
                """
                select
                    nroempresa,
                    seqpessoa,
                    nvl(pzopagtomaximo,0) pzopagtomaximo,
                    nvl(regexp_replace(observacao, '''', ''),'NAO INFORMADA') observacao,
                    to_char(dtacadastro,'dd/mm/yyyy') dtacadastro,
                    nvl(to_char(dtaativou,'dd/mm/yyyy'),'01/01/1899') dtaativou,
                    nvl(to_char(dtainativou,'dd/mm/yyyy'),'01/01/1899') dtainativou,
                    nvl(to_char(dtaultcompra,'dd/mm/yyyy'),'01/01/1899') dtaultcompra,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(dtacadastro,'dd/mm/yyyy')) dtaalteracao,
                    statuscliente
                from implantacao.mrl_cliente
                nolock
                where 1=1
                and nvl(dtaalteracao,dtacadastro) >= to_DATE(sysdate -5)
                order by seqpessoa asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MrlCliente)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vDFrame = vDFrame.astype({"PZOPAGTOMAXIMO": int})
        vTReg = vDFrame["SEQPESSOA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Cliente\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Cliente\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Cliente\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Cliente\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            nroempresa = str(reg.NROEMPRESA)
            seqpessoa = str(reg.SEQPESSOA)
            prazopagamentos = str(reg.PZOPAGTOMAXIMO)
            observacao = reg.OBSERVACAO.title()
            dtacadastro = reg.DTACADASTRO
            dtaativou = reg.DTAATIVOU
            dtainativou = reg.DTAINATIVOU
            dtaultcompra = reg.DTAULTCOMPRA
            statuscliente = reg.STATUSCLIENTE

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_cliente " \
                "where 1=1 " \
                "and nroempresa = " + nroempresa + " " \
                                                   "and seqpessoa = " + seqpessoa + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    nroempresa,
                    seqpessoa,
                    pzopagtomaximo,
                    observacao,
                    dtacadastro,
                    dtaativou,
                    dtainativou,
                    dtaultcompra,
                    statuscliente
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + nroempresa + "','" \
                    + seqpessoa + "','" \
                    + prazopagamentos + "','" \
                    + observacao + "','" \
                    + dtacadastro + "','" \
                    + dtaativou + "','" \
                    + dtainativou + "','" \
                    + dtaultcompra + "','" \
                    + statuscliente + \
                    "')"
                vInsert = "insert into pbi.carga_cliente\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_cliente set " \
                    "pzopagtomaximo = '" + prazopagamentos + "'," \
                                                             "observacao = '" + observacao + "'," \
                                                                                             "dtaativou = '" + dtaativou + "'," \
                                                                                                                           "dtainativou = '" + dtainativou + "'," \
                                                                                                                                                             "dtaultcompra = '" + dtaultcompra + "'," \
                                                                                                                                                                                                 "statuscliente = '" + statuscliente + "' " \
                                                                                                                                                                                                                                       "where 1=1 " \
                                                                                                                                                                                                                                       "and nroempresa = " + nroempresa + " " \
                                                                                                                                                                                                                                                                          "and seqpessoa = " + seqpessoa + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_REPRESENTANTE
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_Representante"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MadRepresentante \
                = \
                """
                select
                    nrorepresentante,
                    nroempresa,
                    seqpessoa,
                    nrosegmento,
                    nroequipe,
                    nvl(apelido,'NAO INFORMADO') apelido,
                    tiprepresentante,
                    nvl(indcomissao,' ') indcomissao,
                    to_char(dtaalteracao,'dd/mm/yyyy') dtaalteracao,
                    status
                from implantacao.mad_representante
                nolock
                where 1=1
                order by nrorepresentante asc
                """
        else:
            Select_MadRepresentante \
                = \
                """
                select
                    nrorepresentante,
                    nroempresa,
                    seqpessoa,
                    nrosegmento,
                    nroequipe,
                    nvl(apelido,'NAO INFORMADO') apelido,
                    tiprepresentante,
                    nvl(indcomissao,' ') indcomissao,
                    to_char(dtaalteracao,'dd/mm/yyyy') dtaalteracao,
                    status
                from implantacao.mad_representante
                nolock
                where 1=1
                and dtaalteracao >= to_DATE(sysdate -5)
                order by nrorepresentante asc
                """

        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MadRepresentante)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["NROREPRESENTANTE"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Representante\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Representante\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Representante\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Representante\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            nrorepresentante = str(reg.NROREPRESENTANTE)
            nroempresa = str(reg.NROEMPRESA)
            seqpessoa = str(reg.SEQPESSOA)
            nrosegmento = str(reg.NROSEGMENTO)
            nroequipe = str(reg.NROEQUIPE)
            apelido = reg.APELIDO.title()
            tiprepresentante = reg.TIPREPRESENTANTE
            indcomissao = reg.INDCOMISSAO
            status = reg.STATUS

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_representante " \
                "where 1=1 " \
                "and nrorepresentante = " + nrorepresentante + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    nrorepresentante,
                    nroempresa,
                    seqpessoa,
                    nrosegmento,
                    nroequipe,
                    apelido,
                    tiprepresentante,
                    indcomissao,
                    status
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + nrorepresentante + "','" \
                    + nroempresa + "','" \
                    + seqpessoa + "','" \
                    + nrosegmento + "','" \
                    + nroequipe + "','" \
                    + apelido + "','" \
                    + tiprepresentante + "','" \
                    + indcomissao + "','" \
                    + status + \
                    "')"
                vInsert = "insert into pbi.carga_representante\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_representante set " \
                    "nroempresa = '" + nroempresa + "'," \
                                                    "seqpessoa = '" + seqpessoa + "'," \
                                                                                  "nrosegmento = '" + nrosegmento + "'," \
                                                                                                                    "nroequipe = '" + nroequipe + "'," \
                                                                                                                                                  "apelido = '" + apelido + "'," \
                                                                                                                                                                            "tiprepresentante = '" + tiprepresentante + "'," \
                                                                                                                                                                                                                        "indcomissao = '" + indcomissao + "'," \
                                                                                                                                                                                                                                                          "status = '" + status + "' " \
                                                                                                                                                                                                                                                                                  "where 1=1 " \
                                                                                                                                                                                                                                                                                  "and nrorepresentante = " + nrorepresentante + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_CLIENTEREP
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select seqpessoa, nrorepresentante from pbi.carga_clienterepresentante order by seqpessoa asc"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["seqpessoa"].count()

        for index, reg in vDFPostgres.iterrows():
            seqpessoa = str(reg.seqpessoa)
            nrorepresentante = str(reg.nrorepresentante)

            Select_MadClienteRepresentante \
                = \
                "select count(*) quantidade " \
                "from implantacao.mad_clienterep " \
                "nolock " \
                "where 1=1 " \
                "and seqpessoa = " + seqpessoa + " " \
                                                 "and nrorepresentante = " + nrorepresentante + "" \
                                                                                                ""

            vCur = cOracle.cursor()
            vCur.execute(Select_MadClienteRepresentante)
            vCol = [Reg[0] for Reg in vCur.description]
            vLin = vCur.fetchall()
            vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
            Quantidade = vDFrame["QUANTIDADE"].values

            if (Quantidade == 0):
                vQuery = \
                    "delete " \
                    "from pbi.carga_clienterepresentante " \
                    "where 1=1 " \
                    "and seqpessoa = " + seqpessoa + " " \
                                                     "and nrorepresentante = " + nrorepresentante + \
                    ""
                fun.process_data(vQuery)

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MadClienteRepresentante \
                = \
                """
                select
                    seqpessoa,
                    nrorepresentante,
                    to_char(dtainclusao,'dd/mm/yyyy') dtainclusao,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(dtainclusao,'dd/mm/yyyy')) dtaalteracao,
                    status
                from implantacao.mad_clienterep
                nolock
                where 1=1
                order by seqpessoa asc
                """
        else:
            Select_MadClienteRepresentante \
                = \
                """
                select
                    seqpessoa,
                    nrorepresentante,
                    to_char(dtainclusao,'dd/mm/yyyy') dtainclusao,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(dtainclusao,'dd/mm/yyyy')) dtaalteracao,
                    status
                from implantacao.mad_clienterep
                nolock
                where 1=1
                and nvl(dtaalteracao,dtainclusao) >= to_DATE(sysdate -5)
                order by seqpessoa asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MadClienteRepresentante)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["SEQPESSOA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Cliente/Representante\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Cliente/Representante\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Cliente/Representante\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Cliente/Representante\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            seqpessoa = str(reg.SEQPESSOA)
            nrorepresentante = str(reg.NROREPRESENTANTE)
            dtainclusao = reg.DTAINCLUSAO
            status = reg.STATUS

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_ClienteRepresentante " \
                "where 1=1 " \
                "and seqpessoa = " + seqpessoa + " " \
                                                 "and nrorepresentante = " + nrorepresentante + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    seqpessoa,
                    nrorepresentante,
                    dtainclusao,
                    status
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + seqpessoa + "','" \
                    + nrorepresentante + "','" \
                    + dtainclusao + "','" \
                    + status + \
                    "')"
                vInsert = "insert into pbi.carga_ClienteRepresentante\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_ClienteRepresentante set " \
                    "seqpessoa = '" + seqpessoa + "'," \
                                                  "nrorepresentante = '" + nrorepresentante + "'," \
                                                                                              "dtainclusao = '" + dtainclusao + "'," \
                                                                                                                                "status = '" + status + "' " \
                                                                                                                                                        "where 1=1 " \
                                                                                                                                                        "and seqpessoa = " + seqpessoa + " " \
                                                                                                                                                                                         "and nrorepresentante = " + nrorepresentante + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_FAMDIVISAO
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_FamiliaDivisao"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MapFamiliaDivisao \
                = \
                """
                select
                    nrodivisao,
                    seqfamilia,
                    seqcomprador,
                    nrotributacao,
                    finalidadefamilia,
                    formaabastecimento,
                    to_char(dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    nvl(to_char(dtahoralteracao,'dd/mm/yyyy'),to_char(dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao
                from implantacao.map_famdivisao
                nolock
                where 1=1
                order by nrodivisao asc
                """
        else:
            Select_MapFamiliaDivisao \
                = \
                """
                select
                    nrodivisao,
                    seqfamilia,
                    seqcomprador,
                    nrotributacao,
                    finalidadefamilia,
                    formaabastecimento,
                    to_char(dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    nvl(to_char(dtahoralteracao,'dd/mm/yyyy'),to_char(dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao
                from implantacao.map_famdivisao
                nolock
                where 1=1
                and nvl(dtahoralteracao,dtahorinclusao) >= to_DATE(sysdate -5)
                order by nrodivisao asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MapFamiliaDivisao)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["SEQFAMILIA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Divisão Família\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Divisão Família\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Divisão Família\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Divisão Família\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            nrodivisao = str(reg.NRODIVISAO)
            seqfamilia = str(reg.SEQFAMILIA)
            seqcomprador = str(reg.SEQCOMPRADOR)
            nrotributacao = str(reg.NROTRIBUTACAO)
            finalidadefamilia = reg.FINALIDADEFAMILIA
            formaabastecimento = reg.FORMAABASTECIMENTO
            dtahorinclusao = reg.DTAHORINCLUSAO

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_FamiliaDivisao " \
                "where 1=1 " \
                "and nrodivisao = " + nrodivisao + " " \
                                                   "and seqfamilia = " + seqfamilia + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    nrodivisao,
                    seqfamilia,
                    seqcomprador,
                    nrotributacao,
                    finalidadefamilia,
                    formaabastecimento,
                    dtahorinclusao
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + nrodivisao + "','" \
                    + seqfamilia + "','" \
                    + seqcomprador + "','" \
                    + nrotributacao + "','" \
                    + finalidadefamilia + "','" \
                    + formaabastecimento + "','" \
                    + dtahorinclusao + \
                    "')"
                vInsert = "insert into pbi.carga_FamiliaDivisao\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_FamiliaDivisao set " \
                    "seqcomprador = '" + seqcomprador + "'," \
                                                        "nrotributacao = '" + nrotributacao + "'," \
                                                                                              "finalidadefamilia = '" + finalidadefamilia + "'," \
                                                                                                                                            "formaabastecimento = '" + formaabastecimento + "'," \
                                                                                                                                                                                            "dtahorinclusao = '" + dtahorinclusao + "' " \
                                                                                                                                                                                                                                    "where 1=1 " \
                                                                                                                                                                                                                                    "and nrodivisao = " + nrodivisao + " " \
                                                                                                                                                                                                                                                                       "and seqfamilia = " + seqfamilia + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_FAMFORNEC
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select seqfamilia, seqfornecedor from pbi.carga_FamiliaFornecedor"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["seqfamilia"].count()

        for index, reg in vDFPostgres.iterrows():
            seqfamilia = str(reg.seqfamilia)
            seqfornecedor = str(reg.seqfornecedor)

            Select_MadFamiliaFornecedor \
                = \
                "select count(*) quantidade " \
                "from implantacao.map_famfornec " \
                "nolock " \
                "where 1=1 " \
                "and seqfamilia = " + seqfamilia + " " \
                                                   "and seqfornecedor = " + seqfornecedor + "" \
                                                                                            ""
            vCur = cOracle.cursor()
            vCur.execute(Select_MadFamiliaFornecedor)
            vCol = [Reg[0] for Reg in vCur.description]
            vLin = vCur.fetchall()
            vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
            Quantidade = vDFrame["QUANTIDADE"].values

            if (Quantidade == 0):
                vQuery = \
                    "delete " \
                    "from pbi.carga_FamiliaFornecedor " \
                    "where 1=1 " \
                    "and seqfamilia = " + seqfamilia + " " \
                                                       "and seqfornecedor = " + seqfornecedor + \
                    ""
                fun.process_data(vQuery)

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MapFamiliaFornecedor \
                = \
                """
                select
                    seqfamilia,
                    seqfornecedor,
                    principal,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) datahoraalteracao
                from implantacao.map_famfornec
                nolock
                where 1=1
                order by seqfamilia asc
                """
        else:
            Select_MapFamiliaFornecedor \
                = \
                """
                select
                    seqfamilia,
                    seqfornecedor,
                    principal,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) datahoraalteracao
                from implantacao.map_famfornec
                nolock
                where 1=1
                and nvl(datahoraalteracao,add_months(trunc(sysdate,'yyyy'),-24)) >= to_DATE(sysdate -5)
                order by seqfamilia asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MapFamiliaFornecedor)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["SEQFAMILIA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Família/Fornecedor\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Família/Fornecedor\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Família/Fornecedor\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Família/Fornecedor\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            seqfamilia = str(reg.SEQFAMILIA)
            seqfornecedor = str(reg.SEQFORNECEDOR)
            principal = reg.PRINCIPAL

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_FamiliaFornecedor " \
                "where 1=1 " \
                "and seqfamilia = " + seqfamilia + " " \
                                                   "and seqfornecedor = " + seqfornecedor + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    seqfamilia,
                    seqfornecedor,
                    principal
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + seqfamilia + "','" \
                    + seqfornecedor + "','" \
                    + principal + \
                    "')"
                vInsert = "insert into pbi.carga_FamiliaFornecedor\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_FamiliaFornecedor set " \
                    "seqfamilia     = '" + seqfamilia + "'," \
                                                        "seqfornecedor  = '" + seqfornecedor + "'," \
                                                                                               "principal      = '" + principal + "' " \
                                                                                                                                  "where 1=1 " \
                                                                                                                                  "and seqfamilia = " + seqfamilia + " " \
                                                                                                                                                                     "and seqfornecedor = " + seqfornecedor + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAX_CODGERALOPER
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_CodigoGeralOperacao"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MaxCodigoGeralOperacao \
                = \
                """
                select
                    codgeraloper,
                    tipcgo,
                    tipuso,
                    nvl(tipdocfiscal,'*') tipdocfiscal,
                    case
                        when tipdocfiscal = 'A' then 'AVARIAS'
                        when tipdocfiscal = 'C' then 'COMPRAS/VENDAS'
                        when tipdocfiscal = 'D' then 'DEVOLUCOES'
                        when tipdocfiscal = 'F' then 'ENTREGAS FUTURAS'
                        when tipdocfiscal = 'O' then 'OUTRAS ENTRADAS/SAIDAS'
                        when tipdocfiscal = 'P' then 'MERC. PODER TERCEIROS'
                        when tipdocfiscal = 'S' then 'DESPESAS'
                        when tipdocfiscal = 'T' then 'TRANSFERENCIAS P/COMERC.'
                        else 'NAO INFORMADO'
                    end
                    tipdocfiscaldesc,
                    descricao,
                    acmcompravenda,
                    geralteracaoestq,
                    nvl(indtipocgomovto,'*') indtipocgomovto,
                    case
                        when indtipocgomovto = 'C'  then 'COMPRAS'
                        when indtipocgomovto = 'TE' then 'TRANSFERENCIAS ENTRADAS'
                        when indtipocgomovto = 'AE' then 'AJUSTES ENTRADAS'
                        when indtipocgomovto = 'IE' then 'AJUSTES INVENTARIOS ENTRADAS'
                        when indtipocgomovto = 'BE' then 'BONIFICACOES'
                        when indtipocgomovto = 'V'  then 'VENDAS'
                        when indtipocgomovto = 'TS' then 'TRANSFERENCIAS SAIDAS'
                        when indtipocgomovto = 'AS' then 'AJUSTES SAIDAS'
                        when indtipocgomovto = 'IS' then 'AJUSTES INVENTARIOS SAIDAS'
                        when indtipocgomovto = 'TD' then 'TROCAS/DESCARTES'
                        else 'NAO INFORMADO'
                    end
                    indtipocgomovtodesc,
                    nvl(indgeradebcredpis,'*') indgeradebcredpis,
                    nvl(indgeradebcredcofins,'*') indgeradebcredcofins,
                    status,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) dtaalteracao
                from implantacao.max_codgeraloper
                nolock
                where 1=1
                order by codgeraloper asc
                """
        else:
            Select_MaxCodigoGeralOperacao \
                = \
                """
                select
                    codgeraloper,
                    tipcgo,
                    tipuso,
                    nvl(tipdocfiscal,'*') tipdocfiscal,
                    case
                        when tipdocfiscal = 'A' then 'AVARIAS'
                        when tipdocfiscal = 'C' then 'COMPRAS/VENDAS'
                        when tipdocfiscal = 'D' then 'DEVOLUCOES'
                        when tipdocfiscal = 'F' then 'ENTREGAS FUTURAS'
                        when tipdocfiscal = 'O' then 'OUTRAS ENTRADAS/SAIDAS'
                        when tipdocfiscal = 'P' then 'MERC. PODER TERCEIROS'
                        when tipdocfiscal = 'S' then 'DESPESAS'
                        when tipdocfiscal = 'T' then 'TRANSFERENCIAS P/COMERC.'
                        else 'NAO INFORMADO'
                    end
                    tipdocfiscaldesc,
                    descricao,
                    acmcompravenda,
                    geralteracaoestq,
                    nvl(indtipocgomovto,'*') indtipocgomovto,
                    case
                        when indtipocgomovto = 'C'  then 'COMPRAS'
                        when indtipocgomovto = 'TE' then 'TRANSFERENCIAS ENTRADAS'
                        when indtipocgomovto = 'AE' then 'AJUSTES ENTRADAS'
                        when indtipocgomovto = 'IE' then 'AJUSTES INVENTARIOS ENTRADAS'
                        when indtipocgomovto = 'BE' then 'BONIFICACOES'
                        when indtipocgomovto = 'V'  then 'VENDAS'
                        when indtipocgomovto = 'TS' then 'TRANSFERENCIAS SAIDAS'
                        when indtipocgomovto = 'AS' then 'AJUSTES SAIDAS'
                        when indtipocgomovto = 'IS' then 'AJUSTES INVENTARIOS SAIDAS'
                        when indtipocgomovto = 'TD' then 'TROCAS/DESCARTES'
                        else 'NAO INFORMADO'
                    end
                    indtipocgomovtodesc,
                    nvl(indgeradebcredpis,'*') indgeradebcredpis,
                    nvl(indgeradebcredcofins,'*') indgeradebcredcofins,
                    status,
                    nvl(to_char(dtaalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) dtaalteracao
                from implantacao.max_codgeraloper
                nolock
                where 1=1
                and dtaalteracao >= to_DATE(sysdate -5)
                order by codgeraloper asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MaxCodigoGeralOperacao)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["CODGERALOPER"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Código Geral Operação\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Código Geral Operação\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Código Geral Operação\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Código Geral Operação\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            codgeraloper = str(reg.CODGERALOPER)
            tipcgo = reg.TIPCGO
            tipuso = reg.TIPUSO
            tipdocfiscal = reg.TIPDOCFISCAL
            tipdocfiscaldesc = reg.TIPDOCFISCALDESC.title()
            descricao = reg.DESCRICAO.title()
            acmcompravenda = reg.ACMCOMPRAVENDA
            geralteracaoestq = reg.GERALTERACAOESTQ
            indtipocgomovto = reg.INDTIPOCGOMOVTO
            indtipocgomovtodesc = reg.INDTIPOCGOMOVTODESC.title()
            indgeradebcredpis = reg.INDGERADEBCREDPIS
            indgeradebcredcofins = reg.INDGERADEBCREDCOFINS
            status = reg.STATUS

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_CodigoGeralOperacao " \
                "where 1=1 " \
                "and codgeraloper = " + codgeraloper + \
                ""
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    codgeraloper,
                    tipcgo,
                    tipuso,
                    tipdocfiscal,
                    tipdocfiscaldesc,
                    descricao,
                    acmcompravenda,
                    geralteracaoestq,
                    indtipocgomovto,
                    indtipocgomovtodesc,
                    indgeradebcredpis,
                    indgeradebcredcofins,
                    status
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + codgeraloper + "','" \
                    + tipcgo + "','" \
                    + tipuso + "','" \
                    + tipdocfiscal + "','" \
                    + tipdocfiscaldesc + "','" \
                    + descricao + "','" \
                    + acmcompravenda + "','" \
                    + geralteracaoestq + "','" \
                    + indtipocgomovto + "','" \
                    + indtipocgomovtodesc + "','" \
                    + indgeradebcredpis + "','" \
                    + indgeradebcredcofins + "','" \
                    + status + \
                    "')"
                vInsert = "insert into pbi.carga_CodigoGeralOperacao\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_CodigoGeralOperacao set " \
                    "tipcgo                 = '" + tipcgo + "'," \
                                                            "tipuso                 = '" + tipuso + "'," \
                                                                                                    "tipdocfiscal           = '" + tipdocfiscal + "'," \
                                                                                                                                                  "tipdocfiscaldesc       = '" + tipdocfiscaldesc + "'," \
                                                                                                                                                                                                    "descricao              = '" + descricao + "'," \
                                                                                                                                                                                                                                               "acmcompravenda         = '" + acmcompravenda + "'," \
                                                                                                                                                                                                                                                                                               "indtipocgomovto        = '" + indtipocgomovto + "'," \
                                                                                                                                                                                                                                                                                                                                                "indtipocgomovtodesc    = '" + indtipocgomovtodesc + "'," \
                                                                                                                                                                                                                                                                                                                                                                                                     "indgeradebcredpis      = '" + indgeradebcredpis + "'," \
                                                                                                                                                                                                                                                                                                                                                                                                                                                        "indgeradebcredcofins   = '" + indgeradebcredcofins + "'," \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              "status                 = '" + status + "' " \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "where 1=1 " \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "and codgeraloper = " + codgeraloper + \
                    ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_EQUIPE
        ###############################################################################################################
        fun.process_data("delete from pbi.carga_Equipe")

        Select_MadEquipe \
            = \
            """
            select
                nroequipe,
                nroempresa,
                nvl(seqpessoa,0) seqpessoa,
                nvl(nroregiao,0) nroregiao,
                descequipe,
                status
            from implantacao.mad_equipe
            nolock
            where 1=1
            order by nroequipe asc
            """

        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MadEquipe)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["NROEQUIPE"].count()

        print("Execultado processo na tabela de \"Cadastro Equipe\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Equipe\" ...\n")
            Arquivo.close()

        for index, reg in vDFrame.iterrows():
            nroequipe = str(reg.NROEQUIPE)
            nroempresa = str(reg.NROEMPRESA)
            seqpessoa = str(reg.SEQPESSOA)
            nroregiao = str(reg.NROREGIAO)
            desequipe = reg.DESCEQUIPE.title()
            status = reg.STATUS

            I += 1
            vCampos \
                = \
                """
                (
                nroequipe,
                nroempresa,
                seqpessoa,
                nroregiao,
                desequipe,
                status
                )
                """
            vValues \
                = \
                "('" \
                + nroequipe + "','" \
                + nroempresa + "','" \
                + seqpessoa + "','" \
                + nroregiao + "','" \
                + desequipe + "','" \
                + status + \
                "')"
            vInsert = "insert into pbi.carga_Equipe\n" + vCampos + "\nvalues\n" + vValues + ""
            fun.process_data(vInsert)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_FAMEMBALAGEM
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_familiaembalagem"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_Map_FamEmbalagem \
                = \
                """
                select
                    seqfamilia,
                    embalagem,
                    qtdembalagem,
                    nvl(qtdunidemb,0) qtdunidemb,
                    pesobruto,
                    pesoliquido,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) datahoraalteracao,
                    status
                from implantacao.map_famembalagem
                nolock
                where 1=1
                order by seqfamilia asc
                """
        else:
            Select_Map_FamEmbalagem \
                = \
                """
                select
                    seqfamilia,
                    embalagem,
                    qtdembalagem,
                    nvl(qtdunidemb,0) qtdunidemb,
                    pesobruto,
                    pesoliquido,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')) datahoraalteracao,
                    status
                from implantacao.map_famembalagem
                nolock
                where 1=1
                and nvl(datahoraalteracao,add_months(trunc(sysdate,'yyyy'),-24)) >= to_DATE(sysdate -5)
                order by seqfamilia asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_Map_FamEmbalagem)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vListaInt = ["QTDEMBALAGEM", "QTDUNIDEMB"]

        for idx in vListaInt:
            vDFrame = vDFrame.astype({"" + idx + "": int})

        vTReg = vDFrame["SEQFAMILIA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Família/Embalagem\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Família/Embalagem\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Família/Embalagem\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Família/Embalagem\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            seqfamilia = str(reg.SEQFAMILIA)
            embalagem = reg.EMBALAGEM
            qtdembalagem = str(reg.QTDEMBALAGEM)
            qtdunidemb = str(reg.QTDUNIDEMB)
            pesobruto = str(reg.PESOBRUTO)
            pesoliquido = str(reg.PESOLIQUIDO)

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_familiaembalagem " \
                "where 1=1 " \
                "and seqfamilia = '" + seqfamilia + "' " \
                                                    "and qtdembalagem = '" + qtdembalagem + "'"
            # "and embalagem = '"+embalagem+"'"
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    seqfamilia,
                    embalagem,
                    qtdembalagem,
                    qtdunidemb,
                    pesobruto,
                    pesoliquido
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + seqfamilia + "','" \
                    + embalagem + "','" \
                    + qtdembalagem + "','" \
                    + qtdunidemb + "','" \
                    + pesobruto + "','" \
                    + pesoliquido + \
                    "')"
                vInsert = "insert into pbi.carga_familiaembalagem\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_familiaembalagem set " \
                    "seqfamilia   = '" + seqfamilia + "'," \
                                                      "embalagem    = '" + embalagem + "'," \
                                                                                       "qtdembalagem = '" + qtdembalagem + "'," \
                                                                                                                           "qtdunidemb   = '" + qtdunidemb + "'," \
                                                                                                                                                             "pesobruto    = '" + pesobruto + "'," \
                                                                                                                                                                                              "pesoliquido  = '" + pesoliquido + "' " \
                                                                                                                                                                                                                                 "where 1=1 " \
                                                                                                                                                                                                                                 "and seqfamilia = '" + seqfamilia + "' " \
                                                                                                                                                                                                                                                                     "and qtdembalagem = '" + qtdembalagem + "'"
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_SEGMENTO
        ###############################################################################################################
        vPrimeiraCarga = True
        vQuery = "select count(*) quantidade from pbi.carga_segmento"
        vDFPostgres = fun.postgres_consulta(vQuery)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_Mad_Segmento \
                = \
                """
                select
                    nrosegmento,
                    nrodivisao,
                    descsegmento,
                    indprecoembalagem,
                    nvl(
                        to_char(datahoraalteracao,'dd/mm/yyyy'),
                        to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')
                    ) 
                    datahoraalteracao,
                    status
                from implantacao.mad_segmento
                nolock
                where 1=1
                order by nrosegmento asc
                """
        else:
            Select_Mad_Segmento \
                = \
                """
                select
                    nrosegmento,
                    nrodivisao,
                    descsegmento,
                    indprecoembalagem,
                    nvl(
                        to_char(datahoraalteracao,'dd/mm/yyyy'),
                        to_char(add_months(trunc(sysdate,'yyyy'),-24),'dd/mm/yyyy')
                    ) 
                    datahoraalteracao,
                    status
                from implantacao.mad_segmento
                nolock
                where 1=1
                and nvl(datahoraalteracao,add_months(trunc(sysdate,'yyyy'),-24)) >= to_DATE(sysdate -5)
                order by nrosegmento asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_Mad_Segmento)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["NROSEGMENTO"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Segmento\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Segmento\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Segmento\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Segmento\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            nrosegmento = str(reg.NROSEGMENTO)
            nrodivisao = str(reg.NRODIVISAO)
            descsegmento = reg.DESCSEGMENTO.title()
            indprecoembalagem = reg.INDPRECOEMBALAGEM
            status = reg.STATUS

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_segmento " \
                "where 1=1 " \
                "and nrosegmento = '" + nrosegmento + "'"
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    nrosegmento,
                    nrodivisao,
                    descsegmento,
                    indprecoembalagem,
                    status
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + nrosegmento + "','" \
                    + nrodivisao + "','" \
                    + descsegmento + "','" \
                    + indprecoembalagem + "','" \
                    + status + \
                    "')"
                vInsert = "insert into pbi.carga_segmento\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_segmento set " \
                    "nrosegmento = '" + nrosegmento + "'," \
                                                      "nrodivisao = '" + nrodivisao + "'," \
                                                                                      "descsegmento = '" + descsegmento + "'," \
                                                                                                                          "indprecoembalagem = '" + indprecoembalagem + "'," \
                                                                                                                                                                        "status = '" + status + "' " \
                                                                                                                                                                                                "where 1=1 " \
                                                                                                                                                                                                "and nrosegmento = '" + nrosegmento + "'"
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_MARCA
        ###############################################################################################################
        vQruey = "select count(*) quantidade from pbi.carga_marca"
        vDFPostgres = fun.postgres_consulta(vQruey)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        vPrimeiraCarga = True
        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MapEmpresa \
                = \
                """
                select
                    seqmarca,
                    nvl(regexp_replace(marca, '''', ''),' ') marca,
                    nvl(tipomarca,'N') tipomarca,
                    status,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),'01/01/1994') datahoraalteracao
                from implantacao.map_marca
                nolock
                where 1=1
                order by seqmarca asc
                """
        else:
            Select_MapEmpresa \
                = \
                """
                select
                    seqmarca,
                    nvl(regexp_replace(marca, '''', ''),' ') marca,
                    nvl(tipomarca,'N') tipomarca,
                    status,
                    nvl(to_char(datahoraalteracao,'dd/mm/yyyy'),'01/01/1994') datahoraalteracao
                from implantacao.map_marca
                nolock
                where 1=1
                and datahoraalteracao >= to_DATE(sysdate -5)
                order by seqmarca asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MapEmpresa)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vTReg = vDFrame["SEQMARCA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Marca\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Marca\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Marca\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Marca\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            seqmarca = str(reg.SEQMARCA)
            marca = reg.MARCA.title()
            tipomarca = reg.TIPOMARCA
            status = reg.STATUS

            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_marca " \
                "where 1=1 " \
                "and seqmarca = '" + seqmarca + "'"

            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos \
                    = \
                    """
                    (
                    seqmarca,
                    marca,
                    tipomarca,
                    status
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + seqmarca + "','" \
                    + marca + "','" \
                    + tipomarca + "','" \
                    + status + \
                    "')"
                vInsert = "insert into pbi.carga_marca\n" + vCampos + "\nvalues\n" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_marca set " \
                    "marca = '" + marca + "'," \
                                          "tipomarca = '" + tipomarca + "'," \
                                                                        "status = '" + status + "' " \
                                                                                                "where 1=1 " \
                                                                                                "and seqmarca = '" + seqmarca + "'"
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAP_FAMILIA
        ###############################################################################################################
        vQruey = "select count(*) quantidade from pbi.carga_familia"
        vDFPostgres = fun.postgres_consulta(vQruey)
        vDFPostgres = pd.DataFrame(vDFPostgres)
        vTRegPostgres = vDFPostgres["quantidade"][0]

        vPrimeiraCarga = True
        if (vTRegPostgres != 0):
            vPrimeiraCarga = False

        if (vPrimeiraCarga == True):
            Select_MapFamilia \
                = \
                """
                select
                    seqfamilia,
                    nvl(seqmarca,0) seqmarca,
                    nvl(regexp_replace(familia, '''', ''),' ') familia,
                    pesavel,
                    nvl(aliqpadraoicms,0) aliqpadraoicms,
                    to_char(dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    nvl(to_char(dtahoralteracao,'dd/mm/yyyy'),to_char(dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao
                from implantacao.map_familia
                nolock
                where 1=1
                order by seqfamilia asc
                """
        else:
            Select_MapFamilia \
                = \
                """
                select
                    seqfamilia,
                    nvl(seqmarca,0) seqmarca,
                    nvl(regexp_replace(familia, '''', ''),' ') familia,
                    pesavel,
                    nvl(aliqpadraoicms,0) aliqpadraoicms,
                    to_char(dtahorinclusao,'dd/mm/yyyy') dtahorinclusao,
                    nvl(to_char(dtahoralteracao,'dd/mm/yyyy'),to_char(dtahorinclusao,'dd/mm/yyyy')) dtahoralteracao
                from implantacao.map_familia
                nolock
                where 1=1
                and dtahoralteracao >= to_DATE(sysdate -5)
                order by seqfamilia asc
                """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MapFamilia)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)
        vDFrame = vDFrame.astype({"ALIQPADRAOICMS": float})
        vTReg = vDFrame["SEQFAMILIA"].count()

        if (vTReg == 0):
            print("Tabela de \"Cadastro Familia\" atualizada ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Tabela de \"Cadastro Familia\" atualizada ...\n")
        else:
            print("Execultado processo na tabela de \"Cadastro Familia\" ...")
            with open(NomeArquivo, "a") as Arquivo:
                Arquivo.write("» Execultado processo na tabela de \"Cadastro Familia\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            vQuery \
                = \
                "select count(*) quantidade " \
                "from pbi.carga_familia " \
                "where 1=1 " \
                "and seqfamilia = '" + str(reg.SEQFAMILIA) + "'"
            vDFPostgres = fun.postgres_consulta(vQuery)
            vDFPostgres = pd.DataFrame(vDFPostgres)
            vTRegPostgres = vDFPostgres["quantidade"][0]

            if (vTRegPostgres == 0):
                I += 1
                vCampos = \
                    """
                    (
                    seqfamilia,
                    seqmarca,
                    familia,
                    pesavel,
                    aliqpadraoicms,
                    dtahorinclusao
                    )
                    """
                vValues \
                    = \
                    "('" \
                    + str(reg.SEQFAMILIA) + "','" \
                    + str(reg.SEQMARCA) + "','" \
                    + reg.FAMILIA.title() + "','" \
                    + reg.PESAVEL + "','" \
                    + str(reg.ALIQPADRAOICMS) + "','" \
                    + reg.DTAHORINCLUSAO + \
                    "')"
                vInsert = "insert into pbi.carga_familia" + vCampos + "values" + vValues + ""
                fun.process_data(vInsert)
            else:
                vupdate \
                    = \
                    "update pbi.carga_familia " \
                    "set " \
                    "seqmarca = '" + str(reg.SEQMARCA) + "'," \
                                                         "familia = '" + reg.FAMILIA + "'," \
                                                                                       "pesavel = '" + reg.PESAVEL + "'," \
                                                                                                                     "aliqpadraoicms = '" + str(
                        reg.ALIQPADRAOICMS) + "'," \
                                              "dtahorinclusao = '" + reg.DTAHORINCLUSAO + "' " \
                                                                                          "where 1=1 " \
                                                                                          "and seqfamilia = '" + str(
                        reg.SEQFAMILIA) + "'" \
                                          ""
                fun.process_data(vupdate)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAX_DIVISAO
        ###############################################################################################################
        fun.process_data("delete from pbi.carga_divisao")
        Select_MaxDivisao \
            = \
            """
            select
                nrodivisao,
                divisao,
                tipdivisao
            from implantacao.max_divisao
            nolock
            where 1=1
            order by nrodivisao asc
            """
        I = 0
        vCur = cOracle.cursor()
        vCur.execute(Select_MaxDivisao)
        vCol = [Reg[0] for Reg in vCur.description]
        vLin = vCur.fetchall()
        vDFrame = pd.DataFrame.from_records(vLin, columns=vCol)

        print("Execultado processo na tabela de \"Cadastro Divisão\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Divisão\" ...\n")
        Arquivo.close()

        for index, reg in vDFrame.iterrows():
            vCampos \
                = \
                """
                (
                nrodivisao,
                divisao,
                tipdivisao
                )
                """
            vValues \
                = \
                "('" \
                + str(reg.NRODIVISAO) + "','" \
                + reg.DIVISAO.title() + "','" \
                + reg.TIPDIVISAO + \
                "')"
            vInsert = "insert into pbi.carga_divisao" + vCampos + "values" + vValues + ""
            fun.process_data(vInsert)
        vCur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA GE_LOGRADOURO
        ###############################################################################################################
        fun.process_data("truncate pbi.carga_logradouro")
        Select_GeLogradouro \
            = \
            """
            select
                seqlogradouro,
                seqcidade,
                nvl(regexp_replace(logradouro, '''', ''),'NAO INFORMADO') logradouro,
                nvl(tiplogradouro,'NAO INFORMADO') tiplogradouro
            from implantacao.ge_logradouro
            where 1=1
            order by seqlogradouro
            """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(Select_GeLogradouro)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        dfGeLogradouro = pd.DataFrame.from_records(Reg, columns=Col)
        TotalReg = dfGeLogradouro["SEQLOGRADOURO"].count()

        print("Execultado processo na tabela de \"Cadastro Logradouro\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Logradouro\" ...\n")
        Arquivo.close()

        Campos = "(seqlogradouro,seqcidade,logradouro,tiplogradouro)"
        I = 0
        Dados = ""
        Parametro = 1000

        for index, reg in dfGeLogradouro.iterrows():
            LogradouroId = str(reg.SEQLOGRADOURO)
            CidadeId = str(reg.SEQCIDADE)
            Nome = reg.LOGRADOURO.title()
            Tipo = reg.TIPLOGRADOURO.title()

            if (I == 0):
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados + Virgula + "('" + LogradouroId + "','" + CidadeId + "','" + Nome + "','" + Tipo + "')\n"

            I += 1

            if (I == Parametro):
                sqlInsert = "insert into pbi.carga_logradouro\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if (TotalReg < Parametro):
                    Parametro = TotalReg
                I = 0
                Dados = ""
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA GE_BAIRRO
        ###############################################################################################################
        fun.process_data("truncate pbi.carga_bairro")
        Select_GeBairro \
            = \
            """
            select
                seqbairro,
                seqcidade,
                nvl(regexp_replace(bairro, '''', ''),'NAO INFORMADO') bairro
            from implantacao.ge_bairro
            nolock 
            where 1=1 
            order by seqbairro asc
            """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(Select_GeBairro)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        dfGeBairro = pd.DataFrame.from_records(Reg, columns=Col)
        TotalReg = dfGeBairro["SEQBAIRRO"].count()

        print("Execultado processo na tabela de \"Cadastro Bairro\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Bairro\" ...\n")
        Arquivo.close()

        Campos = "(seqbairro,seqcidade,bairro)"
        I = 0
        Dados = ""
        Parametro = 1000

        for index, reg in dfGeBairro.iterrows():
            BairroId = str(reg.SEQBAIRRO)
            CidadeId = str(reg.SEQCIDADE)
            Nome = reg.BAIRRO.title()

            if (I == 0):
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados + Virgula + "('" + BairroId + "','" + CidadeId + "','" + Nome + "')\n"

            I += 1

            if (I == Parametro):
                sqlInsert = "insert into pbi.carga_bairro\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if (TotalReg < Parametro):
                    Parametro = TotalReg
                I = 0
                Dados = ""
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA GE_CIDADE
        ###############################################################################################################
        fun.process_data("truncate pbi.carga_cidade")
        Select_GeCidade \
            = \
            """
            select
                seqcidade,
                nvl(regexp_replace(cidade, '''', ''),'NAO INFORMADO') cidade,
                nvl(uf,'') uf,
                nvl(cepinicial,'00000000') cepinicial,
                nvl(cepfinal,'00000000') cepfinal,
                nvl(codibge,'0000000') codibge
            from implantacao.ge_cidade
            where 1=1
            order by seqcidade asc
            """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(Select_GeCidade)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        dfGeCidade = pd.DataFrame.from_records(Reg, columns=Col)
        dfGeCidade = dfGeCidade.astype({"CODIBGE": str})
        TotalReg = dfGeCidade["SEQCIDADE"].count()

        print("Execultado processo na tabela de \"Cadastro Cidade\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Cidade\" ...\n")
        Arquivo.close()

        Campos = "(seqcidade,cidade,uf,cepinicial,cepfinal,codibge)"
        I = 0
        Dados = ""
        Parametro = 1000

        for index, reg in dfGeCidade.iterrows():
            CidadeId = str(reg.SEQCIDADE)
            Nome = reg.CIDADE.title()
            Uf = reg.UF
            CepInicial = reg.CEPINICIAL
            CepFinal = reg.CEPFINAL
            CodIBGE = reg.CODIBGE

            if (I == 0):
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados + Virgula + "('" + CidadeId + "','" + Nome + "','" + Uf + "','" + CepInicial + "','" + CepFinal + "','" + CodIBGE + "')\n"

            I += 1

            if (I == Parametro):
                sqlInsert = "insert into pbi.carga_cidade\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if (TotalReg < Parametro):
                    Parametro = TotalReg
                I = 0
                Dados = ""
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA GE_BANCO
        ###############################################################################################################
        fun.process_data("truncate pbi.carga_banco")
        query = \
            """
            select 
                nrobanco,
                razaosocial,
                fantasia
            from implantacao.ge_banco
            where 1=1 
            order by nrobanco asc
            """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(query)
        dados = cursor.fetchall()
        colunasDB = [row[0] for row in cursor.description]
        colunas = []
        for i in colunasDB: colunas.append(i.lower())
        df = pd.DataFrame.from_records(dados, columns=colunas)
        TotalReg = df["nrobanco"].count()
        for i in df:
            df["razaosocial"] = df["razaosocial"].str.title()
            df["fantasia"] = df["fantasia"].str.title()

        print("Execultado processo na tabela de \"Cadastro Bancos\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Cadastro Bancos\" ...\n")
        Arquivo.close()

        Campos = "(nrobanco,razaosocial,fantasia)"
        I = 0
        Dados = ""
        Parametro = 1000

        if (TotalReg < 1000): Parametro = TotalReg

        for index, reg in df.iterrows():
            nrobanco = str(reg.nrobanco)
            razaosocial = reg.razaosocial
            fantasia = reg.fantasia

            if (I == 0):
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados + Virgula + "('" + nrobanco + "','" + razaosocial + "','" + fantasia + "')\n"

            I += 1

            if (I == Parametro):
                sqlInsert = "insert into pbi.carga_banco\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if (TotalReg < Parametro):
                    Parametro = TotalReg
                I = 0
                Dados = ""
        cursor.close()

    except Exception as erro:
        print("Error ao se conectar no banco de dados")
        print(erro)
    finally:
        if cOracle:
            cOracle.close()

#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dta
import md002Funcoes as fun
import md001CreateTableScripts as tbl

# DataAtual = dta.datetime.today().date() - dta.timedelta(days=5)
DataAtual = dta.datetime.today().date()

Dia = str(DataAtual.day)
Mes = str(DataAtual.month)
Ano = str(DataAtual.year)
DataAtual = Dia.zfill(2) + "/" + Mes.zfill(2) + "/" + Ano.zfill(4)
NomeArquivo = "LogCarga" + Dia.zfill(2) + Mes.zfill(2) + Ano.zfill(4) + ".txt"


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
                    DISTINCT 
                    a.nroempresa, 
                    a.seqproduto,
                    b.dtavalidade,
                    TO_DATE(MAX(a.dtaalteracao)) AS dtaalteracao
                FROM implantacao.mlo_endereco a
                INNER JOIN implantacao.mlo_paleteqtde b on b.seqpaleterf = a.seqpaleterf
                WHERE 1=1
                AND b.dtavalidade = (SELECT DISTINCT MIN(e.dtavalidade)
                                     FROM implantacao.mlo_endereco d
                                     INNER JOIN implantacao.mlo_paleteqtde e on e.seqpaleterf = d.seqpaleterf 
                                     WHERE 1=1
                                     AND d.seqproduto = a.seqproduto)
                GROUP BY a.nroempresa, a.seqproduto, b.dtavalidade
                ORDER BY a.seqproduto ASC
                """
        else:
            select_mloendereco \
                = \
                """
                SELECT 
                    DISTINCT 
                    a.nroempresa, 
                    a.seqproduto,
                    b.dtavalidade,
                    TO_DATE(MAX(a.dtaalteracao)) AS dtaalteracao
                FROM implantacao.mlo_endereco a
                INNER JOIN implantacao.mlo_paleteqtde b on b.seqpaleterf = a.seqpaleterf
                WHERE 1=1
                AND b.dtavalidade = (SELECT DISTINCT MIN(e.dtavalidade)
                                     FROM implantacao.mlo_endereco d
                                     INNER JOIN implantacao.mlo_paleteqtde e on e.seqpaleterf = d.seqpaleterf 
                                     WHERE 1=1
                                     AND d.seqproduto = a.seqproduto)
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - 1)
                GROUP BY a.nroempresa, a.seqproduto, b.dtavalidade
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

        Campos = "(nroempresa, seqproduto, dtavalidade, dtaalteracao)"

        for index, reg in dfdatacritica.iterrows():
            id_empresa = str(reg.NROEMPRESA)
            id_produto = str(reg.SEQPRODUTO)
            dtavalidade = str(reg.DTAVALIDADE)
            dtaalteracao = str(reg.DTAALTERACAO)

            query = f"SELECT * FROM pbi.carga_datacritica WHERE nroempresa = {id_empresa} AND seqproduto = {id_produto}"
            curpostgre.execute(query)
            dados = curpostgre.fetchall()
            Operacao = len(dados)

            if Operacao == 0:
                Dados = "('" + id_empresa + "','" + id_produto + "','" + dtavalidade + "','" + dtaalteracao + "')"
                sqlInsert \
                    = "INSERT INTO pbi.carga_datacritica\n" + Campos + "\nVALUES\n" + Dados + "\n"
                fun.process_data(sqlInsert)
            else:
                sqlupdate \
                    = \
                    "UPDATE pbi.carga_datacritica " \
                    "SET " \
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
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - 1)
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
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - 60)
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
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE -1)
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
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = b.padraoembvenda
                WHERE 1 = 1
                AND TO_DATE(a.dtahoralteracao) >= TO_DATE(SYSDATE -1)
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
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - 1)
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
                AND TO_DATE(a.dtahoralteracao) >= TO_DATE(SYSDATE - 1)
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
                """
                SELECT 
                    a.seqrede,
                    a.seqpessoa,
                    REPLACE(b.descricao, '''', '') AS descricao,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao 
                FROM implantacao.ge_redepessoa a
                INNER JOIN implantacao.ge_rede b ON b.seqrede = a.seqrede	
                WHERE 1=1
                AND a.status = 'A'
                AND TO_DATE(a.dtaalteracao) >= TO_DATE(SYSDATE - 1)
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

        campos = "(seqrede,seqpessoa,descricao,dtaalteracao)"

        for index, reg in redesclientes.iterrows():
            seqrede = str(reg.SEQREDE)
            seqpessoa = str(reg.SEQPESSOA)
            descricao = reg.DESCRICAO
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
                dados = "('" + seqrede + "','" + seqpessoa + "','" + descricao + "','" + dtaalteracao + "')"
                sql_insert \
                    = "INSERT INTO pbi.carga_redesclientes\n" + campos + "\nVALUES\n" + dados + "\n"
                fun.process_data(sql_insert)
            else:
                sql_update \
                    = \
                    "UPDATE pbi.carga_redesclientes " \
                    "SET " \
                    "descricao = '" + descricao + "'," \
                    "dtaalteracao = '" + dtaalteracao + "' " \
                    "WHERE 1 = 1 " \
                    "AND seqrede = '" + seqrede + "' " \
                    "AND seqpessoa = '" + seqpessoa + "'"
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
                AND dtaalteracao >= ADD_MONTHS(TRUNC(SYSDATE,'DD'),0)-1
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
                """
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
                and nvl(a.dtahoralteracao,a.dtahorinclusao) >= TO_DATE(SYSDATE - 5)
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
