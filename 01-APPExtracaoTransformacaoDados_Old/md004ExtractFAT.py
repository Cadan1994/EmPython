import os
import pandas as pd
import datetime as dta
import md002Funcoes as fun
import md001CreateTableScripts as tbl
from pathlib import Path

DataAtualParcial = dta.datetime.today().date() - dta.timedelta(days=15)
Dia = str(DataAtualParcial.day)
Mes = str(DataAtualParcial.month)
Ano = str(DataAtualParcial.year)
Data_Carga = Dia.zfill(2)+"/"+Mes.zfill(2)+"/"+Ano.zfill(4)
dirtxt = "log"
base_dir = Path(__file__).cwd()
NomeArquivo = os.path.join(base_dir, dirtxt) + "\LogCarga" + \
              str(dta.datetime.today().date().day).zfill(2) + \
              str(dta.datetime.today().date().month).zfill(2) + \
              str(dta.datetime.today().date().year).zfill(4) + ".txt"


def etl_fatos():
    ###############################################################################################################
    # SE CONECTAR E BUSCA DE DADOS NO BANCO DE DADOS CONCINCO "ORACLE"
    ###############################################################################################################
    con_oracle = fun.conect_oracle()

    try:
        ###############################################################################################################
        # CRIAS AS TABELAS, CASO NÃO EXISTA
        ###############################################################################################################
        fun.create_table(tbl.CreateTable_MadMetaEquipe)
        fun.create_table(tbl.CreateTable_MadMetaRepresentante)
        fun.create_table(tbl.CreateTable_MrlCustoDiaFamilia)
        fun.create_table(tbl.CreateTable_MaxvABCDistribuicaoBase)
        fun.create_table(tbl.CreateTable_MadPedidoVenda)
        fun.create_table(tbl.CreateTable_MadPedidoVendaItem)
        fun.create_table(tbl.CreateTable_MadRepresentanteCtaFlex)
        fun.create_table(tbl.CreateTable_FiTitulo)
        fun.create_table(tbl.CreateTable_MrlProdutoEmpresa)
        fun.create_table(tbl.CreateTable_MadMovimentacaoCompra)
        fun.create_table(tbl.CreateTable_Mad_PedidosFaturados)
        fun.create_table(tbl.CreateTable_ProdutosPrecoVenda)

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAD_METAPERIODO INNER JOIN MAD_METAREPRESENTANTE
        ###############################################################################################################
        print("Execultado processo na tabela de \"Metas Representantes\" ...")
        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Metas Representantes\" ...\n")
            Arquivo.close()

        dfPostgres = fun.postgres_consulta \
                (
                """
                select count(*) quantidade
                from pbi.carga_metarepresentante
                where 1=1
                """
            )
        dfPostgres = pd.DataFrame(dfPostgres)
        tregPostgres = dfPostgres["quantidade"][0]

        if tregPostgres != 0:
            fun.process_data \
                    (
                    """
                    delete
                    from pbi.carga_metarepresentante
                    where 1=1
                    and dtainicial >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                    """
                )
            query \
                = """
                  select
                  a.seqmetaperiodo,
                  a.nroempresa,
                  a.dtainicial,
                  a.dtafinal,
                  b.nrorepresentante,
                  b.metavlrvenda,
                  b.metanrocliatendido,
                  b.metanroprodvendido,
                  nvl(a.status,'A') status
                  from implantacao.mad_metaperiodo a
                  inner join implantacao.mad_metarepres b on b.seqmetaperiodo = a.seqmetaperiodo
                  where 1=1
                  and a.dtainicial >= add_months(trunc(sysdate,'mm'), -3)
                  order by a.seqmetaperiodo asc,b.nrorepresentante asc
                  """
        else:
            query \
                = """
                  select
                  a.seqmetaperiodo,
                  a.nroempresa,
                  a.dtainicial,
                  a.dtafinal,
                  b.nrorepresentante,
                  b.metavlrvenda,
                  b.metanrocliatendido,
                  b.metanroprodvendido,
                  nvl(a.status,'A') status
                  from implantacao.mad_metaperiodo a
                  inner join implantacao.mad_metarepres b on b.seqmetaperiodo = a.seqmetaperiodo
                  where 1=1
                  and a.dtainicial >= add_months(trunc(sysdate,'YYYY'), -48)
                  order by a.seqmetaperiodo asc,b.nrorepresentante asc
                  """
        Cur = con_oracle.cursor()
        Cur.execute(query)
        Col = [Reg[0] for Reg in Cur.description]
        Lin = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Lin, columns=Col)
        DFrame["DTAINICIAL"] = pd.DatetimeIndex(DFrame["DTAINICIAL"]).date
        DFrame["DTAFINAL"] = pd.DatetimeIndex(DFrame["DTAFINAL"]).date

        for index, reg in DFrame.iterrows():
            Campos \
                = \
                """
                (
                seqmetaperiodo,
                nroempresa,
                dtainicial,
                dtafinal,
                nrorepresentante,
                metavlrvenda,
                metanrocliatendido,
                metanroprodvendido,
                status
                )
                """
            Values \
                = \
                "('" \
                + str(reg.SEQMETAPERIODO) + "','" \
                + str(reg.NROEMPRESA) + "','" \
                + str(reg.DTAINICIAL) + "','" \
                + str(reg.DTAFINAL) + "','" \
                + str(reg.NROREPRESENTANTE) + "','" \
                + str(reg.METAVLRVENDA) + "','" \
                + str(reg.METANROCLIATENDIDO) + "','" \
                + str(reg.METANROPRODVENDIDO) + "','" \
                + reg.STATUS + \
                "')"

            Insert = "insert into pbi.carga_metarepresentante" + Campos + "values" + Values + ""
            fun.process_data(Insert)
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MRL_CUSTODIAFAM - ORACLE
        ###############################################################################################################
        print("Execultado processo na tabela de \"Custo Diário Família\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Custo Diário Família\" ...\n")
        Arquivo.close()

        df = fun.postgres_consulta("select count(*) quantidade from pbi.carga_custodiafamilia")
        TotalReg = df["quantidade"][0]
        Id = 0

        if TotalReg != 0:
            qryDelete = """
                        delete
                        from pbi.carga_custodiafamilia
                        where 1=1
                        and dtaentradasaida >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                        """
            qrySelect = """
                        select id 
                        from pbi.carga_custodiafamilia
                        order by id asc
                        """
            fun.process_data(qryDelete)
            df = fun.postgres_consulta(qrySelect)
            Id = df[["id"]].tail(1) # Peda o último registro do DataFrame
            Id = Id["id"].values    # Peda o valor do DataFrame
            Id = Id[0]              # Index 0

            Select_MrlCustoDiaFamilia = \
            """
            select
            t1.nroempresa,
            t1.seqpessoa,
            t1.nrosegmento,
            nvl(t1.nroequipe,0) nroequipe,
            t1.nrorepresentante,
            nvl(t5.seqfamilia,0) seqfamilia,
            t1.seqproduto,
            t1.codgeraloper,
            to_char(t1.dtavda,'dd/mm/yyyy') dtavda,
            ROUND(
            SUM(
            (DECODE(t1.acmcompravenda,'N',0,NVL(t1.vlrdespoperacionalitem,
            (DECODE(t2.qtdvda * t1.qtditem,0,0,t2.vlrdespesavda*DECODE('S','N',1,NVL(t3.propqtdprodutobase,1))/t2.qtdvda*t1.qtditem))))) - 
            (NVL(t1.vlrdespoperacionalitemdevol,
            (DECODE(t2.qtdvda * t1.qtddevolitem, 0, 0, t2.vlrdespesavda * 
            DECODE('S','N',1,NVL(t3.propqtdprodutobase,1))/t2.qtdvda*t1.qtddevolitem))))
            ),2) 
            as vlrdespoperacional,
            ROUND(
            SUM( 
            (((t2.cmdiavlrnf )+t2.cmdiaipi-t2.cmdiacredicms-NVL(t2.cmdiacredpis,0)-NVL(t2.cmdiacredcofins,0)-NVL(t2.cmdiacredipi,0)+t2.cmdiaicmsst+t2.cmdiadespnf+t2.cmdiadespforanf-t2.cmdiadctoforanf)*
            CASE 
            WHEN t8.utilacresccustprodrelac = 'S' AND NVL(t3.seqprodutobase, t3.seqprodutobaseantigo) IS NOT NULL 
            THEN COALESCE(t9.percacresccustorelacvig,NVL(implantacao.f_retacresccustorelacabc(t1.seqproduto,t1.dtavda),1))
            ELSE 1
            END *
            DECODE('S','N',1,NVL(t3.propqtdprodutobase,1)) -
            (DECODE(t2.qtdverbavda,0,0,t2.vlrverbavda * NVL( t3.propqtdprodutobase,1) / DECODE(NVL(t2.qtdvda,0),0,t2.qtdverbavda,t2.qtdvda))) + 
            DECODE(t1.acmcompravenda,'N',0,DECODE('N','S',0,(DECODE(NVL(t2.vlrverbavdaacr,0),0,0,t2.vlrverbavdaacr * NVL(t3.propqtdprodutobase,1) / DECODE(NVL(t2.qtdvda,0),0,1,t2.qtdvda)))))) * 
            (t1.qtditem - t1.qtddevolitem)
            ),2)
            as vlrcustoliquido,
            ROUND(
            SUM( 
            implantacao.fC5_AbcDistribLucratividade(
            'L',
            'L',
            'N',
            ROUND(t1.vlritem,2),
            'S',
            t1.vlricmsst,
            t1.vlrfcpst,
            t1.vlricmsstemporig,
            t7.uf,
            t1.ufpessoa,
            'S',
            t11.vlrdescregra, 
            'N',
            t1.vlripiitem,
            t1.vlripidevolitem,
            'N',
            t1.vlrdescforanf,
            t2.cmdiavlrnf-0 ,
            t2.cmdiaipi,
            NVL(t2.cmdiacredpis,0),
            NVL(t2.cmdiacredcofins,0),
            t2.cmdiaicmsst,
            t2.cmdiadespnf,
            t2.cmdiadespforanf,
            t2.cmdiadctoforanf,
            'S',
            t3.propqtdprodutobase,
            t1.qtditem,
            t1.vlrembdescressarcst,
            t1.acmcompravenda,
            t1.pisitem,
            t1.cofinsitem,
            DECODE(t1.tipcgo,'S',t2.qtdvda,NVL(t2.qtddevol,t2.qtdvda)),
            (DECODE(t1.tipcgo,'S',t2.vlrimpostovda - NVL(t2.vlripivda,0), 
            NVL(((t2.vlrimpostodevol / DECODE(NVL(t2.qtddevol,0),0,1,t2.qtddevol)) * t1.qtddevolitem) - NVL(t1.vlripidevolitem,0), 
            t2.vlrimpostovda - NVL( t2.vlripivda,0)))),
            'N',
            t1.vlrdespoperacionalitem,
            t2.vlrdespesavda,
            'N',
            NVL(t2.vlrverbavdaacr,0),
            t2.qtdverbavda,
            t2.vlrverbavda - NVL(t2.vlrverbavdaindevida,0),
            'N',
            NVL(t1.vlrtotcomissaoitem,0),
            t1.vlrdevolitem,
            t1.vlrdevolicmsst,
            t1.dvlrfcpst,
            t1.qtddevolitem,
            t1.pisdevolitem,
            t1.cofinsdevolitem,
            t1.vlrdespoperacionalitemdevol,
            t1.vlrtotcomissaoitemdevol,
            t7.perirlucrat,
            t7.percslllucrat,
            t2.cmdiacredicms,
            DECODE(t1.icmsefetivoitem,0,t1.ICMSITEM,t1.icmsefetivoitem),
            t1.vlrfcpicms,
            t1.percpmf,
            t1.peroutroimposto,
            DECODE(t1.icmsefetivodevolitem,0,t1.icmsdevolitem,t1.icmsefetivodevolitem),
            t1.Dvlrfcpicms, 
            CASE 
            WHEN ( 'S' ) = 'N' 
            THEN (NVL(t2.cmdiavlrdescpistransf,0) + NVL(t2.cmdiavlrdesccofinstransf,0) + NVL(t2.cmdiavlrdescicmstransf,0) + NVL(t2.cmdiavlrdescipitransf,0) + NVL(t2.cmdiavlrdesclucrotransf,0) + NVL(t2.cmdiavlrdescverbatransf,0))
            ELSE 0
            END, 
            CASE 
            WHEN t8.utilacresccustprodrelac = 'S' and NVL(t3.seqprodutobase,t3.seqprodutobaseantigo) IS NOT NULL
            THEN COALESCE(t9.percacresccustorelacvig,NVL(implantacao.f_retacresccustorelacabc(t1.seqproduto,t1.dtavda),1))
            ELSE 1 
            END,
            'N',
            0,
            0,
            'S',
            t1.vlrdescmedalha,
            'S',
            t1.vlrdescfornec,
            t1.vlrdescfornecdevol,
            'N',
            t1.vlrfreteitemrateio,
            t1.vlrfreteitemrateiodev,
            'S',
            t1.vlricmsstembutprod,
            t1.vlricmsstembutproddev,
            t1.vlrembdescressarcstdevol,
            CASE 
            WHEN 'N' = 'S' 
            THEN NVL(t1.vlrdescacordoverbapdv,0)
            ELSE 0 
            END,
            NVL(t2.cmdiacredipi,0),
            NVL(t1.vlritemrateiocte,0),
            'N',
            'C'
            )),2)
            as vlrlucratividade
            FROM implantacao.maxv_abcdistribbase t1, 
                 implantacao.mrl_custodiafam t2, 
                 implantacao.map_produto t3, 
                 implantacao.map_produto t4, 
                 implantacao.map_famdivisao t5, 
                 implantacao.map_famembalagem t6, 
                 implantacao.max_empresa t7, 
                 implantacao.max_divisao t8, 
                 implantacao.map_prodacresccustorelac t9, 
                 implantacao.max_codgeraloper t10, 
                 implantacao.mrlv_descontoregra t11 
            WHERE 1 = 1 
            AND t5.seqfamilia = t3.seqfamilia
            AND t5.nrodivisao = t1.nrodivisao
            AND t1.seqproduto = t3.seqproduto
            AND t1.seqprodutocusto = t4.seqproduto
            AND t1.nrosegmento in (1,3,4,5,6,7,8,9,10)
            AND t1.nrodivisao = t5.nrodivisao
            AND t7.nroempresa = t1.nroempresa
            AND t7.nrodivisao = t8.nrodivisao
            AND t1.seqproduto = t9.seqproduto(+)
            AND t1.dtavda = t9.dtamovimentacao(+)
            AND t2.nroempresa = NVL( t7.nroempcustoabc, t7.nroempresa ) 
            AND t2.dtaentradasaida = t1.dtavda
            AND t6.seqfamilia = t3.seqfamilia AND t6.qtdembalagem = 1 AND t1.seqproduto = t11.seqproduto (+) 
            AND t1.dtavda = t11.datafaturamento (+)
            AND t1.nrodocto = t11.numerodf (+)
            AND t1.seriedocto = t11.seriedf (+) 
            AND t1.nroempresa = t11.nroempresa (+) 
            AND t2.seqfamilia = t4.seqfamilia
            AND t1.codgeraloper = t10.codgeraloper
            AND DECODE(t1.tiptabela,'S',t1.cgoacmcompravenda,t1.acmcompravenda) in ('S','I')
            AND t1.seqpessoa NOT IN (1,22401)
            AND t5.seqcomprador NOT IN (8,11)
            AND t1.dtavda >= ADD_MONTHS(TRUNC(SYSdate,'mm'), -3)
            GROUP BY t1.nroempresa,t1.seqpessoa,t1.nrosegmento,t1.nroequipe,t1.nrorepresentante,t1.codgeraloper,t1.dtavda,t5.seqfamilia,t1.seqproduto
            ORDER BY t1.dtavda ASC            
            """
        else:
            Select_MrlCustoDiaFamilia = \
            """
            select
            t1.nroempresa,
            t1.seqpessoa,
            t1.nrosegmento,
            nvl(t1.nroequipe,0) nroequipe,
            t1.nrorepresentante,
            nvl(t5.seqfamilia,0) seqfamilia,
            t1.seqproduto,
            t1.codgeraloper,
            to_char(t1.dtavda,'dd/mm/yyyy') dtavda,
            ROUND(
            SUM(
            (DECODE(t1.acmcompravenda,'N',0,NVL(t1.vlrdespoperacionalitem,
            (DECODE(t2.qtdvda * t1.qtditem,0,0,t2.vlrdespesavda*DECODE('S','N',1,NVL(t3.propqtdprodutobase,1))/t2.qtdvda*t1.qtditem))))) - 
            (NVL(t1.vlrdespoperacionalitemdevol,
            (DECODE(t2.qtdvda * t1.qtddevolitem, 0, 0, t2.vlrdespesavda * 
            DECODE('S','N',1,NVL(t3.propqtdprodutobase,1))/t2.qtdvda*t1.qtddevolitem))))
            ),2) 
            as vlrdespoperacional,
            ROUND(
            SUM( 
            (((t2.cmdiavlrnf )+t2.cmdiaipi-t2.cmdiacredicms-NVL(t2.cmdiacredpis,0)-NVL(t2.cmdiacredcofins,0)-NVL(t2.cmdiacredipi,0)+t2.cmdiaicmsst+t2.cmdiadespnf+t2.cmdiadespforanf-t2.cmdiadctoforanf)*
            CASE 
            WHEN t8.utilacresccustprodrelac = 'S' AND NVL(t3.seqprodutobase, t3.seqprodutobaseantigo) IS NOT NULL 
            THEN COALESCE(t9.percacresccustorelacvig,NVL(implantacao.f_retacresccustorelacabc(t1.seqproduto,t1.dtavda),1))
            ELSE 1
            END *
            DECODE('S','N',1,NVL(t3.propqtdprodutobase,1)) -
            (DECODE(t2.qtdverbavda,0,0,t2.vlrverbavda * NVL( t3.propqtdprodutobase,1) / DECODE(NVL(t2.qtdvda,0),0,t2.qtdverbavda,t2.qtdvda))) + 
            DECODE(t1.acmcompravenda,'N',0,DECODE('N','S',0,(DECODE(NVL(t2.vlrverbavdaacr,0),0,0,t2.vlrverbavdaacr * NVL(t3.propqtdprodutobase,1) / DECODE(NVL(t2.qtdvda,0),0,1,t2.qtdvda)))))) * 
            (t1.qtditem - t1.qtddevolitem)
            ),2)
            as vlrcustoliquido,
            ROUND(
            SUM( 
            implantacao.fC5_AbcDistribLucratividade(
            'L',
            'L',
            'N',
            ROUND(t1.vlritem,2),
            'S',
            t1.vlricmsst,
            t1.vlrfcpst,
            t1.vlricmsstemporig,
            t7.uf,
            t1.ufpessoa,
            'S',
            t11.vlrdescregra, 
            'N',
            t1.vlripiitem,
            t1.vlripidevolitem,
            'N',
            t1.vlrdescforanf,
            t2.cmdiavlrnf-0 ,
            t2.cmdiaipi,
            NVL(t2.cmdiacredpis,0),
            NVL(t2.cmdiacredcofins,0),
            t2.cmdiaicmsst,
            t2.cmdiadespnf,
            t2.cmdiadespforanf,
            t2.cmdiadctoforanf,
            'S',
            t3.propqtdprodutobase,
            t1.qtditem,
            t1.vlrembdescressarcst,
            t1.acmcompravenda,
            t1.pisitem,
            t1.cofinsitem,
            DECODE(t1.tipcgo,'S',t2.qtdvda,NVL(t2.qtddevol,t2.qtdvda)),
            (DECODE(t1.tipcgo,'S',t2.vlrimpostovda - NVL(t2.vlripivda,0), 
            NVL(((t2.vlrimpostodevol / DECODE(NVL(t2.qtddevol,0),0,1,t2.qtddevol)) * t1.qtddevolitem) - NVL(t1.vlripidevolitem,0), 
            t2.vlrimpostovda - NVL( t2.vlripivda,0)))),
            'N',
            t1.vlrdespoperacionalitem,
            t2.vlrdespesavda,
            'N',
            NVL(t2.vlrverbavdaacr,0),
            t2.qtdverbavda,
            t2.vlrverbavda - NVL(t2.vlrverbavdaindevida,0),
            'N',
            NVL(t1.vlrtotcomissaoitem,0),
            t1.vlrdevolitem,
            t1.vlrdevolicmsst,
            t1.dvlrfcpst,
            t1.qtddevolitem,
            t1.pisdevolitem,
            t1.cofinsdevolitem,
            t1.vlrdespoperacionalitemdevol,
            t1.vlrtotcomissaoitemdevol,
            t7.perirlucrat,
            t7.percslllucrat,
            t2.cmdiacredicms,
            DECODE(t1.icmsefetivoitem,0,t1.ICMSITEM,t1.icmsefetivoitem),
            t1.vlrfcpicms,
            t1.percpmf,
            t1.peroutroimposto,
            DECODE(t1.icmsefetivodevolitem,0,t1.icmsdevolitem,t1.icmsefetivodevolitem),
            t1.Dvlrfcpicms, 
            CASE 
            WHEN ( 'S' ) = 'N' 
            THEN (NVL(t2.cmdiavlrdescpistransf,0) + NVL(t2.cmdiavlrdesccofinstransf,0) + NVL(t2.cmdiavlrdescicmstransf,0) + NVL(t2.cmdiavlrdescipitransf,0) + NVL(t2.cmdiavlrdesclucrotransf,0) + NVL(t2.cmdiavlrdescverbatransf,0))
            ELSE 0
            END, 
            CASE 
            WHEN t8.utilacresccustprodrelac = 'S' and NVL(t3.seqprodutobase,t3.seqprodutobaseantigo) IS NOT NULL
            THEN COALESCE(t9.percacresccustorelacvig,NVL(implantacao.f_retacresccustorelacabc(t1.seqproduto,t1.dtavda),1))
            ELSE 1 
            END,
            'N',
            0,
            0,
            'S',
            t1.vlrdescmedalha,
            'S',
            t1.vlrdescfornec,
            t1.vlrdescfornecdevol,
            'N',
            t1.vlrfreteitemrateio,
            t1.vlrfreteitemrateiodev,
            'S',
            t1.vlricmsstembutprod,
            t1.vlricmsstembutproddev,
            t1.vlrembdescressarcstdevol,
            CASE 
            WHEN 'N' = 'S' 
            THEN NVL(t1.vlrdescacordoverbapdv,0)
            ELSE 0 
            END,
            NVL(t2.cmdiacredipi,0),
            NVL(t1.vlritemrateiocte,0),
            'N',
            'C'
            )),2)
            as vlrlucratividade
            FROM implantacao.maxv_abcdistribbase t1, 
                 implantacao.mrl_custodiafam t2, 
                 implantacao.map_produto t3, 
                 implantacao.map_produto t4, 
                 implantacao.map_famdivisao t5, 
                 implantacao.map_famembalagem t6, 
                 implantacao.max_empresa t7, 
                 implantacao.max_divisao t8, 
                 implantacao.map_prodacresccustorelac t9, 
                 implantacao.max_codgeraloper t10, 
                 implantacao.mrlv_descontoregra t11 
            WHERE 1 = 1 
            AND t5.seqfamilia = t3.seqfamilia
            AND t5.nrodivisao = t1.nrodivisao
            AND t1.seqproduto = t3.seqproduto
            AND t1.seqprodutocusto = t4.seqproduto
            AND t1.nrosegmento in (1,3,4,5,6,7,8,9,10)
            AND t1.nrodivisao = t5.nrodivisao
            AND t7.nroempresa = t1.nroempresa
            AND t7.nrodivisao = t8.nrodivisao
            AND t1.seqproduto = t9.seqproduto(+)
            AND t1.dtavda = t9.dtamovimentacao(+)
            AND t2.nroempresa = NVL( t7.nroempcustoabc, t7.nroempresa ) 
            AND t2.dtaentradasaida = t1.dtavda
            AND t6.seqfamilia = t3.seqfamilia AND t6.qtdembalagem = 1 AND t1.seqproduto = t11.seqproduto (+) 
            AND t1.dtavda = t11.datafaturamento (+)
            AND t1.nrodocto = t11.numerodf (+)
            AND t1.seriedocto = t11.seriedf (+) 
            AND t1.nroempresa = t11.nroempresa (+) 
            AND t2.seqfamilia = t4.seqfamilia
            AND t1.codgeraloper = t10.codgeraloper
            AND DECODE(t1.tiptabela,'S',t1.cgoacmcompravenda,t1.acmcompravenda) in ('S','I')
            AND t1.seqpessoa NOT IN (1,22401)
            AND t5.seqcomprador NOT IN (8,11)
            AND t1.dtavda >= ADD_MONTHS(TRUNC(SYSdate,'yyyy'), -48)
            GROUP BY t1.nroempresa,t1.seqpessoa,t1.nrosegmento,t1.nroequipe,t1.nrorepresentante,t1.codgeraloper,t1.dtavda,t5.seqfamilia,t1.seqproduto
            ORDER BY t1.dtavda ASC            
            """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(Select_MrlCustoDiaFamilia)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)
        for Coluna in colunas: df = df.astype({Coluna: str})
        TotalReg = df["NROEMPRESA"].count()

        Campos = \
                """
                (
                id,
                nroempresa,
                seqpessoa,
                nrosegmento,
                nroequipe,
                nrorepresentante,
                seqfamilia,
                seqproduto,
                codgeraloper,
                dtaentradasaida,
                vlrcustoliquido,
                vlrdespoperacional,
                vlrlucratividade
                )
                """

        I = 0
        Dados = ""
        Parametro = 1000
        if (TotalReg < 1000): Parametro = TotalReg

        for index, reg in df.iterrows():
            Id = Id + 1
            nroempresa          = reg.NROEMPRESA
            seqpessoa           = reg.SEQPESSOA
            nrosegmento         = reg.NROSEGMENTO
            nroequipe           = reg.NROEQUIPE
            nrorepresentante    = reg.NROREPRESENTANTE
            seqfamilia          = reg.SEQFAMILIA
            seqproduto          = reg.SEQPRODUTO
            codgeraloper        = reg.CODGERALOPER
            dtaentradasaida     = reg.DTAVDA
            vlrcustoliquido     = reg.VLRCUSTOLIQUIDO
            vlrdespoperacional  = reg.VLRDESPOPERACIONAL
            vlrlucratividade    = reg.VLRLUCRATIVIDADE

            Virgula = ""
            if I != 0: Virgula = ","
            I += 1

            Dados = Dados \
                    +Virgula \
                    +"('" \
                    +str(Id)+"','" \
                    +nroempresa+"','"\
                    +seqpessoa+"','"\
                    +nrosegmento+"','"\
                    +nroequipe+"','"\
                    +nrorepresentante+"','" \
                    +seqfamilia+"','" \
                    +seqproduto+"','" \
                    +codgeraloper+"','"\
                    +dtaentradasaida+"','"\
                    +vlrcustoliquido+"','"\
                    +vlrdespoperacional+"','"\
                    +vlrlucratividade+\
                    "')"

            if I == Parametro:
                sqlInsert = "insert into pbi.carga_custodiafamilia\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if TotalReg < Parametro:
                    Parametro = TotalReg
                I = 0
                Dados = ""
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MRL_PRODEMPSEG INNER JOIN "MAP_FAMEMBALAGEM", "MAP_FAMDIVISAO"
        ###############################################################################################################
        pga_conexao = fun.conect_postgresql()
        cursor = pga_conexao.cursor()
        cursor.execute("SELECT seqproduto FROM pbi.carga_produtoprecovenda")
        colunas = [col[0] for col in cursor.description]
        dados = cursor.fetchall()
        produto_preco_venda = pd.DataFrame(dados, columns=colunas)
        total_reg = produto_preco_venda["seqproduto"].count()

        if total_reg == 0:
            operacao = 1
            select_produto_preco_venda \
                = \
                """
                SELECT
		            DISTINCT	
		            a.nroempresa,	
                    a.seqproduto,
                    ROUND(a.precogernormal/c.qtdembalagem,2) AS precogernormal,
                    ROUND(a.precogerpromoc/c.qtdembalagem,2) AS precogerpromoc,
                    ROUND(
                    CASE
                    WHEN a.precogerpromoc = 0
                    THEN a.precogernormal
                    ELSE a.precogerpromoc
                    END / a.qtdembalagem,2) AS precogervenda,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao
                FROM implantacao.mrl_prodempseg a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = a.qtdembalagem
                INNER JOIN implantacao.map_famdivisao d ON d.seqfamilia = b.seqfamilia
                INNER JOIN implantacao.map_famembalagem e ON e.seqfamilia = b.seqfamilia AND e.qtdembalagem = d.padraoembcompra
                WHERE 1 = 1
                AND a.precogernormal > 0
                AND a.nrosegmento IN (1,3,4,5,6,7,8,9,10)
                AND a.statusvenda = 'A'
                ORDER BY 2 ASC                
                """
        else:
            operacao = 2
            select_produto_preco_venda \
                = \
                """
                SELECT
		            DISTINCT	
		            a.nroempresa,	
                    a.seqproduto,
                    ROUND(a.precogernormal/c.qtdembalagem,2) AS precogernormal,
                    ROUND(a.precogerpromoc/c.qtdembalagem,2) AS precogerpromoc,
                    ROUND(
                    CASE
                    WHEN a.precogerpromoc = 0
                    THEN a.precogernormal
                    ELSE a.precogerpromoc
                    END / a.qtdembalagem,2) AS precogervenda,
                    TO_DATE(a.dtaalteracao) AS dtaalteracao
                FROM implantacao.mrl_prodempseg a
                INNER JOIN implantacao.map_produto b ON b.seqproduto = a.seqproduto
                INNER JOIN implantacao.map_famembalagem c ON c.seqfamilia = b.seqfamilia AND c.qtdembalagem = a.qtdembalagem
                INNER JOIN implantacao.map_famdivisao d ON d.seqfamilia = b.seqfamilia
                INNER JOIN implantacao.map_famembalagem e ON e.seqfamilia = b.seqfamilia AND e.qtdembalagem = d.padraoembcompra
                WHERE 1 = 1
                AND a.precogernormal > 0
                AND a.nrosegmento IN (1,3,4,5,6,7,8,9,10)
                AND a.statusvenda = 'A'
                AND TO_DATE(a.dtaalteracao) >= SYSDATE-1
                ORDER BY 2 ASC               
                """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(select_produto_preco_venda)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        produto_preco_venda = pd.DataFrame.from_records(dados, columns=colunas)
        total_reg = produto_preco_venda["SEQPRODUTO"].count()

        print("Execultado processo na tabela de \"Cadastro Produtos Preco Venda\" ...")

        with open(NomeArquivo, "a") as arquivo:
            arquivo.write("» Execultado processo na tabela de \"Cadastro Produtos Preco Venda\" ...\n")
        arquivo.close()

        campos = "(nroempresa,seqproduto,precogernormal,precogerpromoc,precogervenda)"
        i = 0
        dados = ""
        parametro = 1000
        if total_reg < 1000:
            parametro = total_reg

        for index, reg in produto_preco_venda.iterrows():
            nroempresa = str(reg.NROEMPRESA)
            seqproduto = str(reg.SEQPRODUTO)
            precogernormal = str(reg.PRECOGERNORMAL)
            precogerpromoc = str(reg.PRECOGERPROMOC)
            precogervenda = str(reg.PRECOGERVENDA)

            if (i == 0):
                virgula = ""
            else:
                virgula = ","

            dados = dados + virgula + "('" + nroempresa + "','" + seqproduto + "','" + precogernormal + "','" + precogerpromoc + "','" + precogervenda + "')\n"

            i += 1

            if i == parametro:
                if operacao == 1:
                    sql_insert \
                        = "insert into pbi.carga_produtoprecovenda\n" + campos + "\nvalues\n" + dados + "\n"
                    fun.process_data(sql_insert)
                else:
                    sql_update \
                        = \
                        "UPDATE pbi.carga_produtoprecovenda " \
                        "SET " \
                        "precogernormal = '" + precogernormal + "'," \
                        "precogerpromoc = '" + precogerpromoc + "'," \
                        "precogervenda = '" + precogervenda + "' " \
                        "WHERE 1 = 1 " \
                        "AND nroempresa = '" + nroempresa + "' " \
                        "AND seqproduto = '" + seqproduto + "'"
                    fun.process_data(sql_update)
                total_reg -= i

                if total_reg < parametro:
                    parametro = total_reg
                i = 0

                dados = ""
        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAD_METAPERIODO INNER JOIN MAD_METAEQUIPEREP
        ###############################################################################################################
        print("Execultado processo na tabela de \"Metas Equipes\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Metas Equipes\" ...\n")
            Arquivo.close()

        dfpostgres = fun.postgres_consulta \
            (
                """
                select count(*) quantidade
                from pbi.carga_metaequipe
                where 1=1
                """
            )
        dfpostgres = pd.DataFrame(dfpostgres)
        tregpostgres = dfpostgres["quantidade"][0]

        if tregpostgres != 0:
            fun.process_data \
                (
                    """
                    delete
                    from pbi.carga_metaequipe
                    where 1=1
                    and dtainicial >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                    """
                 )
            query \
                = """ 
                  select
                        a.seqmetaperiodo,
                        a.nroempresa,
                        a.dtainicial,
                        a.dtafinal,
                        b.nroequipe,
                        b.metavlrvenda,
                        nvl(a.status,'A') status
                  from implantacao.mad_metaperiodo a
                  inner join implantacao.mad_metaequiperep b on b.seqmetaperiodo = a.seqmetaperiodo
                  where 1=1
                  and a.dtainicial >= add_months(trunc(sysdate,'mm'), -3)
                  order by a.seqmetaperiodo asc,b.nroequipe asc
                  """
        else:
            query \
                = """ 
                  select
                        a.seqmetaperiodo,
                        a.nroempresa,
                        a.dtainicial,
                        a.dtafinal,
                        b.nroequipe,
                        b.metavlrvenda,
                        nvl(a.status,'A') status
                  from implantacao.mad_metaperiodo a
                  inner join implantacao.mad_metaequiperep b on b.seqmetaperiodo = a.seqmetaperiodo
                  where 1=1
                  and a.dtainicial >= add_months(trunc(sysdate,'YYYY'), -48)
                  order by a.seqmetaperiodo asc,b.nroequipe asc
                  """
        cur = con_oracle.cursor()
        cur.execute(query)
        col = [reg[0] for reg in cur.description]
        lin = cur.fetchall()
        DFrame = pd.DataFrame.from_records(lin, columns=col)
        DFrame["DTAINICIAL"] = pd.DatetimeIndex(DFrame["DTAINICIAL"]).date
        DFrame["DTAFINAL"] = pd.DatetimeIndex(DFrame["DTAFINAL"]).date

        for index, reg in DFrame.iterrows():
            Campos \
                = """
                  (
                  seqmetaperiodo,
                  nroempresa,
                  dtainicial,
                  dtafinal,
                  nroequipe,
                  metavlrvenda,
                  status
                  )
                  """
            Values \
                = \
                "('"\
                +str(reg.SEQMETAPERIODO)+"','"\
                +str(reg.NROEMPRESA)+"','"\
                +str(reg.DTAINICIAL)+"','"\
                +str(reg.DTAFINAL)+"','"\
                +str(reg.NROEQUIPE)+"','"\
                +str(reg.METAVLRVENDA)+"','"\
                +reg.STATUS+\
                "')"

            Insert = "insert into pbi.carga_metaequipe"+Campos+"values"+Values+""
            fun.process_data(Insert)
        cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAXV_ABCDISTRIBBASE - ORACLE
        ###############################################################################################################
        print("Execultado processo na tabela de \"ABC Distribuicao Base\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"ABC Distribuicao Base\" ...\n")
            Arquivo.close()

        df = fun.postgres_consulta("select count(*) quantidade from pbi.carga_abcdistribuicaobase")
        TotalReg = df["quantidade"][0]
        Id = 0

        if TotalReg != 0:
            qryDelete = """
                        delete
                        from pbi.carga_abcdistribuicaobase
                        where 1=1
                        and dtavda >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                        """
            qrySelect = """
                        select id 
                        from pbi.carga_abcdistribuicaobase
                        order by id asc
                        """

            fun.process_data(qryDelete)
            df = fun.postgres_consulta(qrySelect)
            Id = df[["id"]].tail(1) # Peda o último registro do DataFrame
            Id = Id["id"].values    # Peda o valor do DataFrame
            Id = Id[0]              # Index 0

            Select_MaxvABCDistribBase = \
            """
            select
            t1.nroempresa,
            t1.seqpessoa,
            t1.nrosegmento,
            nvl(t1.nroequipe,0) nroequipe,
            t1.nrorepresentante,
            t1.nrodivisao,
            t1.tipnotafiscal,
            t1.codgeraloper,
            t1.nrodocto,
            nvl(t1.seriedocto,'0') seriedocto,
            to_char(t1.dtahorlancto,'dd/mm/yyyy') dtahorlancto,
            to_char(t1.dtavda,'dd/mm/yyyy') dtavda,
            nvl(to_char(t1.dtavencimento,'dd/mm/yyyy'),to_char(t1.dtavda,'dd/mm/yyyy')) dtavencimento,
            nvl(t2.seqfamilia, 0) seqfamilia,
            t1.seqproduto,
            t1.seqprodutocusto,
            nvl(t1.nfreferencianro,0) nfreferencianro,
            nvl(t1.nfreferenciaserie,'0') nfreferenciaserie,
            nvl(round(sum(t1.qtditem)),0) qtditembruto,
            nvl(round(sum(t1.qtddevolitem)),0) qtditemdevolucao,
            nvl(round(sum(t1.qtditem - t1.qtddevolitem)),0) qtditemliquido,
            nvl(round(sum(t1.vlritem - t1.vlricmsst - t1.vlrfcpst),2),0) vlrvendabruta,
            nvl(round(sum(t1.vlritem - t1.vlricmsst - t1.vlrfcpst - t1.vlrdevolitem + t1.vlrdevolicmsst + t1.dvlrfcpst),2),0) vlrvendaliquida,
            nvl(round(sum(t1.vlrdevolitem - t1.vlrdevolicmsst - t1.dvlrfcpst),2),0) vlrdevolucao,
            nvl(round(sum(t1.vlrdescitem - t1.vlrdescdevolitem),2),0) vlrdesconto,
            nvl(round(sum(t1.icmsitem - t1.icmsdevolitem),2),0) vlricms,
            nvl(round(sum(t1.vlripiitem - t1.vlripidevolitem),2),0) vlripi,
            nvl(round(sum(t1.pisitem - t1.pisdevolitem),2),0) vlrpis,
            nvl(round(sum(t1.cofinsitem - t1.cofinsdevolitem),2),0) vlrcofins,
            nvl(round(sum(t1.vlrtotcomissaoitem - t1.vlrtotcomissaoitemdevol),2),0) vlrcomissao,
            nvl(round(sum(t1.vlrdespoperacionalitem - t1.vlrdespoperacionalitemdevol),2),0) vlrdespesaoperacional,
            nvl(round(sum(t1.vlripiitem - t1.vlripidevolitem + (t1.icmsitem - t1.icmsdevolitem) + (t1.pisitem - t1.pisdevolitem) + (t1.cofinsitem - t1.cofinsdevolitem)),2),0) vlrimposto
            from implantacao.maxv_abcdistribbase t1
            inner join implantacao.map_produto t2 on t2.seqproduto = t1.seqproduto
            where 1 = 1
            and t1.nrosegmento in (1,3,4,5,6,7,8,9,10)
            and decode(t1.tiptabela,'S',t1.cgoacmcompravenda,t1.acmcompravenda) in ('S','I')
            and t1.dtavda >= add_months(trunc(sysdate,'mm'), -3)
            group by 
            t1.nroempresa,t1.seqpessoa,t1.nrosegmento,t1.nroequipe,t1.nrorepresentante,t1.nrodivisao,t1.tipnotafiscal,t1.codgeraloper,
            t1.nrodocto,t1.seriedocto,to_char(t1.dtahorlancto, 'dd/mm/yyyy'),to_char(t1.dtavda, 'dd/mm/yyyy'),
            nvl(to_char(t1.dtavencimento, 'dd/mm/yyyy'), to_char(t1.dtavda, 'dd/mm/yyyy')),t1.seqproduto,t1.seqprodutocusto,
            t1.nfreferencianro,t1.nfreferenciaserie,t2.seqfamilia
            order by to_date(dtavda,'dd/mm/yyyy') asc
            """
        else:
            Select_MaxvABCDistribBase = \
            """
            select
            t1.nroempresa,
            t1.seqpessoa,
            t1.nrosegmento,
            nvl(t1.nroequipe,0) nroequipe,
            t1.nrorepresentante,
            t1.nrodivisao,
            t1.tipnotafiscal,
            t1.codgeraloper,
            t1.nrodocto,
            nvl(t1.seriedocto,'0') seriedocto,
            to_char(t1.dtahorlancto,'dd/mm/yyyy') dtahorlancto,
            to_char(t1.dtavda,'dd/mm/yyyy') dtavda,
            nvl(to_char(t1.dtavencimento,'dd/mm/yyyy'),to_char(t1.dtavda,'dd/mm/yyyy')) dtavencimento,
            nvl(t2.seqfamilia, 0) seqfamilia,
            t1.seqproduto,
            t1.seqprodutocusto,
            nvl(t1.nfreferencianro,0) nfreferencianro,
            nvl(t1.nfreferenciaserie,'0') nfreferenciaserie,
            nvl(round(sum(t1.qtditem)),0) qtditembruto,
            nvl(round(sum(t1.qtddevolitem)),0) qtditemdevolucao,
            nvl(round(sum(t1.qtditem - t1.qtddevolitem)),0) qtditemliquido,
            nvl(round(sum(t1.vlritem - t1.vlricmsst - t1.vlrfcpst),2),0) vlrvendabruta,
            nvl(round(sum(t1.vlritem - t1.vlricmsst - t1.vlrfcpst - t1.vlrdevolitem + t1.vlrdevolicmsst + t1.dvlrfcpst),2),0) vlrvendaliquida,
            nvl(round(sum(t1.vlrdevolitem - t1.vlrdevolicmsst - t1.dvlrfcpst),2),0) vlrdevolucao,
            nvl(round(sum(t1.vlrdescitem - t1.vlrdescdevolitem),2),0) vlrdesconto,
            nvl(round(sum(t1.icmsitem - t1.icmsdevolitem),2),0) vlricms,
            nvl(round(sum(t1.vlripiitem - t1.vlripidevolitem),2),0) vlripi,
            nvl(round(sum(t1.pisitem - t1.pisdevolitem),2),0) vlrpis,
            nvl(round(sum(t1.cofinsitem - t1.cofinsdevolitem),2),0) vlrcofins,
            nvl(round(sum(t1.vlrtotcomissaoitem - t1.vlrtotcomissaoitemdevol),2),0) vlrcomissao,
            nvl(round(sum(t1.vlrdespoperacionalitem - t1.vlrdespoperacionalitemdevol),2),0) vlrdespesaoperacional,
            nvl(round(sum(t1.vlripiitem - t1.vlripidevolitem + (t1.icmsitem - t1.icmsdevolitem) + (t1.pisitem - t1.pisdevolitem) + (t1.cofinsitem - t1.cofinsdevolitem)),2),0) vlrimposto
            from implantacao.maxv_abcdistribbase t1
            inner join implantacao.map_produto t2 on t2.seqproduto = t1.seqproduto
            where 1 = 1
            and t1.nrosegmento in (1,3,4,5,6,7,8,9,10)
            and decode(t1.tiptabela,'S',t1.cgoacmcompravenda,t1.acmcompravenda) in ('S','I')
            and t1.dtavda >= add_months(trunc(sysdate,'yyyy'), -48)
            --and extract(year from t1.dtavda) = 2022
            group by 
            t1.nroempresa,t1.seqpessoa,t1.nrosegmento,t1.nroequipe,t1.nrorepresentante,t1.nrodivisao,t1.tipnotafiscal,t1.codgeraloper,
            t1.nrodocto,t1.seriedocto,to_char(t1.dtahorlancto, 'dd/mm/yyyy'),to_char(t1.dtavda, 'dd/mm/yyyy'),
            nvl(to_char(t1.dtavencimento, 'dd/mm/yyyy'), to_char(t1.dtavda, 'dd/mm/yyyy')),t1.seqproduto,t1.seqprodutocusto,
            t1.nfreferencianro,t1.nfreferenciaserie,t2.seqfamilia
            order by to_date(dtavda,'dd/mm/yyyy') asc
            """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(Select_MaxvABCDistribBase)
        dados = cursor.fetchall()
        colunas = [row[0] for row in cursor.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)
        for Coluna in colunas: df = df.astype({Coluna: str})
        TotalReg = df["SEQPESSOA"].count()

        Campos = \
                """
                (id,nroempresa,seqpessoa,nrosegmento,nroequipe,nrorepresentante,nrodivisao,tipnotafiscal,codgeraloper,
                nrodocto,seriedocto,dtahorlancto,dtavda,dtavencimento,seqfamilia,seqproduto,seqprodutocusto,nfreferencianro,
                nfreferenciaserie,qtditembruta,qtditemliquida,qtditemdevolucao,vlrvendabruta,vlrvendaliquida,vlrdevolucao,
                vlrdesconto,vlricms,vlripi,vlrpis,vlrcofins,vlrcomissao,vlrdespesaoperacional,vlrimposto)
                """
        I = 0
        Dados = ""
        Parametro = 1000
        if TotalReg < 1000: Parametro = TotalReg

        for index, reg in df.iterrows():
            Id = Id + 1
            nroempresa                  = reg.NROEMPRESA
            seqpessoa                   = reg.SEQPESSOA
            nrosegmento                 = reg.NROSEGMENTO
            nroequipe                   = reg.NROEQUIPE
            nrorepresentante            = reg.NROREPRESENTANTE
            nrodivisao                  = reg.NRODIVISAO
            tipnotafiscal               = reg.TIPNOTAFISCAL
            codgeraloper                = reg.CODGERALOPER
            nrodocto                    = reg.NRODOCTO
            seriedocto                  = reg.SERIEDOCTO
            dtahoralancto               = reg.DTAHORLANCTO
            dtavenda                    = reg.DTAVDA
            dtavencimento               = reg.DTAVENCIMENTO
            seqfamilia                  = reg.SEQFAMILIA
            seqproduto                  = reg.SEQPRODUTO
            seqprodutocusto             = reg.SEQPRODUTOCUSTO
            nfreferencianro             = reg.NFREFERENCIANRO
            nfreferenciaserie           = reg.NFREFERENCIASERIE
            qtditembruto                = reg.QTDITEMBRUTO
            qtditemliquido              = reg.QTDITEMLIQUIDO
            qtditemdevolucao            = reg.QTDITEMDEVOLUCAO
            vlrvendabruta               = reg.VLRVENDABRUTA
            vlrvendaliquida             = reg.VLRVENDALIQUIDA
            vlrdevolucao                = reg.VLRDEVOLUCAO
            vlrdesconto                 = reg.VLRDESCONTO
            vlricms                     = reg.VLRICMS
            vlripi                      = reg.VLRIPI
            vlrpis                      = reg.VLRPIS
            vlrcofins                   = reg.VLRCOFINS
            vlrcomissao                 = reg.VLRCOMISSAO
            vlrdespesaoperacional       = reg.VLRDESPESAOPERACIONAL
            vlrimposto                  = reg.VLRIMPOSTO

            Virgula = ""
            if I != 0: Virgula = ","
            I += 1

            Dados = Dados \
                    +Virgula \
                    +"('" \
                    +str(Id)+"','" \
                    +nroempresa+"','" \
                    +seqpessoa+"','" \
                    +nrosegmento+"','" \
                    +nroequipe+"','" \
                    +nrorepresentante+"','" \
                    +nrodivisao+"','" \
                    +tipnotafiscal+"','" \
                    +codgeraloper+"','" \
                    +nrodocto+"','" \
                    +seriedocto+"','" \
                    +dtahoralancto+"','" \
                    +dtavenda+"','" \
                    +dtavencimento+"','" \
                    +seqfamilia+ "','" \
                    +seqproduto+"','" \
                    +seqprodutocusto+"','" \
                    +nfreferencianro+"','" \
                    +nfreferenciaserie+"','" \
                    +qtditembruto+"','" \
                    +qtditemliquido+"','" \
                    +qtditemdevolucao+"','" \
                    +vlrvendabruta+"','" \
                    +vlrvendaliquida+"','" \
                    +vlrdevolucao+"','" \
                    +vlrdesconto+"','" \
                    +vlricms+"','" \
                    +vlripi+"','" \
                    +vlrpis+"','" \
                    +vlrcofins+"','" \
                    +vlrcomissao+"','" \
                    +vlrdespesaoperacional+"','" \
                    +vlrimposto+ \
                    "')"

            if I == Parametro:
                sqlInsert = "insert into pbi.carga_abcdistribuicaobase\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if TotalReg < Parametro:
                    Parametro = TotalReg
                I = 0
                Dados = ""

        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_PEDVENDA - ORACLE
        ###############################################################################################################
        print("Execultado processo na tabela de \"Pedido\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Pedido\" ...\n")
        Arquivo.close()

        Parametro = 1000

        qry = "select count(*) quantidade from pbi.carga_pedido where 1=1"

        con = fun.conect_postgresql()
        cur = con.cursor()
        cur.execute(qry)
        col = [c[0] for c in cur.description]
        dados = cur.fetchall()
        dfPostgres = pd.DataFrame.from_records(dados, columns=col)
        TotalReg = dfPostgres["quantidade"][0]

        if TotalReg == 0:
            tregPostgres = 0
        else:
            tregPostgres = TotalReg

        if tregPostgres != 0:
            fun.process_data(
                """
                delete 
                from pbi.carga_pedido 
                where 1=1 
                and dtainclusao >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                """
            )
            Query = \
            """
            select
                nroempresa,
                seqpessoa,
                nrosegmento,
                nrorepresentante,
                nropedvenda,
                codgeraloper,
                to_char(dtainclusao,'dd/mm/yyyy') dtainclusao,
                nrotabvenda,
                nroformapagto,
                situacaoped,
                indentregaretira
            from implantacao.mad_pedvenda
            where 1=1
            and dtainclusao >= add_months(trunc(sysdate,'mm'), -3)
            order by dtainclusao asc
            """
        else:
            Query = \
            """
            select
                nroempresa,
                seqpessoa,
                nrosegmento,
                nrorepresentante,
                nropedvenda,
                codgeraloper,
                to_char(dtainclusao,'dd/mm/yyyy') dtainclusao,
                nrotabvenda,
                nroformapagto,
                situacaoped,
                indentregaretira
            from implantacao.mad_pedvenda
            where 1=1
            and dtainclusao >= add_months(trunc(sysdate,'YYYY'), -48)
            order by dtainclusao asc
            """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.prefetchrows = 1000
        Cur.arraysize = 100
        Cur.execute(Query)
        Col = [Nome[0] for Nome in Cur.description]
        Reg = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Reg, columns=Col)
        for Coluna in Col: DFrame = DFrame.astype({Coluna: str})
        QRegistro = DFrame["NROPEDVENDA"].count()
        I = 0
        Campos = """
                 (
                 nroempresa,
                 seqpessoa,
                 nrosegmento,
                 nrorepresentante,
                 nropedvenda,
                 codgeraloper,
                 dtainclusao,
                 nrotabvenda,
                 nroformapagto,
                 situacaoped,
                 indentregaretira
                 )
                 """
        Dados = ""

        for index, reg in DFrame.iterrows():

            if I == 0:
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados \
                  + Virgula \
                  + "('" \
                  + reg.NROEMPRESA + "','" \
                  + reg.SEQPESSOA + "','" \
                  + reg.NROSEGMENTO + "','" \
                  + reg.NROREPRESENTANTE + "','" \
                  + reg.NROPEDVENDA + "','" \
                  + reg.CODGERALOPER + "','" \
                  + reg.DTAINCLUSAO + "','" \
                  + reg.NROTABVENDA + "','" \
                  + reg.NROFORMAPAGTO + "','" \
                  + reg.SITUACAOPED + "','" \
                  + reg.INDENTREGARETIRA \
                  + "')\n"

            I += 1

            if I == Parametro:
                sqlInsert = \
                "insert into pbi.carga_pedido\n" \
                ""+Campos+"\n" \
                "values\n" \
                ""+Dados+""
                fun.process_data(sqlInsert)
                QRegistro -= I
                I = 0
                Dados = ""

            if QRegistro < Parametro:
                Parametro = QRegistro
                I = 0
                Dados = ""

        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_PEDVENDAITEM - ORACLE
        ###############################################################################################################
        print("Execultado processo na tabela de \"Item do Pedido\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Item do Pedido\" ...\n")
        Arquivo.close()

        Parametro = 1000

        qry = "select count(*) quantidade from pbi.carga_pedidoitem where 1=1"

        con = fun.conect_postgresql()
        cur = con.cursor()
        cur.execute(qry)
        col = [c[0] for c in cur.description]
        dados = cur.fetchall()
        dfPostgres = pd.DataFrame.from_records(dados, columns=col)
        TotalReg = dfPostgres["quantidade"][0]

        if TotalReg == 0:
            tregPostgres = 0
        else:
            tregPostgres = TotalReg

        if tregPostgres != 0:
            fun.process_data(
                """
                delete 
                from pbi.carga_pedidoitem 
                where 1=1 
                and dtainclusao >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                """
            )
            query = """
                    select 
                    t1.nroempresa,
                    t1.nropedvenda,
                    t1.seqpedvendaitem,
                    nvl(t2.seqfamilia, 0) seqfamilia,
                    t1.seqproduto,
                    nvl(t1.qtdembalagem,0) qtdembalagem,
                    nvl(t1.qtdpedida,0) qtdpedida,
                    nvl(t1.qtdatendida,0) qtdatendida,
                    nvl(t1.vlrembtabpreco,0) vlrembtabpreco,
                    nvl(t1.vlrembtabpromoc,0) vlrembtabpromoc,
                    nvl(t1.vlrembinformado,0) vlrembinformado,
                    nvl(t1.vlrembdesconto,0) vlrembdesconto,
                    nvl(t1.vlrtotcomissao,0) vlrtotcomissao,
                    nvl(t1.percomissao,0) percomissao,
                    t1.dtainclusao
                    from implantacao.mad_pedvendaitem t1
                    left join implantacao.map_produto t2 on t2.seqproduto = t1.seqproduto
                    where 1=1
                    and t1.dtainclusao >= add_months(trunc(sysdate,'mm'), -3)
                    order by t1.dtainclusao asc
                    """
        else:
            query = """
                    select 
                    t1.nroempresa,
                    t1.nropedvenda,
                    t1.seqpedvendaitem,
                    nvl(t2.seqfamilia, 0) seqfamilia,
                    t1.seqproduto,
                    nvl(t1.qtdembalagem,0) qtdembalagem,
                    nvl(t1.qtdpedida,0) qtdpedida,
                    nvl(t1.qtdatendida,0) qtdatendida,
                    nvl(t1.vlrembtabpreco,0) vlrembtabpreco,
                    nvl(t1.vlrembtabpromoc,0) vlrembtabpromoc,
                    nvl(t1.vlrembinformado,0) vlrembinformado,
                    nvl(t1.vlrembdesconto,0) vlrembdesconto,
                    nvl(t1.vlrtotcomissao,0) vlrtotcomissao,
                    nvl(t1.percomissao,0) percomissao,
                    t1.dtainclusao
                    from implantacao.mad_pedvendaitem t1
                    left join implantacao.map_produto t2 on t2.seqproduto = t1.seqproduto
                    where 1=1
                    and t1.dtainclusao >= add_months(trunc(sysdate,'yyyy'), -48)
                    order by t1.dtainclusao asc
                    """
        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(query)
        Col = [Nome[0] for Nome in Cur.description]
        Dados = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Dados, columns=Col)

        for Coluna in Col: DFrame = DFrame.astype({Coluna: str})
        QRegistro = DFrame["NROPEDVENDA"].count()
        I = 0
        Campos = """
                 (
                 nroempresa,
                 nropedvenda,
                 seqpedvendaitem,
                 seqfamilia,
                 seqproduto,
                 qtdembalagem,
                 qtdpedida,
                 qtdatendida,
                 vlrembtabpreco,
                 vlrembtabpromoc,
                 vlrembinformado,
                 vlrembdesconto,
                 vlrtotcomissao,
                 percomissao,
                 dtainclusao)
                 """
        Dados = ""

        for index, reg in DFrame.iterrows():

            if (I == 0):
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados \
                  + Virgula \
                  + "('" \
                  + reg.NROEMPRESA + "','" \
                  + reg.NROPEDVENDA + "','" \
                  + reg.SEQPEDVENDAITEM + "','" \
                  + reg.SEQFAMILIA + "','" \
                  + reg.SEQPRODUTO + "','" \
                  + reg.QTDEMBALAGEM + "','" \
                  + reg.QTDPEDIDA + "','" \
                  + reg.QTDATENDIDA + "','" \
                  + reg.VLREMBTABPRECO + "','" \
                  + reg.VLREMBTABPROMOC + "','" \
                  + reg.VLREMBINFORMADO + "','" \
                  + reg.VLREMBDESCONTO + "','" \
                  + reg.VLRTOTCOMISSAO + "','" \
                  + reg.PERCOMISSAO + "','" \
                  + reg.DTAINCLUSAO \
                  + "')\n"

            I += 1

            if I == Parametro:
                sqlInsert = \
                    "insert into pbi.carga_pedidoitem\n"\
                    ""+Campos+"\n"\
                    "values\n"\
                    ""+Dados+""
                fun.process_data(sqlInsert)
                QRegistro -= I
                I = 0
                Dados = ""

            if QRegistro < Parametro:
                Parametro = QRegistro
                I = 0
                Dados = ""
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA MAD_REPCCFLEX - ORACLE
        ###############################################################################################################
        print("Execultado processo na tabela de \"Representante Conta Corrente Flex\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Representante Conta Corrente Flex\" ...\n")
        Arquivo.close()

        Parametro = 1000

        qry = "select count(*) quantidade from pbi.carga_representantectaflex where 1=1"

        con = fun.conect_postgresql()
        cur = con.cursor()
        cur.execute(qry)
        col = [c[0] for c in cur.description]
        dados = cur.fetchall()
        dfPostgres = pd.DataFrame.from_records(dados, columns=col)
        TotalReg = dfPostgres["quantidade"][0]

        if TotalReg == 0:
            tregPostgres = 0
        else:
            tregPostgres = TotalReg

        if tregPostgres != 0:
            fun.process_data(
                """
                delete 
                from pbi.carga_representantectaflex 
                where 1=1 
                and dtalancamento >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                """
            )
            query = """
                    select 
                    a.seqlanctoflex,
                    a.nroempresa,
                    nvl(b.nrosegmento,0) nrosegmento,
                    nvl(b.nroequipe,0) nroequipe,
                    nvl(a.nropedvenda,0) nropedvenda,
                    a.nrorepresentante,
                    a.tipolancamento,
                    to_char(a.dtalancamento,'dd/mm/yyyy') dtalancamento,
                    to_char(a.dtahorlancto,'dd/mm/yyyy') dtahorlancto,
                    a.valor,
                    a.usulancto,
                    a.historico,
                    a.situacaolancto 
                    from implantacao.mad_repccflex a
                    left join implantacao.mad_representante b on b.nroempresa = a.nroempresa and b.nrorepresentante = a.nrorepresentante
                    where 1=1
                    and a.dtalancamento >= add_months(trunc(sysdate,'mm'), -3)
                    order by a.dtalancamento asc
                    """
        else:
            query = """
                    select 
                    a.seqlanctoflex,
                    a.nroempresa,
                    nvl(b.nrosegmento,0) nrosegmento,
                    nvl(b.nroequipe,0) nroequipe,
                    nvl(a.nropedvenda,0) nropedvenda,
                    a.nrorepresentante,
                    a.tipolancamento,
                    to_char(a.dtalancamento,'dd/mm/yyyy') dtalancamento,
                    to_char(a.dtahorlancto,'dd/mm/yyyy') dtahorlancto,
                    a.valor,
                    a.usulancto,
                    a.historico,
                    a.situacaolancto 
                    from implantacao.mad_repccflex a
                    left join implantacao.mad_representante b on b.nroempresa = a.nroempresa and b.nrorepresentante = a.nrorepresentante
                    where 1=1
                    and a.dtalancamento >= add_months(trunc(sysdate,'yyyy'), -48)
                    order by a.dtalancamento asc
                    """

        cOracle = fun.conect_oracle()
        Cur = cOracle.cursor()
        Cur.execute(query)
        Col = [Nome[0] for Nome in Cur.description]
        Dados = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Dados, columns=Col)
        for Coluna in Col: DFrame = DFrame.astype({Coluna: str})
        QRegistro = DFrame["SEQLANCTOFLEX"].count()
        I = 0
        Campos = """
                 (
                 seqlanctoflex,
                 nroempresa,
                 nrosegmento,
                 nroequipe,
                 nropedvenda,
                 nrorepresentante,
                 tipolancamento,
                 dtalancamento,
                 dtahorlancto,
                 valor,
                 usulancto,
                 historico,
                 situacaolancto
                 )
                 """

        Dados = ""

        for index, reg in DFrame.iterrows():

            if I == 0:
                Virgula = ""
            else:
                Virgula = ","

            Dados = Dados \
                  +Virgula \
                  +"('" \
                  +reg.SEQLANCTOFLEX+ "','" \
                  +reg.NROEMPRESA+ "','" \
                  +reg.NROSEGMENTO+ "','" \
                  +reg.NROEQUIPE+ "','" \
                  +reg.NROPEDVENDA+ "','" \
                  +reg.NROREPRESENTANTE+ "','" \
                  +reg.TIPOLANCAMENTO+ "','" \
                  +reg.DTALANCAMENTO+ "','" \
                  +reg.DTAHORLANCTO+ "','" \
                  +reg.VALOR+"','" \
                  +reg.USULANCTO.title()+"','" \
                  +reg.HISTORICO.title()+"','" \
                  +reg.SITUACAOLANCTO \
                  +"')\n"

            I += 1

            if I == Parametro:
                sqlInsert = \
                    "insert into pbi.carga_representantectaflex\n" \
                    ""+Campos+"\n" \
                    "values\n" \
                    ""+Dados+""
                fun.process_data(sqlInsert)
                QRegistro -= I
                I = 0
                Dados = ""

            if QRegistro < Parametro:
                Parametro = QRegistro
                I = 0
                Dados = ""
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DA TABELA FI_TITULO - ORACLE
        ###############################################################################################################

        print("Execultado processo na tabela de \"Lançamentos de Titulos\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Lançamentos de Titulos\" ...\n")
        Arquivo.close()

        pga_conexao = fun.conect_postgresql()
        cursor = pga_conexao.cursor()
        cursor.execute("select count(*) quantidade from pbi.carga_titulo")
        dados = cursor.fetchall()
        colunas = [col[0] for col in cursor.description]
        df = pd.DataFrame.from_records(dados, columns=colunas)
        TotalReg = df["quantidade"][0]

        if TotalReg == 0:
            query \
                = """
                  select 
                        t1.nroempresa,
                        nvl(t3.nrosegmento,0) nrosegmento,
                        nvl(t3.nroequipe,0) nroequipe,
                        nvl(t3.nrorepresentante,0) nrorepresentante,
                        t1.seqpessoa,
                        t1.seqtitulo,
                        t1.nrotitulo,
                        t1.nrobanco,
                        t1.serietitulo,
                        t1.codespecie,
                        t1.nroparcela,
                        t1.abertoquitado,
                        t1.situacao,
                        t1.tipovencoriginal,
                        t1.obrigdireito,
                        t1.dtaemissao,
                        t1.dtainclusao,
                        t1.dtamovimento,
                        t1.dtavencimento,
                        t1.dtaprogramada,
                        nvl(t1.dtaquitacao,to_date('01/01/1899','dd/mm/yyyy')) dtaquitacao,
                        t1.qtdparcela,
                        nvl(t1.vlroriginal,0) vlroriginal,
                        nvl(t1.vlrnominal,0) vlrnominal,
                        nvl(t1.vlrpago,0) vlrpago,
                        nvl(t1.vlrtarifa,0) vlrtarifa,
                        nvl(t1.vlrjuromora,0) vlrjuromora,
                        nvl(t1.vlrmulta,0) vlrmulta,
                        nvl(t1.vlrdesccomercial,0) vlrdesccomercial
                    from implantacao.fi_titulo t1
                    left join implantacao.fi_titrepres t2 on t2.seqpessoa = t1.seqpessoa AND t2.seqtitulo = t1.seqtitulo
                    left join implantacao.mad_representante t3 on t3.nrorepresentante = t2.nrorepresentante
                    where 1=1
                    and t1.dtaemissao >= add_months(trunc(sysdate,'yyyy'), -48)
                    order by t1.dtaemissao asc
                  """
        else:
            fun.process_data(
                """
                delete 
                from pbi.carga_titulo 
                where 1=1 
                and dtaemissao >= date_trunc('month',(now() - '3 month'::interval)::timestamp)
                """
            )
            query \
                =   """ 
                  select 
                        t1.nroempresa,
                        nvl(t3.nrosegmento,0) nrosegmento,
                        nvl(t3.nroequipe,0) nroequipe,
                        nvl(t3.nrorepresentante,0) nrorepresentante,
                        t1.seqpessoa,
                        t1.seqtitulo,
                        t1.nrotitulo,
                        t1.nrobanco,
                        t1.serietitulo,
                        t1.codespecie,
                        t1.nroparcela,
                        t1.abertoquitado,
                        t1.situacao,
                        t1.tipovencoriginal,
                        t1.obrigdireito,
                        t1.dtaemissao,
                        t1.dtainclusao,
                        t1.dtamovimento,
                        t1.dtavencimento,
                        t1.dtaprogramada,
                        nvl(t1.dtaquitacao,to_date('01/01/1899','dd/mm/yyyy')) dtaquitacao,
                        t1.qtdparcela,
                        nvl(t1.vlroriginal,0) vlroriginal,
                        nvl(t1.vlrnominal,0) vlrnominal,
                        nvl(t1.vlrpago,0) vlrpago,
                        nvl(t1.vlrtarifa,0) vlrtarifa,
                        nvl(t1.vlrjuromora,0) vlrjuromora,
                        nvl(t1.vlrmulta,0) vlrmulta,
                        nvl(t1.vlrdesccomercial,0) vlrdesccomercial
                    from implantacao.fi_titulo t1
                    left join implantacao.fi_titrepres t2 on t2.seqpessoa = t1.seqpessoa AND t2.seqtitulo = t1.seqtitulo
                    left join implantacao.mad_representante t3 on t3.nrorepresentante = t2.nrorepresentante
                    where 1=1
                    and t1.dtaemissao >= add_months(trunc(sysdate,'mm'), -3)
                    order by t1.dtaemissao asc
                    """
        ora_conexao = fun.conect_oracle()
        cursor = ora_conexao.cursor()
        cursor.execute(query)
        dados = cursor.fetchall()
        colunasDB = [row[0] for row in cursor.description]
        colunas = []
        for i in colunasDB: colunas.append(i.lower())
        df = pd.DataFrame.from_records(dados, columns=colunas)
        for Coluna in colunas: df = df.astype({Coluna: str})
        TotalReg = df["seqtitulo"].count()

        Campos = """
                 (
                 nroempresa,
                 nrosegmento,
                 nroequipe,
                 nrorepresentante,
                 seqpessoa,
                 seqtitulo,
                 nrotitulo,
                 nrobanco,
                 serietitulo,
                 codespecie,
                 nroparcela,
                 tipovencoriginal,
                 abertoquitado,
                 situacao,
                 obrigdireito,
                 dtaemissao,
                 dtainclusao,
                 dtamovimento,
                 dtavencimento,
                 dtaprogramada,
                 dtaquitacao,
                 qtdparcela,
                 vlroriginal,
                 vlrnominal,
                 vlrpago,
                 vlrtarifa,
                 vlrjuromora,
                 vlrmulta,
                 vlrdesccomercial
                 )
                 """
        I = 0
        Dados = ""
        Parametro = 1000
        if TotalReg < 1000: Parametro = TotalReg

        for index, reg in df.iterrows():
            nroempresa = reg.nroempresa
            nrosegmento = reg.nrosegmento
            nroequipe = reg.nroequipe
            nrorepresentante = reg.nrorepresentante
            seqpessoa = reg.seqpessoa
            seqtitulo = reg.seqtitulo
            nrotitulo = reg.nrotitulo
            nrobanco = reg.nrobanco
            serietitulo = reg.serietitulo
            codespecie = reg.codespecie
            nroparcela = reg.nroparcela
            tipovencoriginal = reg.tipovencoriginal
            abertoquitado = reg.abertoquitado
            situacao = reg.situacao
            obrigdireito = reg.obrigdireito
            dtaemissao = reg.dtaemissao
            dtainclusao = reg.dtainclusao
            dtamovimento = reg.dtamovimento
            dtavencimento = reg.dtavencimento
            dtaprogramada = reg.dtaprogramada
            dtaquitacao = reg.dtaquitacao
            qtdparcela = reg.qtdparcela
            vlroriginal = reg.vlroriginal
            vlrnominal = reg.vlrnominal
            vlrpago = reg.vlrpago
            vlrtarifa = reg.vlrtarifa
            vlrjuromora = reg.vlrjuromora
            vlrmulta = reg.vlrmulta
            vlrdesccomercial = reg.vlrdesccomercial

            Virgula = ""
            if I != 0: Virgula = ","

            Dados = Dados \
                    +Virgula \
                    +"('" \
                    +nroempresa+"','" \
                    +nrosegmento+"','" \
                    +nroequipe+"','" \
                    +nrorepresentante+"','" \
                    +seqpessoa+"','" \
                    +seqtitulo+"','" \
                    +nrotitulo+"','" \
                    +nrobanco+"','" \
                    +serietitulo+"','" \
                    +codespecie+"','" \
                    +nroparcela+"','" \
                    +tipovencoriginal+"','" \
                    +abertoquitado+"','" \
                    +situacao+"','" \
                    +obrigdireito+"','" \
                    +dtaemissao+"','" \
                    +dtainclusao+"','" \
                    +dtamovimento+"','" \
                    +dtavencimento+"','" \
                    +dtaprogramada+"','" \
                    +dtaquitacao+"','" \
                    +qtdparcela+"','" \
                    +vlroriginal+"','" \
                    +vlrnominal+"','" \
                    +vlrpago+"','" \
                    +vlrtarifa+"','" \
                    +vlrjuromora+"','" \
                    +vlrmulta+"','" \
                    +vlrdesccomercial+"'" \
                    ")\n"

            I += 1

            if I == Parametro:
                sqlInsert = "insert into pbi.carga_titulo\n" + Campos + "\nvalues\n" + Dados + "\n"
                fun.process_data(sqlInsert)
                TotalReg -= I
                if TotalReg < Parametro: Parametro = TotalReg
                I = 0
                Dados = ""

        cursor.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MRL_PRODUTOEMPRESA INNER JOIN MAP_PRODUTO
        ###############################################################################################################
        print("Execultado processo na tabela de \"Estoques\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Estoques\" ...\n")
            Arquivo.close()

        dfPostgres = fun.postgres_consulta\
            (
                """
                select count(*) quantidade
                from pbi.carga_estoque
                where 1=1
                """
             )
        dfPostgres = pd.DataFrame(dfPostgres)
        tregPostgres = dfPostgres["quantidade"][0]

        if tregPostgres != 0:
            fun.process_data\
                (
                    """
                    delete
                    from pbi.carga_estoque
                    where 1=1
                    """
                    #and dtahorultmovtoestq >= date_trunc('month', (now() - '3 month'::interval)::timestamp)
            )
            query \
                = """ 
                  select
                        a.nroempresa,
                        a.seqproduto,
                        a.estqdeposito,
                        a.qtdreservadavda,
                        a.dtahorultmovtoestq
                  from implantacao.mrl_produtoempresa a
                  inner join implantacao.map_produto b on b.seqproduto = a.seqproduto and b.desccompleta not like 'ZZ%'
                  where 1=1
                  and a.dtahorultmovtoestq >= add_months(trunc(sysdate,'mm'), -3)
                  order by a.seqproduto asc,a.nroempresa asc
                  """
        else:
            query \
                = """ 
                  select
                        a.nroempresa,
                        a.seqproduto,
                        a.estqdeposito,
                        a.qtdreservadavda,
                        a.dtahorultmovtoestq
                  from implantacao.mrl_produtoempresa a
                  inner join implantacao.map_produto b on b.seqproduto = a.seqproduto and b.desccompleta not like 'ZZ%'
                  where 1=1
                  and a.dtahorultmovtoestq >= add_months(trunc(sysdate,'YYYY'), -48)
                  order by a.seqproduto asc,a.nroempresa asc
                  """
        Cur = cOracle.cursor()
        Cur.execute(query)
        Col = [Reg[0] for Reg in Cur.description]
        Lin = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Lin, columns=Col)
        DFrame["DTAHORULTMOVTOESTQ"] = pd.DatetimeIndex(DFrame["DTAHORULTMOVTOESTQ"]).date

        for index, reg in DFrame.iterrows():
            Campos \
                = """
                  (
                  nroempresa,
                  seqproduto,
                  estqdeposito,
                  qtdreservadavda,
                  dtahorultmovtoestq
                  )
                  """
            Values \
                = \
                "('"\
                +str(reg.NROEMPRESA)+"','"\
                +str(reg.SEQPRODUTO)+"','"\
                +str(reg.ESTQDEPOSITO)+"','"\
                +str(reg.QTDRESERVADAVDA)+"','"\
                +str(reg.DTAHORULTMOVTOESTQ)+\
                "')"

            Insert = "insert into pbi.carga_estoque"+Campos+"values"+Values+""
            fun.process_data(Insert)
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MLF_NOTAFISCAL INNER JOIN MLF_NFITEM
        ###############################################################################################################
        print("Execultado processo na tabela de \"Movimentação Compras\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Movimentação Compras\" ...\n")
            Arquivo.close()

        dfPostgres = fun.postgres_consulta\
            (
                """
                select count(*) quantidade
                from pbi.carga_movimentacaocompra
                where 1=1
                """
             )
        dfPostgres = pd.DataFrame(dfPostgres)
        tregPostgres = dfPostgres["quantidade"][0]

        if (tregPostgres != 0):
            fun.process_data\
                (
                    """
                    DELETE
                    FROM pbi.carga_movimentacaocompra
                    WHERE 1=1
                    AND dtaentrada >= DATE_TRUNC('MONTH',(NOW() - '3 MONTH'::INTERVAL)::TIMESTAMP)
                    """
                 )
            query \
                = """ 
                  SELECT
                        b.nroempresa,
                        a.seqproduto,
                        b.dtaentrada,
                        SUM(a.quantidade/a.qtdembalagem) quantidade
                  FROM implantacao.mlf_nfitem a
                  INNER JOIN implantacao.mlf_notafiscal b 
                  ON b.nroempresa = a.nroempresa 
                  AND b.seqpessoa = a.seqpessoa
                  AND b.numeronf = a.numeronf
                  AND b.serienf = a.serienf
                  AND b.tipnotafiscal = a.tipnotafiscal
                  AND b.dtaentrada IS NOT NULL
                  WHERE 1 = 1
                  AND b.dtaentrada >= ADD_MONTHS(TRUNC(SYSDATE,'MM'), -3)
                  GROUP BY b.nroempresa,a.seqproduto,b.dtaentrada
                  ORDER BY a.seqproduto DESC, b.dtaentrada DESC
                  """
        else:
            query \
                = """ 
                  SELECT
                        b.nroempresa,
                        a.seqproduto,
                        b.dtaentrada,
                        SUM(a.quantidade/a.qtdembalagem) quantidade
                  FROM implantacao.mlf_nfitem a
                  INNER JOIN implantacao.mlf_notafiscal b 
                  ON b.nroempresa = a.nroempresa 
                  AND b.seqpessoa = a.seqpessoa
                  AND b.numeronf = a.numeronf
                  AND b.serienf = a.serienf
                  AND b.tipnotafiscal = a.tipnotafiscal
                  AND b.dtaentrada IS NOT NULL
                  WHERE 1 = 1
                  AND b.dtaentrada >= ADD_MONTHS(TRUNC(SYSDATE,'YYYY'), -48)
                  GROUP BY b.nroempresa,a.seqproduto,b.dtaentrada
                  ORDER BY a.seqproduto DESC, b.dtaentrada DESC
                  """
        Cur = cOracle.cursor()
        Cur.execute(query)
        Col = [Reg[0] for Reg in Cur.description]
        Lin = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Lin, columns=Col)
        DFrame["DTAENTRADA"] = pd.DatetimeIndex(DFrame["DTAENTRADA"]).date

        for index, reg in DFrame.iterrows():
            Campos \
                = """
                  (
                  nroempresa,
                  seqproduto,
                  dtaentrada,
                  quantidade
                  )
                  """
            Values \
                = \
                "('"\
                +str(reg.NROEMPRESA)+"','"\
                +str(reg.SEQPRODUTO)+"','"\
                +str(reg.DTAENTRADA)+"','"\
                +str(reg.QUANTIDADE)+\
                "')"

            Insert = "INSERT INTO pbi.carga_movimentacaocompra"+Campos+"values"+Values+""
            fun.process_data(Insert)
        Cur.close()

        ###############################################################################################################
        # BUSCA DADOS DAS TABELAS MAD_PEDVENDA INNER JOIN MLF_DOCTOFISCAL INNER JOIN MLF_NFITEM
        ###############################################################################################################
        print("Execultado processo na tabela de \"Pedidos Faturados\" ...")

        with open(NomeArquivo, "a") as Arquivo:
            Arquivo.write("» Execultado processo na tabela de \"Pedidos Faturados\" ...\n")
            Arquivo.close()

        dfPostgres = fun.postgres_consulta \
                (
                """
                SELECT COUNT(*) quantidade
                FROM pbi.carga_pedidosfaturados
                WHERE 1=1
                """
            )
        dfPostgres = pd.DataFrame(dfPostgres)
        tregPostgres = dfPostgres["quantidade"][0]

        if tregPostgres != 0:
            fun.process_data \
                    (
                    """
                    DELETE
                    FROM pbi.carga_pedidosfaturados
                    WHERE 1=1
                    AND dtainclusao >= DATE_TRUNC('MONTH',(NOW() - '3 MONTH'::INTERVAL)::TIMESTAMP)
                    """
                )
            query \
                = """
                  SELECT
                      DISTINCT
                      a.nroempresa,
                      a.seqpessoa,
                      a.dtainclusao,
                      a.seqtitulo,
                      a.nroparcela,
                      a.nrotitulo numerodf,
                      b.seriedf,
                      NVL(b.nroserieecf,'CH') nroserieecf,
                      SUM(NVL(d.vlritem + NVL(d.vlrtotdespacessoria,0) + NVL(d.vlricmsst,0) - NVL(d.vlrdesconto,0), a.vlroriginal)) AS vlritem
                  FROM implantacao.fi_titulo a
                  LEFT JOIN implantacao.mfl_doctofiscal b 
                  ON b.seqpessoa = a.seqpessoa AND b.numerodf = a.nrotitulo AND b.seriedf = a.seriedoc AND b.statusdf = 'V'
                  LEFT JOIN implantacao.mad_pedvenda c 
                  ON c.seqpessoa = b.seqpessoa AND c.nropedvenda = b.nropedidovenda AND c.situacaoped = 'F' AND c.codgeraloper IN (201, 207, 314)
                  LEFT JOIN implantacao.mfl_dfitem d 
                  ON d.nroempresa = b.nroempresa AND d.numerodf = b.numerodf AND d.seriedf = b.seriedf AND d.nroserieecf = b.nroserieecf
                  WHERE 1=1	
                  AND a.obrigdireito = 'D'
                  AND a.dtainclusao >= ADD_MONTHS(TRUNC(SYSDATE,'MM'), -3)
                  GROUP BY a.nroempresa,a.seqpessoa,a.dtainclusao,a.seqtitulo,a.nroparcela,a.nrotitulo,b.numerodf,b.seriedf,b.nroserieecf
                  ORDER BY a.nroempresa ASC,a.seqpessoa ASC
                  """
        else:
            query \
                = """
                          SELECT
                              DISTINCT
                              a.nroempresa,
                              a.seqpessoa,
                              a.dtainclusao,
                              a.seqtitulo,
                              a.nroparcela,
                              a.nrotitulo numerodf,
                              b.seriedf,
                              NVL(b.nroserieecf,'CH') nroserieecf,
                              SUM(NVL(d.vlritem + NVL(d.vlrtotdespacessoria,0) + NVL(d.vlricmsst,0) - NVL(d.vlrdesconto,0), a.vlroriginal)) AS vlritem
                          FROM implantacao.fi_titulo a
                          LEFT JOIN implantacao.mfl_doctofiscal b 
                          ON b.seqpessoa = a.seqpessoa AND b.numerodf = a.nrotitulo AND b.seriedf = a.seriedoc AND b.statusdf = 'V'
                          LEFT JOIN implantacao.mad_pedvenda c 
                          ON c.seqpessoa = b.seqpessoa AND c.nropedvenda = b.nropedidovenda AND c.situacaoped = 'F' AND c.codgeraloper IN (201, 207, 314)
                          LEFT JOIN implantacao.mfl_dfitem d 
                          ON d.nroempresa = b.nroempresa AND d.numerodf = b.numerodf AND d.seriedf = b.seriedf AND d.nroserieecf = b.nroserieecf
                          WHERE 1=1	
                          AND a.obrigdireito = 'D'
                          AND a.dtainclusao >= ADD_MONTHS(TRUNC(SYSDATE,'YYYY'), -36)
                          GROUP BY a.nroempresa,a.seqpessoa,a.dtainclusao,a.seqtitulo,a.nroparcela,a.nrotitulo,b.numerodf,b.seriedf,b.nroserieecf
                          ORDER BY a.nroempresa ASC,a.seqpessoa ASC
                          """
        Cur = cOracle.cursor()
        Cur.execute(query)
        Col = [Reg[0] for Reg in Cur.description]
        Lin = Cur.fetchall()
        DFrame = pd.DataFrame.from_records(Lin, columns=Col)
        DFrame["DTAINCLUSAO"] = pd.DatetimeIndex(DFrame["DTAINCLUSAO"]).date

        for index, reg in DFrame.iterrows():
            Campos \
                = """
                          (
                          nroempresa,
                          seqpessoa,
                          dtainclusao,
                          seqtitulo,
                          nroparcela,
                          numerodf,
                          seriedf,
                          nroserieecf,
                          vlritem
                          )
                          """
            Values \
                = \
                "('" \
                + str(reg.NROEMPRESA) + "','" \
                + str(reg.SEQPESSOA) + "','" \
                + str(reg.DTAINCLUSAO) + "','" \
                + str(reg.SEQTITULO) + "','" \
                + str(reg.NROPARCELA) + "','" \
                + str(reg.NUMERODF) + "','" \
                + str(reg.SERIEDF) + "','" \
                + str(reg.NROSERIEECF) + "','" \
                + str(reg.VLRITEM) + \
                "')"

            Insert = "INSERT INTO pbi.carga_pedidosfaturados" + Campos + "values" + Values + ""
            fun.process_data(Insert)
        Cur.close()

    except Exception as erro:
        print("Error ao se conectar no banco de dados")
        print(erro)
    finally:
        if con_oracle:
            con_oracle.close()
    return 1

