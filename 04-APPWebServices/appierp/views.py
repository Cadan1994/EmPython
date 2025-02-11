import json
import os
import cx_Oracle as ora
import re
import pandas as pd
import psutil
from datetime import datetime
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.core.paginator import Paginator
from pathlib import Path


class ConnectDatabase(object):
    """ Parâmetros para conexão com Oracle """
    ora_usuario = 'DEVPOL'
    ora_senha = 'o2T56TZ6x'
    ora_ipservidor = '192.168.168.200'
    ora_porta = '1521'
    ora_banco = 'orcl'
    ora_client = 'oracle\instantclient_21_3'
    base_dir = Path(__file__).cwd()
    for proc in psutil.process_iter(['pid', 'name']):
        if 'init_oracle_client' in proc.info['name']:
            ora.init_oracle_client(lib_dir=os.path.join(base_dir, ora_client))

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


def home(request):
    return render(request, template_name='appierp_home.html')


class ClienteFocoFaixa(object):
    """
    Essa classe pe responsável das operações do cadastro dos select's nas
    tabelas do ERP para extração de dados
    """

    def __init__(self):
        self.GET = None
        self.POST = None

    def clientelista(self):
        try:
            query = """
                    SELECT 
                        DISTINCT
                        a.seqpessoa, 
                        CASE
                        WHEN b.fisicajuridica = 'F'
                        THEN SUBSTR(CONCAT(LPAD(b.nrocgccpf, 9, '0'),LPAD(b.digcgccpf, 2, '0')), 0, 3) || '.' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 9, '0'),LPAD(b.digcgccpf, 2, '0')), 4, 3) || '.' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 9, '0'),LPAD(b.digcgccpf, 2, '0')), 7, 3) || '-' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 9, '0'),LPAD(b.digcgccpf, 2, '0')), 10, 2)
                        ELSE SUBSTR(CONCAT(LPAD(b.nrocgccpf, 12, '0'),LPAD(b.digcgccpf, 2, '0')), 0, 2) || '.' || 
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 12, '0'),LPAD(b.digcgccpf, 2, '0')), 3, 3) || '.' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 12, '0'),LPAD(b.digcgccpf, 2, '0')), 6, 3) || '/' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 12, '0'),LPAD(b.digcgccpf, 2, '0')), 9, 4) || '-' ||
                             SUBSTR(CONCAT(LPAD(b.nrocgccpf, 12, '0'),LPAD(b.digcgccpf, 2, '0')), 13, 2)
                        END AS  
                        cpfcnpj, 
                        b.nomerazao,
                        c.jsondata
                    FROM implantacao.mrl_cliente a 
                    INNER JOIN implantacao.ge_pessoa b 
                    ON b.seqpessoa = a.seqpessoa
                    LEFT JOIN implantacao.cadan_clientefocofaixa c 
                    ON c.seqpessoa = b.seqpessoa
                    WHERE 1=1
                    AND a.statuscliente = 'A'
                    ORDER BY b.nomerazao 
                    """
            conn = ConnectDatabase.conn_oracle()
            cur = conn.cursor()
            cur.execute(query)
            colums = [row[0] for row in cur.description]
            data = cur.fetchall()
            dfdata = pd.DataFrame.from_records(data, columns=colums)
            dfdata.fillna(value='', inplace=True)

            list_clientes = list()
            for index, row in dfdata.iterrows():
                list_clientes.append(row)

            dfdatafoco = pd.read_csv('parametrosfoco.txt')
            list_parametrosfoco = list()
            for index, row in dfdatafoco.iterrows():
                list_parametrosfoco.append(row)

            dfdataaixa = pd.read_csv('parametrosfaixa.txt')
            list_parametrosfaixa = list()
            for index, row in dfdataaixa.iterrows():
                list_parametrosfaixa.append(row)

            paginacao = Paginator(object_list=list_clientes, per_page=5)
            num_pagina = self.GET.get('page')
            obj_pagina = paginacao.get_page(num_pagina)

            data = {
                'clientes': list_clientes,
                'parametrosfoco': list_parametrosfoco,
                'parametrosfaixa': list_parametrosfaixa,
                'objpagina': obj_pagina,
            }

            return render(self, template_name='clientefocofaixa.html', context=data)
        except ora.DatabaseError as Error:
            return HttpResponse(Error)

    def clienteupdate(self):
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        try:
            cliente_codigo = self.POST.get('clientedados')[:6]
            cliente_foco = self.POST.getlist('focodados')
            cliente_faixa = self.POST.getlist('faixadados')

            query = f"""
                    SELECT *
                    FROM implantacao.cadan_clientefocofaixa 
                    WHERE 1=1
                    AND seqpessoa = {cliente_codigo} 
                    """
            cur.execute(query)
            data = cur.fetchall()
            regqtde = len(data)

            foco = ''
            faixa = ''
            separador = ' | '
            listsizefoco = len(cliente_foco)
            listsizefaixa = len(cliente_faixa)

            i = 1
            for v in cliente_foco:
                if i == listsizefoco:
                    foco += v
                else:
                    foco += v + separador
                i += 1

            i = 1
            for v in cliente_faixa:
                if i == listsizefaixa:
                    faixa += v
                else:
                    faixa += v + separador
                i += 1

            jsondata = {
                'foco': foco,
                'colgate_faixa': faixa,
                'frequencia_atendimento': 'S'
            }

            cliente_jsondata = json.dumps(jsondata)

            if regqtde == 0:
                value = "('" \
                        + str(cliente_codigo) + "','" \
                        + str(cliente_jsondata) + "'," \
                        + "SYSDATE" + "," \
                        + "SYSDATE" + \
                        ")"

                insert = f"INSERT INTO implantacao.cadan_clientefocofaixa" \
                         f"(seqpessoa, jsondata, dtainclusao, dtaalteracao)" \
                         f"VALUES {value}"

                cur.execute(insert)
            else:
                update = f"UPDATE implantacao.cadan_clientefocofaixa " \
                         f"SET jsondata = '{cliente_jsondata}', dtaalteracao = SYSDATE " \
                         f"WHERE seqpessoa = {cliente_codigo}"
                cur.execute(update)

            conn.commit()

            return redirect('/appierp/cliente/')
        except ora.DatabaseError as Error:
            conn.rollback()
            return HttpResponse(Error)


class ProdutoBase(object):
    """
    Essa classe é responsável das operações do cadastro dos select's nas
    tabelas do ERP para extração de dados
    """

    def __init__(self):
        self.GET = None
        self.POST = None

    def produtolista(self):
        try:
            query = """
                    SELECT
                        DISTINCT
                        a.seqproduto||'.'||TRUNC(b.qtdembalagem)
                        AS seqproduto,	
                        a.desccompleta,
                        NVL(SUBSTR(d.jsondata,19, 1),'N')
                        AS jsondatabase
                    FROM implantacao.map_produto a
                    INNER JOIN implantacao.map_prodcodigo b 
                    ON b.seqfamilia = a.seqfamilia 
                    AND b.seqproduto = a.seqproduto 
                    AND b.indutilvenda = 'S' 
                    AND b.tipcodigo IN ('E','D')
                    INNER JOIN implantacao.mrl_prodempseg	c 
                    ON c.seqproduto = a.seqproduto 
                    AND c.statusvenda = 'A'
                    LEFT JOIN implantacao.cadan_produtobase d 
                    ON TRIM(d.seqproduto) = TRIM(TO_CHAR(b.seqproduto||'.'||TRUNC(b.qtdembalagem)))
                    WHERE 1=1
                    AND a.desccompleta NOT LIKE ('ZZ%')	
                    AND a.desccompleta NOT LIKE ('=%')
                    ORDER BY a.desccompleta ASC 
                    """
            conn = ConnectDatabase.conn_oracle()
            cur = conn.cursor()
            cur.execute(query)
            colums = [row[0] for row in cur.description]
            data = cur.fetchall()
            dfdata = pd.DataFrame.from_records(data, columns=colums)
            dfdata.fillna(value='', inplace=True)

            list_produtos = list()
            for index, row in dfdata.iterrows():
                list_produtos.append(row)

            paginacao = Paginator(object_list=list_produtos, per_page=5)
            num_pagina = self.GET.get('page')
            obj_pagina = paginacao.get_page(num_pagina)
            
            data = {
                'produtos': list_produtos,
                'objpagina': obj_pagina,
            }

            return render(self, template_name='produtobase.html', context=data)
        except ora.DatabaseError as Error:
            return HttpResponse(Error)

    def produtoupdate(self):
        conn = ConnectDatabase.conn_oracle()
        cur = conn.cursor()
        try:
            produto_codigo = self.POST.get('produtodados')
            produto_base = self.POST.get('produtobase')

            if produto_base is None:
                produto_base = 'N'
            else:
                produto_base = 'S'

            padrao = re.compile('(.*?)<')
            resultado = padrao.match(produto_codigo)

            if resultado:
                produto_codigo = resultado.group(1)

            query = f"""
                    SELECT *
                    FROM implantacao.cadan_produtobase 
                    WHERE 1=1
                    AND seqproduto = {produto_codigo} 
                    """

            cur.execute(query)
            data = cur.fetchall()
            regqtde = len(data)

            jsondata = {
                'produto_base': produto_base
            }

            produto_jsondata = json.dumps(jsondata)

            if regqtde == 0:
                value = "('" \
                        + str(produto_codigo) + "','" \
                        + str(produto_jsondata) + "'," \
                        + "SYSDATE" + "," \
                        + "SYSDATE" + \
                        ")"

                insert = f"INSERT INTO implantacao.cadan_produtobase" \
                         f"(seqproduto, jsondata, dtainclusao, dtaalteracao)" \
                         f"VALUES {value}"

                print(insert)

                cur.execute(insert)
            else:
                update = f"UPDATE implantacao.cadan_produtobase " \
                         f"SET jsondata = '{produto_jsondata}', dtaalteracao = SYSDATE " \
                         f"WHERE seqproduto = {produto_codigo}"
                cur.execute(update)

            conn.commit()

            return redirect('/appierp/produto/')
        except ora.DatabaseError as Error:
            conn.rollback()
            return HttpResponse(Error)
