import pandas as pd
from dbconnect import ConnectBD
def produtolista():
    query = """
            SELECT
                DISTINCT
                a.seqproduto||'.'||TRUNC(b.qtdembalagem)
                AS seqproduto,	
                a.desccompleta,
                NVL(SUBSTR(d.jsondata,2, 18),'"produto_base":"N"')
                AS jsondatabase,
                NVL(SUBSTR(d.jsondata,21, 19),'"produto_disc":"N"')
		        AS jsondatadist
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
    conn = ConnectBD.conn_oracle()
    cur = conn.cursor()
    cur.execute(query)
    colums = [row[0] for row in cur.description]
    data = cur.fetchall()
    dfdata = pd.DataFrame.from_records(data, columns=colums)
    dfdata.fillna(value='', inplace=True)

    list_produtos = list()
    for index, row in dfdata.iterrows():
        list_produtos.append(row)

    data = {
        'produtos': list_produtos
    }

    return print(data)

produtolista()