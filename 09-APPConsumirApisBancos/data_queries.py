import pandas as pd
import json
from connectionsdb import ConnectionsDataBase


def consulta_tabelaparametros():
    conora = ConnectionsDataBase.conn_oracle()
    cur = conora.cursor()
    cur.execute("SELECT * FROM implantacao.cadan_pixparametros")
    columns = [row[0] for row in cur.description]
    data = cur.fetchall()

    data_dict = dict()
    for reg in data:
        dicionario = dict(zip(columns, reg))
        data_dict[f"{dicionario['ID']}"] = dicionario

    return data_dict


def consulta_tabelapixenviados():
    conora = ConnectionsDataBase.conn_oracle()
    cur = conora.cursor()
    cur.execute("SELECT * FROM implantacao.cadan_pixpedidos")
    columns = [row[0] for row in cur.description]
    data = cur.fetchall()

    data_dict = dict()
    for reg in data:
        dicionario = dict(zip(columns, reg))
        data_dict[f"{dicionario['ID']}"] = dicionario

    return data_dict

