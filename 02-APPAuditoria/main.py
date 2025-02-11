import os
import pandas as pd
import shutil
from pathlib import Path
import cx_Oracle as Ora
from dbconnect import ConnectBD


caminho_origem = 'S:/RetornoConsico/'
caminho_destino_mover = 'S:/RetornoConsico/bradesco_copy/'
caminho_destino_criar = 'S:/RetornoConsico/bradesco_ti/'


def lista_arquivo_bradesco():
    lista_arquivos = list()
    for nome_arquivo in os.listdir(caminho_origem):
        nome, extensao = os.path.splitext(nome_arquivo)
        if extensao.upper() in ('.RET', '.PRC'):
            nome_arquivo = nome+extensao
            lista_arquivos.append(nome_arquivo)
    return lista_arquivos


def mover_arquivo():
    lista_arquivos = Path(caminho_origem).glob('*')
    for nome_arquivo in lista_arquivos:
        if nome_arquivo.suffix in ('.RET', '.prc'):
            shutil.move(nome_arquivo, caminho_destino_mover)


def sequencial():
    try:
        conn = ConnectBD.conn_oracle()
        cur = conn.cursor()
        cur.execute(
            'SELECT DISTINCT MAX(numretorno) numretorno '
            'FROM IMPLANTACAO.fi_retbco '
            'WHERE 1=1 '
            'AND nrobanco = 237 '
            'ORDER BY numretorno DESC'
        )
        col = [row[0] for row in cur.description]
        dados = cur.fetchall()
        df = pd.DataFrame.from_records(dados, columns=col)
        numretorno = df['NUMRETORNO'][0]

        for n in range(1, numretorno + 1):
            cur.execute(
                'SELECT COUNT(*) registro '
                'FROM IMPLANTACAO.fi_retbco '
                'WHERE 1 = 1 '
                'AND nrobanco = 237 '
                f'AND numretorno = {n}'
            )
            col = [row[0] for row in cur.description]
            dados = cur.fetchall()
            lista = list()
            for reg in dados:
                registro = dict(zip(col, reg))
                lista.append(registro)
            valor = lista[0]['REGISTRO']
            if valor == 0:
                numretorno = n
                break
        cur.close()
    except Ora.DatabaseError as Error:
        return Error
    return numretorno


def criar_arquivo_bradesco():
    arquivos_processar = lista_arquivo_bradesco()
    nroretorno_new = 0
    for nome_arquivo in arquivos_processar:
        if os.path.exists(f'{caminho_destino_mover}{nome_arquivo}'):
            os.remove(f'{caminho_destino_mover}{nome_arquivo}')

        with (open(f'{caminho_origem}{nome_arquivo}', 'r') as arquivo):
            line = arquivo.readlines()
            nroretorno_old = line[0][94:114]
            nrosequencial = sequencial()
            nrosequencial_new = str(nroretorno_new)[9:].zfill(10)

            if int(nrosequencial_new) == nrosequencial:
                nrosequencial += 1
                nroretorno_new = line[0][94:103] + str(nrosequencial).zfill(10) + ' '
            else:
                nroretorno_new = line[0][94:103] + str(nrosequencial).zfill(10) + ' '

            v = len(line)
            i = 0
            sequencial_new = 1
            f = open(f'{caminho_destino_criar}{nome_arquivo}', 'a')
            f.seek(0)
            f.truncate()
            for i in range(v):
                if i == 0:
                    f.write(line[i].replace(str(nroretorno_old),str(nroretorno_new)))
                else:
                    cod_ocr_banco = line[i][108:110]
                    if not cod_ocr_banco in ['02', '27']:
                        sequencial_new = str(int(sequencial_new) + 1).zfill(6)
                        sequencial_old = line[i][394:400]
                        f.write(line[i].replace(str(sequencial_old),str(sequencial_new)))

            f.close()
    mover_arquivo()
    return print('Processo realizado com sucesso.')


criar_arquivo_bradesco()
