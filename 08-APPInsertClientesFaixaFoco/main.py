import json

import pandas as pd
import cx_Oracle as ora
from connectdb import ConnectDataBase


def cliente_foco_faixa():
    conn = ConnectDataBase.conn_oracle()
    cur = conn.cursor()

    try:
        dfcliente_faixa = pd.read_excel(io='ClientesFaixa.xlsx', sheet_name='FAIXA')
        listafoco = ['COLGATE', 'URCA', 'SANTHER', 'UNILEVER-ATACADO', 'UNILEVER-DEC', 'INDAIA']
        dfclientefocofaixa_a = None
        dfclientefocofaixa_b = None
        dfclientefocofaixa_c = None
        dfclientefocofaixa_d = None
        dfclientefocofaixa_e = None
        dfclientefocofaixa_f = None
        listaidx = 0
        contador = 1

        while contador <= len(listafoco):
            dfcliente_foco = pd.read_excel(io='ClientesFoco.xlsx', sheet_name=f'{listafoco[listaidx]}')
            dfcliente_foco = dfcliente_foco.rename(columns={'FOCO': f'{listafoco[listaidx]}'})

            if listaidx == 0:
                dfclientefocofaixa_a = pd.merge(
                                            dfcliente_faixa,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )
            elif listaidx == 1:
                dfclientefocofaixa_b = pd.merge(
                                            dfclientefocofaixa_a,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )
            elif listaidx == 2:
                dfclientefocofaixa_c = pd.merge(
                                            dfclientefocofaixa_b,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )
            elif listaidx == 3:
                dfclientefocofaixa_d = pd.merge(
                                            dfclientefocofaixa_c,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )
            elif listaidx == 4:
                dfclientefocofaixa_e = pd.merge(
                                            dfclientefocofaixa_d,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )
            elif listaidx == 5:
                dfclientefocofaixa_f = pd.merge(
                                            dfclientefocofaixa_e,
                                            dfcliente_foco,
                                            left_on='SEQPESSOA',
                                            right_on='SEQPESSOA',
                                            how='outer'
                                        )

            contador += 1
            listaidx += 1

        dfclientefocofaixa1 = dfclientefocofaixa_f.fillna('')
        dfclientefocofaixa2 = dfclientefocofaixa1.rename(columns={'UNILEVER-ATACADO': 'UNILEVERA', 'UNILEVER-DEC': 'UNILEVERD'})
        dfclientefocofaixa = dfclientefocofaixa2.drop_duplicates(subset='SEQPESSOA', keep='last', inplace=False)

        pd.set_option('display.max_columns', None)

        separador = ' | '

        for index, reg in dfclientefocofaixa.iterrows():
            cliente = reg.SEQPESSOA
            faixa = reg.FAIXA
            foco_colgate = reg.COLGATE
            foco_urca = reg.URCA
            foco_santher = reg.SANTHER
            foco_unilevera = reg.UNILEVERA
            foco_unileverd = reg.UNILEVERD
            foco_indaia = reg.INDAIA

            if foco_urca == '':
                sep_urca = ''
            else:
                sep_urca = separador
                if foco_colgate == '':
                    sep_urca = ''

            if foco_santher == '':
                sep_santher = ''
            else:
                sep_santher = separador
                if foco_colgate == '':
                    sep_santher = ''

            if foco_unilevera == '':
                sep_unilevera = ''
            else:
                sep_unilevera = separador
                if foco_colgate == '':
                    sep_unilevera = ''

            if foco_unileverd == '':
                sep_unileverd = ''
            else:
                sep_unileverd = separador
                if foco_colgate == '':
                    sep_unileverd = ''

            if foco_indaia == '':
                sep_indaia = ''
            else:
                sep_indaia = separador
                if foco_colgate == '':
                    sep_indaia = ''

            foco = (foco_colgate + sep_urca +
                    foco_urca + sep_santher +
                    foco_santher + sep_unilevera +
                    foco_unilevera + sep_unileverd +
                    foco_unileverd + sep_indaia +
                    foco_indaia)

            json_data = {
                'foco': foco,
                'colgate_faixa': faixa,
                'frequencia_atendimento': 'S'
            }

            value = "('" \
                    + str(cliente) + "','" \
                    + str(json.dumps(json_data)) + "'," \
                    + "SYSDATE" + "," \
                    + "SYSDATE" + \
                    ")"

            insert = f"INSERT INTO implantacao.cadan_clientefocofaixa" \
                     f"(seqpessoa, jsondata, dtainclusao, dtaalteracao)" \
                     f"VALUES {value}"

            print(insert)
            cur.execute(insert)
        conn.commit()
    except ora.DatabaseError as Error:
        print(Error)
        conn.rollback()


if __name__ == '__main__':
    cliente_foco_faixa()
    print('Processo realizado com sucesso!')
