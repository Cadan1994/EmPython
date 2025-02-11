import pandas as pd
import oracledb as ora
import datetime as dt
import getpass


def conexao_db(usuario, senha):
    ora.init_oracle_client(lib_dir="C:\oracle\instantclient_21_3")
    conexao = ora.connect(
                        user=usuario,
                        password=senha,
                        host="192.168.168.200",
                        port=1521,
                        service_name="orcl"
                )
    return conexao


def atualizar_palavrachaveecommerce(user, password):
    oracle = conexao_db(user, password)
    cursor = oracle.cursor()
    cursor.execute(
        "UPDATE implantacao.map_produto " +
        "SET palavrachaveecommerce = '' " +
        "WHERE palavrachaveecommerce IS NOT NULL"
    )
    try:
        grupos = [
            "COSMETICO",
            "FARMACIA",
            "HOTEL E POUSADA",
            "LANCHONETE E RESTAURANTE",
            "SUPERMERCADO E MERCEARIA",
            "SUPRIMENTO CORPORATIVO"
        ]
        for grupo in grupos:
            caminho_arquivo = f'DataExcel/{grupo}.xlsx'
            dados_planilha = pd.read_excel(caminho_arquivo)
            for index, reg in dados_planilha.iterrows():
                codproduto = str(reg.CODIGO)
                desproduto = str(reg.DESCRICAO)
                data = format(dt.datetime.now(), "%d/%m/%Y")
                hora = format(dt.datetime.now(), "%H:%M:%S")
                datahora = data + " " + hora
                print(f"Indice: {index} » Descrição Produto: {desproduto}")

                cursor.execute(
                    "SELECT palavrachaveecommerce " +
                    "FROM implantacao.map_produto " +
                    "WHERE seqproduto = '"+codproduto+"'"
                )
                col = [row[0] for row in cursor.description]
                dados = cursor.fetchall()
                df = pd.DataFrame.from_records(dados, columns=col)
                gruporetorno = df['PALAVRACHAVEECOMMERCE'][0]

                if gruporetorno is None:
                    descricaogrupo = grupo
                else:
                    descricaogrupo = gruporetorno+";"+grupo

                cursor.execute(
                    "UPDATE implantacao.map_produto " +
                    "SET palavrachaveecommerce = '"+descricaogrupo+"'," +
                    "    usuarioalteracao = '"+user.upper()+"'," +
                    "    dtahoralteracao = TO_DATE('"+datahora+"','DD/MM/YYYY HH24:MI:SS') "
                    "WHERE seqproduto = '"+codproduto+"'"
                )
        oracle.commit()
    except ora.DatabaseError as Error:
        oracle.rollback()
        print("Erro: %s" % Error)


def main():
    """
    usuario = "hsantos"
    senha = "H1s@ntos1969"
    """
    usuario = "implantacao"
    senha = getpass.getpass("Dígite a senha do banco: ")
    atualizar_palavrachaveecommerce(usuario, senha)


main()