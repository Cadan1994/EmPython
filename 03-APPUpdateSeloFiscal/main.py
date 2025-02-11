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


def atualizar_codigoselofiscal(user, password):
    oracle = conexao_db(user, password)
    cursor = oracle.cursor()
    try:
        dados_planilha = pd.read_excel('RegistroC177.xlsx')
        for index, reg in dados_planilha.iterrows():
            seqfamilia = str(reg.CODFAMILIA)
            codigoseloipi = str(reg.CODSELOFISCAL)
            data = format(dt.datetime.now(), "%d/%m/%Y")
            hora = format(dt.datetime.now(), "%H:%M:%S")
            datahora = data + " " + hora
            print(f"Indice: {index} » Código Familia: {seqfamilia} » Código Selo Fiscal: {codigoseloipi}")
            cursor.execute(
                "UPDATE implantacao.map_familia " +
                "SET codigoseloipi = '"+codigoseloipi+"'," +
                "    usuarioalteracao = '"+user.upper()+"'," +
                "    dtahoralteracao = TO_DATE('"+datahora+"','DD/MM/YYYY HH24:MI:SS') "
                "WHERE seqfamilia = '"+seqfamilia+"'"
            )
        oracle.commit()
    except ora.DatabaseError as Error:
        oracle.rollback()
        print("Erro: %s" % Error)


def main():
    usuario = input("Dígite o usuário do banco: ")
    senha = getpass.getpass("Dígite a senha do banco: ")
    atualizar_codigoselofiscal(usuario, senha)


main()