import os
import datetime as dta
import md003ExtractDIM as etld
import md003ExtractDIM_old as etld_old
import md004ExtractFAT as etlf
from pathlib import Path

DataAtual = dta.datetime.today().date()
Dia = str(DataAtual.day)
Mes = str(DataAtual.month)
Ano = str(DataAtual.year)

dirtxt = "log"
base_dir = Path(__file__).cwd()
NomeArquivo = os.path.join(base_dir, dirtxt) + "\LogCarga"+Dia.zfill(2)+Mes.zfill(2)+Ano.zfill(4)+".txt"

vHInicial = dta.datetime.now().strftime("%H:%M:%S")
vHi = int(dta.datetime.now().strftime("%H"))
vMi = int(dta.datetime.now().strftime("%M"))
vSi = int(dta.datetime.now().strftime("%S"))

print("INICIANDO PROCESSO DE ETL-DIMENSÕES, AGUARDE")

with open(NomeArquivo, "w") as Arquivo:
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.write("| INICIANDO PROCESSO DE ETL-DIMENSÕES, AGUARDE                                                 |\n")
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.close()

etld.etl_dimensoes()
etld_old.etl_dimensoes()

with open(NomeArquivo, "a") as Arquivo:
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.write("| FINALIZADO PROCESSO DE ETL-DIMENSÕES                                                         |\n")
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.write("")
    Arquivo.write("================================================================================================\n")
    Arquivo.write("")
    Arquivo.close()

print("")
print("")

print("INICIANDO PROCESSO DE ETL-FATOS, AGUARDE")
with open(NomeArquivo, "a") as Arquivo:
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.write("| INICIANDO PROCESSO DE ETL-FATOS, AGUARDE                                                     |\n")
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.close()

etlf.etl_fatos()

with open(NomeArquivo, "a") as Arquivo:
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.write("| FINALIZADO PROCESSO DE ETL-FATOS                                                             |\n")
    Arquivo.write("+----------------------------------------------------------------------------------------------+\n")
    Arquivo.close()

vHFinal = dta.datetime.now().strftime("%H:%M:%S")
vHf = int(dta.datetime.now().strftime("%H"))
vMf = int(dta.datetime.now().strftime("%M"))
vSf = int(dta.datetime.now().strftime("%S"))

if vHf < vHi:
    vRh = vHi - vHf
else:
    vRh = vHf - vHi

if vMf < vMi:
    vRm = vMi - vMf
else:
    vRm = vMf - vMi
if vSf < vSi:
    vRs = vSi - vSf
else:
    vRs = vSf - vSi

vResultado = str("%02d" % vRh) + ":" + str("%02d" % vRm) + ":" + str("%02d" % vRs)+"\n"

with open(NomeArquivo, "a") as Arquivo:
    Arquivo.write(f"Hora iniciada.......: {vHInicial}\n"
                  f"Hora final..........: {vHFinal}\n"
                  f"Resuldado...........: {vResultado}")
    Arquivo.close()

print("FINALIZADO PROCESSO DE ETL")

exit()