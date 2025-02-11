from bbrasil.APIPix import ApiPixCobrancaImediata
from data_queries import consulta_tabelaparametros
from connectionsdb import ConnectionsDataBase
import cx_Oracle as ora
import pandas as pd

if __name__ == '__main__':
    #parametros = consulta_tabelaparametros()
    #print(parametros)
    cobranca_pix = ApiPixCobrancaImediata()
    cobranca_pix.cobranca_definida_usuario()
    #cobranca_pix.cobranca_definida_banco()
    #cobranca_pix.consultar_lista_pix_recebidos()
    #cobranca_pix.simular_pagamento_pix()
    #cobranca_pix.consultar_cobranca_recebida()
    #GerarQrCodePix(nome=getnome, chavepix=getchavepix, valor=getvalor, cidade=getcidade, txtid=gettxtid).gerar_cr16()