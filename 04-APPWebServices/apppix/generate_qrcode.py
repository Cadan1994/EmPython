import crcmod
import qrcode
import os
from pathlib import Path


class GerarQrCodePix(object):
    def __init__(self, nome, chavepix, valor, cidade, txid, diretorio=""):
        self.qrcode = None
        self.crc16Code = None
        self.crc16Code_formatado = None
        self.payload_completa = None
        self.nome = nome                        # Nome do recebedor
        self.chavepix = chavepix                # Chave
        self.valor = valor.replace(",", ".")    # Valor
        self.cidade = cidade                    # Cidade do recebedor
        self.txid = txid                        # Identificador de transação
        self.diretorioQrCode = diretorio        # Diretório da imagem do QR Code

        self.crc16Code_formatado = None
        self.crc16Code = None
        self.payload = None
        self.qrcode = None
        self.payload_completa = None
        self.crc16code = None
        self.crc16code_formatado = None

        # Payload Format Indicator » Informador de formato de carga útil
        self.indicator = "000201"
        # Merchant Acconunt Information » Informações da conta do comerciante
        self.information_gui = "0014br.gov.bcb.pix"
        self.information_chave = f"01{len(self.chavepix):02}{self.chavepix}"
        self.information = f"26{len(self.information_gui+self.information_chave):02}{self.information_gui+self.information_chave}"
        # Merchant Category Code » Código de categoria do comerciante "Chave do PIX"
        self.code = "52040000"
        # Transaction Currency » Moeda da transação
        self.currency = "5303986"
        # Transaction Amount » Valor da transação
        self.amount = f"54{len(self.valor):02}{float(self.valor):.2f}"
        # County Code » Código do país
        self.country = "5802BR"
        # Merchant Name » Nome do comerciante
        self.name = f"59{len(self.nome):02}{self.nome}"
        # Merchant City » Cidade do comerciante
        self.city = f"60{len(self.cidade):02}{self.cidade}"
        # Additional Data Field Templante » Modelo de campo de dados adicionais
        self.templante = f"62{len(f'05{len(self.txid):02}{self.txid}'):02}{f'05{len(self.txid):02}{self.txid}'}"
        # CRC16
        self.crc16 = "6304"

    """
    def gerar_qr_code(self, payload):
        dirqrcode = os.getcwd()
        self.qrcode = qrcode.make(payload)
        self.qrcode.save(os.path.join(dirqrcode, "CadanQRCodePix.png"))
        return payload

    """
    def gerar_cr16(self, payload):
        crc16 = crcmod.mkCrcFun(poly=0x11021, initCrc=0xFFFF, rev=False, xorOut=0x0000)
        self.crc16Code = hex(crc16(str(payload).encode("utf-8")))
        self.crc16Code_formatado = str(self.crc16Code).replace("0x", "").upper().zfill(4)
        self.payload_completa = f"{payload}{self.crc16Code_formatado}"
        self.gerar_qr_code(self.payload_completa, self.diretorioQrCode)

    def gerar_payload(self):
        self.payload = f"{self.indicator}{self.information}{self.code}{self.currency}{self.amount}{self.country}{self.name}{self.city}{self.templante}{self.crc16}"
        self.gerar_cr16(self.payload)

    def gerar_qr_code(self, payload, diretorio):
        diretorio = os.path.expanduser(diretorio)
        self.qrcode = qrcode.make(payload)
        self.qrcode.save(os.path.join(diretorio, "CadanQRCodePix.png"))
        return print(payload)
