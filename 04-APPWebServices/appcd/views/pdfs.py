import io
from io import BytesIO
from django.http import FileResponse
from django.views.generic import View
from reportlab.pdfgen import canvas
from appcd.models import *
from django.shortcuts import get_object_or_404

''''#####################Relatório p/ o controle de equipamentos ################################################'''
def generate_pdf(request):
    response = FileResponse(generate_pdf_file(),
                            as_attachment=True,
                            filename='Equipamentos.pdf')
    return response

def generate_pdf_file():
    buffer = BytesIO()
    p = canvas.Canvas(buffer)

    # Create a PDF document
    equipamentos = Equipamento.objects.all()
    p.drawString(100, 750, "Relatório dos Equipamentos")

    y = 700
    for equipamento in equipamentos:
        p.drawString(100, y, f"ID: {equipamento.equipamento_id}")
        p.drawString(100, y - 20, f"Modelo: {equipamento.modelo}")
        p.drawString(100, y - 40, f"Marca: {equipamento.marca}")
        y -= 60

    p.showPage()
    p.save()

    buffer.seek(0)
    return buffer

''''##################### Termo de Responsabilidade ################################################'''
class Termo1(View):
    def get(self, request, *args, **kwargs):
        equipamento_id = self.kwargs.get('equipamento_id')

        responsavel = get_object_or_404(Responsavel, equipamento_responsavel__equipamento_id=equipamento_id)

        pdf_buffer = io.BytesIO()
        pdf = canvas.Canvas(pdf_buffer)

        self.criar_pdf(pdf, responsavel)

        pdf.showPage()
        pdf.save()

        pdf_buffer.seek(0)
        return FileResponse(pdf_buffer, as_attachment=True, filename='Termo_de_Responsabilidade.pdf')

    def criar_pdf(self, pdf, responsavel):
        pdf.drawString(210, 780, "TERMO DE RESPONSABILIDADE")
        pdf.drawString(350, 730, f"Recife, {responsavel.data_entrega}")
        pdf.drawString(30, 700, f"OPERADORA: CLARO")
        pdf.drawString(38, 670, f"A partir desta data, V.Sa. recebe o aparelho celular abaixo identificado, do plano empresarial desta")
        pdf.drawString(30, 650, f"empresa junto à operadora acima mencionada, ficando sob sua inteira responsabilidade a guarda, ")
        pdf.drawString(30, 630, f"conservação e manutenção, para uso exclusivo da empresa. ")

        pdf.drawString(30, 590, f"Em caso de extravio, perda, roubo ou furto do aparelho, V.Sa. fica orientado de comicar o fato")
        pdf.drawString(30, 570, f"a esse departamento para o devido bloqueio e proceder, conforme o caso, com o registro do respectivo")
        pdf.drawString(30, 550, f"boletim de ocorrência - B.O., entregando uma cópia do mesmo para o Departamento de T.I para as ")
        pdf.drawString(30, 530, f"providências cabíveis junto à operadora.")
        for equipamento in responsavel.equipamento_responsavel.all():
            pdf.drawString(30, 490, f"Fica V.Sa. ciente que será cobrado o valor de R$ {equipamento.valor} reais nos casos extravio, perda, roubo")
            pdf.drawString(30, 470, f"ou furto do aparelho ocorrido fora do expediente, bem como será deverá o empregado arcar com ")
            pdf.drawString(30, 450, f"os custos do conserto em casos de danos ocorridos por negligência, mal uso, imperícia ou imprudência.")
            pdf.drawString(30, 430, f"o referido aparelho deverá, por qualquer razão, ser devolvido em caso de rescisão do contrato")
            pdf.drawString(30, 410, f"de trabalho.")

            pdf.drawString(30, 370, "DADOS APARELHO:")
            pdf.drawString(30, 350, f"Marca/ Modelo: {equipamento.marca} {equipamento.modelo}")
            pdf.drawString(30, 330, f"IMEI A: {equipamento.imei1}")
            pdf.drawString(30, 310, f"IMEI B: {equipamento.imei2}")

        pdf.drawString(30, 290, f"Número da Linha: {responsavel.telefone_celular}")
        pdf.drawString(30, 250, "Assinatura: ________________________________________")
        pdf.drawString(30, 230, f"NOME: {responsavel.nome}")
        pdf.drawString(30, 210, f"RG: {responsavel.rg}")
        pdf.drawString(30, 190, f"CPF: {responsavel.cpf}")
        pdf.drawString(30, 170, f"Cargo: {responsavel.funcao}/{responsavel.departamento}")

''''##################### Termo de Devolução ################################################'''
class Termo2(View):
    def get(self, request, *args, **kwargs):
        # Obtenha o ID do responsável a partir dos parâmetros da solicitação (ajuste conforme necessário)
        responsavel_id = self.kwargs.get('responsavel_id')

        # Cria um arquivo para receber os dados e gerar o PDF
        buffer = io.BytesIO()

        # Cria o arquivo PDF
        pdf = canvas.Canvas(buffer)

        # Insere cabeçalho no PDF
        pdf.drawString(160, 780, "Declaração de devolução de equipamento")

        # Obtém todas as devoluções para o responsável selecionado
        devolucoes = Devolucao.objects.filter(ultimo_responsavel=responsavel_id)

        # Loop sobre as devoluções
        for devolucao in devolucoes:
            pdf.drawString(330, 730, f"Recife, {devolucao.data_devolucao}")

            # Adiciona o texto relacionado à devolução
            pdf.drawString(20, 690, f"O equipamento telefônico {devolucao.ultimo_equipamento.tipo} {devolucao.ultimo_equipamento.marca} {devolucao.ultimo_equipamento.modelo}, de IMEI1: {devolucao.ultimo_equipamento.imei1}, IMEI2: ")
            pdf.drawString(20, 670, f"{devolucao.ultimo_equipamento.imei2} de número: {devolucao.ultimo_responsavel.telefone_celular} Portado pelo colaborador {devolucao.ultimo_responsavel.nome} ")
            pdf.drawString(20, 650, f"foi devolvido no dia {devolucao.data_devolucao}.")

            # Adiciona a assinatura do colaborador
            pdf.drawString(20, 590, f"Assinatura: ________________________________________")
            pdf.drawString(20, 570, f"{devolucao.ultimo_responsavel.nome}")

            # Adiciona a pessoa que recebeu
            pdf.drawString(20, 530, f"Recebido por: ______________________________________")
            pdf.drawString(20, 510, f"")

            # Adiciona a testemunha
            pdf.drawString(20, 490, f"Testemunha: ______________________________________")
            pdf.drawString(20, 470, f"")

        # Finaliza a página e salva o PDF
        pdf.showPage()
        pdf.save()

        # Retorna o buffer para o início do arquivo
        buffer.seek(0)
        return FileResponse(buffer, as_attachment=True, filename='Termo_de_Devolução.pdf')

