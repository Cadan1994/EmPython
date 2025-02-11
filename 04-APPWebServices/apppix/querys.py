"""
SELECT'S DOS PROCESSOS
------------------------------------------------------------------------------------------------------------------------
@Objetivo: SCRIPTS DE CONSULTAS SELECIONADAS
@Create: HILSON SANTOS
@Date: 06/10/2024
------------------------------------------------------------------------------------------------------------------------
"""
# Variável que irá receber o complemento da query #
AND = ""


def orders_group(script_complement):
    """
    AGRUPAMENTO DE PEDIDOS
    --------------------------------------------------------------------------------------------------------------------
    @Create: HILSON SANTOS
    @Date: 06/11/2024
    @Objective: Selecionar os pedidos através do parâmetro "PIXTXID"
    @param script_complement:
    @return: Os dados da tabela "CADAN_PIXPEDIDOS"
    --------------------------------------------------------------------------------------------------------------------
    """
    global AND
    AND = script_complement
    data_pixpedidos = \
    f"""
    SELECT 
        a.nroempresa, 
        a.seqpessoa,
        LPAD(b.nrocgccpf||b.digcgccpf, 15, '0')
        AS nrocgccpf,
        b.nomerazao,
        b.fisicajuridica,
        LISTAGG(a.nropedvenda, ', ') WITHIN GROUP (ORDER BY a.nropedvenda) AS nropedvenda,
        LISTAGG(a.vlrpedido, ', ') WITHIN GROUP (ORDER BY a.vlrpedido) AS vlrpedvenda,
        a.nrorepresentante, 
        a.indentregaretira,
        a.pixemail, 
        a.pixtxid,
        a.pixqrcode, 
        SUM(a.vlrpedido) AS vlrtotalpedvenda
    FROM implantacao.cadan_pixpedidos a
    INNER JOIN implantacao.ge_pessoa b ON b.seqpessoa = a.seqpessoa
    WHERE 1=1
    {AND}
    GROUP BY a.nroempresa, a.seqpessoa, a.nrorepresentante, a.indentregaretira, a.pixemail, a.pixtxid, a.pixqrcode, 
             b.nrocgccpf, b.digcgccpf, b.nomerazao, b.fisicajuridica
    ORDER BY a.seqpessoa ASC
    """
    return data_pixpedidos