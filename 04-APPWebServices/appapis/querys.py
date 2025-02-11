GE_USUARIOS = \
    """
    SELECT 												 
        a.sequsuario,													 
        NVL(a.nroempresa, 1) AS nroempresa,
        UPPER(a.codusuario) AS codusuario, 
        UPPER(a.nome) AS nome,
        b.userdtaimportacao
    FROM implantacao.ge_usuario a
    LEFT JOIN implantacao.cadan_pixusuarios b 
    ON b.userid = a.sequsuario 
    WHERE 1=1
    AND a.tipousuario = 'U'	
    AND a.nivel IN (3, 8)	 
    AND (a.codusuario NOT LIKE ('C5%') 
    AND (a.codusuario NOT LIKE ('DEVIT%') 
    AND (a.codusuario NOT LIKE ('CONSINCO%'))))
    AND b.userdtaimportacao IS NULL				 
    """

MAD_PEDVENDA = \
    """
    SELECT
        DISTINCT
        a.nropedvenda,
        a.nroempresa, 
        a.seqpessoa, 
        a.nrorepresentante, 
        TO_CHAR(a.dtainclusao, 'YYYY-MM-DD HH24:MI:SS') AS dtainclusao, 
        TO_CHAR(a.dtabasefaturamento, 'YYYY-MM-DD HH24:MI:SS') AS dtabasefaturamento,
        a.indentregaretira,
        a.situacaoped,
        SUM(
            ((b.qtdatendida / b.qtdembalagem) * 
            CASE WHEN b.vlrembtabpromoc = 0 THEN b.vlrembtabpreco ELSE b.vlrembtabpromoc END)+b.vlrtoticmsst
        ) AS vlrpedido
    FROM implantacao.mad_pedvenda a
    INNER JOIN implantacao.mad_pedvendaitem b 
    ON b.nroempresa=a.nroempresa 
    AND b.nropedvenda=a.nropedvenda
    INNER JOIN implantacao.ge_pessoa c 
    ON c.seqpessoa=a.seqpessoa	 
    INNER JOIN (SELECT 
                    nroempresa, 
                    seqpessoa, 
                    MIN(nropedvenda) nropedvenda
                FROM implantacao.mad_pedvenda
                WHERE 1=1
                AND nroformapagto = 19
                AND indentregaretira = 'E'
                AND situacaoped = 'A'
                AND dtainclusao >= SYSDATE - 10
                GROUP BY nroempresa, seqpessoa) d 
    ON d.nroempresa = a.nroempresa 
    AND d.seqpessoa = a.seqpessoa
    LEFT JOIN implantacao.cadan_pixpedidos e 
    ON e.nroempresa = a.nroempresa 
    AND e.nropedvenda = a.nropedvenda 
    AND e.seqpessoa = a.seqpessoa
    WHERE 1=1
    AND a.nroformapagto = 19
    AND a.indentregaretira = 'E'
    AND a.situacaoped = 'A'
    AND e.dtaimportacao IS NULL
    AND a.dtainclusao >= SYSDATE - 10
    GROUP BY a.nropedvenda, 
             a.nroempresa, 
             a.seqpessoa, 
             a.nrorepresentante, 
             a.dtainclusao, 
             a.dtabasefaturamento, 
             a.indentregaretira,
             a.situacaoped,
             c.nrocgccpf,
             c.digcgccpf, 
             c.email,
             d.nropedvenda,
             e.dtaimportacao
    """

CADAN_PIXPEDIDOS = \
    """
    SELECT
        a.seqpessoa,
        a.nrorepresentante,
        a.nropedvenda,
        DECODE(
             a.indentregaretira,
             'E', 'ENTREGA',
             'R', 'RETIRA'
        ) AS indentregaretira,
        DECODE(
         a.situacaoped,
             'A', 'EM ANALISE',
             'L', 'LIBERADO',
             'F', 'FATURADO'
        ) AS situacaoped,
        a.vlrpedido,
        a.dtainclusao,
        a.dtabasefaturamento,
        a.pixvalor,
        DECODE(
         a.pixstatus,
             'A','AGUARDANDO RECEBIMENTO',
             'R','RECEBIDO'
        ) AS pixstatus,
        a.pixdtaexpiracao
    FROM implantacao.cadan_pixpedidos a
    WHERE 1=1	
    ORDER BY a.nropedvenda ASC
    """