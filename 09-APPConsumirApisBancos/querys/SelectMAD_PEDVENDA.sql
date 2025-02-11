
    SELECT
        DISTINCT
        TO_CHAR(a.nropedvenda) AS nropedvenda,
        TO_CHAR(a.nroempresa) AS nroempresa, 
        TO_CHAR(a.seqpessoa) AS seqpessoa, 
        TO_CHAR(a.nrorepresentante) AS nrorepresentante, 
        TO_CHAR(a.dtainclusao, 'YYYY-MM-DD HH24:MI:SS') AS dtainclusao, 
        TO_CHAR(a.dtabasefaturamento, 'YYYY-MM-DD HH24:MI:SS') AS dtabasefaturamento, 		 
				a.indentregaretira,
				a.situacaoped,
        SUM(
                ((b.qtdatendida / b.qtdembalagem) * 
                CASE WHEN b.vlrembtabpromoc = 0 THEN b.vlrembtabpreco ELSE b.vlrembtabpromoc END)+b.vlrtoticmsst
        ) AS vlrpedido,	 
				a.nroempresa||
				LPAD(c.nrocgccpf||c.digcgccpf,15, '0')||
				LPAD(a.seqpessoa, 6, '0')||
				'L'||
				LPAD(d.nropedvenda, 12, '0') AS nrotxid,
        TO_CHAR(c.email) AS seqpessoaemail
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
    						AND situacaoped = 'A'
    						AND dtainclusao >= SYSDATE - 10
								GROUP BY nroempresa, seqpessoa
								) d 
		ON d.nroempresa = a.nroempresa 
		AND d.seqpessoa = a.seqpessoa
		LEFT JOIN implantacao.cadan_pixpedidos e 
		ON e.nroempresa = a.nroempresa 
		AND e.nropedvenda = a.nropedvenda 
		AND e.seqpessoa = a.seqpessoa
    WHERE 1=1														 
    AND a.nroformapagto = 19
    AND a.situacaoped = 'A'
		AND e.pixdtaimportacao IS NULL
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
						 e.pixdtaimportacao	 
		ORDER BY 1 ASC