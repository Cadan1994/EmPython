    SELECT
        DISTINCT
        a.nroempresa,
        a.seqpessoa,
				b.nomerazao,
				b.fisicajuridica,	
				b.nrocgccpf||b.digcgccpf AS cpfcnpj,	 
				LISTAGG(a.nropedvenda, ', ') WITHIN GROUP (ORDER BY a.nropedvenda) AS npedidos,
				LISTAGG(a.vlrpedido, ', ') WITHIN GROUP (ORDER BY a.vlrpedido) AS vpedidos,
        SUM(a.vlrpedido) AS vlrpedido,	 
        a.nroempresa||
        LPAD(b.nrocgccpf||b.digcgccpf,15, '0')||
        LPAD(a.seqpessoa, 6, '0')||
        'L'||
        LPAD(c.nropedvenda, 12, '0') AS nrotxid
    FROM implantacao.cadan_pixpedidos a
    INNER JOIN implantacao.ge_pessoa b 
    ON b.seqpessoa=a.seqpessoa	 
    INNER JOIN (SELECT 
                    nroempresa, 
                    seqpessoa, 
                    MIN(nropedvenda) nropedvenda
                FROM implantacao.cadan_pixpedidos
                WHERE 1=1
								AND nropedvenda IN (4446771,4446772)
                GROUP BY nroempresa, seqpessoa) c 
    ON c.nroempresa = a.nroempresa 
    AND c.seqpessoa = a.seqpessoa
    LEFT JOIN implantacao.cadan_pixpedidos e 
    ON e.nroempresa = a.nroempresa 
    AND e.nropedvenda = a.nropedvenda 
    AND e.seqpessoa = a.seqpessoa
    WHERE 1=1
		AND a.nropedvenda IN (4446771,4446789,4446810)
    GROUP BY a.nroempresa, 
             a.seqpessoa,
						 b.nomerazao,
						 b.fisicajuridica,
             b.nrocgccpf,
             b.digcgccpf,
						 c.nropedvenda
