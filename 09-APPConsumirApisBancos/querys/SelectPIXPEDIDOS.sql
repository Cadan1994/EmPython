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
--FOR UPDATE