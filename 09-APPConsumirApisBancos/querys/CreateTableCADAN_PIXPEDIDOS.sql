-- Create table
create table IMPLANTACAO.CADAN_PIXPEDIDOS
(
  NROPEDVENDA        NUMBER(10) not null,
  NROEMPRESA         NUMBER(6) not null,
  SEQPESSOA          NUMBER not null,
  NROREPRESENTANTE   NUMBER(5) not null,
  INDENTREGARETIRA   VARCHAR2(1) not null,
  SITUACAOPED        VARCHAR2(1) not null,
  VLRPEDIDO          NUMBER(12,2) not null,
  DTAINCLUSAO        DATE not null,
  DTABASEFATURAMENTO DATE,
  DTAIMPORTACAO      DATE not null,
  TXID               VARCHAR2(35),
  DOCIMPRESSO        VARCHAR2(1) default 'N'
)
tablespace TSD_CONSINCO
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 8K
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table IMPLANTACAO.CADAN_PIXPEDIDOS
  add constraint PK_ID_CADAN_PIXPEDIDOS primary key (NROEMPRESA, NROPEDVENDA)
  using index 
  tablespace TSD_CONSINCO
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
