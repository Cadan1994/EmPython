-- Create table
create table IMPLANTACAO.CADAN_PIXPARAMETROS
(
  ID        NVARCHAR2(25) not null,
  DESCRICAO NVARCHAR2(255) not null,
  VALOR     NVARCHAR2(1000) not null
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
