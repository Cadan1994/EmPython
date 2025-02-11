-- Create table
create table IMPLANTACAO.CADAN_PIXTRANSACOES
(
  PIXEMPRESA        NUMBER(6) not null,
  PIXPESSOA         NUMBER not null,
  PIXTXID           VARCHAR2(35) not null,
  PIXQRCODE         VARCHAR2(255),
  PIXVALOR          NUMBER(12,2) not null,
  PIXSTATUS         VARCHAR2(1) not null,
  PIXDTAENVIO       DATE,
  PIXDTACONSULTA    DATE,
  PIXDTARECEBIMENTO DATE,
  PIXDTAEXPIRACAO   DATE,
  PIXE2EID          VARCHAR2(32)
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
alter table IMPLANTACAO.CADAN_PIXTRANSACOES
  add constraint PK_ID_CADAN_PIXTRANSACOES primary key (PIXEMPRESA, PIXPESSOA, PIXTXID)
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
