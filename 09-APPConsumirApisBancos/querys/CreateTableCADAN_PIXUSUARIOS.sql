-- Create table
create table IMPLANTACAO.CADAN_PIXUSUARIOS
(
  USERID            INTEGER not null,
  USEREMPRESA       NUMBER(6),
  USERLOGIN         VARCHAR2(12) not null,
  USERNOME          VARCHAR2(40) not null,
  USERSENHA         VARCHAR2(100) not null,
  USERDTACADASTRO   DATE not null,
  USERDTAIMPORTACAO DATE
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
