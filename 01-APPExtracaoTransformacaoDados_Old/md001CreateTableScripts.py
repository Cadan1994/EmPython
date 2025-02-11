""" CRIAÇÃO DAS TABELAS DIMENSÕES """

CreateTable_GeEmpresa \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Empresa
    (
        nroempresa INTEGER NOT NULL,
        nrocgc CHARACTER VARYING (15) NOT NULL,
        razaosocial CHARACTER VARYING (60) NOT NULL,
        nomereduzido CHARACTER VARYING (12) NOT NULL,
        cep CHARACTER VARYING (8) NOT NULL,
        endereco CHARACTER VARYING (40) NOT NULL,
        bairro CHARACTER VARYING (30) NOT NULL,
        cidade CHARACTER VARYING (30) NOT NULL,
        estado CHARACTER VARYING (2) NOT NULL,
        CONSTRAINT pk_id_geempresa PRIMARY KEY (nroempresa)
    )
    """
CreateTable_GePessoa \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Pessoa
    (
        seqpessoa INTEGER NOT NULL,
        nrocgccpf CHARACTER VARYING(15) NOT NULL,
        nomerazao CHARACTER VARYING(100) NOT NULL,
        fantasia CHARACTER VARYING(30) NOT NULL,
        fisicajuridica CHARACTER VARYING(1) NOT NULL,
        atividade CHARACTER VARYING(35) NOT NULL,
        cep CHARACTER VARYING(8) NOT NULL,
        seqlogradouro INTEGER NOT NULL,
        seqbairro INTEGER NOT NULL,
        seqcidade INTEGER NOT NULL,
        dtainclusao DATE NOT NULL,
        dtaativacao DATE NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_gepessoa PRIMARY KEY (seqpessoa)
    )
    """
CreateTable_MaxDivisao \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Divisao 
    (
        nrodivisao INTEGER NOT NULL,
        divisao CHARACTER VARYING(15) NOT NULL,
        tipdivisao CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_maxdivisao PRIMARY KEY (nrodivisao)
    )
    """
CreateTable_MadSegmento \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Segmento
    (
        nrosegmento INTEGER NOT NULL,
        nrodivisao INTEGER NOT NULL,
        descsegmento CHARACTER VARYING(15) NOT NULL,
        indprecoembalagem CHARACTER VARYING(1) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madsegmento PRIMARY KEY (nrosegmento)
    )
    """
CreateTable_MapMarca \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Marca
    (
        seqmarca INTEGER NOT NULL,
        marca CHARACTER VARYING(20) NOT NULL,
        tipomarca CHARACTER VARYING(1) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madmarca PRIMARY KEY (seqmarca)
    )
    """
CreateTable_MapFamilia \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Familia
    (
        seqfamilia INTEGER NOT NULL,
        seqmarca INTEGER NOT NULL,
        familia CHARACTER VARYING(35) NOT NULL,
        pesavel CHARACTER VARYING(1) NOT NULL,
        aliqpadraoicms NUMERIC(15,2) NOT NULL,
        dtahorinclusao DATE NOT NULL,
        CONSTRAINT pk_id_madfamilia PRIMARY KEY (seqfamilia)
    )
    """
CreateTable_MapFamiliaDivisao \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_FamiliaDivisao
    (
        seqfamilia INTEGER NOT NULL,
        nrodivisao INTEGER NOT NULL,
        seqcomprador INTEGER NOT NULL,
        nrotributacao INTEGER NOT NULL,
        finalidadefamilia CHARACTER VARYING(1) NOT NULL,
        formaabastecimento CHARACTER VARYING(1) NOT NULL,
        dtahorinclusao DATE NOT NULL,
        CONSTRAINT pk_id_mapfamiliadivisao PRIMARY KEY (seqfamilia,nrodivisao)
    )
    """
CreateTable_MapFamiliaEmbalagem \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_FamiliaEmbalagem
    (
        seqfamilia INTEGER NOT NULL,
        qtdembalagem INTEGER NOT NULL,
        embalagem CHARACTER VARYING(2) NOT NULL,
        qtdunidemb INTEGER NOT NULL,
        pesobruto NUMERIC(7,3) NOT NULL,
        pesoliquido NUMERIC(7,3) NOT NULL,
        CONSTRAINT pk_id_mapfamilia PRIMARY KEY (seqfamilia,qtdembalagem)
    )
    """
CreateTable_MaxCodigoGeralOperacao \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_CodigoGeralOperacao
    (
        codgeraloper INTEGER NOT NULL,
        tipcgo CHARACTER VARYING(1) NOT NULL,
        tipuso CHARACTER VARYING(1) NOT NULL,
        tipdocfiscal CHARACTER VARYING(1) NOT NULL,
        tipdocfiscaldesc CHARACTER VARYING(40) NOT NULL,
        descricao CHARACTER VARYING(40) NOT NULL,
        acmcompravenda CHARACTER VARYING(1) NOT NULL,
        geralteracaoestq CHARACTER VARYING(1) NOT NULL,
        indtipocgomovto CHARACTER VARYING(2) NOT NULL,
        indtipocgomovtodesc CHARACTER VARYING(40) NOT NULL,
        indgeradebcredpis CHARACTER VARYING(1) NOT NULL,
        indgeradebcredcofins CHARACTER VARYING(1) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_maxcodigogeraloperacao PRIMARY KEY (codgeraloper)
    )
    """
CreateTable_GePessoaCadastro \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_PessoaCadastro
    (
        seqpessoa INTEGER NOT NULL,
        situacaocredito CHARACTER VARYING(1) NOT NULL,
        limitecredito NUMERIC(15,2) NOT NULL,
        obscredito CHARACTER VARYING(200) NOT NULL,
        CONSTRAINT pk_id_gepessoacadastro PRIMARY KEY (seqpessoa)
    )
    """
CreateTable_MrlCliente \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Cliente
    (
        seqpessoa INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        pzopagtomaximo INTEGER NOT NULL,
        observacao CHARACTER VARYING(250) NOT NULL,
        dtacadastro DATE NOT NULL,
        dtaativou DATE NOT NULL,
        dtainativou DATE NOT NULL,
        dtaultcompra DATE NOT NULL,
        statuscliente CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_mrlcliente PRIMARY KEY (nroempresa,seqpessoa)
    )
    """
CreateTable_MadRepresentante \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Representante
    (
        nrorepresentante INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        nrosegmento INTEGER NOT NULL,
        nroequipe INTEGER NOT NULL,
        apelido CHARACTER VARYING(15) NOT NULL,
        tiprepresentante CHARACTER VARYING(1) NOT NULL,
        indcomissao CHARACTER VARYING(1) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madrepresentante PRIMARY KEY (nrorepresentante)
    )
    """
CreateTable_MadClienteRepresentante \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_ClienteRepresentante
    (
        seqpessoa INTEGER NOT NULL,
        nrorepresentante integer null,
        dtainclusao DATE NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madclienterepresentante PRIMARY KEY (seqpessoa,nrorepresentante)
    )
    """
CreateTable_MapFamiliaFornecedor \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_FamiliaFornecedor
    (
        seqfamilia INTEGER NOT NULL,
        seqfornecedor INTEGER NOT NULL,
        principal CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_mapfamiliafornecedor PRIMARY KEY (seqfamilia,seqfornecedor)
    )
    """
CreateTable_MadEquipe \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Equipe
    (
        nroequipe INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        nroregiao INTEGER NOT NULL,
        desequipe CHARACTER VARYING(40) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madequipe PRIMARY KEY (nroequipe)
    )
    """
CreateTable_MapProduto \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Produto
    (
        seqproduto INTEGER NOT NULL,
        seqfamilia INTEGER NOT NULL,
        seqmarca INTEGER NOT NULL,
        seqsecao INTEGER NOT NULL,
        seqcategoria INTEGER NOT NULL,
        seqsubcategoria INTEGER NOT NULL,
        seqfornecedor INTEGER NOT NULL,
        descompleta CHARACTER VARYING(50) NOT NULL,
        desreduzida CHARACTER VARYING(24) NOT NULL,
        complemento CHARACTER VARYING(50) NOT NULL,
        dtahorinclusao DATE NOT NULL,
        indprocfabricacao CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_mapproduto PRIMARY KEY (seqproduto)
    )
    """
CreateTable_GeLogradouro \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Logradouro
    (
        seqlogradouro INTEGER NOT NULL,
        seqcidade INTEGER NOT NULL,
        logradouro CHARACTER VARYING(150) NOT NULL,
        tiplogradouro CHARACTER VARYING(25) NOT NULL,
        CONSTRAINT pk_id_gelogradouro PRIMARY KEY(seqlogradouro,seqcidade)
    )
    """
CreateTable_GeBairro \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Bairro
    (
        seqbairro INTEGER NOT NULL,
        seqcidade INTEGER NOT NULL,
        bairro CHARACTER VARYING(100) NOT NULL,
        CONSTRAINT pk_id_gebairro PRIMARY KEY(seqbairro,seqcidade)
    )
    """
CreateTable_GeCidade \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_Cidade
    (
        seqcidade INTEGER NOT NULL,
        cidade CHARACTER VARYING(60) NOT NULL,
        uf CHARACTER VARYING(2) NOT NULL,
        cepinicial CHARACTER VARYING(8) NOT NULL,
        cepfinal CHARACTER VARYING(8) NOT NULL,
        codibge CHARACTER VARYING(7) NOT NULL,
        CONSTRAINT pk_id_gecidade PRIMARY KEY(seqcidade)
    )
    """
CreateTable_MafFornecedor \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_fornecedor
    (
        seqfornecedor INTEGER NOT NULL,
        tipfornecedor CHARACTER VARYING(20) NOT NULL,
        nomerazaosocial CHARACTER VARYING(100),
        status CHARACTER VARYING(1),
        CONSTRAINT pk_id_maffornecedor PRIMARY KEY (seqfornecedor)
    )
    """
CreateTable_GeBanco \
  = """ 
    CREATE TABLE IF NOT EXISTS pbi.carga_banco
    (
        nrobanco INTEGER NOT NULL,
        razaosocial CHARACTER VARYING(40) NOT NULL,
        fantasia CHARACTER VARYING(20) NOT NULL,
        CONSTRAINT pk_id_gebanco PRIMARY KEY (nrobanco)
    )
    """
CreateTable_FiEspecie \
  = """ 
    CREATE TABLE IF NOT EXISTS pbi.carga_tituloespecie
    (
        seqespecie INTEGER NOT NULL,
        codespecie CHARACTER VARYING(6) NOT NULL,
        descricao CHARACTER VARYING(40) NOT NULL,
        descreduzida CHARACTER VARYING(40) NOT NULL,
        obrigdireito CHARACTER VARYING(1) NOT NULL,
        qtddiasatraso INTEGER NOT NULL,
        taxamaxjuros NUMERIC(5,2) NOT NULL ,
        taxaminjuros NUMERIC(5,2) NOT NULL,
        permultaatraso NUMERIC(5,2) NOT NULL,
        tipoespecie CHARACTER VARYING(1) NOT NULL,
        observacao CHARACTER VARYING(250) NOT NULL,
        CONSTRAINT pk_id_fiespecie PRIMARY KEY (seqespecie,codespecie)
    )
    """
CreateTable_MadMetaRepresentante \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_MetaRepresentante
    (
        seqmetaperiodo INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        dtainicial DATE NOT NULL,
        dtafinal DATE NOT NULL,
        nrorepresentante INTEGER NOT NULL,
        metavlrvenda NUMERIC(13,2) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madmetarepresentante PRIMARY KEY (seqmetaperiodo,nrorepresentante)
    )
    """
CreateTable_MadMetaEquipe \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_MetaEquipe
    (
        seqmetaperiodo INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        dtainicial DATE NOT NULL,
        dtafinal DATE NOT NULL,
        nroequipe INTEGER NOT NULL,
        metavlrvenda NUMERIC(13,2) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madmetaequipe PRIMARY KEY (seqmetaperiodo,nroequipe)
    )
    """
CreateTable_MrlCustoDiaFamilia \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_CustoDiaFamilia
    (
        id INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        nroequipe INTEGER NOT NULL,
        codgeraloper INTEGER NOT NULL,
        dtaentradasaida DATE NOT NULL,
        vlrcustoliquido NUMERIC(10,2) NOT NULL,
        vlrdespoperacional NUMERIC(10,2) NOT NULL,
        vlrlucratividade NUMERIC(10,2) NOT NULL,
        CONSTRAINT pk_id_custodiafamilia PRIMARY KEY(id)
    )
    """
CreateTable_MaxvABCDistribuicaoBase \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_ABCDistribuicaoBase
    (
        id INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        nrosegmento INTEGER NOT NULL,
        nroequipe INTEGER NOT NULL,
        nrorepresentante INTEGER NOT NULL,
        nrodivisao INTEGER NOT NULL,
        tipnotafiscal CHARACTER VARYING(1) NOT NULL,
        codgeraloper INTEGER NOT NULL,
        nrodocto INTEGER NOT NULL,
        seriedocto CHARACTER VARYING(3) NOT NULL,
        dtahorlancto DATE NOT NULL,
        dtavda DATE NOT NULL,
        dtavencimento DATE NOT NULL,
        seqproduto INTEGER NOT NULL,
        seqprodutocusto INTEGER NOT NULL,
        nfreferencianro INTEGER NOT NULL,
        nfreferenciaserie CHARACTER VARYING(3) NOT NULL,
        qtditembruta NUMERIC(10) NOT NULL,
        qtditemliquida NUMERIC(10) NOT NULL,
        qtditemdevolucao NUMERIC(10) NOT NULL,
        vlrvendabruta NUMERIC(10,2) NOT NULL,
        vlrvendaliquida NUMERIC(10,2) NOT NULL,
        vlrdevolucao NUMERIC(10,2) NOT NULL,
        vlrdesconto NUMERIC(10,2) NOT NULL,
        vlricms NUMERIC(10,2) NOT NULL,
        vlripi NUMERIC(10,2) NOT NULL,
        vlrpis NUMERIC(10,2) NOT NULL,
        vlrcofins NUMERIC(10,2) NOT NULL,
        vlrcomissao NUMERIC(10,2) NOT NULL,
        vlrdespesaoperacional NUMERIC(10,2) NOT NULL,
        vlrimposto NUMERIC(10,2) NOT NULL,
        CONSTRAINT pk_id_abcdistribbase PRIMARY KEY(id)
    )
    """

CreateTable_MadPedidoVenda \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_pedido
    (
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        nrosegmento INTEGER NOT NULL,
        nrorepresentante INTEGER NOT NULL,
        nropedvenda INTEGER NOT NULL,
        codgeraloper INTEGER NOT NULL,
        dtainclusao DATE NOT NULL,
        nrotabvenda CHARACTER VARYING(3) NOT NULL,
        nroformapagto INTEGER NOT NULL,
        situacaoped CHARACTER VARYING(1) NOT NULL,
        indentregaretira CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madpedvenda PRIMARY KEY (nroempresa,nropedvenda)
    )
    """
CreateTable_MadPedidoVendaItem \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_pedidoitem(
        nroempresa INTEGER NOT NULL,
        nropedvenda INTEGER NOT NULL,
        seqpedvendaitem INTEGER NOT NULL,
        seqproduto INTEGER NOT NULL,
        qtdembalagem NUMERIC(12,6) NOT NULL,
        qtdpedida NUMERIC(12,3) NOT NULL,
        qtDATEndida NUMERIC(12,3) NOT NULL,
        vlrembtabpreco NUMERIC(13,2) NOT NULL,
        vlrembtabpromoc NUMERIC(13,2) NOT NULL,
        vlrembinformado NUMERIC(13,2) NOT NULL,
        vlrembdesconto NUMERIC(17,6) NOT NULL,
        vlrtotcomissao NUMERIC(15,4) NOT NULL,
        percomissao NUMERIC(7,4) NOT NULL,
        dtainclusao DATE NOT NULL,
        CONSTRAINT pk_id_madpedvendaitem PRIMARY KEY (nroempresa,nropedvenda,seqpedvendaitem)
    )
    """
CreateTable_MadRepresentanteCtaFlex \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_representantectaflex
    (
        seqlanctoflex INTEGER NOT NULL,
        nroempresa INTEGER NOT NULL,
        nrosegmento INTEGER NOT NULL,
        nroequipe INTEGER NOT NULL,
        nropedvenda INTEGER NOT NULL,
        nrorepresentante INTEGER NOT NULL,
        tipolancamento CHARACTER VARYING(1) NOT NULL,
        dtalancamento DATE NOT NULL,
        dtahorlancto DATE NOT NULL,
        valor NUMERIC (13,2) NOT NULL,
        usulancto CHARACTER VARYING(12) NOT NULL,
        historico CHARACTER VARYING(250) NOT NULL,
        situacaolancto CHARACTER VARYING(1) NOT NULL,
        CONSTRAINT pk_id_madrepresentantectaflex PRIMARY KEY (seqlanctoflex)
    )
    """
CreateTable_FiTitulo \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_titulo
    (
        nroempresa INTEGER NOT NULL,
        nrosegmento INTEGER NOT NULL,
        nroequipe INTEGER NOT NULL,
        nrorepresentante INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        seqtitulo NUMERIC(15) NOT NULL,
        nrotitulo NUMERIC(10) NOT NULL,
        nrobanco INTEGER NOT NULL,
        serietitulo CHARACTER VARYING(6) NOT NULL,
        codespecie CHARACTER VARYING(6) NOT NULL,
        nroparcela CHARACTER VARYING(3) NOT NULL,
        abertoquitado CHARACTER VARYING(1) NOT NULL,
        situacao CHARACTER VARYING(1) NOT NULL,
        tipovencoriginal CHARACTER VARYING(1) NOT NULL,
        obrigdireito CHARACTER VARYING(1) NOT NULL,
        dtainclusao DATE NOT NULL,
        dtaemissao DATE NOT NULL,
        dtavencimento DATE NOT NULL,
        dtamovimento DATE NOT NULL,
        dtaprogramada DATE NOT NULL,
        dtaquitacao DATE NOT NULL,
        qtdparcela NUMERIC(3,0) NOT NULL,
        vlroriginal NUMERIC(15,2) NOT NULL,
        vlrnominal NUMERIC(15,5) NOT NULL,
        vlrpago NUMERIC(15,5) NOT NULL,
        vlrtarifa NUMERIC(15,2) NOT NULL,
        vlrjuromora NUMERIC(15,2) NOT NULL,
        vlrmulta NUMERIC(15,2) NOT NULL,
        vlrdesccomercial NUMERIC(15,2) NOT NULL, 
        CONSTRAINT pk_id_fititulo PRIMARY KEY (seqtitulo)
    )       
    """
CreateTable_MrlProdutoEmpresa \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_estoque
    (
        nroempresa INTEGER NOT NULL,
        seqproduto INTEGER NOT NULL,
        estqdeposito NUMERIC(12,3) NOT NULL,
        qtdreservadavda NUMERIC(12,3) NOT NULL,
        dtahorultmovtoestq DATE NOT NULL,
        CONSTRAINT pk_id_mrlprodutoempresa PRIMARY KEY (nroempresa,seqproduto)    
    )

    """
CreateTable_MadMovimentacaoCompra \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_movimentacaocompra
    (
        nroempresa INTEGER NOT NULL,
        seqproduto INTEGER NOT NULL,
        dtaentrada DATE NOT NULL,
        quantidade NUMERIC(12,2) NOT NULL,
        CONSTRAINT pk_id_madmovimentacaocompra PRIMARY KEY (nroempresa,seqproduto,dtaentrada) 
    )
    """
CreateTable_Mrl_ProdutoStatusVenda \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_produtostatusvenda
    (
        seqproduto INTEGER NOT NULL,
        desccompleta CHARACTER VARYING(255) NOT NULL,
        descreduzida CHARACTER VARYING(255) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_ProdutoStatusVenda PRIMARY KEY (seqproduto) 
    )
    """
CreateTable_Map_ProdutoStatusCompra \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_produtostatuscompra
    (
        seqproduto INTEGER NOT NULL,
        desccompleta CHARACTER VARYING(255) NOT NULL,
        descreduzida CHARACTER VARYING(255) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_ProdutoStatusCompra PRIMARY KEY (seqproduto) 
    )
    """
CreateTable_Mad_PedidosFaturados \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_pedidosfaturados
    (
        nroempresa INTEGER NOT NULL,
        seqpessoa INTEGER NOT NULL,
        dtainclusao DATE NOT NULL,
        seqtitulo NUMERIC(15) NOT NULL,
        nroparcela CHARACTER VARYING(3) NOT NULL,
        numerodf NUMERIC(10) NOT NULL,
        seriedf CHARACTER VARYING(6) NOT NULL,
        nroserieecf CHARACTER VARYING(40) NOT NULL,
        vlritem NUMERIC(15,2) NOT NULL,
        CONSTRAINT pk_id_madpedidofaturado PRIMARY KEY (seqtitulo) 
    )
    """
CreateTable_Map_Secao \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_secoes
    (
        seqsecao INTEGER NOT NULL,
        secao CHARACTER VARYING(25) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_mapsecao PRIMARY KEY (seqsecao)
    )
    """
CreateTable_Map_Categoria \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_categorias
    (
        seqcategoria INTEGER NOT NULL,
        seqsecao INTEGER NOT NULL,
        categoria CHARACTER VARYING(25) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_mapcategoria PRIMARY KEY (seqcategoria,seqsecao)
    )
    """
CreateTable_Map_SubCategoria \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_subcategorias
    (
        seqsubcategoria INTEGER NOT NULL,
        seqcategoria INTEGER NOT NULL,
        seqsecao INTEGER NOT NULL,
        subcategoria CHARACTER VARYING(25) NOT NULL,
        status CHARACTER VARYING(1) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_mapsubcategoria PRIMARY KEY (seqsubcategoria,seqcategoria,seqsecao)
    )
    """
CreateTable_ProdutosPrecoVenda \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_produtoPrecoVenda
    (
        nroempresa INTEGER NOT NULL,
        seqproduto INTEGER NOT NULL,
        precogernormal NUMERIC(15,2) NOT NULL,
        precogerpromoc NUMERIC(15,2) NOT NULL,
        precogervenda NUMERIC(15,2) NOT NULL,
        CONSTRAINT pk_id_ProdutoPrecoVenda PRIMARY KEY (nroempresa,seqproduto) 
    )
    """
CreateTable_ProdutosEmbalagemCompra \
  = """
    CREATE TABLE IF NOT EXISTS pbi.carga_produtoEmbalagemCompra
    (
        seqfamilia INTEGER NOT NULL,
        seqproduto INTEGER NOT NULL,
        qtdembalagem INTEGER NOT NULL,
        embalagem CHARACTER VARYING(2) NOT NULL,
        dtaalteracao DATE NOT NULL,
        CONSTRAINT pk_id_ProdutoEmbalagemCompra PRIMARY KEY (seqfamilia,seqproduto) 
    )
    """
CreateTable_ProdutosEmbalagemVenda \
  = """
      CREATE TABLE IF NOT EXISTS pbi.carga_produtoEmbalagemVenda
      (
          seqfamilia INTEGER NOT NULL,
          seqproduto INTEGER NOT NULL,
          qtdembalagem INTEGER NOT NULL,
          embalagem CHARACTER VARYING(2) NOT NULL,
          dtaalteracao DATE NOT NULL,
          CONSTRAINT pk_id_ProdutoEmbalagemVenda PRIMARY KEY (seqfamilia,seqproduto) 
      )
      """
CreateTable_RedesClientes \
  = """
      CREATE TABLE IF NOT EXISTS pbi.carga_redesclientes
      (
          seqrede INTEGER NOT NULL,
          seqpessoa INTEGER NOT NULL,
          descricao CHARACTER VARYING(40) NOT NULL,
          status CHARACTER VARYING(1) NOT NULL,
          dtaalteracao DATE NOT NULL,
          CONSTRAINT pk_id_clienterede PRIMARY KEY (seqpessoa) 
      )
    """
CreateTable_Compradores \
  = """
      CREATE TABLE IF NOT EXISTS pbi.carga_compradores
      (
          seqfornecedor INTEGER NOT NULL,
          seqcomprador INTEGER NOT NULL,
          compradornome CHARACTER VARYING(40) NOT NULL,
          compradorapelido CHARACTER VARYING(15) NOT NULL,
          dtaalteracao DATE NOT NULL,
          CONSTRAINT pk_id_compradorfornecedor PRIMARY KEY (seqfornecedor,seqcomprador) 
      )
    """
CreateTable_DatasCritica \
  = """
      CREATE TABLE IF NOT EXISTS pbi.carga_datacritica
      (
          nroempresa INTEGER NOT NULL,
          seqproduto INTEGER NOT NULL,
          embalagem char(10) NOT NULL,
          qtdatual NUMERIC(12,3) NOT NULL,
          qtdestoque NUMERIC(12,3) NOT NULL,
          dtavalidade DATE NOT NULL,
          dtaalteracao DATE NOT NULL,
          CONSTRAINT pk_id_empresaproduto PRIMARY KEY (nroempresa,seqproduto) 
      )
    """