function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {

        google.charts.load('current', {'packages':['corechart'], 'language': 'pt-BR'})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart() {
            const formatter = new Intl.NumberFormat('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})
            var mesDiaInicial = jsonData.DataInicial
            var mesDiaFinal = jsonData.DataFinal
            var mesDiaAtual = jsonData.DataAtual
            /* DADOS DA EMPRESA MATRIZ */
            var vendaMesMatriz = jsonData.VendasMesMatriz
            var vendaDiaMatriz = jsonData.VendasDiaMatriz
            var pesoMesMatriz = jsonData.PesoBrutoMesMatriz
            var pesoDiaMatriz = jsonData.PesoBrutoDiaMatriz
            var margemLucroMesMatriz = jsonData.MargemLucroMesMatriz
            var tendenciaMatriz = jsonData.TendenciasMatriz
            var devolucaoMesMatriz = jsonData.DevolucoesMesMatriz
            var devolucaoDiaMatriz = jsonData.DevolucoesDiaMatriz
            var estoqueMatrizQtd = jsonData.EstoqueGeralQtdMatriz
            var estoqueMatrizVlr = jsonData.EstoqueGeralVlrMatriz
            var estoque90DiasMatriz = jsonData.Estoque90DiasVlrMatriz
            /* DADOS DA EMPRESA FILIAL */
            var vendaMesFilial = jsonData.VendasMesFilial
            var vendaDiaFilial = jsonData.VendasDiaFilial
            var pesoMesFilial = jsonData.PesoBrutoMesFilial
            var pesoDiaFilial = jsonData.PesoBrutoDiaFilial
            var margemLucroMesFilial = jsonData.MargemLucroMesFilial
            var tendenciaFilial = jsonData.TendenciasFilial
            var devolucaoMesFilial = jsonData.DevolucoesMesFilial
            var devolucaoDiaFilial = jsonData.DevolucoesDiaFilial
            var estoqueFilialQtd = jsonData.EstoqueGeralQtdFilial
            var estoqueFilialVlr = jsonData.EstoqueGeralVlrFilial
            var estoque90DiasFilial = jsonData.Estoque90DiasVlrFilial

            /* DADOS DA EMPRESA MATRIZ */
            /*
            document.getElementById('vendamatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('vendamatriz-data-dia').innerHTML = mesDiaAtual
            document.getElementById('pesomatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('pesomatriz-data-dia').innerHTML = mesDiaAtual
            document.getElementById('margemmatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('tendenciamatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaomatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaomatriz-data-dia').innerHTML = mesDiaAtual
            */
            document.getElementById('vendamatriz-valormes').innerHTML = formatter.format(vendaMesMatriz)
            document.getElementById('vendamatriz-valordia').innerHTML = formatter.format(vendaDiaMatriz)
            document.getElementById('pesomatriz-valormes').innerHTML = formatter.format(pesoMesMatriz)
            document.getElementById('pesomatriz-valordia').innerHTML = formatter.format(pesoDiaMatriz)
            document.getElementById('margemlucro-matriz').innerHTML = formatter.format(margemLucroMesMatriz)
            document.getElementById('tendencia-matriz').innerHTML = formatter.format(tendenciaMatriz)
            document.getElementById('devolucaomatriz-valormes').innerHTML = formatter.format(devolucaoMesMatriz)
            document.getElementById('devolucaomatriz-valordia').innerHTML = formatter.format(devolucaoDiaMatriz)
            document.getElementById('estoquevlr-matriz').innerHTML = formatter.format(estoqueMatrizVlr)
            document.getElementById('estoquevlr90dias-matriz').innerHTML = formatter.format(estoque90DiasMatriz)
            /* DADOS DA EMPRESA FILIAL */
            /*
            document.getElementById('vendafilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('vendafilial-data-dia').innerHTML = mesDiaAtual
            document.getElementById('pesofilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('pesofilial-data-dia').innerHTML = mesDiaAtual
            document.getElementById('margemfilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('tendenciafilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaofilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaofilial-data-dia').innerHTML = mesDiaAtual
            */
            document.getElementById('vendafilial-valormes').innerHTML = formatter.format(vendaMesFilial)
            document.getElementById('vendafilial-valordia').innerHTML = formatter.format(vendaDiaFilial)
            document.getElementById('pesofilial-valormes').innerHTML = formatter.format(pesoMesFilial)
            document.getElementById('pesofilial-valordia').innerHTML = formatter.format(pesoDiaFilial)
            document.getElementById('margemlucro-filial').innerHTML = formatter.format(margemLucroMesFilial)
            document.getElementById('tendencia-filial').innerHTML = formatter.format(tendenciaFilial)
            document.getElementById('devolucaofilial-valormes').innerHTML = formatter.format(devolucaoMesFilial)
            document.getElementById('devolucaofilial-valordia').innerHTML = formatter.format(devolucaoDiaFilial)
            document.getElementById('estoquevlr-filial').innerHTML = formatter.format(estoqueFilialVlr)
            document.getElementById('estoquevlr90dias-filial').innerHTML = formatter.format(estoque90DiasFilial)
        }
    })
}
