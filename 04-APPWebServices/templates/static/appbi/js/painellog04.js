var anoSelecionado = 0
var arrayColaboradores = [
        {valor: 0, texto: 'Todos'},
        {valor: 1, texto: 'Salmo'},
        {valor: 2, texto: 'Kebson'},
        {valor: 3, texto: 'Edilson'},
        {valor: 4, texto: 'Hilson'}
    ]
var options = arrayColaboradores
var select = document.getElementById("colaboradores-Select")
options.forEach(function(opcao){
    var option = document.createElement("option")   // Criar um novo elemento option
    option.value = opcao.valor                      // Definir o valor da opção
    option.text = opcao.texto                       // Definir o texto da opção
    select.add(option)                              // Adicionar a opção ao select
})
select.value = "0"

function onChance(element, callback){
    var previousValue = element.value
    element.addEventListener(
        'change',
        function (){
            var currentValue = element.value
            if (currentValue !== previousValue){
                callback(currentValue)
                previousValue = currentValue
            }
        }
    )
}

function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {

        google.charts.load('current', {'packages':['corechart'], 'language': 'pt-BR'})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart(){
            var arrayRuas = jsonData.Ruas
            var numeroRuas = arrayRuas.length

            for (var i = 1; i <= numeroRuas; i++)
                document.getElementById('log-rua'+i).innerHTML = arrayRuas[i-1]

            var opcaoSelecionada = document.getElementById('colaboradores-Select')
            onChance(opcaoSelecionada, function(value){

            })

            /*
            const formatter = new Intl.NumberFormat('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})
            var mesDiaInicial = jsonData.DataInicial
            var mesDiaFinal = jsonData.DataFinal
            var mesDiaAtual = jsonData.DataAtual

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

            document.getElementById('vendamatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('vendamatriz-data-dia').innerHTML = mesDiaAtual
            document.getElementById('pesomatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('pesomatriz-data-dia').innerHTML = mesDiaAtual
            document.getElementById('margemmatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('tendenciamatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaomatriz-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaomatriz-data-dia').innerHTML = mesDiaAtual
            document.getElementById('vendamatriz-valormes').innerHTML = formatter.format(vendaMesMatriz)
            document.getElementById('vendamatriz-valordia').innerHTML = formatter.format(vendaDiaMatriz)
            document.getElementById('pesomatriz-valormes').innerHTML = formatter.format(pesoMesMatriz)
            document.getElementById('pesomatriz-valordia').innerHTML = formatter.format(pesoDiaMatriz)
            document.getElementById('margemlucro-matriz').innerHTML = formatter.format(margemLucroMesMatriz)
            document.getElementById('tendencia-matriz').innerHTML = formatter.format(tendenciaMatriz)
            document.getElementById('devolucaomatriz-valormes').innerHTML = formatter.format(devolucaoMesMatriz)
            document.getElementById('devolucaomatriz-valordia').innerHTML = formatter.format(devolucaoDiaMatriz)
            document.getElementById('estoqueqtd-matriz').innerHTML = formatter.format(estoqueMatrizQtd)
            document.getElementById('estoquevlr-matriz').innerHTML = formatter.format(estoqueMatrizVlr)

            document.getElementById('vendafilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('vendafilial-data-dia').innerHTML = mesDiaAtual
            document.getElementById('pesofilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('pesofilial-data-dia').innerHTML = mesDiaAtual
            document.getElementById('margemfilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('tendenciafilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaofilial-data-mes').innerHTML = mesDiaInicial + ' à ' + mesDiaFinal
            document.getElementById('devolucaofilial-data-dia').innerHTML = mesDiaAtual
            document.getElementById('vendafilial-valormes').innerHTML = formatter.format(vendaMesFilial)
            document.getElementById('vendafilial-valordia').innerHTML = formatter.format(vendaDiaFilial)
            document.getElementById('pesofilial-valormes').innerHTML = formatter.format(pesoMesFilial)
            document.getElementById('pesofilial-valordia').innerHTML = formatter.format(pesoDiaFilial)
            document.getElementById('margemlucro-filial').innerHTML = formatter.format(margemLucroMesFilial)
            document.getElementById('tendencia-filial').innerHTML = formatter.format(tendenciaFilial)
            document.getElementById('devolucaofilial-valormes').innerHTML = formatter.format(devolucaoMesFilial)
            document.getElementById('devolucaofilial-valordia').innerHTML = formatter.format(devolucaoDiaFilial)
            document.getElementById('estoqueqtd-filial').innerHTML = formatter.format(estoqueFilialQtd)
            document.getElementById('estoquevlr-filial').innerHTML = formatter.format(estoqueFilialVlr)
            */
        }
    })
}
