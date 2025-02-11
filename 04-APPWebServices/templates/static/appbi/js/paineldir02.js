function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {

        google.charts.load('current', {'packages':['corechart'], 'language': 'pt-BR'})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart() {
            const formatter = new Intl.NumberFormat('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})
            var pesoLiberado = jsonData.pesoatendidolib
            var valorLiberado = jsonData.valoratendidolib
            var pesoSeparacao = jsonData.pesoatendidosep
            var valorSeparacao = jsonData.valoratendidosep

            document.getElementById('PesoLiberado').innerHTML = formatter.format(pesoLiberado)
            document.getElementById('ValorLiberado').innerHTML = formatter.format(valorLiberado)
            document.getElementById('PesoSeparacao').innerHTML = formatter.format(pesoSeparacao)
            document.getElementById('ValorSeparacao').innerHTML = formatter.format(valorSeparacao)


            if (valorLiberado == 0){
                var pesoLiberado = new google.visualization.DataTable();
                pesoSeparacao.addColumn('string', 'Topping');
                pesoSeparacao.addColumn('number', 'Slices');
                pesoSeparacao.addRows([
                    ['SEM INFORMAÇÃO', 0]
                ])
            } else {
                jsonData.jsonpesoatendidolib[0].push({role: 'style'})
                jsonPesoAtendidoLib = jsonData.jsonpesoatendidolib
                for (let i = 1; i < jsonPesoAtendidoLib.length; i++) {
                    jsonPesoAtendidoLib[i].push('color: #222c66')
                }
                var pesoLiberacao = google.visualization.arrayToDataTable(jsonPesoAtendidoLib)
            }

            var barchart_options = {
                    title: 'Análise de Peso por Segmento',
                    legend: 'none',
                    titleTextStyle: {fontSize: 24},
                    hAxis: {
                        title: 'Segmentos',
                        titleTextStyle: {fontSize: 20},
                        fontColor: '#222c66'
                    },
                    vAxis: {
                        title: 'Valor',
                        titleTextStyle: {fontSize: 20},
                        format: '#,##0.00'
                    },
                    tooltip: {
                        isHtml: true,
                        trigger: 'both',

                    },
                    bar: {groupWidth: '90%'},
                }
            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-liberacao'))
            barchart.draw(pesoLiberacao, barchart_options)

            if (valorSeparacao == 0){
                var pesoSeparacao = new google.visualization.DataTable();
                pesoSeparacao.addColumn('string', 'Topping');
                pesoSeparacao.addColumn('number', 'Slices');
                pesoSeparacao.addRows([
                    ['SEM INFORMAÇÃO NO MOMENTO', 0]
                ])
            } else {
                jsonData.jsonpesoatendidosep[0].push({role: 'style'})
                jsonPesoAtendidoSep = jsonData.jsonpesoatendidosep
                for (let i = 1; i < jsonPesoAtendidoSep.length; i++) {
                    jsonPesoAtendidoSep[i].push('color: #222c66')
                }
                var pesoSeparacao = google.visualization.arrayToDataTable(jsonPesoAtendidoSep)
            }

            var barchart_options = {
                    title: 'Análise de Peso por Segmento',
                    legend: 'none',
                    titleTextStyle: {textAlign: 'center', fontSize: 24},
                    hAxis: {
                        title: 'Segmentos',
                        titleTextStyle: {fontSize: 20},
                    },
                    vAxis: {
                        title: 'Valor',
                        titleTextStyle: {fontSize: 20},
                        format: '#,##0.00'
                    },
                    tooltip: {
                        isHtml: true,
                        trigger: 'both',

                    },
                    bar: {groupWidth: '90%'},
                }
            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-separacao'))
            barchart.draw(pesoSeparacao, barchart_options)
        }
    })
}
