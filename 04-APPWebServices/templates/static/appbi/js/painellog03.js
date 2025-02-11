function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {

        google.charts.load('current', {'packages':['corechart'], 'language': 'pt-BR'})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart() {
            const formatter = new Intl.NumberFormat('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})
            var pesoBrutoLibRetira = jsonData.pesobrutolibretira
            var pesoBrutoLibEntrega = jsonData.pesobrutolibentrega
            var pesoBrutoSepRetira = jsonData.pesobrutosepretira
            var pesoBrutoSepEntrega = jsonData.pesobrutosepentrega
            var totalPesoBrutoLiberado = jsonData.totalpesobrutoliberado
            var totalPesoBrutoSeparacao = jsonData.totalpesobrutoseparacao

            document.getElementById('PesoBrutoLibRetira').innerHTML = formatter.format(pesoBrutoLibRetira/1000)
            document.getElementById('PesoBrutoLibEntrega').innerHTML = formatter.format(pesoBrutoLibEntrega/1000)
            document.getElementById('PesoBrutoSetRetira').innerHTML = formatter.format(pesoBrutoSepRetira/1000)
            document.getElementById('PesoBrutoSetEntrega').innerHTML = formatter.format(pesoBrutoSepEntrega/1000)

            if (totalPesoBrutoLiberado == 0){
                var pesoLiberado = new google.visualization.DataTable();
                pesoSeparacao.addColumn('string', 'Topping');
                pesoSeparacao.addColumn('number', 'Slices');
                pesoSeparacao.addRows([
                    ['SEM INFORMAÇÃO', 0]
                ])
            } else {
                jsonData.jsonpesobrutoliberado[0].push({role: 'style'})
                jsonPesoBrutoLiberado = jsonData.jsonpesobrutoliberado
                for (let i = 1; i < jsonPesoBrutoLiberado.length; i++) {
                    jsonPesoBrutoLiberado[i].push('color: #222c66')
                }
                var pesoLiberacao = google.visualization.arrayToDataTable(jsonPesoBrutoLiberado)
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

            if (totalPesoBrutoSeparacao == 0){
                var pesoSeparacao = new google.visualization.DataTable();
                pesoSeparacao.addColumn('string', 'Topping');
                pesoSeparacao.addColumn('number', 'Slices');
                pesoSeparacao.addRows([
                    ['SEM INFORMAÇÃO NO MOMENTO', 0]
                ])
            } else {
                jsonData.jsonpesobrutoseparacao[0].push({role: 'style'})
                jsonPesoBrutoSeparacao = jsonData.jsonpesobrutoseparacao
                for (let i = 1; i < jsonPesoBrutoSeparacao.length; i++) {
                    jsonPesoBrutoSeparacao[i].push('color: #222c66')
                }
                var pesoSeparacao = google.visualization.arrayToDataTable(jsonPesoBrutoSeparacao)
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
