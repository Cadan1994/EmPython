function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {
        google.charts.load('current', {'packages':['table']})
        google.charts.load('current', {'packages':['corechart']})
        google.charts.setOnLoadCallback(drawTable)

        function drawTable() {
            /* GRÁFICO QUE REPRESENTA A QUANTIDADE DE PEDIDOS POR TIPO */
            var data = jsonData.jsonpedidostipos
            var pedidosTipos = new google.visualization.arrayToDataTable(data)
            var options = {
                allowHtml: true,
                showRowNumber: false,
                width: '100%',
                height: 'auto',
                cssClassNames: {
                    headerRow: 'header-row',
                    tableRow: 'table-row',
                    headerCell: 'header-cell',
                    tableCell: 'table-cell'
                }
            }
            var charttable = new google.visualization.Table(document.getElementById('table-pedidostipo'))
            charttable.draw(pedidosTipos, options)

            /* GRÁFICO QUE REPRESENTA A QUANTIDADE DE PEDIDOS POR TIPO */
            var data = jsonData.jsonpedidostipos
            jsonData.jsonpedidostipos[0].push({role: 'style'})
            pedidosTiposQuantidade = jsonData.jsonpedidostipos
            for (let i = 1; i < pedidosTiposQuantidade.length; i++) {
                pedidosTiposQuantidade[i].push('color: #222c66')
            }
            var pedidosTiposQuantidade = google.visualization.arrayToDataTable(pedidosTiposQuantidade)

            var options = {
                    legend: 'none'
                }
            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-pedidostipos-quantidade'))
            barchart.draw(pedidosTiposQuantidade, options)

            /* GRÁFICO QUE REPRESENTA A QUANTIDADE DE PEDIDOS POR DATA LIMITE DE FATURA */
            var data = jsonData.jsonpedidosdataslimfatura
            var pedidosDataLimFatura = new google.visualization.arrayToDataTable(data)
            var options = {
                allowHtml: true,
                showRowNumber: false,
                width: '100%',
                height: 'auto',
                cssClassNames: {
                    headerRow: 'header-row',
                    tableRow: 'table-row',
                    headerCell: 'header-cell',
                    tableCell: 'table-cell'
                }
            }
            var charttable = new google.visualization.Table(document.getElementById('table-pedidosdatalimfatura'))
            charttable.draw(pedidosDataLimFatura, options)

            /* GRÁFICO QUE REPRESENTA A QUANTIDADE DE PEDIDOS POR STATUS */
            var data = jsonData.jsonpedidosstatus
            var pedidosStatus = new google.visualization.arrayToDataTable(data)
            var options = {
                allowHtml: true,
                showRowNumber: false,
                width: '100%',
                height: 'auto',
                cssClassNames: {
                    headerRow: 'header-row',
                    tableRow: 'table-row',
                    headerCell: 'header-cell',
                    tableCell: 'table-cell'
                }
            }
            var charttable = new google.visualization.Table(document.getElementById('table-pedidosstatus'))
            charttable.draw(pedidosStatus, options)

            /* GRÁFICO QUE REPRESENTA A QUANTIDADE DE PEDIDOS POR CRÍTICAS */
            var data = jsonData.jsonpedidoscriticas
            var pedidosCriticas = new google.visualization.arrayToDataTable(data)
            var options = {
                allowHtml: true,
                showRowNumber: false,
                width: '100%',
                height: 'auto',
                cssClassNames: {
                    headerRow: 'header-row',
                    tableRow: 'table-row',
                    headerCell: 'header-cell',
                    tableCell: 'table-cell'
                }
            }
            var charttable = new google.visualization.Table(document.getElementById('table-pedidoscriticas'))
            charttable.draw(pedidosCriticas, options)
        }
    })
}
