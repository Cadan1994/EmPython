function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {
        google.charts.load('current', {'packages':['corechart']})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart() {
            /* GRÁFICO REFERENTE A QUANTIDADE DE PEDIDOS/ENTREGA POR INTERVALO */
            var data = jsonData.jsonpedidosentregaquantidade
            jsonData.jsonpedidosentregaquantidade[0].push({role: 'style'})
            jsonPedidosEntregaQuantidade = jsonData.jsonpedidosentregaquantidade

            for (let i = 1; i < jsonPedidosEntregaQuantidade.length; i++) {
                jsonPedidosEntregaQuantidade[i].push('color: #222c66')
            }

            var pedidosEntregaQuantidade = google.visualization.arrayToDataTable(jsonPedidosEntregaQuantidade)

            var view = new google.visualization.DataView(pedidosEntregaQuantidade);
            view.setColumns(
                [
                    0,
                    1,
                    2,
                    { calc: "stringify", sourceColumn: 1, type: "string", role: "annotation" }
                ]
            )

            var options = {
                    title: 'Pedidos/Entrega (Quantidade)',
                    legend: 'none',
                    titleTextStyle: {fontSize: 18},
                    bar: {groupWidth: '90%'},
                }

            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-pedentrega-quantidade'))
            barchart.draw(view, options)

            /* GRÁFICO REFERENTE A QUANTIDADE DE PEDIDOS/RETIRA POR INTERVALO */
            var data = jsonData.jsonpedidosretiraquantidade
            jsonData.jsonpedidosretiraquantidade[0].push({role: 'style'})
            jsonPedidosRetiraQuantidade = jsonData.jsonpedidosretiraquantidade

            for (let i = 1; i < jsonPedidosRetiraQuantidade.length; i++) {
                jsonPedidosRetiraQuantidade[i].push('color: #222c66')
            }

            var pedidosRetiraQuantidade = google.visualization.arrayToDataTable(jsonPedidosRetiraQuantidade)

            var view = new google.visualization.DataView(pedidosRetiraQuantidade);
            view.setColumns(
                [
                    0,
                    1,
                    2,
                    { calc: "stringify", sourceColumn: 1, type: "string", role: "annotation" },
                ]
            )

            var options = {
                    title: 'Pedidos/Retira (Quantidade)',
                    legend: 'none',
                    titleTextStyle: {fontSize: 18},
                    bar: {groupWidth: '90%'},
                }

            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-pedretira-quantidade'))
            barchart.draw(view, options)

            /* GRÁFICO REFERENTE AO PESO BRUTO DE PEDIDOS/ENTREGA POR INTERVALO */
            var data = jsonData.jsonpedidosentregapesobruto
            jsonData.jsonpedidosentregapesobruto[0].push({role: 'style'})
            jsonPedidosEntregaPesobruto = jsonData.jsonpedidosentregapesobruto

            for (let i = 1; i < jsonPedidosEntregaPesobruto.length; i++) {
                jsonPedidosEntregaPesobruto[i].push('color: #222c66')
            }

            var pedidosEntregaPesobruto = google.visualization.arrayToDataTable(jsonPedidosEntregaPesobruto)

            var view = new google.visualization.DataView(pedidosEntregaPesobruto);
            view.setColumns(
                [
                    0,
                    1,
                    2,
                    { calc: "stringify", sourceColumn: 1, type: "string", role: "annotation" },
                ]
            )

            var options = {
                    title: 'Pedidos/Entrega (Peso Bruto)',
                    legend: 'none',
                    titleTextStyle: {fontSize: 18},
                    bar: {groupWidth: '90%'},
                }

            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-pedentrega-pesobruto'))
            barchart.draw(view, options)

            /* GRÁFICO REFERENTE AO PESO BRUTO DE PEDIDOS/RETIRA POR INTERVALO */
            var data = jsonData.jsonpedidosretirapesobruto
            jsonData.jsonpedidosretirapesobruto[0].push({role: 'style'})
            jsonPedidosRetiraPesobruto = jsonData.jsonpedidosretirapesobruto

            for (let i = 1; i < jsonPedidosRetiraPesobruto.length; i++) {
                jsonPedidosRetiraPesobruto[i].push('color: #222c66')
            }

            var pedidosRetiraPesobruto = google.visualization.arrayToDataTable(jsonPedidosRetiraPesobruto)

            var view = new google.visualization.DataView(pedidosRetiraPesobruto);
            view.setColumns(
                [
                    0,
                    1,
                    2,
                    { calc: "stringify", sourceColumn: 1, type: "string", role: "annotation" },
                ]
            )
            var options = {
                    title: 'Pedidos/Retira (Peso Bruto)',
                    legend: 'none',
                    titleTextStyle: {fontSize: 18},
                    bar: {groupWidth: '90%'},
                }

            var barchart = new google.visualization.ColumnChart(document.getElementById('barchart-pedretira-pesobruto'))
            barchart.draw(view, options)

            /* GRÁFICO REFERENTE AO PESO BRUTO DE PEDIDOS/ENTREGA POR INTERVALO */
            var data = jsonData.jsonpedidosentregapesobruto

            var pedidosEntregaPesobruto = google.visualization.arrayToDataTable(data)

            var options = {
                    title: 'Pedidos/Entrega (Peso Bruto)',
                    titleTextStyle: {fontSize: 18}
                }

            var piechart = new google.visualization.PieChart(document.getElementById('piechart-pedentrega-pesobruto'))
            piechart.draw(pedidosEntregaPesobruto, options)

            /* GRÁFICO REFERENTE AO PESO BRUTO DE PEDIDOS/RETIRA POR INTERVALO */
            var data = jsonData.jsonpedidosretirapesobruto

            var pedidosRetiraPesobruto = google.visualization.arrayToDataTable(data)

            var options = {
                    title: 'Pedidos/Retira (Peso Bruto)',
                    titleTextStyle: {fontSize: 18}
                }

            var piechart = new google.visualization.PieChart(document.getElementById('piechart-pedretira-pesobruto'))
            piechart.draw(pedidosRetiraPesobruto, options)
        }
    })
}
