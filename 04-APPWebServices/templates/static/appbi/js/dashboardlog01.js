function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(data => {
        const xValues = data.dtafatura_labels
        const yValues = data.dtafatura_values
        const plugin = {
          id: 'customCanvasBackgroundColor',
          beforeDraw: (chart, args, options) => {
            const {ctx} = chart
            ctx.save()
            ctx.globalCompositeOperation = 'destination-over'
            ctx.fillStyle = options.color || '#000000'
            ctx.fillRect(0, 0, chart.width, chart.height)
            ctx.restore()
          }
        }
        new Chart(
            "myPlot1",
            {
                type: "bar",
                data: {
                    labels: xValues,
                    datasets: [{
                        data: yValues,
                        backgroundColor: "#222C66"
                    }]
                },
                options: {
                    layout: {
                        autoPadding: true
                    },
                    scales: {
                        x: {ticks: {color: '#000000', weight: 'bold'}},
                        y: {ticks: {color: '#000000', weight: 'bold'}},
                    },
                    plugins: {
                        responsive: false,
                        legend: {display: false},
                        title: {
                            display: false,
                            position: "top",
                            text: "Pedidos por Data",
                            color: "#222c66",
                            font: {size: 22.4},
                            backgroundColor: 'red',
                        },
                        customCanvasBackgroundColor: {color: '#ffffff'},
                        tooltip: {
                            backgroundColor: '#bbbdd0',
                            titleColor: '#000000',
                            bodyColor: '#000000',
                            callbacks: {
                                label: function(context) {
                                    let label = context.dataset.label || " "

                                    if (context.parsed.y !== null) {
                                        label += "Quantidade: " + context.parsed.y
                                    }
                                    return label
                                }
                            }
                        }
                    }
                },
                plugins: [plugin],
            }
        )
    })
}