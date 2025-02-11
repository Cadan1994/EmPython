// 1 - lógica com o total de dispositivos cadastrados.

fetch('/total-equipamentos/')
            .then(response => response.json())
            .then(data => {
                // Obtendo os dados do JSON
                const totalEquipamentos = data.total_equipamentos;

                // Configurando os dados para o gráfico
                const dataForChart = {
                    labels: ['Total de Equipamentos'],
                    datasets: [{
                        label: 'Equipamentos',
                        backgroundColor: 'rgba(54, 162, 235, 0.2)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1,
                        data: [totalEquipamentos]
                    }]
                };

                // Configurando as opções do gráfico
                const options = {
                    scales: {
                        yAxes: [{
                            ticks: {
                                beginAtZero: true
                            }
                        }]
                    }
                };

                // Obtendo o contexto do canvas
                const ctx = document.getElementById('equipamentosChart').getContext('2d');

                // Criando o gráfico de barras
                const equipamentosChart = new Chart(ctx, {
                    type: 'bar',
                    data: dataForChart,
                    options: options
                });
            })
            .catch(error => console.error('Erro ao buscar total de equipamentos:', error));