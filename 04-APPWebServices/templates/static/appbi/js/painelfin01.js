function openDashboard(url){
    fetch(url)
    .then(response => response.json())
    .then(jsonData => {

        google.charts.load('current', {'packages':['corechart'], 'language': 'pt-BR'})
        google.charts.setOnLoadCallback(drawChart)

        function drawChart() {
        /**/
        }
    })
}
