/* Filtrar os clientes */
function filterRegisterCliente() {
    var input, filter, table, tr, td, i, txtValue
    input = document.getElementById('search-input-cliente')
    filter = input.value.toUpperCase()
    table = document.getElementById('table-cliente-lista')
    tr = table.getElementsByTagName('tr')

    for (i = 0; i < tr.length; i++) {
        td = tr[i].getElementsByTagName('td')[2]
        if (td) {
            txtValue = td.textContent || td.innerText
            if (txtValue.toUpperCase().indexOf(filter) > -1) {
                tr[i].style.display = ''
            } else {
                tr[i].style.display = 'none'
            }
        }
    }
}
/* Filtrar os produtos */
function filterRegisterProduto() {
    var input, filter, table, tr, td, i, txtValue
    input = document.getElementById('search-input-produto')
    filter = input.value.toUpperCase()
    table = document.getElementById('table-produto-lista')
    tr = table.getElementsByTagName('tr')

    for (i = 0; i < tr.length; i++) {
        td = tr[i].getElementsByTagName('td')[2]
        if (td) {
            txtValue = td.textContent || td.innerText
            if (txtValue.toUpperCase().indexOf(filter) > -1) {
                tr[i].style.display = ''
            } else {
                tr[i].style.display = 'none'
            }
        }
    }
}

/* Cálcula a altura da div 'div-menu' e 'div-imagem' */
function calcularAlturaDaDivContainer() {
    var headerHeight = document.querySelector('header').offsetHeight
    var footerHeight = 45
    var divHeight = window.innerHeight - headerHeight - footerHeight
    console.log(divHeight)
    document.querySelector('.div-menu').style.height = divHeight + 'px'
    document.querySelector('.div-imagem').style.height = divHeight + 'px'
}

/* Cálcula a altura da div 'div-tabela-clientes' e 'div-tabela-tbody' */
function calcularAlturaDaDivTableClientes() {
    var headerHeight = document.querySelector('header').offsetHeight
    var footerHeight = 115
    var divHeight = window.innerHeight - headerHeight - footerHeight
    document.getElementById('div-tabela-clientes').style.height = divHeight + 'px'
    var divTabelaHeight = document.getElementById('div-tabela-clientes').offsetHeight
    document.getElementById('div-tabela-tbody-clientes').style.height = (divTabelaHeight - 55) + 'px'
    document.getElementById('table-cliente-lista').style.height = (divTabelaHeight - 55) + 'px'
}

/* Cálcula a altura da div 'div-tabela-clientes' e 'div-tabela-tbody' */
function calcularAlturaDaDivTableProdutos() {
    var headerHeight = document.querySelector('header').offsetHeight
    var footerHeight = 115
    var divHeight = window.innerHeight - headerHeight - footerHeight
    document.getElementById('div-tabela-produtos').style.height = divHeight + 'px'
    var divTabelaHeight = document.getElementById('div-tabela-produtos').offsetHeight
    document.getElementById('div-tabela-tbody-produtos').style.height = (divTabelaHeight - 55) + 'px'
    document.getElementById('table-produto-lista').style.height = (divTabelaHeight - 55) + 'px'
}
