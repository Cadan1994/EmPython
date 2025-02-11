/* ABRI O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function abrirModalCliente(){
    var formModal = document.getElementById('form-modal')
    var formTabela = document.getElementById("table-cliente-lista")
    var formTabelalinhas = formTabela.getElementsByTagName("tr")

    document.getElementById('p-title-ins').innerHTML = 'Parametrizações'
    formModal.style.display = 'flex'

    for (var i = 0; i < formTabelalinhas.length; i++) {
        var formTabelalinha = formTabelalinhas[i]
        formTabelalinha.onclick = function() {
            var formTabelaCelulas = this.getElementsByTagName("td")
            console.log(formTabelaCelulas)
            var clienteCodigo = formTabelaCelulas[0].innerText
            var clienteNomeRazao = formTabelaCelulas[2].innerText
            document.getElementById('form-cliente').value = clienteCodigo+' <> '+clienteNomeRazao
        }
    }
}

/* FECHA O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function fecharModal(){
    var formModal = document.getElementById('form-modal')
    formModal.style.display = 'none'
}
