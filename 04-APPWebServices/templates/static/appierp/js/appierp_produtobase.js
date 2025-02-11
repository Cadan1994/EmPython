/* ABRI O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function abrirModalProduto(){
    var formModal = document.getElementById('form-modal')
    var formTabela = document.getElementById("table-produto-lista")
    var formTabelalinhas = formTabela.getElementsByTagName("tr")

    document.getElementById('p-title-ins').innerHTML = 'Parametrização'
    formModal.style.display = 'flex'

    for (var i = 0; i < formTabelalinhas.length; i++) {
        var formTabelalinha = formTabelalinhas[i]
        formTabelalinha.onclick = function() {
            var formTabelaCelulas = this.getElementsByTagName("td")
            var produtoCodigo = formTabelaCelulas[0].innerText
            var produtoBase = formTabelaCelulas[1].innerText
            var produtoDescricao = formTabelaCelulas[2].innerText
            document.getElementById('form-produto').value = produtoCodigo+' <> '+produtoDescricao
            document.getElementById('checkbox').value = produtoBase

            if (produtoBase == 'S') {
                document.getElementById('checkbox').checked = true
            } else {
                document.getElementById('checkbox').checked = false
            }
        }
    }
}

/* FECHA O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function fecharModal(){
    var formModal = document.getElementById('form-modal')
    formModal.style.display = 'none'
}