
/* FILTRA O REGISTRO PELA COLUNA NOME */
function filterRegister() {
    var input, filter, table, tr, td, i, txtValue
    input = document.getElementById("search-input");
    filter = input.value.toUpperCase();
    table = document.getElementById("listTable");
    tr = table.getElementsByTagName("tr");

    for (i = 0; i < tr.length; i++) {
        // Coluna para filtrar (no exemplo, a primeira coluna)
        td = tr[i].getElementsByTagName("td")[3];

        if (td) {
            txtValue = td.textContent || td.innerText;
            if (txtValue.toUpperCase().indexOf(filter) > -1) {
                tr[i].style.display = "";
            } else {
                tr[i].style.display = "none";
            }
        }
    }
}

/* ABRI O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function abrirModalIns(){
    // Obtém a referência para o elemento do formulário modal
    var formModal = document.getElementById('form-modal-register-ins')

    // Informa no título do formulário a variável "formTitle"
    document.getElementById('p-title-ins').innerHTML = 'Inclusão de Cadastro'

    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'flex'

    // Obtém a referência para o elemento input do formulário
    //var formInput = document.getElementById('name')
    //formInput.focus()

    // Obtém a referência para o elemento textarea do formulário
    var formTextarea = document.getElementById('description_ins')
    formTextarea.value = '';
    formTextarea.selectionStart = 0;

    // Obtém a referência para o elemento textarea do formulário
    var formTextarea = document.getElementById('selectinitial')
    formTextarea.value = '';
    formTextarea.selectionStart = 0;

    // Obtém a referência para o elemento textarea do formulário
    var formTextarea = document.getElementById('selectpartial')
    formTextarea.value = '';
    formTextarea.selectionStart = 0;
}

/* DISABILITA O CAMPO DO SELECT PARCIAL, CASO A OPÇÃO SEJA NÃO */
function selectPartial(){
    var option = document.getElementById('optionNoYes').value
    var selectPartial = document.getElementById('selectpartial')
    selectPartial.disabled = false
    if (option == 'N'){
        selectPartial.disabled = true
        selectPartial.value = ''
    }
}

/* FECHA O FORMULÁRIO MODAL DE INSERIR CADASTRO */
function fecharModalIns(){
    // Recebe ID do formulário
    var formModal = document.getElementById('form-modal-register-ins')
    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'none'
}

/* ABRI O FORMULÁRIO MODAL DE ALTERAR CADASTRO */
function abrirModalUpd(){

    // Obtém a referência para o elemento do formulário modal
    var formModal = document.getElementById('form-modal-register-upd')

    // Informa no título do formulário a variável "formTitle"
    document.getElementById('p-title-upd').innerHTML = 'Alteração de Cadastro'

    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'flex'

    // Obtém a referência para o elemento input do formulário
    //var formInput = document.getElementById('name_upd');
    //formInput.focus();


    var formId = document.getElementsByTagName('tr')
    var formName = document.getElementsByClassName('col-name')
    var formDescription = document.getElementsByClassName('col-description')
    var formSelectInitial = document.getElementsByClassName('col-selectinitial')
    var formSelectPartial = document.getElementsByClassName('col-selectpartial')
    var formSelectNoYes = document.getElementsByClassName('col-selectnoyes')
    var formType = document.getElementsByClassName('col-type')
    var formStatus =  document.getElementsByClassName('col-status')
    var formTable = document.getElementsByClassName('col-tableid')

    var formTableOpcao = document.getElementById('table')
    var formTypeOpcao = document.getElementById('type')
    var formStatusOpcao = document.getElementById('status')
    var formSelectNoYesOpcao = document.getElementById('optionNoYes_Upd')

    for (var i = 0; i < formId.length; i++) {
        formId[i].addEventListener(
            'click',
            function() {
                var rowId = this.id
                var rowTable = formTable[rowId].id
                var rowName = formName[rowId].id
                var rowDescription = formDescription[rowId].id
                var rowSelectInitial = formSelectInitial[rowId].id
                var rowSelectPartial = formSelectPartial[rowId].id
                var rowType = formType[rowId].id
                var rowStatus = formStatus[rowId].id
                var rowSelectNoYes = formSelectNoYes[rowId].id

                document.getElementById('id_upd').value = rowId.toString().padStart(3, '0')
                document.getElementById('name_upd').value = rowName
                document.getElementById('description_upd').innerHTML = rowDescription
                document.getElementById('selectinitial_upd').innerHTML = rowSelectInitial
                document.getElementById('selectpartial_upd').innerHTML = rowSelectPartial

                if (rowTable == 0) {
                    opcao = formTableOpcao.options[0]
                } else {
                    opcao = formTableOpcao.options[rowTable]
                }
                opcao.selected = true

                if (rowType == 'D') {
                    opcao = formTypeOpcao.options[1]
                } else {
                    opcao = formTypeOpcao.options[2]
                }
                opcao.selected = true

                if (rowStatus == 'A') {
                    opcao = formStatusOpcao.options[0]
                } else {
                    opcao = formStatusOpcao.options[1]
                }
                opcao.selected = true

                if (rowSelectNoYes == 'N') {
                    opcao = formSelectNoYesOpcao.options[0]
                } else {
                    opcao = formSelectNoYesOpcao.options[1]
                }
                opcao.selected = true
            }
        )
    }
}

/* FECHA O FORMULÁRIO MODAL DE ALTERAR CADASTRO */
function fecharModalUpd(){
    // Recebe ID do formulário
    var formModal = document.getElementById('form-modal-register-upd')
    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'none'
}

/* ABRI O FORMULÁRIO MODAL DE ALTERAR CADASTRO */
function abrirModalDel(){

    // Obtém a referência para o elemento do formulário modal
    var formModal = document.getElementById('form-modal-register-del')

    // Informa no título do formulário a variável "formTitle"
    document.getElementById('p-title-del').innerHTML = 'Exclusão de Cadastro'

    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'flex'

    // Obtém a referência para o elemento input do formulário
    var formInput = document.getElementById('name_del');
    formInput.focus();


    var formId = document.getElementsByTagName('tr')
    var formName = document.getElementsByClassName('col-name')
    var formDescription = document.getElementsByClassName('col-description')

    for (var i = 0; i < formId.length; i++) {
        formId[i].addEventListener(
            'click',
            function() {
                var rowId = this.id
                var rowName = formName[rowId].id
                var rowDescription = formDescription[rowId].id

                document.getElementById('id_del').value = rowId.toString().padStart(3, '0')
                document.getElementById('name_del').value = rowName
                document.getElementById('description_del').innerHTML = rowDescription
            }
        )
    }
    formId.removeEventListener('click')
}

/* FECHA O FORMULÁRIO MODAL DE EXCLUIR CADASTRO */
function fecharModalDel(){
    // Recebe ID do formulário
    let formModal = document.getElementById('form-modal-register-del')
    // Altera no CSS o parâmetro display do class "formModal"
    formModal.style.display = 'none'
}

/* MENSAGEM DE INCLUSÃO DE REGISTRO */
function messageInsert(){
    alert('Registro incluído com sucesso!')
}

/* MENSAGEM DE ALTERAÇÃO DE REGISTRO */
function messageUpdate(){
    alert('Registro alterado com sucesso!')
}

/* MENSAGEM DE EXCLUSÃO DE REGISTRO */
function messageDelete(){
    alert('Registro excluído com sucesso!')
}