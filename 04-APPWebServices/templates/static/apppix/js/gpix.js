/*----------------------------------------------------------------------------------------------------------------------
Objective: CAPTURAR O INPUT E A TABELA
Create: HILSON SANTOS
Date: 21/11/2024
----------------------------------------------------------------------------------------------------------------------*/
const searchInput = document.getElementById('search-input-customer');
const table = document.getElementById('table-customer-list');

/* Adiciona um evento ao input para capturar as teclas digitadas */
/*
searchInput.addEventListener(
    'keyup',
    function() {
        const filter = searchInput.value.toLowerCase(); // Converte o valor para minúsculas
        const rows = table.getElementsByTagName('tr'); // Pega todas as linhas da tabela

        // Itera pelas linhas da tabela (ignorando o cabeçalho)
        for (let i = 1; i < rows.length; i++) {
            const cells = rows[i].getElementsByTagName('td');
            let match = false;

            // Itera pelas células da linha atual
            for (let j = 0; j < cells.length; j++) {
                if (cells[j]) {
                    const cellText = cells[j].textContent || cells[j].innerText;
                    if (cellText.toLowerCase().includes(filter)) {
                        match = true; // Marca como "encontrado"
                        break;
                    }
                }
            }

            // Mostra ou oculta a linha com base no resultado
            rows[i].style.display = match ? '' : 'none';
        }
    }
)
*/

/*----------------------------------------------------------------------------------------------------------------------
Objective: CAPTURAR O CÓDIGO DO CLIENTE NA TABELA
Create: HILSON SANTOS
Date: 21/11/2024
----------------------------------------------------------------------------------------------------------------------*/
const valueFormat = new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
});

/* FORMATAR VALORES COMO MOEDA NA TABELA LISTA DE CLIENTES */
document.querySelectorAll(".td-pedvalor").forEach(function (cell) {
    let value = parseFloat(cell.textContent);
    if (!isNaN(value)) {
        cell.textContent = valueFormat.format(value);
    }
});
/* FORMATAR VALORES COMO MOEDA NA TABELA LISTA DOS PEDIDOS PARA ENVIAR O PIX */
document.querySelectorAll(".td-valor").forEach(function (cell) {
    let value = parseFloat(cell.textContent);
    if (!isNaN(value)) {
        cell.textContent = valueFormat.format(value)
    }
});

document.querySelectorAll(".button-pay").forEach(button => {
    button.addEventListener("click", function() {
        /* Pega a linha onde está o botão */
        var row = this.closest("tr");

        var form = document.getElementById("formTable");
        /* Captura o valor do input nessa linha */
        var codeCustomer = row.querySelector(".input-codecustomer").value;
        var nameCustomer = row.querySelector(".input-namecustomer").value;

        /* Adiciona o valor ao formulário */
        const customerCode = document.createElement("input");
        customerCode.type = "hidden";
        customerCode.name = "selectedCustomerCode";
        customerCode.value = codeCustomer;

        /* Adiciona um campo para o ID, caso necessário */
        const customerName = document.createElement('input');
        customerName.type = 'hidden';
        customerName.name = 'selectedCustomerName';
        customerName.value = nameCustomer;


        form.appendChild(customerCode);
        form.appendChild(customerName);

        /* Envia o formulário */
        form.submit();
    });
});
/*----------------------------------------------------------------------------------------------------------------------
Objective: CAPTURAR OS VALORES DOS CHECKBOXES NA TABELA
Create: HILSON SANTOS
Date: 21/11/2024
----------------------------------------------------------------------------------------------------------------------*/
function getValues() {
    // Selecionar todos os checkboxes pelo atributo name
    var checkboxes = document.querySelectorAll('input[name="option"]:checked');
    // Extrair os valores dos checkboxes marcados
    var orderNumber = Array.from(checkboxes).map(checkbox => checkbox.value);
    var button = document.getElementById("gererateQRCodeButton");
    var inputHidden = document.createElement("input");
    var modal = document.getElementById("div-modal")

    // Adiciona o valor ao formulário
    inputHidden.type = "hidden";
    inputHidden.name = "selectedOrderNumber";
    inputHidden.value = orderNumber;
    button.appendChild(inputHidden);

    if (orderNumber != "" && orderNumber[0] != 0) {
        modal.style.display = "block"
    }

    // Envia o formulário
    button.submit();
}