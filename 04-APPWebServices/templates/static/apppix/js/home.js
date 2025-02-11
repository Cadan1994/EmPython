const interval = setInterval(() => {
        /* Função de seta "Arrow Function" para formatar 1 em 01 */
        const zeroEsquerda = n => {
            return ('0' + n).slice(-2);
        }

        /* Pega o horário atual */
        const now = new Date();

        /* Formata a data conforme dd/mm/aaaa hh:ii:ss */
        const data = zeroEsquerda(now.getUTCDate()) + '/' +
                     zeroEsquerda((now.getMonth() + 1)) + '/' +
                     now.getFullYear();
        const hora = zeroEsquerda(now.getHours()) + ':' +
                     zeroEsquerda(now.getMinutes()) + ':' +
                     zeroEsquerda(now.getSeconds());

        /* Exibe na tela usando a div#data-hora */
        document.getElementById('datetime').innerHTML = '<strong>Data/Hora:&nbsp;</strong>' + data +' '+hora;
    },
    1000
);