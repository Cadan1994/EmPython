//funcionalidade do menu
var menuItem = document.querySelectorAll('.item-menu')

function selectlink(){
    menuItem.forEach((item)=>
        item.classList.remove('ativo')
    )
    this.classList.add('ativo')
}

menuItem.forEach((item)=>
   item.addEventListener('click', selectlink)
)

//Expandir menu
var bntExp = document.querySelector('#btn-exp')
var menuSide = document.querySelector('.menu-lateral')

bntExp.addEventListener('click', function(){
    menuSide.classList.toggle('expandir')
})