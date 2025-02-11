from django.urls import path
from . import views

urlpatterns = [
    path('erp/', views.home, name='appierp-home'),
    path('cliente/', views.ClienteFocoFaixa.clientelista, name='cliente-list'),
    path('cliente/update', views.ClienteFocoFaixa.clienteupdate, name='cliente-update'),
    path('produto/', views.ProdutoBase.produtolista, name='produto-list'),
    path('produto/update', views.ProdutoBase.produtoupdate, name='produto-update'),
]