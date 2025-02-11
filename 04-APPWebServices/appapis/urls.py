from django.urls import path
from .views import TokenService, AutomaticUserService, OrderService


urlpatterns = [
    path("authorization/token/", TokenService.access_token),
    path("usuario/", AutomaticUserService.query_user),
    path("SelecionarClientesPedidos/", OrderService.select_customers_orders),
    path("SelecionarPedidosPagar/", OrderService.select_orders_pays),
]