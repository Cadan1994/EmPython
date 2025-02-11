import requests
from django.urls import path
from .views import *


urlpatterns = [
    path("", Login.login),
    path("login/", Login.authentication, name="login-authentication"),
    path("login-alteracao-senha/", Login.login_select, name="login-select"),
    path("login-update/", Login.login_update_password, name="login-update-password"),
    path("home/", Home.home, name="home"),
    path("home/logoff", Home.logoff, name="logoff"),
    path("brasil-pix/", OperationPix.pix_manager_brasil_custumers, name="pix-manager-brasil-custumers"),
    path("brasil-pix-orders/", OperationPix.pix_manager_brasil_orders, name="pix-manager-brasil-orders"),
    path("brasil-pix-orders-pay/", OperationPix.send_orders_immediate_collection, name="pix-manager-brasil-pays"),
]
