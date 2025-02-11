from django.urls import path
from . import views


urlpatterns = [
    path("", views.Users.login),
    path("/login", views.Users.authentication, name="login-authentication"),
    path("/logoff", views.Users.logoff, name="login-logoff"),
    path("/home", views.Home.home, name="home"),
    #path('tabelas/', views.Tabelas.cadlist, name='tabelas-list'),
    #path('tabelas-ins/', views.Tabelas.cadinsert, name='tabela-insert'),
    #path('tabelas-upd/', views.Tabelas.cadupdate, name='tabela-update'),
    #path('tabelas-del/', views.Tabelas.caddelete, name='tabela-delete'),
    #path('selects/', views.Selects.cadlist, name='selects-list'),
    #path('selects-ins/', views.Selects.cadinsert, name='select-insert'),
    #path('selects-upd/', views.Selects.cadupdate, name='select-update'),
    #path('selects-del/', views.Selects.caddelete, name='select-delete'),
]