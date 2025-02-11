from django.urls import path
from . import views

urlpatterns = [
    # REFERENTE AOS PAINEIS LOGÍSTICA
    path('bilog01/', views.painellog01_dashboard),
    path('bilog01analisepedidos/', views.analise_log_painel01, name='data-analysis-pedidos'),
    path('bilog02/', views.painellog02_dashboard),
    path('bilog02analisepedidos/', views.analise_log_painel02, name='data-analysis-pedidosdia'),
    path('bilog03/', views.painellog03_dashboard),
    path('bilog03analisepedidos/', views.analise_log_painel03, name='data-analysis-pedidospeso'),
    path('bilog04/', views.painellog04_dashboard),
    path('bilog04estoquesenderecos/', views.analise_log_painel04, name='data-analysis-estoqueendereco'),

    # REFERENTE AOS PAINEIS DIRETORIA
    path('bidir01/', views.paineldir01_dashboard),
    path('bidir01analisedados/', views.analise_dir_painel01, name='data-analysis-analise_dir_painel01'),

    # REFERENTE AOS DASHBOARD DE ANÁLISE
    path('biloganalise01/', views.bilog01_dashboard, name='biloganalise01'),
    path('bilog01_processo/', views.bilog01_processo, name='data-bilog01_processo'),
    path('bifinanalise01/', views.bifin01_dashboard, name='bifinanalise01'),
    path('bifin01_processo/', views.bifin01_processo, name='data-bifin01_processo'),
]