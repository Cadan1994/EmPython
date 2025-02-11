from django.urls import path
from . import views

app_name = 'appcd'

urlpatterns = [
   #urls Equipamentos
   path('dispositivos/', views.listar_equipamento, name='dispositivos'),
   path('dispositivos/<int:equipamento_id>/', views.ver_equipamento, name='ver_equipamento'),
   path('dispositivos/NovoCadastro/', views.novo_cadastro_equipamento, name='novo_cadastro_equipamento'),
   path('dispositivos/<int:equipamento_id>/atualizar_equipamento', views.atualizar_equipamento, name='atualizar_equipamento'),
   path('dispositivos/<int:equipamento_id>/excluir_equipamento', views.excluir_equipamento,name='excluir_equipamento'),

   #urls lógica
   path('dashboard/', views.gerenciamento, name='dashboard'),
   path('quantidade_dispositivos/', views.quantidade_dispositivos, name='quantidade_dispositivos'),
   path('info-devolucao/', views.info_devolucao, name='info_devolucao'),
   path('dados_departamentos/', views.dados_departamentos, name='dados_departamentos'),

   #urls para gera relatórios
   path('generate-pdf/', views.generate_pdf, name='generate_pdf'),
   path('termo_responsabilidade/<int:equipamento_id>/', views.Termo1.as_view(), name='termo_responsabilidade'),
   path('termo_devolucao_responsavel/<int:responsavel_id>/', views.Termo2.as_view(), name='termo_devolucao_responsavel'),


   #urls para as telas principais
   path('login/', views.login_admin, name='login'),
   path('home/', views.home, name='home'),
   path('search/', views.search, name='search'),
   path('sair/', views.sair, name='sair'),


   #urls para usuarizo
   path('usuario/register/', views.register, name='register'),
   path('usuario/atualizar/', views.usuario_atualizar, name='usuario_atualizar'),


   #urls devolução
   path('devolucao/', views.listar_devolucao, name='devolucao'),
   path('devolucao/<int:devolucao_id>/', views.ver_devolucao, name='ver_devolucao'),
   path('devolucao/NovoCadastro/', views.novo_cadastro_devolucao, name='novo_cadastro_devolucao'),
   path('devolucao/<int:devolucao_id>/atualizar_devolucao', views.atualizar_devolucao, name='atualizar_devolucao'),
   path('devolucao/<int:devolucao_id>/excluir_devolucao', views.excluir_devolucao,name='excluir_devolucao'),

   #urls para funcionários
   path('responsaveis/', views.listar_responsavel, name='responsaveis'),
   path('responsaveis/<int:responsavel_id>/', views.ver_responsaveis, name='ver_responsaveis'),
   path('responsavel/NovoCadastroresponsavel/', views.novo_cadastro_responsavel, name='novo_cadastro_responsavel'),
   path('responsavel/<int:responsavel_id>/atualizar_responsavel', views.atualizar_responsavel, name='atualizar_responsavel'),
   path('responsavel/<int:responsavel_id>/excluir_responsavel', views.excluir_responsavel,name='excluir_responsavel'),

   #urls em Manutenção
   path('manutencao/', views.listar_manutencao, name='manutencao'),
   path('manutencao/<int:manutencao_id>/', views.ver_manutencao, name='ver_manutencao'),
   path('manutencao/NovoCadastromanutencao/', views.novo_cadastro_manutencao, name='novo_cadastro_manutencao'),
   path('manutencao/<int:manutencao_id>/atualizar_manutencao', views.atualizar_manutencao, name='atualizar_manutencao'),
   path('manutencao/<int:manutencao_id>/excluir_manutencao', views.excluir_manutencao,name='excluir_manutencao'),


   #urls em Manutenção
   path('tipo/', views.listar_tipo, name='tipo'),
   path('tipo/<int:tipo_id>/', views.ver_tipo, name='ver_tipo'),
   path('tipo/NovoCadastrotipo/', views.novo_cadastro_tipo, name='novo_cadastro_tipo'),
   path('tipo/<int:tipo_id>/atualizar_tipo', views.atualizar_tipo, name='atualizar_tipo'),
   path('tipo/<int:tipo_id>/excluir_tipo', views.excluir_tipo,name='excluir_tipo'),
]