from apscheduler.schedulers.background import BackgroundScheduler
from appbi.views import gerar_arquivo_dados


def job1():
    gerar_arquivo_dados()

"""
def job2():
    gerar_clientes_sefaz()
"""


def start():
    print("Scheduler started...")
    # Crie uma instância do scheduler
    scheduler = BackgroundScheduler()
    try:
        # Adicione a função que você deseja chamar e configure o trigger (gatilho)
        scheduler.add_job(func=job1, trigger='interval', minutes=1)
        """scheduler.add_job(func=job2, trigger='interval', minutes=10)"""

        # Inicie o scheduler
        scheduler.start()
    except KeyboardInterrupt:
        # Se o usuário pressionar Ctrl+C, pare o scheduler
        scheduler.shutdown()