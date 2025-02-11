from django.apps import AppConfig


class AppapisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'appapis'

    def ready(self):
        from scheduler import scheduler
        scheduler.start()
