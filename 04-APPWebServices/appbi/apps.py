from django.apps import AppConfig


class AppbiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'appbi'

    def ready(self):
        from scheduler import scheduler
        scheduler.start()


