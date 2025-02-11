from django.contrib import admin
from .models import *


class TableDataWarehouseAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')

class SelectTableDataExtractionAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')


admin.site.register(TableDataWarehouse, TableDataWarehouseAdmin)
admin.site.register(SelectTablesDataExtraction, SelectTableDataExtractionAdmin)