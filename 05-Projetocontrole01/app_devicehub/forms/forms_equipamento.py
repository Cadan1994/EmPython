from app_devicehub.models import Equipamento
from django import forms

'''############### Form Equipamento  ##############################'''
class EquipamentoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        '''self.fields[].widget.attrs.update({

            'placeholder':'Digite Aqui',
        })'''


    class Meta:
        model = Equipamento
        fields = (
            'tipo','modelo', 'marca', 'serial', 'imei1', 'imei2',
            'processador', 'memoria', 'descricao', 'status', 'valor',
            'total_equipamentos','data_entrega'
        )
    def clean(self):
        return super().clean()
