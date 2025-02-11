from app_devicehub.models import Devolucao
from django import forms

'''############### Form Equipamento  ##############################'''
class DevolucaoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        '''self.fields[].widget.attrs.update({

            'placeholder':'Digite Aqui',
        })'''


    class Meta:
        model = Devolucao
        fields = (
            'devolucao_id','ultimo_equipamento',
            'ultimo_responsavel', 'descricao', 'data_devolucao'
        )

    def clean(self):
        return super().clean()