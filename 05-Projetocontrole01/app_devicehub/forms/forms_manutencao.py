from app_devicehub.models import Manutencao
from django import forms

'''############### Form Equipamento  ##############################'''
class ManutencaoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        '''self.fields[].widget.attrs.update({

            'placeholder':'Digite Aqui',
        })'''


    class Meta:
        model = Manutencao
        fields = (
            'manutencao_id',
            'equipamento_manutencao',
            'responsavel_manutencao',
            'descricao',
            'data_manutencao'
        )
    def clean(self):
        return super().clean()