from app_devicehub.models import Responsavel
from django import forms

'''############### Form Equipamento  ##############################'''
class ResponsavelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        '''self.fields[].widget.attrs.update({

            'placeholder':'Digite Aqui',
        })'''


    class Meta:
        model = Responsavel
        fields = (
            'nome','cpf', 'rg','departamento', 'funcao',
            'telefone_celular', 'equipamento_responsavel', 'data_entrega'
        )
    def clean(self):
        return super().clean()