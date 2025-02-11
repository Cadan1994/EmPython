from app_devicehub.models import Tipo
from django import forms

'''############### Form Equipamento  ##############################'''
class TipoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        '''self.fields[].widget.attrs.update({

            'placeholder':'Digite Aqui',
        })'''


    class Meta:
        model = Tipo
        fields = (
            'tipo_id','nome_tipo',
            'descricao', 'data'
        )

    def clean(self):
        return super().clean()