from appcd.models import Devolucao
from django import forms

'''############### Form Equipamento  ##############################'''
class DevolucaoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control', 'placeholder': 'Digite aqui'})


    class Meta:
        model = Devolucao
        fields = (
            'devolucao_id','ultimo_equipamento',
            'ultimo_responsavel', 'descricao', 'data_devolucao'
        )

    def clean(self):
        return super().clean()