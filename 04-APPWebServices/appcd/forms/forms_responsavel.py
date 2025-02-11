from appcd.models import Responsavel
from django import forms

'''############### Form Equipamento  ##############################'''
class ResponsavelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control', 'placeholder': 'Digite aqui'})

    class Meta:
        model = Responsavel
        fields = (
            'nome','cpf', 'rg','departamento', 'funcao',
            'telefone_celular', 'equipamento_responsavel', 'data_entrega'
        )
    def clean(self):
        return super().clean()