from appcd.models import Manutencao
from django import forms

'''############### Form Equipamento  ##############################'''
class ManutencaoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control', 'placeholder': 'Digite aqui'})


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