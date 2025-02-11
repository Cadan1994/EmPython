from appcd.models import Equipamento
from django import forms

'''############### Form Equipamento  ##############################'''
class EquipamentoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control', 'placeholder': 'Digite aqui'})


    class Meta:
        model = Equipamento
        fields = (
            'tipo','modelo', 'marca', 'serial', 'imei1', 'imei2',
            'processador', 'memoria', 'descricao', 'status', 'valor',
            'total_equipamentos','data_entrega'
        )
    def clean(self):
        return super().clean()
