from appcd.models import Tipo
from django import forms

'''############### Form Equipamento  ##############################'''
class TipoForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs.update({'class': 'form-control', 'placeholder': 'Digite aqui'})


    class Meta:
        model = Tipo
        fields = (
            'tipo_id','nome_tipo',
            'descricao', 'data'
        )

    def clean(self):
        return super().clean()