from django.contrib.auth.forms import AuthenticationForm


class PanelAuthenticationForm(AuthenticationForm):
    def __init__(self, request=None, *args, **kwargs):
        super().__init__(request, *args, **kwargs)
        self.fields['username'].widget.attrs.update(
            {
                'autocomplete': 'username',
                'autocapitalize': 'none',
            }
        )
        self.fields['password'].widget.attrs.update(
            {'autocomplete': 'current-password'}
        )
