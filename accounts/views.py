from django.contrib.auth.views import LoginView, LogoutView
from django.urls import reverse_lazy

from accounts.forms import PanelAuthenticationForm


class PanelLoginView(LoginView):
    """Login do painel (fora do /admin/)."""

    template_name = 'accounts/login.html'
    authentication_form = PanelAuthenticationForm
    redirect_authenticated_user = True


class PanelLogoutView(LogoutView):
    """Encerra sessão (apenas POST, com CSRF)."""

    next_page = reverse_lazy('trader:panel_hub')
