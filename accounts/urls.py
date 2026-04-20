from django.urls import path

from accounts import views

app_name = 'accounts'

urlpatterns = [
    path('entrar/', views.PanelLoginView.as_view(), name='login'),
    path('sair/', views.PanelLogoutView.as_view(), name='logout'),
]
