from django.urls import path

from api_auth import views

app_name = 'api_auth'

urlpatterns = [
    path('auth/status/', views.auth_status, name='auth_status'),
]
