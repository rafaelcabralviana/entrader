from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpRequest, JsonResponse

from api_auth.exceptions import (
    SmartTraderAuthError,
    SmartTraderConfigurationError,
)
from api_auth.services.auth import get_access_token


@staff_member_required
def auth_status(request: HttpRequest) -> JsonResponse:
    """
    Confirma que a autenticação com a API está funcional.
    Não expõe o access_token na resposta.
    """
    try:
        get_access_token()
    except SmartTraderConfigurationError as exc:
        return JsonResponse(
            {'status': 'misconfigured', 'detail': str(exc)},
            status=503,
        )
    except SmartTraderAuthError as exc:
        return JsonResponse(
            {'status': 'auth_failed', 'detail': str(exc)},
            status=502,
        )

    return JsonResponse({'status': 'ok'})
