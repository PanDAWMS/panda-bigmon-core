import logging
from django.http import HttpResponseRedirect, JsonResponse
from functools import wraps
from rest_framework.exceptions import AuthenticationFailed

from core.oauth.authentication import BPTokenAuthentication
from core.utils import is_json_request

_logger = logging.getLogger('social')

def login_customrequired(function):
    @wraps(function)
    def wrap(request, *args, **kwargs):

        if request.user.is_authenticated:
            return function(request, *args, **kwargs)

        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR') or request.META.get('REMOTE_ADDR')
        auth_header = request.META.get('HTTP_AUTHORIZATION', None)
        if auth_header:
            auth = BPTokenAuthentication()
            try:
                result = auth.authenticate(request)
                if result is not None:
                    user, token = result
                    request.user = user
                    request.auth = token
                    _logger.info(f"[TOKEN_AUTH] successfully authenticated user {user.username}, req: {request}")
                    return function(request, *args, **kwargs)
            except AuthenticationFailed as e:
                _logger.error(f"[TOKEN_AUTH] failed with: {e}, req: {request} from {x_forwarded_for}")
                return JsonResponse({'detail': f"{e}"}, status=401)

        elif is_json_request(request):
            _logger.info(f"[TOKEN_AUTH] no token in request header for JSON req: {request} from {x_forwarded_for}")
            return function(request, *args, **kwargs)
            # return JsonResponse({'error': f"No token provided"}, status=401)
        else:
            return HttpResponseRedirect('/login/?next='+request.get_full_path())
    return wrap


def login_required(function):
    @wraps(function)
    def wrap(request, *args, **kwargs):

        if request.user.is_authenticated:
            _logger.info(f"[AUTH] user {request.user.username} is already authenticated, req: {request}")
            return function(request, *args, **kwargs)

        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR') or request.META.get('REMOTE_ADDR')
        auth_header = request.META.get('HTTP_AUTHORIZATION', None)
        if auth_header:
            auth = BPTokenAuthentication()
            try:
                result = auth.authenticate(request)
                if result is not None:
                    user, token = result
                    request.user = user
                    request.auth = token
                    _logger.info(f"[TOKEN_AUTH] successfully authenticated user {user.username}, req: {request}")
                    return function(request, *args, **kwargs)
            except AuthenticationFailed as e:
                _logger.error(f"[TOKEN_AUTH] failed with: {e}, req: {request} from {x_forwarded_for}")
                return JsonResponse({'detail': f"{e}"}, status=401)
        elif is_json_request(request):
            _logger.info(f"[TOKEN_AUTH] no token in request header for JSON req: {request} from {x_forwarded_for}")
            return JsonResponse({'error': f"No token provided"}, status=401)
        else:
            return HttpResponseRedirect('/login/?next='+request.get_full_path())

    return wrap