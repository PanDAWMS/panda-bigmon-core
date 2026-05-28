"""
    Functions connected to management of authorized users
"""

import logging
from datetime import timedelta
from django.http import HttpResponseRedirect, JsonResponse
from django.utils import timezone
from core.utils import is_json_request
from core.oauth.models import BPUser
from django.conf import settings as django_settings
from rest_framework.exceptions import AuthenticationFailed

_logger = logging.getLogger('social')


def login_customrequired(function):
    def wrap(request, *args, **kwargs):

        # we check here if it is a crawler:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for is None:
            x_forwarded_for = request.META.get('REMOTE_ADDR')  # in case of one server config
        if x_forwarded_for and x_forwarded_for in django_settings.CACHING_CRAWLER_HOSTS:
            return function(request, *args, **kwargs)

        if request.user.is_authenticated:
            return function(request, *args, **kwargs)
        elif is_json_request(request):

            ### DEBUG, TODO remove later
            for key in sorted(request.META.keys()):
                _logger.info(f"[META_DEBUG] {key}: {request.META[key]}")
            ### END DEBUG

            from core.oauth.authentication import BPTokenAuthentication
            auth = BPTokenAuthentication()
            try:
                result = auth.authenticate(request)
            except AuthenticationFailed as e:
                auth_token = request.META.get('HTTP_AUTHORIZATION', '---no token---')
                _logger.error(f"[TOKEN_AUTH_FAILED] failed, token: {auth_token} with: {e}, req: {request} from {x_forwarded_for}")
                # return JsonResponse({'detail': f"{e}"}, status=401)
                return function(request, *args, **kwargs)

            if result is None:
                _logger.info(f"[TOKEN_AUTH_FAILED] no token in header found, req: {request} from {x_forwarded_for}")
                # return JsonResponse({'detail': 'Authentication credentials were not provided.'}, status=401)
                return function(request, *args, **kwargs)

            user, token = result
            request.user = user
            request.auth = token
            _logger.info(f"[TOKEN_AUTH] successfully authenticated user {user.username}, req: {request}")

            return function(request, *args, **kwargs)
        else:
            return HttpResponseRedirect('/login/?next='+request.get_full_path())
    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


def login_customrequired_strict(function):
    def wrap(request, *args, **kwargs):

        # we check here if it is a crawler:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for is None:
            x_forwarded_for = request.META.get('REMOTE_ADDR')  # in case of one server config
        if x_forwarded_for and x_forwarded_for in django_settings.CACHING_CRAWLER_HOSTS:
            return function(request, *args, **kwargs)

        if request.user.is_authenticated:
            return function(request, *args, **kwargs)
        elif is_json_request(request):
            from core.oauth.authentication import BPTokenAuthentication
            auth = BPTokenAuthentication()
            try:
                result = auth.authenticate(request)
            except AuthenticationFailed as e:
                auth_token = request.META.get('HTTP_AUTHORIZATION', '---no token---')
                _logger.error(f"[TOKEN_AUTH_FAILED] failed, token: {auth_token} with: {e}, req: {request} from {x_forwarded_for}")
                return JsonResponse({'detail': f"{e}"}, status=401)

            if result is None:
                _logger.info(f"[TOKEN_AUTH_FAILED] no token in header found, req: {request} from {x_forwarded_for}")
                return JsonResponse({'detail': 'Authentication credentials were not provided.'}, status=401)

            user, token = result
            request.user = user
            request.auth = token
            _logger.info(f"[TOKEN_AUTH] successfully authenticated user {user.username}, req: {request}")

            return function(request, *args, **kwargs)
        else:
            return HttpResponseRedirect('/login/?next='+request.get_full_path())
    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


def get_token_expiry_info(token, user=None):
    """
    Returns a dictionary containing expiration states and calculated dates for a token.
    """
    if not token:
        return {
            "is_expired": True,
            "created": None,
            "expires_at": None,
        }

    # Use provided user object or fallback to the token's attached user
    eval_user = user or token.user
    if eval_user.groups.filter(name="service-accounts").exists():
        lifetime_days = 365 * 10
    else:
        lifetime_days = django_settings.SESSION_COOKIE_AGE/(60*60*24)
    expires = token.created + timedelta(days=lifetime_days)
    is_expired = timezone.now() > expires

    return {
        "created": token.created,
        "expires": expires,
        "is_expired": is_expired,
    }


def grant_rights(request, rtype):

    try:
        bpuser = BPUser.objects.get(id=request.user.id)
    except:
        _logger.exception('Exception was caught while getting row from AUTH_USER by user_id')
        return False
    if bpuser:
        if rtype == 'tester':
            bpuser.is_tester = 1
    else:
        _logger.exception('There is no user with user_id equals {}'.format(request.session['USER_ID']))
        return False

    try:
        bpuser.save()
    except:
        _logger.exception('Exception was caught while granting user the tester rights')
        return False

    return True


def deny_rights(request, rtype):

    try:
        bpuser = BPUser.objects.get(id=request.user.id)
    except:
        _logger.exception('Exception was caught while getting row from AUTH_USER by user_id')
        return False
    if bpuser:
        if rtype == 'tester':
            bpuser.is_tester = 0
    else:
        _logger.exception('There is no user with user_id equals {}'.format(request.session['USER_ID']))
        return False

    try:
        bpuser.save()
    except:
        _logger.exception('Exception was caught while denying tester rights from user')
        return False

    return True


def is_expert(request):

    try:
        bpuser = BPUser.objects.get(id=request.user.id)
    except:
        _logger.exception('Exception was caught while getting row from AUTH_USER by user_id')
        return False
    if bpuser:
        if bpuser.is_expert == 1:
            return True
        else:
            return False
    else:
        _logger.exception('There is no user with user_id equals {}'.format(request.session['USER_ID']))
        return False


def get_full_name(email):
    """
    Getting full name of user by email
    :param email: str -
    :return: list of full names
    """
    full_names = []
    bp_users = BPUser.objects.filter(email=email).values('first_name', 'last_name')
    if len(bp_users) > 0:
        full_names.extend([f"{u['first_name']} {u['last_name']}" for u in bp_users])

    return list(set(full_names))


def get_username(user):
    """
    Getting true username from social auth user table
    :param user: request.user object
    :return: username: str
    """
    if user.is_authenticated:
        social_user = user.social_auth.filter(user_id=user.id).first()
        if social_user and social_user.extra_data and 'username' in social_user.extra_data:
            username = social_user.extra_data['username']
        else:
            username = user.username
    else:
        username = None
    return username


def user_email_sort(email):
    """"""
    domain = email.split('@')[-1]
    # sort order map (lower value = higher priority)
    priority_map = {
        "cern.ch": 1,
        "gmail.com": 8,
        "github.com": 9,
    }
    # decide priority, in case uni emails which are not in map -> middle
    priority = priority_map.get(domain, 5)

    return (priority, email)