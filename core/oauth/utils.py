"""
    Functions connected to management of authorized users
"""

import logging

from django.http import HttpResponseRedirect

from core.utils import is_json_request
from core.oauth.models import BPUser
from django.conf import settings as django_settings

_logger = logging.getLogger('bigpandamon-error')


def login_customrequired(function):
    def wrap(request, *args, **kwargs):

        # we check here if it is a crawler:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for is None:
            x_forwarded_for = request.META.get('REMOTE_ADDR')  # in case of one server config
        if x_forwarded_for and x_forwarded_for in django_settings.CACHING_CRAWLER_HOSTS:
            return function(request, *args, **kwargs)

        if request.user.is_authenticated or is_json_request(request):
            return function(request, *args, **kwargs)
        else:
            # if '/user/' in request.path:
            #     return HttpResponseRedirect('/login/?next=' + request.get_full_path())
            # else:
            # return function(request, *args, **kwargs)
            return HttpResponseRedirect('/login/?next='+request.get_full_path())
    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


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

def get_auth_provider(request):
    user = request.user

    if user.is_authenticated and user.social_auth is not None:
        try:
            auth_provider = (request.user.social_auth.get()).provider
        except Exception as ex:
            _logger.exception('{0}. User: {1}'.
                               format(ex, user))
            auth_provider = None
    else:
        auth_provider = None
    return auth_provider