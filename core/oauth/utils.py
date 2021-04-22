"""
    Functions connected to management of authorized users
"""

import logging

from django.http import HttpResponseRedirect

from core.utils import is_json_request
from core.oauth.models import BPUser

_logger = logging.getLogger('bigpandamon-error')


def login_customrequired(function):
    def wrap(request, *args, **kwargs):

        # we check here if it is a crawler:
        notcachedRemoteAddress = ['188.184.185.129', '188.184.116.46']
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')

        if x_forwarded_for and x_forwarded_for in notcachedRemoteAddress:
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