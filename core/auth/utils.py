"""
    Functions connected to management of authorized users
"""

import logging
from core.common.models import BPUser

_logger = logging.getLogger('bigpandamon-error')


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