"""
    Functions connected to management of authorized users
"""

import logging

from django.http import HttpResponseRedirect
from django.contrib.auth.models import Group
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


def update_user_groups(email, user_roles):
    """
    Update user groups
    :param email: str
    :param user_roles: list of str, user roles = egroup names
    :return: bool
    """
    # get users by email, there can be multiple users with the same email due to different auth providers
    try:
        users = BPUser.objects.filter(email=email)
    except:
        _logger.exception('Exception was caught while getting row from AUTH_USER by email')
        return False

    # get existing groups
    groups_existing = [g['name'] for g in Group.objects.filter(name__in=user_roles).values('name')]

    # update user groups
    if len(users) > 0:
        for user in users:
            for role in list(set(user_roles) & set(groups_existing)):
                if not user.groups.filter(name=role).exists():
                    user.groups.add(Group.objects.get(name=role))
                    user.save()
    else:
        _logger.exception(f'There is no user with this email {email}')
        return False

    return True


def get_username(user):
    """
    Getting true user name from social auth user table
    :param user: request.user object
    :return: username: str
    """
    if user.is_authenticated:
        social_user = user.social_auth.get()
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