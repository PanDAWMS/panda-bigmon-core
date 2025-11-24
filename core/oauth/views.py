"""

"""
import json
from datetime import datetime

from django.shortcuts import render, redirect
from django.contrib.auth import logout as auth_logout
from django.views.decorators.cache import never_cache
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.conf import settings

import core.constants as const
from core.utils import extensibleURL, error_response
from core.views import initRequest
from core.libs.DateTimeEncoder import DateTimeEncoder
from core.oauth.utils import login_customrequired, grant_rights, deny_rights, user_email_sort
from core.oauth.models import BPUser, BPUserSettings, Visits


@never_cache
def login(request):
    """
    Login view
    """
    if 'next' in request.GET:
        next_url = str(request.GET['next'])
        if len(request.GET) > 1:
            next_url += '&' + '&'.join(['{}={}'.format(k, v) for k, v in request.GET.items() if k != 'next'])
    elif 'HTTP_REFERER' in request.META:
        next_url = extensibleURL(request, request.META['HTTP_REFERER'])
    else:
        next_url = '/'

    # redirect to the next if user already authenticated
    if request.user.is_authenticated:
        return redirect(next_url)

    # auth providers
    if hasattr(settings, 'AUTH_PROVIDER_LIST') and settings.AUTH_PROVIDER_LIST:
        auth_providers = settings.AUTH_PROVIDER_LIST
    else:
        auth_providers = None

    # store the redirect url in the session to be picked up after the auth completed
    request.session['next'] = next_url
    request.session['urls_cut'] = {}
    data = {
        'request': request,
        'viewParams': {},
        'requestParams': {},
        'auth_providers': auth_providers,
        'vo': settings.MON_VO if hasattr(settings, 'MON_VO') else '',
        'built': datetime.now().strftime("%H:%M:%S")
    }
    response = render(request, 'login.html', data, content_type='text/html')
    response.delete_cookie('sessionid')
    return response


def loginerror(request):
    warning = """The login to BigPanDA monitor has failed. Cleaning of your browser cookies might help. 
                 If the error persists, please write to """
    response = render(request, 'login.html', {'request': request, 'warning': warning}, content_type='text/html')
    return response


def logout(request):
    """Logs out user"""
    auth_logout(request)
    return redirect('/')


@login_customrequired
def grantRights(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'type' in request.session['requestParams']:
        rtype = request.session['requestParams']['type']
        grant_rights(request, rtype)

    return HttpResponse(status=204)


@login_customrequired
def denyRights(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'type' in request.session['requestParams']:
        rtype = request.session['requestParams']['type']
        deny_rights(request, rtype)

    return HttpResponse(status=204)


@never_cache
def statpixel(request):
    valid, response = initRequest(request, callselfmon=False)

    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    if 'HTTP_REFERER' in request.META:
        url = request.META['HTTP_REFERER']
        service = 0
        userid = -1
        if request.user.is_authenticated:
            userids = BPUser.objects.filter(email=request.user.email).values('id')
            userid = userids[0]['id']
        Visits.objects.create(url=url, service=service, remote=ip, time=str(timezone.now()), userid=userid)

    # this is a transparent gif pixel
    pixel_ = "\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b"
    response = HttpResponse(pixel_, content_type='image/gif')
    return response


def saveSettings(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    data = {}
    if 'page' in request.session['requestParams']:
        page = request.session['requestParams']['page']
        if page == 'errors':
            errorspage_tables = ['jobattrsummary', 'errorsummary', 'siteerrorsummary', 'usererrorsummary',
                                'taskerrorsummary']
            preferences = {}
            if 'jobattr' in request.session['requestParams']:
                preferences["jobattr"] = request.session['requestParams']['jobattr'].split(",")
                try:
                    del request.session['requestParams']['jobattr']
                    request.session.pop('jobattr')
                except:
                    pass
            else:
                preferences["jobattr"] = const.JOB_FIELDS_ERROR_VIEW
            if 'tables' in request.session['requestParams']:
                preferences['tables'] = request.session['requestParams']['tables'].split(",")
                try:
                    del request.session['requestParams']['tables']
                    request.session.pop('tables')
                except:
                    pass
            else:
                preferences['tables'] = errorspage_tables
            query = {}
            query['page'] = str(page)
            if request.user.is_authenticated:
                userids = BPUser.objects.filter(email=request.user.email).values('id')
                userid = userids[0]['id']
                try:
                    userSetting = BPUserSettings.objects.get(page=page, userid=userid)
                    userSetting.preferences = json.dumps(preferences)
                    userSetting.save(update_fields=['preferences'])
                except BPUserSettings.DoesNotExist:
                    userSetting = BPUserSettings(page=page, userid=userid, preferences=json.dumps(preferences))
                    userSetting.save()

        return HttpResponse(status=204)
    else:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')


@login_customrequired
def get_user_contact(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # allow only POST requests
    if request.META['REQUEST_METHOD'] != 'POST':
        return error_response(request, message='only POST requests are allowed', status=405)

    # allow only authenticated users with permission
    if request.user.is_authenticated and request.user.has_perm('oauth.can_contact_users'):
        if 'user' in request.session['requestParams']:
            user_name_split = request.session['requestParams']['user'].split(' ')
        else:
            return error_response(request, message='no user name supplied', status=400)

        emails = BPUser.objects.filter(
            first_name__istartswith=user_name_split[0],
            last_name__iendswith=user_name_split[-1]
        ).values('email')
        if len(emails) == 0:
            return error_response(request, message='no user found', status=404)
        elif len(emails) > 0:
            # dict -> list and filter
            emails = list(set([e['email'] for e in emails if e['email'] != '']))
            if len(emails) > 0:
                # custom sorting, cern and edu emails first and gmail and other last
                emails = sorted(emails, key=user_email_sort)
            else:
                return error_response(request, message='no user found', status=404)
        return JsonResponse({'email': emails[0]}, status=200)
    else:
        return error_response(request, message='No permission to ask this', status=403)