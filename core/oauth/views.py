"""

"""
import json
from django.shortcuts import render_to_response, redirect
from django.contrib.auth import logout as auth_logout
from django.views.decorators.cache import never_cache
from django.utils.cache import patch_response_headers
from django.http import HttpResponse
from django.utils import timezone

import core.constants as const
from core.utils import extensibleURL
from core.views import initRequest, DateTimeEncoder
from core.oauth.utils import login_customrequired, grant_rights, deny_rights
from core.oauth.models import BPUser, BPUserSettings, Visits

@never_cache
def loginauth2(request):
    if 'next' in request.GET:
        next = str(request.GET['next'])
    elif 'HTTP_REFERER' in request.META:
        next = extensibleURL(request, request.META['HTTP_REFERER'])
    else:
        next = '/'
    response = render_to_response('login.html', {'request': request, 'next': next,}, content_type='text/html')
    response.delete_cookie('sessionid')
    return response


def loginerror(request):
    warning = """The login to BigPanDA monitor is failed. Cleaning of your browser cookies might help. 
                 If the error is persistent, please write to """
    response = render_to_response('login.html', {'request': request, 'warning': warning}, content_type='text/html')
    #patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@login_customrequired
def testauth(request):
    response = render_to_response('testauth.html', {'request': request,}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def logout(request):
    """Logs out user"""
    auth_logout(request)
    return redirect('/')


@login_customrequired
def grantRights(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'type' in request.session['requestParams']:
        rtype = request.session['requestParams']['type']
        grant_rights(request, rtype)

    return HttpResponse(status=204)


@login_customrequired
def denyRights(request):
    valid, response = initRequest(request)
    if not valid: return response

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