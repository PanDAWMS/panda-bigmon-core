import json, urllib3
from datetime import datetime, timedelta

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers

from core.views import initRequest, login_customrequired, DateEncoder
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import parse_datetime

from core.oi.utils import round_time
from django import template
from django.http import HttpResponseRedirect
from django.http import JsonResponse

from django.template.defaulttags import register
from core.iDDS.models import Transforms, Collections

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def main(request):
    #request.session['viewParams']['selection'] = '' + hashtag
    collections = Collections.objects.using('idds_intr').all()
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }
    response = render_to_response('landing.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def collections(request):
    initRequest(request)
    resp = {}
    return JsonResponse(data=resp, encoder=DateEncoder, safe=False)
