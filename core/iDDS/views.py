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
from core.iDDS.models import Transforms, Collections, Requests, Req2transforms, Processings, Contents
from django.db.models import Q, F

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def main(request):
    #request.session['viewParams']['selection'] = '' + hashtag
    iDDSrequests = list(Requests.objects.using('idds_intr').values())
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
        'iDDSrequests':json.dumps(iDDSrequests, cls=DateEncoder),
    }
    response = render_to_response('landing.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def collections(request):
    initRequest(request)
    resp = {}
    query = Q()
    if 'transform_id' in request.session['requestParams']:
        query = Q(transform_id=request.session['requestParams']['transform_id'])
    if 'relation_type' in request.session['requestParams']:
        query = Q(relation_type=request.session['requestParams']['relation_type']) & query

    iDDScollections = list(Collections.objects.using('idds_intr').filter(query)
                          .values('coll_id',
                                  'status',
                                  'total_files',
                                  'new_files',
                                  'processed_files',
                                  'relation_type'
                                  ))
    return JsonResponse({'data': iDDScollections}, encoder=DateEncoder, safe=False)


def iddsсontents(request):
    initRequest(request)
    query = Q()
    if 'coll_id' in request.session['requestParams']:
        query = Q(coll_id=request.session['requestParams']['coll_id'])
    iDDSсontents = list(Contents.objects.using('idds_intr').filter(query)
                          .values('content_id',
                                  'scope',
                                  'name',
                                  'min_id',
                                  'max_id',
                                  'status',
                                  'storage_id'
                                  ))
    return JsonResponse({'data': iDDSсontents}, encoder=DateEncoder, safe=False)



def processings(request):
    initRequest(request)
    query = Q()
    if 'transform_id' in request.session['requestParams']:
        query = Q(transform_id=request.session['requestParams']['transform_id'])
    iDDSprocessings = list(Processings.objects.using('idds_intr').filter(query)
                          .values('processing_id',
                                  'transform_id',
                                  'status',
                                  'created_at',
                                  'updated_at',
                                  'finished_at'
                                  ))
    return JsonResponse({'data': iDDSprocessings}, encoder=DateEncoder, safe=False)



def transforms(request):
    initRequest(request)
    query = Q()
    if 'requestid' in request.session['requestParams']:
        query = Q(request_id_fk=request.session['requestParams']['requestid'])
    iDDStransforms = list(Req2transforms.objects.using('idds_intr').select_related('transform_id_fk').filter(query)
                          .values('transform_id_fk__transform_id',
                                  'transform_id_fk__transform_type',
                                  'transform_id_fk__transform_tag',
                                  'transform_id_fk__priority',
                                  'transform_id_fk__safe2get_output_from_input',
                                  'transform_id_fk__status',
                                  'transform_id_fk__substatus',
                                  'transform_id_fk__locking',
                                  'transform_id_fk__retries',
                                  'transform_id_fk__created_at',
                                  'transform_id_fk__updated_at',
                                  'transform_id_fk__started_at',
                                  'transform_id_fk__finished_at',
                                  'transform_id_fk__expired_at',
                                  'transform_id_fk__transform_metadata',
                                  ))

    return JsonResponse({'data': iDDStransforms}, encoder=DateEncoder, safe=False)
