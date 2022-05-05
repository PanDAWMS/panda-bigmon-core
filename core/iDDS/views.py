import json

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.http import JsonResponse
from django.template.defaulttags import register
from django.db.models import Q
from core.oauth.utils import login_customrequired
from core.views import initRequest
from core.iDDS.models import Transforms, Collections, Processings, Contents
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getRequests, getTransforms
from core.iDDS.algorithms import generate_requests_summary, parse_request, getiDDSInfoForTask
from core.libs.exlib import lower_dicts_in_list
from core.libs.DateEncoder import DateEncoder
from django.core.cache import cache


CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def main(request):
    initRequest(request)
    query_params = parse_request(request)

    iDDSrequests = cache.get('iDDSrequests')
    iDDSrequests = None
    if not iDDSrequests:
        iDDSrequests = getRequests(query_params)
        cache.set("iDDSrequests", iDDSrequests, 10 * 60)
    iDDSrequests = lower_dicts_in_list(iDDSrequests)
    subtitleValue.replace('requests', iDDSrequests)
    requests_summary = generate_requests_summary(iDDSrequests)

    data = {
        'requests_summary':requests_summary,
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

    iDDScollections = list(Collections.objects.filter(query)
                          .values('coll_id',
                                  'status',
                                  'total_files',
                                  'new_files',
                                  'processed_files',
                                  'relation_type'
                                  ))
    subtitleValue.replace('collections', iDDScollections)
    return JsonResponse({'data': iDDScollections}, encoder=DateEncoder, safe=False)


def iddscontents(request):
    initRequest(request)
    query = Q()
    if 'coll_id' in request.session['requestParams']:
        query = Q(coll_id=request.session['requestParams']['coll_id'])
    iDDSсontents = list(Contents.objects.filter(query)
                          .values('content_id',
                                  'scope',
                                  'name',
                                  'min_id',
                                  'max_id',
                                  'status',
                                  'storage_id'
                                  ))
    subtitleValue.replace('сontents', iDDSсontents)
    return JsonResponse({'data': iDDSсontents}, encoder=DateEncoder, safe=False)


def processings(request):
    initRequest(request)
    query = Q()
    if 'transform_id' in request.session['requestParams']:
        query = Q(transform_id=request.session['requestParams']['transform_id'])
    iDDSprocessings = list(Processings.objects.filter(query)
                          .values('processing_id',
                                  'transform_id',
                                  'status',
                                  'created_at',
                                  'updated_at',
                                  'finished_at'
                                  ))
    subtitleValue.replace('processings', iDDSprocessings)
    return JsonResponse({'data': iDDSprocessings}, encoder=DateEncoder, safe=False)


def transforms(request):
    initRequest(request)
    query = Q()
    if 'requestid' in request.session['requestParams']:
        values = getTransforms(request.session['requestParams']['requestid'])
        queries = [Q(transform_id=value['TRANSFORM_ID']) for value in values]
        query = queries.pop()
        for item in queries:
            query |= item
    iDDStransforms = list(Transforms.objects.filter(query)
                          .values('transform_id',
                                  'transform_type',
                                  'transform_tag',
                                  'priority',
                                  'safe2get_output_from_input',
                                  'status',
                                  'substatus',
                                  'locking',
                                  'retries',
                                  'created_at',
                                  'updated_at',
                                  'started_at',
                                  'finished_at',
                                  'expired_at',
                                  'transform_metadata',
                                  ))
    return JsonResponse({'data': iDDStransforms}, encoder=DateEncoder, safe=False)


def getiDDSInfoForTaskRequest(request):
    initRequest(request)
    transformationWithNested = None
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = request.session['requestParams']['jeditaskid']
        transformationWithNested = getiDDSInfoForTask(jeditaskid)
    return JsonResponse({'data': transformationWithNested}, encoder=DateEncoder, safe=False)