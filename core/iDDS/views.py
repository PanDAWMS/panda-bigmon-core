import json
import logging
from django.shortcuts import render
from django.utils.cache import patch_response_headers
from django.http import JsonResponse
from django.template.defaulttags import register
from django.db.models import Q
from core.oauth.utils import login_customrequired
from core.views import initRequest, setupView
from core.utils import is_json_request
from core.iDDS.models import Transforms, Collections, Processings, Contents
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getRequests
from core.iDDS.algorithms import generate_requests_summary, parse_request, getiDDSInfoForTask
from core.iDDS.workflowprogress import get_workflow_progress_data, prepare_requests_summary
from core.libs.exlib import lower_dicts_in_list
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import getCacheEntry, setCacheEntry


_logger = logging.getLogger('bigpandamon')

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def main(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    data = getCacheEntry(request, "iDDSrequests")
    if data is not None:
        data = json.loads(data)
        response = render(request, 'landing.html', data, content_type='text/html')
        return response

    query_time = setupView(request, hours=24*7, querytype='idds')
    condition = "r.created_at > to_date('{1}', '{0}') AND r.created_at <= to_date('{2}', '{0}')".format(
        'YYYY-MM-DD',
        query_time['modificationtime__castdate__range'][0][:10],
        query_time['modificationtime__castdate__range'][1][:10]
    )

    query_params = parse_request(request)
    try:
        iDDSrequests = getRequests(query_params, condition=condition)
    except Exception as e:
        iDDSrequests = []
        _logger.exception('Failed to load iDDS requests from DB: \n{}'.format(e))

    iDDSrequests = lower_dicts_in_list(iDDSrequests)
    subtitleValue.replace('requests', iDDSrequests)
    requests_summary = generate_requests_summary(iDDSrequests)

    data = {
        'requests_summary': requests_summary,
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
        'iDDSrequests': json.dumps(iDDSrequests, cls=DateEncoder),
    }
    setCacheEntry(request, "iDDSrequests", json.dumps(data, cls=DateEncoder), 60 * 10)
    response = render(request, 'landing.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)

    return response


def collections(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    resp = {}
    query = Q()
    if 'transform_id' in request.session['requestParams']:
        query = Q(transform_id=request.session['requestParams']['transform_id'])
    if 'relation_type' in request.session['requestParams']:
        query = Q(relation_type=request.session['requestParams']['relation_type']) & query

    iDDScollections = list(Collections.objects.filter(query).values(
        'coll_id',
        'status',
        'total_files',
        'new_files',
        'processed_files',
        'relation_type'
        ))
    subtitleValue.replace('collections', iDDScollections)
    return JsonResponse({'data': iDDScollections}, encoder=DateEncoder, safe=False)


def iddscontents(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    query = Q()
    if 'coll_id' in request.session['requestParams']:
        query = Q(coll_id=request.session['requestParams']['coll_id'])
    iDDSсontents = list(Contents.objects.filter(query).values(
        'content_id',
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
    valid, response = initRequest(request)
    if not valid:
        return response
    query = Q()
    if 'transform_id' in request.session['requestParams']:
        query = Q(transform_id=request.session['requestParams']['transform_id'])
    iDDSprocessings = list(Processings.objects.filter(query).values(
        'processing_id',
        'transform_id',
        'status',
        'created_at',
        'updated_at',
        'finished_at'
        ))
    subtitleValue.replace('processings', iDDSprocessings)
    return JsonResponse({'data': iDDSprocessings}, encoder=DateEncoder, safe=False)


def transforms(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    iDDStransforms = []
    if 'requestid' in request.session['requestParams']:
        query = {'request_id': request.session['requestParams']['requestid']}
        iDDStransforms = list(Transforms.objects.filter(**query).values(
            'transform_id',
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
            ))
    return JsonResponse({'data': iDDStransforms}, encoder=DateEncoder, safe=False)


def getiDDSInfoForTaskRequest(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    transformationWithNested = None
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = request.session['requestParams']['jeditaskid']
        transformationWithNested = getiDDSInfoForTask(jeditaskid)
    return JsonResponse({'data': transformationWithNested}, encoder=DateEncoder, safe=False)


@login_customrequired
def wfprogress(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    kwargs = {}
    try:
        iDDSrequests = get_workflow_progress_data(request.session['requestParams'], **kwargs)
    except Exception as e:
        iDDSrequests = []
        _logger.exception('Failed to load iDDS requests from DB: \n{}'.format(e))

    iDDSsummary = prepare_requests_summary(iDDSrequests)
    if is_json_request(request):
        return JsonResponse(iDDSrequests, encoder=DateEncoder, safe=False)

    data = {
        'iDDSrequests': iDDSrequests,
        'iDDSsummary': iDDSsummary,
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }
    response = render(request, 'workflows.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
