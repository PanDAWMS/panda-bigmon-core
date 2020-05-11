import json

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.http import JsonResponse
from django.template.defaulttags import register
from django.db.models import Q, F

from core.views import initRequest, login_customrequired, DateEncoder
from core.iDDS.models import Transforms, Collections, Requests, Req2transforms, Processings, Contents
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getRequests
from core.iDDS.algorithms import generate_requests_summary, parse_request
from core.libs.exlib import lower_dicts_in_list
from django.core.cache import cache
from django.db import connection
from core.libs.exlib import dictfetchall

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


def iddsсontents(request):
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
        query = Q(request_id_fk=request.session['requestParams']['requestid'])
    iDDStransforms = list(Req2transforms.objects.select_related('transform_id_fk').filter(query)
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
    subtitleValue.replace('transforms', iDDStransforms)
    return JsonResponse({'data': iDDStransforms}, encoder=DateEncoder, safe=False)


def getiDDSInfoForTask(request):
    initRequest(request)
    transformationWithNested = None
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = request.session['requestParams']['jeditaskid']
        new_cur = connection.cursor()
        new_cur.execute(
            """ select r.request_id, r.scope, r.name, r.request_type, r.transform_tag, r.workload_id, r.status, r.created_at request_created_at, r.updated_at request_updated_at, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files, tr.out_created_at, tr.out_updated_at
                from atlas_idds.requests r
                 join atlas_idds.req2transforms rt on (r.request_id=rt.request_id and r.workload_id={0})
                 join (
                    select t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files, in_coll.processed_files in_processed_files,
                    out_coll.status out_status, out_coll.total_files out_total_files, out_coll.processed_files out_processed_files, out_coll.out_created_at, out_coll.out_updated_at
                    from atlas_idds.transforms t
                    join (select coll_id , transform_id, status, total_files, processed_files from atlas_idds.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
                    join (select coll_id , transform_id, status, total_files, processed_files, created_at out_created_at, updated_at out_updated_at from atlas_idds.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
                    ) tr on (rt.transform_id=tr.transform_id)
                """.format(int(jeditaskid)))
        transformationWithNested = dictfetchall(new_cur)
        if len(transformationWithNested) > 0:
            transformationWithNested = {k.lower(): v for k, v in transformationWithNested[0].items()}
            map = subtitleValue.substitleMap
            transformationWithNested['status'] = map['requests']['status'][transformationWithNested['status']]
            transformationWithNested['request_type'] = map['requests']['request_type'][transformationWithNested['request_type']]
            transformationWithNested['transform_status'] = map['requests']['transform_status'][transformationWithNested['transform_status']]
            transformationWithNested['in_status'] = map['requests']['in_status'][transformationWithNested['in_status']]
            transformationWithNested['out_status'] = map['requests']['out_status'][transformationWithNested['out_status']]
    return JsonResponse({'data': transformationWithNested}, encoder=DateEncoder, safe=False)


