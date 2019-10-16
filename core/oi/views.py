"""
Created by Tatiana Korchuganova on 10.10.2019
Views for Operational Intelligence project
"""

import json, urllib3
from datetime import datetime, timedelta

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers

from core.views import initRequest, login_customrequired, DateEncoder
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import parse_datetime

CACHE_TIMEOUT = 60
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

@login_customrequired
def job_problems(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "jobProblem")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('jobProblems.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    message = {}

    # process params
    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=hours)
    elif 'endtimerange' in request.session['requestParams'] and request.session['requestParams']['endtimerange']:
        endtime = parse_datetime(request.session['requestParams']['endtimerange'].split('|')[1])
        starttime = parse_datetime(request.session['requestParams']['endtimerange'].split('|')[0])
    else:
        default_hours = 12
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=default_hours)

    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = 'prod'

    # getting data from ...
    base_url = 'http://aipanda030.cern.ch:8001/process/?'
    url = base_url + 'endtimerange=' + starttime.strftime(OI_DATETIME_FORMAT) + '|' + endtime.strftime(OI_DATETIME_FORMAT)
    url += '&jobtype=' + jobtype

    http = urllib3.PoolManager()
    resp = http.request('GET', url)
    if resp and len(resp.data) > 0:
        try:
            resp_data = json.loads(resp.data)
        except:
            message['warning'] = "No data was received"
    else:
        message['warning'] = "No data was received"

    # processing data
    plots = {}
    plots['hist'] = resp_data['hist'] if 'hist' in resp_data else {}

    spots = []
    spots_raw = resp_data['spots'] if 'spots' in resp_data else {}
    spots = [v for k, v in sorted(spots_raw.items())]


    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'message': message,
        'plots': plots,
        'spots': spots,
    }

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json',))) and (
                'json' not in request.session['requestParams'])):
        response = render_to_response('jobProblems.html', data, content_type='text/html')
    else:
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    setCacheEntry(request, "jobProblem", json.dumps(data, cls=DateEncoder), 60 * CACHE_TIMEOUT)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
