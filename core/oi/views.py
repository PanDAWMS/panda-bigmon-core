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

from core.oi.utils import round_time
from django import template

from django.template.defaulttags import register

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def job_problems(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "jobProblem")
    #data = None
    if data is not None and len(data) > 10:
        data = json.loads(data)
        if not ('message' in data and 'warning' in data['message'] and len(data['message']['warning']) > 1):
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

    # getting data from jobbuster API
    base_url = 'http://aipanda030.cern.ch:8010/process/?'
    url = base_url + 'endtimerange={}|{}'.format(
        round_time(starttime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT),
        round_time(endtime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT))
    # url += '&jobtype=' + jobtype

    http = urllib3.PoolManager()
    try:
        resp = http.request('GET', url, timeout=500)
    except:
        resp = None
        message['warning'] = "Can not connect to jobbuster API, please try later"

    resp_data = None
    if resp and len(resp.data) > 10:
        try:
            resp_data = json.loads(resp.data)
        except:
            message['warning'] = "No data was received"
    else:
        message['warning'] = "No data was received"
    http.clear()

    # processing data
    plots = {}
    spots = []

    if resp_data:
        plots['hist'] = resp_data['hist'] if 'hist' in resp_data else {}
        spots_raw = resp_data['spots'] if 'spots' in resp_data else {}
        spots = [v for k, v in sorted(spots_raw.items(), reverse=True)]

    # transform period str to list of time range to use for highlighting bars in histogram
    for spot in spots:
        try:
            time_range = [spot['period'].split(' to')[0], spot['period'].split(' to')[1]]
            spot['time_range'] = time_range
        except:
            pass

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
    if resp and len(resp.data) > 10:
        setCacheEntry(request, "jobProblem", json.dumps(data, cls=DateEncoder), 60 * CACHE_TIMEOUT)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
