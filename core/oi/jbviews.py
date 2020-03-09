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
from django.http import HttpResponseRedirect
import matplotlib


from django.template.defaulttags import register

CACHE_TIMEOUT = 5
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


@login_customrequired
def jbhome(request):

    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "jobProblem")
    data = None
    if data is not None and len(data) > 10:
        data = json.loads(data)
        if not ('message' in data and 'warning' in data['message'] and len(data['message']['warning']) > 1):
            data['request'] = request
            response = render_to_response('jobsbuster.html', data, content_type='text/html')
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response

    message = {}

    # process params
    metric = None
    if 'metric' in request.session['requestParams'] and request.session['requestParams']['metric']:
        metric = request.session['requestParams']['metric']

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=hours)
    elif 'timewindow' in request.session['requestParams'] and request.session['requestParams']['timewindow']:
        endtime = parse_datetime(request.session['requestParams']['timewindow'].split('|')[1])
        starttime = parse_datetime(request.session['requestParams']['timewindow'].split('|')[0])
    else:
        default_hours = 12
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=default_hours)

    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = 'prod'

    # getting data from jobbuster API
    base_url = 'http://aipanda030.cern.ch:8010/jobsbuster/api/?'
    url = base_url + 'timewindow={}|{}'.format(
        round_time(starttime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT),
        round_time(endtime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT))
    if metric:
        url += '&metric=' + metric

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
    names = []
    if resp_data:

        for i, problem in enumerate(resp_data['mesuresW']):
            resp_data['mesuresW'][i] = list(map(lambda x: x/3.154e+7 if not type(x) is str else x, problem))

        names = [i[0] for i in resp_data['mesuresW']]

    timeticks = []
    if resp_data and 'ticks' in resp_data:
        timeticks = [parse_datetime(tick) for tick in resp_data['ticks']]
        timeticks = ['x'] + [tick.strftime("%Y-%m-%d %H:%M:%S") for tick in timeticks]
        resp_data['mesuresW'].insert(0,timeticks)
        resp_data['mesuresNF'].insert(0,timeticks)

        colors = {}
        for name, color in zip(names, resp_data['colorsW']):
            colors[name] = matplotlib.colors.to_hex(color)

        for issue in resp_data['issues']:
            card = {}
            card['color'] = colors[issue['name']]
            card['impactloss'] = str(round(issue['sumWLoss'] / 3.154e+7, 2))
            card['impactfails'] = issue['sumJFails']
            card['name'] = issue['name']
            card['params'] = {}
            urlstr = "https://bigpanda.cern.ch/jobs/?endtimerange=" + str(issue['observation_started']).replace(" ", "T") + "|" + str(issue['observation_finished']).replace(" ", "T")

            for key,value in issue['features'].items():
                card['params'][key] = value

                # if isinstance(value, tuple):
                #     propname = value[i]
                # else:
                propname = value
                urlstr += "&" + str(key).lower() + "=" + str(propname)

            urlstr += "&mode=nodrop"
            card['url'] = urlstr
            spots.append(card)

        measures = resp_data['mesuresW'] if not metric or metric=='loss' else resp_data['mesuresNF']
        resp_dict = {
            'mesures': measures,
            'ticks': resp_data['ticks'],
            'issnames': names,
            'doGroup': False if len(names) < 2 else True,
            'colors': colors,
            'spots':spots,
        }

    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'message': message,
        'mesures': [],
        'metric': metric,
        #'plots': plots,
        #'spots': spots,
    }
    if resp_dict:
        data.update(resp_dict)

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json',))) and (
                'json' not in request.session['requestParams'])):
        response = render_to_response('jobsbuster.html', data, content_type='text/html')
    else:
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    if resp and len(resp.data) > 10:
        setCacheEntry(request, "jobProblem", json.dumps(data, cls=DateEncoder), 60 * CACHE_TIMEOUT)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
