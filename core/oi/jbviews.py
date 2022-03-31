import json, urllib3
from datetime import datetime, timedelta

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from core.oauth.utils import login_customrequired
from core.views import initRequest
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.datetimestrings import parse_datetime

from core.oi.utils import round_time
import matplotlib

from core.views import removeParam

from django.template.defaulttags import register

CACHE_TIMEOUT = 5
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


@register.filter(takes_context=True)
def to_float(value):
    return float(value)


def formatError(json):
    outstr = ""
    for errorcat, errormessages in json.items():
        outstr += '<p>Error cathegory: '+errorcat+'<br />'
        for message, num in errormessages.items():
            outstr += 'Error message:<b>' + message + '</b>: ' +str(num)+ '<br />'
    return outstr


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
    computetype = None
    if 'metric' in request.session['requestParams'] and request.session['requestParams']['metric']:
        metric = request.session['requestParams']['metric']

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=hours)
    elif 'endtime_from' in request.session['requestParams'] and 'endtime_to' in request.session['requestParams']:
        endtime = parse_datetime(request.session['requestParams']['endtime_to'])
        starttime = parse_datetime(request.session['requestParams']['endtime_from'])
    else:
        default_hours = 12
        endtime = datetime.now()
        starttime = datetime.now() - timedelta(hours=default_hours)

    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = 'prod'

    if 'computetype' in request.session['requestParams'] and request.session['requestParams']['computetype']:
        computetype = request.session['requestParams']['computetype']

    # getting data from jobbuster API
    base_url = 'http://aipanda030.cern.ch:8010/jobsbuster/api/?'
    url = base_url + 'timewindow={}|{}'.format(
        round_time(starttime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT),
        round_time(endtime, timedelta(minutes=1)).strftime(OI_DATETIME_FORMAT))
    if metric:
        url += '&metric=' + metric
    if computetype:
        url += '&computetype=' + computetype


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

    resp_dict = None
    timeticks = []
    errormessages = {}
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
            card['impactloss'] = str(round(issue['walltime_loss'] / 3.154e+7, 2))
            card['impactfails'] = issue['nFailed_jobs']
            card['name'] = issue['name']
            card['params'] = {}
            id = str(len(errormessages.keys()))
            card['errormessagesid'] = id
            errormessages[id] = formatError(json.loads(issue['err_messages']))
            urlstr = "https://bigpanda.cern.ch/jobs/?endtimerange=" + str(issue['observation_started']).replace(" ", "T") + "|" + str(issue['observation_finished']).replace(" ", "T")

            for key,value in issue['features'].items():
                card['params'][key] = value

                # if isinstance(value, tuple):
                #     propname = value[i]
                # else:
                propname = value
                urlstr += "&" + str(key).lower() + "=" + str(propname)

            urlstr += "&mode=nodrop&prodsourcelabel=managed"
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

    url_no_computetype = removeParam(request.get_full_path(), 'computetype')

    request.session['timerange'] = [starttime.strftime(OI_DATETIME_FORMAT), endtime.strftime(OI_DATETIME_FORMAT)]
    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'timerange': request.session['timerange'],
        'message': message,
        'mesures': [],
        'metric': metric,
        'urlBase': url_no_computetype + ('&' if url_no_computetype.find('?') > -1 else '?'),
        'errormessages': json.dumps(errormessages)
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

