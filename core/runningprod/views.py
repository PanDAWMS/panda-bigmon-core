import random
import json
import copy
import time
import logging

from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.utils.cache import patch_response_headers

from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.exlib import round_to_n_digits, convert_grams
from core.libs.task import task_summary_dict
from core.libs.elasticsearch import get_gco2_sum_for_tasklist
from core.oauth.utils import login_customrequired
from core.libs.DateEncoder import DateEncoder
from core.views import initRequest, setupView
from core.utils import is_json_request, removeParam

from core.runningprod.utils import saveNeventsByProcessingType, prepareNeventsByProcessingType, clean_running_task_list, prepare_plots, updateView
from core.runningprod.models import RunningProdTasksModel, RunningProdRequestsModel, FrozenProdTasksModel, ProdNeventsHistory

from django.conf import settings
import core.constants as const

_logger = logging.getLogger('bigpandamon')

@login_customrequired
def runningProdTasks(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'dt' in request.session['requestParams'] and 'tk' in request.session['requestParams']:
        tk = request.session['requestParams']['tk']
        data = getCacheEntry(request, tk, isData=True)
        return HttpResponse(data, content_type='application/json')

    # Here we try to get cached data
    data = getCacheEntry(request, "runningProdTasks")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'runningProdTasks.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    productiontype = ''
    if 'preset' in request.session['requestParams']:
        if request.session['requestParams']['preset'] and request.session['requestParams']['preset'].upper() == 'MC':
            productiontype = 'MC'
            if 'workinggroup' not in request.session['requestParams']:
                request.session['requestParams']['workinggroup'] = '!AP_REPR,!AP_VALI,!GP_PHYS,!GP_THLT'
            if 'processingtype' not in request.session['requestParams']:
                request.session['requestParams']['processingtype'] = 'evgen|pile|simul|recon'
            if 'campaign' not in request.session['requestParams']:
                request.session['requestParams']['campaign'] = 'mc*'
        if request.session['requestParams']['preset'] and request.session['requestParams']['preset'].upper() == 'DPD':
            productiontype = 'DPD'
            request.session['requestParams']['scope'] = '!valid*'
            if 'workinggroup' not in request.session['requestParams']:
                request.session['requestParams']['workinggroup'] = 'GP_*'
            if 'processingtype' not in request.session['requestParams']:
                request.session['requestParams']['processingtype'] = 'merge|deriv'
        if request.session['requestParams']['preset'] and request.session['requestParams']['preset'].upper() == 'DATA':
            productiontype = 'DATA'
            if 'workinggroup' not in request.session['requestParams']:
                request.session['requestParams']['workinggroup'] = 'AP_REPR'
            if 'processingtype' not in request.session['requestParams']:
                request.session['requestParams']['processingtype'] = 'reprocessing'

    exquery = {}
    tquery_timelimited, extra_str, _ = setupView(
        request,
        hours=24*7, # default 7 days if no limit in request and applies only if show_ended_tasks activated
        limit=9999999,
        querytype='task',
        wildCardExt=True)
    tquery_timelimited, exquery, extra_str = updateView(request, tquery_timelimited, exquery, extra_str)

    load_ended_tasks = False
    if len(list(set(request.session['requestParams'].keys()) & set(const.TIME_LIMIT_OPTIONS))) > 0 or (
        'eventservice' in tquery_timelimited and tquery_timelimited['eventservice'] == 1):
        load_ended_tasks = True
        request.session['viewParams']['selection'] = ', ended tasks limited by creation time from {} to {}'.format(
            tquery_timelimited['creationdate__castdate__range'][0],
            tquery_timelimited['creationdate__castdate__range'][1]
        )
        if "((UPPER(status)  LIKE UPPER('all')))" in extra_str:
            extra_str = extra_str.replace("((UPPER(status)  LIKE UPPER('all')))", "(1=1)")
    else:
        request.session['viewParams']['selection'] = ''

        # remove time limit
    tquery = copy.deepcopy(tquery_timelimited)
    if 'creationdate__castdate__range' in tquery:
        del tquery['creationdate__castdate__range']

    tasks = []
    tasks.extend(
        RunningProdTasksModel.objects.filter(**tquery).extra(where=[extra_str]).exclude(**exquery).values())
    _logger.debug("Got running tasks, N={} : {}".format(len(tasks), time.time() - request.session['req_init_time']))
    if load_ended_tasks:
        tasks.extend(
            FrozenProdTasksModel.objects.filter(**tquery_timelimited).extra(where=[extra_str]).exclude(**exquery).values())
        _logger.debug("Got frozen tasks, N={} : {}".format(len(tasks), time.time() - request.session['req_init_time']))
    qtime = datetime.now()
    task_list = [t for t in tasks]
    ntasks = len(tasks)

    # clean task list
    task_list = clean_running_task_list(task_list)

    # produce required plots
    plots_dict = prepare_plots(task_list, productiontype=productiontype)

    # get param summaries for select drop down menus
    sumd = task_summary_dict(request, task_list, ['status', 'workinggroup', 'cutcampaign', 'processingtype'])

    # get global sum
    gsum = {
        'nevents': sum([t['nevents'] for t in task_list]),
        'aslots': sum([t['aslots'] for t in task_list]),
        'ntasks': len(task_list),
    }
    _logger.debug("Summary prepared : {}".format(time.time() - request.session['req_init_time']))

    # get gCO2 from ES
    gco2_sum = None
    try:
        gco2_sum = get_gco2_sum_for_tasklist(task_list=[t['jeditaskid'] for t in task_list])
        _logger.debug("Got CO2 summary: {}".format(time.time() - request.session['req_init_time']))
    except Exception as ex:
        _logger.exception('Internal Server Error: failed to get gCO2 values from ES with: {}'.format(str(ex)))
    if gco2_sum and 'total' in gco2_sum:
        gsum['gco2_sum'] = {}
        for k, v in gco2_sum.items():
            cv, unit = convert_grams(float(v), output_unit='auto')
            gsum['gco2_sum'][k] = {'unit': unit, 'value': round_to_n_digits(cv, n=0, method='floor')}
    else:
        gsum['gco2_sum'] = {}

    if is_json_request(request):
        if 'snap' in request.session['requestParams'] and len(request.session['requestParams']) == 2:
            snapdata = prepareNeventsByProcessingType(task_list)
            if saveNeventsByProcessingType(snapdata, qtime):
                data = {'message': 'success'}
            else:
                data = {'message': 'fail'}
            dump = json.dumps(data, cls=DateEncoder)
            return HttpResponse(dump, content_type='application/json')
        dump = json.dumps(task_list, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # putting list of tasks to cache separately for dataTables plugin
        transactionKey = random.randrange(100000000)
        setCacheEntry(request, transactionKey, json.dumps(task_list, cls=DateEncoder), 60 * 30, isData=True)

        xurl = request.get_full_path()
        if xurl.find('?') > 0:
            xurl += '&'
        else:
            xurl += '?'
        nohashtagurl = removeParam(xurl, 'hashtags', mode='extensible')

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'load_ended_tasks': load_ended_tasks,
            'xurl': xurl,
            'nohashtagurl': nohashtagurl,
            'built': datetime.now().strftime("%H:%M:%S"),
            'transKey': transactionKey,
            'qtime': qtime,
            'productiontype': json.dumps(productiontype),
            'sumd': sumd,
            'gsum': gsum,
            'plots': plots_dict,
        }
        response = render(request, 'runningProdTasks.html', data, content_type='text/html')
        _logger.debug("Template rendered: {}".format(time.time() - request.session['req_init_time']))
        setCacheEntry(request, "runningProdTasks", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def prodNeventsTrend(request):
    """
    The view presents historical trend of nevents in different states for various processing types
    Default time window - 1 week
    """
    valid, response=  initRequest(request)
    defaultdays = 7
    tquery = setupView(request, hours=defaultdays*24, querytype='task', wildCardExt=False)
    equery = {'timestamp__castdate__range': tquery['modificationtime__castdate__range']}

    if 'processingtype' in request.session['requestParams'] and request.session['requestParams']['processingtype']:
        if '|' not in request.session['requestParams']['processingtype']:
            equery['processingtype'] = request.session['requestParams']['processingtype']
        else:
            pts = request.session['requestParams']['processingtype'].split('|')
            equery['processingtype__in'] = pts

    events = ProdNeventsHistory.objects.filter(**equery).values()

    timeline = set([ev['timestamp'] for ev in events])
    timelinestr = [datetime.strftime(ts, settings.DATETIME_FORMAT) for ts in timeline]

    if 'view' in request.session['requestParams'] and request.session['requestParams']['view'] and request.session['requestParams']['view'] == 'separated':
        view = request.session['requestParams']['view']
    else:
        view = 'joint'

    plot_data = []

    if view == 'joint':
        ev_states = ['running', 'waiting']

        data = {}
        for es in ev_states:
            data[es] = {}
            for ts in timelinestr:
                data[es][ts] = 0
        for ev in events:
            for es in ev_states:
                data[es][datetime.strftime(ev['timestamp'], settings.DATETIME_FORMAT)] += ev['nevents' + str(es)]
    else:
        processingtypes = set([ev['processingtype'] for ev in events])
        ev_states = ['running', 'waiting']
        lines = []
        for prtype in processingtypes:
            for evst in ev_states:
                lines.append(str(prtype + '_' + evst))
        if len(processingtypes) > 1:
            lines.append('total_running')
            lines.append('total_waiting')

        data = {}
        for l in lines:
            data[l] = {}
            for ts in timelinestr:
                data[l][ts] = 0
        for ev in events:
            for l in lines:
                if ev['processingtype'] in l:
                    data[l][datetime.strftime(ev['timestamp'], settings.DATETIME_FORMAT)] += ev['nevents' + str(l.split('_')[1])]
                if l.startswith('total'):
                    data[l][datetime.strftime(ev['timestamp'], settings.DATETIME_FORMAT)] += ev['nevents' + str(l.split('_')[1])]

    for key, value in data.items():
        newDict = {'state': key, 'values':[]}
        for ts, nevents in value.items():
            newDict['values'].append({'timestamp': ts, 'nevents':nevents})
        newDict['values'] = sorted(newDict['values'], key=lambda k: k['timestamp'])
        plot_data.append(newDict)


    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        plot_data_list = [['timestamp'],]
        plot_data_list[0].extend([point['timestamp'] for point in plot_data[0]['values']])
        for i, line in enumerate(plot_data):
            plot_data_list.append([line['state']])
            plot_data_list[i+1].extend([point['nevents'] for point in plot_data[i]['values']])

        dump = json.dumps(plot_data_list, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'plotData': json.dumps(plot_data)
        }
        response = render(request, 'prodNeventsTrend.html', data, content_type='text/html')
        setCacheEntry(request, "prodNeventsTrend", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def runningProdRequests(request):
    valid, response = initRequest(request)

    if ('dt' in request.session['requestParams'] and 'tk' in request.session['requestParams']):
        tk = request.session['requestParams']['tk']
        data = getCacheEntry(request, tk, isData=True)
        return HttpResponse(data, content_type='application/json')

    # Here we try to get cached data
    data = getCacheEntry(request, "runningProdRequests")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        # if 'ages' in data:
        #     data['ages'] = preparePlotData(data['ages'])
        # if 'neventsFStasksSum' in data:
        #     data['neventsFStasksSum'] = preparePlotData(data['neventsFStasksSum'])
        # if 'neventsAFIItasksSum' in data:
        #     data['neventsAFIItasksSum'] = preparePlotData(data['neventsAFIItasksSum'])
        # if 'neventsByProcessingType' in data:
        #     data['neventsByProcessingType'] = preparePlotData(data['neventsByProcessingType'])
        # if 'aslotsByType' in data:
        #     data['aslotsByType'] = preparePlotData(data['aslotsByType'])
        response = render(request, 'runningProdRequests.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    xurl = request.get_full_path()
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    exquery = {}

    rquery = {}
    if 'fullcampaign' in request.session['requestParams']:
        if ':' in request.session['requestParams']['fullcampaign']:
            rquery['campaign'] = request.session['requestParams']['fullcampaign'].split(':')[0]
            rquery['subcampaign'] = request.session['requestParams']['fullcampaign'].split(':')[1]
        else:
            rquery['campaign'] = request.session['requestParams']['fullcampaign']

    if 'group' in request.session['requestParams'] and '_' in request.session['requestParams']['group']:
        rquery['provenance'] = request.session['requestParams']['group'].split('_')[0]
        rquery['physgroup'] = request.session['requestParams']['group'].split('_')[1]

    if 'requesttype' in request.session['requestParams']:
        rquery['requesttype'] = request.session['requestParams']['requesttype']

    if 'status' in request.session['requestParams']:
        rquery['status'] = request.session['requestParams']['status']


    rrequests = RunningProdRequestsModel.objects.filter(**rquery).values()

    request_list = [t for t in rrequests]
    nrequests = len(request_list)
    slots = 0
    aslots = 0
    # ages = []
    neventsTotSum = 0
    neventsUsedTotSum = 0
    # rjobs1coreTot = 0
    # rjobs8coreTot = 0
    for req in request_list:
        neventsTotSum += req['nevents'] if req['nevents'] is not None else 0
        neventsUsedTotSum += req['neventsused']
        slots += req['slots'] if req['slots'] else 0
        aslots += req['aslots'] if req['aslots'] else 0
        req['fullcampaign'] = req['campaign'] + ':' + req['subcampaign'] if req['subcampaign'] is not None and len(req['subcampaign']) > 0 else req['campaign']
        req['group'] = req['provenance'] + '_' + req['physgroup']

        # ages.append(req['age'])


    plotageshistogram = 0
    # if sum(ages) == 0: plotageshistogram = 0
    # sumd = task_summary_dict(request, task_list, ['status','workinggroup','cutcampaign', 'processingtype'])

    ### Putting list of requests to cache separately for dataTables plugin
    transactionKey = random.randrange(100000000)
    setCacheEntry(request, transactionKey, json.dumps(request_list, cls=DateEncoder), 60 * 30, isData=True)

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        dump = json.dumps(request_list, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': xurl,
            'nosorturl': nosorturl,
            'requests': request_list,
            'nrequests': nrequests,
            # 'ages': ages,
            'slots': slots,
            'aslots': aslots,
            # 'sumd': sumd,
            'neventsUsedTotSum': round(neventsUsedTotSum / 1000000., 1),
            'neventsTotSum': round(neventsTotSum / 1000000., 1),
            # 'plotageshistogram': plotageshistogram,
            'built': datetime.now().strftime("%H:%M:%S"),
            'transKey': transactionKey,
        }
        response = render(request, 'runningProdRequests.html', data, content_type='text/html')
        setCacheEntry(request, "runningProdRequests", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def runningDPDProdTasks(request):
    # redirect to united runningProdTasks page
    return redirect('/runningprodtasks/?preset=DPD')


@login_customrequired
def runningMCProdTasks(request):
    # redirect to united runningProdTasks page
    return redirect('/runningprodtasks/?preset=MC')