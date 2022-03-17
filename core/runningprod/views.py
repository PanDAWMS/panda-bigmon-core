import random
import json
import copy

from datetime import datetime, timedelta

from django.utils import timezone
from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.utils.cache import patch_response_headers

from core.settings import defaultDatetimeFormat
from core.libs.cache import getCacheEntry, setCacheEntry, preparePlotData
from core.oauth.utils import login_customrequired
from core.views import initRequest, setupView, DateEncoder, removeParam, taskSummaryDict
from core.utils import is_json_request

from core.runningprod.utils import saveNeventsByProcessingType, prepareNeventsByProcessingType, clean_running_task_list, prepare_plots, updateView
from core.runningprod.models import RunningProdTasksModel, RunningProdRequestsModel, FrozenProdTasksModel, ProdNeventsHistory


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

    xurl = request.get_full_path()
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    nohashtagurl = removeParam(xurl, 'hashtags', mode='extensible')
    exquery = {}

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

    tquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request,
                                                            hours=0,
                                                            limit=9999999,
                                                            querytype='task',
                                                            wildCardExt=True)
    tquery, exquery, wildCardExtension = updateView(request, tquery, exquery, wildCardExtension)

    load_ended_tasks = False
    if 'days' in request.session['requestParams'] and ((
            'show_ended_tasks' in request.session['requestParams'] and request.session['requestParams']['show_ended_tasks'] == 'true') or (
                 'eventservice' in tquery and tquery['eventservice'] == 1)):
        load_ended_tasks = True

        tquery_timelimited = copy.deepcopy(tquery)

        # add time window selection for query from Frozen* model
        days = int(request.session['requestParams']['days'])
        tquery_timelimited['modificationtime__castdate__range'] = [
            (timezone.now() - timedelta(days=days)).strftime(defaultDatetimeFormat),
            timezone.now().strftime(defaultDatetimeFormat)
        ]

        if "((UPPER(status)  LIKE UPPER('all')))" in wildCardExtension:
            wildCardExtension = wildCardExtension.replace("((UPPER(status)  LIKE UPPER('all')))", "(1=1)")

    if 'sortby' in request.session['requestParams'] and '-' in request.session['requestParams']['sortby']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'creationdate-desc'
    oquery = '-' + sortby.split('-')[0] if sortby.split('-')[1].startswith('d') else sortby.split('-')[0]

    tasks = []
    if load_ended_tasks:
        tasks.extend(RunningProdTasksModel.objects.filter(**tquery).extra(where=[wildCardExtension]).exclude(
            **exquery).values().annotate(nonetoend=Count(sortby.split('-')[0])).order_by('-nonetoend', oquery)[:])
        tasks.extend(FrozenProdTasksModel.objects.filter(**tquery_timelimited).extra(where=[wildCardExtension]).exclude(
            **exquery).values().annotate(nonetoend=Count(sortby.split('-')[0])).order_by('-nonetoend', oquery)[:])
    else:
        tasks.extend(RunningProdTasksModel.objects.filter(**tquery).extra(where=[wildCardExtension]).exclude(**exquery).values().annotate(nonetoend=Count(sortby.split('-')[0])).order_by('-nonetoend', oquery))

    qtime = datetime.now()
    task_list = [t for t in tasks]
    ntasks = len(tasks)

    # clean task list
    task_list = clean_running_task_list(task_list)

    # produce required plots
    plots_dict = prepare_plots(task_list, productiontype=productiontype)

    # get param summaries for select drop down menus
    sumd = taskSummaryDict(request, task_list, ['status', 'workinggroup', 'cutcampaign', 'processingtype'])

    # get global sum
    gsum = {
        'nevents': sum([t['nevents'] for t in task_list]),
        'aslots': sum([t['aslots'] for t in task_list]),
        'ntasks': len(task_list)
    }

    # putting list of tasks to cache separately for dataTables plugin
    transactionKey = random.randrange(100000000)
    setCacheEntry(request, transactionKey, json.dumps(task_list, cls=DateEncoder), 60 * 30, isData=True)

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
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': xurl,
            'nosorturl': nosorturl,
            'nohashtagurl': nohashtagurl,
            'sortby': sortby,
            'built': datetime.now().strftime("%H:%M:%S"),
            'transKey': transactionKey,
            'qtime': qtime,
            'productiontype': json.dumps(productiontype),
            'sumd': sumd,
            'gsum': gsum,
            'plots': plots_dict,
        }
        response = render(request, 'runningProdTasks.html', data, content_type='text/html')
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
    equery = {}
    if 'days' in request.session['requestParams'] and request.session['requestParams']['days']:
        try:
            days = int(request.session['requestParams']['days'])
        except:
            days = defaultdays
        starttime = datetime.now() - timedelta(days=days)
        endtime = datetime.now()
        request.session['requestParams']['days'] = days
    else:
        starttime = datetime.now() - timedelta(days=defaultdays)
        endtime = datetime.now()
        request.session['requestParams']['days'] = defaultdays
    equery['timestamp__range'] = [starttime, endtime]

    if 'processingtype' in request.session['requestParams'] and request.session['requestParams']['processingtype']:
        if '|' not in request.session['requestParams']['processingtype']:
            equery['processingtype'] = request.session['requestParams']['processingtype']
        else:
            pts = request.session['requestParams']['processingtype'].split('|')
            equery['processingtype__in'] = pts

    events = ProdNeventsHistory.objects.filter(**equery).values()

    timeline = set([ev['timestamp'] for ev in events])
    timelinestr = [datetime.strftime(ts, defaultDatetimeFormat) for ts in timeline]

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
                data[es][datetime.strftime(ev['timestamp'], defaultDatetimeFormat)] += ev['nevents' + str(es)]
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
                    data[l][datetime.strftime(ev['timestamp'], defaultDatetimeFormat)] += ev['nevents' + str(l.split('_')[1])]
                if l.startswith('total'):
                    data[l][datetime.strftime(ev['timestamp'], defaultDatetimeFormat)] += ev['nevents' + str(l.split('_')[1])]

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
    # sumd = taskSummaryDict(request, task_list, ['status','workinggroup','cutcampaign', 'processingtype'])

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