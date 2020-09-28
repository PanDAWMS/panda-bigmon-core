import random
import json
import copy

from datetime import datetime, timedelta

from django.utils import timezone
from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect
from django.utils.cache import patch_response_headers

from core.settings import defaultDatetimeFormat
from core.libs.cache import getCacheEntry, setCacheEntry, preparePlotData
from core.auth.utils import login_customrequired
from core.views import initRequest, setupView, DateEncoder, removeParam, preprocessWildCardString, taskSummaryDict
from core.utils import is_json_request

from core.runningprod.utils import saveNeventsByProcessingType, prepareNeventsByProcessingType, clean_running_task_list, prepare_plots
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
        response = render_to_response('runningProdTasks.html', data, content_type='text/html')
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

    tquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=0, limit=9999999, querytype='task',
                                                            wildCardExt=True)

    if 'workinggroup' in tquery and 'preset' in request.session['requestParams'] and request.session['requestParams']['preset'] == 'MC' and ',' in tquery['workinggroup']:
        #     excludeWGList = list(str(wg[1:]) for wg in request.session['requestParams']['workinggroup'].split(','))
        #     exquery['workinggroup__in'] = excludeWGList
        try:
            del tquery['workinggroup']
        except:
            pass
    if 'status' in request.session['requestParams'] and request.session['requestParams']['status'] == '':
        try:
            del tquery['status']
        except:
            pass
    if 'site' in request.session['requestParams'] and request.session['requestParams']['site'] == 'hpc':
        try:
            del tquery['site']
        except:
            pass
        exquery['site__isnull'] = True
    if 'simtype' in request.session['requestParams'] and request.session['requestParams']['simtype']:
            tquery['simtype'] = request.session['requestParams']['simtype']
    if 'runnumber' in request.session['requestParams'] and request.session['requestParams']['runnumber']:
            tquery['runnumber'] = request.session['requestParams']['runnumber']
    if 'ptag' in request.session['requestParams'] and request.session['requestParams']['ptag']:
            tquery['ptag'] = request.session['requestParams']['ptag']
    if 'hashtags' in request.session['requestParams']:
        wildCardExtension += ' AND ('
        wildCards = request.session['requestParams']['hashtags'].split(',')
        currentCardCount = 1
        countCards = len(wildCards)
        for card in wildCards:
            if '*' not in card:
                card = '*' + card + '*'
            elif card.startswith('*'):
                card = card + '*'
            elif card.endswith('*'):
                card = '*' + card
            wildCardExtension += preprocessWildCardString(card, 'hashtags')
            if (currentCardCount < countCards): wildCardExtension += ' AND '
            currentCardCount += 1
        wildCardExtension += ')'
    if 'jumbo' in request.session['requestParams'] and request.session['requestParams']['jumbo']:
        tquery['jumbo'] = request.session['requestParams']['jumbo']
    if 'sortby' in request.session['requestParams'] and '-' in request.session['requestParams']['sortby'] :
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'creationdate-desc'
    if 'currentpriority__gte' in tquery and 'currentpriority__lte' in tquery:
        tquery['priority__gte'] = tquery['currentpriority__gte']
        tquery['priority__lte'] = tquery['currentpriority__lte']
        del tquery['currentpriority__gte']
        del tquery['currentpriority__lte']

    oquery = '-' + sortby.split('-')[0] if sortby.split('-')[1].startswith('d') else sortby.split('-')[0]

    tasks = []
#    if "((UPPER(status)  LIKE UPPER('all')))" in wildCardExtension and tquery['eventservice'] == 1:
    if 'eventservice' in tquery and tquery['eventservice'] == 1 and 'days' in request.session['requestParams']:

        setupView(request)
        if 'status__in' in tquery:
            del tquery['status__in']
        excludedTimeQuery = copy.deepcopy(tquery)

        if ('days' in request.GET) and (request.GET['days']):
            days = int(request.GET['days'])
            hours = 24 * days
            startdate = timezone.now() - timedelta(hours=hours)
            startdate = startdate.strftime(defaultDatetimeFormat)
            enddate = timezone.now().strftime(defaultDatetimeFormat)
            tquery['modificationtime__range'] = [startdate, enddate]

        if "((UPPER(status)  LIKE UPPER('all')))" in wildCardExtension:
            wildCardExtension = wildCardExtension.replace("((UPPER(status)  LIKE UPPER('all')))", "(1=1)")
        tasks.extend(RunningProdTasksModel.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension]).exclude(
            **exquery).values().annotate(nonetoend=Count(sortby.split('-')[0])).order_by('-nonetoend', oquery)[:])
        tasks.extend(FrozenProdTasksModel.objects.filter(**tquery).extra(where=[wildCardExtension]).exclude(
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
            # 'tasks': task_list,
            # 'ntasks': ntasks,
            'sumd': sumd,
            'gsum': gsum,
            'plots': plots_dict,
        }
        response = render_to_response('runningProdTasks.html', data, content_type='text/html')
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
        response = render_to_response('prodNeventsTrend.html', data, content_type='text/html')
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
        response = render_to_response('runningProdRequests.html', data, content_type='text/html')
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
        response = render_to_response('runningProdRequests.html', data, content_type='text/html')
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