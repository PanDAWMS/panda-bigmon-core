from collections import OrderedDict

from datetime import datetime

from django.db.models import Count
from django.shortcuts import render_to_response

from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, escapeInput
from core.harvester.models import HarvesterWorkers,HarvesterRelJobsWorkers

harvWorkStatuses = [
    'missed', 'submitted', 'ready', 'running', 'idle', 'finished', 'failed', 'cancelled'
]


@login_customrequired
def harvesterWorkersDash(request):
    valid, response = initRequest(request)

    hours = 24 * 3
    if 'days' in request.session['requestParams']:
        days = int(request.session['requestParams']['days'])
        hours = days*24
    query= setupView(request, hours=hours, wildCardExt=False)


    tquery = {}
    tquery['status__in'] = ['missed', 'submitted', 'idle', 'finished', 'failed', 'cancelled']
    tquery['lastupdate__range'] = query['modificationtime__range']
    if 'harvesterid__in' in query:
        tquery['harvesterid__in'] = query['harvesterid__in']

    harvesterWorkers = []
    harvesterWorkers.extend(HarvesterWorkers.objects.values('computingsite','status').filter(**tquery).annotate(Count('status')).order_by('computingsite'))

    # This is for exclusion of intermediate states from time window
    tquery['status__in'] = ['ready', 'running']
    del tquery['lastupdate__range']
    harvesterWorkers.extend(HarvesterWorkers.objects.values('computingsite','status').filter(**tquery).annotate(Count('status')).order_by('computingsite'))

    statusesSummary = OrderedDict()
    for harvesterWorker in harvesterWorkers:
        if not harvesterWorker['computingsite'] in statusesSummary:
            statusesSummary[harvesterWorker['computingsite']] = OrderedDict()
            for harwWorkStatus in harvWorkStatuses:
                statusesSummary[harvesterWorker['computingsite']][harwWorkStatus] = 0
        statusesSummary[harvesterWorker['computingsite']][harvesterWorker['status']] = harvesterWorker['status__count']

    # SELECT computingsite,status, workerid, LASTUPDATE, row_number() over (partition by workerid, computingsite ORDER BY LASTUPDATE ASC) partid FROM ATLAS_PANDA.HARVESTER_WORKERS /*GROUP BY WORKERID ORDER BY COUNT(WORKERID) DESC*/

    data = {
        'statusesSummary': statusesSummary,
        'harvWorkStatuses':harvWorkStatuses,
        'request': request,
        'hours':hours,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    endSelfMonitor(request)
    response = render_to_response('harvworksummarydash.html', data, content_type='text/html')
    return response


# SELECT COMPUTINGSITE,STATUS, count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS WHERE SUBMITTIME > (sysdate - interval '35' day) group by COMPUTINGSITE,STATUS
@login_customrequired
def harvesterWorkList(request):
    valid, response = initRequest(request)
    query,extra, LAST_N_HOURS_MAX = setupView(request, hours=24*3, wildCardExt=True)

    statusDefined = False
    if 'status__in' in query:
        statusDefined = True

    tquery = {}

    if statusDefined:
        tquery['status__in'] = list(set(query['status__in']).intersection(['missed', 'submitted', 'idle', 'finished', 'failed', 'cancelled']))
    else:
        tquery['status__in'] = ['missed', 'submitted', 'idle', 'finished', 'failed', 'cancelled']

    tquery['lastupdate__range'] = query['modificationtime__range']

    workerslist = []
    if len(tquery['status__in']) > 0:
        workerslist.extend(HarvesterWorkers.objects.values('computingsite','status', 'submittime','harvesterid','workerid').filter(**tquery).extra(where=[extra]))

    if statusDefined:
        tquery['status__in'] = list(set(query['status__in']).intersection(['ready', 'running']))

    del tquery['lastupdate__range']
    if len(tquery['status__in']) > 0:
        workerslist.extend(HarvesterWorkers.objects.values('computingsite','status', 'submittime','harvesterid','workerid').filter(**tquery).extra(where=[extra]))

    data = {
        'workerslist':workerslist,
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    endSelfMonitor(request)
    response = render_to_response('harvworkerslist.html', data, content_type='text/html')
    return response

@login_customrequired
def harvesterWorkerInfo(request):
    valid, response = initRequest(request)
    harvesterid = None
    workerid = None
    workerinfo = {}

    if 'harvesterid' in request.session['requestParams']:
        harvesterid = escapeInput(request.session['requestParams']['harvesterid'])
    if 'workerid' in request.session['requestParams']:
        workerid = int(request.session['requestParams']['workerid'])

    workerslist = []
    error = None
    if harvesterid and workerid:
        tquery = {}
        tquery['harvesterid'] = harvesterid
        tquery['workerid'] = workerid
        workerslist.extend(HarvesterWorkers.objects.filter(**tquery).values('harvesterid','workerid',
                                                                            'lastupdate','status','batchid','nodeid',
                                                                            'queuename', 'computingsite','submittime',
                                                                            'starttime','endtime','ncore','errorcode',
                                                                            'stdout','stderr','batchlog'))

        if len(workerslist) > 0:
            corrJobs = []
            corrJobs.extend(HarvesterRelJobsWorkers.objects.filter(**tquery).values('pandaid'))
            workerinfo = workerslist[0]
            workerinfo['corrJobs'] = []
            for corrJob in corrJobs:
                workerinfo['corrJobs'].append(corrJob['pandaid'])
        else:
            workerinfo = None
    else:
        error = "Harvesterid + Workerid is not specified"

    data = {
        'request': request,
        'error': error,
        'workerinfo': workerinfo,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'built': datetime.now().strftime("%H:%M:%S"),
    }

    endSelfMonitor(request)
    response = render_to_response('harvworkerinfo.html', data, content_type='text/html')
    return response