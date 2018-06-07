import random
import json
from collections import OrderedDict

from datetime import datetime

from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect
from django.db import connection

from django.utils.cache import patch_cache_control, patch_response_headers
from core.libs.cache import setCacheEntry, getCacheEntry
from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, escapeInput, DateEncoder, \
    extensibleURL, DateTimeEncoder
from core.harvester.models import HarvesterWorkers,HarvesterRelJobsWorkers,HarvesterDialogs,HarvesterWorkerStats

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
    query = setupView(request, hours=hours, wildCardExt=False)


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
from datetime import datetime,timedelta
import json
def harvesterfm (request):
    return redirect('/harvesters/')

def harvesters(request):
    import json
    valid, response = initRequest(request)
    #query, extra, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    extra = '1=1'
    xurl = extensibleURL(request)
    URL = ''
    if 'instance' in request.session['requestParams']:
        instance = request.session['requestParams']['instance']

        # if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or ('json' in request.session['requestParams']):
        #     data = getCacheEntry(request, instance,isData=True)
        #     import json
        #     return HttpResponse(data, content_type='text/html')
        # data = getCacheEntry(request, "harvester")
        # if data is not None:
        #     import json
        #     data = json.loads(data)
        #     data['request'] = request
        #     response = render_to_response('harvesters.html', data, content_type='text/html')
        #     patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        #     endSelfMonitor(request)
        #     return response
        if ('workersstats' in request.session['requestParams'] and 'instance' in request.session['requestParams']):
            harvsterworkerstats = []
            tquery = {}
            tquery['harvesterid'] = instance
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']
            harvsterworkerstat = HarvesterWorkerStats.objects.filter(**tquery).values('computingsite', 'resourcetype', 'status',
                                                                           'nworkers','lastupdate').filter(**tquery).extra(
                where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for stat in harvsterworkerstat:
                stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
                harvsterworkerstats.append(stat)
            return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='text/html')
        if ('dialogs' in request.session['requestParams'] and 'instance' in request.session['requestParams']):
            dialogs = []
            tquery = {}
            tquery['harvesterid'] = instance
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']
            dialogsList = HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for dialog in dialogsList:
                dialog['creationtime'] = datetime.strptime(str(dialog['creationtime']), old_format).strftime(new_format)
                dialogs.append(dialog)
            return HttpResponse(json.dumps(dialogs, cls=DateTimeEncoder), content_type='text/html')


        lastupdateCache = ''
        workersListCache = []


        data = {}
        setCacheEntry(request, instance, json.dumps(data, cls=DateEncoder), 1, isData=True)

        workersListisEmty = True
        if 'status' not in request.session['requestParams'] and 'computingsite' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
            data = getCacheEntry(request, instance,isData=True)
            if data is not None and data !="null":
                if 'lastupdate'  in data:
                    data = json.loads(data)
                    lastupdateCache = data['lastupdate'].replace('T',' ')
                    lastupdateCache = """ AND "wrklastupdate" >= to_date('%s','yyyy-mm-dd hh24:mi:ss')"""%(lastupdateCache)
                    workersListCache = data['workersList']
                    workersListisEmty = False

                    tmpworkerList = data['workersList'].keys()
                    for worker in tmpworkerList:
                        if datetime.strptime(data['workersList'][worker]['wrklastupdate'], '%d-%m-%Y %H:%M:%S') < datetime.now() - timedelta(days=60):
                            del data['workersList'][worker]
        else:
            lastupdateCache = ''

        URL += '?instance=' + request.session['requestParams']['instance']
        status = ''
        computingsite = ''
        workerid=''
        days =''
        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" %(str(request.session['requestParams']['status']))
            URL +=  '&status=' +str(request.session['requestParams']['status'])
        if 'computingsite' in request.session['requestParams']:
            computingsite = """AND computingsite like '%s'""" %(str(request.session['requestParams']['computingsite']))
            URL += '&computingsite=' + str(request.session['requestParams']['computingsite'])
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" %(request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'days' in request.session['requestParams']:
            days = """AND to_date(wrklastupdate, 'dd-mm-yyyy hh24:mi:ss') > sysdate - %s """ %(request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
        sqlquery = """
        SELECT * FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
        where harvester_id like '%s' %s %s %s %s %s and ROWNUM<=1
        order by workerid DESC
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days)

        sqlquerycomputingsite = """
        SELECT COMPUTINGSITE,count(*) FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
        where harvester_id like '%s' %s %s %s %s %s group by COMPUTINGSITE
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days)

        sqlquerystatus = """
        SELECT status,count(*) FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
        where harvester_id like '%s' %s %s %s %s %s group by status
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days)

        workersList = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        harvesterinfo = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]

        cur.execute(sqlquerycomputingsite)
        computingsites = cur.fetchall()
        cur.execute(sqlquerystatus)
        statuses = cur.fetchall()

        computingsitesDict = {}
        for computingsite in computingsites:
            computingsitesDict[computingsite[0]] = computingsite[1]

        statusesDict = {}
        for status in statuses:
            statusesDict[status[0]] = status[1]

        for worker in harvesterinfo:
            object = {}
            object = dict(zip(columns, worker))
            workersList.append(object)
        if len(workersList)==0:
            return HttpResponse(json.dumps({'message': 'Instance is not found'}),
                            content_type='text/html')

        # dbCache = {
        #     "workersList": workersDictinoary,
        #     "lastupdate": timeLastUpdate
        # }
        # print len(workersListCache)
        # if 'status' not in request.session['requestParams'] and 'computingsite' not in request.session['requestParams'] and 'workerid' not in request.session['requestParams'] :
        #     setCacheEntry(request, instance, json.dumps(dbCache, cls=DateEncoder), 86400, isData=True)

        statuses = {}
        computingsites = {}
        workerIDs = set()
        generalInstanseInfo = {}

        # if 'display_limit_workers' in request.session['requestParams']:
        #     display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
        #     URL += '&display_limit_workers=' + str(request.session['requestParams']['display_limit_workers'])
        # else:
        #     display_limit_workers = 30000
        #     URL += '&display_limit_workers=' + str(display_limit_workers)

        generalInstanseInfo = {'HarvesterID':workersList[0]['harvester_id'], 'Description':workersList[0]['description'], 'Starttime': workersList[0]['insstarttime'],
                                      'Owner':workersList[0]['owner'], 'Hostname':workersList[0]['hostname'],'Lastupdate':workersList[0]['inslastupdate'], 'Computingsites':computingsitesDict,'Statuses':statusesDict,'Software version':workersList[0]['sw_version'],'Commit stamp':workersList[0]['commit_stamp']
        }

        data = {
                'generalInstanseInfo':generalInstanseInfo,
                'type':'workers',
                'instance':instance,
                'computingsite':0,
                'xurl':xurl,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'url': URL
                }
        # setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, 'harvester',  json.dumps(data, cls=DateEncoder),60 * 60)
        endSelfMonitor(request)
        return render_to_response('harvesters.html', data, content_type='text/html')
    elif 'computingsite' in request.session['requestParams'] and 'instance' not in request.session['requestParams']:
        computingsite = request.session['requestParams']['computingsite']
        if ('workersstats' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']):
            harvsterworkerstats = []
            tquery = {}
            tquery['computingsite'] = computingsite
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']
            harvsterworkerstat = HarvesterWorkerStats.objects.filter(**tquery).values('harvesterid', 'resourcetype', 'status',
                                                                           'nworkers','lastupdate').filter(**tquery).extra(
                where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for stat in harvsterworkerstat:
                stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
                harvsterworkerstats.append(stat)
            return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='text/html')
        if ('dialogs' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']):
            dialogs = []
            tquery = {}
            instancelist = request.session['requestParams']['instancelist'].split(',')
            limit = 100
            if 'limit' in request.session['requestParams']:
                    limit = request.session['requestParams']['limit']
            dialogsList = HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(harvesterid__in=instancelist).extra(where=[extra]).order_by('-creationtime')[:limit]
                # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for dialog in dialogsList:
                dialog['creationtime'] = datetime.strptime(str(dialog['creationtime']), old_format).strftime(new_format)
                dialogs.append(dialog)
            return HttpResponse(json.dumps(dialogs, cls=DateTimeEncoder), content_type='text/html')

        URL += '?computingsite=' + request.session['requestParams']['computingsite']
        status = ''

        workerid = ''
        days = ''
        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
            URL += '&status=' + str(request.session['requestParams']['status'])
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'days' in request.session['requestParams']:
            days = """AND to_date(wrklastupdate, 'dd-mm-yyyy hh24:mi:ss') > sysdate - %s """ % (
            request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
        sqlquery = """
          SELECT * FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
          where computingsite like '%s' %s %s %s  and ROWNUM<=1
          order by workerid DESC
          """ % (str(computingsite), status,  workerid, days)

        sqlquerycomputingsite = """
          SELECT harvester_id,count(*) FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
          where computingsite like '%s' %s %s %s group by harvester_id
          """ % (str(computingsite), status,  workerid, days)

        sqlquerystatus = """
          SELECT status,count(*) FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
          where computingsite like '%s' %s %s %s group by status
          """ % (str(computingsite), status,  workerid, days)

        workersList = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        harvesterinfo = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]

        cur.execute(sqlquerycomputingsite)
        harvesterids = cur.fetchall()
        cur.execute(sqlquerystatus)
        statuses = cur.fetchall()

        harvesteridDict = {}
        for harvester in harvesterids:
            harvesteridDict[harvester[0]] = harvester[1]

        statusesDict = {}
        for status in statuses:
            statusesDict[status[0]] = status[1]

        for worker in harvesterinfo:
            object = {}
            object = dict(zip(columns, worker))
            workersList.append(object)
        if len(workersList)==0:
            return HttpResponse(json.dumps({'message': 'Computingsite is not found'}),
                            content_type='text/html')
        generalInstanseInfo = {'Computingsite':workersList[0]['computingsite'], 'Description':workersList[0]['description'], 'Starttime': workersList[0]['insstarttime'],
                                      'Owner':workersList[0]['owner'], 'Hostname':workersList[0]['hostname'],'Lastupdate':workersList[0]['inslastupdate'], 'Harvesters':harvesteridDict,'Statuses':statusesDict,'Software version':workersList[0]['sw_version'],'Commit stamp':workersList[0]['commit_stamp']
        }

        data = {
                'generalInstanseInfo':generalInstanseInfo,
                'type':'workers',
                'instance':0,
                'instancelist':','.join(harvesteridDict.keys()),
                'computingsite':computingsite,
                'xurl':xurl,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'url': URL
                }
        # setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, 'harvester',  json.dumps(data, cls=DateEncoder),60 * 60)
        endSelfMonitor(request)
        return render_to_response('harvesters.html', data, content_type='text/html')


    # # elif 'instance' in request.session['requestParams'] and 'workerid' in 'instance' in request.session['requestParams']:
    #     pass
    else:
        sqlquery = """
        select  
        R.harvid,
        count(R.workid) as total,
        (select cnt from   (select harvid, count(*) as cnt from (
        SELECT
        a.harvester_id as harvid, 
        b.workerid as workid,
        to_char(b.lastupdate, 'dd-mm-yyyy hh24:mi:ss') as alldate,
        (SELECT
        to_char(max(O.lastupdate), 'dd-mm-yyyy hh24:mi:ss')
        FROM atlas_panda.harvester_workers O WHERE O.harvesterid = a.harvester_id   Group by O.harvesterid) as recently, 
        a.DESCRIPTION as description
        FROM
        atlas_panda.harvester_workers b,
        atlas_panda.harvester_instances a
        WHERE a.harvester_id = b.harvesterid
        ) WHERE alldate = recently Group by harvid) W WHERE W.harvid=R.harvid) as recent,
        R.recently,
        R.sw_version,
        R.commit_stamp,
        R.lastupdate,
        R.description
        FROM (SELECT
        a.harvester_id as harvid, 
        b.workerid as workid,
        to_char(b.lastupdate, 'dd-mm-yyyy hh24:mi:ss') as alldate,
        (SELECT
        to_char(max(O.lastupdate), 'dd-mm-yyyy hh24:mi:ss')
        FROM atlas_panda.harvester_rel_jobs_workers O where  O.harvesterid = a.harvester_id   Group by O.harvesterid) as recently,
        a.sw_version,
        a.commit_stamp,
        to_char(a.lastupdate, 'dd-mm-yyyy hh24:mi:ss') as lastupdate, 
        a.DESCRIPTION as description
        FROM
        atlas_panda.harvester_workers b,
        atlas_panda.harvester_instances a
        WHERE a.harvester_id = b.harvesterid) R group by harvid,recently,sw_version,commit_stamp,lastupdate,description
        """
        instanceDictionary = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        for instance in cur:
            instanceDictionary.append(
                {'instance': instance[0], 'total': instance[1], 'recently': instance[2], 'when': instance[3],'sw_version':instance[4],'commit_stamp':instance[5],'lastupdate':instance[6], 'descr': instance[7]})

        data = {
            'instances':instanceDictionary,
            'type': 'instances',
            'xurl': xurl,
            'request':request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams']
        }
        #data =json.dumps(data,cls=DateEncoder)
        response = render_to_response('harvesters.html', data, content_type='text/html')
    return response

def getHarvesterJobs(request,instance = '', workerid = ''):
    pandaidsList = []
    qinstance = ''
    qworkerid = ''
    if instance != '':
        qinstance = "=" + "'"+instance+"'"
    else:
        qinstance = 'is not null'
    if workerid != '':
        qworkerid = '=' + workerid
    else:
        qworkerid = 'is not null'

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        from core.pandajob.models import Jobsactive4
        values = [f.name for f in Jobsactive4._meta.get_fields()]
    else:
        values = 'corecount','jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss', 'maxrss', 'nucleus', 'eventservice', 'nevents','gshare','noutputdatafiles','parentid','actualcorecount'

    sqlRequest = '''
    SELECT DISTINCT {2} FROM
    (SELECT {2}  FROM ATLAS_PANDA.JOBSARCHIVED4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1}) 
    PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSARCHIVED4.PANDAID
    UNION ALL
    SELECT {2}  FROM ATLAS_PANDA.JOBSACTIVE4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSACTIVE4.PANDAID
    UNION ALL 
    SELECT {2}  FROM ATLAS_PANDA.JOBSDEFINED4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSDEFINED4.PANDAID
    UNION ALL 
    SELECT {2} FROM ATLAS_PANDA.JOBSWAITING4,
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSWAITING4.PANDAID
    UNION ALL
    SELECT {2} FROM ATLAS_PANDAARCH.JOBSARCHIVED, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDAARCH.JOBSARCHIVED.PANDAID)
        '''
    sqlRequestFull = sqlRequest.format(qinstance,qworkerid,', '.join(values))
    cur = connection.cursor()
    cur.execute(sqlRequestFull)
    pandaids = cur.fetchall()
    columns = [str(column[0]).lower() for column in cur.description]
    for pid in pandaids:
        pandaidsList.append(dict(zip(columns, pid)))
    return pandaidsList

def workersJSON(request):
    valid, response = initRequest(request)
    if 'instance' in request.session['requestParams']:
        instance = request.session['requestParams']['instance']
        if ('dt' in request.session['requestParams']):
            if 'display_limit_workers' in request.session['requestParams']:
                display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
            else:
                display_limit_workers = 1000

            generalWorkersFields = ['workerid', 'status', 'batchid', 'nodeid', 'queuename', 'computingsite',
                                'submittime', 'wrklastupdate', 'wrkstarttime', 'wrkendtime', 'ncore', 'errorcode',
                                'stdout', 'stderr', 'batchlog', 'resourcetype', 'nativeexitcode', 'nativestatus',
                                'diagmessage', 'njobs', 'computingelement','harvester_id']

            status = ''
            computingsite = ''
            workerid = ''
            days = ''
            lastupdateCache = ''
            if 'status' in request.session['requestParams']:
                status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
            if 'computingsite' in request.session['requestParams']:
                computingsite = """AND computingsite like '%s'""" % (
                    str(request.session['requestParams']['computingsite']))
            if 'workerid' in request.session['requestParams']:
                workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            if 'days' in request.session['requestParams']:
                days = """AND to_date(wrklastupdate, 'dd-mm-yyyy hh24:mi:ss') > sysdate - %s """ % (
                    request.session['requestParams']['days'])
            fields = ','.join(generalWorkersFields)
            sqlquery = """
            SELECT * FROM(SELECT %s FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
            where harvester_id like '%s' %s %s %s %s %s
            order by WRKLASTUPDATE DESC) WHERE ROWNUM<=%s
            """ % (fields, str(instance), status, computingsite, workerid, lastupdateCache, days, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)
            return HttpResponse(json.dumps(workersList), content_type='text/html')

    if 'computingsite' in request.session['requestParams'] and 'instance' not in request.session['requestParams']:
        computingsite = request.session['requestParams']['computingsite']
        if ('dt' in request.session['requestParams']):
            if 'display_limit_workers' in request.session['requestParams']:
                display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
            else:
                display_limit_workers = 1000

            generalWorkersFields = ['workerid', 'status', 'batchid', 'nodeid', 'queuename', 'harvester_id',
                                    'submittime', 'wrklastupdate', 'wrkstarttime', 'wrkendtime', 'ncore', 'errorcode',
                                    'stdout', 'stderr', 'batchlog', 'resourcetype', 'nativeexitcode', 'nativestatus',
                                    'diagmessage', 'njobs', 'computingelement']

            status = ''
            workerid = ''
            days = ''
            lastupdateCache = ''
            if 'status' in request.session['requestParams']:
                status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
            if 'workerid' in request.session['requestParams']:
                workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            if 'days' in request.session['requestParams']:
                days = """AND to_date(wrklastupdate, 'dd-mm-yyyy hh24:mi:ss') > sysdate - %s """ % (
                    request.session['requestParams']['days'])
            fields = ','.join(generalWorkersFields)
            sqlquery = """
             SELECT * FROM(SELECT %s FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
             where computingsite like '%s' %s %s %s 
             order by WRKLASTUPDATE DESC) WHERE ROWNUM<=%s
             """ % (
            fields, str(computingsite), status,  workerid, days, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            # if workersListisEmty == False:
            #     for worker in cur:
            #         object = {}
            #         object = dict(zip(columns, worker))
            #         workersListCache[int(object['workerid'])] = object
            #         timeLastUpdate = object['inslastupdate']
            #     workersList = workersListCache.values()
            #     workersDictinoary = workersListCache
            #
            # else:
            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)

            return HttpResponse(json.dumps(workersList), content_type='text/html')