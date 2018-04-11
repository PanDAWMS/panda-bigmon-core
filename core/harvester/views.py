import random
import json
from collections import OrderedDict

from datetime import datetime

from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.db import connection

from django.utils.cache import patch_cache_control, patch_response_headers
from core.libs.cache import setCacheEntry, getCacheEntry
from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, escapeInput, DateEncoder, \
    extensibleURL, DateTimeEncoder
from core.harvester.models import HarvesterWorkers,HarvesterRelJobsWorkers,HarvesterDialogs

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

def harvesterfm(request):
    import json
    valid, response = initRequest(request)
    #query, extra, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    extra = '1=1'
    xurl = extensibleURL(request)

    if 'instance' in request.session['requestParams']:
        instance = request.session['requestParams']['instance']
        # if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or ('json' in request.session['requestParams']):
        #     data = getCacheEntry(request, instance,isData=True)
        #     import json
        #     return HttpResponse(data, content_type='text/html')
        data = getCacheEntry(request, "harvester")
        if data is not None:
            import json
            data = json.loads(data)
            data['request'] = request
            response = render_to_response('harvesterfm.html', data, content_type='text/html')
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            endSelfMonitor(request)
            return response
        if ('dialogs' in request.session['requestParams'] and 'instance' in request.session['requestParams']):
            dialogs = []
            tquery = {}
            tquery['harvesterid'] = instance
            dialogsList = HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime')
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for dialog in dialogsList:
                dialog['creationtime'] = datetime.strptime(str(dialog['creationtime']), old_format).strftime(new_format)
                dialogs.append(dialog)
            return HttpResponse(json.dumps(dialogs, cls=DateTimeEncoder), content_type='text/html')
        if ('dt' in request.session['requestParams'] and 'tk' in request.session['requestParams'] ):
            tk = request.session['requestParams']['tk']
            data = getCacheEntry(request, tk,isData=True)
            return HttpResponse(data, content_type='text/html')
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
                        if datetime.strptime(data['workersList'][worker]['wrklastupdate'].replace('T',' '), '%Y-%m-%d %H:%M:%S') < datetime.now() - timedelta(days=60):
                            del data['workersList'][worker]
        else:
            lastupdateCache = ''
            workersListCache = []

        status = ''
        computingsite = ''
        workerid=''
        days =''
        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" %(str(request.session['requestParams']['status']))
        if 'computingsite' in request.session['requestParams']:
            computingsite = """AND computingsite like '%s'""" %(str(request.session['requestParams']['computingsite']))
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" %(request.session['requestParams']['workerid'])
        if 'days' in request.session['requestParams']:
            days = """AND to_date("wrklastupdate", 'dd-mm-yyyy hh24:mi:ss') > sysdate - %s """ %(request.session['requestParams']['days'])
        sqlquery = """
        select * from (SELECT
        ff.harvester_id,
        ff.description,
        to_char(ff.starttime, 'dd-mm-yyyy hh24:mi:ss') as "insstarttime",
        ff.owner,
        ff.hostname,
        ff.sw_version,
        ff.commit_stamp,
        gg.workerid,
        to_char((select max(lastupdate) from atlas_panda.harvester_workers where harvesterid like '%s'), 'dd-mm-yyyy hh24:mi:ss') as "inslastupdate",
        gg.status,
        gg.batchid,
        gg.nodeid,
        gg.queuename,
        gg.computingsite,
        to_char(gg.submittime, 'dd-mm-yyyy hh24:mi:ss') as "submittime",
        to_char(gg.lastupdate , 'dd-mm-yyyy hh24:mi:ss') as "wrklastupdate",
        to_char(gg.starttime , 'dd-mm-yyyy hh24:mi:ss') as "wrkstarttime",
        to_char(gg.endtime, 'dd-mm-yyyy hh24:mi:ss') as "wrkendtime",
        gg.ncore,
        gg.errorcode,
        gg.stdout,
        gg.stderr,
        gg.batchlog,
        gg.resourcetype,
        gg.nativeexitcode,
        gg.nativestatus,
        gg.diagmessage,
        gg.computingelement,
        gg.njobs,
        (select count(pandaid) from atlas_panda.harvester_rel_jobs_workers where atlas_panda.harvester_rel_jobs_workers.harvesterid =  ff.harvester_id and atlas_panda.harvester_rel_jobs_workers.workerid = gg.workerid) as harvesterpandaids,
        (select count(pandaid) from atlas_panda.harvester_rel_jobs_workers where  atlas_panda.harvester_rel_jobs_workers.workerid = gg.workerid) as totalpandaids

        FROM
        atlas_panda.harvester_workers gg,
        atlas_panda.harvester_instances ff
        WHERE
        ff.harvester_id = gg.harvesterid) where harvester_id like '%s' %s %s %s %s %s
        order by workerid DESC
        """ % (str(instance), str(instance),status, computingsite, workerid, lastupdateCache,days)
        workersList = []
        cur = connection.cursor()
        cur.execute(sqlquery)
        columns = [str(i[0]).lower() for i in cur.description]
        workersDictinoary = {}

        timeLastUpdate = ''
        if workersListisEmty == False:
            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersListCache[int(object['workerid'])] = object
                timeLastUpdate = object['inslastupdate']
            workersList = workersListCache.values()
            workersDictinoary = workersListCache

        else:
            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersDictinoary[int(object['workerid'])] = object
                workersList.append(object)
                timeLastUpdate = object['inslastupdate']

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

        if 'display_limit_workers' in request.session['requestParams']:
            display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
        else:
            display_limit_workers = 30000

        generalWorkersFields = ['workerid','status','batchid','nodeid','queuename','computingsite','submittime','wrklastupdate','wrkstarttime','wrkendtime','ncore','errorcode','stdout','stderr','batchlog','resourcetype','nativeexitcode','nativestatus','diagmessage','totalpandaids', 'harvesterpandaids','njobs','computingelement']
        generalWorkersList = []

        wrkPandaIDs ={}
        for i, worker in enumerate(workersList):
            object = {}
            computingsites.setdefault(worker['computingsite'],[]).append(worker['workerid'])
            statuses.setdefault(worker['status'],[]).append(worker['workerid'])
            wrkPandaIDs[worker['workerid']] = worker['harvesterpandaids']
            workerIDs.add(worker['workerid'])
            for field in generalWorkersFields:
                object[field] = worker[field]
            generalWorkersList.append(object)
            if i == len(workersList) - 1:
                for computingsite in computingsites.keys():
                    computingsites[computingsite] = len(computingsites[computingsite])
                for status in statuses.keys():
                    statuses[status] = len(statuses[status])
                generalInstanseInfo = {'HarvesterID':worker['harvester_id'], 'Description':worker['description'], 'Starttime': worker['insstarttime'],
                                      'Owner':worker['owner'], 'Hostname':worker['hostname'],'Lastupdate':worker['inslastupdate'], 'Computingsites':computingsites,'Statuses':statuses,'Software version':worker['sw_version'],'Commit stamp':worker['commit_stamp'], 'wrkpandaids': OrderedDict(sorted(wrkPandaIDs.items(), key=lambda x: x[1], reverse=True)[:200])
                                      }
        transactionKey = random.randrange(1000000)
        data = {
                'generalInstanseInfo':generalInstanseInfo,
                'type':'workers',
                'instance':instance,
                'xurl':xurl,
                'tk':transactionKey,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                }
        setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, 'harvester',  json.dumps(data, cls=DateEncoder),60 * 60)
        endSelfMonitor(request)
        return render_to_response('harvesterfm.html', data, content_type='text/html')

    # elif 'instance' in request.session['requestParams'] and 'workerid' in 'instance' in request.session['requestParams']:
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
        response = render_to_response('harvesterfm.html', data, content_type='text/html')
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

