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
from core.harvester.models import HarvesterWorkers,HarvesterRelJobsWorkers,HarvesterDialogs,HarvesterWorkerStats, HarvesterSlots

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
                                                                            'stdout','stderr','batchlog','resourcetype','nativeexitcode',
                                                                            'nativestatus','diagmessage','computingelement','njobs',))

        if len(workerslist) > 0:
            # corrJobs = []
            # corrJobs.extend(HarvesterRelJobsWorkers.objects.filter(**tquery).values('pandaid'))
            workerinfo = workerslist[0]
            workerinfo['corrJobs'] = []
            workerinfo['jobsStatuses'] = {}
            workerinfo['jobsSubStatuses'] = {}
            jobs = getHarvesterJobs(request, instance=harvesterid,workerid=workerid)
            for job in jobs:
                workerinfo['corrJobs'].append(job['pandaid'])
                if job['jobstatus'] not in workerinfo['jobsStatuses']:
                    workerinfo['jobsStatuses'][job['jobstatus']] = 1
                else:
                    workerinfo['jobsStatuses'][job['jobstatus']] += 1
                if job['jobsubstatus'] not in workerinfo['jobsSubStatuses']:
                    workerinfo['jobsSubStatuses'][job['jobsubstatus']] = 1
                else:
                    workerinfo['jobsSubStatuses'][job['jobsubstatus']] += 1
        else:
            workerinfo = None
    else:
        error = "Harvesterid + Workerid is not specified"

    data = {
        'request': request,
        'error': error,
        'workerinfo': workerinfo,
        'harvesterid': harvesterid,
        'workerid': workerid,
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
@login_customrequired
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
                                                                           'nworkers','lastupdate').extra(
                where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for stat in harvsterworkerstat:
                stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
                harvsterworkerstats.append(stat)
            return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='text/html')
        'pandaids' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']
        if ('pandaids' in request.session['requestParams']  and 'instance' in request.session['requestParams']) :
            status = ''
            computingsite = ''
            workerid = ''
            days = ''
            defaulthours = 24
            resourcetype = ''
            computingelement = ''

            if 'status' in request.session['requestParams']:
                status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))

            if 'computingsite' in request.session['requestParams']:
                computingsite = """AND computingsite like '%s'""" % (
                    str(request.session['requestParams']['computingsite']))
            if 'resourcetype' in request.session['requestParams']:
                resourcetype = """AND resourcetype like '%s'""" % (
                    str(request.session['requestParams']['resourcetype']))
            if 'computingelement' in request.session['requestParams']:
                computingelement = """AND computingelement like '%s'""" % (
                    str(request.session['requestParams']['computingelement']))
            if 'workerid' in request.session['requestParams']:
                workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            if 'hours' in request.session['requestParams']:
                defaulthours = request.session['requestParams']['hours']
                hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                    defaulthours)
            else:
                hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                    defaulthours)
            if 'days' in request.session['requestParams']:
                days = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ % (
                    request.session['requestParams']['days'])
                hours = ''
                defaulthours = int(request.session['requestParams']['days']) * 24
            harvsterpandaids = []

            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']
            sqlqueryjobs = """
            SELECT * FROM (SELECT * from atlas_panda.harvester_rel_jobs_workers where  harvesterid like '%s' and workerid in (SELECT workerid FROM ATLAS_PANDA.HARVESTER_WORKERS
            where harvesterid like '%s' %s %s %s %s %s %s %s)  ORDER by lastupdate DESC) WHERE  rownum <= %s
            """ % (str(instance), str(instance), status, computingsite, workerid, days, hours, resourcetype,
            computingelement, limit)

            cur = connection.cursor()
            cur.execute(sqlqueryjobs)

            jobs = cur.fetchall()

            columns = [str(i[0]).lower() for i in cur.description]

            for job in jobs:
                object = {}
                object = dict(zip(columns, job))
                harvsterpandaids.append(object)

            return HttpResponse(json.dumps(harvsterpandaids, cls=DateTimeEncoder), content_type='text/html')
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
        defaulthours = 24
        resourcetype =''
        computingelement =''

        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" %(str(request.session['requestParams']['status']))
            URL +=  '&status=' +str(request.session['requestParams']['status'])
        if 'computingsite' in request.session['requestParams']:
            computingsite = """AND computingsite like '%s'""" %(str(request.session['requestParams']['computingsite']))
            URL += '&computingsite=' + str(request.session['requestParams']['computingsite'])
        if 'pandaid' in request.session['requestParams']:
            pandaid = request.session['requestParams']['pandaid']
            try:
                jobsworkersquery, pandaids = getWorkersByJobID(pandaid, request.session['requestParams']['instance'])
            except:
                message = """Pandaid for this instance is not found """
                return HttpResponse(json.dumps({'message': message}),
                                    content_type='text/html')
            workerid = """AND workerid in (%s)""" % (jobsworkersquery)
            URL += '&pandaid=' + str(request.session['requestParams']['pandaid'])
        if 'resourcetype' in request.session['requestParams']:
            resourcetype = """AND resourcetype like '%s'""" %(str(request.session['requestParams']['resourcetype']))
            URL +=  '&resourcetype=' +str(request.session['requestParams']['resourcetype'])
        if 'computingelement' in request.session['requestParams']:
            computingelement = """AND computingelement like '%s'""" %(str(request.session['requestParams']['computingelement']))
            URL += '&computingelement=' + str(request.session['requestParams']['computingelement'])
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" %(request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'hours' in request.session['requestParams']:
            defaulthours = request.session['requestParams']['hours']
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ %(request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
            hours = ''
            defaulthours = int(request.session['requestParams']['days']) * 24
        sqlquery = """
        SELECT * FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
        where harvester_id like '%s' %s %s %s %s %s %s and ROWNUM<=1
        order by workerid DESC
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,resourcetype,computingelement)

        sqlquerycomputingsite = """
        SELECT COMPUTINGSITE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where harvesterid like '%s' %s %s %s %s %s %s %s %s group by COMPUTINGSITE
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days,hours,resourcetype,computingelement)

        sqlquerystatus = """
        SELECT status,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where harvesterid like '%s' %s %s %s %s %s %s  %s %s group by status
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days,hours,resourcetype,computingelement)

        sqlqueryresource = """
        SELECT RESOURCETYPE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where harvesterid like '%s' %s %s %s %s %s %s %s %s group by RESOURCETYPE
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days,hours,resourcetype,computingelement)

        sqlqueryce = """
        SELECT COMPUTINGELEMENT,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where harvesterid like '%s' %s %s %s %s %s %s %s %s group by COMPUTINGELEMENT
        """ % (str(instance),status, computingsite, workerid, lastupdateCache,days, hours ,resourcetype,computingelement)

        sqlqueryjobcount = """
        SELECT count(distinct(pandaid)) as jobscount from atlas_panda.harvester_rel_jobs_workers where  harvesterid like '%s' and workerid in (SELECT workerid FROM ATLAS_PANDA.HARVESTER_WORKERS
        where harvesterid like '%s' %s %s %s %s %s %s %s %s) group by harvesterid
        """  % (str(instance),str(instance),status, computingsite, workerid, lastupdateCache,days, hours,resourcetype,computingelement)

        workersList = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        harvesterinfo = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]

        cur.execute(sqlquerycomputingsite)
        computingsites = cur.fetchall()

        cur.execute(sqlquerystatus)
        statuses = cur.fetchall()

        cur.execute(sqlqueryresource)
        resourcetypes = cur.fetchall()

        cur.execute(sqlqueryce)
        computingelements = cur.fetchall()

        cur.execute(sqlqueryjobcount)
        jobscount = cur.fetchall()

        computingsitesDict = {}
        for computingsite in computingsites:
            computingsitesDict[computingsite[0]] = computingsite[1]

        statusesDict = {}
        for status in statuses:
            statusesDict[status[0]] = status[1]

        resourcetypesDict = {}
        for resourcetype in resourcetypes:
            resourcetypesDict[resourcetype[0]] = resourcetype[1]

        computingelementsDict = {}
        for computingelement in computingelements:
            computingelementsDict[computingelement[0]] = computingelement[1]

        jobcnt = 0
        for cnt in jobscount:
            jobcnt = cnt[0]

        for worker in harvesterinfo:
            object = {}
            object = dict(zip(columns, worker))
            workersList.append(object)

        if len(workersList) == 0 or len(computingsitesDict) == 0:
            if 'hours' in request.session['requestParams']:
                URL = URL.replace('&hours='+request.session['requestParams']['hours'],'')
            if 'days' in request.session['requestParams']:
                URL = URL.replace('&days=' + request.session['requestParams']['days'], '')

            message = """Instance is not found OR no workers for this instance or time period. Try to using this <a href =/harvesters/%s&days=365>link (last 365 days)</a>""" %(URL)
            return HttpResponse(json.dumps({'message': message}),
                            content_type='text/html')

        generalInstanseInfo = {'HarvesterID':workersList[0]['harvester_id'], 'Description':workersList[0]['description'], 'Starttime': workersList[0]['insstarttime'],
                                      'Owner':workersList[0]['owner'], 'Hostname':workersList[0]['hostname'],'Lastupdate':workersList[0]['inslastupdate'], 'Computingsites':computingsitesDict,'Statuses':statusesDict,'Resourcetypes':resourcetypesDict,'Computingelements':computingelementsDict,'Software version':workersList[0]['sw_version'], 'Jobscount':jobcnt ,'Commit stamp':workersList[0]['commit_stamp']
        }
        request.session['viewParams']['selection'] = 'Harvester workers, last %s hours' %(defaulthours)
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
        if ('pandaids' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']):
            status = ''
            computingsite = ''
            workerid = ''
            days = ''
            defaulthours = 24
            resourcetype = ''
            computingelement = ''
            instance =''
            if 'instance' not in request.session['requestParams']:
                sqlqueryinstances = """
                       SELECT harvesterid
                       FROM ATLAS_PANDA.HARVESTER_WORKERS where computingsite like '%s' group by harvesterid
                       """ % (
                    request.session['requestParams']['computingsite'])
                cur = connection.cursor()
                cur.execute(sqlqueryinstances)

                instances = cur.fetchall()
                for ins in instances:
                    instance += "'" + ins[0] + "',"
                instance = instance[:-1]

            if 'status' in request.session['requestParams']:
                status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))

            if 'computingsite' in request.session['requestParams']:
                computingsite = """AND computingsite like '%s'""" % (
                    str(request.session['requestParams']['computingsite']))
            if 'resourcetype' in request.session['requestParams']:
                resourcetype = """AND resourcetype like '%s'""" % (
                    str(request.session['requestParams']['resourcetype']))
            if 'computingelement' in request.session['requestParams']:
                computingelement = """AND computingelement like '%s'""" % (
                    str(request.session['requestParams']['computingelement']))
            if 'workerid' in request.session['requestParams']:
                workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])

            if 'hours' in request.session['requestParams']:
                defaulthours = request.session['requestParams']['hours']
                hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                    defaulthours)
            else:
                hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                    defaulthours)
            if 'days' in request.session['requestParams']:
                days = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ % (
                    request.session['requestParams']['days'])
                hours = ''
                defaulthours = int(request.session['requestParams']['days'])*24
            harvsterpandaids = []

            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']
            sqlqueryjobs = """
                   SELECT * FROM (SELECT * from atlas_panda.harvester_rel_jobs_workers where harvesterid in (%s) and workerid in (SELECT workerid FROM ATLAS_PANDA.HARVESTER_WORKERS
                   where harvesterid in (%s) %s %s %s %s %s %s %s)  ORDER by lastupdate DESC) WHERE  rownum <= %s
                   """ % (str(instance), str(instance), status, computingsite, workerid, days, hours, resourcetype,
                          computingelement, limit)

            cur = connection.cursor()
            cur.execute(sqlqueryjobs)

            jobs = cur.fetchall()

            columns = [str(i[0]).lower() for i in cur.description]

            for job in jobs:
                object = {}
                object = dict(zip(columns, job))
                harvsterpandaids.append(object)
            # harvsterworkerstat = HarvesterRelJobsWorkers.objects.filter(**tquery).values('harvesterid', 'workerid', 'pandaid',
            #                                                                'lastupdate').filter(**tquery).extra(
            #     where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))

            # old_format = '%Y-%m-%d %H:%M:%S'
            # new_format = '%d-%m-%Y %H:%M:%S'
            # for stat in harvsterworkerstat:
            #     stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
            #     harvsterpandaids.append(stat)
            return HttpResponse(json.dumps(harvsterpandaids, cls=DateTimeEncoder), content_type='text/html')

        URL += '?computingsite=' + request.session['requestParams']['computingsite']
        status = ''

        workerid = ''
        days = ''
        defaulthours = 24
        resourcetype =''
        computingelement =''
        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
            URL += '&status=' + str(request.session['requestParams']['status'])
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'resourcetype' in request.session['requestParams']:
            resourcetype = """AND resourcetype like '%s'""" % (str(request.session['requestParams']['resourcetype']))
            URL += '&resourcetype=' + str(request.session['requestParams']['resourcetype'])
        if 'computingelement' in request.session['requestParams']:
            computingelement = """AND computingelement like '%s'""" %(str(request.session['requestParams']['computingelement']))
            URL += '&computingelement=' + str(request.session['requestParams']['computingelement'])
        if 'hours' in request.session['requestParams']:
            defaulthours = request.session['requestParams']['hours']
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """AND submittime  > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ % (
            request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
            hours = ''
            defaulthours = int(request.session['requestParams']['days']) * 24

        sqlquery = """
          SELECT * FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
          where computingsite like '%s' %s %s %s %s and ROWNUM<=1
          order by workerid DESC
          """ % (str(computingsite),status, workerid, resourcetype,computingelement)

        sqlquerycomputingsite = """
          SELECT harvesterid,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where computingsite like '%s' %s %s %s %s %s %s group by harvesterid
          """ % (str(computingsite), status,  workerid, days, hours, resourcetype, computingelement)

        sqlquerystatus = """
          SELECT status,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where computingsite like '%s' %s %s %s %s %s %s group by status
          """ % (str(computingsite), status,  workerid, days, hours, resourcetype, computingelement)

        sqlqueryresource = """
        SELECT RESOURCETYPE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where computingsite like '%s' %s %s %s %s %s %s group by RESOURCETYPE
        """ % (str(computingsite),status, workerid, days, hours, resourcetype, computingelement)

        sqlqueryce = """
        SELECT COMPUTINGELEMENT,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where computingsite like '%s' %s %s %s %s %s %s  group by COMPUTINGELEMENT
        """ % (str(computingsite),status, workerid, days, hours, resourcetype, computingelement)

        workersList = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        harvesterinfo = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]

        cur.execute(sqlquerycomputingsite)
        harvesterids = cur.fetchall()

        cur.execute(sqlquerystatus)
        statuses = cur.fetchall()

        cur.execute(sqlqueryresource)
        resourcetypes = cur.fetchall()

        cur.execute(sqlqueryce)
        computingelements = cur.fetchall()

        harvesteridDict = {}
        for harvester in harvesterids:
            harvesteridDict[harvester[0]] = harvester[1]

        statusesDict = {}
        for status in statuses:
            statusesDict[status[0]] = status[1]

        resourcetypesDict = {}
        for resourcetype in resourcetypes:
            resourcetypesDict[resourcetype[0]] = resourcetype[1]

        computingelementsDict = {}

        for computingelement in computingelements:
            computingelementsDict[computingelement[0]] = computingelement[1]

        for worker in harvesterinfo:
            object = dict(zip(columns, worker))
            workersList.append(object)

        if len(workersList) == 0 or len(harvesteridDict) == 0:
            message ="""Computingsite is not found OR no workers for this computingsite or time period. Try using this <a href =/harvesters/?computingsite=%s&days=365>link (last 365 days)</a>""" % (computingsite)
            return HttpResponse(json.dumps({'message':  message}),
                            content_type='text/html')
        generalInstanseInfo = {'Computingsite':workersList[0]['computingsite'], 'Description':workersList[0]['description'], 'Starttime': workersList[0]['insstarttime'],
                                      'Owner':workersList[0]['owner'], 'Hostname':workersList[0]['hostname'],'Lastupdate':workersList[0]['inslastupdate'], 'Harvesters':harvesteridDict,'Statuses':statusesDict, 'Resourcetypes':resourcetypesDict,'Computingelements':computingelementsDict,'Software version':workersList[0]['sw_version'],'Commit stamp':workersList[0]['commit_stamp']
        }
        request.session['viewParams']['selection'] =  'Harvester workers, last %s hours' %(defaulthours)

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

    elif 'pandaid' in request.session['requestParams'] and 'computingsite' not in request.session['requestParams'] and 'instance' not in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
        workerid = ''
        days = ''
        defaulthours = 24
        resourcetype = ''
        computingelement = ''
        status = ''
        jobsworkersquery, pandaids = getWorkersByJobID(pandaid)

        if jobsworkersquery == '':
            message = """No workers for this pandaid or time period. Try using this <a href =/harvesters/?pandaid=%s&days=365>link (last 365 days)</a>""" % (
                    pandaid)
            return HttpResponse(json.dumps({'message': message}),
                                    content_type='text/html')
        URL += '?pandaid=' + request.session['requestParams']['pandaid']
        if 'hours' in request.session['requestParams']:
            defaulthours = request.session['requestParams']['hours']
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """AND submittime  > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ % (
            request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
            hours = ''
            defaulthours = int(request.session['requestParams']['days']) * 24
        if hours != '':
            jobsworkersquery += ' ' + hours
        if days != '':
            jobsworkersquery += ' ' + days
        if 'status' in request.session['requestParams']:
            status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
            URL += '&status=' + str(request.session['requestParams']['status'])
        if 'workerid' in request.session['requestParams']:
            workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'resourcetype' in request.session['requestParams']:
            resourcetype = """AND resourcetype like '%s'""" % (str(request.session['requestParams']['resourcetype']))
            URL += '&resourcetype=' + str(request.session['requestParams']['resourcetype'])
        if 'computingelement' in request.session['requestParams']:
            computingelement = """AND computingelement like '%s'""" %(str(request.session['requestParams']['computingelement']))
            URL += '&computingelement=' + str(request.session['requestParams']['computingelement'])
        # sqlquery = """
        #   SELECT * FROM ATLAS_PANDABIGMON.HARVESTERWORKERS
        #   where computingsite like '%s' %s %s %s %s and ROWNUM<=1
        #   order by workerid DESC
        #   """ % (str(computingsite),status, workerid, resourcetype,computingelement)
        sqlharvester = """
          SELECT harvesterid,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where (%s) %s %s %s %s group by harvesterid
          """ % (jobsworkersquery, status,  workerid, resourcetype, computingelement)

        sqlquerystatus = """
          SELECT status,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where (%s) %s %s %s %s group by status
          """ % (jobsworkersquery, status,  workerid, resourcetype, computingelement)

        sqlqueryresource = """
        SELECT RESOURCETYPE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where (%s) %s %s %s %s group by RESOURCETYPE
        """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        sqlqueryce = """
        SELECT COMPUTINGELEMENT,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where (%s) %s %s %s %s group by COMPUTINGELEMENT
        """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        sqlquerycomputingsite = """
           SELECT COMPUTINGSITE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
           where (%s) %s %s %s %s  group by COMPUTINGSITE
           """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        cur = connection.cursor()

        cur.execute(sqlharvester)
        harvesterids = cur.fetchall()

        cur.execute(sqlquerystatus)
        statuses = cur.fetchall()

        cur.execute(sqlqueryresource)
        resourcetypes = cur.fetchall()

        cur.execute(sqlqueryce)
        computingelements = cur.fetchall()

        cur.execute(sqlquerycomputingsite)
        computingsites = cur.fetchall()

        harvesteridDict = {}
        for harvester in harvesterids:
            harvesteridDict[harvester[0]] = harvester[1]

        if len(harvesteridDict) == 0:
            message = """No workers for this pandaid or time period. Try using this <a href =/harvesters/?pandaid=%s&days=365>link (last 365 days)</a>""" % (
                    pandaid)
            return HttpResponse(json.dumps({'message': message}),
                                    content_type='text/html')

        computingsitesDict = {}
        for computingsite in computingsites:
            computingsitesDict[computingsite[0]] = computingsite[1]

        statusesDict = {}
        for status in statuses:
            statusesDict[status[0]] = status[1]

        resourcetypesDict = {}
        for resourcetype in resourcetypes:
            resourcetypesDict[resourcetype[0]] = resourcetype[1]

        computingelementsDict = {}
        for computingelement in computingelements:
            computingelementsDict[computingelement[0]] = computingelement[1]

        for harvester in pandaids.keys():
            if harvester not in harvesteridDict.keys():
                del pandaids[harvester]

        generalInstanseInfo = {'JobID' : ' '.join(pandaids.values()), 'Harvesters' : harvesteridDict, 'Statuses' : statusesDict, 'Resourcetypes' : resourcetypesDict, 'Computingelements' : computingelementsDict, 'Computingsites': computingsitesDict}
        request.session['viewParams']['selection'] = 'Harvester workers, last %s hours' %(defaulthours)

        data = {
                'generalInstanseInfo':generalInstanseInfo,
                'type':'workers',
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
      SELECT HARVESTER_ID as HARVID,
      SW_VERSION,
      DESCRIPTION,
      COMMIT_STAMP,
      to_char(LASTUPDATE, 'dd-mm-yyyy hh24:mi:ss') as LASTUPDATE
      FROM ATLAS_PANDA.HARVESTER_INSTANCES
        """
        instanceDictionary = []
        cur = connection.cursor()
        cur.execute(sqlquery)

        for instance in cur:
            instanceDictionary.append(
                {'instance': instance[0], 'sw_version':instance[1],'commit_stamp':instance[2], 'descr': instance[3],'lastupdate':instance[4]})
        request.session['viewParams']['selection'] = 'Harvester instances'
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

def getHarvesterJobs(request, instance = '', workerid = '', jobstatus=''):
    pandaidsList = []
    qinstance = ''
    qworkerid = ''
    qjobstatus =''
    if instance != '':
        if instance.islower() == 'all':
           qinstance = 'is not null'
           if jobstatus != '':
              qjobstatus += " and jobstatus like '" + jobstatus +"'"
           else: qjobstatus += " and jobstatus like 'running'"
        else:
            qinstance = "=" + "'"+instance+"'"
            if jobstatus != '':
                qjobstatus += " and jobstatus in ('" + jobstatus + "')"
    else:
        qinstance = 'is not null'
    if workerid != '':
        qworkerid = '=' + str(workerid)
    else:
        qworkerid = 'is not null'

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        from core.pandajob.models import Jobsactive4
        values = [f.name for f in Jobsactive4._meta.get_fields()]
    else:
        values = 'corecount','jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss', 'maxrss', 'nucleus', 'eventservice', 'nevents','gshare','noutputdatafiles','parentid','actualcorecount','schedulerid'

    sqlRequest = '''
    SELECT DISTINCT {2} FROM
    (SELECT {2}  FROM ATLAS_PANDA.JOBSARCHIVED4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1}) 
    PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSARCHIVED4.PANDAID {3}
    UNION ALL
    SELECT {2}  FROM ATLAS_PANDA.JOBSACTIVE4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSACTIVE4.PANDAID  {3}
    UNION ALL 
    SELECT {2}  FROM ATLAS_PANDA.JOBSDEFINED4, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSDEFINED4.PANDAID  {3}
    UNION ALL 
    SELECT {2} FROM ATLAS_PANDA.JOBSWAITING4,
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1})  PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSWAITING4.PANDAID  {3}
    UNION ALL
    SELECT {2} FROM ATLAS_PANDAARCH.JOBSARCHIVED, 
    (select
    pandaid as pid
    from atlas_panda.harvester_rel_jobs_workers where
    atlas_panda.harvester_rel_jobs_workers.harvesterid {0} and atlas_panda.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid=ATLAS_PANDAARCH.JOBSARCHIVED.PANDAID {3})  
        '''
    sqlRequestFull = sqlRequest.format(qinstance, qworkerid, ', '.join(values), qjobstatus)
    cur = connection.cursor()
    cur.execute(sqlRequestFull)
    pandaids = cur.fetchall()
    columns = [str(column[0]).lower() for column in cur.description]
    for pid in pandaids:
        pandaidsList.append(dict(zip(columns, pid)))
    return pandaidsList

def isharvesterjob(pandaid):
    jobHarvesterInfo = []
    sqlRequest = '''
    SELECT workerid,HARVESTERID, BATCHLOG, COMPUTINGELEMENT FROM (SELECT 
  a.PANDAID,
  a.workerid,
  a.HARVESTERID,
  b.BATCHLOG,
  b.COMPUTINGELEMENT
  FROM ATLAS_PANDA.HARVESTER_REL_JOBS_WORKERS a,
  ATLAS_PANDA.HARVESTER_WORKERS b
  WHERE a.harvesterid = b.harvesterid and a.workerid = b.WORKERID) where pandaid = {0}
  '''
    sqlRequestFull = sqlRequest.format(str(pandaid))
    cur = connection.cursor()
    cur.execute(sqlRequestFull)
    job = cur.fetchall()
    if len(job) == 0:
        return False
    columns = [str(column[0]).lower() for column in cur.description]
    for pid in job:
        jobHarvesterInfo.append(dict(zip(columns, pid)))
    return jobHarvesterInfo

def workersJSON(request):
    valid, response = initRequest(request)

    status = ''
    computingsite = ''
    workerid = ''
    days = ''
    defaulthours = 24
    lastupdateCache = ''
    resourcetype = ''
    computingelement = ''
    if 'status' in request.session['requestParams']:
        status = """AND status like '%s'""" % (str(request.session['requestParams']['status']))
    if 'computingsite' in request.session['requestParams']:
        computingsite = """AND computingsite like '%s'""" % (
            str(request.session['requestParams']['computingsite']))
    if 'workerid' in request.session['requestParams']:
        workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
    if 'hours' in request.session['requestParams']:
        defaulthours = request.session['requestParams']['hours']
        hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
            defaulthours)
    else: hours = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) """ % (
        defaulthours)
    if 'days' in request.session['requestParams']:
        days = """AND submittime > sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) """ % (
            request.session['requestParams']['days'])
        hours = ''
        defaulthours = int(request.session['requestParams']['days']) * 24
    if 'resourcetype' in request.session['requestParams']:
        resourcetype = """AND resourcetype like '%s'""" % (
            str(request.session['requestParams']['resourcetype']))
    if 'computingelement' in request.session['requestParams']:
        computingelement = """AND computingelement like '%s'""" % (
            str(request.session['requestParams']['computingelement']))

    if 'instance' in request.session['requestParams']:
        instance = request.session['requestParams']['instance']
        if 'pandaid' in request.session['requestParams']:
            pandaid = request.session['requestParams']['pandaid']
            jobsworkersquery, pandaids = getWorkersByJobID(pandaid,instance)
            workerid = """AND workerid in (%s)""" % (jobsworkersquery)
        if ('dt' in request.session['requestParams']):
            if 'display_limit_workers' in request.session['requestParams']:
                display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
            else:
                display_limit_workers = 1000

            generalWorkersFields = ['workerid', 'status', 'batchid', 'nodeid', 'queuename', 'computingsite','harvesterid',
                                'submittime', 'lastupdate', 'starttime', 'endtime', 'ncore', 'errorcode',
                                'stdout', 'stderr', 'batchlog', 'resourcetype', 'nativeexitcode', 'nativestatus',
                                'diagmessage', 'njobs', 'computingelement']

            fields = ','.join(generalWorkersFields)
            sqlquery = """
            SELECT * FROM(SELECT %s FROM ATLAS_PANDA.HARVESTER_WORKERS
            where harvesterid like '%s' %s %s %s %s %s %s %s %s
            order by submittime DESC) WHERE ROWNUM<=%s
            """ % (fields, str(instance), status, computingsite, workerid, lastupdateCache, days, hours, resourcetype,computingelement, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)
            return HttpResponse(json.dumps(workersList,cls=DateTimeEncoder), content_type='text/html')

    elif 'computingsite' in request.session['requestParams'] and 'instance' not in request.session['requestParams']:
        computingsite = request.session['requestParams']['computingsite']
        if ('dt' in request.session['requestParams']):
            if 'display_limit_workers' in request.session['requestParams']:
                display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
            else:
                display_limit_workers = 1000

            generalWorkersFields = ['workerid', 'status', 'batchid', 'nodeid', 'queuename', 'computingsite','harvesterid',
                                    'submittime', 'lastupdate', 'starttime', 'endtime', 'ncore', 'errorcode',
                                    'stdout', 'stderr', 'batchlog', 'resourcetype', 'nativeexitcode', 'nativestatus',
                                    'diagmessage', 'njobs', 'computingelement']

            fields = ','.join(generalWorkersFields)
            sqlquery = """
             SELECT * FROM(SELECT %s FROM ATLAS_PANDA.HARVESTER_WORKERS
             where computingsite like '%s' %s %s %s %s %s %s
             order by  submittime  DESC) WHERE ROWNUM<=%s
             """ % (
            fields, str(computingsite), status,  workerid, days, hours, resourcetype,computingelement, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)

            return HttpResponse(json.dumps(workersList,cls=DateTimeEncoder), content_type='text/html')

    elif 'pandaid' in request.session['requestParams'] and 'computingsite' not in request.session[
        'requestParams'] and 'instance' not in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
        jobsworkersquery, pandaids = getWorkersByJobID(pandaid)
        if hours != '':
            jobsworkersquery += ' ' + hours
        if days != '':
            jobsworkersquery += ' ' + days

        if ('dt' in request.session['requestParams']):
            if 'display_limit_workers' in request.session['requestParams']:
                display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
            else:
                display_limit_workers = 1000

            generalWorkersFields = ['workerid', 'status', 'batchid', 'nodeid', 'queuename', 'computingsite','harvesterid',
                                    'submittime', 'lastupdate', 'starttime', 'endtime', 'ncore', 'errorcode',
                                    'stdout', 'stderr', 'batchlog', 'resourcetype', 'nativeexitcode', 'nativestatus',
                                    'diagmessage', 'njobs', 'computingelement']

            fields = ','.join(generalWorkersFields)
            sqlquery = """
                SELECT * FROM(SELECT %s FROM ATLAS_PANDA.HARVESTER_WORKERS
                where (%s) %s %s %s %s
                order by submittime DESC) WHERE ROWNUM<=%s
                """ % (fields, jobsworkersquery, status,  workerid, resourcetype,computingelement, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)
            return HttpResponse(json.dumps(workersList, cls=DateTimeEncoder), content_type='text/html')

@login_customrequired
def harvesterslots(request):
    valid, response = initRequest(request)
    harvesterslotsList = []
    harvesterslots=HarvesterSlots.objects.values('pandaqueuename','gshare','resourcetype','numslots','modificationtime','expirationtime')
    old_format = '%Y-%m-%d %H:%M:%S'
    new_format = '%d-%m-%Y %H:%M:%S'
    for slot in harvesterslots:
        slot['modificationtime'] = datetime.strptime(str(slot['modificationtime']), old_format).strftime(new_format)
        if slot['expirationtime'] is not None:
            slot['expirationtime'] = datetime.strptime(str(slot['expirationtime']), old_format).strftime(new_format)
        harvesterslotsList.append(slot)
    xurl = extensibleURL(request)
    data = {
        'harvesterslots': harvesterslotsList,
        'type': 'workers',
        'xurl': xurl,
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'built': datetime.now().strftime("%H:%M:%S"),

    }
    endSelfMonitor(request)
    return render_to_response('harvesterslots.html', data, content_type='text/html')

def getWorkersByJobID(pandaid, instance=''):
    instancequery = ''
    if '|' in pandaid:
        pandaid = 'where pandaid in (' + pandaid.replace('|', ',') + ')'
    elif ',' in pandaid:
        pandaid = 'where pandaid in (' + pandaid + ')'
    else:
        pandaid = 'where pandaid = ' + pandaid

    if instance !='':
       instancequery = """ AND harvesterid like '%s' """ %(instance)

    sqlquery = """
    select harvesterid, workerid, pandaid from atlas_panda.Harvester_Rel_Jobs_Workers %s %s
    """ % (pandaid, instancequery)

    cur = connection.cursor()
    cur.execute(sqlquery)

    reljobsworkers = cur.fetchall()
    workersList = {}
    pandaidList = {}
    for worker in reljobsworkers:
        workersList.setdefault(worker[0], []).append(str(worker[1]))
        pandaidList[worker[0]] = str(worker[2])
    jobsworkersquery = ''
    instances = workersList.keys()
    cntinstances = len(instances)

    if instance != '':
        jobsworkersquery = ', '.join(workersList[instance])

    else:
        for instance in instances:
            jobsworkersquery += 'harvesterid like \'{}\' and workerid in ({})'.format(instance,', '.join(workersList[instance]))
            if cntinstances > 1:
                jobsworkersquery += ' OR '
                cntinstances = cntinstances - 1
    return jobsworkersquery, pandaidList