import json
import math
import collections
import logging
import time
from collections import Counter

from datetime import datetime, timedelta

from django.db import connection

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

from django.utils.cache import patch_response_headers

from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import is_timestamp
from core.libs.sqlcustom import escape_input
from core.libs.DateEncoder import DateEncoder
from core.libs.DateTimeEncoder import DateTimeEncoder
from core.oauth.utils import login_customrequired
from core.utils import is_json_request, removeParam
from core.views import initRequest, extensibleURL
from core.harvester.models import HarvesterWorkers, HarvesterDialogs, HarvesterWorkerStats, HarvesterSlots, \
    HarvesterInstances, HarvesterRelJobsWorkers
from core.harvester.utils import get_harverster_workers_for_task, setup_harvester_view

from django.conf import settings

harvesterWorkerStatuses = [
    'missed', 'submitted', 'ready', 'running', 'idle', 'finished', 'failed', 'cancelled'
]

_logger = logging.getLogger('bigpandamon')


@login_customrequired
def harvesterInstances(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # here we get cache
    data = getCacheEntry(request, "harvesterInstances")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'harvesterInstances.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    xurl = extensibleURL(request)
    iquery = {}
    instances = list(HarvesterInstances.objects.filter(**iquery).values())

    request.session['viewParams']['selection'] = 'Harvester instances'
    data = {
        'instances': list(instances),
        'type': 'instances',
        'xurl': xurl,
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams']
    }
    if not is_json_request(request):
        return render(request, 'harvesterInstances.html', data, content_type='text/html')
    else:
        return JsonResponse({'instances': instances}, encoder=DateTimeEncoder, safe=False)


@login_customrequired
def harvesterWorkers(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=400)

    # data = None
    data = getCacheEntry(request, "harvesterWorkers")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'harvesterWorkers.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    warning = {}
    xurl = extensibleURL(request)
    url = removeParam(removeParam(xurl, 'hours', 'extensible'), 'days', 'extensible')

    # getting workers
    wquery, extra = setup_harvester_view(request, 'worker')
    # check if there are workers in the timewindow, if not, search for last submitted worker and extend timewindow
    n_workers = HarvesterWorkers.objects.filter(**wquery).extra(where=[extra]).count()
    if n_workers == 0:
        del wquery['submittime__range']
        try:
            last_submission_time = HarvesterWorkers.objects.filter(**wquery).latest('submittime').submittime
        except:
            print("no workers at all")
            data = {
                'warning': 'No workers have been find for the instance',
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
            }
            return render(request, 'harvesterWorkers.html', data, content_type='text/html')

        warning['timewindow'] = """The time window was extended to the last submitted worker 
                        since no workers have been found for specified or default time period"""
        wquery['submittime__range'] = [
            (last_submission_time - timedelta(hours=1)).strftime(settings.DATETIME_FORMAT),
            datetime.now().strftime(settings.DATETIME_FORMAT)
        ]

    timewindow = (datetime.strptime(wquery['submittime__range'][1], settings.DATETIME_FORMAT) -
                  datetime.strptime(wquery['submittime__range'][0], settings.DATETIME_FORMAT))
    timewindow_sec = timewindow.days * 24 * 60 * 60 + timewindow.seconds
    if math.ceil(timewindow_sec / 3600.0) <= 24:
        request.session['viewParams']['selection'] = 'Harvester workers, last {} hours'.format(
            int(math.ceil(timewindow_sec / 3600.0)))
        url += '&hours=' + str(int(math.ceil(timewindow_sec / 3600.0)))
    else:
        request.session['viewParams']['selection'] = 'Harvester workers, last {} days'.format(
            int(math.ceil(timewindow_sec / 3600.0 / 24.0)))
        url += '&days=' + str(int(math.ceil(timewindow_sec / 3600.0 / 24.0)))

    worker_list = []
    worker_list.extend(list(HarvesterWorkers.objects.filter(**wquery).extra(where=[extra]).values()))
    _logger.debug('Got workers: {}'.format(time.time() - request.session['req_init_time']))

    # making attribute summary
    sumd = {}
    worker_params_to_count = [
        'status',
        'computingsite',
        'computingelement',
        'resourcetype',
        'harvesterid',
        'nativestatus',
        'jobtype'
    ]
    for wp in worker_params_to_count:
        sumd[wp] = dict(Counter(worker[wp] for worker in worker_list))
    sumd = collections.OrderedDict(sumd)

    if is_json_request(request):
        data = {
            'summary': sumd,
            'workers': worker_list,
        }
        response = JsonResponse(data, encoder=DateTimeEncoder, safe=False)
    else:
        # dict -> list for template
        suml = []
        for field, attr_summary in sumd.items():
            tmp_dict = {'field': field, 'attrs': []}
            if isinstance(attr_summary, dict) and len(attr_summary) > 0:
                tmp_dict['attrs'] = [[attr, count] for attr, count in attr_summary.items()]
            suml.append(tmp_dict)
        suml = sorted(suml, key=lambda x: x['field'])

        # prepare harvester info if instance is specified
        instance = None
        harvester_info = {}
        if 'instance' in request.session['requestParams']:
            instance = request.session['requestParams']['instance']
        elif 'harvesterid' in request.session['requestParams']:
            instance = request.session['requestParams']['harvesterid']
        else:
            # check if all found workers are from the same instance
            if 'harvesterid' in sumd and len(sumd['harvesterid']) == 1:
                instance = list(sumd['harvesterid'].keys())[0]
                url += '&harvesterid={}'.format(instance)
        if instance:
            iquery = {'harvesterid': instance}
            instance_params_to_show = [
                'harvesterid',
                'description',
                'starttime',
                'owner',
                'hostname',
                'lastupdate',
                'swversion',
                'commitstamp'
            ]
            harvester_info = HarvesterInstances.objects.filter(**iquery).values(*instance_params_to_show)[0]
            _logger.debug('Got instance: {}'.format(time.time() - request.session['req_init_time']))
            for datetime_field in ('lastupdate', 'submittime', 'starttime', 'endtime'):
                if datetime_field in harvester_info and isinstance(harvester_info[datetime_field], datetime):
                    harvester_info[datetime_field] = harvester_info[datetime_field].strftime(settings.DATETIME_FORMAT)
        _logger.debug('Finished preprocessing: {}'.format(time.time() - request.session['req_init_time']))

        data = {
            'harvesterinfo': harvester_info,
            'nworkers': len(worker_list),
            'suml': suml,
            'type': 'workers',
            'instance': instance,
            'computingsite': 0,
            'xurl': xurl,
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'url': '?' + url.split('?')[1] if '?' in url else '?' + url
        }
        setCacheEntry(request, "harvesterWorkers", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render(request, 'harvesterWorkers.html', data, content_type='text/html')
        _logger.debug('Rendered: {}'.format(time.time() - request.session['req_init_time']))
    return response


@login_customrequired
def harvesterWorkerInfo(request, workerid=None):
    valid, response = initRequest(request)
    if not valid:
        return response

    if workerid is None:
        if 'workerid' in request.session['requestParams']:
            workerid = request.session['requestParams']['workerid']
            try:
                workerid = int(workerid)
            except:
                _logger.exception('Provided workerid is not integer')

    harvesterid = None
    if 'harvesterid' in request.session['requestParams']:
        harvesterid = escape_input(request.session['requestParams']['harvesterid'])
    elif 'instance' in request.session['requestParams']:
        harvesterid = escape_input(request.session['requestParams']['instance'])

    workerinfo = {}
    error = None
    if harvesterid and workerid:
        workerslist = []
        tquery = {'harvesterid': harvesterid, 'workerid': workerid}
        workerslist.extend(HarvesterWorkers.objects.filter(**tquery).values())

        if len(workerslist) > 0:
            workerinfo = workerslist[0]
            workerinfo['corrJobs'] = []
            workerinfo['jobsStatuses'] = {}
            workerinfo['jobsSubStatuses'] = {}

            jobs = getHarvesterJobs(request, instance=harvesterid, workerid=workerid)

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
            for k, v in list(workerinfo.items()):
                if is_timestamp(k):
                    try:
                        val = v.strftime(settings.DATETIME_FORMAT)
                        workerinfo[k] = val
                    except:
                        pass
        else:
            workerinfo = None
    else:
        error = "harvesterid or/and workerid not specified"

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
    if is_json_request(request):
        return HttpResponse(json.dumps(data['workerinfo'], cls=DateEncoder), content_type='application/json')
    else:
        response = render(request, 'harvesterWorkerInfo.html', data, content_type='text/html')
        return response


# API views for dataTables in harvesterWorkerList page

def get_harvester_workers(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    xurl = extensibleURL(request)
    if '_' in request.session['requestParams']:
        xurl = xurl.replace('_={0}&'.format(request.session['requestParams']['_']), '')

    # data = None
    data = getCacheEntry(request, xurl, isData=True)
    if data is not None:
        data = json.loads(data)
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    if 'dt' in request.session['requestParams']:
        if 'display_limit_workers' in request.session['requestParams']:
            display_limit_workers = int(request.session['requestParams']['display_limit_workers'])
        else:
            display_limit_workers = 1000

        worker_list = []
        wquery, extra = setup_harvester_view(request, 'worker')
        worker_list.extend(list(HarvesterWorkers.objects.filter(**wquery).extra(where=[extra]).order_by('-lastupdate')[:display_limit_workers].values()))

        if 'key' not in request.session['requestParams']:
            setCacheEntry(request, xurl, json.dumps(worker_list, cls=DateTimeEncoder), 60 * 20, isData=True)

        return HttpResponse(json.dumps(worker_list, cls=DateTimeEncoder), content_type='application/json')
    else:
        return HttpResponse(status=400)


def get_harvester_diagnostics(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    dquery, extra = setup_harvester_view(request, 'dialog')

    limit = 100
    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])

    dialogs_list = []
    values = ['creationtime', 'harvesterid', 'modulename', 'messagelevel', 'diagmessage']
    dialogs_list.extend(HarvesterDialogs.objects.filter(**dquery).order_by('-creationtime')[:limit].values(*values))

    return HttpResponse(json.dumps(dialogs_list, cls=DateTimeEncoder), content_type='application/json')


def get_harvester_worker_stats(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=400)

    wquery, extra = setup_harvester_view(request, 'workerstat')
    harvsterworkerstats = []
    wvalues = ('harvesterid', 'computingsite', 'resourcetype', 'status', 'nworkers', 'lastupdate')
    harvsterworkerstats.extend(HarvesterWorkerStats.objects.filter(**wquery).values(*wvalues).order_by('-lastupdate'))

    return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='application/json')


def get_harvester_jobs(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=400)

    jquery, extra = setup_harvester_view(request, 'jobs')

    harvsterpandaids = []
    limit = 1000
    if 'limit' in request.session['requestParams']:
        try:
            limit = int(request.session['requestParams']['limit'])
        except Exception as ex:
            _logger.exception('Provided limit is not int, we will use default={} instead\n{}'.format(limit, ex))

    jvalues = ('harvesterid', 'workerid', 'pandaid', 'lastupdate')
    harvsterpandaids.extend(HarvesterRelJobsWorkers.objects.filter(**jquery).extra(where=[extra]).values(*jvalues).order_by('-lastupdate')[:limit])

    return HttpResponse(json.dumps(harvsterpandaids, cls=DateTimeEncoder), content_type='application/json')






















#
#
# @login_customrequired
# def harvesterWorkerInfo(request):
#     valid, response = initRequest(request)
#     if not valid:
#         return response
#
#     harvesterid = None
#     workerid = None
#     workerinfo = {}
#     workerslist = []
#     error = None
#
#     if 'harvesterid' in request.session['requestParams']:
#         harvesterid = escape_input(request.session['requestParams']['harvesterid'])
#     if 'workerid' in request.session['requestParams']:
#         workerid = int(request.session['requestParams']['workerid'])
#
#     if harvesterid and workerid:
#         tquery = {'harvesterid': harvesterid, 'workerid': workerid}
#         workerslist.extend(HarvesterWorkers.objects.filter(**tquery).values())
#
#         if len(workerslist) > 0:
#             workerinfo = workerslist[0]
#             workerinfo['corrJobs'] = []
#             workerinfo['jobsStatuses'] = {}
#             workerinfo['jobsSubStatuses'] = {}
#
#             jobs = getHarvesterJobs(request, instance=harvesterid, workerid=workerid)
#
#             for job in jobs:
#                 workerinfo['corrJobs'].append(job['pandaid'])
#                 if job['jobstatus'] not in workerinfo['jobsStatuses']:
#                     workerinfo['jobsStatuses'][job['jobstatus']] = 1
#                 else:
#                     workerinfo['jobsStatuses'][job['jobstatus']] += 1
#                 if job['jobsubstatus'] not in workerinfo['jobsSubStatuses']:
#                     workerinfo['jobsSubStatuses'][job['jobsubstatus']] = 1
#                 else:
#                     workerinfo['jobsSubStatuses'][job['jobsubstatus']] += 1
#             for k, v in workerinfo.items():
#                 if is_timestamp(k):
#                     try:
#                         val = v.strftime(settings.DATETIME_FORMAT)
#                         workerinfo[k] = val
#                     except:
#                         pass
#         else:
#             workerinfo = None
#     else:
#         error = "Harvesterid + Workerid is not specified"
#
#     data = {
#         'request': request,
#         'error': error,
#         'workerinfo': workerinfo,
#         'harvesterid': harvesterid,
#         'workerid': workerid,
#         'viewParams': request.session['viewParams'],
#         'requestParams': request.session['requestParams'],
#         'built': datetime.now().strftime("%H:%M:%S"),
#     }
#     if is_json_request(request):
#         return HttpResponse(json.dumps(data['workerinfo'], cls=DateEncoder), content_type='application/json')
#     else:
#         response = render(request, 'harvworkerinfo.html', data, content_type='text/html')
#         return response

@login_customrequired
def harvestermon(request):

    valid, response = initRequest(request)

    data = getCacheEntry(request, "harvester")

    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'harvestermon.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    extra = '1=1'
    xurl = extensibleURL(request)

    URL = ''

    if 'instance' in request.session['requestParams']:
        instance = request.session['requestParams']['instance']

        if ('workersstats' in request.session['requestParams'] and 'instance' in request.session['requestParams']):
            harvsterworkerstats = []
            tquery = {}
            tquery['harvesterid'] = instance
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = int(request.session['requestParams']['limit'])
            harvsterworkerstat = HarvesterWorkerStats.objects.filter(**tquery).values('computingsite', 'resourcetype', 'status',
                                                                           'nworkers','lastupdate').extra(
                where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for stat in harvsterworkerstat:
                stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
                harvsterworkerstats.append(stat)
            return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='application/json')

        if ('pandaids' in request.session['requestParams'] and 'instance' in request.session['requestParams']):

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
                hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE)""" % (
                    defaulthours)
            else:
                hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
                    defaulthours)
            if 'days' in request.session['requestParams']:
                days = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) AS DATE) """ % (
                    request.session['requestParams']['days'])
                hours = ''
                defaulthours = int(request.session['requestParams']['days']) * 24

            harvsterpandaids = []

            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']

            sqlQueryJobsStates = """
                select hw.*, cj.jobstatus from (
                    select * from {}.harvester_rel_jobs_workers 
                        where harvesterid like '{}'
                            and workerid in (
                              select workerid from (
                                select workerid from {}.harvester_workers
                                    where harvesterid like '{}' {} {} {} {} {} {} {}
                                    order by lastupdate desc
                                )
                              where rownum <= {} 
                              )
                    ) hw , {}.combined_wait_act_def_arch4 cj
                where hw.pandaid = cj.pandaid 
                """.format(
                settings.DB_SCHEMA_PANDA, str(instance), settings.DB_SCHEMA_PANDA,
                str(instance), status, computingsite, workerid, days, hours, resourcetype, computingelement,
                limit, settings.DB_SCHEMA)

            cur = connection.cursor()
            cur.execute(sqlQueryJobsStates)
            jobs = cur.fetchall()
            columns = [str(i[0]).lower() for i in cur.description]
            for job in jobs:
                object = dict(zip(columns, job))
                harvsterpandaids.append(object)

            return HttpResponse(json.dumps(harvsterpandaids, cls=DateTimeEncoder), content_type='application/json')

        if 'dialogs' in request.session['requestParams'] and 'instance' in request.session['requestParams']:
            dialogs = []
            tquery = {}
            tquery['harvesterid'] = instance
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = int(request.session['requestParams']['limit'])
            dialogsList = HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime')[:limit]
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for dialog in dialogsList:
                dialog['creationtime'] = datetime.strptime(str(dialog['creationtime']), old_format).strftime(new_format)
                dialogs.append(dialog)

            return HttpResponse(json.dumps(dialogs, cls=DateTimeEncoder), content_type='application/json')

        lastupdateCache = ''

        URL += '?instance=' + request.session['requestParams']['instance']
        status = ''
        computingsite = ''
        workerid = ''
        days = ''
        defaulthours = 24
        resourcetype = ''
        computingelement = ''

        if 'status' in request.session['requestParams']:
            status = """and status like '%s'""" %(str(request.session['requestParams']['status']))
            URL += '&status=' + str(request.session['requestParams']['status'])
        if 'computingsite' in request.session['requestParams']:
            computingsite = """and computingsite like '%s'""" %(str(request.session['requestParams']['computingsite']))
            URL += '&computingsite=' + str(request.session['requestParams']['computingsite'])
        if 'pandaid' in request.session['requestParams']:
            pandaid = request.session['requestParams']['pandaid']
            try:
                jobsworkersquery, pandaids = getWorkersByJobID(pandaid, request.session['requestParams']['instance'])
            except:
                message = """pandaid for this instance is not found"""
                return HttpResponse(json.dumps({'message': message}), content_type='text/html')
            workerid = """and workerid in (%s)""" % (jobsworkersquery)
            URL += '&pandaid=' + str(request.session['requestParams']['pandaid'])
        if 'resourcetype' in request.session['requestParams']:
            resourcetype = """and resourcetype like '%s'""" %(str(request.session['requestParams']['resourcetype']))
            URL += '&resourcetype=' +str(request.session['requestParams']['resourcetype'])
        if 'computingelement' in request.session['requestParams']:
            computingelement = """and computingelement like '%s'""" %(str(request.session['requestParams']['computingelement']))
            URL += '&computingelement=' + str(request.session['requestParams']['computingelement'])
        if 'workerid' in request.session['requestParams']:
            workerid = """and workerid in (%s)""" %(request.session['requestParams']['workerid'])
            URL += '&workerid=' + str(request.session['requestParams']['workerid'])
        if 'hours' in request.session['requestParams']:
            defaulthours = request.session['requestParams']['hours']
            hours = """and submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """and submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval  '%s' hour(3) AS DATE) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """and submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) AS DATE) """ %(request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
            hours = ''
            defaulthours = int(request.session['requestParams']['days']) * 24

        sqlQuery = """
            SELECT
                ii.harvester_id,
                ii.description,
                to_char(ii.starttime, 'dd-mm-yyyy hh24:mi:ss') as starttime,
                to_char(ii.lastupdate, 'dd-mm-yyyy hh24:mi:ss') as lastupdate,
                ii.owner,
                ii.hostname,
                ii.sw_version,
                ii.commit_stamp,
                to_char(ww.submittime, 'dd-mm-yyyy hh24:mi:ss') as submittime
            FROM
                {2}.harvester_instances ii 
                INNER JOIN 
                {2}.harvester_workers ww on ww.harvesterid = ii.harvester_id {0} and ii.harvester_id like '{1}'
        """.format(hours, str(instance), settings.DB_SCHEMA_PANDA)

        cur = connection.cursor()
        cur.execute(sqlQuery)
        qinstanceinfo = cur.fetchall()
        columns = [str(i[0]).lower() for i in cur.description]
        instanceinfo = {}
        for info in qinstanceinfo:
            instanceinfo = dict(zip(columns, info))

        if len(qinstanceinfo) == 0:
            sqlQuery = """
            SELECT
            ii.harvester_id,
            ii.description,
            to_char(ii.starttime, 'dd-mm-yyyy hh24:mi:ss') as starttime,
            to_char(ii.lastupdate, 'dd-mm-yyyy hh24:mi:ss') as lastupdate,
            ii.owner,
            ii.hostname,
            ii.sw_version,
            ii.commit_stamp,
            to_char(ww.submittime, 'dd-mm-yyyy hh24:mi:ss') as submittime
            FROM
            {1}.harvester_instances ii INNER JOIN 
            {1}.harvester_workers ww on ww.harvesterid = ii.harvester_id and ww.submittime = (select max(submittime) 
        from {1}.harvester_workers 
        where harvesterid like '{0}') and ii.harvester_id like '{0}'
            """.format(str(instance), settings.DB_SCHEMA_PANDA)

            cur = connection.cursor()
            cur.execute(sqlQuery)
            qinstanceinfo = cur.fetchall()
            columns = [str(i[0]).lower() for i in cur.description]

            for info in qinstanceinfo:
                instanceinfo = dict(zip(columns, info))

            if bool(instanceinfo) != True or instanceinfo['submittime'] is None:
                message = """Instance is not found OR no workers for this instance or time period"""
                return HttpResponse(json.dumps({'message': message}),
                                    content_type='text/html')
        _logger.debug('Got instance: {}'.format(time.time() - request.session['req_init_time']))

        if datetime.strptime(instanceinfo['submittime'], '%d-%m-%Y %H:%M:%S') < datetime.now() - timedelta(hours=24):
            days = """AND submittime > CAST(TO_DATE('{0}', 'dd-mm-yyyy hh24:mi:ss') - interval '{1}' day AS DATE)""".format(instanceinfo['submittime'], 1)
            daysdelta = (datetime.now() - datetime.strptime(instanceinfo['submittime'], '%d-%m-%Y %H:%M:%S')).days + 1
            URL += '&days=' + str(daysdelta)
            hours = ''
            defaulthours = daysdelta * 24

        harvesterWorkersQuery = """
        SELECT * FROM {DB_SCHEMA_PANDA}.HARVESTER_WORKERS 
        where harvesterid = '{0}' {1} {2} {3} {4} {5} {6} {7}"""\
            .format(str(instance), status, computingsite, workerid, lastupdateCache,
                    days, hours, resourcetype, computingelement, DB_SCHEMA_PANDA=settings.DB_SCHEMA_PANDA)
        harvester_dicts = query_to_dicts(harvesterWorkersQuery)
        harvester_list = []
        harvester_list.extend(harvester_dicts)
        _logger.debug('Got workers: {}'.format(time.time() - request.session['req_init_time']))
        statusesDict = dict(Counter(harvester['status'] for harvester in harvester_list))
        computingsitesDict = dict(Counter(harvester['computingsite'] for harvester in harvester_list))
        computingelementsDict = dict(Counter(harvester['computingelement'] for harvester in harvester_list))
        resourcetypesDict = dict(Counter(harvester['resourcetype'] for harvester in harvester_list))

        jobscnt = 0
        for harvester in harvester_list:
            if harvester['njobs'] is not None:
                jobscnt += harvester['njobs']

        generalInstanseInfo = {'HarvesterID': instanceinfo['harvester_id'], 'Description': instanceinfo['description'], 'Starttime': instanceinfo['starttime'],
                               'Owner': instanceinfo['owner'], 'Hostname': instanceinfo['hostname'], 'Lastupdate': instanceinfo['lastupdate'], 'Computingsites':computingsitesDict,
                               'Statuses': statusesDict,'Resourcetypes': resourcetypesDict, 'Computingelements': computingelementsDict,'Software version': instanceinfo['sw_version'],
                               'Jobscount': jobscnt, 'Commit stamp': instanceinfo['commit_stamp']
        }
        generalInstanseInfo = collections.OrderedDict(generalInstanseInfo)
        request.session['viewParams']['selection'] = 'Harvester workers, last %s hours' %(defaulthours)

        data = {
                'generalInstanseInfo': generalInstanseInfo,
                'type': 'workers',
                'instance': instance,
                'computingsite': 0,
                'xurl': xurl,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'url': URL
                }
        # setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, "harvester", json.dumps(data, cls=DateEncoder), 60 * 20)
        _logger.debug('Finished preprocessing: {}'.format(time.time() - request.session['req_init_time']))
        return render(request, 'harvestermon.html', data, content_type='text/html')

    elif 'computingsite' in request.session['requestParams'] and 'instance' not in request.session['requestParams']:

        computingsite = request.session['requestParams']['computingsite']

        if ('workersstats' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']):
            harvsterworkerstats = []
            tquery = {}
            tquery['computingsite'] = computingsite
            limit = 100
            if 'limit' in request.session['requestParams']:
                limit = int(request.session['requestParams']['limit'])
            harvsterworkerstat = HarvesterWorkerStats.objects.filter(**tquery).values('harvesterid', 'resourcetype', 'status',
                                                                           'nworkers','lastupdate').filter(**tquery).extra(
                where=[extra]).order_by('-lastupdate')[:limit]
            # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for stat in harvsterworkerstat:
                stat['lastupdate'] = datetime.strptime(str(stat['lastupdate']), old_format).strftime(new_format)
                harvsterworkerstats.append(stat)

            return HttpResponse(json.dumps(harvsterworkerstats, cls=DateTimeEncoder), content_type='application/json')

        if ('dialogs' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']):
            dialogs = []
            tquery = {}
            instancelist = request.session['requestParams']['instancelist'].split(',')
            limit = 100
            if 'limit' in request.session['requestParams']:
                    limit = int(request.session['requestParams']['limit'])
            dialogsList = HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(harvesterid__in=instancelist).extra(where=[extra]).order_by('-creationtime')[:limit]
                # dialogs.extend(HarvesterDialogs.objects.filter(**tquery).values('creationtime','modulename', 'messagelevel','diagmessage').filter(**tquery).extra(where=[extra]).order_by('-creationtime'))
            old_format = '%Y-%m-%d %H:%M:%S'
            new_format = '%d-%m-%Y %H:%M:%S'
            for dialog in dialogsList:
                dialog['creationtime'] = datetime.strptime(str(dialog['creationtime']), old_format).strftime(new_format)
                dialogs.append(dialog)
            return HttpResponse(json.dumps(dialogs, cls=DateTimeEncoder), content_type='application/json')

        if 'pandaids' in request.session['requestParams'] and 'computingsite' in request.session['requestParams']:

            status = ''
            computingsite = ''
            workerid = ''
            days = ''
            defaulthours = 24
            resourcetype = ''
            computingelement = ''
            instance = ''

            if 'instance' not in request.session['requestParams']:
                sqlQueryInstances = """
                       SELECT harvesterid
                       FROM ATLAS_PANDA.HARVESTER_WORKERS where computingsite like '%s' group by harvesterid
                       """ % (request.session['requestParams']['computingsite'])

                cur = connection.cursor()
                cur.execute(sqlQueryInstances)

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
                resourcetype = """AND resourcetype like '%s'""" % (str(request.session['requestParams']['resourcetype']))
            if 'computingelement' in request.session['requestParams']:
                computingelement = """AND computingelement like '%s'""" % (str(request.session['requestParams']['computingelement']))
            if 'workerid' in request.session['requestParams']:
                workerid = """AND workerid in (%s)""" % (request.session['requestParams']['workerid'])
            if 'hours' in request.session['requestParams']:
                defaulthours = request.session['requestParams']['hours']
                hours = """AND submittime >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE)""" % (
                    defaulthours)
            else:
                hours = """AND submittime >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
                    defaulthours)
            if 'days' in request.session['requestParams']:
                days = """AND submittime >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) AS DATE) """ % (
                    request.session['requestParams']['days'])
                hours = ''
                defaulthours = int(request.session['requestParams']['days'])*24

            harvsterpandaids = []

            limit = 100

            if 'limit' in request.session['requestParams']:
                limit = request.session['requestParams']['limit']

            sqlQueryJobsStates = """
            SELECT hw.*, cj.jobstatus FROM (
                SELECT * from {}.harvester_rel_jobs_workers 
                    where harvesterid in ({})
                        and workerid in (
                          select workerid from (
                            SELECT workerid FROM {}.HARVESTER_WORKERS
                                where harvesterid in ({}) {} {} {} {} {} {} {}
                                ORDER by lastupdate DESC
                            )
                          where rownum <= {}
                          )
                ) hw , {}.combined_wait_act_def_arch4 cj
            WHERE hw.pandaid = cj.pandaid   
            """.format(
                settings.DB_SCHEMA_PANDA,
                str(instance),
                settings.DB_SCHEMA_PANDA,
                str(instance), status, computingsite, workerid, days, hours, resourcetype, computingelement,
                limit,
                settings.DB_SCHEMA
            )

            cur = connection.cursor()
            cur.execute(sqlQueryJobsStates)

            jobs = cur.fetchall()

            columns = [str(i[0]).lower() for i in cur.description]

            for job in jobs:
                object = {}
                object = dict(zip(columns, job))
                harvsterpandaids.append(object)

            return HttpResponse(json.dumps(harvsterpandaids, cls=DateTimeEncoder), content_type='application/json')

        URL += '?computingsite=' + request.session['requestParams']['computingsite']
        status = ''

        workerid = ''
        days = ''
        defaulthours = 24
        resourcetype = ''
        computingelement = ''

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
            hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """AND submittime  > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3)  AS DATE) """ % (
            request.session['requestParams']['days'])
            URL += '&days=' + str(request.session['requestParams']['days'])
            hours = ''
            defaulthours = int(request.session['requestParams']['days']) * 24

        sqlQuery = """
          select * from {5}.harvester_workers
          where computingsite like '{0}' {1} {2} {3} {4} and rownum<=1
          order by workerid desc
          """.format(str(computingsite), status, workerid, resourcetype, computingelement, settings.DB_SCHEMA_PANDA)

        workersList = []
        cur = connection.cursor()
        cur.execute(sqlQuery)

        harvesterinfo = cur.fetchall()

        columns = [str(i[0]).lower() for i in cur.description]

        for worker in harvesterinfo:
            object = dict(zip(columns, worker))
            workersList.append(object)

        if len(workersList) == 0:
            message ="""Computingsite is not found OR no workers for this computingsite or time period. 
            Try using this <a href =/harvesters/?computingsite={0}&days=365>link (last 365 days)</a>""".format(computingsite)
            return HttpResponse(json.dumps({'message':  message}), content_type='text/html')

        harvesterworkersquery = """
        SELECT * FROM ATLAS_PANDA.HARVESTER_WORKERS 
        where computingsite = '{0}' {1} {2} {3} {4} {5} """\
            .format(str(computingsite), status, workerid, days, hours, resourcetype, computingelement)

        harvester_dicts = query_to_dicts(harvesterworkersquery)

        harvester_list = []
        harvester_list.extend(harvester_dicts)

        statusesDict = dict(Counter(harvester['status'] for harvester in harvester_list))
        harvesteridDict = dict(Counter(harvester['harvesterid'] for harvester in harvester_list))
        computingelementsDict = dict(Counter(harvester['computingelement'] for harvester in harvester_list))
        resourcetypesDict = dict(Counter(harvester['resourcetype'] for harvester in harvester_list))

        jobscnt = 0

        for harvester in harvester_list:
            if harvester['njobs'] is not None:
                jobscnt += harvester['njobs']

        generalInstanseInfo = {'Computingsite': workersList[0]['computingsite'],
                               # 'Starttime': workersList[0]['insstarttime'],
                               # 'Hostname': workersList[0]['hostname'],
                               # 'Lastupdate': workersList[0]['inslastupdate'],
                               'Harvesters': harvesteridDict,
                               'Statuses': statusesDict,
                               'Resourcetypes': resourcetypesDict,
                               'Computingelements': computingelementsDict,
                               # 'Software version': workersList[0]['sw_version'],
                               # 'Commit stamp': workersList[0]['commit_stamp']
        }
        request.session['viewParams']['selection'] = 'Harvester workers, last %s hours' %(defaulthours)

        data = {
                'generalInstanseInfo': generalInstanseInfo,
                'type': 'workers',
                'instance': 0,
                'instancelist': ','.join(harvesteridDict.keys()),
                'computingsite': computingsite,
                'xurl': xurl,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'url': URL
                }
        # setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, "harvester", json.dumps(data, cls=DateEncoder), 60 * 20)
        return render(request, 'harvestermon.html', data, content_type='text/html')
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
            message = """
            No workers for this pandaid or time period. 
            Try using this <a href =/harvesters/?pandaid={0}&days=365>link (last 365 days)</a>""".format(pandaid)
            return HttpResponse(json.dumps({'message': message}),
                                    content_type='text/html')
        URL += '?pandaid=' + request.session['requestParams']['pandaid']

        if 'hours' in request.session['requestParams']:
            defaulthours = request.session['requestParams']['hours']
            hours = """AND submittime >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
            defaulthours)
            URL += '&hours=' + str(request.session['requestParams']['hours'])
        else:
            hours = """AND submittime >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
                defaulthours)
        if 'days' in request.session['requestParams']:
            days = """AND submittime  >= CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) AS DATE) """ % (
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

        sqlQueryHarvester = """
          SELECT harvesterid, count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where (%s) %s %s %s %s group by harvesterid
          """ % (jobsworkersquery, status,  workerid, resourcetype, computingelement)

        sqlQueryStatus = """
          SELECT status,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
          where (%s) %s %s %s %s group by status
          """ % (jobsworkersquery, status,  workerid, resourcetype, computingelement)

        sqlQueryResource = """
        SELECT RESOURCETYPE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where (%s) %s %s %s %s group by RESOURCETYPE
        """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        sqlQueryCE = """
        SELECT COMPUTINGELEMENT,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
        where (%s) %s %s %s %s group by COMPUTINGELEMENT
        """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        sqlQueryComputingsite = """
           SELECT COMPUTINGSITE,count(*) FROM ATLAS_PANDA.HARVESTER_WORKERS
           where (%s) %s %s %s %s  group by COMPUTINGSITE
           """ % (jobsworkersquery, status, workerid, resourcetype, computingelement)

        cur = connection.cursor()

        cur.execute(sqlQueryHarvester)
        harvesterids = cur.fetchall()

        cur.execute(sqlQueryStatus)
        statuses = cur.fetchall()

        cur.execute(sqlQueryResource)
        resourcetypes = cur.fetchall()

        cur.execute(sqlQueryCE)
        computingelements = cur.fetchall()

        cur.execute(sqlQueryComputingsite)
        computingsites = cur.fetchall()

        harvesteridDict = {}

        for harvester in harvesterids:
            harvesteridDict[harvester[0]] = harvester[1]

        if len(harvesteridDict) == 0:
            message = """
            No workers for this pandaid or time period. 
            Try using this <a href =/harvesters/?pandaid=%s&days=365>link (last 365 days)
            </a>""" % (
                    pandaid)
            return HttpResponse(json.dumps({'message': message}), content_type='text/html')

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

        generalInstanseInfo = {'JobID': ' '.join(pandaids.values()), 'Harvesters': harvesteridDict,
                               'Statuses': statusesDict,
                               'Resourcetypes':resourcetypesDict, 'Computingelements': computingelementsDict,
                               'Computingsites': computingsitesDict}

        request.session['viewParams']['selection'] = 'Harvester workers, last %s hours' % defaulthours

        data = {
                'generalInstanseInfo': generalInstanseInfo,
                'type': 'workers',
                'instance': ','.join(list(harvesteridDict.keys())),
                'xurl': xurl,
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'url': URL
                }
        # setCacheEntry(request, transactionKey, json.dumps(generalWorkersList[:display_limit_workers], cls=DateEncoder), 60 * 60, isData=True)
        setCacheEntry(request, "harvester", json.dumps(data, cls=DateEncoder), 60 * 20)
        return render(request, 'harvestermon.html', data, content_type='text/html')
    else:
        sqlQuery = f"""
          SELECT HARVESTER_ID as HARVID,
          SW_VERSION,
          DESCRIPTION,
          COMMIT_STAMP,
          to_char(LASTUPDATE, 'dd-mm-yyyy hh24:mi:ss') as LASTUPDATE
          FROM {settings.DB_SCHEMA_PANDA}.HARVESTER_INSTANCES
        """
        instanceDictionary = []

        cur = connection.cursor()
        cur.execute(sqlQuery)

        for instance in cur:
            instanceDictionary.append(
                {'instance': instance[0],
                 'sw_version':instance[1],
                 'commit_stamp':instance[2],
                 'descr': instance[3],'lastupdate':instance[4]}
            )

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
        if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
            return render(request, 'harvestermon.html', data, content_type='text/html')
        else:
            return HttpResponse(json.dumps(instanceDictionary, cls=DateTimeEncoder), content_type='application/json')


def workersJSON(request):

    valid, response = initRequest(request)

    xurl = extensibleURL(request)

    if '_' in request.session['requestParams']:
        xurl = xurl.replace('_={0}&'.format(request.session['requestParams']['_']), '')

    data = getCacheEntry(request, xurl, isData=True)

    if data is not None:
        data = json.loads(data)
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

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
        hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
            defaulthours)
    else: hours = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' hour(3) AS DATE) """ % (
        defaulthours)
    if 'days' in request.session['requestParams']:
        days = """AND submittime > CAST(sys_extract_utc(SYSTIMESTAMP) - interval '%s' day(3) AS DATE) """ % (
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
                                'diagmessage', 'njobs', 'computingelement','jdl']

            fields = ','.join(generalWorkersFields)

            sqlquery = f"""
            SELECT * FROM (SELECT %s FROM {settings.DB_SCHEMA_PANDA}.HARVESTER_WORKERS
            where harvesterid like '%s' %s %s %s %s %s %s %s %s
            order by submittime DESC) WHERE ROWNUM<=%s
            """ % (fields, str(instance), status, computingsite, workerid, lastupdateCache, days, hours, resourcetype, computingelement, display_limit_workers)

            cur = connection.cursor()
            cur.execute(sqlquery)
            columns = [str(i[0]).lower() for i in cur.description]
            workersList = []

            for worker in cur:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)
            if 'key' not in request.session['requestParams']:
                setCacheEntry(request, xurl, json.dumps(workersList, cls=DateTimeEncoder), 60 * 20, isData = True)

            return HttpResponse(json.dumps(workersList, cls=DateTimeEncoder), content_type='application/json')

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
                                    'diagmessage', 'njobs', 'computingelement','jdl']

            fields = ','.join(generalWorkersFields)
            sqlquery = """
             SELECT * FROM (SELECT %s FROM ATLAS_PANDA.HARVESTER_WORKERS
             where computingsite like '%s' %s %s %s %s %s %s
             order by  submittime  DESC) WHERE ROWNUM <= %s
             """ % (fields, str(computingsite), status,  workerid, days, hours, resourcetype, computingelement, display_limit_workers)

            workers = connection.cursor()
            workers.execute(sqlquery)
            columns = [str(i[0]).lower() for i in workers.description]
            workersList = []

            for worker in workers:
                object = {}
                object = dict(zip(columns, worker))
                workersList.append(object)
            if 'key' not in request.session['requestParams']:
                setCacheEntry(request, xurl, json.dumps(workersList, cls=DateTimeEncoder), 60 * 20, isData = True)

            return HttpResponse(json.dumps(workersList, cls=DateTimeEncoder), content_type='application/json')

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
                                    'diagmessage', 'njobs', 'computingelement','jdl']

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

            return HttpResponse(json.dumps(workersList, cls=DateTimeEncoder), content_type='application/json')

@login_customrequired
def harvesterSlots(request):
    valid, response = initRequest(request)

    harvesterslotsList = []
    harvesterslots = HarvesterSlots.objects.values('pandaqueuename','gshare','resourcetype','numslots','modificationtime','expirationtime')

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
    return render(request, 'harvesterSlots.html', data, content_type='text/html')


def getWorkersByJobID(pandaid, instance=''):

    instancequery = ''

    if '|' in pandaid:
        pandaid = 'where pandaid in (' + pandaid.replace('|', ',') + ')'
    elif ',' in pandaid:
        pandaid = 'where pandaid in (' + pandaid + ')'
    else:
        pandaid = 'where pandaid = ' + pandaid

    if instance != '':
       instancequery = """ AND harvesterid like '%s' """ %(instance)

    sqlQuery = """
    select harvesterid, workerid, pandaid from {}.harvester_rel_jobs_workers {} {}
    """.format(settings.DB_SCHEMA_PANDA, pandaid, instancequery)

    cur = connection.cursor()
    cur.execute(sqlQuery)

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
            jobsworkersquery += 'harvesterid like \'{0}\' and workerid in ({1})'.format(instance,', '.join(workersList[instance]))
            if cntinstances > 1:
                jobsworkersquery += ' OR '
                cntinstances = cntinstances - 1

    return jobsworkersquery, pandaidList


def query_to_dicts(query_string, *query_args):
    from itertools import zip_longest as izip

    cursor = connection.cursor()
    cursor.execute(query_string, query_args)
    col_names = [str(desc[0]).lower() for desc in cursor.description]
    while True:
        row = cursor.fetchone()
        if row is None:
            break

        row_dict = dict(izip(col_names, row))
        yield row_dict
    return


def getHarvesterJobs(request, instance='', workerid='', jobstatus='', fields='', **kwargs):
    '''
    Get jobs list for the particular harvester instance and worker
    :param request: request object
    :param instance: harvester instance
    :param workerid: harvester workerid
    :param jobstatus: jobs statuses
    :param fields: jobs fields
    :return: harvester jobs list
    '''

    jobsList = []
    renamed_fields = {
        'resourcetype': 'resource_type',
        'memoryleak': 'memory_leak',
        'memoryleakx2': 'memory_leak_x2',
        'joblabel': 'job_label',
    }
    qjobstatus = ''

    if instance != '':
        qinstance = 'in (\'' + str(instance) + '\')'
    else:
        qinstance = 'is not null'

    if workerid != '':
        qworkerid = 'in (' + str(workerid) + ')'
    else:
        qworkerid = 'is not null'

    if fields != '':
        values = list(fields)
    else:
        if is_json_request(request):
            from core.pandajob.models import Jobsactive4
            values = [f.name for f in Jobsactive4._meta.get_fields() if f.name != 'jobparameters' and f.name != 'metadata']
        else:
            values = (
                'corecount', 'jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime',
                'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime',
                'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid',
                'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag',
                'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode',
                'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag',
                'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage',
                'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock',
                'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss',
                'maxrss', 'nucleus', 'eventservice', 'nevents','gshare','noutputdatafiles','parentid','actualcorecount',
                'schedulerid')

    # rename fields that has '_' in DB but not in model
    for k, v in renamed_fields.items():
        if k in values:
            values.remove(k)
            values.append(v)

    sqlQuery = """
    SELECT {2} FROM
    (SELECT {2} FROM {DB_SCHEMA_PANDA}.JOBSARCHIVED4, 
    (select
    pandaid as pid
    from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers where
    {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.harvesterid {0} and {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.workerid {1}) 
    PIDACTIVE WHERE PIDACTIVE.pid={DB_SCHEMA_PANDA}.JOBSARCHIVED4.PANDAID {3}
    UNION
    SELECT {2} FROM {DB_SCHEMA_PANDA}.JOBSACTIVE4, 
    (select
    pandaid as pid
    from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers where
    {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.harvesterid {0} and {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid={DB_SCHEMA_PANDA}.JOBSACTIVE4.PANDAID {3}
    UNION 
    SELECT {2} FROM {DB_SCHEMA_PANDA}.JOBSDEFINED4, 
    (select
    pandaid as pid
    from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers where
    {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.harvesterid {0} and {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid={DB_SCHEMA_PANDA}.JOBSDEFINED4.PANDAID {3}
    UNION 
    SELECT {2} FROM {DB_SCHEMA_PANDA}.JOBSWAITING4,
    (select
    pandaid as pid
    from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers where
    {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.harvesterid {0} and {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid={DB_SCHEMA_PANDA}.JOBSWAITING4.PANDAID {3}
    UNION 
    SELECT {2} FROM {DB_SCHEMA_PANDA_ARCH}.JOBSARCHIVED, 
    (select
    pandaid as pid
    from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers where
    {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.harvesterid {0} and {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers.workerid {1}) PIDACTIVE WHERE PIDACTIVE.pid={DB_SCHEMA_PANDA_ARCH}.JOBSARCHIVED.PANDAID {3})  
    """

    sqlQuery = sqlQuery.format(
        qinstance,
        qworkerid,
        ', '.join(values),
        qjobstatus,
        DB_SCHEMA_PANDA=settings.DB_SCHEMA_PANDA,
        DB_SCHEMA_PANDA_ARCH=settings.DB_SCHEMA_PANDA_ARCH)

    cur = connection.cursor()
    cur.execute(sqlQuery)
    jobs = cur.fetchall()

    columns = [str(column[0]).lower() for column in cur.description]
    for job in jobs:
        jobsList.append(dict(zip(columns, job)))

    return jobsList


def getCeHarvesterJobs(request, computingelment, fields=''):
    '''
    Get jobs for the particular CE
    
    :param computingelment: harvester computingelement
    :param fields: list of fields for jobs tables
    :return: 
    '''
    jobList = []

    if 'hours' in request.session['requestParams']:
        lastupdated_time = "'{0}' hour".format(request.session['requestParams']['hours'])
    elif 'days' in request.session['requestParams']:
        lastupdated_time = "'{0}' day".format(request.session['requestParams']['days'])
    else:
        lastupdated_time = "'{0}' hour".format(str(int((request.session['TLAST'] - request.session['TFIRST']).seconds/3600)))

    if fields != '':
        values = fields
    else:
        if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
            values = []
            from core.pandajob.models import Jobsactive4
            for f in Jobsactive4._meta.get_fields():
                if f.name =='resourcetype':
                    values.append('resource_type')
                elif f.name !='jobparameters' and f.name != 'metadata':
                    values.append(f.name)
        else:
            values = 'corecount', 'jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss', 'maxrss', 'nucleus', 'eventservice', 'nevents','gshare','noutputdatafiles','parentid','actualcorecount','schedulerid'

    sqlQuery = """
    SELECT DISTINCT {2} FROM
    (SELECT {2} FROM ATLAS_PANDA.JOBSARCHIVED4, 
    (SELECT jw.pandaid as pid FROM atlas_panda.harvester_rel_jobs_workers jw, atlas_panda.harvester_workers w
    WHERE jw.harvesterid=w.harvesterid AND jw.workerid = w.workerid
    AND w.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND jw.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND w.computingelement like '%{0}%') PIDACTIVE 
    WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSARCHIVED4.PANDAID 
    UNION ALL
    SELECT {2} FROM ATLAS_PANDA.JOBSACTIVE4, 
    (SELECT jw.pandaid as pid FROM atlas_panda.harvester_rel_jobs_workers jw, atlas_panda.harvester_workers w
    WHERE jw.harvesterid=w.harvesterid AND jw.workerid = w.workerid
    AND w.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE) 
    AND jw.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND w.computingelement like '%{0}%') PIDACTIVE 
    WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSACTIVE4.PANDAID 
    UNION ALL 
    SELECT {2} FROM ATLAS_PANDA.JOBSDEFINED4, 
    (SELECT jw.pandaid as pid FROM atlas_panda.harvester_rel_jobs_workers jw, atlas_panda.harvester_workers w
    WHERE jw.harvesterid=w.harvesterid AND jw.workerid = w.workerid
    AND w.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND jw.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND w.computingelement like '%{0}%') PIDACTIVE 
    WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSDEFINED4.PANDAID
    UNION ALL 
    SELECT {2} FROM ATLAS_PANDA.JOBSWAITING4,
    (SELECT jw.pandaid as pid FROM atlas_panda.harvester_rel_jobs_workers jw, atlas_panda.harvester_workers w
    WHERE jw.harvesterid=w.harvesterid AND jw.workerid = w.workerid
    AND w.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND jw.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND w.computingelement like '%{0}%') PIDACTIVE 
    WHERE PIDACTIVE.pid=ATLAS_PANDA.JOBSWAITING4.PANDAID 
    UNION ALL
    SELECT {2} FROM ATLAS_PANDAARCH.JOBSARCHIVED, 
    (SELECT jw.pandaid as pid FROM atlas_panda.harvester_rel_jobs_workers jw, atlas_panda.harvester_workers w
    WHERE jw.harvesterid=w.harvesterid AND jw.workerid = w.workerid
    AND w.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND jw.lastupdate >  CAST (sys_extract_utc(SYSTIMESTAMP) - interval {1} as DATE)
    AND w.computingelement like '%{0}%') PIDACTIVE 
    WHERE PIDACTIVE.pid=ATLAS_PANDAARCH.JOBSARCHIVED.PANDAID)  
    """.format(computingelment, lastupdated_time, ', '.join(values))

    cur = connection.cursor()
    cur.execute(sqlQuery)

    jobs = cur.fetchall()

    columns = [str(column[0]).lower() for column in cur.description]
    for job in jobs:
        jobList.append(dict(zip(columns, job)))

    return jobList


def getHarversterWorkersForTask(request):
    valid, response = initRequest(request)
    if not valid: return response
    if 'requestParams' in request.session and 'jeditaskid' in request.session['requestParams']:
        try:
            jeditaskid = int(request.session['requestParams']['jeditaskid'])
        except:
            return HttpResponse(status=400)

        data = get_harverster_workers_for_task(jeditaskid)
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    return HttpResponse(status=400)