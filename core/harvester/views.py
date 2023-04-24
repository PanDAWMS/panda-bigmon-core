import json
import math
import collections
import logging
import time
from collections import Counter

from datetime import datetime, timedelta

from django.db import connection, connections

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

from django.utils.cache import patch_response_headers

from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import is_timestamp
from core.libs.sqlcustom import escape_input
from core.libs.sqlsyntax import interval_last
from core.libs.DateEncoder import DateEncoder
from core.libs.DateTimeEncoder import DateTimeEncoder
from core.oauth.utils import login_customrequired
from core.utils import is_json_request, removeParam
from core.views import initRequest, extensibleURL
from core.harvester.models import HarvesterWorkers, HarvesterDialogs, HarvesterWorkerStats, HarvesterSlots, \
    HarvesterInstances, HarvesterRelJobsWorkers
from core.harvester.utils import get_harverster_workers_for_task, setup_harvester_view

from django.conf import settings
import core.constants as const

harvesterWorkerStatuses = [
    'missed', 'submitted', 'ready', 'running', 'idle', 'finished', 'failed', 'cancelled'
]

_logger = logging.getLogger('bigpandamon')


@login_customrequired
def harvesters(request):
    """
    It is a view to redirect requests to specific views depending on request params
        in the decommissioned 'all in one' /harvesters/ view.
    :param request:
    :return: redirect
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    if len(request.session['requestParams']) == 0:
        # redirect to list of instances page
        return redirect('/harvester/instances/')
    else:
        # redirect to list of workers page
        return redirect('/harvester/workers/?{}'.format('&'.join(['{}={}'.format(p, v) for p, v in request.session['requestParams'].items()])))


@login_customrequired
def harvesterWorkerInfoLegacy(request):
    """
    Redirecting to  /harvester/worker/ view.
    :param request:
    :return: redirect
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # redirect to list of workers page
    return redirect('/harvester/worker/?{}'.format('&'.join(['{}={}'.format(p, v) for p, v in request.session['requestParams'].items()])))


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
    if 'days' in request.session['requestParams'] or 'hours' in request.session['requestParams']:
        iquery, _ = setup_harvester_view(request, 'instance')
    else:
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
        last_submission_time = None
        try:
            last_submission_time = HarvesterWorkers.objects.filter(**wquery).latest('submittime').submittime
        except:
            _logger.exception('Failed to get last submitted worker - it seams there are no workers at all ')
        if last_submission_time is not None:
            warning['timewindow'] = """The time window was extended to the last submitted worker 
                            since no workers have been found for specified or default time period"""
            wquery['submittime__range'] = [
                (last_submission_time - timedelta(hours=1)).strftime(settings.DATETIME_FORMAT),
                datetime.now().strftime(settings.DATETIME_FORMAT)
            ]
        else:
            data = {
                'warning': 'No workers have been find for the instance',
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
            }
            return render(request, 'harvesterWorkers.html', data, content_type='text/html')

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


def getHarvesterJobs(request, instance='', workerid='', jobstatus='', fields='', **kwargs):
    """
    Get jobs list for the particular harvester instance and worker
    :param request: request object
    :param instance: harvester instance
    :param workerid: harvester workerid
    :param jobstatus: jobs statuses
    :param fields: jobs fields
    :return: harvester jobs list
    """

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
            values = list(const.JOB_FIELDS)

    # rename fields that has '_' in DB but not in model
    for k, v in renamed_fields.items():
        if k in values:
            values.remove(k)
            values.append(v)

    sqlQuery = """
    select {2} from (
        select {2} from {DB_SCHEMA_PANDA}.jobsarchived4 jarch4 , (
            select pandaid as pid
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers 
            where harvesterid {0} and workerid {1}
            ) hj 
        where hj.pid=jarch4.pandaid {3}
        union
        select {2} from {DB_SCHEMA_PANDA}.jobsactive4 jact4, (
            select pandaid as pid
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers 
            where harvesterid {0} and workerid {1}
            ) hj 
        where hj.pid=jact4.pandaid {3}
        union 
        select {2} from {DB_SCHEMA_PANDA}.jobsdefined4 jd4, (
            select pandaid as pid
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers 
            where harvesterid {0} and workerid {1}
            ) hj 
        where hj.pid=jd4.pandaid {3}
        union 
        select {2} FROM {DB_SCHEMA_PANDA}.jobswaiting4 jw4, (
            select pandaid as pid
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers 
            where harvesterid {0} and workerid {1}
            ) hj 
        where hj.pid=jw4.pandaid {3}
        union 
        select {2} from {DB_SCHEMA_PANDA_ARCH}.jobsarchived ja, (
            select pandaid as pid
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers 
            where harvesterid {0} and workerid {1}
            ) hj 
        where hj.pid=ja.pandaid {3}
    )  comb_data
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


def getCeHarvesterJobs(request, computingelement, fields=''):
    """
    Get jobs for the particular CE
    :param computingelement: harvester computingelement
    :param fields: list of fields for jobs tables
    :return: job_list
    """
    job_list = []
    if 'hours' in request.session['requestParams']:
        lastupdated_hours = request.session['requestParams']['hours']
    elif 'days' in request.session['requestParams']:
        lastupdated_hours = int(request.session['requestParams']['days']) * 24
    else:
        lastupdated_hours = int((request.session['TLAST'] - request.session['TFIRST']).seconds/3600)

    if fields != '':
        values = fields
    else:
        if is_json_request(request):
            values = []
            from core.pandajob.models import Jobsactive4
            for f in Jobsactive4._meta.get_fields():
                if f.name == 'resourcetype':
                    values.append('resource_type')
                elif f.name != 'jobparameters' and f.name != 'metadata':
                    values.append(f.name)
        else:
            values = list(const.JOB_FIELDS)

    # rename fields that has '_' in DB but not in model
    renamed_fields = {
        'resourcetype': 'resource_type',
        'memoryleak': 'memory_leak',
        'memoryleakx2': 'memory_leak_x2',
        'joblabel': 'job_label',
    }
    for k, v in renamed_fields.items():
        if k in values:
            values.remove(k)
            values.append(v)

    db = connections['default'].vendor
    sql_query = """
    select distinct {2} from (
        select {2} from {DB_SCHEMA_PANDA}.jobsarchived4 jarch4, (
            select jw.pandaid as pid 
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers jw, {DB_SCHEMA_PANDA}.harvester_workers w
            where jw.harvesterid=w.harvesterid and jw.workerid = w.workerid
                and w.lastupdate >  {1}
                and jw.lastupdate >  {1}
                and w.computingelement like '%{0}%'
            ) hj 
        where hj.pid=jarch4.pandaid 
        union all
        select {2} from {DB_SCHEMA_PANDA}.jobsactive4 ja4, (
            select jw.pandaid as pid 
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers jw, {DB_SCHEMA_PANDA}.harvester_workers w
            where jw.harvesterid=w.harvesterid and jw.workerid = w.workerid
                and w.lastupdate >  {1} 
                and jw.lastupdate >  {1}
                and w.computingelement like '%{0}%'
            ) hj 
        where hj.pid=ja4.pandaid 
        union all 
        select {2} from {DB_SCHEMA_PANDA}.jobsdefined4 jd4, (
            select jw.pandaid as pid 
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers jw, {DB_SCHEMA_PANDA}.harvester_workers w
            where jw.harvesterid=w.harvesterid and jw.workerid = w.workerid
                and w.lastupdate >  {1}
                and jw.lastupdate >  {1}
                and w.computingelement like '%{0}%'
            ) hj 
        where hj.pid=jd4.pandaid
        union all 
        select {2} from {DB_SCHEMA_PANDA}.jobswaiting4 jw4, (
            select jw.pandaid as pid 
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers jw, {DB_SCHEMA_PANDA}.harvester_workers w
            where jw.harvesterid=w.harvesterid and jw.workerid = w.workerid
                and w.lastupdate >  {1}
                and jw.lastupdate >  {1}
                and w.computingelement like '%{0}%'
            ) hj 
        where hj.pid=jw4.pandaid 
        union all
        select {2} from {DB_SCHEMA_PANDA_ARCH}.jobsarchived ja, (
            select jw.pandaid as pid 
            from {DB_SCHEMA_PANDA}.harvester_rel_jobs_workers jw, {DB_SCHEMA_PANDA}.harvester_workers w
            where jw.harvesterid=w.harvesterid and jw.workerid = w.workerid
                and w.lastupdate > {1}
                and jw.lastupdate > {1}
                and w.computingelement like '%{0}%'
            ) hj 
        where hj.pid=ja.pandaid
    )  
    """.format(
        computingelement,
        interval_last(lastupdated_hours, inter_unit='hour', db=db),
        ', '.join(values),
        DB_SCHEMA_PANDA=settings.DB_SCHEMA_PANDA,
        DB_SCHEMA_PANDA_ARCH=settings.DB_SCHEMA_PANDA_ARCH,
    )

    cur = connection.cursor()
    cur.execute(sql_query)
    jobs = cur.fetchall()

    columns = [str(column[0]).lower() for column in cur.description]
    for job in jobs:
        job_list.append(dict(zip(columns, job)))

    return job_list


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