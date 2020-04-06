"""

"""
import json
import logging
import urllib3
import copy
from datetime import datetime

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection
from django.db.models import Q, Sum
from django.core.cache import cache

from core.libs.cache import getCacheEntry, setCacheEntry
from core.views import login_customrequired, initRequest, setupView, DateEncoder

from core.schedresource.models import SchedconfigJson
from core.harvester.models import HarvesterWorkerStats

_logger = logging.getLogger('bigpandamon')


@login_customrequired
def dashboard(request):
    """
    A new job summary dashboard for regions that allows to split jobs in Grand Unified Queue
    by analy|prod and resource types
    Regions column order:
        region, status, job type, resource type, Njobstotal, [Njobs by status]
    Queues column order:
        queue name, type [GU, U, Simple], region, status, job type, resource type, Njobstotal, [Njobs by status]
    :param request: request
    :return: HTTP response
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "JobSummaryRegion")
    data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('JobSummaryRegion.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    job_states_order = [
        'defined',
        'waiting',
        'pending',
        'assigned',
        'throttled',
        'activated',
        'sent',
        'starting',
        'running',
        'holding',
        'merging',
        'transferring',
        'finished',
        'failed',
        'cancelled',
        'closed'
    ]

    if 'splitby' in request.session['requestParams'] and request.session['requestParams']['splitby']:
        split_by = request.session['requestParams']['splitby']
    else:
        split_by = None

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
    else:
        hours = 12

    jquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='job', wildCardExt=True)

    # add queue related request params to query dict
    if 'queuetype' in request.session['requestParams'] and request.session['requestParams']['queuetype']:
        jquery['queuetype'] = request.session['requestParams']['queuetype']
    if 'queuestatus' in request.session['requestParams'] and request.session['requestParams']['queuestatus']:
        jquery['queuestatus'] = request.session['requestParams']['queuestatus']

    # get job summary data
    jsr_queues_dict, jsr_regions_dict = get_job_summary_region(jquery, job_states_order, extra=wildCardExtension)

    # transform dict to list and filter out rows depending on split by request param
    jsr_queues_list = []
    jsr_regions_list = []
    for pq, params in jsr_queues_dict.items():
        for jt, resourcetypes in params['summary'].items():
            for rt, summary in resourcetypes.items():
                if sum([v for k, v in summary.items() if k in job_states_order]) > 0:  # filter out rows with 0 jobs
                    row = list()
                    row.append(pq)
                    row.append(params['pq_params']['pqtype'])
                    row.append(params['pq_params']['region'])
                    row.append(params['pq_params']['status'])
                    row.append(jt)
                    row.append(rt)
                    row.append(summary['nwsubmitted'])
                    row.append(summary['nwrunning'])
                    row.append(summary['rcores'])
                    row.append(sum([v for k, v in summary.items() if k in job_states_order]))
                    if summary['failed'] + summary['finished'] > 0:
                        row.append(round(100.0*summary['failed']/(summary['failed'] + summary['finished']), 1))
                    else:
                        row.append(0)
                    for js in job_states_order:
                        row.append(summary[js])

                    if split_by is None:
                        if jt == 'all' and rt == 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' in split_by:
                        if jt != 'all' and rt != 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                        if jt != 'all' and rt == 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_queues_list.append(row)

    for reg, jobtypes in jsr_regions_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                if sum([v for k, v in summary.items() if k in job_states_order]) > 0:  # filter out rows with 0 jobs
                    row = list()
                    row.append(reg)
                    row.append(jt)
                    row.append(rt)
                    row.append(summary['nwsubmitted'])
                    row.append(summary['nwrunning'])
                    row.append(summary['rcores'])
                    row.append(sum([v for k, v in summary.items() if k in job_states_order]))
                    if summary['failed'] + summary['finished'] > 0:
                        row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
                    else:
                        row.append(0)
                    for js in job_states_order:
                        row.append(summary[js])

                    if split_by is None:
                        if jt == 'all' and rt == 'all':
                            jsr_regions_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' in split_by:
                        if jt != 'all' and rt != 'all':
                            jsr_regions_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                        if jt != 'all' and rt == 'all':
                            jsr_regions_list.append(row)
                    elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_regions_list.append(row)

    select_params_dict = {}
    select_params_dict['region'] = sorted(list(set([r[0] for r in jsr_regions_list])))
    select_params_dict['queuetype'] = sorted(list(set([pq[1] for pq in jsr_queues_list])))
    select_params_dict['queuestatus'] = sorted(list(set([pq[3] for pq in jsr_queues_list])))

    xurl = request.get_full_path()
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'built': datetime.now().strftime("%H:%M:%S"),
        'hours': hours,
        'xurl': xurl,
        'selectParams': select_params_dict,
        'jobstates': job_states_order,
        'regions': jsr_regions_list,
        'queues': jsr_queues_list,
        'timerange': jquery['modificationtime__castdate__range'],
    }

    response = render_to_response('JobSummaryRegion.html', data, content_type='text/html')
    setCacheEntry(request, "JobSummaryRegion", json.dumps(data, cls=DateEncoder), 60 * 20)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def get_job_summary_region(query, job_states_order, extra='(1=1)'):
    """
    :param query: dict of query params for jobs retrieving
    :return: dict of groupings
    """
    jsr_queues_dict = {}
    jsr_regions_dict = {}

    job_types = ['analy', 'prod']
    resource_types = ['SCORE', 'MCORE', 'SCORE_HIMEM', 'MCORE_HIMEM']
    worker_metrics = ['nwrunning', 'nwsubmitted']
    extra_metrics = copy.deepcopy(worker_metrics)
    extra_metrics.append('rcores')

    # get info from AGIS|CRIC
    try:
        panda_queues_dict = get_AGIS_panda_queues()
    except:
        panda_queues_dict = None
        _logger.error("[JSR] cannot get json from AGIS")

    if not panda_queues_dict:
        # get data from new SCHEDCONFIGJSON table
        panda_queues_list = []
        panda_queues_dict = {}
        panda_queues_list.extend(SchedconfigJson.objects.values())
        if len(panda_queues_list) > 0:
            for pq in panda_queues_list:
                try:
                    panda_queues_dict[pq['pandaqueue']] = json.loads(pq['data'])
                except:
                    panda_queues_dict[pq['pandaqueue']] = None
                    _logger.error("[JSR] cannot load json from SCHEDCONFIGJSON table for {} PanDA queue".format(pq['pandaqueue']))

    regions_list = list(set([params['cloud'] for pq, params in panda_queues_dict.items()]))

    # filter out queues by queue related selection params
    pq_to_remove = []
    if 'queuestatus' in query:
        pq_to_remove.extend([pqn for pqn, params in panda_queues_dict.items() if params['status'] != query['queuestatus']])
    if 'queuetype' in query:
        pq_to_remove.extend([pqn for pqn, params in panda_queues_dict.items() if params['type'] != query['queuetype']])
    if len(pq_to_remove) > 0:
        for pqr in list(set(pq_to_remove)):
            del panda_queues_dict[pqr]

    # create template structure for grouping by queue
    for pqn, params in panda_queues_dict.items():
        jsr_queues_dict[pqn] = {'pq_params': {}, 'summary': {}}
        jsr_queues_dict[pqn]['pq_params']['pqtype'] = params['type']
        jsr_queues_dict[pqn]['pq_params']['region'] = params['cloud']
        jsr_queues_dict[pqn]['pq_params']['status'] = params['status']
        for jt in job_types:
            jsr_queues_dict[pqn]['summary'][jt] = {}
            jsr_queues_dict[pqn]['summary']['all'] = {}
            for rt in resource_types:
                jsr_queues_dict[pqn]['summary'][jt][rt] = {}
                jsr_queues_dict[pqn]['summary'][jt]['all'] = {}
                jsr_queues_dict[pqn]['summary']['all'][rt] = {}
                jsr_queues_dict[pqn]['summary']['all']['all'] = {}
                for js in job_states_order:
                    jsr_queues_dict[pqn]['summary'][jt][rt][js] = 0
                    jsr_queues_dict[pqn]['summary'][jt]['all'][js] = 0
                    jsr_queues_dict[pqn]['summary']['all'][rt][js] = 0
                    jsr_queues_dict[pqn]['summary']['all']['all'][js] = 0

                for em in extra_metrics:
                    jsr_queues_dict[pqn]['summary'][jt][rt][em] = 0
                    jsr_queues_dict[pqn]['summary'][jt]['all'][em] = 0
                    jsr_queues_dict[pqn]['summary']['all'][rt][em] = 0
                    jsr_queues_dict[pqn]['summary']['all']['all'][em] = 0



    # create template structure for grouping by region
    for r in regions_list:
        jsr_regions_dict[r] = {}
        for jt in job_types:
            jsr_regions_dict[r][jt] = {}
            jsr_regions_dict[r]['all'] = {}
            for rt in resource_types:
                jsr_regions_dict[r][jt][rt] = {}
                jsr_regions_dict[r][jt]['all'] = {}
                jsr_regions_dict[r]['all'][rt] = {}
                jsr_regions_dict[r]['all']['all'] = {}
                for js in job_states_order:
                    jsr_regions_dict[r][jt][rt][js] = 0
                    jsr_regions_dict[r][jt]['all'][js] = 0
                    jsr_regions_dict[r]['all'][rt][js] = 0
                    jsr_regions_dict[r]['all']['all'][js] = 0

                for em in extra_metrics:
                    jsr_regions_dict[r][jt][rt][em] = 0
                    jsr_regions_dict[r][jt]['all'][em] = 0
                    jsr_regions_dict[r]['all'][rt][em] = 0
                    jsr_regions_dict[r]['all']['all'][em] = 0

    # get job info
    jsq = get_job_summary_split(query, extra=extra)

    # get workers info
    wsq = get_workers_summary_split(query)

    # fill template with real values of job states counts
    for row in jsq:
        if row['computingsite'] in jsr_queues_dict and row['jobtype'] in job_types and row['resourcetype'] in resource_types and row['jobstatus'] in job_states_order and 'count' in row:
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all']['all'][row['jobstatus']] += int(row['count'])

            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][row['jobstatus']] += int(row['count'])

            # fill sum of running cores
            if row['jobstatus'] == 'running':
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all']['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary']['all']['all']['rcores'] += int(row['rcores'])

                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all']['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all']['rcores'] += int(row['rcores'])

    # fill template with real values of n workers stats
    for row in wsq:
        if row['computingsite'] in jsr_queues_dict and row['jobtype'] in job_types and row['resourcetype'] in resource_types:
            for wm in worker_metrics:
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all'][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary']['all']['all'][wm] += int(row[wm])

                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all'][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][wm] += int(row[wm])

    return jsr_queues_dict, jsr_regions_dict


def get_job_summary_split(query, extra):
    """ Query the jobs summary """

    extra_str = extra
    if 'computingsite__in' in query:
        extra_str += " and (computingsite in ("
        for pq in query['computingsite__in']:
            extra_str += "'" + str(pq) + "',"
        extra_str = extra_str[:-1]
        extra_str += "))"
    if 'prodsourcelabel__in' in query:
        extra_str += " and (prodsourcelabel in ("
        for pl in query['prodsourcelabel__in']:
            extra_str += "'" + str(pl) + "',"
        extra_str = extra_str[:-1]
        extra_str += "))"
    if 'resourcetype' in query:
        extra_str += " and (resource_type = '" + query['resourcetype'] + "')"

    # get jobs groupings
    query_raw = """
        select computingsite, resource_type as resourcetype, prodsourcelabel, jobstatus, count(pandaid) as count, sum(rcores) as rcores
        from  (
        select ja4.pandaid, ja4.resource_type, ja4.computingsite, ja4.prodsourcelabel, ja4.jobstatus, ja4.modificationtime, 0 as rcores
        from ATLAS_PANDA.JOBSARCHIVED4 ja4  where modificationtime > TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')
        union
        select jav4.pandaid, jav4.resource_type, jav4.computingsite, jav4.prodsourcelabel, jav4.jobstatus, jav4.modificationtime,
        case when jobstatus = 'running' then actualcorecount else 0 end as rcores
        from ATLAS_PANDA.jobsactive4 jav4
        union
        select jw4.pandaid, jw4.resource_type, jw4.computingsite, jw4.prodsourcelabel, jw4.jobstatus, jw4.modificationtime, 0 as rcores
        from ATLAS_PANDA.jobswaiting4 jw4 
        union
        select jd4.pandaid, jd4.resource_type, jd4.computingsite, jd4.prodsourcelabel, jd4.jobstatus, jd4.modificationtime, 0 as rcores
        from ATLAS_PANDA.jobsdefined4 jd4 
        )
        where {}
        GROUP BY computingsite, prodsourcelabel, resource_type, jobstatus
        order by computingsite, prodsourcelabel, resource_type, jobstatus
    """.format(query['modificationtime__castdate__range'][0], extra_str)

    cur = connection.cursor()
    cur.execute(query_raw)
    job_summary_tuple = cur.fetchall()
    job_summary_header = ['computingsite', 'resourcetype', 'prodsourcelabel', 'jobstatus', 'count', 'rcores']
    summary = [dict(zip(job_summary_header, row)) for row in job_summary_tuple]

    # Translate prodsourcelabel values to descriptive analy|prod job types
    summary = prodsourcelabel_to_jobtype(summary)

    return summary


def get_workers_summary_split(query):
    """Get statistics of submitted and running Harvester workers"""

    wquery = {}
    if 'computingsite__in' in query:
        wquery['computingsite__in'] = query['computingsite__in']
    if 'resourcetype' in query:
        wquery['resourcetype'] = query['resourcetype']
    wquery['status__in'] = ['running', 'submitted']
    wquery['jobtype__in'] = ['managed', 'user', 'panda']
    w_running = Sum('nworkers', filter=Q(status__exact='running'))
    w_submitted = Sum('nworkers', filter=Q(status__exact='submitted'))
    w_values = ['computingsite', 'resourcetype', 'jobtype']
    worker_summary = HarvesterWorkerStats.objects.filter(**wquery).values(*w_values).annotate(nwrunning=w_running).annotate(nwsubmitted=w_submitted)

    # Translate prodsourcelabel values to descriptive analy|prod job types
    worker_summary = prodsourcelabel_to_jobtype(worker_summary, field_name='jobtype')

    return list(worker_summary)


def get_AGIS_panda_queues():
    """Get PanDA queues config from AGIS"""
    panda_queues_dict = cache.get('pandaQueues')

    if not panda_queues_dict:
        panda_queues_dict = {}
        url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas"
        http = urllib3.PoolManager()
        data = {}
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for pq, params in data.items():
                if 'vo_name' in params and params['vo_name'] == 'atlas':
                    panda_queues_dict[pq] = params
        except Exception as exc:
            print (exc)

        cache.set('pandaQueues', panda_queues_dict, 3600)

    return panda_queues_dict


def prodsourcelabel_to_jobtype(list_of_dict, field_name='prodsourcelabel'):
    """Translate prodsourcelabel values to descriptive analy|prod job types"""

    psl_to_jt = {
        'panda': 'analy',
        'user': 'analy',
        'managed': 'prod',
    }
    new_list_of_dict = []
    for row in list_of_dict:
        if field_name in row and row[field_name] in psl_to_jt.keys():
            row['jobtype'] = psl_to_jt[row[field_name]]
            new_list_of_dict.append(row)

    return new_list_of_dict