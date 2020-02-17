"""

"""
import json
import logging
import copy
import urllib3

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db.models import Count
from django.core.cache import cache

from core.libs.cache import getCacheEntry, setCacheEntry
from core.views import login_customrequired, initRequest, setupView, DateEncoder

from core.schedresource.models import SchedconfigJson
from core.pandajob.models import Jobsdefined4, Jobswaiting4, Jobsactive4, Jobsarchived4, CombinedWaitActDefArch4

from core.views import getAGISSites

_logger = logging.getLogger('bigpandamon')


# @login_customrequired
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
    # data = None
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
        'transferring',
        'finished',
        'failed',
        'cancelled',
        'merging',
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

    # get job summary data
    jsr_queues_dict, jsr_regions_dict = get_job_summary_region(jquery, job_states_order, extra=wildCardExtension)

    # transform dict to list and filter out rows depending on split by request param
    jsr_queues_list = []
    jsr_regions_list = []
    for pq, params in jsr_queues_dict.items():
        for jt, resourcetypes in params['summary'].items():
            for rt, summary in resourcetypes.items():
                if sum(summary.values()) > 0:  # filter out rows with 0 jobs
                    row = list()
                    row.append(pq)
                    row.append(params['pq_params']['pqtype'])
                    row.append(params['pq_params']['region'])
                    row.append(params['pq_params']['status'])
                    row.append(jt)
                    row.append(rt)
                    row.append(sum(summary.values()))
                    for js in job_states_order:
                        row.append(summary[js])
                    if summary['failed'] + summary['finished'] > 0:
                        row.append(round(100.0*summary['failed']/(summary['failed'] + summary['finished']), 1))
                    else:
                        row.append(0)

                    if split_by is None:
                        if jt == 'all' and rt == 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' in split_by:
                        if jt != 'all' and rt != 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_queues_list.append(row)
                    elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_queues_list.append(row)

    for reg, jobtypes in jsr_regions_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                if sum(summary.values()) > 0:  # filter out rows with 0 jobs
                    row = list()
                    row.append(reg)
                    row.append(jt)
                    row.append(rt)
                    row.append(sum(summary.values()))
                    for js in job_states_order:
                        row.append(summary[js])
                    if summary['failed'] + summary['finished'] > 0:
                        row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
                    else:
                        row.append(0)

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

    xurl = request.get_full_path()
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'hours': hours,
        'xurl': xurl,
        'jobstates': job_states_order,
        'regions': jsr_regions_list,
        'queues': jsr_queues_list,
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

    # get job info
    jsq = get_job_summary_split(query, extra=extra)

    # fill template with real values
    for row in jsq:
        if row['computingsite'] in jsr_queues_dict.keys() and row['jobtype'] in job_types and row['resourcetype'] in resource_types and row['jobstatus'] in job_states_order and 'count' in row:
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all']['all'][row['jobstatus']] += int(row['count'])

            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][row['jobstatus']] += int(row['count'])

    return jsr_queues_dict, jsr_regions_dict


def get_job_summary_split(query, extra):
    summary = []
    querynotime = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in querynotime:
        del querynotime['modificationtime__castdate__range']

    job_values = ('computingsite', 'resourcetype', 'prodsourcelabel', 'jobstatus')
    order_by = ('computingsite', 'jobstatus',)

    # get jobs groupings

    # summary.extend(CombinedWaitActDefArch4.objects.filter(**querynotime).values(*job_values).extra(where=[extra]).annotate(count=Count('jobstatus')).order_by(*order_by))

    summary.extend(
        Jobsactive4.objects.filter(**querynotime).values(*job_values).extra(where=[extra]).annotate(count=Count('jobstatus')).order_by(*order_by))
    summary.extend(
        Jobsdefined4.objects.filter(**querynotime).values(*job_values).extra(where=[extra]).annotate(count=Count('jobstatus')).order_by(*order_by))
    summary.extend(
        Jobswaiting4.objects.filter(**querynotime).values(*job_values).extra(where=[extra]).annotate(count=Count('jobstatus')).order_by(*order_by))
    summary.extend(
        Jobsarchived4.objects.filter(**query).values(*job_values).extra(where=[extra]).annotate(count=Count('jobstatus')).order_by(*order_by))

    # translate prodsourcelabel values to descriptive analy|prod job types
    psl_to_jt = {
        'panda': 'analy',
        'user': 'analy',
        'managed': 'prod',
    }
    jsq = []
    for row in summary:
        if 'prodsourcelabel' in row and row['prodsourcelabel'] in psl_to_jt.keys():
            row['jobtype'] = psl_to_jt[row['prodsourcelabel']]
            jsq.append(row)

    return jsq


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