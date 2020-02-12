"""

"""
import json
import logging
import copy

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db.models import Count

from core.libs.cache import getCacheEntry, setCacheEntry
from core.views import login_customrequired, initRequest, setupView, DateEncoder

from core.schedresource.models import SchedconfigJson
from core.pandajob.models import Jobsdefined4, Jobswaiting4, Jobsactive4, Jobsarchived4, Jobsarchived

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

    jquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, limit=9999999, querytype='job', wildCardExt=True)

    # olddata_dict = dashSummary(request, hours=1)

    jsr_queues_dict, jsr_regions_dict = get_job_summary_region(jquery, job_states_order)

    # transform dict to list
    jsr_queues_list = []
    jsr_regions_list = []
    for pq, params in jsr_queues_dict.items():
        for jt, resourcetypes in params['summary'].items():
            for rt, summary in resourcetypes.items():
                l = []
                l.append(pq)
                l.append(params['pq_params']['pqtype'])
                l.append(params['pq_params']['region'])
                l.append(params['pq_params']['status'])
                l.append(jt)
                l.append(rt)
                l.append(sum(summary.values()))
                for js in job_states_order:
                    l.append(summary[js])
                if summary['failed'] + summary['finished'] > 0:
                    l.append(round(100.0*summary['failed']/(summary['failed'] + summary['finished']), 1))
                else:
                    l.append(0)
                jsr_queues_list.append(l)

    for reg, jobtypes in jsr_regions_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                l = []
                l.append(reg)
                l.append(jt)
                l.append(rt)
                l.append(sum(summary.values()))
                for js in job_states_order:
                    l.append(summary[js])
                if summary['failed'] + summary['finished'] > 0:
                    l.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
                else:
                    l.append(0)
                jsr_regions_list.append(l)

    xurl = request.get_full_path()
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'xurl': xurl,
        'jobstates': job_states_order,
        'regions': jsr_regions_list,
        'queues': jsr_queues_list,
    }

    response = render_to_response('JobSummaryRegion.html', data, content_type='text/html')
    setCacheEntry(request, "JobSummaryRegion", json.dumps(data, cls=DateEncoder), 60 * 20)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def get_job_summary_region(query, job_states_order):
    """
    :param query: dict of query params for jobs retrieving
    :return: dict of groupings
    """
    jsr_queues_dict = {}
    jsr_regions_dict = {}

    job_types = ['analy', 'prod']
    resource_types = ['SCORE', 'MCORE', 'SCORE_HIMEM', 'MCORE_HIMEM']

    # get info from AGIS|CRIC
    ucoreComputingSites, harvesterComputingSites, _ = getAGISSites()

    # get data from new schedconfigjson table
    panda_queues_list = []
    panda_queues_dict = {}
    panda_queues_list.extend(SchedconfigJson.objects.values())
    if len(panda_queues_list) > 0:
        for pq in panda_queues_list:
            try:
                panda_queues_dict[pq['pandaqueue']] = json.loads(pq['data'])
            except:
                panda_queues_dict[pq['pandaqueue']] = None
                _logger.error("[JSR] cannot load json from SHEDCONFIGJSON table for {} PanDA queue".format(pq['pandaqueue']))

    regions_list = list(set([params['cloud'] for pq, params in panda_queues_dict.items()]))

    # create template structure for grouping by queue
    for pqn, params in panda_queues_dict.items():
        jsr_queues_dict[pqn] = {'pq_params': {}, 'summary': {}}
        jsr_queues_dict[pqn]['pq_params']['pqtype'] = params['type']
        jsr_queues_dict[pqn]['pq_params']['region'] = params['cloud']
        jsr_queues_dict[pqn]['pq_params']['status'] = params['status']
        for jt in job_types:
            jsr_queues_dict[pqn]['summary'][jt] = {}
            for rt in resource_types:
                jsr_queues_dict[pqn]['summary'][jt][rt] = {}
                for js in job_states_order:
                    jsr_queues_dict[pqn]['summary'][jt][rt][js] = 0

    # create template structure for grouping by region
    for r in regions_list:
        jsr_regions_dict[r] = {}
        for jt in job_types:
            jsr_regions_dict[r][jt] = {}
            for rt in resource_types:
                jsr_regions_dict[r][jt][rt] = {}
                for js in job_states_order:
                    jsr_regions_dict[r][jt][rt][js] = 0

    # get job info
    jsq = get_job_summary_split(query, extra='(1=1)')

    # fill template with real values
    for row in jsq:
        if row['computingsite'] in jsr_queues_dict.keys() and row['jobtype'] in job_types and row['resourcetype'] in resource_types and row['jobstatus'] in job_states_order and 'count' in row:
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])

    return jsr_queues_dict, jsr_regions_dict


def get_job_summary_split(query, extra):
    summary = []
    querynotime = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in querynotime:
        del querynotime['modificationtime__castdate__range']

    job_values = ('computingsite', 'jobstatus', 'resourcetype', 'corecount', 'prodsourcelabel')
    order_by = ('computingsite', 'jobstatus',)

    # get jobs groupings
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

