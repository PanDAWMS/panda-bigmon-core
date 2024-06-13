
import json
import logging

from django.http import HttpResponse

from core.libs.CustomJSONSerializer import NpEncoder
from core.libs.cache import getCacheEntry
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, drop_duplicates
from core.libs.job import add_job_category
from core.libs.jobconsumption import job_consumption_plots

from core.pandajob.models import Jobsdefined4, Jobsarchived, Jobswaiting4, Jobsactive4, Jobsarchived4
from core.utils import error_response

_logger = logging.getLogger('bigpandamon')


def getJobsData(request):

    data = {
        'error': '',
        'data': [],
    }
    idList = request.GET.get('idtasks', '')
    tasksList = getCacheEntry(request, idList, isData=True)
    if tasksList is None or len(tasksList) == 0:
        return error_response(request, message='No tasks found in cache', status=404)
    else:
        results = get_jobs_plot_data(tasksList)
        if len(results['error']) > 0:
            data['error'] = results['error']
        else:
            data['data'] = results['plot_data']

    return HttpResponse(json.dumps(data, cls=NpEncoder), content_type='application/json')


def get_jobs_plot_data(taskid_list):
    error = ''
    plots_list = []

    MAX_JOBS = 1000000
    MAX_ENTRIES__IN = 100
    extra_str = "(1=1)"
    query = {}
    if len(taskid_list) < MAX_ENTRIES__IN:
        query["jeditaskid__in"] = taskid_list
        query["jobstatus__in"] = ['finished', 'failed']
    else:
        # insert taskids to temp DB table
        tmp_table_name = get_tmp_table_name()
        tk_taskids = insert_to_temp_table(taskid_list)
        extra_str += " AND jeditaskid in (select id from {} where TRANSACTIONKEY={} ) ".format(tmp_table_name, tk_taskids)

    values = 'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid', \
             'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', \
             'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid', 'attemptnr', \
             'processingtype', 'transformation', 'creationtime', 'pilottiming'

    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_str]).values(*values))

    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_str]).values(*values))

    _logger.info("Number of found jobs: {}".format(len(jobs)))
    _logger.info("Number of sites: {}".format(len(set([j['computingsite'] for j in jobs]))))
    if len(jobs) > MAX_JOBS:
        error = 'Too many jobs to prepare plots. Please decrease the selection of tasks and try again.'
    else:
        # drop duplicate jobs
        jobs = drop_duplicates(jobs, id='pandaid')

        # determine jobs category (build, run or merge)
        jobs = add_job_category(jobs)

        # prepare data for job consumption plots
        plots_list = job_consumption_plots(jobs)

    return {'plot_data': plots_list, 'error': error}

