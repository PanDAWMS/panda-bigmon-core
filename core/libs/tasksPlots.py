
import json

from django.http import HttpResponse

from core.libs.CustomJSONSerializer import NpEncoder
from core.libs.cache import getCacheEntry
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name
from core.libs.task import drop_duplicates, job_consumption_plots
from core.libs.job import add_job_category

from core.pandajob.models import Jobsdefined4, Jobsarchived, Jobswaiting4, Jobsactive4, Jobsarchived4


def getJobsData(request):

    data = {
        'error': '',
        'data': [],
    }
    idList = request.GET.get('idtasks', '')
    tasksList = getCacheEntry(request, idList, isData=True)
    if len(tasksList) == 0:
        return HttpResponse(data, status=500, content_type='application/json')
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
             'processingtype', 'transformation', 'creationtime'

    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_str]).values(*values))

    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_str]).values(*values))

    print("Number of found jobs: {}".format(len(jobs)))
    print("Number of sites: {}".format(len(set([j['computingsite'] for j in jobs]))))
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

