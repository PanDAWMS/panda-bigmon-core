import json
import logging
from django.http import HttpResponse
from core.libs.CustomJSONSerializer import NpEncoder
from core.libs.cache import getCacheEntry
from core.libs.job import get_job_list
from core.libs.jobconsumption import job_consumption_plots
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
    query = {
        "jeditaskid__in": taskid_list,
        "jobstatus__in": ['finished', 'failed']
    }
    values = (
        'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid',
        'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite',
        'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid', 'attemptnr',
        'processingtype', 'transformation', 'creationtime', 'pilottiming'
    )
    jobs = get_job_list(query, values=values, error_info=False)
    _logger.info("Number of found jobs: {}".format(len(jobs)))
    _logger.info("Number of sites: {}".format(len(set([j['computingsite'] for j in jobs]))))

    if len(jobs) > MAX_JOBS:
        error = 'Too many jobs to prepare plots. Please decrease the selection of tasks and try again.'
    else:
        # prepare data for job consumption plots
        plots_list = job_consumption_plots(jobs)

    return {'plot_data': plots_list, 'error': error}

