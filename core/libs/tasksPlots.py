import time

from datetime import timedelta, datetime

import json

import numpy as np

from django.http import HttpResponse

from core.libs.CustomJSONSerializer import NpEncoder

from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name
from core.libs.task import drop_duplicates, add_job_category, job_consumption_plots

from core.pandajob.models import Jobsdefined4, Jobsarchived, Jobswaiting4, Jobsactive4, Jobsarchived4

pandaSites = {}


def getJobsData(request):

    data = {}
    idList = request.GET.get('idtasks', '')
    tasksList = getCacheEntry(request, idList, isData=True)
    if len(tasksList) == 0:
        return HttpResponse(data, status=500, content_type='application/json')
    else:
        results = get_jobs_plot_data(tasksList)
        # results = getJobsInfo(request, tasksList, idList)
        data = json.dumps(results, cls=NpEncoder)

    return HttpResponse(data, content_type='application/json')


def get_jobs_plot_data(taskid_list):

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
             'processingtype', 'transformation'

    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_str]).values(*values))

    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_str]).values(*values))

    # drop duplicate jobs
    jobs = drop_duplicates(jobs, id='pandaid')

    # determine jobs category (build, run or merge)
    jobs = add_job_category(jobs)

    # prepare data for job consumption plots
    plots_list = job_consumption_plots(jobs)

    return plots_list

