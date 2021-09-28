
import logging
import time
import copy
import random
import json
import numpy as np
from datetime import datetime, timedelta
from django.db import connection
from django.db.models import Count, Sum
from core.common.models import JediEvents, JediDatasetContents, JediDatasets, JediTaskparams, JediDatasetLocality, JediTasks
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4, Jobsarchived_y2015
from core.libs.exlib import dictfetchall, insert_to_temp_table, drop_duplicates, add_job_category, get_job_walltime, \
    job_states_count_by_param, get_tmp_table_name, parse_datetime, get_job_queuetime, convert_bytes
from core.libs.job import parse_jobmetrics
from core.libs.dropalgorithm import drop_job_retries, insert_dropped_jobs_to_tmp_table
from core.pandajob.utils import get_pandajob_models_by_year
from core.filebrowser.ruciowrapper import ruciowrapper

import core.constants as const

from core.libs.elasticsearch import create_esatlas_connection
from elasticsearch_dsl import Search

from core.settings.local import defaultDatetimeFormat

_logger = logging.getLogger('bigpandamon')


def is_event_service_task(jeditaskid):
    """
    Check if a task is EventService
    :param jeditaskid: int
    :return: eventservice: bool
    """
    eventservice = False

    query = {'jeditaskid': jeditaskid}
    task = list(JediTasks.objects.filter(**query).values('eventservice'))
    if len(task) > 0 and 'eventservice' in task[0] and task[0]['eventservice'] is not None and task[0]['eventservice'] == 1:
        eventservice = True

    return eventservice


def cleanTaskList(tasks, **kwargs):

    add_datasets_info = False
    add_datasets_locality = False
    sortby = None

    if 'add_datasets_info' in kwargs:
        add_datasets_info = kwargs['add_datasets_info']
    if 'add_datasets_locality' in kwargs:
        add_datasets_locality = kwargs['add_datasets_locality']
        add_datasets_info = True
    if 'sortby' in kwargs:
        sortby = kwargs['sortby']

    for task in tasks:
        if task['transpath']:
            task['transpath'] = task['transpath'].split('/')[-1]
        if task['statechangetime'] is None:
            task['statechangetime'] = task['modificationtime']
        if 'eventservice' in task:
            if task['eventservice'] == 1:
                task['eventservice'] = 'eventservice'
            else:
                task['eventservice'] = 'ordinary'
        if 'reqid' in task and task['reqid'] and task['reqid'] < 100000 and task['reqid'] > 100 \
                and task['reqid'] != 300 and 'tasktype' in task and not task['tasktype'].startswith('anal'):
            task['deftreqid'] = task['reqid']
        if 'corecount' in task and task['corecount'] is None:
            task['corecount'] = 1
        task['age'] = get_task_age(task)
        if 'campaign' in task:
            task['campaign_cut'] = ':'.join(task['campaign'].split(':')[1:]) if ':' in task['campaign'] else task['campaign']

    # Get status of input processing as indicator of task progress if requested
    if add_datasets_info:
        N_MAX_IN_QUERY = 100
        dvalues = ('jeditaskid', 'nfiles', 'nfilesfinished', 'nfilesfailed')
        dsquery = {
            'type__in': ['input', 'pseudo_input'],
            'masterid__isnull': True,
        }
        extra = '(1=1)'

        taskl = [t['jeditaskid'] for t in tasks if 'jeditaskid' in t]

        if len(taskl) <= N_MAX_IN_QUERY:
            dsquery['jeditaskid__in'] = taskl
        else:
            # Backend dependable
            tk = insert_to_temp_table(taskl)
            extra = "JEDITASKID in (SELECT ID FROM {} WHERE TRANSACTIONKEY={})".format(get_tmp_table_name(), tk)

        dsets = JediDatasets.objects.filter(**dsquery).extra(where=[extra]).values(*dvalues)
        dsinfo = {}
        if len(dsets) > 0:
            for ds in dsets:
                taskid = ds['jeditaskid']
                if taskid not in dsinfo:
                    dsinfo[taskid] = []
                dsinfo[taskid].append(ds)

        if add_datasets_locality:
            input_dataset_rse = get_dataset_locality(taskl)

        for task in tasks:
            if 'totevrem' not in task:
                task['totevrem'] = None
            dstotals = {
                'nfiles': 0,
                'nfilesfinished': 0,
                'nfilesfailed': 0,
                'pctfinished': 0,
                'pctfailed': 0,
            }
            if task['jeditaskid'] in dsinfo:
                nfiles = 0
                nfinished = 0
                nfailed = 0
                for ds in dsinfo[task['jeditaskid']]:
                    if int(ds['nfiles']) > 0:
                        nfiles += int(ds['nfiles'])
                        nfinished += int(ds['nfilesfinished'])
                        nfailed += int(ds['nfilesfailed'])
                if nfiles > 0:
                    dstotals['nfiles'] = nfiles
                    dstotals['nfilesfinished'] = nfinished
                    dstotals['nfilesfailed'] = nfailed
                    dstotals['pctfinished'] = round(100. * nfinished / nfiles, 2)
                    dstotals['pctfailed'] = round(100. * nfailed / nfiles, 2)

            task['dsinfo'] = dstotals
            task.update(dstotals)

    if sortby is not None:
        if sortby == 'time-ascending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'])
        if sortby == 'time-descending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'], reverse=True)
        if sortby == 'statetime-descending':
            tasks = sorted(tasks, key=lambda x: x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            tasks = sorted(tasks, key=lambda x: x['taskpriority'], reverse=True)
        elif sortby == 'nfiles':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['nfiles'], reverse=True)
        elif sortby == 'pctfinished':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfinished'], reverse=True)
        elif sortby == 'pctfailed':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfailed'], reverse=True)
        elif sortby == 'taskname':
            tasks = sorted(tasks, key=lambda x: x['taskname'])
        elif sortby == 'jeditaskid' or sortby == 'taskid':
            tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])
        elif sortby == 'cloud':
            tasks = sorted(tasks, key=lambda x: x['cloud'], reverse=True)
    else:
        tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])

    return tasks


def job_summary_for_task(query, extra="(1=1)", **kwargs):
    """An attempt to rewrite it moving dropping to db request level"""
    start_time = time.time()

    mode = 'nodrop'
    if 'mode' in kwargs:
        mode = kwargs['mode']

    task_archive_flag = 1
    if 'task_archive_flag' in kwargs and kwargs['task_archive_flag']:
        task_archive_flag = kwargs['task_archive_flag']
    jobs = []

    # getting jobs from DB
    jquery = copy.deepcopy(query)
    jquery_notime = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in jquery_notime:
        try:
            del jquery_notime['modificationtime__castdate__range']
        except:
            _logger.warning('failed to remove modificationtime range from jquery')

    values = ('actualcorecount', 'eventservice', 'modificationtime', 'jobsubstatus', 'pandaid', 'jobstatus',
              'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', 'jobmetrics',
              'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'cpuconsumptionunit', 'transformation',
              'jobsetid', 'specialhandling', 'creationtime')

    if task_archive_flag >= 0:
        jobs.extend(Jobsdefined4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsactive4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsarchived.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        _logger.info("Got jobs from ADCR: {} sec".format(time.time() - start_time))
    if task_archive_flag <= 0:
        # get list of jobsarchived models
        jobsarchived_models = get_pandajob_models_by_year(jquery['modificationtime__castdate__range'])
        if len(jobsarchived_models) > 0:
            for jam in jobsarchived_models:
                jobs.extend(jam.objects.filter(**jquery).extra(where=[extra]).values(*values))
            _logger.info("Got jobs from ATLARC: {} sec".format(time.time() - start_time))
    _logger.info("Got jobs: {} sec".format(time.time() - start_time))

    # drop duplicate jobs
    jobs = drop_duplicates(jobs, id='pandaid')
    _logger.info("Dropped jobs: {} sec".format(time.time() - start_time))

    if mode == 'drop':
        jobs, dj, dmj = drop_job_retries(jobs, jquery['jeditaskid'], is_return_dropped_jobs=False)
        _logger.info("Dropped job retries (drop mode): {} sec".format(time.time() - start_time))

    # determine jobs category (build, run or merge)
    jobs = add_job_category(jobs)
    _logger.info("Determine job category: {} sec".format(time.time() - start_time))

    # parse job metrics and add to job dict
    jobs = parse_jobmetrics(jobs)
    _logger.info("Parsed and added job metrics: {} sec".format(time.time() - start_time))

    # prepare data for job consumption plots
    plots_list = job_consumption_plots(jobs)
    _logger.info("Prepared job consumption plots: {} sec".format(time.time() - start_time))

    # jobs states aggregation by category
    job_summary_list = job_states_count_by_param(jobs, param='category')
    job_summary_list_ordered = []
    job_category_order = ['build', 'run', 'merge']
    for jc in job_category_order:
        for jcs in job_summary_list:
            if jc == jcs['value']:
                job_summary_list_ordered.append(jcs)
    _logger.info("Got summary by job category: {} sec".format(time.time() - start_time))

    # find scouts
    scouts = get_task_scouts(jobs)
    _logger.info("Got scouts: {} sec".format(time.time() - start_time))

    # calculate metrics
    metrics = calculate_metrics(jobs, metrics_names=[
        'resimevents_avg', 'resimeventspernevents_avgpercent', 'resimevents_sum'])

    return plots_list, job_summary_list_ordered, scouts, metrics


def get_task_scouts(jobs):
    """
    Get PanDAIDs of selected scouting metrics for a task
    :param jobs: list of dicts
    :return: dict:
    """
    scouts_dict = {}
    scout_types = ['cpuTime', 'walltime', 'ramCount', 'ioIntensity', 'outDiskCount']
    for jst in scout_types:
        scouts_dict[jst] = []

    for job in jobs:
        for jst in scout_types:
            if 'jobmetrics' in job and 'scout=' in job['jobmetrics'] and jst in job['jobmetrics'][job['jobmetrics'].index('scout='):]:
                scouts_dict[jst].append(job['pandaid'])

    # remove scout type if no scouts
    st_to_remove = []
    for jst, jstd in scouts_dict.items():
        if len(jstd) == 0:
            st_to_remove.append(jst)
    for st in st_to_remove:
        if st in scouts_dict:
            del scouts_dict[st]

    return scouts_dict


def calculate_metrics(jobs, metrics_names):
    """
    Calculate job metrics for a task
    :param jobs:
    :param metrics_names:
    :return:
    """
    metrics_def_dict = {mn: {'metric': mn.split('_')[0], 'agg': mn.split('_')[1], 'data': [], 'value': -1} for mn in metrics_names}

    for job in jobs:
        if job['category'] == 'run' and job['jobstatus'] == 'finished':
            for mn, mdata in metrics_def_dict.items():
                if 'per' in mdata['metric']:
                    if mdata['metric'].split('per')[0] in job and mdata['metric'].split('per')[1] in job and job[mdata['metric'].split('per')[1]] > 0:
                        mdata['data'].append(job[mdata['metric'].split('per')[0]]/(1.0*job[mdata['metric'].split('per')[1]]))
                elif mdata['metric'] in job and job[mdata['metric']]:
                    mdata['data'].append(job[mdata['metric']])

    for mn, mdata in metrics_def_dict.items():
        if 'avg' in mdata['agg']:
            mdata['value'] = sum(mdata['data'])/(1.0*len(mdata['data'])) if len(mdata['data']) > 0 else -1
        if 'sum' in mdata['agg']:
            mdata['value'] = sum(mdata['data'])

    metrics = {}
    for mn, mdata in metrics_def_dict.items():
        if mdata['value'] > 0:
            if 'percent' in mdata['agg']:
                metrics[mn] = round(mdata['value'] * 100.0, 2)
            else:
                metrics[mn] = round(mdata['value'], 2)

    return metrics


def job_consumption_plots(jobs):
    start_time = time.time()
    plots_dict = {}
    plot_details = {
        'nevents_sum_finished': {
            'type': 'pie', 'group_by': 'computingsite',
            'title': 'Number of events', 'xlabel': 'N events', 'ylabel': 'N jobs'},
        'nevents_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Number of events', 'xlabel': 'N events', 'ylabel': 'N jobs'},
        'resimevents_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Resim events (finished jobs)', 'xlabel': 'N resim events', 'ylabel': 'N jobs'},
        'maxpss_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Max PSS (finished jobs)', 'xlabel': 'MaxPSS, MB', 'ylabel': 'N jobs'},
        'maxpsspercore_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Max PSS/core (finished jobs)', 'xlabel': 'MaxPSS per core, MB', 'ylabel': 'N jobs'},
        'walltime_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Walltime (finished jobs)', 'xlabel': 'Walltime, s', 'ylabel': 'N jobs'},
        'walltimeperevent_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Walltime/event (finished jobs)', 'xlabel': 'Walltime per event, s', 'ylabel': 'N jobs'},
        'queuetime_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Time to start (finished jobs)', 'xlabel': 'Time to start, s', 'ylabel': 'N jobs'},
        'hs06s_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'HS06s (finished jobs)', 'xlabel': 'HS06s', 'ylabel': 'N jobs'},
        'cputime_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'CPU time (finished jobs)', 'xlabel': 'CPU time, s', 'ylabel': 'N jobs'},
        'cputimeperevent_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'CPU time/event (finished jobs)', 'xlabel': 'CPU time, s', 'ylabel': 'N jobs'},
        'dbtime_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'DB time (finished jobs)', 'xlabel': 'DB time, s', 'ylabel': 'N jobs'},
        'dbdata_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'DB data (finished jobs)', 'xlabel': 'DB data, MB', 'ylabel': 'N jobs'},
        'workdirsize_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Workdir size (finished jobs)', 'xlabel': 'Workdir, MB', 'ylabel': 'N jobs'},
        'leak_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Memory leak (finished jobs)', 'xlabel': 'Memory leak, B/s', 'ylabel': 'N jobs'},
        'nprocesses_finished': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'N processes (finished jobs)', 'xlabel': 'N proceeses', 'ylabel': 'N jobs'},

        'maxpss_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Maximum PSS (failed jobs)', 'xlabel': 'MaxPSS, MB', 'ylabel': 'N jobs'},
        'maxpsspercore_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite', 'title': 'Max PSS/core (failed jobs)',
            'xlabel': 'MaxPSS per core, MB', 'ylabel': 'N jobs'},
        'walltime_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Walltime (failed jobs)', 'xlabel': 'walltime, s', 'ylabel': 'N jobs'},
        'queuetime_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Time to start (failed jobs)', 'xlabel': 'Time to start, s', 'ylabel': 'N jobs'},
        'walltimeperevent_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Walltime/event (failed jobs)', 'xlabel': 'Walltime per event, s', 'ylabel': 'N jobs'},
        'hs06s_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'HS06s (failed jobs)', 'xlabel': 'HS06s', 'ylabel': 'N jobs'},
        'cputime_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'CPU time (failed jobs)', 'xlabel': 'CPU time, s', 'ylabel': 'N jobs'},
        'cputimeperevent_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'CPU time/event (failed jobs)', 'xlabel': 'CPU time, s', 'ylabel': 'N jobs'},
        'dbtime_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'DB time (failed jobs)', 'xlabel': 'DB time, s', 'ylabel': 'N jobs'},
        'dbdata_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'DB data (failed jobs)', 'xlabel': 'DB data, MB', 'ylabel': 'N jobs'},
        'workdirsize_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Workdir size (failed jobs)', 'xlabel': 'Workdir, MB', 'ylabel': 'N jobs'},
        'leak_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'Memory leak (failed jobs)', 'xlabel': 'Memory leak, B/s', 'ylabel': 'N jobs'},
        'nprocesses_failed': {
            'type': 'stack_bar', 'group_by': 'computingsite',
            'title': 'N processes (failed jobs)', 'xlabel': 'N proceeses', 'ylabel': 'N jobs'},

        'walltime_bycpuunit_finished': {
            'type': 'stack_bar', 'group_by': 'cpuconsumptionunit',
            'title': 'Walltime (finished jobs)', 'xlabel': 'Walltime, s', 'ylabel': 'N jobs'},
        'walltime_bycpuunit_failed': {
            'type': 'stack_bar', 'group_by': 'cpuconsumptionunit',
            'title': 'Walltime (failed jobs)', 'xlabel': 'Walltime, s', 'ylabel': 'N jobs'},
    }

    plots_data = {}
    for pname, pd in plot_details.items():
        if pd['type'] not in plots_data:
            plots_data[pd['type']] = {}
        plots_data[pd['type']][pname] = {
            'build': {},
            'run': {},
            'merge': {}
        }

    MULTIPLIERS = {
        "SEC": 1.0,
        "MIN": 60.0,
        "HOUR": 60.0 * 60.0,
        "MB": 1024.0,
        "GB": 1024.0 * 1024.0,
    }

    # prepare data for plots
    for job in jobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if 'duration' not in job:
            job['duration'] = get_job_walltime(job)
        if 'queuetime' not in job:
            job['queuetime'] = get_job_queuetime(job)

        if job['jobstatus'] in ('finished', 'failed'):
            for pname, pd in plot_details.items():
                if pd['group_by'] in job and job[pd['group_by']] not in plots_data[pd['type']][pname][job['category']]:
                    plots_data[pd['type']][pname][job['category']][job[pd['group_by']]] = []
        else:
            continue

        if 'nevents' in job and job['nevents'] > 0 and job['jobstatus'] == 'finished':
            plots_data['stack_bar']['nevents' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['nevents'])

            plots_data['pie']['nevents_sum_finished'][job['category']][job['computingsite']].append(job['nevents'])

        if 'maxpss' in job and job['maxpss'] is not None and job['maxpss'] >= 0:
            plots_data['stack_bar']['maxpss' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['maxpss'] / MULTIPLIERS['MB']
            )
            if job['actualcorecount'] and job['actualcorecount'] > 0:
                plots_data['stack_bar']['maxpsspercore' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['maxpss'] / MULTIPLIERS['MB'] / job['actualcorecount']
                )

        if 'hs06sec' in job and job['hs06sec']:
            plots_data['stack_bar']['hs06s' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['hs06sec'])

        if 'queuetime' in job and job['queuetime']:
            plots_data['stack_bar']['queuetime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['queuetime'])

        if 'duration' in job and job['duration']:
            plots_data['stack_bar']['walltime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['duration'])
            if 'walltimeperevent' in job:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['walltimeperevent']
                )
            elif 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['duration'] / (job['nevents'] * 1.0)
                )

            if 'cpuconsumptionunit' in job and job['cpuconsumptionunit']:
                plots_data['stack_bar']['walltime_bycpuunit' + '_' + job['jobstatus']][job['category']][job['cpuconsumptionunit']].append(job['duration'])

        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None and job['cpuconsumptiontime'] > 0:
            plots_data['stack_bar']['cputime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['cpuconsumptiontime']
            )
            if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['cputimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['cpuconsumptiontime'] / (job['nevents'] * 1.0)
                )
        if 'leak' in job and job['leak'] is not None:
            plots_data['stack_bar']['leak' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['leak']
            )
        if 'nprocesses' in job and job['nprocesses'] is not None and job['nprocesses'] > 0:
            plots_data['stack_bar']['nprocesses' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['nprocesses']
            )
        if 'workdirsize' in job and job['workdirsize'] is not None and job['workdirsize'] > 0:
            plots_data['stack_bar']['workdirsize' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                convert_bytes(job['workdirsize'], output_unit='MB')
            )
        if 'dbtime' in job and job['dbtime'] is not None and job['dbtime'] > 0:
            plots_data['stack_bar']['dbtime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['dbtime']
            )
        if 'dbdata' in job and job['dbdata'] is not None and job['dbdata'] > 0:
            plots_data['stack_bar']['dbdata' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                convert_bytes(job['dbdata'], output_unit='MB')
            )

        if 'resimevents' in job and job['resimevents'] and job['jobstatus'] == 'finished':
            plots_data['stack_bar']['resimevents' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['resimevents'])

    _logger.info("prepare plots data: {} sec".format(time.time() - start_time))

    # remove empty categories
    cat_to_remove = {'build': True, 'run': True, 'merge': True}
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, cd in pd.items():
                if len(cd) > 0:
                    cat_to_remove[cat] = False
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, is_remove in cat_to_remove.items():
                if is_remove:
                    del pd[cat]

    # add 'all' category to histograms
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            all_cat = {}
            for cat, cd in pd.items():
                for site, sd in cd.items():
                    if site not in all_cat:
                        all_cat[site] = []
                    all_cat[site].extend(sd)
            pd['all'] = all_cat

    # remove empty plots
    plots_to_remove = []
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            if sum([len(site_data) for site, site_data in pd['all'].items()]) == 0:
                plots_to_remove.append(pm)
    for pm in plots_to_remove:
        for pt, td in plots_data.items():
            if pm in td:
                del plots_data[pt][pm]
                del plot_details[pm]
    _logger.info("clean up plots data: {} sec".format(time.time() - start_time))

    # prepare stack histogram data
    for pname, pd in plot_details.items():
        if pd['type'] == 'stack_bar':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }

            for cat, cd in plots_data[pd['type']][pname].items():
                n_decimals = 0
                if 'per' in pname:
                    n_decimals = 2
                stats, columns = build_stack_histogram(cd, n_decimals=n_decimals)
                plots_dict[pname]['data'][cat] = {
                    'columns': columns,
                    'stats': stats,
                }
        elif pd['type'] == 'pie':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }
            for cat, cd in plots_data[pd['type']][pname].items():

                columns = []
                for site in cd:
                    columns.append([site, sum(cd[site])])

                plots_dict[pname]['data'][cat] = {
                    'columns': sorted(columns, key=lambda x: -x[1]),
                }
            if max([len(i['columns']) for i in plots_dict[pname]['data'].values()]) > 15:
                plots_dict[pname]['details']['legend_position'] = 'bottom'
                plots_dict[pname]['details']['size'] = [800, 300 + 20 * int(max([len(i['columns']) for i in plots_dict[pname]['data'].values()])/6)]
    _logger.info("built plots: {} sec".format(time.time() - start_time))

    # transform dict to list
    plots_list = []
    for pname, pdata in plots_dict.items():
        plots_list.append({'name': pname, 'data': pdata})

    return plots_list


def build_stack_histogram(data_raw, **kwargs):
    """
    Prepare stack histogram data and calculate mean and std metrics
    :param data_raw: dict of lists
    :param kwargs:
    :return:
    """

    n_decimals = 0
    if 'n_decimals' in kwargs:
        n_decimals = kwargs['n_decimals']

    N_BINS_MAX = 50
    stats = []
    columns = []

    data_all = []
    for site, sd in data_raw.items():
        data_all.extend(sd)

    stats.append(np.average(data_all) if not np.isnan(np.average(data_all)) else 0)
    stats.append(np.std(data_all) if not np.isnan(np.std(data_all)) else 0)

    bins_all, ranges_all = np.histogram(data_all, bins='auto')
    if len(ranges_all) > N_BINS_MAX + 1:
        bins_all, ranges_all = np.histogram(data_all, bins=N_BINS_MAX)
    ranges_all = list(np.round(ranges_all, n_decimals))

    x_axis_ticks = ['x']
    x_axis_ticks.extend(ranges_all[:-1])

    for stack_param, data in data_raw.items():
        column = [stack_param]
        column.extend(list(np.histogram(data, ranges_all)[0]))
        # do not add if all the values are zeros
        if sum(column[1:]) > 0:
            columns.append(column)

    # sort by biggest impact
    columns = sorted(columns, key=lambda x: sum(x[1:]), reverse=True)

    columns.insert(0, x_axis_ticks)

    return stats, columns


def event_summary_for_task(mode, query, **kwargs):
    """
    Event summary for a task.
    If drop mode, we need a transaction key (tk_dj) to except job retries. If it is not provided we do it here.
    :param mode: str (drop or nodrop)
    :param query: dict
    :return: eventslist: list of dict (number of events in different states)
    """
    tk_dj = -1
    if tk_dj in kwargs:
        tk_dj = kwargs['tk_dj']

    if mode == 'drop' and tk_dj == -1:
        # inserting dropped jobs to tmp table
        extra = '(1=1)'
        extra, tk_dj = insert_dropped_jobs_to_tmp_table(query, extra)

    eventservicestatelist = [
        'ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed', 'fatal', 'merged',
        'corrupted'
    ]
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    print ('getting events states summary')
    if mode == 'drop':
        jeditaskid = query['jeditaskid']
        # explicit time window for better searching over partitioned JOBSARCHIVED
        time_field = 'modificationtime'
        time_format = "YYYY-MM-DD HH24:MI:SS"
        if 'creationdate__range' in query:
            extra_str = " AND ( {} > TO_DATE('{}', '{}') AND {} < TO_DATE('{}', '{}') )".format(
                time_field, query['creationdate__range'][0], time_format,
                time_field, query['creationdate__range'][1], time_format)
        else:  # if no time range -> look in last 3 months
            extra_str = 'AND {} > SYSDATE - 90'.format(time_field)
        equerystr = """
            SELECT 
            /*+ cardinality(tmp 10) INDEX_RS_ASC(ev JEDI_EVENTS_PK) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  
                SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS EVCOUNT, 
                ev.STATUS 
            FROM ATLAS_PANDA.JEDI_EVENTS ev, 
                (select ja4.pandaid from ATLAS_PANDA.JOBSARCHIVED4 ja4 
                        where ja4.jeditaskid = :tid and ja4.eventservice is not NULL and ja4.eventservice != 2 
                            and ja4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union 
                select ja.pandaid from ATLAS_PANDAARCH.JOBSARCHIVED ja 
                    where ja.jeditaskid = :tid and ja.eventservice is not NULL and ja.eventservice != 2 {} 
                        and ja.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jav4.pandaid from ATLAS_PANDA.jobsactive4 jav4 
                    where jav4.jeditaskid = :tid and jav4.eventservice is not NULL and jav4.eventservice != 2 
                        and jav4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jw4.pandaid from ATLAS_PANDA.jobswaiting4 jw4 
                    where jw4.jeditaskid = :tid and jw4.eventservice is not NULL and jw4.eventservice != 2 
                        and jw4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jd4.pandaid from ATLAS_PANDA.jobsdefined4 jd4 
                    where jd4.jeditaskid = :tid and jd4.eventservice is not NULL and jd4.eventservice != 2 
                        and jd4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                )  j
            WHERE ev.PANDAID = j.pandaid AND ev.jeditaskid = :tid 
            GROUP BY ev.STATUS
        """.format(extra_str)
        new_cur = connection.cursor()
        new_cur.execute(equerystr, {'tid': jeditaskid, 'tkdj': tk_dj})

        evtable = dictfetchall(new_cur)

        for ev in evtable:
            essummary[eventservicestatelist[ev['STATUS']]] += ev['EVCOUNT']
    if mode == 'nodrop':
        event_counts = []
        equery = {'jeditaskid': query['jeditaskid']}
        event_counts.extend(
            JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
        for state in event_counts:
            essummary[eventservicestatelist[state['status']]] = state['count']

    # creating ordered list of eventssummary
    for state in eventservicestatelist:
        eventstatus = {}
        eventstatus['statusname'] = state
        eventstatus['count'] = essummary[state]
        eventslist.append(eventstatus)

    return eventslist


def datasets_for_task(jeditaskid):
    """
    Getting list of datasets corresponding to a task and file state summary
    :param jeditaskid: int
    :return: dsets: list of dicts
    :return: dsinfo: dict
    """
    dsets = []
    dsinfo = {
        'nfiles': 0,
        'nfilesfinished': 0,
        'nfilesfailed': 0,
        'pctfinished': 0.0,
        'pctfailed': 0,
        'neventsTot': 0,
        'neventsUsedTot': 0,
        'neventsOutput': 0,
    }

    dsquery = {
        'jeditaskid': jeditaskid,
    }
    values = (
        'jeditaskid', 'datasetid', 'datasetname', 'containername', 'type', 'masterid', 'streamname', 'status',
        'storagetoken', 'nevents', 'neventsused', 'neventstobeused', 'nfiles', 'nfilesfinished', 'nfilesfailed'
    )
    dsets.extend(JediDatasets.objects.filter(**dsquery).values(*values))

    scope = ''
    newdslist = []
    if len(dsets) > 0:
        for ds in dsets:
            if len(ds['datasetname']) > 0:
                if not str(ds['datasetname']).startswith('user'):
                    scope = str(ds['datasetname']).split('.')[0]
                else:
                    scope = '.'.join(str(ds['datasetname']).split('.')[:2])
                if ':' in scope:
                    scope = str(scope).split(':')[0]
                ds['scope'] = scope
            newdslist.append(ds)

            # input primary datasets
            if ds['type'] in ['input', 'pseudo_input'] and ds['masterid'] is None:
                if not ds['nevents'] is None and int(ds['nevents']) > 0:
                    dsinfo['neventsTot'] += int(ds['nevents'])
                if not ds['neventsused'] is None and int(ds['neventsused']) > 0:
                    dsinfo['neventsUsedTot'] += int(ds['neventsused'])

                if int(ds['nfiles']) > 0:
                    ds['percentfinished'] = int(100. * int(ds['nfilesfinished']) / int(ds['nfiles']))
                    dsinfo['nfiles'] += int(ds['nfiles'])
                    dsinfo['nfilesfinished'] += int(ds['nfilesfinished'])
                    dsinfo['nfilesfailed'] += int(ds['nfilesfailed'])
            elif ds['type'] in ('output', ):
                dsinfo['neventsOutput'] += int(ds['nevents']) if ds['nevents'] and ds['nevents'] > 0 else 0

        dsets = newdslist
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())

        dsinfo['pctfinished'] = round(100.*dsinfo['nfilesfinished']/dsinfo['nfiles'], 2) if dsinfo['nfiles'] > 0 else 0
        dsinfo['pctfailed'] = round(100.*dsinfo['nfilesfailed']/dsinfo['nfiles'], 2) if dsinfo['nfiles'] > 0 else 0

    return dsets, dsinfo


def input_summary_for_task(taskrec, dsets):
    """
    The function returns:
    Input event chunks list for separate table
    Input event chunks summary by states
    A dictionary with tk as key and list of input files IDs that is needed for jobList view filter
    """
    jeditaskid = taskrec['jeditaskid']
    # Getting statuses of inputfiles
    if datetime.strptime(taskrec['creationdate'], defaultDatetimeFormat) < \
            datetime.strptime('2018-10-22 10:00:00', defaultDatetimeFormat):
        ifsquery = """
            select  
            ifs.jeditaskid,
            ifs.datasetid,
            ifs.fileid,
            ifs.lfn, 
            ifs.startevent, 
            ifs.endevent, 
            ifs.attemptnr, 
            ifs.maxattempt, 
            ifs.failedattempt, 
            ifs.maxfailure,
            case when cstatus not in ('running') then cstatus 
                 when cstatus in ('running') and esmergestatus is null then cstatus
                 when cstatus in ('running') and esmergestatus = 'esmerge_transferring' then 'transferring' 
                 when cstatus in ('running') and esmergestatus = 'esmerge_merging' then 'merging' 
            end as status
            from (
                select jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus, count(f.esmergestatus) as n
                  from
                (select jd.jeditaskid, jd.datasetid, jdc.fileid, 
                    jdc.lfn, jdc.startevent, jdc.endevent, 
                    jdc.attemptnr, jdc.maxattempt, jdc.failedattempt, jdc.maxfailure,
                    case when (jdc.maxattempt <= jdc.attemptnr or jdc.failedattempt >= jdc.maxfailure) and jdc.status = 'ready' then 'failed' else jdc.status end as cstatus
                 from atlas_panda.jedi_dataset_contents jdc, 
                     atlas_panda.jedi_datasets jd
                 where jd.datasetid = jdc.datasetid 
                    and jd.jeditaskid = {}
                    and jd.masterid is NULL
                    and jdc.type in ( 'input', 'pseudo_input')
                ) jdcf 
                LEFT JOIN
                (select f4.jeditaskid, f4.fileid, f4.datasetid, f4.pandaid, 
                    case when ja4.jobstatus = 'transferring' and ja4.eventservice = 2 then 'esmerge_transferring' when ja4.eventservice = 2 then 'esmerge_merging' else null end as esmergestatus
                 from atlas_panda.filestable4 f4, ATLAS_PANDA.jobsactive4 ja4 
                 where f4.pandaid = ja4.pandaid and f4.type in ( 'input', 'pseudo_input') 
                            and f4.jeditaskid = {}
                ) f
                on jdcf.datasetid = f.datasetid and jdcf.fileid = f.fileid
                group by jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus
            ) ifs """.format(jeditaskid, jeditaskid)

        cur = connection.cursor()
        cur.execute(ifsquery)
        inputfiles = cur.fetchall()
        cur.close()

        inputfiles_names = ['jeditaskid', 'datasetid', 'fileid', 'lfn', 'startevent', 'endevent', 'attemptnr',
                            'maxattempt', 'failedattempt', 'maxfailure', 'procstatus']
        inputfiles_list = [dict(zip(inputfiles_names, row)) for row in inputfiles]

    else:
        ifsquery = {}
        ifsquery['jeditaskid'] = jeditaskid
        indsids = [ds['datasetid'] for ds in dsets if ds['type'] == 'input' and ds['masterid'] is None]
        ifsquery['datasetid__in'] = indsids if len(indsids) > 0 else [-1,]
        inputfiles_list = []
        inputfiles_list.extend(JediDatasetContents.objects.filter(**ifsquery).values())

    # counting of files in different states and building list of fileids for jobList
    inputfiles_counts = {}
    inputfilesids_states = {}
    dsids = []
    for inputfile in inputfiles_list:
        if inputfile['procstatus'] not in inputfiles_counts:
            inputfiles_counts[inputfile['procstatus']] = 0
        if inputfile['procstatus'] not in inputfilesids_states:
            inputfilesids_states[inputfile['procstatus']] = []
        if inputfile['datasetid'] not in dsids:
            dsids.append(inputfile['datasetid'])
        inputfiles_counts[inputfile['procstatus']] += 1
        inputfilesids_states[inputfile['procstatus']].append(inputfile['fileid'])

    inputfiles_tk = {}
    ifs_states = ['ready', 'queued', 'running', 'merging', 'transferring', 'finished', 'failed']
    ifs_summary = []
    for ifstate in ifs_states:
        ifstatecount = 0
        tk = random.randrange(100000000)
        if ifstate in inputfiles_counts.keys():
            ifstatecount = inputfiles_counts[ifstate]
            inputfiles_tk[tk] = inputfilesids_states[ifstate]
        ifs_summary.append({'name': ifstate, 'count': ifstatecount, 'tk': tk, 'ds': dsids})

    return inputfiles_list, ifs_summary, inputfiles_tk


def job_summary_for_task_light(taskrec):
    """
    Light version of jobSummary for ES tasks specifically. Nodrop mode by default. See ATLASPANDA-466 for details.
    :param taskrec:
    :return:
    """
    jeditaskidstr = str(taskrec['jeditaskid'])
    statelistlight = ['defined', 'assigned', 'activated', 'starting', 'running', 'holding', 'transferring', 'finished',
                      'failed', 'cancelled']
    estypes = ['es', 'esmerge', 'jumbo', 'unknown']

    # create structure and fill the dicts by 0 values
    jobSummaryLight = {}
    jobSummaryLightSplitted = {}
    for state in statelistlight:
        jobSummaryLight[str(state)] = 0
    for estype in estypes:
        jobSummaryLightSplitted[estype] = {}
        for state in statelistlight:
            jobSummaryLightSplitted[estype][str(state)] = 0

    js_count_list = []
    # decide which tables to query, if -1: only atlarc, 1: adcr, 0: both
    task_archive_flag = get_task_time_archive_flag(get_task_timewindow(taskrec, format_out='datatime'))

    if task_archive_flag >= 0:
        jsquery = """
            select jobstatus, case eventservice when 1 then 'es' when 5 then 'es' when 2 then 'esmerge' when 4 then 'jumbo' else 'unknown' end, count(pandaid) as njobs from (
            (
            select pandaid, es as eventservice, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
            )
            union all
            (
            select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid
            minus
            select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid and pandaid in (
                select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
                )
            )
            )
            group by jobstatus, eventservice
        """
        cur = connection.cursor()
        cur.execute(jsquery, {'jtid': jeditaskidstr})
        js_count = cur.fetchall()
        cur.close()
        js_count_names = ['state', 'es', 'count']
        js_count_list = [dict(zip(js_count_names, row)) for row in js_count]

    # if old task go to ATLARC for jobs summary
    if task_archive_flag <= 0:
        js_count_raw_list = []
        jquery = {
            'jeditaskid': taskrec['jeditaskid'],
            'modificationtime__castdate__range': get_task_timewindow(taskrec, format_out='str')
        }
        jobsarchived_models = get_pandajob_models_by_year(get_task_timewindow(taskrec, format_out='str'))
        if len(jobsarchived_models) > 0:
            for jam in jobsarchived_models:
                js_count_raw_list.extend(jam.objects.filter(**jquery).values('eventservice', 'jobstatus').annotate(count=Count('pandaid')))
            _logger.info("Got jobs summary from ATLARC")
        if len(js_count_raw_list) > 0:
            for row in js_count_raw_list:
                tmp_dict = {
                    'state': row['jobstatus'],
                    'count': row['count'],
                }
                if row['eventservice']:
                    tmp_dict['es'] = const.EVENT_SERVICE_JOB_TYPES[row['eventservice']] if row['eventservice'] in const.EVENT_SERVICE_JOB_TYPES else 'unknown'
                else:
                    tmp_dict['es'] = 'unknown'
                js_count_list.append(tmp_dict)

    for row in js_count_list:
        if row['state'] in statelistlight:
            if not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLight[row['state']] += row['count']
            if row['es'] in estypes and not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLightSplitted[row['es']][row['state']] += row['count']
    # delete 'unknown' if count = 0
    if 'unknown' in jobSummaryLightSplitted and sum(v for v in jobSummaryLightSplitted['unknown'].values()) == 0:
        try:
            del jobSummaryLightSplitted['unknown']
        except:
            _logger.warning("Failed to delete empty unknown category in jobSummaryLightSplitted")

    # dict -> list for template
    jobsummarylight = [dict(name=state, count=jobSummaryLight[state]) for state in statelistlight]
    jobsummarylightsplitted = {}
    for estype, count_dict in jobSummaryLightSplitted.items():
        jobsummarylightsplitted[estype] = [dict(name=state, count=count_dict[state]) for state in statelistlight]

    return jobsummarylight, jobsummarylightsplitted


def get_top_memory_consumers(taskrec):

    jeditaskidstr = str(taskrec['jeditaskid'])
    topmemoryconsumedjobs = []
    tmcquerystr = """
    select jeditaskid, pandaid, computingsite, jobmaxpss, jobmaxpss_percore, sitemaxrss, sitemaxrss_percore, maxpssratio 
    from (
        select j.jeditaskid, j.pandaid, j.computingsite, j.jobmaxpss, j.jobmaxpss_percore, s.maxrss as sitemaxrss, 
            s.maxrss/s.corecount as sitemaxrss_percore, j.jobmaxpss_percore/(s.maxrss/s.corecount) as maxpssratio, 
            row_number() over (partition by jeditaskid order by j.jobmaxpss_percore/(s.maxrss/s.corecount) desc) as jobrank
        from atlas_pandameta.schedconfig s,
        (select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDA.jobsarchived4 
            where jeditaskid = :jdtsid and maxrss is not null
        union
        select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDAARCH.jobsarchived 
            where jeditaskid = :jdtsid  and maxrss is not null
        ) j
        where j.computingsite = s.nickname
    ) 
    where jobrank <= 3
    """
    try:
        cur = connection.cursor()
        cur.execute(tmcquerystr, {'jdtsid': jeditaskidstr})
        tmc_list = cur.fetchall()
        cur.close()
    except:
        tmc_list = []
    tmc_names = ['jeditaskid', 'pandaid', 'computingsite', 'jobmaxrss', 'jobmaxpss_percore',
                 'sitemaxrss', 'sitemaxrss_percore', 'maxrssratio']
    topmemoryconsumedjobs = [dict(zip(tmc_names, row)) for row in tmc_list]
    for row in topmemoryconsumedjobs:
        try:
            row['maxrssratio'] = int(row['maxrssratio'])
        except:
            row['maxrssratio'] = 0
        row['jobmaxpss_percore'] = round(row['jobmaxpss_percore']) if row['jobmaxpss_percore'] else 0
        row['sitemaxrss_percore'] = round(row['sitemaxrss_percore']) if row['sitemaxrss_percore'] else 0
    return topmemoryconsumedjobs


def get_job_state_summary_for_tasklist(tasks):
    """
    Getting job state summary for list of tasks. Nodrop mode only
    :return: taskJobStateSummary : dictionary
    """

    taskids = [int(task['jeditaskid']) for task in tasks]
    trans_key = insert_to_temp_table(taskids)

    tmp_table = get_tmp_table_name()

    jsquery = """
        select  jeditaskid, jobstatus, count(pandaid) as njobs from (
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
        )
        union all
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
        minus
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk ) 
                and pandaid in (
                    select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 
                        where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
            )
        )
        )
        group by jeditaskid, jobstatus
        """.format(tmp_table)
    cur = connection.cursor()
    cur.execute(jsquery, {'tk': trans_key})
    js_count_bytask = cur.fetchall()
    cur.close()

    js_count_bytask_names = ['jeditaskid', 'jobstatus', 'count']
    js_count_bytask_list = [dict(zip(js_count_bytask_names, row)) for row in js_count_bytask]

    # list -> dict
    js_count_bytask_dict = {}
    for row in js_count_bytask_list:
        if row['jeditaskid'] not in js_count_bytask_dict:
            js_count_bytask_dict[row['jeditaskid']] = {}
        if row['jobstatus'] not in js_count_bytask_dict[row['jeditaskid']]:
            js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] = 0
        js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] += int(row['count'])

    return js_count_bytask_dict


def get_task_params(jeditaskid):
    """
    Extract task and job parameter lists from CLOB in  Jedi_TaskParams table
    :param jeditaskid: int
    :return: taskparams: dict
    :return: jobparams: list
    """

    query = {'jeditaskid': jeditaskid}
    taskparams = JediTaskparams.objects.filter(**query).values()

    if len(taskparams) > 0:
        taskparams = taskparams[0]['taskparams']
    try:
        taskparams = json.loads(taskparams)
    except ValueError:
        pass

    return taskparams


def humanize_task_params(taskparams):
    """
    Prepare list of params for template output
    :param taskparams: dict
    :return: taskparams_list, jobparams_list
    """
    taskparams_list = []
    jobparams_list = []

    for k in taskparams:
        rec = {'name': k, 'value': taskparams[k]}
        taskparams_list.append(rec)
    taskparams_list = sorted(taskparams_list, key=lambda x: x['name'].lower())

    jobparams = taskparams['jobParameters']
    if 'log' in taskparams:
        jobparams.append(taskparams['log'])

    for p in jobparams:
        if p['type'] == 'constant':
            ptxt = p['value']
        elif p['type'] == 'template':
            ptxt = "<i>{} template:</i> value='{}' ".format(p['param_type'], p['value'])
            for v in p:
                if v in ['type', 'param_type', 'value']:
                    continue
                ptxt += "  {}='{}'".format(v, p[v])
        else:
            ptxt = '<i>unknown parameter type {}:</i> '.format(p['type'])
            for v in p:
                if v in ['type', ]:
                    continue
                ptxt += "  {}='{}'".format(v, p[v])
        jobparams_list.append(ptxt)
    jobparams_list = sorted(jobparams_list, key=lambda x: x.lower())

    return taskparams_list, jobparams_list


def get_hs06s_summary_for_task(query):
    """"""
    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}

    hquery = copy.deepcopy(query)
    hquery['jobstatus__in'] = ('finished', 'failed')

    if 'jeditaskid' in hquery:

        hs06sec_sum = []
        pj_models = get_pandajob_models_by_year(query['modificationtime__castdate__range'])

        for pjm in pj_models:
            hs06sec_sum.extend(pjm.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))

        if len(hs06sec_sum) > 0:
            for hs in hs06sec_sum:
                if hs['jobstatus'] == 'finished':
                    hs06sSum['finished'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                    hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                elif hs['jobstatus'] == 'failed':
                    hs06sSum['failed'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                    hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0

    return hs06sSum


def get_task_age(task):
    """
    :param task: dict of task params, creationtime is obligatory
    :return: age in days or -1 if not enough data provided
    """
    task_age = -1

    if 'endtime' in task and task['endtime'] is not None:
        endtime = parse_datetime(task['endtime']) if not isinstance(task['endtime'], datetime) else task['endtime']
    else:
        endtime = datetime.now()
    if 'creationdate' in task and task['creationdate'] is not None:
        creationtime = parse_datetime(task['creationdate']) if not isinstance(task['creationdate'], datetime) else task['creationdate']
    else:
        creationtime = None

    if endtime and creationtime:
        task_age = round((endtime-creationtime).total_seconds() / 60. / 60. / 24., 2)

    return task_age


def get_task_timewindow(task, **kwargs):
    """
    Return a list of two datetime when task run
    :param task:
    :return: timewindow: list of datetime or str
    """
    format_out = 'datetime'
    if 'format_out' in kwargs and kwargs['format_out'] == 'str':
        format_out = 'str'

    timewindow = [datetime.now(), datetime.now()]

    if 'creationdate' in task and task['creationdate']:
        timewindow[0] = task['creationdate'] if isinstance(task['creationdate'], datetime) else parse_datetime(task['creationdate'])
    else:
        timewindow[0] = datetime.now()

    if task['status'] in const.TASK_STATES_FINAL:
        if 'endtime' in task and task['endtime']:
            timewindow[1] = task['endtime'] if isinstance(task['endtime'], datetime) else parse_datetime(task['endtime'])
        elif 'modificationtime' in task and task['modificationtime']:
            timewindow[1] = task['modificationtime'] if isinstance(task['modificationtime'], datetime) else parse_datetime(task['modificationtime'])
        else:
            timewindow[1] = datetime.now()
    else:
        timewindow[1] = datetime.now()

    if format_out == 'str':
        timewindow = [t.strftime(defaultDatetimeFormat) for t in timewindow]

    return timewindow


def get_task_time_archive_flag(task_timewindow):
    """
    Decide which tables query, if -1: only atlarc, 1: adcr, 0: both
    :param timewindow: list of two datetime
    :return: task_age_flag: -1, 0 or 1
    """
    #
    task_age_flag = 1
    if task_timewindow[1] < datetime.now() - timedelta(days=365*3):
        task_age_flag = -1
    elif task_timewindow[0] > datetime.now() - timedelta(days=365*3) and task_timewindow[1] < datetime.now() - timedelta(days=365*2):
        task_age_flag = 0
    elif task_timewindow[0] > datetime.now() - timedelta(days=365*2):
        task_age_flag = 1

    return task_age_flag


def get_dataset_locality(jeditaskid):
    """
    Getting RSEs for a task input datasets
    :return:
    """
    N_IN_MAX = 100
    query = {}
    extra_str = ' (1=1) '
    if isinstance(jeditaskid, list) or isinstance(jeditaskid, tuple):
        if len(jeditaskid) > N_IN_MAX:
            trans_key = insert_to_temp_table(jeditaskid)
            tmp_table = get_tmp_table_name()
            extra_str += ' AND jeditaskid IN (SELEECT id FROM {} WHERE transactionkey = {} )'.format(tmp_table, trans_key)
        else:
            query['jeditaskid__in'] = jeditaskid
    elif isinstance(jeditaskid, int):
        query['jeditaskid'] = jeditaskid

    rse_list = JediDatasetLocality.objects.filter(**query).extra(where=[extra_str]).values()

    rse_dict = {}
    if len(rse_list) > 0:
        for item in rse_list:
            if item['jeditaskid'] not in rse_dict:
                rse_dict[item['jeditaskid']] = {}
            if item['datasetid'] not in rse_dict[item['jeditaskid']]:
                rse_dict[item['jeditaskid']][item['datasetid']] = []
            rse_dict[item['jeditaskid']][item['datasetid']].append({'rse': item['rse'], 'timestamp': item['timestamp']})

    return rse_dict


def get_prod_slice_by_taskid(jeditaskid):
    jsquery = """
        SELECT tasks.taskid, tasks.PR_ID, tasks.STEP_ID, datasets.SLICE from ATLAS_DEFT.T_PRODUCTION_TASK tasks 
        JOIN ATLAS_DEFT.T_PRODUCTION_STEP steps on tasks.step_id = steps.step_id 
        JOIN ATLAS_DEFT.T_INPUT_DATASET datasets ON datasets.IND_ID=steps.IND_ID  
        where tasks.taskid=:taskid
    """
    cur = connection.cursor()
    cur.execute(jsquery, {'taskid': jeditaskid})
    task_prod_info = cur.fetchall()
    cur.close()
    slice = None
    if task_prod_info:
        slice = task_prod_info[0][3]
    return slice


def get_logs_by_taskid(jeditaskid):

    tasks_logs = []

    connection = create_esatlas_connection()

    s = Search(using=connection, index='atlas_jedilogs-*')

    s = s.filter('term', **{'jediTaskID': jeditaskid})

    s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    response = s.execute()

    for agg in response['aggregations']['logName']['buckets']:
        for types in agg['type']['buckets']:
            type = types['key']
            for levelnames in types['logLevel']['buckets']:
                levelname = levelnames['key']
                tasks_logs.append({'jediTaskID': jeditaskid, 'logname': type, 'loglevel': levelname,
                                   'lcount': str(levelnames['doc_count'])})

    s = Search(using=connection, index='atlas_pandalogs-*')

    s = s.filter('term', **{'jediTaskID': jeditaskid})

    s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    response = s.execute()

    for agg in response['aggregations']['logName']['buckets']:
        for types in agg['type']['buckets']:
            type = types['key']
            for levelnames in types['logLevel']['buckets']:
                levelname = levelnames['key']
                tasks_logs.append({'jediTaskID': jeditaskid, 'logname': type, 'loglevel': levelname,
                                   'lcount': str(levelnames['doc_count'])})

    return tasks_logs


def taskNameDict(jobs):
    """
    Translate IDs to names. Awkward because models don't provide foreign keys to task records.
    :param jobs: list of dist
    :return:
    """
    N_MAX_IN_QUERY = 100
    jeditaskids = {}
    for job in jobs:
        if 'taskid' in job and job['taskid'] and job['taskid'] > 0:
            jeditaskids[job['taskid']] = 1
        if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0:
            jeditaskids[job['jeditaskid']] = 1
    jeditaskidl = list(jeditaskids.keys())

    # Write ids to temp table to avoid too many bind variables oracle error
    tasknamedict = {}
    if len(jeditaskidl) > 0:
        tquery = {}
        if len(jeditaskidl) < N_MAX_IN_QUERY:
            tquery['jeditaskid__in'] = jeditaskidl
            extra = "(1=1)"
        else:
            tmp_table_name = get_tmp_table_name()
            transaction_key = insert_to_temp_table(jeditaskidl)
            extra = 'JEDITASKID IN (SELECT ID FROM {} WHERE TRANSACTIONKEY = {})'.format(tmp_table_name, transaction_key)
        jeditasks = JediTasks.objects.filter(**tquery).extra(where=[extra]).values('taskname', 'jeditaskid')
        for t in jeditasks:
            tasknamedict[t['jeditaskid']] = t['taskname']

    return tasknamedict


def get_task_flow_data(jeditaskid):
    """
    Getting data for task data flow diagram
    RSE -> dataset -> PQ -> njobs in state
    :param jeditaskid: int
    :return: data for sankey diagram: list ['from', 'to', 'weight']
    """
    data = []
    # get datasets
    datasets = []
    dquery = {'jeditaskid': jeditaskid, 'type__in': ['input', 'pseudo_input'], 'masterid__isnull': True}
    datasets.extend(JediDatasets.objects.filter(**dquery).values('jeditaskid', 'datasetname', ))

    dataset_dict = {}
    for d in datasets:
        dname = d['datasetname'] if ':' not in d['datasetname'] else d['datasetname'].split(':')[1]
        dataset_dict[dname] = {'replica': {}, 'jobs': {}}

    # get jobs aggregated by status, computingsite and proddblock (input dataset name)
    jobs = []
    jquery = {'jeditaskid': jeditaskid, 'prodsourcelabel__in': ['user', 'managed'], }
    extra_str = "( processingtype not in ('pmerge') )"
    jvalues = ['proddblock', 'computingsite', 'jobstatus']
    jobs.extend(Jobsarchived4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsarchived.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsactive4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsdefined4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobswaiting4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))

    if len(jobs) > 0:
        for j in jobs:
            dname = j['proddblock'] if ':' not in j['proddblock'] else j['proddblock'].split(':')[1]
            if j['computingsite'] not in dataset_dict[dname]['jobs']:
                dataset_dict[dname]['jobs'][j['computingsite']] = {}
            job_state = j['jobstatus'] if j['jobstatus'] in const.JOB_STATES_FINAL else 'active'
            if job_state not in dataset_dict[dname]['jobs'][j['computingsite']]:
                dataset_dict[dname]['jobs'][j['computingsite']][job_state] = 0
            dataset_dict[dname]['jobs'][j['computingsite']][job_state] += j['njobs']

    # get RSE for datasets
    replicas = []
    if len(datasets) > 0:
        dids = []
        for d in datasets:
            did = {
                'scope': d['datasetname'].split(':')[0] if ':' in d['datasetname'] else d['datasetname'].split('.')[0],
                'name': d['datasetname'].split(':')[1] if ':' in d['datasetname'] else d['datasetname'],
                }
            dids.append(did)

        rw = ruciowrapper()
        replicas = rw.getRSEbyDID(dids)

        if replicas is not None and len(replicas) > 0:
            for r in replicas:
                if r['name'] in dataset_dict and 'TAPE' not in r['rse']:
                    dataset_dict[r['name']]['replica'][r['rse']] = {
                        'state': r['state'],
                        'available_pct': round(100.0 * r['available_length']/r['length'], 1) if r['length'] > 0 else 0
                    }

    # transform data for plot
    # TODO ...

    return {'datasets': dataset_dict, }
