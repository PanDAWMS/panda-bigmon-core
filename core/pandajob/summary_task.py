"""
A set of functions to get jobs from JOBS* and group them by task
"""
import logging
import copy
import time

from django.db import connection
from django.db.models import Count

from core.libs.dropalgorithm import drop_job_retries
from core.libs.exlib import drop_duplicates, get_tmp_table_name, insert_to_temp_table
from core.libs.job import parse_jobmetrics, add_job_category, job_states_count_by_param
from core.libs.jobconsumption import job_consumption_plots
from core.libs.task import taskNameDict, get_task_timewindow, get_task_scouts, calculate_metrics, get_task_time_archive_flag

from core.pandajob.utils import get_pandajob_models_by_year
from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4, Jobsarchived

from core.settings.config import DB_SCHEMA, DB_SCHEMA_PANDA_ARCH, DEPLOYMENT

import core.constants as const

_logger = logging.getLogger('bigpandamon')


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

    values = ['actualcorecount', 'modificationtime', 'jobsubstatus', 'pandaid', 'jobstatus',
              'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', 'jobmetrics',
              'nevents', 'hs06', 'cpuconsumptiontime', 'cpuconsumptionunit', 'transformation',
              'jobsetid', 'specialhandling', 'creationtime', 'pilottiming']
    if DEPLOYMENT == 'ORACLE_ATLAS':
        values.append('eventservice')
        values.append('hs06sec')

    if task_archive_flag >= 0:
        jobs.extend(Jobsdefined4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsactive4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        jobs.extend(Jobsarchived.objects.filter(**jquery_notime).extra(where=[extra]).values(*values))
        _logger.info("Got jobs from ADCR: {} sec".format(time.time() - start_time))
    if task_archive_flag <= 0 and DEPLOYMENT == 'ORACLE_ATLAS':
        # get list of jobsarchived models
        jobsarchived_models = get_pandajob_models_by_year(jquery['modificationtime__castdate__range'])
        if len(jobsarchived_models) > 0:
            for jam in jobsarchived_models:
                try:
                    jobs.extend(jam.objects.filter(**jquery).extra(where=[extra]).values(*values))
                except Exception as ex:
                    _logger.exception('Failed to get jobs from {} at ATLARC: \n{}'.format(jam, ex))
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
    """Getting top jobs by memory consumption that exceeded site limit"""
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
        select jeditaskid, pandaid, jobstatus from {DB_SCHEMA}.combined_wait_act_def_arch4 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
        )
        union all
        (
        select jeditaskid, pandaid, jobstatus from {DB_SCHEMA_PANDA_ARCH}.jobsarchived 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
        minus
        select jeditaskid, pandaid, jobstatus from {DB_SCHEMA_PANDA_ARCH}.jobsarchived 
            where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk ) 
                and pandaid in (
                    select pandaid from {DB_SCHEMA}.combined_wait_act_def_arch4 
                        where jeditaskid in (select id from {0} where TRANSACTIONKEY = :tk )
            )
        )
        )
        group by jeditaskid, jobstatus
        """.format(tmp_table, DB_SCHEMA=DB_SCHEMA, DB_SCHEMA_PANDA_ARCH=DB_SCHEMA_PANDA_ARCH)
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


def task_summary(query, limit=999999, view='all', sortby='taskid'):

    tasksummarydata = task_summary_data(query, limit=limit)
    tasks = {}
    totstates = {}
    totjobs = 0
    for state in const.JOB_STATES_SITE:
        totstates[state] = 0

    taskids = []
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskids.append({'jeditaskid': rec['jeditaskid']})
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskids.append({'taskid': rec['taskid']})
    
    tasknamedict = taskNameDict(taskids)
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskid = rec['jeditaskid']
            tasktype = 'JEDI'
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskid = rec['taskid']
            tasktype = 'old'
        else:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in const.JOB_STATES_SITE: 
            continue
        totjobs += count
        totstates[jobstatus] += count
        if taskid not in tasks:
            tasks[taskid] = {}
            tasks[taskid]['taskid'] = taskid
            if taskid in tasknamedict:
                tasks[taskid]['name'] = tasknamedict[taskid]
            else:
                tasks[taskid]['name'] = str(taskid)
            tasks[taskid]['count'] = 0
            tasks[taskid]['states'] = {}
            tasks[taskid]['statelist'] = []
            for state in const.JOB_STATES_SITE:
                tasks[taskid]['states'][state] = {}
                tasks[taskid]['states'][state]['name'] = state
                tasks[taskid]['states'][state]['count'] = 0
        tasks[taskid]['count'] += count
        tasks[taskid]['states'][jobstatus]['count'] += count
   
    if view == 'analysis':
        # Show only tasks starting with 'user.'
        kys = list(tasks.keys())
        for t in kys:
            if not str(tasks[t]['name'].encode('ascii', 'ignore')).startswith('user.'): del tasks[t]
    
    # Convert dict to summary list
    taskkeys = list(tasks.keys())
    taskkeys = sorted(taskkeys)
    fullsummary = []
    for taskid in taskkeys:
        for state in const.JOB_STATES_SITE:
            tasks[taskid]['statelist'].append(tasks[taskid]['states'][state])
        if tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count'] > 0:
            tasks[taskid]['pctfail'] = int(100. * float(tasks[taskid]['states']['failed']['count']) / (
                tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count']))
        else:
            tasks[taskid]['pctfail'] = 0
        fullsummary.append(tasks[taskid])
    
    # sorting
    if sortby in const.JOB_STATES_SITE:
        fullsummary = sorted(fullsummary, key=lambda x: x['states'][sortby], reverse=True)
    elif sortby == 'pctfail':
        fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)

    return fullsummary


def task_summary_data(query, limit=1000):
    summary = []
    querynotime = copy.deepcopy(query)
    del querynotime['modificationtime__castdate__range']
    
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobsarchived4.objects.filter(**query).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])

    summary.extend(Jobsactive4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobsarchived4.objects.filter(**query).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    
    return summary
