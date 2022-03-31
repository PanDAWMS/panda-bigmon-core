"""
Set of functions related to jobs metadata

Created by Tatiana Korchuganova on 05.03.2020
"""

import statistics
from core.libs.exlib import convert_bytes
from datetime import datetime, timedelta
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4
from core.common.models import JediJobRetryHistory
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table, drop_duplicates
from core.libs.datetimestrings import parse_datetime

import core.constants as const


def is_event_service(job):
    if 'eventservice' in job and job['eventservice'] is not None:
        if 'specialhandling' in job and job['specialhandling'] and (
                    job['specialhandling'].find('eventservice') >= 0 or job['specialhandling'].find('esmerge') >= 0 or (
                job['eventservice'] != 'ordinary' and job['eventservice'])) and job['specialhandling'].find('sc:') == -1:
                return True
        else:
            return False
    else:
        return False


def is_debug_mode(job):
    """
    Check if a job in a debug mode, i.e. tail of stdout available or real-time logging is activated in Pilot
    :param job: dict
    :return: bool
    """
    if ('specialhandling' in job and not job['specialhandling'] is None and 'debug' in job['specialhandling']) or (
            'commandtopilot' in job and job['commandtopilot'] is not None and len(job['commandtopilot']) > 0 and job['commandtopilot'] != 'tobekilled'
    ):
        return True
    else:
        return False


def is_job_active(jobststus):
    """
    Check if jobstatus is one of the active
    :param jobststus: str
    :return: True or False
    """
    end_status_list = ['finished', 'failed', 'cancelled', 'closed']
    if jobststus in end_status_list:
        return False

    return True


def get_job_queuetime(job):
    """
    :param job: dict of job params, starttime and creationtime is obligatory
    :return: queuetime in seconds or None if not enough data provided
    """
    queueutime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']
    else:
        starttime = None
    if 'creationtime' in job and job['creationtime'] is not None:
        creationtime = parse_datetime(job['creationtime']) if not isinstance(job['creationtime'], datetime) else job['creationtime']
    else:
        creationtime = None

    if starttime and creationtime:
        queueutime = (starttime-creationtime).total_seconds()

    return queueutime


def get_job_walltime(job):
    """
    :param job: dict of job params, starttime and endtime is obligatory;
                creationdate, statechangetime, and modificationtime are optional
    :return: walltime in seconds or None if not enough data provided
    """
    walltime = None

    if 'endtime' in job and job['endtime'] is not None:
        endtime = parse_datetime(job['endtime']) if not isinstance(job['endtime'], datetime) else job['endtime']
    elif 'statechangetime' in job and job['statechangetime'] is not None:
        endtime = parse_datetime(job['statechangetime']) if not isinstance(job['statechangetime'], datetime) else job['statechangetime']
    elif 'modificationtime' in job and job['modificationtime'] is not None:
        endtime = parse_datetime(job['modificationtime']) if not isinstance(job['modificationtime'], datetime) else job['modificationtime']
    else:
        endtime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']
    elif 'creationdate' in job and job['creationdate'] is not None:
        starttime = parse_datetime(job['creationdate']) if not isinstance(job['creationdate'], datetime) else job['creationdate']
    else:
        starttime = 0

    if starttime and endtime:
        walltime = (endtime-starttime).total_seconds()

    return walltime


def add_job_category(jobs):
    """
    Determine which category job belong to among: build, run or merge and add 'category' param to dict of a job
    Need 'processingtype', 'eventservice' and 'transformation' params to make a decision
    :param jobs: list of dicts
    :return: jobs: list of updated dicts
    """

    for job in jobs:
        if 'transformation' in job and 'build' in job['transformation']:
            job['category'] = 'build'
        elif 'processingtype' in job and job['processingtype'] == 'pmerge':
            job['category'] = 'merge'
        elif 'eventservice' in job and (job['eventservice'] == 2 or job['eventservice'] == 'esmerge'):
            job['category'] = 'merge'
        else:
            job['category'] = 'run'

    return jobs


def job_states_count_by_param(jobs, **kwargs):
    """
    Counting jobs in different states and group by provided param
    :param jobs:
    :param kwargs:
    :return:
    """
    param = 'category'
    if 'param' in kwargs:
        param = kwargs['param']

    job_states_count_dict = {}
    param_values = list(set([job[param] for job in jobs if param in job]))

    if len(param_values) > 0:
        for pv in param_values:
            job_states_count_dict[pv] = {}
            for state in const.JOB_STATES:
                job_states_count_dict[pv][state] = 0

    for job in jobs:
        job_states_count_dict[job[param]][job['jobstatus']] += 1

    job_summary_dict = {}
    for pv, data in job_states_count_dict.items():
        if pv not in job_summary_dict:
            job_summary_dict[pv] = []

            for state in const.JOB_STATES:
                statecount = {
                    'name': state,
                    'count': job_states_count_dict[pv][state],
                }
                job_summary_dict[pv].append(statecount)

    # dict -> list
    job_summary_list = []
    for key, val in job_summary_dict.items():
        tmp_dict = {
            'param': param,
            'value': key,
            'job_state_counts': val,
        }
        job_summary_list.append(tmp_dict)

    return job_summary_list


def get_job_list(query, **kwargs):

    MAX_ENTRIES__IN = 100

    jobs = []
    values = [
        'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid',
        'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite',
        'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid', 'attemptnr',
        'processingtype', 'transformation', 'creationtime', 'diskio'
    ]
    if 'values' in kwargs:
        values.extend(kwargs['values'])
        values = set(values)

    extra_str = "(1=1)"
    if 'extra_str' in kwargs and kwargs['extra_str'] != '':
        extra_str = kwargs['extra_str']

    if 'jeditaskid__in' in query and len(query['jeditaskid__in']) > MAX_ENTRIES__IN:
        # insert taskids to temp DB table
        tmp_table_name = get_tmp_table_name()
        tk_taskids = insert_to_temp_table(query['jeditaskid__in'])
        extra_str += " AND jeditaskid in (select ID from {} where TRANSACTIONKEY={})".format(tmp_table_name, tk_taskids)
        del query['jeditaskid__in']

    jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_str]).values(*values))

    if 'jeditaskid' in query or 'jeditaskid__in' in query or 'jeditaskid' in extra_str or (
        'modificationtime__castdate__range' in query and query['modificationtime__castdate__range'][0] < (
            datetime.now() - timedelta(days=3))) or (len(jobs) == 0 and 'pandaid' in query):
        jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_str]).values(*values))

    # drop duplicate jobs
    jobs = drop_duplicates(jobs, id='pandaid')

    # add job category
    jobs = add_job_category(jobs)

    return jobs


def parse_jobmetrics(jobs):
    """
    Parse and add the metrics to job dicts
    :param jobs: list
    :return: jobs
    """
    for job in jobs:
        if 'jobmetrics' in job and job['jobmetrics'] and len(job['jobmetrics']) > 0:
            jobmetrics = {str(jm.split('=')[0]).lower(): jm.split('=')[1] for jm in job['jobmetrics'].split(' ')}
            if 'actualcorecount' in jobmetrics:
                jobmetrics['nprocesses'] = jobmetrics['actualcorecount']
                del jobmetrics['actualcorecount']
            for jm in jobmetrics:
                try:
                    jobmetrics[jm] = float(jobmetrics[jm])
                except:
                    pass
            job.update(jobmetrics)

    return jobs


def calc_jobs_metrics(jobs, group_by='jeditaskid'):
    """
    Calculate interesting metrics, e.g. avg maxpss/core, avg job walltime, avg job queuetime, failedpct
    :param jobs: list of dicts
    :param group_by:
    :return metrics_dict: dict
    """
    metrics_dict = {
        'maxpss_per_actualcorecount': {'total': [], 'group_by': {}, 'agg': 'median'},
        'diskio': {'total': [], 'group_by': {}, 'agg': 'median'},
        'walltime': {'total': [], 'group_by': {}, 'agg': 'median'},
        'queuetime': {'total': [], 'group_by': {}, 'agg': 'median'},
        'failed': {'total': [], 'group_by': {}, 'agg': 'average'},
        'efficiency': {'total': [], 'group_by': {}, 'agg': 'median'},
        'attemptnr': {'total': [], 'group_by': {}, 'agg': 'average'},
        'walltime_loss': {'total': [], 'group_by': {}, 'agg': 'sum'},
        'cputime_loss': {'total': [], 'group_by': {}, 'agg': 'sum'},
    }

    # calc metrics
    for job in jobs:
        if group_by in job and job[group_by]:

            job['failed'] = 100 if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0
            job['attemptnr'] = job['attemptnr'] + 1

            if job['category'] == 'run':
                if 'maxpss' in job and job['maxpss'] and isinstance(job['maxpss'], int) and (
                        'actualcorecount' in job and isinstance(job['actualcorecount'], int) and job['actualcorecount'] > 0):
                    job['maxpss_per_actualcorecount'] = convert_bytes(1.0*job['maxpss']/job['actualcorecount'], output_unit='MB')

                job['diskio'] = job['diskio'] if 'diskio' in job and job['diskio'] is not None else 0

                job['walltime'] = get_job_walltime(job)
                job['queuetime'] = get_job_queuetime(job)

                if job['walltime'] and 'cpuconsumptiontime' in job and (
                        isinstance(job['cpuconsumptiontime'], int) and job['cpuconsumptiontime'] > 0) and (
                        'actualcorecount' in job and isinstance(job['actualcorecount'], int) and job['actualcorecount'] > 0):
                    job['efficiency'] = round(1.0*job['cpuconsumptiontime']/job['walltime']/job['actualcorecount'], 2)

                job['cputime'] = round(job['cpuconsumptiontime'] / 60. / 60., 2) if 'cpuconsumptiontime' in job and isinstance(job['cpuconsumptiontime'], int) else 0

                job['walltime'] = round(job['walltime'] / 60. / 60., 2) if job['walltime'] is not None else 0
                job['queuetime'] = round(job['queuetime'] / 60. / 60., 2) if job['queuetime'] is not None else 0

                job['walltime_loss'] = job['walltime'] if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0
                job['cputime_loss'] = job['cputime'] if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0

            for metric in metrics_dict:
                if metric in job and job[metric] is not None:
                    if job[group_by] not in metrics_dict[metric]['group_by']:
                        metrics_dict[metric]['group_by'][job[group_by]] = []
                    metrics_dict[metric]['group_by'][job[group_by]].append(job[metric])
                    metrics_dict[metric]['total'].append(job[metric])

    for metric in metrics_dict:
        if len(metrics_dict[metric]['total']) > 0:
            if metrics_dict[metric]['agg'] == 'median':
                metrics_dict[metric]['total'] = round(statistics.median(metrics_dict[metric]['total']), 2)
            elif metrics_dict[metric]['agg'] == 'average':
                metrics_dict[metric]['total'] = round(statistics.mean(metrics_dict[metric]['total']), 2)
        else:
            metrics_dict[metric]['total'] = -1
        for gbp in metrics_dict[metric]['group_by']:
            if len(metrics_dict[metric]['group_by'][gbp]) > 0:
                if metrics_dict[metric]['agg'] == 'median':
                    metrics_dict[metric]['group_by'][gbp] = round(statistics.median(metrics_dict[metric]['group_by'][gbp]), 2)
                elif metrics_dict[metric]['agg'] == 'average':
                    metrics_dict[metric]['group_by'][gbp] = round(statistics.mean(metrics_dict[metric]['group_by'][gbp]), 2)
            else:
                metrics_dict[metric]['group_by'][gbp] = -1

    return metrics_dict


def getSequentialRetries(pandaid, jeditaskid, countOfInvocations):
    retryquery = {}
    countOfInvocations.append(1)
    retryquery['jeditaskid'] = jeditaskid
    retryquery['newpandaid'] = pandaid
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('oldpandaid').reverse().values()
        newretries.extend(retries)
        for retry in retries:
            if retry['relationtype'] in ['merge', 'retry']:
                jsquery = {}
                jsquery['jeditaskid'] = jeditaskid
                jsquery['pandaid'] = retry['oldpandaid']
                values = ['pandaid', 'jobstatus', 'jeditaskid']
                jsjobs = []
                jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
                for job in jsjobs:
                    if job['jobstatus'] == 'failed':
                        for retry in newretries:
                            if (retry['oldpandaid'] == job['pandaid']):
                                retry['relationtype'] = 'retry'
                        newretries.extend(getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations))

    outlist = []
    added_keys = set()
    for row in newretries:
        lookup = row['oldpandaid']
        if lookup not in added_keys:
            outlist.append(row)
            added_keys.add(lookup)

    return outlist


def getSequentialRetries_ES(pandaid, jobsetid, jeditaskid, countOfInvocations, recurse=0):
    retryquery = {}
    retryquery['jeditaskid'] = jeditaskid
    retryquery['newpandaid'] = jobsetid
    retryquery['relationtype'] = 'jobset_retry'
    countOfInvocations.append(1)
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('oldpandaid').reverse().values()
        newretries.extend(retries)
        for retry in retries:
            jsquery = {}
            jsquery['jeditaskid'] = jeditaskid
            jsquery['jobstatus'] = 'failed'
            jsquery['jobsetid'] = retry['oldpandaid']
            values = ['pandaid', 'jobstatus', 'jobsetid', 'jeditaskid']
            jsjobs = []
            jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
            for job in jsjobs:
                if job['jobstatus'] == 'failed':
                    for retry in newretries:
                        if (retry['oldpandaid'] == job['jobsetid']):
                            retry['relationtype'] = 'retry'
                            retry['jobid'] = job['pandaid']

                        newretries.extend(getSequentialRetries_ES(job['pandaid'],
                                                                  jobsetid, job['jeditaskid'], countOfInvocations,
                                                                  recurse + 1))
    outlist = []
    added_keys = set()
    for row in newretries:
        if 'jobid' in row:
            lookup = row['jobid']
            if lookup not in added_keys:
                outlist.append(row)
                added_keys.add(lookup)
    return outlist


def getSequentialRetries_ESupstream(pandaid, jobsetid, jeditaskid, countOfInvocations, recurse=0):
    retryquery = {}
    retryquery['jeditaskid'] = jeditaskid
    retryquery['oldpandaid'] = jobsetid
    retryquery['relationtype'] = 'jobset_retry'
    countOfInvocations.append(1)
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').values()
        newretries.extend(retries)
        for retry in retries:
            jsquery = {}
            jsquery['jeditaskid'] = jeditaskid
            jsquery['jobsetid'] = retry['newpandaid']
            values = ['pandaid', 'jobstatus', 'jobsetid', 'jeditaskid']
            jsjobs = []
            jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
            for job in jsjobs:
                for retry in newretries:
                    if (retry['newpandaid'] == job['jobsetid']):
                        retry['relationtype'] = 'retry'
                        retry['jobid'] = job['pandaid']

    outlist = []
    added_keys = set()
    for row in newretries:
        if 'jobid' in row:
            lookup = row['jobid']
            if lookup not in added_keys:
                outlist.append(row)
                added_keys.add(lookup)
    return outlist
