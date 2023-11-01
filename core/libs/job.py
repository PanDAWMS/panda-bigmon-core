"""
Set of functions related to jobs metadata

Created by Tatiana Korchuganova on 05.03.2020
"""
import math
import statistics
import logging
import re
from core.libs.exlib import convert_bytes
from datetime import datetime, timedelta
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4
from core.common.models import JediJobRetryHistory, Filestable4, FilestableArch, JediDatasetContents
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table, drop_duplicates, convert_sec
from core.libs.datetimestrings import parse_datetime
from core.libs.jobmetadata import addJobMetadata
from core.libs.error import errorInfo, get_job_error_desc
from core.schedresource.utils import get_pq_clouds

from django.conf import settings
import core.constants as const

_logger = logging.getLogger('bigpandamon')


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
    is_debug = False

    to_enable_debug_mode_sh = ['debug','dm']

    if ('specialhandling' in job and job['specialhandling'] is not None):
        specialhandling_list = str(job['specialhandling']).split(',')
        is_debug = any(item in specialhandling_list for item in to_enable_debug_mode_sh)

    if  'commandtopilot' in job and job['commandtopilot'] is not None \
            and len(job['commandtopilot']) > 0 \
            and job['commandtopilot'] != 'tobekilled':
        is_debug = True

    return is_debug


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

    if 'creationtime' in job and job['creationtime'] is not None:
        creationtime = parse_datetime(job['creationtime']) if not isinstance(job['creationtime'], datetime) else job['creationtime']
    else:
        creationtime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']
    else:
        # count time until now if a job is not started yet
        starttime = datetime.now()

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
    starttime = None
    endtime = None

    if 'starttime' in job and job['starttime'] is not None:
        starttime = parse_datetime(job['starttime']) if not isinstance(job['starttime'], datetime) else job['starttime']

    if 'endtime' in job and job['endtime'] is not None:
        endtime = parse_datetime(job['endtime']) if not isinstance(job['endtime'], datetime) else job['endtime']

    if endtime is None:
        if 'jobstatus' in job and job['jobstatus'] not in const.JOB_STATES_FINAL:
            # for active job take time up to now
            endtime = datetime.now()
        else:
            # for ended job try to take another available timestamps
            if 'statechangetime' in job and job['statechangetime'] is not None:
                endtime = parse_datetime(job['statechangetime']) if not isinstance(job['statechangetime'], datetime) else job['statechangetime']
            elif 'modificationtime' in job and job['modificationtime'] is not None:
                endtime = parse_datetime(job['modificationtime']) if not isinstance(job['modificationtime'], datetime) else job['modificationtime']

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


def parse_job_pilottiming(pilottiming_str):
    """
    Parsing pilot timing str into dict
    :param pilottiming_str: dict
    :return: dict of separate pilot timings
    """
    pilot_timings_names = ['timegetjob', 'timestagein', 'timepayload', 'timestageout', 'timetotal_setup']

    try:
        pilot_timings = [int(pti) for pti in pilottiming_str.split('|')]
    except:
        pilot_timings = [None] * 5

    pilot_timings_dict = dict(zip(pilot_timings_names, pilot_timings))

    return pilot_timings_dict


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


def job_state_count(jobs):
    statecount = {}
    for state in const.JOB_STATES:
        statecount[state] = 0
    for job in jobs:
        statecount[job['jobstatus']] += 1
    return statecount


def get_job_list(query, **kwargs):

    MAX_ENTRIES__IN = 100

    jobs = []
    values = [
        'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid',
        'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite',
        'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid', 'attemptnr',
        'processingtype', 'transformation', 'creationtime', 'diskio', 'gco2_global'
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

    for job_table in (Jobsdefined4, Jobswaiting4, Jobsactive4, Jobsarchived4):
        jobs.extend(job_table.objects.filter(**query).extra(where=[extra_str]).values(*values))

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
        'running_slots': {'total': [], 'group_by': {}, 'agg': 'sum'},
        'gco2': {'total': [], 'group_by': {}, 'agg': 'sum'},
        'gco2_loss': {'total': [], 'group_by': {}, 'agg': 'sum'},
    }

    # calc metrics
    for job in jobs:
        if group_by in job and job[group_by]:

            job['failed'] = 100 if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0
            job['attemptnr'] = job['attemptnr'] + 1
            # protection if cpuconsumptiontime is decimal in non Oracle DBs
            if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None:
                job['cpuconsumptiontime'] = float(job['cpuconsumptiontime'])
            if 'gco2_global' in job:
                job['gco2'] = float(job['gco2_global']) if job['gco2_global'] is not None else 0
                job['gco2_loss'] = float(job['gco2']) if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0

            if job['category'] == 'run':
                if 'maxpss' in job and job['maxpss'] and isinstance(job['maxpss'], int) and (
                        'actualcorecount' in job and isinstance(job['actualcorecount'], int) and job['actualcorecount'] > 0):
                    job['maxpss_per_actualcorecount'] = convert_bytes(1.0*job['maxpss']/job['actualcorecount'], output_unit='MB')

                job['diskio'] = job['diskio'] if 'diskio' in job and job['diskio'] is not None else 0

                job['walltime'] = get_job_walltime(job)
                job['queuetime'] = get_job_queuetime(job)

                if job['walltime'] and 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None and (
                        'actualcorecount' in job and isinstance(job['actualcorecount'], int) and job['actualcorecount'] > 0):
                    job['efficiency'] = round(1.0*job['cpuconsumptiontime']/job['walltime']/job['actualcorecount'], 2)

                job['cputime'] = round(job['cpuconsumptiontime'] / 60. / 60., 2) if 'cpuconsumptiontime' in job else 0

                job['walltime'] = round(job['walltime'] / 60. / 60., 2) if job['walltime'] is not None else 0
                job['queuetime'] = round(job['queuetime'] / 60. / 60., 2) if job['queuetime'] is not None else 0

                job['walltime_loss'] = job['walltime'] if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0
                job['cputime_loss'] = job['cputime'] if 'jobstatus' in job and job['jobstatus'] == 'failed' else 0

                job['running_slots'] = job['actualcorecount'] if 'jobstatus' in job and job['jobstatus'] == 'running' else 0

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
            elif metrics_dict[metric]['agg'] == 'sum':
                metrics_dict[metric]['total'] = sum(metrics_dict[metric]['total'])
        else:
            metrics_dict[metric]['total'] = -1
        for gbp in metrics_dict[metric]['group_by']:
            if len(metrics_dict[metric]['group_by'][gbp]) > 0:
                if metrics_dict[metric]['agg'] == 'median':
                    metrics_dict[metric]['group_by'][gbp] = round(statistics.median(metrics_dict[metric]['group_by'][gbp]), 2)
                elif metrics_dict[metric]['agg'] == 'average':
                    metrics_dict[metric]['group_by'][gbp] = round(statistics.mean(metrics_dict[metric]['group_by'][gbp]), 2)
                elif metrics_dict[metric]['agg'] == 'sum':
                    metrics_dict[metric]['group_by'][gbp] = sum(metrics_dict[metric]['group_by'][gbp])
            else:
                metrics_dict[metric]['group_by'][gbp] = -1

    return metrics_dict


def clean_job_list(request, jobl, do_add_metadata=False, do_add_errorinfo=False):
    """
    Cleaning the list of jobs including:
     removing duplicates, adding metadata if needed, calculate metrics, humanize parameters' values
    :param request:
    :param jobl: list of jobs
    :param do_add_metadata: bool: True or False (load metadata from special DB table)
    :param do_add_errorinfo: bool: True or False (summarize code + diag fields into error info str)
    :return: jobs: list of jobs
    """
    _logger.debug('Got {} jobs to clean'.format(len(jobl)))
    if 'fields' in request.session['requestParams']:
        fieldsStr = request.session['requestParams']['fields']
        fields = fieldsStr.split("|")
        if 'metastruct' in fields:
            do_add_metadata = True

    pq_clouds = get_pq_clouds()

    errorCodes = {}
    if do_add_errorinfo:
        errorCodes = get_job_error_desc()

    # drop duplicate jobs
    jobs = drop_duplicates(jobl, id='pandaid')
    _logger.debug('{} jobs last after duplicates dropping'.format(len(jobs)))

    if do_add_metadata:
        jobs = addJobMetadata(jobl)
        _logger.debug('Added metadata')

    for job in jobs:
        # find max and min values of priority and modificationtime for current selection of jobs
        if job['modificationtime'] > request.session['TLAST']:
            request.session['TLAST'] = job['modificationtime']
        if job['modificationtime'] < request.session['TFIRST']:
            request.session['TFIRST'] = job['modificationtime']
        if job['currentpriority'] > request.session['PHIGH']:
            request.session['PHIGH'] = job['currentpriority']
        if job['currentpriority'] < request.session['PLOW']:
            request.session['PLOW'] = job['currentpriority']

        if do_add_errorinfo:
            job['errorinfo'] = errorInfo(job, errorCodes=errorCodes)

        job['jobinfo'] = ''
        if is_event_service(job):
            if job['eventservice'] == 1:
                job['eventservice'] = 'eventservice'
                job['jobinfo'] = 'Event service job. '
            elif job['eventservice'] == 2:
                job['eventservice'] = 'esmerge'
                job['jobinfo'] = 'Event service merge job. '
            elif job['eventservice'] == 3:
                job['eventservice'] = 'clone'
            elif job['eventservice'] == 4:
                job['eventservice'] = 'jumbo'
                job['jobinfo'] = 'Jumbo job. '
            elif job['eventservice'] == 5:
                job['eventservice'] = 'cojumbo'
                job['jobinfo'] = 'Cojumbo job. '
            elif job['eventservice'] == 6:
                job['eventservice'] = 'finegrained'
                job['jobinfo'] = 'Fine grained processing job. '

            if 'taskbuffererrordiag' in job and job['taskbuffererrordiag'] is None:
                job['taskbuffererrordiag'] = ''
            if 'taskbuffererrordiag' in job and len(job['taskbuffererrordiag']) > 0:
                job['jobinfo'] += job['taskbuffererrordiag']

            # extract job substatus
            if 'jobmetrics' in job:
                pat = re.compile('.*mode\=([^\s]+).*HPCStatus\=([A-Za-z0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['jobmode'] = mat.group(1)
                    job['substate'] = mat.group(2)
                pat = re.compile('.*coreCount\=([0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['corecount'] = mat.group(1)
            if 'jobsubstatus' in job and job['jobstatus'] == 'closed' and job['jobsubstatus'] == 'toreassign':
                job['jobstatus'] += ':' + job['jobsubstatus']
        else:
            job['eventservice'] = 'ordinary'

        if is_debug_mode(job):
            job['jobinfo'] += 'Real-time logging is activated for this job.'

        if 'destinationdblock' in job and job['destinationdblock']:
            ddbfields = job['destinationdblock'].split('.')
            if len(ddbfields) == 6 and ddbfields[0] != 'hc_test':
                job['outputfiletype'] = ddbfields[4]
            elif len(ddbfields) >= 7:
                job['outputfiletype'] = ddbfields[6]
            # else:
            #     job['outputfiletype'] = None
            #     print job['destinationdblock'], job['outputfiletype'], job['pandaid']

        try:
            job['homecloud'] = pq_clouds[job['computingsite']]
        except:
            job['homecloud'] = None

        if 'produsername' in job and not job['produsername']:
            if ('produserid' in job) and job['produserid']:
                job['produsername'] = job['produserid']
            else:
                job['produsername'] = 'Unknown'
        if job['transformation']:
            job['transformation'] = job['transformation'].split('/')[-1]

        job['durationsec'] = get_job_walltime(job)
        job['durationsec'] = job['durationsec'] if job['durationsec'] is not None else 0
        job['durationmin'] = math.floor(job['durationsec'] / 60.0)
        job['duration'] = convert_sec(job['durationsec'], out_unit='str')

        job['waittimesec'] = get_job_queuetime(job)
        job['waittimesec'] = job['waittimesec'] if job['waittimesec'] is not None else 0
        job['waittime'] = convert_sec(job['waittimesec'], out_unit='str')

        if 'currentpriority' in job:
            plo = int(job['currentpriority']) - int(job['currentpriority']) % 100
            phi = plo + 99
            job['priorityrange'] = "%d:%d" % (plo, phi)
        if 'jobsetid' in job and job['jobsetid']:
            plo = int(job['jobsetid']) - int(job['jobsetid']) % 100
            phi = plo + 99
            job['jobsetrange'] = "%d:%d" % (plo, phi)
        if 'corecount' in job and job['corecount'] is None:
            job['corecount'] = 1
        if 'maxpss' in job and isinstance(job['maxpss'], int) and (
                'actualcorecount' in job and isinstance(job['actualcorecount'], int) and job['actualcorecount'] > 0):
            job['maxpssgbpercore'] = round(job['maxpss']/1024./1024./job['actualcorecount'], 2)

        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None:
            job['cpuconsumptiontime'] = float(job['cpuconsumptiontime'])
        if ('cpuconsumptiontime' in job and job['cpuconsumptiontime'] and job['cpuconsumptiontime'] > 0) and (
                'actualcorecount' in job and job['actualcorecount'] is not None and job['actualcorecount'] > 0) and (
                    'durationsec' in job and job['durationsec'] is not None and job['durationsec'] > 0):
            job['cpuefficiency'] = round(100.0 * job['cpuconsumptiontime'] / job['durationsec'] / job['actualcorecount'], 2)

    _logger.debug('Job list cleaned')
    return jobs


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


def add_files_info_to_jobs(jobs):
    """
    Get files info for list of jobs, i.e. ninputfiles, nevents, name of log file
    :param jobs: list of dicts
    :return: jobs: list of dicts with extra key-value pairs added
    """
    # collect info from filestable where files for each attempt of a job is stored
    files_list = []
    fquery = {
        'pandaid__in': [j['pandaid'] for j in jobs],
        'type__in': ['input', 'log']
    }
    fvalues = ('pandaid', 'fileid', 'datasetid', 'lfn', 'type', 'scope')
    files_list.extend(Filestable4.objects.filter(**fquery).values(*fvalues))
    if len(set([f['pandaid'] for f in files_list])) < len(jobs):
        files_list.extend(FilestableArch.objects.filter(**fquery).values(*fvalues))
    files_list = drop_duplicates(files_list, id='fileid')

    files_per_job = {}
    for f in files_list:
        if f['pandaid'] not in files_per_job:
            files_per_job[f['pandaid']] = {
                'lfn_list': [],
                'log_dids': []
            }
        if f['type'] == 'input' and f['lfn'] not in files_per_job[f['pandaid']]['lfn_list']:
            files_per_job[f['pandaid']]['lfn_list'].append(f['lfn'])
        elif f['type'] == 'log':
            files_per_job[f['pandaid']]['log_dids'].append({'scope': f['scope'], 'name': f['lfn']})

    # getting info from dataset contents, where only last attempt is kept
    dataset_contents_list = []
    dcquery = {
        'datasetid__in': list(set([f['datasetid'] for f in files_list if f['type'] == 'input'])),
    }
    # only primary input
    extra_str = "datasetid in (select datasetid from {}.jedi_datasets where masterid is null)".format(
        settings.DB_SCHEMA_PANDA
    )
    dataset_contents_list.extend(JediDatasetContents.objects.filter(**dcquery).extra(where=[extra_str]).values())

    dc_per_job = {}
    for jds in dataset_contents_list:
        if jds['pandaid'] not in dc_per_job:
            dc_per_job[jds['pandaid']] = {
                'nevents': 0,
                'lfn_list': [],
            }
        if jds['endevent'] is not None and jds['startevent'] is not None:
            dc_per_job[jds['pandaid']]['nevents'] = int(jds['endevent']) + 1 - int(jds['startevent'])
        else:
            dc_per_job[jds['pandaid']]['nevents'] = int(jds['nevents']) if jds['nevents'] is not None else 0
        if jds['type'] == 'input':
            dc_per_job[jds['pandaid']]['lfn_list'].append(jds['lfn'])

    # adding collected info to jobs
    for j in jobs:
        if j['pandaid'] in  dc_per_job:
            j['ninputs'] = len(dc_per_job[j['pandaid']]['lfn_list'])
            j['nevents'] = dc_per_job[j['pandaid']]['nevents']
        elif j['pandaid'] in files_per_job:
            j['ninputs'] = len(files_per_job[j['pandaid']]['lfn_list'])
        else:
            j['ninputs'] = 0

        if j['pandaid'] in files_per_job and len(files_per_job[j['pandaid']]['log_dids']) > 0:
            j['log_did'] = files_per_job[j['pandaid']]['log_dids'][0]

    return jobs