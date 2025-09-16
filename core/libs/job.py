"""
Set of functions related to jobs metadata

Created by Tatiana Korchuganova on 05.03.2020
"""
import copy

import math
import statistics
import logging
from django.db.models import Q

from core.libs.checks import is_positive_int_field
from core.libs.exlib import convert_bytes
from datetime import datetime
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobsdefined4, Jobsarchived4
from core.pandajob.utils import is_archived_jobs, get_job_error_descriptions
from core.common.models import JediJobRetryHistory, Filestable4, FilestableArch, JediDatasetContents, JediDatasets
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table, drop_duplicates, convert_sec
from core.libs.datetimestrings import parse_datetime
from core.libs.jobmetadata import addJobMetadata
from core.libs.error import add_error_info_to_job
from core.libs.eventservice import add_event_service_info_to_job, is_event_service
from core.schedresource.utils import get_pq_clouds, get_panda_queues, get_pq_atlas_sites

from django.conf import settings
import core.constants as const
from core.utils import is_json_request

_logger = logging.getLogger('bigpandamon')


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
        if 'jobstatus' in job and job['jobstatus'] not in (const.JOB_STATES_FINAL + ('merging',)):
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


def get_job_info(job):
    """
    Get job info string. If job is event service, add event service type. If job is not failed but has error diag, add it.
    :param job: dict
    :return: str
    """
    job_info = ''
    if is_event_service(job):
        es_job_types_desc = {
            'eventservice': 'Event service job. ',
            'esmerge': 'Event service merge job. ',
            'clone': 'Clone job. ',
            'jumbo': 'Jumbo job. ',
            'cojumbo': 'Cojumbo job. ',
            'finegrained': 'Fine grained processing job. ',
        }
        job_info += es_job_types_desc[job['eventservice']] if job['eventservice'] in es_job_types_desc else ''

    if is_debug_mode(job):
        job_info += 'Real-time logging is activated for this job.'

    # add diag with error code = 0 or any error code for not failed jobs
    diag_no_error_list = [
        (c['name'], job[c['error']], job[c['diag']])
        for c in const.JOB_ERROR_COMPONENTS
        if c['error'] in job and c['diag'] and c['diag'] in job and job[c['diag']] and len(job[c['diag']]) > 0
        and (
            job[c['error']] in (0, '0') or ('jobstatus' in job and job['jobstatus'] not in ('failed', 'holding'))
        )
    ]
    if len(diag_no_error_list) > 0:
        for d in diag_no_error_list:
            if d[1] == 0 or d[1] == '0':
                job_info += f'{d[2]}. '
            else:
                job_info += f'{d[0]}: {d[1]} {d[2]}. '

    return job_info


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


def add_error_info(jobs):
    """
    Transform error-related fields to errorinfo str
    :param jobs: list of dicts
    :return: jobs: list of dicts
    """

    error_desc = get_job_error_descriptions()
    jobs_new = []
    for job in jobs:
        jobs_new.append(add_error_info_to_job(job, mode='str', do_add_desc=True, error_desc=error_desc))

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
        if job['jobsubstatus'] == 'fg_partial':
            job['jobstatus'] = 'subfinished'
        elif job['jobsubstatus'] == 'fg_stumble':
            job['jobstatus'] = 'failed'
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
    """
    Get list of jobs
    :param query: Django ORM query object for jobs* models
    :keyword: values: list - list of extra fields to be queried
    :keyword: error_info: bool - if True, to add error-related fields
    :keyword: query_complex: expression with Q() objects
    :keyword: extra_str: str - custom where clause for query
    :return: jobs: list of dicts - jobs
    """
    jobs = []
    # minimum list of values needed for adding categories etc
    values = ['pandaid', 'jobstatus', 'jeditaskid', 'processingtype', 'transformation', 'eventservice', 'computingsite', 'produsername']
    if 'values' in kwargs:
        values.extend(kwargs['values'])
    else:
        # these values should be enough for clean job list etc
        values.extend([
            'actualcorecount', 'specialhandling', 'jobsubstatus', 'parentid', 'attemptnr', 'jobsetid', 'currentpriority',
            'creationtime', 'starttime', 'endtime', 'modificationtime', 'statechangetime',
            'jobmetrics', 'nevents', 'maxpss', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'cpuconsumptionunit', 'diskio', 'gco2_global',
        ])
    if 'error_info' in kwargs and kwargs['error_info']:
        for c in const.JOB_ERROR_COMPONENTS:
            values.append(c['error'])
            if c['diag'] is not None:
                values.append(c['diag'])
    values = set(values)

    extra_str = "(1=1)"
    if 'extra_str' in kwargs and kwargs['extra_str'] != '':
        extra_str = copy.deepcopy(kwargs['extra_str'])

    query_complex = None
    if 'query_complex' in kwargs:
        query_complex = kwargs['query_complex']

    id_in_params = []
    if 'pandaid__in' in query:
        id_in_params.append('pandaid')
    if 'jeditaskid__in' in query:
        id_in_params.append('jeditaskid')
    for idp in id_in_params:
        if len(query[idp + "__in"]) > settings.DB_N_MAX_IN_QUERY:
            # insert ids to temp DB table & add where join to query
            tmp_table_name = get_tmp_table_name()
            tks = insert_to_temp_table(query[idp + '__in'])
            extra_str += f" AND {idp} in (select ID from {tmp_table_name} where TRANSACTIONKEY={tks})"
            del query[idp + '__in']

    # in case there is a part of query with OR relation
    if query_complex is not None:
        query_general = Q(**query) & query_complex
    else:
        query_general = Q(**query)
    for job_table in (Jobsdefined4, Jobsactive4, Jobsarchived4):
        jobs.extend(job_table.objects.filter(query_general).extra(where=[extra_str]).values(*values))

    if 'jeditaskid' in query or 'jeditaskid__in' in query or 'jeditaskid' in extra_str or (
            'pandaid' in query or 'pandaid__in' in query or 'pandaid' in extra_str) or  (
                'modificationtime__castdate__range' in query and is_archived_jobs(query['modificationtime__castdate__range'])) or (
                    len(jobs) == 0 and 'pandaid' in query):
        if 'modificationtime__castdate__range' in query:
            # jobsarchived table has index by statechangetime, use it instead of modificationtime
            query['statechangetime__castdate__range'] = query['modificationtime__castdate__range']
            del query['modificationtime__castdate__range']
            # do query_general again, after changes to query
            if query_complex is not None:
                query_general = Q(**query) & query_complex
            else:
                query_general = Q(**query)
        jobs.extend(Jobsarchived.objects.filter(query_general).extra(where=[extra_str]).values(*values))

    # drop duplicate jobs
    jobs = drop_duplicates(jobs, id='pandaid')

    # add job category
    jobs = add_job_category(jobs)

    # add error info
    if 'error_info' in kwargs and kwargs['error_info']:
        jobs = add_error_info(jobs)

    return jobs


def parse_jobmetrics(jobs):
    """
    Parse and add the metrics to job dicts
    :param jobs: list
    :return: jobs
    """
    for job in jobs:
        if 'jobmetrics' in job and job['jobmetrics'] and len(job['jobmetrics']) > 0:
            jobmetrics = {str(jm.split('=')[0]).lower(): jm.split('=')[1] for jm in job['jobmetrics'].split(' ') if '=' in jm}
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

    if do_add_errorinfo:
        error_descriptions = get_job_error_descriptions()

    # drop duplicate jobs
    jobs = drop_duplicates(jobl, id='pandaid')
    _logger.debug('{} jobs last after duplicates dropping'.format(len(jobs)))

    if do_add_metadata:
        jobs = addJobMetadata(jobl)
        _logger.debug('Added metadata')

    for job in jobs:
        # find max and min values of priority and modificationtime for current selection of jobs
        if 'modificationtime' in job and job['modificationtime'] > request.session['TLAST']:
            request.session['TLAST'] = job['modificationtime']
        if 'modificationtime' in job and job['modificationtime'] < request.session['TFIRST']:
            request.session['TFIRST'] = job['modificationtime']
        if 'currentpriority' in job and job['currentpriority'] > request.session['PHIGH']:
            request.session['PHIGH'] = job['currentpriority']
        if 'currentpriority' in job and job['currentpriority'] < request.session['PLOW']:
            request.session['PLOW'] = job['currentpriority']

        if is_event_service(job):
            job = add_event_service_info_to_job(job)
        else:
            job['eventservice'] = 'ordinary'

        # add error info only for failed and holding jobs
        if do_add_errorinfo and 'jobstatus' in job and job['jobstatus'] in ('failed', 'holding'):
            job = add_error_info_to_job(
                job,
                mode='html' if not is_json_request(request) else 'str',
                do_add_desc=True if not is_json_request(request) else False,
                error_desc=error_descriptions
            )

        job['jobinfo'] = get_job_info(job)

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
        if 'jobmetrics' in job and job['jobmetrics'] and 'nGPU' in job['jobmetrics']:
            job['processor_type'] = 'GPU'
        else:
            job['processor_type'] = 'CPU'
        if 'modificationhost' in job and job['modificationhost']:
            job['wn'] = job['modificationhost'].split('@')[1] if '@' in job['modificationhost'] else job['modificationhost']
        else:
            job['wn'] = 'Unknown'

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
                jsquery = {'jeditaskid': jeditaskid, 'pandaid': retry['oldpandaid']}
                values = ['pandaid', 'jobstatus', 'jeditaskid']
                jsjobs = []
                for jt in (Jobsdefined4, Jobsactive4, Jobsarchived4, Jobsarchived):
                    jsjobs.extend(jt.objects.filter(**jsquery).values(*values))
                for job in jsjobs:
                    for retry in newretries:
                        if retry['oldpandaid'] == job['pandaid']:
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
        'type__in': ['input', 'log', 'output'],
    }
    fvalues = ('pandaid', 'fileid', 'datasetid', 'lfn', 'type', 'scope', 'dataset')
    files_list.extend(Filestable4.objects.filter(**fquery).values(*fvalues))
    if len(set([f['pandaid'] for f in files_list])) < len(jobs):
        files_list.extend(FilestableArch.objects.filter(**fquery).values(*fvalues))
    files_list = drop_duplicates(files_list, id='fileid')

    files_per_job = {}
    for f in files_list:
        if f['pandaid'] not in files_per_job:
            files_per_job[f['pandaid']] = {
                'lfn_list': [],
                'did_log_list': [],
                'did_input': {},
                'did_output': {},
            }
        if f['type'] == 'input' and f['lfn'] not in files_per_job[f['pandaid']]['lfn_list']:
            files_per_job[f['pandaid']]['lfn_list'].append(f['lfn'])
            if '.lib.' not in f['dataset'] and f['datasetid'] not in files_per_job[f['pandaid']]['did_input']:
                files_per_job[f['pandaid']]['did_input'][f['datasetid']] = f['dataset']
        elif f['type'] == 'log':
            files_per_job[f['pandaid']]['did_log_list'].append({'scope': f['scope'], 'name': f['lfn']})
        elif f['type'] == 'output' and f['datasetid'] not in files_per_job[f['pandaid']]['did_output']:
            files_per_job[f['pandaid']]['did_output'][f['datasetid']] = f['dataset']

    # getting info from dataset contents, where only last attempt is kept
    dataset_contents_list = []
    dcquery = {
        'datasetid__in': list(set([f['datasetid'] for f in files_list if f['type'] == 'input'])),
    }
    # only primary input
    extra_str = "datasetid in (select datasetid from {}.jedi_datasets where masterid is null)".format(
        settings.DB_SCHEMA_PANDA
    )
    dvalues = ('pandaid', 'lfn', 'type', 'startevent', 'endevent', 'nevents', 'datasetid')
    dataset_contents_list.extend(JediDatasetContents.objects.filter(**dcquery).extra(where=[extra_str]).values(*dvalues))

    dc_per_job = {}
    for jds in dataset_contents_list:
        if jds['pandaid'] not in dc_per_job:
            dc_per_job[jds['pandaid']] = {
                'nevents': 0,
                'lfn_list': [],
            }
        if jds['endevent'] is not None and jds['startevent'] is not None:
            dc_per_job[jds['pandaid']]['nevents'] += int(jds['endevent']) + 1 - int(jds['startevent'])
        else:
            dc_per_job[jds['pandaid']]['nevents'] += int(jds['nevents']) if jds['nevents'] is not None else 0
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

        if j['pandaid'] in files_per_job:
            if len(files_per_job[j['pandaid']]['did_log_list']) > 0:
                j['log_did'] = files_per_job[j['pandaid']]['did_log_list'][0]
            if len(files_per_job[j['pandaid']]['did_input']) > 0:
                j['did_input'] = [{'id': did, 'name': name} for did, name in files_per_job[j['pandaid']]['did_input'].items()]
            else:
                j['did_input'] = []
            if len(files_per_job[j['pandaid']]['did_output']) > 0:
                j['did_output'] = [{'id': did, 'name': name} for did, name in files_per_job[j['pandaid']]['did_output'].items()]
            else:
                j['did_output'] = []


    return jobs


def get_files_for_job(pandaid):
    """
    Get files for a job. There are 2 sources for it, filestable and JEDI dataset content, we query both and merge it.
    :param pandaid: int
    :return files: list[dict]
    :return file_stats: dict
    """
    files = []
    file_stats = {ftype: {'n': 0, 'size_mb': 0, 'nevents': 0} for ftype in ('input', 'output', 'pseudo_input', 'log')}

    # get job files from filestable
    files.extend(Filestable4.objects.filter(pandaid=pandaid).order_by('type').values())
    if len(files) == 0:
        files.extend(FilestableArch.objects.filter(pandaid=pandaid).order_by('type').values())

    if len(files) > 0:
        # get datasets
        dquery = {'datasetid__in': [f['datasetid'] for f in files]}
        dsets = JediDatasets.objects.filter(**dquery).values('datasetid', 'datasetname')
        datasets_dict = {ds['datasetid']: ds['datasetname'] for ds in dsets}

        # get dataset contents
        dcquery = copy.deepcopy(dquery)
        dcfiles = JediDatasetContents.objects.filter(**dcquery).values()
        dcfiles_dict = {}
        if len(dcfiles) > 0:
            for dcf in dcfiles:
                dcfiles_dict[dcf['fileid']] = dcf
    else:
        datasets_dict = {}
        dcfiles_dict = {}

    # prepare files data for the template
    for f in files:
        f['fsizemb'] = round(convert_bytes(f['fsize'], output_unit='MB'), 2)
        f['fsize'] = int(f['fsize'])
        if 'creationdate' not in f:
            f['creationdate'] = f['modificationtime']
        if 'fileid' not in f:
            f['fileid'] = f['row_id']
        if 'datasetname' not in f:
            if f['scope'] and f['scope'] + ":" in f['dataset']:
                f['datasetname'] = f['dataset']
                f['ruciodatasetname'] = f['dataset'].split(":")[1]
            else:
                f['datasetname'] = f['dataset']
                f['ruciodatasetname'] = f['dataset']
        if 'modificationtime' in f:
            f['oldfiletable'] = 1
        if 'destinationdblock' in f and f['destinationdblock'] is not None:
            f['destinationdblock_vis'] = f['destinationdblock'].split('_')[-1]
        # add info from datasets
        if f['datasetid'] in datasets_dict:
            f['datasetname'] = datasets_dict[f['datasetid']]
            if f['scope'] and f['scope'] + ":" in f['datasetname']:
                f['ruciodatasetname'] = f['datasetname'].split(":")[1]
            else:
                f['ruciodatasetname'] = f['datasetname']
        if f['destinationdblocktoken'] and 'dst' in f['destinationdblocktoken']:
            parced = f['destinationdblocktoken'].split("_")
            f['ddmsite'] = parced[0][4:] if len(parced) > 0 and len(parced[0]) > 4 else ''
            f['dsttoken'] = 'ATLAS' + parced[1] if len(parced) > 1 else ''
        # add info from dataset contents
        if f['type'] == 'input':
            f['attemptnr'] = dcfiles_dict[f['fileid']]['attemptnr'] if f['fileid'] in dcfiles_dict else f['attemptnr']
            f['maxattempt'] = dcfiles_dict[f['fileid']]['maxattempt'] if f['fileid'] in dcfiles_dict else None

        # collect stats
        if f['type'] in file_stats:
            file_stats[f['type']]['n'] += 1
            file_stats[f['type']]['size_mb'] += f['fsizemb']
            file_stats[f['type']]['nevents'] += dcfiles_dict[f['fileid']]['nevents'] if (
                    f['fileid'] in dcfiles_dict and is_positive_int_field(dcfiles_dict[f['fileid']], 'nevents')) else 0
        # save log file info separately
        if f['type'] == 'log':
            file_stats['log']['details'] = {
                'lfn': f['lfn'],
                'guid': f['guid'],
                'scope': f['scope'],
                'fileid': f['fileid'],
                'site': f['destinationse'] if 'destinationse' in f else ''
            }

    files = sorted(files, key=lambda x: x['type'])
    return files, file_stats