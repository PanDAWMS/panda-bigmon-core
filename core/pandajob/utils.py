""""""
import datetime
import logging
import collections, functools, operator
from django.core.cache import cache
from core.pandajob.models import Jobsarchived_y2014, Jobsarchived_y2015, Jobsarchived_y2016, Jobsarchived_y2017, \
    Jobsarchived_y2018, Jobsarchived_y2019, Jobsarchived_y2020, Jobsarchived_y2021, Jobsarchived_y2022, Jobsarchived, Jobsarchived4, \
    ErrorDescription
from core.libs.datetimestrings import parse_datetime
from core.libs.eventservice import is_event_service, get_event_status_summary
from core.libs.exlib import split_into_intervals, get_maxrampercore_dict
from core.libs.checks import is_positive_int_field

from django.conf import settings
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def get_pandajob_models_by_year(timewindow):
    """
    List of PanDA job models
    :return:

    """
    if settings.DEPLOYMENT == "ORACLE_ATLAS":
        pjm_year_dict = {
            2014: [Jobsarchived_y2014, ],
            2015: [Jobsarchived_y2015, ],
            2016: [Jobsarchived_y2016, ],
            2017: [Jobsarchived_y2017, ],
            2018: [Jobsarchived_y2018, ],
            2019: [Jobsarchived_y2019, ],
            2020: [Jobsarchived_y2020, ],
            2021: [Jobsarchived_y2021, ],
            2022: [Jobsarchived_y2022, ],
            2023: [Jobsarchived, ],
            2024: [Jobsarchived, ],
            2025: [Jobsarchived, Jobsarchived4],
        }
    else:
        pjm_year_dict = {
            2022: [Jobsarchived, ],
            2023: [Jobsarchived, ],
            2024: [Jobsarchived, ],
            2025: [Jobsarchived, Jobsarchived4],
        }
    pandajob_models = []

    if len(timewindow) == 2 and isinstance(timewindow[0], str):
        timewindow = [parse_datetime(t) for t in timewindow]

    for y in range(timewindow[0].year, timewindow[1].year + 1):
        if y in pjm_year_dict:
            pandajob_models.extend(pjm_year_dict[y])

    pandajob_models = list(set(pandajob_models))

    return pandajob_models


def get_pandajob_arch_models_by_year(timewindow):
    """
    List of PanDA job models
    :return:

    """
    pjm_year_dict = {}
    if settings.DEPLOYMENT == "ORACLE_ATLAS":
        # need to update at time of archival procedure made by DBA experts
        pjm_year_dict = {
            2014: [Jobsarchived_y2014, ],
            2015: [Jobsarchived_y2015, ],
            2016: [Jobsarchived_y2016, ],
            2017: [Jobsarchived_y2017, ],
            2018: [Jobsarchived_y2018, ],
            2019: [Jobsarchived_y2019, ],
            2020: [Jobsarchived_y2020, ],
            2021: [Jobsarchived_y2021, ],
            2022: [Jobsarchived_y2022, ],
        }

    pandajob_models = []

    if len(timewindow) == 2 and isinstance(timewindow[0], str):
        timewindow = [parse_datetime(t) for t in timewindow]

    for y in range(timewindow[0].year, timewindow[1].year + 1):
        if y in pjm_year_dict:
            pandajob_models.extend(pjm_year_dict[y])

    pandajob_models = list(set(pandajob_models))

    return pandajob_models


def is_archived_jobs(timerange):
    """
    Check if archived PanDA jobs are needed for the given time range as jobs go there after ~2-4 days
    :param timerange: list of datetime strings
    :return: bool
    """
    if len(timerange) == 2 and isinstance(timerange[0], str):
        timerange = [parse_datetime(t) for t in timerange]

    # if start or end is older than 4 days - need to check archived
    if (datetime.datetime.now() - timerange[0]).days > 2 or (datetime.datetime.now() - timerange[1]).days > 2:
        return True

    return False


def is_archived_only_jobs(timerange):
    """
    Check ifonly  archived PanDA jobs are needed for the given time range as jobs go there after 4 days
    :param timerange: list of datetime strings
    :return: bool
    """
    if len(timerange) == 2 and isinstance(timerange[0], str):
        timerange = [parse_datetime(t) for t in timerange]

    # if start or end is older than 4 days - need to check archived
    if (datetime.datetime.now() - timerange[0]).days > 4 and (datetime.datetime.now() - timerange[1]).days > 4:
        return True

    return False


def identify_jobtype(list_of_dict, field_name='prodsourcelabel'):
    """
    Translate prodsourcelabel values to descriptive analy|prod job types
    The base param is prodsourcelabel, but to identify which HC test template a job belong to we need transformation.
    If transformation ends with '.py' - prod, if it is PanDA server URL - analy.
    Using this as complementary info to make a decision.
    """

    psl_to_jt = {
        'panda': 'analy',
        'user': 'analy',
        'managed': 'prod',
        'prod_test': 'prod',
        'ptest': 'prod',
        'rc_alrb': 'analy',
        'rc_test2': 'analy',
    }

    if 'ATLAS' not in settings.DEPLOYMENT:
        psl_to_jt.update({
            'test': 'prod',
            'ANY': 'prod',
        })

    trsfrm_to_jt = {
        'run': 'analy',
        'py': 'prod',
    }

    new_list_of_dict = []
    for row in list_of_dict:
        if field_name in row and row[field_name] in psl_to_jt.keys():
            row['jobtype'] = psl_to_jt[row[field_name]]
            if 'transform' in row and row['transform'] in trsfrm_to_jt and row[field_name] in ('rc_alrb', 'rc_test2'):
                row['jobtype'] = trsfrm_to_jt[row['transform']]
        else:
            row['jobtype'] = 'prod'
        new_list_of_dict.append(row)

    return new_list_of_dict


def job_summary_dict(jobs, fieldlist=None, produsername=None, sortby='alpha'):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    if fieldlist:
        flist = fieldlist
    else:
        flist = const.JOB_FIELDS_ATTR_SUMMARY

    numeric_fields = (
        'attemptnr', 'jeditaskid', 'noutputdatafiles', 'actualcorecount', 'corecount', 'reqid', 'jobsetid',
    )
    numeric_intervals = ('durationmin', 'nevents',)

    agg_fields = {
        'nevents': 'neventsrange'
    }

    maxrampercore_dict = get_maxrampercore_dict()

    for job in jobs:
        for f in flist:
            if f == 'pilotversion':
                if 'pilotid' in job and job['pilotid'] and '|' in job['pilotid']:
                    job[f] = job['pilotid'].split('|')[-1]
                else:
                    job[f] = 'Not specified'
            if f == 'schedulerid':
                if 'schedulerid' in job and job[f] is not None:
                    if 'harvester' in job[f]:
                        job[f] = job[f].replace('harvester-', '')
                    else:
                        job[f] = 'Not specified'
            if f == 'taskid' and int(job[f]) < 1000000 and produsername is None:
                continue
            elif f == 'specialhandling':
                if not 'specialhandling' in sumd:
                    sumd['specialhandling'] = {}
                shl = job['specialhandling'].split(',') if job['specialhandling'] is not None else []
                for v in shl:
                    # ignore tq = taskQueuedTime timestamp
                    if v.startswith('tq'):
                        continue
                    if not v in sumd['specialhandling']:
                        sumd['specialhandling'][v] = 0
                    sumd['specialhandling'][v] += 1
            else:
                if f not in sumd:
                    sumd[f] = {}
                if f in job and (job[f] is None or job[f] == ''):
                    kval = -1 if f in numeric_fields else 'Not specified'
                elif f in job:
                    kval = job[f]
                else:
                    kval = 'Not specified'
                if kval not in sumd[f]:
                    sumd[f][kval] = 0
                sumd[f][kval] += 1
        for extra in ('jobmode', 'substate', 'durationmin'):
            if extra in job:
                if extra not in sumd:
                    sumd[extra] = {}
                if job[extra] not in sumd[extra]:
                    sumd[extra][job[extra]] = 0
                sumd[extra][job[extra]] += 1
    if 'schedulerid' in sumd:
        sumd['harvesterinstance'] = sumd['schedulerid']
        del sumd['schedulerid']

    # event service
    esjobdict = {}
    esjobs = []
    for job in jobs:
        if is_event_service(job):
            esjobs.append(job['pandaid'])
    if len(esjobs) > 0:
        sumd['eventservicestatus'] = get_event_status_summary(esjobs)

    # convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        if f in agg_fields:
            itemd['field'] = agg_fields[f]
        else:
            itemd['field'] = f
        iteml = []
        kys = list(sumd[f].keys())
        if f == 'minramcount':
            newvalues = {}
            for ky in kys:
                roundedval = int(ky / 1000)
                if roundedval in newvalues:
                    newvalues[roundedval] += sumd[f][ky]
                else:
                    newvalues[roundedval] = sumd[f][ky]
            for ky in newvalues:
                iteml.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
            iteml = sorted(iteml, key=lambda x: int(x['kname'].split("-")[0]))
        elif f in numeric_intervals:
            if len(kys) == 1 and kys[0] == 0:
                iteml.append({'kname': '0-0', 'kvalue': sumd[f][0]})
            else:
                if f == 'nevents':
                    minstep = 1000
                elif f == 'durationmin':
                    minstep = 10
                else:
                    minstep = 1
                iteml.extend(split_into_intervals([job[f] for job in jobs if f in job], minstep=minstep))
        else:
            if f in ('priorityrange', 'jobsetrange'):
                skys = []
                for k in kys:
                    if k != 'Not specified':
                        skys.append({'key': k, 'val': int(k[:k.index(':')])})
                    else:
                        skys.append({'key': 'Not specified', 'val': -1})
                skys = sorted(skys, key=lambda x: x['val'])
                kys = []
                for sk in skys:
                    kys.append(sk['key'])
            elif f in numeric_fields:
                kys = sorted(kys, key=lambda x: int(x))
            else:
                try:
                    kys = sorted(kys)
                except:
                    _logger.exception('Failed to sort list of values for {} attribute \n {}'.format(str(f), str(kys)))
            for ky in kys:
                if ky == -1:
                    iteml.append({'kname': 'Not specified', 'kvalue': sumd[f][ky]})
                else:
                    iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
            if sortby == 'count':
                iteml = sorted(iteml, key=lambda x: x['kvalue'], reverse=True)
            elif f not in ('priorityrange', 'jobsetrange', 'attemptnr', 'jeditaskid', 'taskid','noutputdatafiles','actualcorecount'):
                iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())

        itemd['list'] = iteml
        if f == 'actualcorecount':
            itemd['stats'] = {}
            itemd['stats']['sum'] = sum([x['kname'] * x['kvalue'] for x in iteml if isinstance(x['kname'], int)])
        if f == 'minramcount':
            itemd['stats'] = {}
            # calculate sum(minramcount)/sum(cores) and convert MB->GB
            try:
                result = dict(functools.reduce(operator.add, map(collections.Counter, [
                    {'corecount': j['corecount'], 'minramcount': j['minramcount']} for j in jobs if isinstance(j[f], int) and isinstance(j['corecount'], int) and j['corecount'] > 0
                ])))
                itemd['stats']['sum'] = round(1.0/1000*result['minramcount']/result['corecount'], 2)
            except Exception as ex:
                _logger.warning(f"Can not calculate {f}/core with {ex}")
            # calculate requested ramcount estimate resource_type.maxrampercore*corecount*njobs
            try:
                result = dict(functools.reduce(operator.add, map(collections.Counter, [
                    {'corecount': j['corecount'], 'maxram': 1.0*j['corecount']*maxrampercore_dict[j['resourcetype']][j['computingsite']]} for j in jobs if (
                        'resourcetype' in j and j['resourcetype'] in maxrampercore_dict and 'computingsite' in j and j['computingsite'] in maxrampercore_dict[j['resourcetype']] and isinstance(maxrampercore_dict[j['resourcetype']][j['computingsite']], int) and isinstance(j['corecount'], int) and j['corecount'] > 0)
                ])))
                itemd['stats']['sum_allocated'] = round(1.0/1000*result['maxram']/result['corecount'], 2)
            except Exception as ex:
                _logger.warning(f"Can not calculate allocated ram/core with {ex}")
        suml.append(itemd)
        suml = sorted(suml, key=lambda x: x['field'])
    return suml, esjobdict


def get_job_error_descriptions():
    """
    Retrieves error descriptions from the cache or database. Firstly it attempts to fetch error descriptions from the cache.
    If the cache is unavailable or empty, it loads the error descriptions directly from the database and stores them in the cache
    for future use.

    :return: dict - A dictionary containing error descriptions, where the keys are formatted as
             "component:code" and the values are the corresponding error description objects.
    """
    error_descriptions = {}
    try:
        error_descriptions = cache.get('error_descriptions', None)
    except Exception as e:
        _logger.debug('Can not get error codes from cache: \n{} \nLoading directly from DB instead...'.format(e))
        error_descriptions = None
    if not error_descriptions:
        error_descriptions_list = list(ErrorDescription.objects.values())
        error_descriptions = {f"{d['component']}:{d['code']}": d for d in error_descriptions_list}
        cache.set('error_descriptions', error_descriptions, 60*60*24)
    return error_descriptions


def error_summary_for_job(job):
    """
    Prepare error summary for a job
    :param job: dict, job data
    :return: list of dicts with error summary
    """
    error_summary = []
    for comp in const.JOB_ERROR_COMPONENTS:
        if is_positive_int_field(job, comp['error']) and job[comp['error']] > 0:
            error_summary.append({
                'component': comp['title'],
                'code': job[comp['error']],
                'diagnostics': job['transformerrordiag'] if comp['name'] == 'transform' and 'transformerrordiag' in job else job[
                    comp['diag']],
                'description': job[f"{comp['name']}_error_desc"] if f"{comp['name']}_error_desc" in job else '',
            })
    if 'harvesterInfo' in job and is_positive_int_field(job['harvesterInfo'], 'errorcode') and job['harvesterInfo']['errorcode'] > 0:
        error_summary.append({
            'component': 'Harvester worker',
            'code': job['harvesterInfo']['errorcode'],
            'diagnostics': job['harvesterInfo']['diagmessage'] if 'diagmessage' in job['harvesterInfo'] else '',
            'description': '-',
        })
    return error_summary
