""""""
import logging
from core.pandajob.models import Jobsarchived_y2014, Jobsarchived_y2015, Jobsarchived_y2016, Jobsarchived_y2017, \
    Jobsarchived_y2018, Jobsarchived_y2019, Jobsarchived_y2020, Jobsarchived_y2021,  Jobsarchived, Jobsarchived4
from core.libs.datetimestrings import parse_datetime
from core.libs.job import is_event_service
from core.libs.eventservice import get_event_status_summary
from core.libs.exlib import split_into_intervals

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
            2022: [Jobsarchived, ],
            2023: [Jobsarchived, ],
            2024: [Jobsarchived, Jobsarchived4],
        }
    else:
        pjm_year_dict = {
            2022: [Jobsarchived, ],
            2023: [Jobsarchived, ],
            2024: [Jobsarchived, Jobsarchived4],
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
        }

    pandajob_models = []

    if len(timewindow) == 2 and isinstance(timewindow[0], str):
        timewindow = [parse_datetime(t) for t in timewindow]

    for y in range(timewindow[0].year, timewindow[1].year + 1):
        if y in pjm_year_dict:
            pandajob_models.extend(pjm_year_dict[y])

    pandajob_models = list(set(pandajob_models))

    return pandajob_models


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


def job_summary_dict(request, jobs, fieldlist=None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    if fieldlist:
        flist = fieldlist
    else:
        flist = const.JOB_FIELDS_ATTR_SUMMARY

    numeric_fields = ('attemptnr', 'jeditaskid', 'taskid', 'noutputdatafiles', 'actualcorecount', 'corecount',
                      'reqid', 'jobsetid',)
    numeric_intervals = ('durationmin', 'nevents',)

    agg_fields = {
        'nevents': 'neventsrange'
    }

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
            if f == 'taskid' and int(job[f]) < 1000000 and 'produsername' not in request.session['requestParams']:
                continue
            elif f == 'specialhandling':
                if not 'specialhandling' in sumd:
                    sumd['specialhandling'] = {}
                shl = job['specialhandling'].split() if job['specialhandling'] is not None else []
                for v in shl:
                    if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
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
        for extra in ('jobmode', 'substate', 'outputfiletype', 'durationmin'):
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
        sumd['eventservicestatus'] = get_event_status_summary(esjobs, const.EVENT_STATES)

    sumd['processor_type'] = {
        'GPU': len(list(filter(lambda x: x.get('cmtconfig') and 'gpu' in x.get('cmtconfig'), jobs))),
        'CPU': len(list(filter(lambda x: x.get('cmtconfig') and not 'gpu' in x.get('cmtconfig'), jobs)))
    }

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
            if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
                iteml = sorted(iteml, key=lambda x: x['kvalue'], reverse=True)
            elif f not in ('priorityrange', 'jobsetrange', 'attemptnr', 'jeditaskid', 'taskid','noutputdatafiles','actualcorecount'):
                iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())

        itemd['list'] = iteml
        if f in ('actualcorecount', ):
            itemd['stats'] = {}
            itemd['stats']['sum'] = sum([x['kname'] * x['kvalue'] for x in iteml if isinstance(x['kname'], int)])
        suml.append(itemd)
        suml = sorted(suml, key=lambda x: x['field'])
    return suml, esjobdict
