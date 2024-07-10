"""

"""
import logging
import copy

from django.conf import settings
from django.db import connection, connections

from core.pandajob.models import PandaJob
from core.pandajob.utils import identify_jobtype
from core.schedresource.utils import get_panda_queues
from core.libs.exlib import getPilotCounts, get_resource_types
from core.libs.sqlsyntax import interval_to_sec

import core.constants as const

_logger = logging.getLogger('bigpandamon')


def prepare_job_summary_region(jsr_queues_dict, jsr_sites_dict, jsr_regions_dict, **kwargs):
    """
    Convert dict of region job summary to list
    :param jsr_queues_dict: dict of region job summary
    :param jsr_regions_dict: dict of queue job summary
    :return: list of region job summary, list of queue job summary
    """
    split_by = None
    if 'split_by' in kwargs and kwargs['split_by']:
        split_by = kwargs['split_by']
    jsr_queues_list = []
    jsr_sites_list = []
    jsr_regions_list = []
    for pq, params in jsr_queues_dict.items():
        for jt, resourcetypes in params['summary'].items():
            for rt, summary in resourcetypes.items():
                row = list()
                row.append(pq)
                row.append(params['pq_params']['pqtype'])
                row.append(params['pq_params']['region'])
                row.append(params['pq_params']['status'])
                row.append(jt)
                row.append(rt)
                row.append(params['pq_pilots']['count'])
                row.append(params['pq_pilots']['count_nojob'])
                row.append(summary['nwsubmitted'])
                row.append(summary['nwrunning'])
                row.append(summary['rcores'])
                row.append(sum([v for k, v in summary.items() if k in const.JOB_STATES]))
                if summary['failed'] + summary['finished'] > 0:
                    row.append(round(100.0*summary['failed']/(summary['failed'] + summary['finished']), 1))
                else:
                    row.append(0)
                for js in const.JOB_STATES:
                    row.append(summary[js])

                if split_by is None:
                    if jt == 'all' and rt == 'all':
                        jsr_queues_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' in split_by:
                    if jt != 'all' and rt != 'all':
                        jsr_queues_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                    if jt != 'all' and rt == 'all':
                        jsr_queues_list.append(row)
                elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                    if jt == 'all' and rt != 'all':
                        jsr_queues_list.append(row)

    for site, jobtypes in jsr_sites_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                row = list()
                row.append(site)
                row.append(jt)
                row.append(rt)
                row.append(summary['nwsubmitted'])
                row.append(summary['nwrunning'])
                row.append(summary['rcores'])
                row.append(sum([v for k, v in summary.items() if k in const.JOB_STATES]))
                if summary['failed'] + summary['finished'] > 0:
                    row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
                else:
                    row.append(0)
                for js in const.JOB_STATES:
                    row.append(summary[js])

                if split_by is None:
                    if jt == 'all' and rt == 'all':
                        jsr_sites_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' in split_by:
                    if jt != 'all' and rt != 'all':
                        jsr_sites_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                    if jt != 'all' and rt == 'all':
                        jsr_sites_list.append(row)
                elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                    if jt == 'all' and rt != 'all':
                        jsr_sites_list.append(row)

    for reg, jobtypes in jsr_regions_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                row = list()
                row.append(reg)
                row.append(jt)
                row.append(rt)
                row.append(summary['nwsubmitted'])
                row.append(summary['nwrunning'])
                row.append(summary['rcores'])
                row.append(sum([v for k, v in summary.items() if k in const.JOB_STATES]))
                if summary['failed'] + summary['finished'] > 0:
                    row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
                else:
                    row.append(0)
                for js in const.JOB_STATES:
                    row.append(summary[js])

                if split_by is None:
                    if jt == 'all' and rt == 'all':
                        jsr_regions_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' in split_by:
                    if jt != 'all' and rt != 'all':
                        jsr_regions_list.append(row)
                elif 'jobtype' in split_by and 'resourcetype' not in split_by:
                    if jt != 'all' and rt == 'all':
                        jsr_regions_list.append(row)
                elif 'jobtype' not in split_by and 'resourcetype' in split_by:
                    if jt == 'all' and rt != 'all':
                        jsr_regions_list.append(row)
    return jsr_queues_list, jsr_sites_list, jsr_regions_list


def get_job_summary_region(query, **kwargs):
    """
    :param query: dict of query params for jobs retrieving
    :return: dict of groupings
    """
    jsr_queues_dict = {}
    jsr_sites_dict = {}
    jsr_regions_dict = {}

    job_types = ['analy', 'prod']
    resource_types = get_resource_types()
    worker_metrics = ['nwrunning', 'nwsubmitted']
    extra_metrics = copy.deepcopy(worker_metrics)
    extra_metrics.append('rcores')

    if 'extra' in kwargs and len(kwargs['extra']) > 1:
        extra = kwargs['extra']
    else:
        extra = '(1=1)'
    if 'region' in kwargs and kwargs['region'] != 'all':
        region = kwargs['region']
        regions_list = [region]
    else:
        region = 'all'
    if 'jobtype' in kwargs and kwargs['jobtype'] != 'all':
        jobtype = kwargs['jobtype']
        job_types = [kwargs['jobtype']]
    else:
        jobtype = 'all'
    if 'resourcetype' in kwargs and kwargs['resourcetype'] != 'all':
        resourcetype = kwargs['resourcetype']
        resource_types = [kwargs['resourcetype']]
    else:
        resourcetype = 'all'
    if 'split_by' in kwargs and kwargs['split_by']:
        split_by = kwargs['split_by']
    else:
        split_by = None
    if 'pqs_dict' in kwargs and kwargs['pqs_dict']:
        pqs_dict = kwargs['pqs_dict']
    else:
        # get PQ info
        pqs_dict = get_panda_queues()

    # filter out queues that do not match to provided jobtype (analy/prod)
    if jobtype != 'all':
        pqs_dict = {pqn: params for pqn, params in pqs_dict.items() if is_jobtype_match_queue(jobtype, params)}

    # filter out queues if there is computingsite in the query for jobs
    if 'computingsite' in query:
        pqs_dict = {pqn: params for pqn, params in pqs_dict.items() if pqn == query['computingsite']}
    elif 'computingsite__in' in query:
        pqs_dict = {pqn: params for pqn, params in pqs_dict.items() if pqn in query['computingsite__in']}

    sites_list = list(set([params['atlas_site'] for pq, params in pqs_dict.items() if 'atlas_site' in params]))
    regions_list = list(set([params['cloud'] for pq, params in pqs_dict.items() if 'cloud' in params]))

    # get job summary
    jsq = get_job_summary_split(query, extra=extra)

    # check if there is more values of resourcetype than 4 default ones, if yes -> add them to the list
    if len(jsq) > 0:
        job_resource_types_extra = list(set([row['resourcetype'] for row in jsq]) - set(resource_types))
        if None in job_resource_types_extra or '' in job_resource_types_extra:
            # replace None with 'Not specified'
            resource_types.append('Not specified')
            for row in jsq:
                if row['resourcetype'] is None or row['resourcetype'] == '':
                    row['resourcetype'] = 'Not specified'

        # adding extra job resourcetype to the list
        resource_types.extend([rt for rt in job_resource_types_extra if rt and rt != ''])

    # get workers info
    wsq = []
    if 'core.harvester' in settings.INSTALLED_APPS:
        from core.harvester.utils import get_workers_summary_split
        if 'computingsite__in' not in query:
            # put full list of compitingsites to use index in workers table
            query['computingsite__in'] = list(set([row['computingsite'] for row in jsq]))
        wsq = get_workers_summary_split(query)

    # get PanDA getJob, updateJob request counts
    psq_dict = {}
    if split_by is None and jobtype == 'all' and resourcetype == 'all':
        psq_dict = getPilotCounts('all')

    # create template structure for grouping by queue
    for pqn, params in pqs_dict.items():
        jsr_queues_dict[pqn] = {'pq_params': {}, 'pq_pilots': {},  'summary': {'all': {'all': {}}}}
        jsr_queues_dict[pqn]['pq_params']['pqtype'] = params['type'] if 'type' in params else '-'
        jsr_queues_dict[pqn]['pq_params']['region'] = params['cloud'] if 'cloud' in params else '-'
        jsr_queues_dict[pqn]['pq_params']['atlas_site'] = params['atlas_site'] if 'atlas_site' in params else '-'
        jsr_queues_dict[pqn]['pq_params']['status'] = params['status'] if 'status' in params else '-'
        jsr_queues_dict[pqn]['pq_pilots']['count'] = psq_dict[pqn]['count_abs'] if pqn in psq_dict else -1
        jsr_queues_dict[pqn]['pq_pilots']['count_nojob'] = psq_dict[pqn]['count_nojobabs'] if pqn in psq_dict else -1
        for jt in job_types:
            if is_jobtype_match_queue(jt, params):
                jsr_queues_dict[pqn]['summary'][jt] = {}
                for rt in resource_types:
                    jsr_queues_dict[pqn]['summary'][jt][rt] = {}
                    jsr_queues_dict[pqn]['summary'][jt]['all'] = {}
                    jsr_queues_dict[pqn]['summary']['all'][rt] = {}
                    for js in const.JOB_STATES:
                        jsr_queues_dict[pqn]['summary'][jt][rt][js] = 0
                        jsr_queues_dict[pqn]['summary'][jt]['all'][js] = 0
                        jsr_queues_dict[pqn]['summary']['all'][rt][js] = 0
                        jsr_queues_dict[pqn]['summary']['all']['all'][js] = 0

                    for em in extra_metrics:
                        jsr_queues_dict[pqn]['summary'][jt][rt][em] = 0
                        jsr_queues_dict[pqn]['summary'][jt]['all'][em] = 0
                        jsr_queues_dict[pqn]['summary']['all'][rt][em] = 0
                        jsr_queues_dict[pqn]['summary']['all']['all'][em] = 0

    # create template structure for grouping by region
    for r in regions_list:
        jsr_regions_dict[r] = {}
        for jt in job_types:
            jsr_regions_dict[r][jt] = {}
            jsr_regions_dict[r]['all'] = {}
            for rt in resource_types:
                jsr_regions_dict[r][jt][rt] = {}
                jsr_regions_dict[r][jt]['all'] = {}
                jsr_regions_dict[r]['all'][rt] = {}
                jsr_regions_dict[r]['all']['all'] = {}
                for js in const.JOB_STATES:
                    jsr_regions_dict[r][jt][rt][js] = 0
                    jsr_regions_dict[r][jt]['all'][js] = 0
                    jsr_regions_dict[r]['all'][rt][js] = 0
                    jsr_regions_dict[r]['all']['all'][js] = 0

                for em in extra_metrics:
                    jsr_regions_dict[r][jt][rt][em] = 0
                    jsr_regions_dict[r][jt]['all'][em] = 0
                    jsr_regions_dict[r]['all'][rt][em] = 0
                    jsr_regions_dict[r]['all']['all'][em] = 0

    # create template structure for grouping by site
    for s in sites_list:
        jsr_sites_dict[s] = {}
        for jt in job_types:
            jsr_sites_dict[s][jt] = {}
            jsr_sites_dict[s]['all'] = {}
            for rt in resource_types:
                jsr_sites_dict[s][jt][rt] = {}
                jsr_sites_dict[s][jt]['all'] = {}
                jsr_sites_dict[s]['all'][rt] = {}
                jsr_sites_dict[s]['all']['all'] = {}
                for js in const.JOB_STATES:
                    jsr_sites_dict[s][jt][rt][js] = 0
                    jsr_sites_dict[s][jt]['all'][js] = 0
                    jsr_sites_dict[s]['all'][rt][js] = 0
                    jsr_sites_dict[s]['all']['all'][js] = 0

                for em in extra_metrics:
                    jsr_sites_dict[s][jt][rt][em] = 0
                    jsr_sites_dict[s][jt]['all'][em] = 0
                    jsr_sites_dict[s]['all'][rt][em] = 0
                    jsr_sites_dict[s]['all']['all'][em] = 0

    # fill template with real values of job states counts
    for row in jsq:
        if row['computingsite'] in jsr_queues_dict and row['jobtype'] in jsr_queues_dict[row['computingsite']]['summary'] and row['resourcetype'] in resource_types and row['jobstatus'] in const.JOB_STATES and 'count' in row:
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all']['all'][row['jobstatus']] += int(row['count'])

            jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all']['all'][row['jobstatus']] += int(row['count'])

            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all'][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][row['jobstatus']] += int(row['count'])

            # fill sum of running cores
            if row['jobstatus'] == 'running':
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all']['rcores'] += int(row['rcores'])
                jsr_queues_dict[row['computingsite']]['summary']['all']['all']['rcores'] += int(row['rcores'])

                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all'][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']]['all']['rcores'] += int(row['rcores'])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all']['all']['rcores'] += int(row['rcores'])

                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']]['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all']['rcores'] += int(row['rcores'])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all']['rcores'] += int(row['rcores'])

    # fill template with real values of n workers stats
    for row in wsq:
        if row['computingsite'] in jsr_queues_dict and row['jobtype'] in jsr_queues_dict[row['computingsite']]['summary'] and row['resourcetype'] in resource_types:
            for wm in worker_metrics:
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']][row['resourcetype']][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary'][row['jobtype']]['all'][wm] += int(row[wm])
                jsr_queues_dict[row['computingsite']]['summary']['all']['all'][wm] += int(row[wm])

                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']][row['resourcetype']][wm] += int(row[wm])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all'][row['resourcetype']][wm] += int(row[wm])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']][row['jobtype']]['all'][wm] += int(row[wm])
                jsr_sites_dict[jsr_queues_dict[row['computingsite']]['pq_params']['atlas_site']]['all']['all'][wm] += int(row[wm])

                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']][row['resourcetype']][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['jobtype']]['all'][wm] += int(row[wm])
                jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][wm] += int(row[wm])

    return jsr_queues_dict, jsr_sites_dict, jsr_regions_dict


def get_job_summary_split(query, extra):
    """ Query the jobs summary """
    db = connections['default'].vendor
    fields = {field.attname: field.db_column for field in PandaJob._meta.get_fields()}
    extra_str = extra
    for qn, qvs in query.items():
        if '__in' in qn and qn.replace('__in', '') in fields:
            extra_str += " and (" + fields[qn.replace('__in', '')] + " in ("
            if len(qvs) > 0:
                for qv in qvs:
                    extra_str += "'" + str(qv) + "',"
                extra_str = extra_str[:-1]
            else:
                extra_str += "''"
            extra_str += "))"
        elif '__icontains' in qn or '__contains' in qn and qn.replace('__icontains', '').replace('__contains', '') in fields:
            extra_str += " and (" + fields[qn.replace('__icontains', '').replace('__contains', '')] + " LIKE '%%" + qvs + "%%')"
        elif '__startswith' in qn and qn.replace('__startswith', '') in fields:
            extra_str += " and (" + fields[qn.replace('__startswith', '')] + " LIKE '" + qvs + "%%')"
        elif '__endswith' in qn and qn.replace('__endswith', '') in fields:
            extra_str += " and (" + fields[qn.replace('__endswith', '')] + " LIKE '%%" + qvs + "')"
        elif qn in fields:
            extra_str += " and (" + fields[qn] + "= '" + str(qvs) + "' )"

    # get jobs groupings, the jobsactive4 table can keep failed analysis jobs for up to 7 days, so splitting the query
    query_raw = """
        select j.computingsite, j.resource_type as resourcetype, j.prodsourcelabel, j.transform, j.jobstatus, 
            count(j.pandaid) as count, sum(j.rcores) as rcores, round(sum(j.walltime)) as walltime
        from  (
        select ja4.pandaid, ja4.resource_type, ja4.computingsite, ja4.prodsourcelabel, ja4.jobstatus, ja4.modificationtime, 0 as rcores,
            case when jobstatus in ('finished', 'failed') then {walltime_sec} else 0 end as walltime,
            case when transformation like 'http%' then 'run' when transformation like '%.py' then 'py' else 'unknown' end as transform
        from {db_scheme}.jobsarchived4 ja4  where modificationtime > TO_DATE('{date_from}', 'YYYY-MM-DD HH24:MI:SS') and {extra_str}
        union
        select jav4.pandaid, jav4.resource_type, jav4.computingsite, jav4.prodsourcelabel, jav4.jobstatus, jav4.modificationtime,
            case when jobstatus = 'running' then actualcorecount else 0 end as rcores, 0 as walltime,
            case when transformation like 'http%' then 'run' when transformation like '%.py' then 'py' else 'unknown' end as transform
        from {db_scheme}.jobsactive4 jav4 where modificationtime > TO_DATE('{date_from}', 'YYYY-MM-DD HH24:MI:SS') and 
            jobstatus in ('failed', 'finished', 'closed', 'cancelled') and {extra_str}
        union
        select jav4.pandaid, jav4.resource_type, jav4.computingsite, jav4.prodsourcelabel, jav4.jobstatus, jav4.modificationtime,
            case when jobstatus = 'running' then actualcorecount else 0 end as rcores, 0 as walltime,
            case when transformation like 'http%' then 'run' when transformation like '%.py' then 'py' else 'unknown' end as transform
        from {db_scheme}.jobsactive4 jav4 where jobstatus not in ('failed', 'finished', 'closed', 'cancelled') and {extra_str}
        union
        select jw4.pandaid, jw4.resource_type, jw4.computingsite, jw4.prodsourcelabel, jw4.jobstatus, jw4.modificationtime, 0 as rcores, 0 as walltime,
            case when transformation like 'http%' then 'run' when transformation like '%.py' then 'py' else 'unknown' end as transform
        from {db_scheme}.jobswaiting4 jw4 where {extra_str}
        union
        select jd4.pandaid, jd4.resource_type, jd4.computingsite, jd4.prodsourcelabel, jd4.jobstatus, jd4.modificationtime, 0 as rcores, 0 as walltime,
            case when transformation like 'http%' then 'run' when transformation like '%.py' then 'py' else 'unknown' end as transform
        from {db_scheme}.jobsdefined4 jd4  where {extra_str}
        ) j
        GROUP BY j.computingsite, j.prodsourcelabel, j.transform, j.resource_type, j.jobstatus
        order by j.computingsite, j.prodsourcelabel, j.transform, j.resource_type, j.jobstatus
    """.format(
        date_from=query['modificationtime__castdate__range'][0],
        extra_str=extra_str,
        db_scheme=settings.DB_SCHEMA_PANDA,
        walltime_sec=interval_to_sec('endtime-starttime', db=db)
    )

    cur = connection.cursor()
    cur.execute(query_raw)
    job_summary_tuple = cur.fetchall()
    job_summary_header = ['computingsite', 'resourcetype', 'prodsourcelabel', 'transform', 'jobstatus', 'count', 'rcores', 'walltime']
    summary = [dict(zip(job_summary_header, row)) for row in job_summary_tuple]

    # Translate prodsourcelabel values to descriptive analy|prod job types
    summary = identify_jobtype(summary)

    return summary


def prettify_json_output(jsr_queues_dict, jsr_sites_dict, jsr_regions_dict, **kwargs):
    """
    Remove queues|regions with 0 jobs, add links to jobs page if requested
    :param jsr_queues_dict:
    :param jsr_regions_dict:
    :return:
    """
    region_summary = {}
    site_summary = {}
    queue_summary = {}

    is_add_link = False
    if 'extra' in kwargs and 'links' in kwargs['extra'] and kwargs['extra']['links'] is True:
        is_add_link = True
    if 'hours' in kwargs:
        hours = kwargs['hours']
    else:
        hours = 12

    base_url = 'https://bigpanda.cern.ch'
    jobs_path = '/jobs/?hours=' + str(hours)

    for rn, rdict in jsr_regions_dict.items():
        for jt, jtdict in rdict.items():
            for rt, rtdict in jtdict.items():
                if sum([rtdict[js] for js in const.JOB_STATES if js in rtdict]) > 0:
                    if rn not in region_summary:
                        region_summary[rn] = {}
                    if jt not in region_summary[rn]:
                        region_summary[rn][jt] = {}
                    if rt not in region_summary[rn][jt]:
                        region_summary[rn][jt][rt] = []
                    for js in const.JOB_STATES:
                        temp_dict = {
                            'jobstatus': js,
                            'count': rtdict[js]
                        }
                        if is_add_link:
                            temp_dict['job_link'] = base_url + jobs_path + '&region=' + rn + '&jobstatus=' + js
                            if jt != 'all':
                                temp_dict['job_link'] += '&jobtype=' + jt
                            if rt != 'all':
                                temp_dict['job_link'] += '&resourcetype=' + rt
                        region_summary[rn][jt][rt].append(temp_dict)

    for rn, rdict in jsr_sites_dict.items():
        for jt, jtdict in rdict.items():
            for rt, rtdict in jtdict.items():
                if sum([rtdict[js] for js in const.JOB_STATES if js in rtdict]) > 0:
                    if rn not in site_summary:
                        site_summary[rn] = {}
                    if jt not in site_summary[rn]:
                        site_summary[rn][jt] = {}
                    if rt not in site_summary[rn][jt]:
                        site_summary[rn][jt][rt] = []
                    for js in const.JOB_STATES:
                        temp_dict = {
                            'jobstatus': js,
                            'count': rtdict[js]
                        }
                        if is_add_link:
                            temp_dict['job_link'] = base_url + jobs_path + '&region=' + rn + '&jobstatus=' + js
                            if jt != 'all':
                                temp_dict['job_link'] += '&jobtype=' + jt
                            if rt != 'all':
                                temp_dict['job_link'] += '&resourcetype=' + rt
                        site_summary[rn][jt][rt].append(temp_dict)

    for qn, qdict in jsr_queues_dict.items():
        for jt, jtdict in qdict['summary'].items():
            for rt, rtdict in jtdict.items():
                if sum([rtdict[js] for js in const.JOB_STATES if js in rtdict]) > 0:
                    if qn not in queue_summary:
                        queue_summary[qn] = qdict['pq_params']
                        if 'pq_pilots' in qdict and 'count' in qdict['pq_pilots'] and qdict['pq_pilots']['count'] and qdict['pq_pilots']['count'] > 0:
                            queue_summary[qn]['n_pilots_total'] = qdict['pq_pilots']['count']
                        else:
                            queue_summary[qn]['n_pilots_total'] = 0
                        if 'pq_pilots' in qdict and 'count_nojob' in qdict['pq_pilots'] and qdict['pq_pilots']['count_nojob'] and qdict['pq_pilots']['count_nojob'] > 0:
                            queue_summary[qn]['n_pilots_nojob'] = qdict['pq_pilots']['count_nojob']
                        else:
                            queue_summary[qn]['n_pilots_nojob'] = 0
                        queue_summary[qn]['n_running_cores'] = qdict['summary']['all']['all']['rcores']
                        if (qdict['summary']['all']['all']['failed'] + qdict['summary']['all']['all']['finished']) > 0:
                            queue_summary[qn]['failed_percentage'] = round(100. * qdict['summary']['all']['all']['failed'] / (
                                        qdict['summary']['all']['all']['failed'] + qdict['summary']['all']['all']['finished']), 1)
                        else:
                            queue_summary[qn]['failed_percentage'] = 0
                        queue_summary[qn]['n_jobs'] = sum([v for k, v in qdict['summary']['all']['all'].items() if k in const.JOB_STATES])
                        queue_summary[qn]['job_summary'] = {}
                    if jt not in queue_summary[qn]['job_summary']:
                        queue_summary[qn]['job_summary'][jt] = {}
                    if rt not in queue_summary[qn]['job_summary'][jt]:
                        queue_summary[qn]['job_summary'][jt][rt] = []
                    for js in const.JOB_STATES:
                        temp_dict = {
                            'jobstatus': js,
                            'count': rtdict[js]
                        }
                        if is_add_link:
                            temp_dict['job_link'] = base_url + jobs_path + '&computingsite=' + qn + '&jobstatus=' + js
                            if jt != 'all':
                                temp_dict['job_link'] += '&jobtype=' + jt
                            if rt != 'all':
                                temp_dict['job_link'] += '&resourcetype=' + rt
                        queue_summary[qn]['job_summary'][jt][rt].append(temp_dict)


    return queue_summary, site_summary, region_summary


def is_jobtype_match_queue(jobtype, pq_dict):
    """
    Check if analy/prod job can be run in a PanDA queue
    :param jobtype: str: analy or prod
    :param pq_dict: dict: PQ info
    :return: bool
    """
    pq_type = pq_dict['type'] if 'type' in pq_dict else 'unified'
    pq_jt_matrix = {
        'unified': ('prod', 'analy'),
        'production': ('prod',),
        'analysis': ('analy',),
        'special': ('prod',),
    }
    if pq_type in pq_jt_matrix and jobtype in pq_jt_matrix[pq_type]:
        return True

    return False
