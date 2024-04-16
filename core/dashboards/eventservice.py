"""
Collection of functions needed for EventService dashboard preparation
Created by Tatiana Korchuganova on 2020-07-04
"""

import copy
import logging

import core.constants as const

from django.db.models import Count

from core.libs.exlib import get_resource_types
from core.schedresource.utils import get_panda_queues, get_object_stores
from core.pandajob.models import CombinedWaitActDefArch4

_logger = logging.getLogger('bigpandamon')


def prepare_es_job_summary_region(jsr_queues_dict, jsr_regions_dict, **kwargs):
    """
    Convert dict of region job summary to list
    :param jsr_queues_dict:
    :param jsr_regions_dict:
    :return: jsr_queues_list, jsr_regions_list
    """
    jsr_queues_list = []
    jsr_regions_list = []

    split_by = None
    if 'split_by' in kwargs and kwargs['split_by']:
        split_by = kwargs['split_by']

    for pq, params in jsr_queues_dict.items():
        for jt, resourcetypes in params['summary'].items():
            for rt, summary in resourcetypes.items():
                if sum([v for k, v in summary.items() if k in const.JOB_STATES]) > 0 or 1 == 1:  # filter out rows with 0 jobs
                    row = list()
                    row.append(pq)
                    row.append(params['pq_params']['pqtype'])
                    row.append(params['pq_params']['region'])
                    row.append(params['pq_params']['status'])
                    row.append(jt)
                    row.append(rt)
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
                    elif 'eventservice' in split_by and 'resourcetype' in split_by:
                        if jt != 'all' and rt != 'all':
                            jsr_queues_list.append(row)
                    elif 'eventservice' in split_by and 'resourcetype' not in split_by:
                        if jt != 'all' and rt == 'all':
                            jsr_queues_list.append(row)
                    elif 'eventservice' not in split_by and 'resourcetype' in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_queues_list.append(row)

    for reg, jobtypes in jsr_regions_dict.items():
        for jt, resourcetypes in jobtypes.items():
            for rt, summary in resourcetypes.items():
                if sum([v for k, v in summary.items() if k in const.JOB_STATES]) > 0:  # filter out rows with 0 jobs
                    row = list()
                    row.append(reg)
                    row.append(jt)
                    row.append(rt)
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
                    elif 'eventservice' in split_by and 'resourcetype' in split_by:
                        if jt != 'all' and rt != 'all':
                            jsr_regions_list.append(row)
                    elif 'eventservice' in split_by and 'resourcetype' not in split_by:
                        if jt != 'all' and rt == 'all':
                            jsr_regions_list.append(row)
                    elif 'eventservice' not in split_by and 'resourcetype' in split_by:
                        if jt == 'all' and rt != 'all':
                            jsr_regions_list.append(row)

    return jsr_queues_list, jsr_regions_list


def get_es_job_summary_region(query, extra, **kwargs):
    """

    :param query: dict
    :param extra: str
    :return:
    """
    if 'pqquery' in kwargs and kwargs['pqquery']:
        pqquery = kwargs['pqquery']
    else:
        pqquery = dict()

    jsr_queues_dict = dict()
    jsr_regions_dict = dict()

    resource_types = get_resource_types()
    job_es_types = list(const.EVENT_SERVICE_JOB_TYPES.values())
    job_states_order = list(const.JOB_STATES)

    # get PQ info
    panda_queues_dict = get_panda_queues()
    regions_list = list(set([params['cloud'] for pq, params in panda_queues_dict.items()]))

    # get OS info
    object_stores = get_object_stores()

    # filter out queues by queue related selection params
    pq_to_remove = [pq for pq, params in panda_queues_dict.items() if not params['jobseed'].startswith('es')]
    if 'queuestatus' in pqquery:
        pq_to_remove.extend([pqn for pqn, params in panda_queues_dict.items() if params['status'] != pqquery['queuestatus']])
    if 'queuetype' in query:
        pq_to_remove.extend([pqn for pqn, params in panda_queues_dict.items() if params['type'] != pqquery['queuetype']])
    if len(pq_to_remove) > 0:
        for pqr in list(set(pq_to_remove)):
            del panda_queues_dict[pqr]

    # create template structure for grouping by queue
    for pqn, params in panda_queues_dict.items():
        jsr_queues_dict[pqn] = {'pq_params': {}, 'summary': {}}
        jsr_queues_dict[pqn]['pq_params']['pqtype'] = params['type']
        jsr_queues_dict[pqn]['pq_params']['region'] = params['cloud']
        jsr_queues_dict[pqn]['pq_params']['status'] = params['status']
        for jt in job_es_types:
            jsr_queues_dict[pqn]['summary'][jt] = {}
            jsr_queues_dict[pqn]['summary']['all'] = {}
            for rt in resource_types:
                jsr_queues_dict[pqn]['summary'][jt][rt] = {}
                jsr_queues_dict[pqn]['summary'][jt]['all'] = {}
                jsr_queues_dict[pqn]['summary']['all'][rt] = {}
                jsr_queues_dict[pqn]['summary']['all']['all'] = {}
                for js in job_states_order:
                    jsr_queues_dict[pqn]['summary'][jt][rt][js] = 0
                    jsr_queues_dict[pqn]['summary'][jt]['all'][js] = 0
                    jsr_queues_dict[pqn]['summary']['all'][rt][js] = 0
                    jsr_queues_dict[pqn]['summary']['all']['all'][js] = 0


    # create template structure for grouping by region
    for r in regions_list:
        jsr_regions_dict[r] = {}
        for jt in list(const.EVENT_SERVICE_JOB_TYPES.values()):
            jsr_regions_dict[r][jt] = {}
            jsr_regions_dict[r]['all'] = {}
            for rt in resource_types:
                jsr_regions_dict[r][jt][rt] = {}
                jsr_regions_dict[r][jt]['all'] = {}
                jsr_regions_dict[r]['all'][rt] = {}
                jsr_regions_dict[r]['all']['all'] = {}
                for js in list(const.JOB_STATES):
                    jsr_regions_dict[r][jt][rt][js] = 0
                    jsr_regions_dict[r][jt]['all'][js] = 0
                    jsr_regions_dict[r]['all'][rt][js] = 0
                    jsr_regions_dict[r]['all']['all'][js] = 0

    # get split data
    jsq = get_es_job_summary_region_split(query, extra)

    # fill template with real values of job states counts
    for row in jsq:
        if row['computingsite'] in jsr_queues_dict and row['eventservice'] in job_es_types and row['resourcetype'] in resource_types and row['jobstatus'] in job_states_order and 'count' in row:
            jsr_queues_dict[row['computingsite']]['summary'][row['eventservice']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary'][row['eventservice']]['all'][row['jobstatus']] += int(row['count'])
            jsr_queues_dict[row['computingsite']]['summary']['all']['all'][row['jobstatus']] += int(row['count'])

            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['eventservice']][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all'][row['resourcetype']][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']][row['eventservice']]['all'][row['jobstatus']] += int(row['count'])
            jsr_regions_dict[jsr_queues_dict[row['computingsite']]['pq_params']['region']]['all']['all'][row['jobstatus']] += int(row['count'])

    return jsr_queues_dict, jsr_regions_dict


def get_es_job_summary_region_split(query, extra):
    """
    Query DB
    :param query: dict
    :param extra: str
    :return:
    """

    query['es__gt'] = 0
    values = ('computingsite', 'es', 'resourcetype', 'jobstatus')

    # This is done for consistency with /jobs/ results
    query_archive = copy.deepcopy(query)
    query_archive['jobstatus__in'] = list(const.JOB_STATES_FINAL)

    query_active = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in query_active:
        del query_active['modificationtime__castdate__range']
    query_active['jobstatus__in'] = [s for s in const.JOB_STATES if s not in const.JOB_STATES_FINAL]

    job_summary = []
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_active).extra(where=[extra]).values(*values).annotate(
            count=Count('jobstatus')
        )
    )
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_archive).values(*values).extra(where=[extra]).annotate(
            count=Count('jobstatus')
        )
    )

    # translate numeric ES job types to descriptive str
    job_summary = eventservice_to_jobtype(job_summary)

    return job_summary


def eventservice_to_jobtype(list_of_dict, field_name='es'):
    """Translate eventservice values to descriptive ES job types"""

    trans_dict = const.EVENT_SERVICE_JOB_TYPES

    new_list_of_dict = []
    for row in list_of_dict:
        if field_name in row and row[field_name] in trans_dict:
            row['eventservice'] = trans_dict[row[field_name]]
            new_list_of_dict.append(row)

    return new_list_of_dict