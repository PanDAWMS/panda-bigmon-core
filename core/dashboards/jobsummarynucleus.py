"""

Created by Tatiana Korchuganova on 2020-07-10
"""
import copy
import logging

from django.db import connection
from django.db.models import Count
from django.db.utils import DatabaseError

from core.pandajob.models import CombinedWaitActDefArch4
import core.constants as const 

_logger = logging.getLogger('bigpandamon')


def prepare_job_summary_nucleus(jsn_nucleus_dict, jsn_satellite_dict, **kwargs):
    """
    Converting dict to list
    :param jsn_nucleus_dict:
    :param jsn_satellite_dict:
    :return: jsn_nucleus_list, jsn_satellite_list
    """

    jsn_nucleus_list = []
    jsn_satellite_list = []

    for nuc, sattelites in jsn_satellite_dict.items():
        for sat, summary in sattelites.items():
            row = [sat, nuc]
            row.append(round(summary['hs06s_used']/1000) if 'hs06s_used' in summary else 0)
            row.append(round(summary['hs06s_failed']/1000) if 'hs06s_failed' in summary else 0)
            row.append(sum([v for k, v in summary.items() if k in const.JOB_STATES]))
            if summary['failed'] + summary['finished'] > 0:
                row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
            else:
                row.append(0)
            for js in const.JOB_STATES:
                if js in summary:
                    row.append(summary[js])
                else:
                    row.append(0)
            jsn_satellite_list.append(row)

    for nuc, summary in jsn_nucleus_dict.items():
        row = [nuc]
        row.append(round(summary['hs06s_used']/1000) if 'hs06s_used' in summary else 0)
        row.append(round(summary['hs06s_failed']/1000) if 'hs06s_failed' in summary else 0)
        row.append(sum([v for k, v in summary.items() if k in const.JOB_STATES]))
        if summary['failed'] + summary['finished'] > 0:
            row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
        else:
            row.append(0)
        for js in const.JOB_STATES:
            if js in summary:
                row.append(summary[js])
            else:
                row.append(0)
        jsn_nucleus_list.append(row)

    return jsn_nucleus_list, jsn_satellite_list


def get_job_summary_nucleus(query, **kwargs):
    """

    :param query: dict
    :return: jsn_nucleus_dict, jsn_satellite_dict
    """

    if 'extra' in kwargs:
        extra = kwargs['extra']
    else:
        extra = '(1=1)'
    is_add_hs06s = False
    if 'hs06s' in kwargs and kwargs['hs06s']:
        is_add_hs06s = True

    values = ['nucleus', 'computingsite', 'jobstatus']

    # This is done for consistency with /jobs/ results
    query_archive = copy.deepcopy(query)
    query_archive['jobstatus__in'] = const.JOB_STATES_FINAL

    query_active = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in query_active:
        del query_active['modificationtime__castdate__range']
    query_active['jobstatus__in'] = [s for s in const.JOB_STATES if s not in const.JOB_STATES_FINAL]

    job_summary = []
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_active).extra(where=[extra]).values(*values).annotate(
            countjobsinstate=Count('jobstatus')
        )
    )
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_archive).values(*values).extra(where=[extra]).annotate(
            countjobsinstate=Count('jobstatus')
        )
    )

    jsn_satellite_dict = {}
    if len(job_summary) > 0:
        for row in job_summary:
            if row['nucleus'] is not None and row['nucleus'] != '':
                if row['nucleus'] not in jsn_satellite_dict:
                    jsn_satellite_dict[row['nucleus']] = {}
                if row['computingsite'] not in jsn_satellite_dict[row['nucleus']]:
                    jsn_satellite_dict[row['nucleus']][row['computingsite']] = {}
                    for js in const.JOB_STATES:
                        if js not in jsn_satellite_dict[row['nucleus']][row['computingsite']]:
                            jsn_satellite_dict[row['nucleus']][row['computingsite']][js] = 0
                jsn_satellite_dict[row['nucleus']][row['computingsite']][row['jobstatus']] += row['countjobsinstate']

    jsn_nucleus_dict = {}
    for nuc, satellites in jsn_satellite_dict.items():
        if nuc not in jsn_nucleus_dict:
            jsn_nucleus_dict[nuc] = {}
        for sat, summary in satellites.items():
            for js, count in summary.items():
                if js not in jsn_nucleus_dict[nuc]:
                    jsn_nucleus_dict[nuc][js] = 0
                jsn_nucleus_dict[nuc][js] += count

    if is_add_hs06s:
        hs06s_nucleus_dict, hs06s_satellite_dict = get_world_hs06_summary(query, extra=extra)

        for nuc, satellites in hs06s_satellite_dict.items():
            for sat, summary in satellites.items():
                if nuc in jsn_satellite_dict and sat in jsn_satellite_dict[nuc]:
                    jsn_satellite_dict[nuc][sat]['hs06s_used'] = summary['hs06s_used']
                    jsn_satellite_dict[nuc][sat]['hs06s_failed'] = summary['hs06s_failed']

        for nuc, summary in hs06s_nucleus_dict.items():
            if nuc in jsn_nucleus_dict:
                jsn_nucleus_dict[nuc]['hs06s_used'] = summary['hs06s_used']
                jsn_nucleus_dict[nuc]['hs06s_failed'] = summary['hs06s_failed']

    return jsn_nucleus_dict, jsn_satellite_dict


def get_world_hs06_summary(query, **kwargs):

    if 'extra' in kwargs:
        extra = kwargs['extra']
        extra = extra.replace("'", "\'\'")
    else:
        extra = '(1=1)'

    extra += """ 
        AND modificationtime > TO_DATE(\'\'{0}\'\',\'\'{2}\'\') 
        AND modificationtime < TO_DATE(\'\'{1}\'\',\'\'{2}\'\')
        """.format(
            query['modificationtime__castdate__range'][0],
            query['modificationtime__castdate__range'][1],
            'YYYY-MM-DD HH24:MI:SS'
    )

    try:
        cur = connection.cursor()
        cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.GETHS06SSUMMARY('{}'))".format(extra))
        hspersite = cur.fetchall()
        cur.close()
    except DatabaseError:
        _logger.exception('Internal Server Error to get HS06s summary from GETHS06SSUMMARY SQL function')
        raise

    keys = ['nucleus', 'computingsite', 'hs06s_used', 'hs06s_failed']
    world_HS06s_summary = [dict(zip(keys, row)) for row in hspersite]

    sum_params = ['hs06s_used', 'hs06s_failed']
    hs06s_satellite_dict = {}
    if len(world_HS06s_summary) > 0:
        for row in world_HS06s_summary:
            if row['nucleus'] is not None and row['nucleus'] != '':
                if row['nucleus'] not in hs06s_satellite_dict:
                    hs06s_satellite_dict[row['nucleus']] = {}
                if row['computingsite'] not in hs06s_satellite_dict[row['nucleus']]:
                    hs06s_satellite_dict[row['nucleus']][row['computingsite']] = {}
                    for sp in sum_params:
                        if sp not in hs06s_satellite_dict[row['nucleus']][row['computingsite']]:
                            hs06s_satellite_dict[row['nucleus']][row['computingsite']][sp] = 0
                        if row[sp] is not None:
                            hs06s_satellite_dict[row['nucleus']][row['computingsite']][sp] += row[sp]

    hs06s_nucleus_dict = {}
    for nuc, satellites in hs06s_satellite_dict.items():
        if nuc not in hs06s_nucleus_dict:
            hs06s_nucleus_dict[nuc] = {}
        for sat, summary in satellites.items():
            for sp, value in summary.items():
                if sp not in hs06s_nucleus_dict[nuc]:
                    hs06s_nucleus_dict[nuc][sp] = 0
                hs06s_nucleus_dict[nuc][sp] += value

    return hs06s_nucleus_dict, hs06s_satellite_dict
