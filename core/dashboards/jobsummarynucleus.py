"""

Created by Tatiana Korchuganova on 2020-07-10
"""
import copy

from django.db.models import Count, Sum

from core.pandajob.models import CombinedWaitActDefArch4


def prepare_job_summary_nucleus(jsn_nucleus_dict, jsn_satellite_dict, **kwargs):
    """
    Converting dict to list
    :param jsn_nucleus_dict:
    :param jsn_satellite_dict:
    :return: jsn_nucleus_list, jsn_satellite_list
    """
    if 'job_states_order' in kwargs:
        job_states_order = kwargs['job_states_order']
    else:
        job_states_order = [
            'pending', 'defined', 'waiting', 'assigned', 'throttled',
            'activated', 'sent', 'starting', 'running', 'holding',
            'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']

    jsn_nucleus_list = []
    jsn_satellite_list = []

    for nuc, sattelites in jsn_satellite_dict.items():
        for sat, summary in sattelites.items():
            row = [sat, nuc]
            row.append(sum([v for k, v in summary.items() if k in job_states_order]))
            if summary['failed'] + summary['finished'] > 0:
                row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
            else:
                row.append(0)
            for js in job_states_order:
                if js in summary:
                    row.append(summary[js])
                else:
                    row.append(0)
            jsn_satellite_list.append(row)

    for nuc, summary in jsn_nucleus_dict.items():
        row = [nuc]
        row.append(sum([v for k, v in summary.items() if k in job_states_order]))
        if summary['failed'] + summary['finished'] > 0:
            row.append(round(100.0 * summary['failed'] / (summary['failed'] + summary['finished']), 1))
        else:
            row.append(0)
        for js in job_states_order:
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

    if 'job_state_order' in kwargs:
        job_states_order = kwargs['job_states_order']
    else:
        job_states_order = [
            'pending', 'defined', 'waiting', 'assigned', 'throttled',
            'activated', 'sent', 'starting', 'running', 'holding',
            'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']
    if 'extra' in kwargs:
        extra = kwargs['extra']
    else:
        extra = '(1=1)'

    final_job_states = ['finished', 'failed', 'cancelled', 'closed', 'merging']
    final_es_job_states = ('finished', 'failed', 'merging')

    values = ['nucleus', 'computingsite', 'jobstatus']

    # This is done for consistency with /jobs/ results
    query_archive = copy.deepcopy(query)
    query_archive['jobstatus__in'] = final_job_states

    query_active = copy.deepcopy(query)
    if 'modificationtime__castdate__range' in query_active:
        del query_active['modificationtime__castdate__range']
    query_active['jobstatus__in'] = [s for s in job_states_order if s not in final_job_states]

    job_summary = []
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_active).extra(where=[extra]).values(*values).annotate(
            countjobsinstate=Count('jobstatus')
        )
    )
    job_summary.extend(
        CombinedWaitActDefArch4.objects.filter(**query_archive).values(*values).extra(where=[extra]).annotate(
            countjobsinstate=Count('jobstatus')
        ).annotate(
            counteventsinstate=Sum('nevents'))
    )

    jsn_satellite_dict = {}
    if len(job_summary) > 0:
        for row in job_summary:
            if row['nucleus'] is not None and row['nucleus'] != '':
                if row['nucleus'] not in jsn_satellite_dict:
                    jsn_satellite_dict[row['nucleus']] = {}
                if row['computingsite'] not in jsn_satellite_dict[row['nucleus']]:
                    jsn_satellite_dict[row['nucleus']][row['computingsite']] = {}
                    for js in job_states_order:
                        if js not in jsn_satellite_dict[row['nucleus']][row['computingsite']]:
                            jsn_satellite_dict[row['nucleus']][row['computingsite']][js] = 0
                        if js in final_es_job_states:
                            jsn_satellite_dict[row['nucleus']][row['computingsite']]['events'+js] = 0
                jsn_satellite_dict[row['nucleus']][row['computingsite']][row['jobstatus']] += row['countjobsinstate']
                if row['jobstatus'] in final_es_job_states:
                    jsn_satellite_dict[row['nucleus']][row['computingsite']]['events' + row['jobstatus']] += row['counteventsinstate']

    jsn_nucleus_dict = {}
    for nuc, satellites in jsn_satellite_dict.items():
        if nuc not in jsn_nucleus_dict:
            jsn_nucleus_dict[nuc] = {}
        for sat, summary in satellites.items():
            for js, count in summary.items():
                if js not in jsn_nucleus_dict[nuc]:
                    jsn_nucleus_dict[nuc][js] = 0
                jsn_nucleus_dict[nuc][js] += count

    return jsn_nucleus_dict, jsn_satellite_dict


