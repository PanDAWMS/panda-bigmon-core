"""
Created by Tatiana Korchuganova on 05.03.2020
"""
import json

from django.db.models import Sum, Count

from core.pandajob.models import Jobsactive4
from core.common.models import Metrics


def get_running_jobs_stats(computingsites):
    """
    Getting summary of allocated slots, minramcount etc by running jobs by PQ
    :return: dict
    """
    rjs_dict = {}
    rquery = {'jobstatus': 'running'}
    if len(computingsites) == 0:
        return {}
    elif len(computingsites) == 1:
        rquery['computingsite'] = computingsites[0]
    else:
        rquery['computingsite__in'] = computingsites
    rvalues = ('computingsite',)

    rjs_list = Jobsactive4.objects.filter(**rquery).values(*rvalues).annotate(
        ncores=Sum('actualcorecount'),
        nminramcount=Sum('minramcount')*1.0/1000.0,
        nrjobs=Count('pandaid'),
    )

    # list -> dict
    for row in rjs_list:
        if 'computingsite' in row and row['computingsite'] not in rjs_dict:
            rjs_dict[row['computingsite']] = row

    return rjs_dict


def get_pq_metrics(pq='all'):
    """
    Get calculated metrics from METRICS table far PQs
    :return: pq_metrics_list
    """

    query = {}
    if pq == 'all' or pq == '':
        query['computingsite__isnull'] = False
    else:
        query['computingsite'] = pq

    pq_metrics_list = []
    values = ['computingsite', 'datajson']
    pq_metrics_list.extend(Metrics.objects.filter(**query).values(*values))

    # list -> dict
    pq_metrics = {}
    if len(pq_metrics_list) > 0:
        pq_metrics = {m['computingsite']: json.loads(m['datajson']) for m in pq_metrics_list}

    return pq_metrics


