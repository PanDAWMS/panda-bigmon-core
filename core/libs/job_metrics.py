from array import array
import statistics

from core.libs.exlib import convert_bytes
from core.libs.job import get_job_walltime, get_job_queuetime

# metric -> aggregation (order is the same as in the original metrics_dict)
JOB_METRICS_AGG = {
    'maxpss_per_actualcorecount': 'median',
    'diskio': 'median',
    'walltime': 'median',
    'queuetime': 'median',
    'failed': 'average',
    'efficiency': 'median',
    'attemptnr': 'average',
    'walltime_loss': 'sum',
    'cputime_loss': 'sum',
    'running_slots': 'sum',
    'gco2': 'sum',
    'gco2_loss': 'sum',
}
# metrics with integer values; streamed results are converted back to int when integral
JOB_METRICS_INT = {'failed', 'attemptnr', 'running_slots', 'diskio'}


def _add_job_metric_fields(job):
    """Per-job part of the metrics calculation. Modifies the job dict in place."""
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


def calc_jobs_metrics(jobs, group_by='jeditaskid'):
    """
    Calculate interesting metrics, e.g. avg maxpss/core, avg job walltime, avg job queuetime, failedpct
    :param jobs: list of dicts
    :param group_by:
    :return metrics_dict: dict
    """
    metrics_dict = {m: {'total': [], 'group_by': {}, 'agg': agg} for m, agg in JOB_METRICS_AGG.items()}

    # calc metrics
    for job in jobs:
        if group_by in job and job[group_by]:
            _add_job_metric_fields(job)
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


class JobsMetricsAccumulator:
    """
    Calculate job metrics streamed in batches to control memory usage.
    Call add() with batches of jobs, then result() returns the same structure of metrics dict.
    Keeps only one array of floats per metric per group (8 bytes per value).
    """
    def __init__(self, group_by='jeditaskid'):
        self.group_by = group_by
        self._values = {m: {} for m in JOB_METRICS_AGG}  # metric -> group value -> array('d')

    def add(self, jobs):
        for job in jobs:
            gb = job.get(self.group_by)
            if not gb:
                continue
            _add_job_metric_fields(job)
            for metric, groups in self._values.items():
                v = job.get(metric)
                if v is not None:
                    arr = groups.get(gb)
                    if arr is None:
                        arr = groups[gb] = array('d')
                    arr.append(v)

    def result(self):
        metrics_dict = {}
        for metric, agg in JOB_METRICS_AGG.items():
            groups = self._values[metric]
            total = array('d')
            for arr in groups.values():
                total.extend(arr)
            metrics_dict[metric] = {
                'total': self._aggregate(metric, agg, total) if len(total) > 0 else -1,
                'group_by': {gb: self._aggregate(metric, agg, arr) for gb, arr in groups.items()},
                'agg': agg,
            }
        return metrics_dict

    @staticmethod
    def _aggregate(metric, agg, values):
        if agg == 'median':
            r = statistics.median(values)
        elif agg == 'average':
            r = statistics.mean(values)
        else:
            r = sum(values)
        if metric in JOB_METRICS_INT and float(r).is_integer():
            r = int(r)
        return r if agg == 'sum' else round(r, 2)