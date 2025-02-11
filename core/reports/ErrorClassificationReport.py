import copy

from core.libs.job import get_job_list
from core.dashboards.jobsummaryregion import get_job_summary_region
import core.constants as const
import logging
_logger = logging.getLogger('bigpandamon')

class ErrorClassificationReport:
    def __init__(self, query=None):
        if query is None:
            raise ValueError('Query is not provided')

        self.query_jobs = copy.deepcopy(query)
        self.extra_query = "(" + " or ".join([f"({k['error']} is not null and {k['error']} > 0)" for k in list(const.JOB_ERROR_CATEGORIES)]) + ")"
        self.jobs = []
        _logger.debug(f"{self.query_jobs}, {self.extra_query}")


    def get_jobs(self):
        self.jobs = get_job_list(copy.deepcopy(self.query_jobs), extra_str=self.extra_query, error_info=True)
        _logger.info(f'Found jobs: {len(self.jobs)}')


    def get_job_counts_per_pq(self):
        job_counts = {}
        job_summary_queues, _, _ = get_job_summary_region(copy.deepcopy(self.query_jobs))
        for pq, counts in job_summary_queues.items():
            job_counts[pq] = counts['summary']['all']['all']
        return job_counts


    def prepare_report(self):
        """
        Prepare error statistics report for jobs per PQ
        :return:
        """
        data = {}
        self.get_jobs()

        error_per_site = {}
        error_code_dist = {}
        for j in self.jobs:
            for k in list(const.JOB_ERROR_CATEGORIES):
                if k['error'] in j and j[k['error']] is not None and j[k['error']] != '' and int(j[k['error']]) > 0:
                    error_category = f"{k['name']}:{j[k['error']]}"
                    if error_category not in error_code_dist:
                        error_code_dist[error_category] = 0
                    error_code_dist[error_category] += 1
                    if j['computingsite'] not in error_per_site:
                        error_per_site[j['computingsite']] = {'job_count_per_status': {}, 'error_code_counts': {}}
                    if error_category not in error_per_site[j['computingsite']]['error_code_counts']:
                        error_per_site[j['computingsite']]['error_code_counts'][error_category] = 0
                    error_per_site[j['computingsite']]['error_code_counts'][error_category] += 1

        job_state_counts = self.get_job_counts_per_pq()
        for pq, counts in job_state_counts.items():
            if pq not in error_per_site:
                error_per_site[pq] = {'job_count_per_status': {}, 'error_code_counts': {}}
            error_per_site[pq]['job_count_per_status'] = counts

        data['error_code_total'] = error_code_dist
        data['error_code_per_site'] =  error_per_site
        return data

