"""
API for ATLAS reconstruction large-scale tests report (ATLASPANDA-665)
"""
import copy
import logging
import json
from dateutil.parser import parse
from django.db.models import Count, Avg, F

from core.common.models import JediTasks, JediTaskparams, JediDatasetContents
from core.libs.dropalgorithm import drop_job_retries
from core.libs.job import get_job_list, job_state_count
from core.libs.jobconsumption import job_consumption_plots
from core.libs.elasticsearch import create_os_connection, upload_data
from core.libs.exlib import count_occurrences
from core.libs.task import get_datasets_for_tasklist, calculate_dataset_stats


_logger = logging.getLogger('bigpandamon')


class LargeScaleAthenaTestsReport:
    def __init__(self, jeditaskid_list):
        self.jeditaskid_list = jeditaskid_list
        self.data = {}
        self.data_template = {
            'jeditaskid': 0,
            'creationdate': None,
            'configuration__release_project': '',
            'configuration__release_branch': '',
            'configuration__release_nightly_tag': '',
            'configuration__release_nightly_date': None,
            'configuration__release_platform': '',
            'configuration__input_scope': '',
            'configuration__input_datasetname': '',
            'configuration__input_nfiles': 0,
            'configuration__input_nevents': 0,
            'configuration__output_formats': [],
            'configuration__conditions_tag': '',
            'configuration__options': '',
            'configuration__submit_command': '',
            'jobs__attempts': {},
            'jobs__computingsites': {},
            'jobs__nfilesperjob_average': 0,
            'jobs__status_all': 0,
            'jobs__status_retries_excl': 0,
            'errors__athena': {},
            'jobsconsumption__maxpsspercore_avg': 0,
            'jobsconsumption__maxpsspercore_std': 0,
            'jobsconsumption__walltimeperevent_avg': 0,
            'jobsconsumption__walltimeperevent_std': 0,
            'jobsconsumption__cputimeperevent_avg': 0,
            'jobsconsumption__cputimeperevent_std': 0,
            'jobsmemoryleak__pss_mean': 0,
            'jobsmemoryleak__vmem_mean': 0,
        }

    def collect_data(self):
        """
        Collect data to form a report
        :return: exit_code
        :return: message
        """
        query = {'jeditaskid__in': self.jeditaskid_list}
        # get task data
        tasks_list = list(JediTasks.objects.filter(**query).values())

        if len(tasks_list) > 0:
            # get task parameters
            tasks_params_list = list(JediTaskparams.objects.filter(**query).values())
            tasks_params_dict = {t['jeditaskid']: json.loads(t['taskparams']) for t in tasks_params_list}

            # get datasets info
            tasks_list = get_datasets_for_tasklist(tasks_list)

            # get jobs data
            values_extra = ('exeerrorcode', 'exeerrordiag')
            jobs = get_job_list(query=query, values=values_extra)
            # # add metadata from metatable
            # jobs = addJobMetadata(jobs)
            jobs_per_task = {}
            for job in jobs:
                if job['jeditaskid'] not in jobs_per_task:
                    jobs_per_task[job['jeditaskid']] = {'jobs_all': [], 'jobs': []}
                jobs_per_task[job['jeditaskid']]['jobs_all'].append(job)
            for jeditaskid, task_jobs in jobs_per_task.items():
                jobs_per_task[jeditaskid]['jobs'], _, _ = drop_job_retries(task_jobs['jobs_all'], jeditaskid)

            # add aggregates to task_list
            for task in tasks_list:
                tid = task['jeditaskid']
                if tid not in self.data:
                    self.data[tid] = copy.deepcopy(self.data_template)
                    self.data[tid]['jeditaskid'] = tid
                if 'creationdate' in task and task['creationdate']:
                    self.data[tid]['creationdate'] = task['creationdate']
                if 'transuses' in task and task['transuses'] and '-' in task['transuses']:
                    self.data[tid]['configuration__release_branch'] = task['transuses'].split('-')[1]
                if 'transhome' in task and task['transhome'] and '-' in task['transhome']:
                    self.data[tid]['configuration__release_project'] = task['transhome'].split('_')[0].split('-')[1]
                    self.data[tid]['configuration__release_nightly_tag'] = task['transhome'].split('_')[1]
                    try:
                        self.data[tid]['configuration__release_nightly_date'] = parse(
                            self.data[tid]['configuration__release_nightly_tag'])
                    except Exception as ex:
                        _logger.exception('Failed to parse nightly tag {} to datetime: {}'.format(
                            self.data[tid]['configuration__release_nightly_tag'], ex))
                if 'architecture' in task and task['architecture']:
                    self.data[tid]['configuration__release_platform'] = task['architecture']

                if tid in tasks_params_dict:
                    if 'cliParams' in tasks_params_dict[tid]:
                        self.data[tid]['configuration__submit_command'] = tasks_params_dict[tid]['cliParams']
                        cliparams = [option for option in tasks_params_dict[tid]['cliParams'].split(' --')[1:]]
                        self.data[tid]['configuration__conditions_tag'] = [
                            p.replace("'", "").split(':')[1] for p in cliparams if p.startswith('conditionsTag') and ':' in p][0]
                        self.data[tid]['configuration__output_formats'] = [
                            p.split('=')[1].split('.')[1] for p in cliparams if p.startswith('output') and '=' in p]
                if 'datasets' in task:
                    _, ds_stats = calculate_dataset_stats(task['datasets'])
                    self.data[tid]['configuration__input_datasetname'] = ','.join(
                        [ds['datasetname'] for ds in task['datasets'] if ds['type'] == 'input'])
                    self.data[tid]['configuration__input_scope'] = task['datasets'][0]['scope'] if len(task['datasets']) > 0 else ''
                    self.data[tid]['configuration__input_nevents'] = ds_stats['neventsTot']
                    self.data[tid]['configuration__input_nfiles'] = ds_stats['nfiles']
                if tid in jobs_per_task:
                    self.data[tid]['jobs__status_all'] = {
                        s: n for s, n in job_state_count(jobs_per_task[tid]['jobs_all']).items() if n > 0}
                    self.data[tid]['jobs__status_retries_excl'] = {
                        s: n for s, n in job_state_count(jobs_per_task[tid]['jobs']).items() if n > 0}
                    counts = count_occurrences(jobs_per_task[tid]['jobs_all'], ['attemptnr', 'computingsite', 'exeerrorcode'])
                    self.data[tid]['jobs__attempts'] = counts['attemptnr']
                    self.data[tid]['jobs__computingsites'] = counts['computingsite']
                    # "exe:65" or "exe:68"
                    self.data[tid]['errors__athena'] = {c: n for c, n in counts['exeerrorcode'].items() if c in (65, 68)}
                    jobconsumption = job_consumption_plots(jobs_per_task[tid]['jobs_all'])
                    for metric in ['maxpsspercore', 'walltimeperevent', 'cputimeperevent']:
                        for jcm in jobconsumption:
                            if jcm['name'] == '{}_finished'.format(metric):
                                self.data[tid]['jobsconsumption__' + metric + '_avg'] = jcm['data']['data']['run']['stats'][0]
                                self.data[tid]['jobsconsumption__' + metric + '_std'] = jcm['data']['data']['run']['stats'][1]

                # get average nfilesperjob
                self.data[tid]['jobs__nfilesperjob_average'] = JediDatasetContents.objects.filter(
                    jeditaskid=tid, type='input'
                ).values('jeditaskid', 'pandaid').annotate(
                    nfilesperjob=Count('pandaid')
                ).aggregate(nfilesperjob_avg=Avg(F('nfilesperjob')))['nfilesperjob_avg']

        else:
            return 'Provided jeditaskid does not exist'

        return ''

    def export_data(self):
        """
        Export report data to OpenSearch
        :return:
        """
        # prepare data
        data_os = []
        for tid, data in self.data.items():
            dict_tmp = {}
            for pn, pv in data.items():
                if isinstance(pv, dict):
                    dict_tmp[pn] = ', '.join(['{} ({})'.format(k, v) for k, v in pv.items()])
                else:
                    dict_tmp[pn] = pv
            data_os.append(dict_tmp)

        # upload
        index_name = "atlas_large_scale_athena_tests"
        connection_os = create_os_connection()
        result = upload_data(connection_os, index_name, data_os,
                             timestamp_param='configuration__release_nightly_date',
                             id_param='jeditaskid')

        return result
