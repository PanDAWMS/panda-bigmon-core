import logging
import math
from datetime import datetime, timedelta
from core.common.models import JediTasks, TaskAttempts
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from core.libs.exlib import drop_duplicates
from core.libs.job import add_job_category
from core.libs.task import get_task_duration

import core.constants as const
from django.conf import settings

_logger = logging.getLogger('bigpandamon')

class TaskProgressPlot:
    taskid = None
    task_profile_dict = None
    def __init__(self, taskid):
        self.taskid = taskid
        task_profile_dict = None

    def get_task_info(self):
        """
        Getting task info, creationdate, starttime etc.
        :return: task: dict - containing task timestamps (creationdate, starttime etc.) or None if task not found
        """
        query = {'jeditaskid': self.taskid}
        tasks = JediTasks.objects.filter(**query).values('status', 'starttime', 'creationdate', 'modificationtime')
        if len(tasks) > 0:
            task = list(tasks)[0]
            # calculate xmin and xmax for the plot
            task_duration = get_task_duration(task, out_unit='sec')
            task['xmin'] = (task['creationdate'] - timedelta(seconds=task_duration * 0.01)).strftime(settings.DATETIME_FORMAT)
            task['xmax'] = (task['modificationtime'] + timedelta(seconds=task_duration * 0.01)).strftime(settings.DATETIME_FORMAT)
            return task
        else:
            return None

    def get_task_attempts(self):
        """
        Getting timestamps of task attempts
        :return:
        """
        query = {'jeditaskid': self.taskid}
        task_attempts = list(TaskAttempts.objects.filter(**query).order_by('attemptnr', 'starttime').values())
        return task_attempts

    def get_raw_task_profile_full(self, jobstatus=None, category=None):
        """
        A method to form a task execution profile
        :param jobstatus: list - list of job statuses (finished, failed, closed, cancelled)
        :param category: list - list of job categories (build, run, merge)
        :return:
        """

        jobs = []
        jquery = {
            'jeditaskid': self.taskid,
        }
        if len(jobstatus) > 0:
            jquery['jobstatus__in'] = jobstatus
        jvalues = ('pandaid', 'processingtype', 'transformation', 'nevents', 'ninputdatafiles',
                   'jobstatus', 'starttime', 'creationtime', 'endtime',)
        jobs_models = (Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived)
        for jm in jobs_models:
            jobs.extend(jm.objects.filter(**jquery).values(*jvalues))
        jobs = drop_duplicates(jobs)
        jobs = add_job_category(jobs)
        jobs = [j for j in jobs if j['category'] in category]
        jobs = sorted(jobs, key=lambda x: x['endtime'] if x['endtime'] is not None else x['creationtime'])

        # overwrite nevents to 0 for unfinished and build/merge jobs
        job_categories = ['build', 'run', 'merge']
        job_timestamps = ['creation', 'start', 'end']
        for j in jobs:
            if j['category'] in ('build', 'merge'):
                j['nevents'] = 0
                j['ninputdatafiles'] = 0
            if j['jobstatus'] != 'finished':
                j['nevents'] = 0
                j['ninputdatafiles'] = 0
            if j['ninputdatafiles'] is None:
                j['ninputdatafiles'] = 0

            j['creation'] = j['creationtime']
            if j['starttime'] is None or j['starttime'] < j['creationtime']:
                j['start'] = j['creation']
            else:
                j['start'] = j['starttime']
            j['end'] = j['endtime']

        # create task profile dict
        task_profile_dict = {}
        for jc in job_categories:
            task_profile_dict[jc] = []
        fin_i = 0
        fin_ev = 0
        fin_files = 0
        for j in jobs:
            fin_i += 1
            fin_ev += j['nevents']
            fin_files += j['ninputdatafiles']
            temp = {}
            for jtm in job_timestamps:
                temp[jtm] = j[jtm]
            temp['events'] = fin_ev
            temp['files'] = fin_files
            temp['jobs'] = fin_i
            temp['jobstatus'] = j['jobstatus'] if j['jobstatus'] in const.JOB_STATES_FINAL else 'active'
            temp['pandaid'] = j['pandaid']
            task_profile_dict[j['category']].append(temp)

        return task_profile_dict


    def prepare_plot_data(self, jobstatus=None, category=None, progress_unit=None):
        """
        Prepare plot data for task profile
        :param jobstatus: list - list of job statuses (finished, failed, closed, cancelled)
        :param category: list - list of job categories (build, run, merge)
        :param progress_unit: str - 'jobs' or 'events' or 'files'
        :return:
        """
        if jobstatus is None:
            jobstatus = list(const.JOB_STATES)
        if category is None:
            category = ['build', 'run', 'merge']
        if progress_unit is None:
            progress_unit = 'jobs'

        # get raw jobs data
        self.task_profile_dict = self.get_raw_task_profile_full(jobstatus, category)

        # convert raw data to format acceptable by chart.js library
        job_time_names = ['end', 'start', 'creation']
        job_types = ['build', 'run', 'merge']
        job_states = ['active', 'finished', 'failed', 'closed', 'cancelled']
        colors = {
            'creation': {'active': 'RGBA(0,169,255,0.75)', 'finished': 'RGBA(162,198,110,1)', 'failed': 'RGBA(255,176,176,1)',
                         'closed': 'RGBA(214,214,214,1)', 'cancelled': 'RGBA(255,227,177,1)'},
            'start': {'active': 'RGBA(0,85,183,0.75)', 'finished': 'RGBA(70,181,117,0.8)', 'failed': 'RGBA(235,0,0,0.8)',
                      'closed': 'RGBA(100,100,100,0.8)', 'cancelled': 'RGBA(255,165,0,0.8)'},
            'end': {'active': 'RGBA(0,0,141,0.75)', 'finished': 'RGBA(2,115,0,0.8)', 'failed': 'RGBA(137,0,0,0.8)',
                    'closed': 'RGBA(0,0,0,0.8)', 'cancelled': 'RGBA(157,102,0,0.8)'},
        }
        markers = {'build': 'triangle', 'run': 'circle', 'merge': 'crossRot'}
        order_mpx = {
            'creation': 3,
            'start': 2,
            'end': 1,
            'active': 3,
            'finished': 7,
            'failed': 8,
            'closed': 9,
            'cancelled': 9,
        }
        order_dict = {}
        for jtn in job_time_names:
            for js in job_states:
                order_dict[jtn + '_' + js] = order_mpx[js] * order_mpx[jtn]  # the higher value print first

        task_profile_data_dict = {}
        for jt in job_types:
            if len(self.task_profile_dict[jt]) > 0:
                for js in list(set(job_states) & set([r['jobstatus'] for r in self.task_profile_dict[jt]])):
                    for jtmn in job_time_names:
                        task_profile_data_dict['_'.join((jtmn, js, jt))] = {
                            'name': '_'.join((jtmn, js, jt)),
                            'label': jtmn.capitalize() + ' time of a ' + js + ' ' + jt + ' job',
                            'pointRadius': round(1 + 3.0 * math.exp(-0.0004 * len(self.task_profile_dict[jt]))),
                            'backgroundColor': colors[jtmn][js],
                            'borderColor': colors[jtmn][js],
                            'pointStyle': markers[jt],
                            'order': order_dict[jtmn + '_' + js],
                            'data': [],
                        }

        for jt in job_types:
            if jt in self.task_profile_dict:
                rdata = self.task_profile_dict[jt]
                for r in rdata:
                    for jtn in job_time_names:
                        if jtn in r and r[jtn] is not None:
                            task_profile_data_dict['_'.join((jtn, r['jobstatus'], jt))]['data'].append({
                                'x': r[jtn].strftime(settings.DATETIME_FORMAT),
                                'y': r[progress_unit],
                                'label': r['pandaid'],
                            })

        # deleting point groups if data is empty
        group_to_remove = []
        for group in task_profile_data_dict:
            if len(task_profile_data_dict[group]['data']) == 0:
                group_to_remove.append(group)
        for group in group_to_remove:
            try:
                del task_profile_data_dict[group]
            except:
                _logger.info('failed to remove key from dict')

        # dict -> list
        task_profile_data = [v for k, v in task_profile_data_dict.items()]

        return task_profile_data

    def prepare_attempts_data(self):
        """
        Prepare data for task attempts plot
        :return:
        """
        task_attempts = self.get_task_attempts()
        task_attempts_data_dict = {}
        for t in task_attempts:
            task_attempts_data_dict[f'Attempt {t['attemptnr']+1} start'] = {
                'type': 'line',
                'borderWidth': 1,
                'label': {'content': f'{t['attemptnr']+1}', 'position': 'end', 'display': True, 'yAdjust': 20, 'padding': 2, 'backgroundColor': 'rgba(0, 128, 186, 0.8)', 'z':10},
                'xMin': t['starttime'].strftime(settings.DATETIME_FORMAT),
                'xMax': t['starttime'].strftime(settings.DATETIME_FORMAT),
            }
            if t['endtime'] is not None:
                task_attempts_data_dict[f'Attempt {t['attemptnr']+1} end'] = {
                    'type': 'line',
                    'borderWidth': 1,
                    'borderColor': 'rgba(0, 0, 0, 1)',
                    'label': {'content':f'{t['attemptnr']+1}', 'position': 'end', 'display': True, 'padding': 2, 'z':10},
                    'xMin': t['endtime'].strftime(settings.DATETIME_FORMAT),
                    'xMax': t['endtime'].strftime(settings.DATETIME_FORMAT),
                }
        return task_attempts_data_dict








