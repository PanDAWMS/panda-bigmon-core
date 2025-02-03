import logging
from datetime import timedelta, datetime
from django.utils import timezone

from core.common.models import Rating, JediTasks
from core.oauth.models import BPUser
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, round_to_n_digits

from django.conf import settings

class TasksRatedReport:
    """
    This class is responsible for generating the report of the tasks rated by the users.
    """
    def __init__(self, days=7, rating_threshold=3):
        self.tasks = []
        self.overall_stats = {}
        self.emojis_dict = {
            1: "\U0001F621",  # 😡 Enraged Face
            2: "\U0001F620",  # 😠 Angry Face
            3: "\U0001F610",  # 😐 Neutral Face
            4: "\U0001F600",  # 😀 Grinning Face
            5: "\U0001F60D"   # 😍 Heart Eyes
        }
        self.date_format = '%Y-%m-%d'
        self.user_names = {}
        self.ratings = []
        self.days = days
        if isinstance(rating_threshold, int) and 1 <= rating_threshold <= 5:
            self.rating_threshold = rating_threshold
        else:
            raise ValueError("Rating threshold must be an integer between 1 and 5.")


    def collect_data(self):
        """
        Get the rated tasks from the database.
        """
        # get list of rated tasks
        self.ratings.extend(
            Rating.objects.filter(timestamp__gte=timezone.now() - timedelta(days=self.days), rating__lte=self.rating_threshold).values()
        )

        # get usernames who rated the tasks
        user_names = BPUser.objects.filter(id__in=[r['user_id'] for r in self.ratings]).values('id', 'first_name', 'last_name')
        self.user_names = {u['id']: f"{u['first_name']} {u['last_name']}" for u in user_names}

        # get task info for the rated tasks
        query = {}
        extra_str = '(1=1)'
        taskids = [r['task_id'] for r in self.ratings]
        if len(taskids) > settings.DB_N_MAX_IN_QUERY:
            tkey, _ = insert_to_temp_table(taskids)
            extra_str = f'jeditaskid in (select id from {settings.DB_SCHEMA}.{get_tmp_table_name()} where transactionkey={tkey})'
        else:
            query['jeditaskid__in'] = taskids

        self.tasks.extend(JediTasks.objects.filter(**query).extra(where=[extra_str]).values(
            'jeditaskid', 'tasktype', 'taskname', 'creationdate', 'modificationtime', 'status', 'username', 'framework',
            'transpath', 'resourcetype'))


    def prepare_data_page(self):
        """
        Prepare data for the report page.
        :return:
        """
        self.collect_data()
        task_dict = {t['jeditaskid']: t for t in self.tasks}
        for r in self.ratings:
            r['username'] = self.user_names[r['user_id']]
            r['emoji'] = self.emojis_dict[r['rating']]
            r['timestamp'] = datetime.strftime(r['timestamp'], self.date_format)
            if r['task_id'] in task_dict:
                if 'tasktype' in task_dict[r['task_id']] and task_dict[r['task_id']]['tasktype'].startswith('ana'):
                    r['task_type'] = 'analy'
                else:
                    r['task_type'] = 'prod'
                if 'framework' in task_dict[r['task_id']]:
                    r['task_framework'] = task_dict[r['task_id']]['framework']
                else:
                    r['task_framework'] = ''
                if 'transpath' in task_dict[r['task_id']]:
                    r['task_transpath'] = task_dict[r['task_id']]['transpath'].split('/')[-1]
                if 'creationdate' in task_dict[r['task_id']]:
                    r['task_creationdate'] = datetime.strftime(task_dict[r['task_id']]['creationdate'], self.date_format)
                if 'modificationtime' in task_dict[r['task_id']]:
                    r['task_modificationtime'] = datetime.strftime(task_dict[r['task_id']]['modificationtime'], self.date_format)
                if 'status' in task_dict[r['task_id']]:
                    r['task_status'] = task_dict[r['task_id']]['status']
                if 'resourcetype' in task_dict[r['task_id']]:
                    r['task_resourcetype'] = task_dict[r['task_id']]['resourcetype']
                if 'username' in task_dict[r['task_id']]:
                    r['task_username'] = task_dict[r['task_id']]['username']

        return self.ratings


    def prepare_data_email(self):
        """
        Prepare data for the report email.
        """

        self.collect_data()

        # add usernames, rating and feedback to the tasks
        for task in self.tasks:
            task['traspath'] = task['transpath'].split('/')[-1]
            task['creationdate'] = datetime.strftime(task['creationdate'], self.date_format)
            task['modificationtime'] = datetime.strftime(task['modificationtime'], self.date_format)
            if task['tasktype'].startswith('ana'):
                task['tasktype'] = 'analysis'
            if 'ratings' not in task:
                task['ratings'] = []
            for rating in self.ratings:
                if rating['task_id'] == task['jeditaskid']:
                    task['ratings'].append({
                        'value': rating['rating'],
                        'emoji': self.emojis_dict[rating['rating']],
                        'feedback': rating['feedback'],
                        'reporter': self.user_names[rating['user_id']]
                    })

        # calculate average rating and count of ratings for each task
        for task in self.tasks:
            task['rating_avg'] = sum([r['value'] for r in task['ratings']]) / len(task['ratings'])
            task['rating_avg_emoji'] = self.emojis_dict[round_to_n_digits(task['rating_avg'], 0, method='floor')]
            task['rating_count'] = len(task['ratings'])

        # calculate overall stats
        self.overall_stats['rating_avg'] = round_to_n_digits(sum([task['rating_avg'] for task in self.tasks]) / len(self.tasks), 2, method='normal')
        self.overall_stats['ratings_count'] = sum([task['rating_count'] for task in self.tasks])
        self.overall_stats['tasks_count'] = len(self.tasks)
        self.overall_stats['reporter_count'] = len(self.user_names)
        self.overall_stats['time_period'] = [
            datetime.strftime(timezone.now() - timedelta(days=self.days), self.date_format),
            datetime.strftime(timezone.now(), self.date_format)
        ]

        return {'tasks': self.tasks, 'stats': self.overall_stats}
