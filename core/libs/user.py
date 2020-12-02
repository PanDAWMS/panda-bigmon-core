"""
Created by Tatiana Korchuganova on 18.11.2020
"""

from core.settings import defaultDatetimeFormat

from core.libs.task import build_stack_histogram
from core.common.models import Users


def get_panda_user_stats(fullname):
    """
    Getting statistics of recent CPU usage by a user
    :param fullname:
    :return userstats: dict
    """
    query = {'name__icontains': fullname}

    userdb = Users.objects.filter(**query).values()
    if len(userdb) > 0:
        userstats = userdb[0]
        for field in ['cpua1', 'cpua7', 'cpup1', 'cpup7']:
            try:
                userstats[field] = "%0.1f" % (float(userstats[field]) / 3600.)
            except:
                userstats[field] = '-'
        for timefield in ['cachetime', 'firstjob', 'lastmod', 'latestjob']:
            try:
                userstats[timefield] = userstats[timefield].strftime(defaultDatetimeFormat)
            except:
                userstats[timefield] = userstats[timefield]
    else:
        userstats = None

    return userstats


def prepare_user_dash_plots(tasks, **kwargs):

    plots_dict = {
        'nfiles_sum_status': {
            'type': 'pie',
            'data': {'done': 0, 'failed': 0, 'remaining': 0},
            'title': 'Input files by status',
            'options': {'legend_position': 'bottom'}
        },
        'ntasks_by_status': {
            'type': 'pie',
            'data': {},
            'title': 'N tasks by status',
            'options': {'legend_position': 'bottom'}
        },
        'age_hist': {
            'type': 'bar',
            'data': {},
            'title': 'Task age histogram',
            'options': {'labels': ['Task age, days', 'Number of tasks']}
        },
    }

    # collect data
    for task in tasks:
        if 'nfiles' in task and isinstance(task['nfiles'], int) and task['status'] not in ('aborting', 'aborted'):
            plots_dict['nfiles_sum_status']['data']['remaining'] += task['nfiles']
            if 'nfilesfinished' in task and isinstance(task['nfilesfinished'], int):
                plots_dict['nfiles_sum_status']['data']['done'] += task['nfilesfinished']
                plots_dict['nfiles_sum_status']['data']['remaining'] -= task['nfilesfinished']
            if 'nfilesfailed' in task and isinstance(task['nfilesfailed'], int):
                plots_dict['nfiles_sum_status']['data']['failed'] += task['nfilesfailed']
                plots_dict['nfiles_sum_status']['data']['remaining'] -= task['nfilesfailed']

        if task['status'] not in plots_dict['ntasks_by_status']['data']:
            plots_dict['ntasks_by_status']['data'][task['status']] = 0
        plots_dict['ntasks_by_status']['data'][task['status']] += 1

        if task['age'] > 0:
            if task['username'] not in plots_dict['age_hist']['data']:
                plots_dict['age_hist']['data'][task['username']] = []
            plots_dict['age_hist']['data'][task['username']].append((task['age']))

    for plot, pdict in plots_dict.items():
        if pdict['type'] == 'bar':
            stats, columns = build_stack_histogram(plots_dict['age_hist']['data'], n_decimals=1)
            plots_dict[plot]['data'] = columns
            plots_dict[plot]['options']['stats'] = stats

    # dict -> list
    for plot, pdict in plots_dict.items():
        if pdict['type'] == 'pie':
            pdict['data'] = [[p, v] for p, v in pdict['data'].items() if v > 0]

    plots_list = []
    for plot, pdict in plots_dict.items():
        pdict['name'] = plot
        plots_list.append(pdict)

    return plots_list


def humanize_metrics(metrics):
    """
    Prepare interesting metrics for display
    :param metrics:
    :return:
    """
    metric_defs = {
        'failed': {
            'title': 'Jobs failure',
            'unit': '%',
        },
        'maxpss_per_actualcorecount': {
            'title': 'Average maxPSS/core',
            'unit': 'GB',
        },
        'walltime': {
            'title': 'Average jobs walltime',
            'unit': 'hours',
        },
        'queuetime': {
            'title': 'Average jobs time to start',
            'unit': 'hours',
        },
        'efficiency': {
            'title': ' Average jobs efficiency',
            'unit': '%',
        },
        'attemptnr': {
            'title': ' Average number of job attempts',
            'unit': '',
        },
        'cpua7': {
            'title': 'Personal CPU hours for last 7 days',
            'unit': '',
        },
        'cpup7': {
            'title': 'Group CPU hours for last 7 days',
            'unit': '',
        },
    }

    metrics_thresholds = {
        'pss': {
            'warning': [2.0, 2.5],
            'alert': [2.5, 1000000]
        },
        'time': {
            'warning': [24, 36],
            'alert': [48, 1000000]
        },
        'walltime': {
            'warning': [1, 2],
            'alert': [0, 1]
        },
        'queuetime': {
            'warning': [4, 12],
            'alert': [12, 1000000]
        },
        'fail': {
            'warning': [25, 50],
            'alert': [50, 100]
        },
        'efficiency': {
            'warning': [50, 70],
            'alert': [0, 50]
        },
        'attemptnr': {
            'warning': [3, 5],
            'alert': [5, 100]
        }
    }

    metrics_list = []
    for md in metric_defs:
        if md in metrics and metrics[md]:
            metric_defs[md]['class'] = []
            if 'pss' in md:
                metric_defs[md]['value'] = round(metrics[md]/1024., 2)
            elif 'efficiency' in md:
                metric_defs[md]['value'] = round(metrics[md] * 100., 2)
            else:
                metric_defs[md]['value'] = metrics[md]

            for key, thresholds in metrics_thresholds.items():
                if key in md:
                    metric_defs[md]['class'].extend([c for c, crange in thresholds.items() if metric_defs[md]['value'] >= crange[0] and metric_defs[md]['value'] < crange[1]])

            metric_defs[md]['class'] = metric_defs[md]['class'][0] if len(metric_defs[md]['class']) > 0 else ''
            metrics_list.append(metric_defs[md])

    return metrics_list
