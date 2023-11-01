"""
Created by Tatiana Korchuganova on 18.11.2020
"""
import logging

from core.libs.exlib import build_stack_histogram, convert_grams, round_to_n_digits
from core.common.models import Users

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


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
                userstats[timefield] = userstats[timefield].strftime(settings.DATETIME_FORMAT)
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
            'options': {'legend_position': 'bottom', 'size_mp': 0.2,}
        },
        'ntasks_by_status': {
            'type': 'pie',
            'data': {},
            'title': 'N tasks by status',
            'options': {'legend_position': 'bottom', 'size_mp': 0.2,}
        },
        'age_hist': {
            'type': 'bar_stacked',
            'options': {
                'labels': ['Task age, days', 'Number of tasks'],
                'title': 'Task age histogram, days',
                'size_mp': 0.4,
                'color_scheme': 'task_states',
            },
            'data': {'columns': [], 'stats': [], 'data_raw': {} },
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
            if task['status'] not in plots_dict['age_hist']['data']['data_raw']:
                plots_dict['age_hist']['data']['data_raw'][task['status']] = []
            plots_dict['age_hist']['data']['data_raw'][task['status']].append((task['age']))

    for plot, pdict in plots_dict.items():
        if pdict['type'] == 'bar_stacked':
            stats, columns = build_stack_histogram(plots_dict['age_hist']['data']['data_raw'], n_decimals=1, n_bin_max=50)
            plots_dict[plot]['data']['columns'] = columns
            plots_dict[plot]['data']['stats'] = stats
            try:
                del plots_dict['age_hist']['data']['data_raw']
            except:
                _logger.exception('Failed to remove raw data for bar chart')

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
            'class': [],
        },
        'maxpss_per_actualcorecount': {
            'title': 'Median maxPSS/core',
            'unit': 'GB',
            'class': [],
        },
        'walltime': {
            'title': 'Median jobs walltime',
            'unit': 'hours',
            'class': [],
        },
        'queuetime': {
            'title': 'Median jobs time to start',
            'unit': 'hours',
            'class': [],
        },
        'efficiency': {
            'title': 'Median jobs efficiency',
            'unit': '%',
            'class': [],
        },
        'attemptnr': {
            'title': 'Average number of job attempts',
            'unit': '',
            'class': [],
        },
        'running_slots': {
            'title': 'Number of currently allocated slots',
            'unit': '',
            'class': ['neutral',],
        },
        'gco2': {
            'title': 'Estimated CO2 total',
            'unit': '',
            'class': ['neutral', ],
        },
        'gco2_loss': {
            'title': 'Estimated CO2 by failed jobs',
            'unit': '',
            'class': ['neutral', ],
        },
        'cpua7': {
            'title': 'Personal CPU hours for last 7 days',
            'unit': '',
            'class': ['neutral', ],
        },
        'cpup7': {
            'title': 'Group CPU hours for last 7 days',
            'unit': '',
            'class': ['neutral', ],
        },
    }

    metrics_thresholds = {
        'pss': {
            'warning': [2, 2.5],
            'alert': [2.5, 1000000]
        },
        'time': {
            'warning': [36, 48],
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
            'warning': [20, 40],
            'alert': [40, 100]
        },
        'efficiency': {
            'warning': [50, 70],
            'alert': [0, 50]
        },
        'attemptnr': {
            'warning': [2, 4],
            'alert': [4, 100]
        }
    }

    metrics_list = []
    for md in metric_defs:
        if md in metrics and metrics[md]:
            if 'pss' in md:
                metric_defs[md]['value'] = round(metrics[md], 2)
            elif 'efficiency' in md:
                metric_defs[md]['value'] = round(metrics[md] * 100., 2)
            elif 'co2' in md:
                metric_defs[md]['value'], metric_defs[md]['unit'] = convert_grams(metrics[md], output_unit='auto')
                metric_defs[md]['value'] = round_to_n_digits(metric_defs[md]['value'], n=0, method='floor')
            else:
                metric_defs[md]['value'] = metrics[md]

            for key, thresholds in metrics_thresholds.items():
                if key in md:
                    metric_defs[md]['class'].extend([c for c, crange in thresholds.items() if metric_defs[md]['value'] >= crange[0] and metric_defs[md]['value'] < crange[1]])

            metric_defs[md]['class'] = metric_defs[md]['class'][0] if len(metric_defs[md]['class']) > 0 else 'ok'
            metrics_list.append(metric_defs[md])

    return metrics_list
