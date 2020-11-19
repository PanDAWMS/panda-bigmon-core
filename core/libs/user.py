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
            'title': 'N files by status',
            'options': {'legend_position': 'bottom'}
        },
        'ntasks_by_status': {
            'type': 'pie',
            'data': {},
            'title': 'N tasks by status',
            'options': {'legend_position': 'bottom'}
        },
        'ntasks_by_gshare': {
            'type': 'pie',
            'data': {},
            'title': 'N tasks by gshare',
            'options': {'legend_position': 'bottom'}
        },
        'ntasks_by_processingtype': {
            'type': 'pie',
            'data': {},
            'title': 'Tasks by proc. type',
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
        if 'nfiles' in task and isinstance(task['nfiles'], int):
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

        if task['gshare'] not in plots_dict['ntasks_by_gshare']['data']:
            plots_dict['ntasks_by_gshare']['data'][task['gshare']] = 0
        plots_dict['ntasks_by_gshare']['data'][task['gshare']] += 1

        if task['processingtype'] and task['processingtype'].startswith('panda'):
            task['processingtype'] = task['processingtype'].split('jedi-')[1]
        if task['processingtype'] not in plots_dict['ntasks_by_processingtype']['data']:
            plots_dict['ntasks_by_processingtype']['data'][task['processingtype']] = 0
        plots_dict['ntasks_by_processingtype']['data'][task['processingtype']] += 1

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