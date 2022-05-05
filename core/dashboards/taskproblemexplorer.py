"""
Task problem explorer dashboard aiming to help identify causes of task problems.
We target long analysis tasks that are or were in exhausted or throttled states.

"""

import logging
import json
import re
from datetime import datetime, timedelta

from django.shortcuts import render_to_response
from django.http import HttpResponse
from django.utils.cache import patch_response_headers

from core.oauth.utils import login_customrequired
from core.utils import is_json_request, extensibleURL
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, build_time_histogram, count_occurrences, \
    duration_df, build_stack_histogram
from core.libs.task import cleanTaskList
from core.libs.sqlcustom import preprocess_wild_card_string
from core.libs.TasksErrorCodesAnalyser import TasksErrorCodesAnalyser
from core.views import initRequest, setupView

from core.common.models import JediTasks, TasksStatusLog

from core.settings import defaultDatetimeFormat
import core.constants as const

_logger = logging.getLogger('bigpandamon')


@login_customrequired
def taskProblemExplorer(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "taskProblemExplorer")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('taskProblemExplorer.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    query, extra_str, _ = setupView(request, querytype='task', wildCardExt=True)

    age = 3
    if 'age' in request.session['requestParams']:
        age = int(request.session['requestParams']['age'])
    request.session['viewParams']['selection'] = ', active analysis tasks older than {} days'.format(age)
    if 'modificationtime__castdate__range' in query:
        del query['modificationtime__castdate__range']
    query['tasktype'] = 'anal'
    query['creationdate__castdate__range'] = [
            (datetime.now() - timedelta(days=360)).strftime(defaultDatetimeFormat),
            datetime.now().strftime(defaultDatetimeFormat)]
    exquery = {
        'status__in': const.TASK_STATES_FINAL + ('paused', ),
        # 'superstatus__in': const.TASK_STATES_FINAL,
    }
    extra_str += ' AND creationdate < sysdate - {}'.format(age)
    if 'owner' in request.session['requestParams'] and request.session['requestParams']['owner']:
        owner = request.session['requestParams']['owner']
        if '!' in owner:
            extra_str += ' AND (({} {}) AND ({}))'.format(
                preprocess_wild_card_string(owner, 'workinggroup'), 'OR workinggroup IS NULL' if owner.startswith('!') else '',
                preprocess_wild_card_string(owner, 'username'), 'OR username IS NULL' if owner.startswith('!') else '',
            )
        elif '*' in owner:
            extra_str += ' AND ({} OR {})'.format(
                preprocess_wild_card_string(owner, 'workinggroup'), preprocess_wild_card_string(owner, 'username'),
            )
        else:
            extra_str += " AND (workinggroup='{owner}' OR username='{owner}') ".format(owner=request.session['requestParams']['owner'])
    values = (
        'jeditaskid', 'tasktype', 'creationdate', 'starttime', 'statechangetime', 'modificationtime',
        'superstatus', 'status', 'corecount', 'taskpriority', 'currentpriority', 'username',
        'transuses', 'transpath', 'workinggroup', 'processingtype', 'campaign',
        'reqid', 'nucleus', 'eventservice', 'gshare', 'container_name', 'attemptnr',)

    tasks = []
    tasks.extend(JediTasks.objects.filter(**query).exclude(**exquery).extra(where=[extra_str]).values())
    _logger.debug('Got {} tasks'.format(len(tasks)))

    # clean  task list
    tasks = cleanTaskList(tasks, add_datasets_info=True)

    # filter out not suspicious tasks
    taskids_to_remove = {}
    diag_patterns_to_ignore = ['pending until parent produces input', 'insufficient inputs are ready']
    for task in tasks:
        for dp in diag_patterns_to_ignore:
            if dp in task['errordialog']:
                taskids_to_remove[task['jeditaskid']] = 1
    tasks = [task for task in tasks if task['jeditaskid'] not in taskids_to_remove]
    _logger.debug('{} tasks left after extra filtering'.format(len(tasks)))

    # put taskids to tmp table for the following queries
    taskids = [t['jeditaskid'] for t in tasks]
    tk = insert_to_temp_table(taskids)
    where_in_tids_str = 'jeditaskid in (select id from {} where transactionkey={})'.format(get_tmp_table_name(), tk)

    # check if a task were in exhausted|throttled states
    task_error_messages = []
    task_transient_states = []
    ts_query = {}
    # ts_query = {'status__in': ['exhausted', 'throttled', 'broken', 'failed']}
    task_transient_states.extend(
        TasksStatusLog.objects.filter(**ts_query).extra(where=[where_in_tids_str]).values('jeditaskid', 'status', 'modificationtime', 'reason'))
    task_transient_states_dict = {}
    for row in task_transient_states:
        if row['status'] in ('exhausted', 'throttled', 'broken', 'failed'):
            if row['jeditaskid'] not in task_transient_states_dict:
                task_transient_states_dict[row['jeditaskid']] = {}
                task_transient_states_dict[row['jeditaskid']]['status'] = {}
                task_transient_states_dict[row['jeditaskid']]['reasons'] = []
            if row['status'] not in task_transient_states_dict[row['jeditaskid']]['status']:
                task_transient_states_dict[row['jeditaskid']]['status'][row['status']] = 0
            task_transient_states_dict[row['jeditaskid']]['status'][row['status']] += 1
            task_transient_states_dict[row['jeditaskid']]['reasons'].append(row['reason'])
            # put reasons for error messages analyser
            task_error_messages.append({'jeditaskid': row['jeditaskid'], 'errordialog': re.sub('<[^>]*>', '', row['reason'])})

    # calculate duration of each task state
    task_status_agg = {
        'queued': ['defined', 'ready'],
        'running': ['scouting', 'running'],
        'troubling': ['pending', 'exhausted', 'throttled'],
        'final': ['finished', 'failed', 'broken', 'aborted']
    }
    task_transient_states_duration = duration_df(task_transient_states, id_name='jeditaskid', timestamp_name='modificationtime')
    states_duration_summary = {}
    states_duration_lists = {}
    states_agg_duration_dict = {}
    for task, states_duration in task_transient_states_duration.items():
        if task not in states_agg_duration_dict:
            states_agg_duration_dict[task] = {k: 0 for k in task_status_agg}
        for state, duration in states_duration.items():
            if duration > 0.0001:
                if state not in states_duration_summary:
                    states_duration_summary[state] = 0
                states_duration_summary[state] += duration
                if state not in const.TASK_STATES_FINAL:
                    if state not in states_duration_lists:
                        states_duration_lists[state] = []
                    states_duration_lists[state].append(duration*24)

                for agg_state, task_states_list in task_status_agg.items():
                    if state in task_states_list:
                        states_agg_duration_dict[task][agg_state] += duration*24

    for task in tasks:
        task['problematic_transient_states'] = '-'
        if task['jeditaskid'] in task_transient_states_dict:
            task['problematic_transient_states'] = ','.join([' {} times {}'.format(c, ts) for ts, c in task_transient_states_dict[task['jeditaskid']]['status'].items()])

        if task['jeditaskid'] in states_agg_duration_dict:
            task.update(states_agg_duration_dict[task['jeditaskid']])

    _logger.debug('Got and processed tasks transient states')

    error_codes_analyser = TasksErrorCodesAnalyser()
    error_codes_analyser.schedule_preprocessing(task_error_messages)

    # check for memory leaks

    # check for scouting problems

    # check for brokerage problems

    # plot queuetime, walltime, etc for task and jobs   


    error_summary_table = error_codes_analyser.get_errors_table()
    error_summary_table = json.dumps(error_summary_table, cls=DateEncoder)

    if not is_json_request(request):

        counts = count_occurrences(tasks, ['owner', ], output='list')

        plots = {
            'time_hist': {
                'name': 'time_hist',
                'type': 'bar_time',
                'title': 'Task submit time',
                'options': {
                    'labels': ['Task submission time', 'Count'],
                    'timeFormat': '%Y-%m-%d',
                },
                'data': [['Time'], ['Count']],
            },
            'tasks_by_owner': {
                'name': 'tasks_by_owner',
                'type': 'pie',
                'title': 'Owners',
                'options': {
                    'labels': ['Owner', 'Count'],
                },
                'data': counts['owner'] if 'owner' in counts else [],
            },
            'state_duration': {
                'name': 'state_duration',
                'type': 'pie',
                'title': 'Duration, days',
                'options': {
                    'labels': ['State', 'Duration, days'],
                    'size_mp': 0.3,
                },
                'data': [[state, round(dur, 3)] for state, dur in states_duration_summary.items() if dur > 0.0001],
            },
            'state_duration_hist': {
                'name': 'state_duration_hist',
                'type': 'bar_hist',
                'options': {
                    'labels': ['Total time in task state, hours', 'Number of tasks'],
                    'title': 'Task active state duration, hours',
                    'size_mp': 0.7,
                },
                'data': {'columns': [], 'stats': [], }
            },
        }
        for time, count in build_time_histogram([t['creationdate'] for t in tasks]):
            plots['time_hist']['data'][0].append(time.strftime(plots['time_hist']['options']['timeFormat']))
            plots['time_hist']['data'][1].append(count[0])
        stats, columns = build_stack_histogram(states_duration_lists, n_decimals=2, n_bin_max=100)
        plots['state_duration_hist']['data']['columns'] = columns
        plots['state_duration_hist']['data']['stats'] = stats

        timestamp_vars = ['modificationtime', 'statechangetime', 'starttime', 'creationdate', 'resquetime',
                          'endtime', 'lockedtime', 'frozentime', 'ttcpredictiondate']
        for task in tasks:
            for tp in task:
                if tp in timestamp_vars and task[tp] is not None:
                    task[tp] = task[tp].strftime(defaultDatetimeFormat)
                if task[tp] is None:
                    task[tp] = ''

        # prepare data for datatable
        task_list_table_headers = [
            'jeditaskid', 'category', 'owner', 'attemptnr', 'age', 'superstatus', 'status',
            'problematic_transient_states', 'queued', 'running', 'troubling',
            'nfiles', 'nfilesfinished', 'nfilesfailed', 'pctfinished', 'errordialog',
        ]
        tasks_to_show = []
        for t in tasks:
            tmp_list = []
            for h in task_list_table_headers:
                if h in t:
                    tmp_list.append(t[h])
                else:
                    tmp_list.append("-")
            tasks_to_show.append(tmp_list)

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': extensibleURL(request),
            'built': datetime.now().strftime("%H:%M:%S"),
            'tasks': tasks_to_show,
            'plots': plots,
            'error_summary_table': error_summary_table,
        }
        response = render_to_response('taskProblemExplorer.html', data, content_type='text/html')
        setCacheEntry(request, "taskProblemExplorer", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(tasks, cls=DateEncoder), content_type='application/json')
