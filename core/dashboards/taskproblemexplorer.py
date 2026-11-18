"""
Task problem explorer dashboard aiming to help identify causes of task problems.
We target long analysis tasks that are or were in exhausted or throttled states.

"""

import logging
import json
import re
from collections import defaultdict, Counter
from datetime import datetime, timedelta

from django.db.models import Case, When, F, Value, CharField
from django.db.models.functions import Substr
from django.shortcuts import render
from django.http import HttpResponse
from django.utils.cache import patch_response_headers

from core.oauth.decorators import login_required
from core.utils import is_json_request, extensibleURL
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, build_time_histogram, count_occurrences, \
    group_low_occurrences, duration_df, build_stack_histogram
from core.libs.task import cleanTaskList
from core.libs.sqlcustom import preprocess_wild_card_string
from core.libs.TasksErrorCodesAnalyser import TasksErrorCodesAnalyser
from core.views import initRequest, setupView

from core.pandajob.models import JediTasks, TasksStatusLog

from django.conf import settings
import core.constants as const

_logger = logging.getLogger('bigpandamon')

TASK_AGE_THRESHOLD = 7
PROBLEM_STATES = ('exhausted', 'throttled', 'broken', 'failed')
N_TASKS_CHUNK_SIZE = 200
TASK_STATUS_AGG = {
    'queued': ['defined', 'ready'],
    'running': ['scouting', 'running'],
    'troubling': ['pending', 'exhausted', 'throttled'],
    'final': ['finished', 'failed', 'broken', 'aborted'],
}
STATE_TO_AGG = {s: agg for agg, states in TASK_STATUS_AGG.items() for s in states}
PROBLEM_REASON = Case(
    When(status__in=PROBLEM_STATES, then=Substr(F('reason'), 1, 4000)),
    default=Value(None),
    output_field=CharField(),
)


@login_required
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
        response = render(request, 'taskProblemExplorer.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    query, extra_str, _ = setupView(request, querytype='task', wildCardExt=True)

    age = TASK_AGE_THRESHOLD
    if 'age' in request.session['requestParams']:
        age = int(request.session['requestParams']['age'])
    request.session['viewParams']['selection'] = ', active analysis tasks older than {} days'.format(age)
    if 'modificationtime__castdate__range' in query:
        del query['modificationtime__castdate__range']
    query['tasktype'] = 'anal'
    query['creationdate__castdate__range'] = [
            (datetime.now() - timedelta(days=360)).strftime(settings.DATETIME_FORMAT),
            datetime.now().strftime(settings.DATETIME_FORMAT)]
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
        'reqid', 'nucleus', 'eventservice', 'gshare', 'container_name', 'attemptnr', 'errordialog',)

    tasks = []
    tasks.extend(JediTasks.objects.filter(**query).exclude(**exquery).extra(where=[extra_str]).values(*values))
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

    # aggregate durations when task was in problematic/rinning states + reasons
    taskids = sorted([t['jeditaskid'] for t in tasks])
    problem_states = {}
    duration_summary = defaultdict(float)
    duration_lists = defaultdict(list)
    agg_duration = {}
    reasons = Counter()
    for i in range(0, len(taskids), N_TASKS_CHUNK_SIZE):
        rows_by_task = defaultdict(list)
        chunk = taskids[i:i + N_TASKS_CHUNK_SIZE]
        chunk += [chunk[-1]] * (N_TASKS_CHUNK_SIZE - len(chunk))
        qs = (TasksStatusLog.objects
              .filter(jeditaskid__in=chunk)
              .values_list('jeditaskid', 'status', 'modificationtime', 'reason')
              .iterator())
        for tid, status, mtime, reason in qs:
            rows_by_task[tid].append((mtime, status, reason if status in PROBLEM_STATES else None))

        for tid, rows in rows_by_task.items():
            rows.sort(key=lambda r: r[0])
            per_state = defaultdict(float)
            for j, (mtime, status, reason) in enumerate(rows):
                if status in PROBLEM_STATES:
                    st = problem_states.setdefault(tid, {'status': {}})['status']
                    st[status] = st.get(status, 0) + 1
                    reasons[(tid, re.sub('<[^>]*>', '', reason or ''))] += 1
                if j + 1 < len(rows):
                    per_state[status] += (rows[j + 1][0] - mtime).total_seconds() / 86400

            agg = {k: 0 for k in TASK_STATUS_AGG}
            for status, dur in per_state.items():
                if dur > 0.0001:
                    duration_summary[status] += dur
                    if status not in const.TASK_STATES_FINAL:
                        duration_lists[status].append(dur * 24)
                    if status in STATE_TO_AGG:
                        agg[STATE_TO_AGG[status]] += dur * 24
            agg_duration[tid] = agg

    error_messages = [{'jeditaskid': tid, 'errordialog': msg} for (tid, msg) in reasons]
    agg_duration_dict = dict(agg_duration)
    states_duration_lists = dict(duration_lists)

    for task in tasks:
        task['problematic_transient_states'] = '-'
        if task['jeditaskid'] in problem_states:
            task['problematic_transient_states'] = ','.join([' {} times {}'.format(c, ts) for ts, c in problem_states[task['jeditaskid']]['status'].items()])

        if task['jeditaskid'] in agg_duration_dict:
            task.update(agg_duration_dict[task['jeditaskid']])
    _logger.debug('Got and processed tasks transient states')

    error_codes_analyser = TasksErrorCodesAnalyser()
    error_codes_analyser.schedule_preprocessing(error_messages)
    error_summary_table = error_codes_analyser.get_errors_table()
    error_summary_table = json.dumps(error_summary_table, cls=DateEncoder)

    if not is_json_request(request):
        if len(tasks) > 0:
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
                    'data': group_low_occurrences(counts['owner'], threshold=0.005) if 'owner' in counts else [],
                },
                'state_duration': {
                    'name': 'state_duration',
                    'type': 'pie',
                    'title': 'Duration, days',
                    'options': {
                        'labels': ['State', 'Duration, days'],
                        'size_mp': 0.3,
                    },
                    'data': [[state, round(dur, 3)] for state, dur in duration_summary.items() if dur > 0.0001],
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
                        task[tp] = task[tp].strftime(settings.DATETIME_FORMAT)
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
        else:
            tasks_to_show = []
            plots = {}

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
        response = render(request, 'taskProblemExplorer.html', data, content_type='text/html')
        setCacheEntry(request, "taskProblemExplorer", json.dumps(data, cls=DateEncoder), 60 * 60)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(tasks, cls=DateEncoder), content_type='application/json')
