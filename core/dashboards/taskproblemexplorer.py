"""
Task problem explorer dashboard aiming to help identify causes of task problems.
We target long analysis tasks that are or were in exhausted or throttled states.

"""

import logging
import json
from datetime import datetime

from django.shortcuts import render_to_response
from django.http import HttpResponse
from django.utils.cache import patch_response_headers

from core.oauth.utils import login_customrequired
from core.utils import is_json_request, extensibleURL
from core.libs.DateEncoder import DateEncoder
from core.libs.cache import setCacheEntry
from core.views import initRequest, setupView

from core.common.models import JediTasks

import core.constants as const

_logger = logging.getLogger('bipandamon')


@login_customrequired
def taskProblemExplorer(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    query, extra, _ = setupView(request, querytype='task', wildCardExt=True)

    age = 3
    if 'age' in request.session['requestParams']:
        age = int(request.session['requestParams']['age'])

    query = {
        'tasktype': 'anal',
    }
    exquery = {
        'status__in': const.TASK_STATES_FINAL
    }
    extra_str = 'creationdate > sysdate - {}'.format(age)

    tasks = []
    tasks.extend(JediTasks.objects.filter(**query).exclude(**exquery).extra(where=[extra_str]).values())

    _logger.debug('Got {} tasks'.format(len(tasks)))

    # check if a task were in exhausted|throttled states
    # check for memory leaks
    # check for scouting problems
    # check for brokerage problems

    # plot queuetime, walltime, etc for task and jobs   

    if not is_json_request(request):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': extensibleURL(request),
            'built': datetime.now().strftime("%H:%M:%S"),

        }
        response = render_to_response('taskProblemExplorer.html', data, content_type='text/html')
        setCacheEntry(request, "taskProblemExplorer", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps({}), content_type='application/json')
