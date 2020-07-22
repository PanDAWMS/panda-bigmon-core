from django.shortcuts import render_to_response
from core.settings import DKB_CAMPAIGN_URL
from django.db import connection
from core.views import initRequest, escapeInput
from django.views.decorators.cache import never_cache
import core.libs.CampaignPredictionHelper as cph
from django.core.cache import cache
import operator
from django.http import JsonResponse
import numpy as np
import humanize
from core.views import login_customrequired
import urllib3
import json

taskFinalStates = ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']
stepsOrder = ['Evgen', 'Evgen Merge', 'Simul', 'Merge', 'Digi', 'Reco', 'Rec Merge', 'Deriv', 'Deriv Merge', 'Rec TAG', 'Atlfast', 'Atlf Merge']


# @never_cache
# def campaignPredictionInfo(request):
#     initRequest(request)
#     return JsonResponse('', safe=False)

@login_customrequired
def campaignProgressDash(request):
    initRequest(request)
    if 'hashtag' in request.GET:
        hashtag = escapeInput(request.GET['hashtag'])
    else:
        hashtag = 'newfastcalosimntuponly'

    http = urllib3.PoolManager()
    data = {}
    try:
        r = http.request('GET', DKB_CAMPAIGN_URL, fields = {'htag':hashtag, 'pretty':'True'})
        data = json.loads(r.data.decode('utf-8'))['data']
    except Exception as exc:
        print (exc)
        return

    tasks_processing_summary = data['tasks_processing_summary']
    steps_tasks = set(tasks_processing_summary).intersection(stepsOrder)
    tasks_progress_seq_in_step_info = ['running', 'finished/done', 'aborted', 'broken', 'obsolete',  'total', 'start/end']
    tasks_progress = []
    for infoitem in tasks_progress_seq_in_step_info:
        row_in_progress_table = []
        row_in_progress_table.append(infoitem)
        for step in steps_tasks:
            if infoitem not in ('finished/done', 'start/end'):
                row_in_progress_table.append(tasks_processing_summary[step].get(infoitem, 0))
            elif infoitem=='finished/done':
                row_in_progress_table.append(str(tasks_processing_summary[step].get('finished', 0)) +
                                             ' / '+str(tasks_processing_summary[step].get('done', 0)))
            elif infoitem=='start/end':
                row_in_progress_table.append(str(tasks_processing_summary[step].get('start', '')) +
                                             ' / '+str(tasks_processing_summary[step].get('end', '')))
        tasks_progress.append(row_in_progress_table)

    tasks_progress_wrap = {
        'tasks_progress': tasks_progress,
        'tasks_progress_seq_in_step_info': tasks_progress_seq_in_step_info,
        'steps': steps_tasks,
    }

    events_processing_summary = data['overall_events_processing_summary']
    steps_events = set(events_processing_summary).intersection(stepsOrder)
    events_progress_seq_in_step_info = ['input', 'output', 'ratio']
    events_progress = []
    for infoitem in events_progress_seq_in_step_info:
        row_in_progress_table = []
        row_in_progress_table.append(infoitem)
        for step in steps_events:
            if infoitem != 'ratio':
                row_in_progress_table.append(events_processing_summary[step].get(infoitem, 0))
            else:
                row_in_progress_table.append(str(round(events_processing_summary[step].get(infoitem, 0)*100, 1)) + '%')
        events_progress.append(row_in_progress_table)

    events_progress_wrap = {
        'events_progress': events_progress,
        'events_progress_seq_in_step_info': events_progress_seq_in_step_info,
        'steps': steps_events,
    }

    request.session['viewParams']['selection'] = '' + hashtag
    data = {
        'tasks_progress_wrap':tasks_progress_wrap,
        'events_progress_wrap':events_progress_wrap,
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render_to_response('campaignProgressDKB.html', data, content_type='text/html')
    return response
