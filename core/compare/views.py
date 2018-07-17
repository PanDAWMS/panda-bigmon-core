"""
    Created on 16.07.2018
    :author Tatiana Korchuganova
    A set of views showing comparison of job parameters
"""

import json, urllib3

from datetime import datetime, timedelta

from django.utils import timezone
from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect
from django.db import connection, transaction, DatabaseError

from django.utils.cache import patch_cache_control, patch_response_headers


from core.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat
from core.libs.cache import deleteCacheTestData, getCacheEntry, setCacheEntry, preparePlotData
from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, escapeInput, DateEncoder, \
    extensibleURL, DateTimeEncoder, removeParam, taskSummaryDict, preprocessWildCardString

from core.pandajob.models import Jobsactive4, Jobsarchived4, Jobswaiting4, Jobsdefined4, Jobsarchived


@login_customrequired
def compareJobs(request):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "compareJobs")
    data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('compareJobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    if 'pandaid' in request.session['requestParams']:
        pandaidstr = request.session['requestParams']['pandaid'].split('|')

    pandaids = []
    for pid in pandaidstr:
        try:
            pid = int(pid)
            pandaids.append(pid)
        except:
            pass
    maxNJobs = 5
    if len(pandaids) > maxNJobs:
        pandaids = pandaids[:maxNJobs-1]


    jobInfoJSON = []

    jobURL = "http://bigpanda.cern.ch/job"  # This is deployment specific because memory monitoring is intended to work in ATLAS
    http = urllib3.PoolManager()
    for pandaid in pandaids:
        response = http.request('GET', jobURL, fields={'pandaid': pandaid, 'json': 1})
        jobInfoJSON.append(json.loads(response.data)['job'])

    compareParamNames = {'produsername': 'Owner', 'reqid': 'Request ID', 'jeditaskid': 'Task ID', 'jobstatus': 'Status',
                     'creationtime': 'Created', 'waittime': 'Time to start', 'duration': 'Duration',
                     'modificationtime': 'Modified', 'cloud': 'Cloud', 'computingsite': 'Site', 'currentpriority': 'Priority',
                     'jobname': 'Name', 'processingtype': 'Type', 'transformation': 'Transformation', 'proddblock': 'Input',
                     'destinationdblock': 'Output', 'jobsetid': 'Jobset ID'}

    compareParams = ['produsername', 'reqid', 'jeditaskid', 'jobstatus','creationtime', 'waittime', 'duration',
                         'modificationtime', 'cloud', 'computingsite','currentpriority',
                         'jobname', 'processingtype', 'transformation','proddblock','destinationdblock', 'jobsetid']

    jobsComparisonMain = []
    for param in compareParams:
        row = [compareParamNames[param]]
        for job in jobInfoJSON:
            if param in job:
                row.append(job[param])
            else:
                row.append('-')
        jobsComparisonMain.append(row)


    all_params = []
    for job in jobInfoJSON:
        all_params.extend(list(job.keys()))
    all_params = sorted(set(all_params))

    jobsComparisonAll = []
    for param in all_params:
        row = [param]
        for job in jobInfoJSON:
            if param in job:
                row.append(job[param])
            else:
                row.append('-')
        jobsComparisonAll.append(row)




    xurl = extensibleURL(request)
    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'url': request.path,
        'jobsComparisonMain': jobsComparisonMain,
        'jobsComparisonAll': jobsComparisonAll,
        'pandaids': pandaids,
        'xurl': xurl,
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    setCacheEntry(request, "compareJobs", json.dumps(data, cls=DateEncoder), 60 * 20)

    ##self monitor
    endSelfMonitor(request)
    response = render_to_response('compareJobs.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response