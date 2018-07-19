"""
    Created on 16.07.2018
    :author Tatiana Korchuganova
    A set of views showing comparison of PanDA object parameters
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
from core.common.models import BPUser
from core.compare.modelsCompare import ObjectsComparison
from core.compare.utils import add_to_comparison, clear_comparison_list, delete_from_comparison

@login_customrequired
def addToComparison(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'object' in request.session['requestParams']:
        object = request.session['requestParams']['object']
    if 'value' in request.session['requestParams']:
        value = request.session['requestParams']['value']

    newList = []
    if request.user.is_authenticated():
        userid = request.user.id
        # try:
        newList = add_to_comparison(object, userid, value)
        # except:
        #     pass

    data = {'newList': newList}
    dump = json.dumps(data, cls=DateEncoder)
    ##self monitor
    endSelfMonitor(request)
    return HttpResponse(dump, content_type='text/html')

@login_customrequired
def deleteFromComparison(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'object' in request.session['requestParams']:
        object = request.session['requestParams']['object']
    if 'value' in request.session['requestParams']:
        value = request.session['requestParams']['value']

    newList = []
    if request.user.is_authenticated():
        userid = request.user.id
        # try:
        newList = delete_from_comparison(object, userid, value)
        # except:
        #     pass

    data = {'newList': newList}
    dump = json.dumps(data, cls=DateEncoder)
    ##self monitor
    endSelfMonitor(request)
    return HttpResponse(dump, content_type='text/html')


@login_customrequired
def clearComparison(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'object' in request.session['requestParams']:
        object = request.session['requestParams']['object']

    newList = []
    if request.user.is_authenticated():
        userid = request.user.id
        # try:
        result = clear_comparison_list(object, userid)
        # except:
        #     pass

    data = {'result': result}
    dump = json.dumps(data, cls=DateEncoder)
    ##self monitor
    endSelfMonitor(request)
    return HttpResponse(dump, content_type='text/html')

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

    pandaidstr = None
    if 'pandaid' in request.session['requestParams']:
        pandaidstr = request.session['requestParams']['pandaid'].split('|')
    else:
        query = {}
        query['userid'] = request.user.id
        query['object'] = 'job'
        try:
            jobsComparison = ObjectsComparison.objects.get(**query)
            pandaidstr = json.loads(jobsComparison.comparisonlist)
        except ObjectsComparison.DoesNotExist:
            pandaidstr = None


    if not pandaidstr:
        return render_to_response('errorPage.html', {'errormessage': 'No pandaids for comparison provided'}, content_type='text/html')



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
                     'attemptnr': 'Attempt', 'creationtime': 'Created', 'waittime': 'Time to start', 'duration': 'Duration',
                     'modificationtime': 'Modified', 'cloud': 'Cloud', 'computingsite': 'Site', 'currentpriority': 'Priority',
                     'jobname': 'Name', 'processingtype': 'Type', 'transformation': 'Transformation', 'proddblock': 'Input',
                     'destinationdblock': 'Output', 'jobsetid': 'Jobset ID', 'batchid': 'Batch ID', 'eventservice': 'Event Service'}

    compareParams = ['produsername', 'reqid', 'jeditaskid', 'jobstatus', 'attemptnr','creationtime', 'waittime', 'duration',
                         'modificationtime', 'cloud', 'computingsite','currentpriority',
                         'jobname', 'processingtype', 'transformation','proddblock','destinationdblock', 'jobsetid', 'batchid','eventservice']

    ###Excluded params because of too long values###
    excludedParams = ['metadata', 'metastruct']

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
        if param not in excludedParams:
            row = [param]
            for job in jobInfoJSON:
                if param in job and job[param] is not None:
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