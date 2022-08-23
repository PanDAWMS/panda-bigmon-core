"""
    Created on 16.07.2018
    :author Tatiana Korchuganova
    A set of views showing comparison of PanDA object parameters
"""

import json
import multiprocessing
from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.db import connection, transaction, DatabaseError

from django.utils.cache import patch_response_headers

from core.oauth.utils import login_customrequired
from core.libs.cache import getCacheEntry
from core.libs.DateEncoder import DateEncoder
from core.views import initRequest, extensibleURL

from core.compare.modelsCompare import ObjectsComparison
from core.compare.utils import add_to_comparison, clear_comparison_list, delete_from_comparison, job_info_getter

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
    return HttpResponse(dump, content_type='application/json')

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
    return HttpResponse(dump, content_type='application/json')


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
    return HttpResponse(dump, content_type='application/json')


@login_customrequired
def compareJobs(request):
    valid, response = initRequest(request)
    if not valid: return response

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
        return render(request, 'errorPage.html', {'errormessage': 'No pandaids for comparison provided'}, content_type='text/html')

    pandaids = []
    for pid in pandaidstr:
        try:
            pid = int(pid)
            pandaids.append(pid)
        except:
            pass
    maxNJobs = 5
    if len(pandaids) > maxNJobs:
        pandaids = pandaids[:maxNJobs]


    jobInfoJSON = []

    # Looking for a job in cache
    pandaidsToBeLoad = []
    for pandaid in pandaids:
        data = getCacheEntry(request, "compareJob_" + str(pandaid), isData=True)
        # data = None
        if data is not None:
            jobInfoJSON.append(json.loads(data))
        else:
            pandaidsToBeLoad.append(pandaid)

    #Loading jobs info in parallel
    nprocesses = 1
    if len(pandaidsToBeLoad) > 0:
        url_params = [('?json=1&pandaid=' + str(pid)) for pid in pandaidsToBeLoad]
        pool = multiprocessing.Pool(processes=nprocesses)
        jobInfoJSON.extend(pool.map(job_info_getter, url_params))
        pool.close()
        pool.join()

    compareParamNames = {'produsername': 'Owner', 'reqid': 'Request ID', 'jeditaskid': 'Task ID', 'jobstatus': 'Status',
                     'attemptnr': 'Attempt', 'creationtime': 'Created', 'waittime': 'Time to start', 'duration': 'Duration',
                     'modificationtime': 'Modified', 'cloud': 'Cloud', 'computingsite': 'Site', 'currentpriority': 'Priority',
                     'jobname': 'Name', 'processingtype': 'Type', 'transformation': 'Transformation', 'proddblock': 'Input',
                     'destinationdblock': 'Output', 'jobsetid': 'Jobset ID', 'batchid': 'Batch ID', 'eventservice': 'Event Service'}

    compareParams = ['produsername', 'reqid', 'jeditaskid', 'jobstatus', 'attemptnr','creationtime', 'waittime', 'duration',
                         'modificationtime', 'cloud', 'computingsite','currentpriority',
                         'jobname', 'processingtype', 'transformation','proddblock','destinationdblock', 'jobsetid', 'batchid','eventservice']

    # Excluded params because of too long values###
    excludedParams = ['metadata', 'metastruct']
    params_to_exclude = []
    for job in jobInfoJSON:
        params_to_exclude.extend([p for p, v in job['job'].items() if isinstance(v, dict) or isinstance(v, list)])
    excludedParams.extend(list(set(params_to_exclude)))

    jobsComparisonMain = []
    for param in compareParams:
        row = [{'paramname': compareParamNames[param]}]
        for jobd in jobInfoJSON:
            job = jobd['job']
            if param in job:
                row.append({'value': job[param]})
            else:
                row.append({'value': '-'})
        if len(set([d['value'] for d in row if 'value' in d])) == 1:
            row[0]['mark'] = 'equal'
        jobsComparisonMain.append(row)


    all_params = []
    for jobd in jobInfoJSON:
        all_params.extend(list(jobd['job'].keys()))
    all_params = sorted(set(all_params))

    jobsComparisonAll = []
    for param in all_params:
        if param not in excludedParams:
            row = [{'paramname': param}]
            for jobd in jobInfoJSON:
                job = jobd['job']
                if param in job and job[param] is not None:
                    row.append({'value': job[param]})
                else:
                    row.append({'value': '-'})
            if len(set([d['value'] for d in row if 'value' in d])) == 1:
                row[0]['mark'] = 'equal'
            jobsComparisonAll.append(row)


    # #Put loaded jobs info to cache
    # for job in jobInfoJSON:
    #     setCacheEntry(request, "compareJob_" + str(job.keys()[0]),
    #                   json.dumps(job.values()[0], cls=DateEncoder), 60 * 30, isData=True)

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

    response = render(request, 'compareJobs.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response