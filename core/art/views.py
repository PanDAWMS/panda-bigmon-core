"""
    art.views

"""
import json
import re
from datetime import datetime, timedelta
from django.utils import timezone
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection, transaction
from core.art.modelsART import ARTTask, ARTTasks, ARTResults
from django.db.models.functions import Concat, Substr
from django.db.models import Value as V, Sum
from core.views import initRequest, extensibleURL, removeParam
from core.views import setCacheEntry, getCacheEntry, DateEncoder, endSelfMonitor
from core.art.jobSubResults import getJobReport, getARTjobSubResults
from core.settings import defaultDatetimeFormat
from django.db.models import Q

artdateformat = '%Y-%m-%d'
humandateformat = '%d %b %Y'
cache_timeout = 15


def setupView(request, querytype='task'):
    if not 'view' in request.session['requestParams']:
        request.session['requestParams']['view'] = 'packages'
    query = {}
    if 'ntag_from' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag_from']
        try:
            startdate = datetime.strptime(startdatestr, '%Y-%m-%d')
        except:
            del request.session['requestParams']['ntag_from']

    if 'ntag_to' in request.session['requestParams']:
        enddatestr = request.session['requestParams']['ntag_to']
        try:
            enddate = datetime.strptime(enddatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag_to']

    if 'ntag' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag']
        try:
            startdate = datetime.strptime(startdatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag']
    if 'ntags' in request.session['requestParams']:
        dateliststr = request.session['requestParams']['ntags']
        datelist = []
        for datestr in dateliststr.split(','):
            try:
                datei = datetime.strptime(datestr, artdateformat)
                datelist.append(datei)
            except:
                pass

    if 'ntag_from' in request.session['requestParams'] and not 'ntag_to' in request.session['requestParams']:
        enddate = startdate + timedelta(days=7)
    elif not 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams']:
        startdate = enddate - timedelta(days=7)
    elif not 'ntag_from' in request.session['requestParams'] and not 'ntag_to' in request.session['requestParams']:
        if 'ntag' in request.session['requestParams']:
            enddate = startdate
        else:
            enddate = datetime.now()
            startdate = enddate - timedelta(days=7)
    elif 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams'] and (enddate-startdate).days > 7:
        enddate = startdate + timedelta(days=7)


    if not 'ntag' in request.session['requestParams']:
        request.session['requestParams']['ntag_from'] = startdate
        request.session['requestParams']['ntag_to'] = enddate
    elif 'ntags' in request.session['requestParams']:
        request.session['requestParams']['ntags'] = datelist
        request.session['requestParams']['ntag_from'] = min(datelist)
        request.session['requestParams']['ntag_to'] = max(datelist)

    else:
        request.session['requestParams']['ntag'] = startdate



    querystr = ''
    if querytype == 'job':
        if 'package' in request.session['requestParams']:
            packages = request.session['requestParams']['package'].split(',')
            querystr += '(UPPER(PACKAGE) IN ( '
            for p in packages:
                querystr += 'UPPER(\'\'' + p + '\'\'), '
            if querystr.endswith(', '):
                querystr = querystr[:len(querystr) - 2]
            querystr += ')) AND '
        if 'branch' in request.session['requestParams']:
            branches = request.session['requestParams']['branch'].split(',')
            querystr += '(UPPER(NIGHTLY_RELEASE_SHORT || \'\'/\'\' || PLATFORM || \'\'/\'\' || PROJECT)  IN ( '
            for b in branches:
                querystr += 'UPPER(\'\'' + b + '\'\'), '
            if querystr.endswith(', '):
                querystr = querystr[:len(querystr) - 2]
            querystr += ')) AND '
        if 'taskid' in request.session['requestParams']:
            querystr += '(a.TASK_ID = ' + request.session['requestParams']['taskid'] + ' ) AND '
        if 'ntags' in request.session['requestParams']:
            querystr += '((SUBSTR(NIGHTLY_TAG, 0, INSTR(NIGHTLY_TAG, \'\'T\'\')-1)) IN ('
            for datei in datelist:
                querystr+= '\'\'' + datei.strftime(artdateformat) + '\'\', '
            if querystr.endswith(', '):
                querystr = querystr[:len(querystr) - 2]
            querystr += ')) AND '
        if querystr.endswith('AND '):
            querystr = querystr[:len(querystr)-4]
        else:
            querystr += '(1=1)'
        query['strcondition'] = querystr
        query['ntag_from'] = startdate.strftime(artdateformat)
        query['ntag_to'] = enddate.strftime(artdateformat)
    elif querytype == 'task':
        if 'package' in request.session['requestParams'] and not ',' in request.session['requestParams']['package']:
            query['package'] = request.session['requestParams']['package']
        elif 'package' in request.session['requestParams'] and ',' in request.session['requestParams']['package']:
            query['package__in'] = [p for p in request.session['requestParams']['package'].split(',')]
        if 'branch' in request.session['requestParams'] and not ',' in request.session['requestParams']['branch']:
            query['branch'] = request.session['requestParams']['branch']
        elif 'branch' in request.session['requestParams'] and ',' in request.session['requestParams']['branch']:
            query['branch__in'] = [b for b in request.session['requestParams']['branch'].split(',')]
        if not 'ntags' in request.session['requestParams']:
            query['ntag__range'] = [startdate.strftime(artdateformat), enddate.strftime(artdateformat)]
        else:
            query['ntag__in'] = [ntag.strftime(artdateformat) for ntag in datelist]



    return query


def art(request):
    valid, response = initRequest(request)
    tquery = {}
    packages = ARTTask.objects.filter(**tquery).values('package').distinct()
    branches = ARTTask.objects.filter(**tquery).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'platform', V('/'), 'project')).values('branch').distinct()
    ntags = ARTTask.objects.values('nightly_tag').annotate(nightly_tag_date=Substr('nightly_tag', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]


    data = {
        'viewParams': request.session['viewParams'],
        'packages':[p['package'] for p in packages],
        'branches':[b['branch'] for b in branches],
        'ntags':[t['nightly_tag_date'] for t in ntags]
    }
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
        response = render_to_response('artMainPage.html', data, content_type='text/html')
    else:
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def artOverview(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')

    # Here we try to get cached data
    data = getCacheEntry(request, "artOverview")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artOverview.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response


    statestocount = ['finished', 'failed', 'active']
    # packages = ARTTasks.objects.filter(**query).values('package', 'ntag').annotate(nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))

    artpackagesdict = {}
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        query_raw = """SELECT package, ntag, status, count(*) as njobs FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))
                      group by package, ntag, status""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])
        cur = connection.cursor()
        cur.execute(query_raw)
        tasks_raw = cur.fetchall()
        cur.close()
        artJobs = ['package', 'ntag', 'status', 'njobs']
        packages = [dict(zip(artJobs, row)) for row in tasks_raw]
        ntagslist = list(sorted(set([x['ntag'] for x in packages])))
        # packages = ARTTasks.objects.filter(**query).values('package', 'ntag').annotate(
        #     nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
        for p in packages:
            if p['package'] not in artpackagesdict.keys():
                artpackagesdict[p['package']] = {}
                for n in ntagslist:
                    artpackagesdict[p['package']][n.strftime(artdateformat)] = {}
                    artpackagesdict[p['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    for state in statestocount:
                        artpackagesdict[p['package']][n.strftime(artdateformat)][state] = 0
    
            if p['ntag'].strftime(artdateformat) in artpackagesdict[p['package']]:

                if p['status'] in ('finished', 'failed'):
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)][p['status']] += p['njobs']
                else:
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]['active'] = p['njobs']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        query_raw = """SELECT branch, ntag, status, count(*) as njobs FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))
                        group by branch, ntag, status""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])
        cur = connection.cursor()
        cur.execute(query_raw)
        tasks_raw = cur.fetchall()
        cur.close()
        artJobs = ['branch', 'ntag', 'status', 'njobs']
        packages = [dict(zip(artJobs, row)) for row in tasks_raw]
        ntagslist = list(sorted(set([x['ntag'] for x in packages])))
        # packages = ARTTasks.objects.filter(**query).values('branch', 'ntag').annotate(
        #     nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
        for p in packages:
            if p['branch'] not in artpackagesdict.keys():
                artpackagesdict[p['branch']] = {}
                for n in ntagslist:
                    artpackagesdict[p['branch']][n.strftime(artdateformat)] = {}
                    artpackagesdict[p['branch']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    for state in statestocount:
                        artpackagesdict[p['branch']][n.strftime(artdateformat)][state] = 0

            if p['ntag'].strftime(artdateformat) in artpackagesdict[p['branch']]:
                if p['status'] in ('finished', 'failed'):
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)][p['status']] += p['njobs']
                else:
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]['active'] = p['njobs']
        
    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    data = {
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'artpackages': artpackagesdict,
        'noviewurl': noviewurl,
        'ntaglist': [ntag.strftime(artdateformat) for ntag in ntagslist],
    }
    setCacheEntry(request, "artOverview", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    response = render_to_response('artOverview.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response


def artTasks(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')

    # Here we try to get cached data
    data = getCacheEntry(request, "artTasks")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artTasks.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    cur = connection.cursor()
    query_raw = """SELECT package, branch, ntag, taskid, status, count(*) as njobs FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))
        group by package, branch, ntag, taskid, status""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])

    cur.execute(query_raw)
    tasks_raw = cur.fetchall()
    cur.close()

    artJobs = ['package', 'branch', 'ntag', 'task_id', 'status', 'njobs']
    tasks = [dict(zip(artJobs, row)) for row in tasks_raw]

    # tasks = ARTTasks.objects.filter(**query).values('package','branch','task_id', 'ntag', 'nfilesfinished', 'nfilesfailed')
    ntagslist = list(sorted(set([x['ntag'] for x in tasks])))
    statestocount = ['finished', 'failed', 'active']
    arttasksdict = {}
    if not 'view' in request.session['requestParams'] or ('view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for task in tasks:
            if task['package'] not in arttasksdict.keys():
                arttasksdict[task['package']] = {}
            if task['branch'] not in arttasksdict[task['package']].keys():
                arttasksdict[task['package']][task['branch']] = {}
                for n in ntagslist:
                    arttasksdict[task['package']][task['branch']][n.strftime(artdateformat)] = {}
                    arttasksdict[task['package']][task['branch']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    arttasksdict[task['package']][task['branch']][n.strftime(artdateformat)]['tasks'] = {}
            if task['ntag'].strftime(artdateformat) in arttasksdict[task['package']][task['branch']]:
                if task['task_id'] not in arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['tasks']:
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['tasks'][task['task_id']] = {}
                    for state in statestocount:
                        arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['tasks'][
                            task['task_id']][state] = 0
                if task['status'] in ('finished','failed'):
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['tasks'][
                        task['task_id']][task['status']] += task['njobs']
                else:
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['tasks'][
                        task['task_id']]['active'] += task['njobs']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for task in tasks:
            if task['branch'] not in arttasksdict.keys():
                arttasksdict[task['branch']] = {}
            if task['package'] not in arttasksdict[task['branch']].keys():
                arttasksdict[task['branch']][task['package']] = {}
                for n in ntagslist:
                    arttasksdict[task['branch']][task['package']][n.strftime(artdateformat)] = {}
                    arttasksdict[task['branch']][task['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    arttasksdict[task['branch']][task['package']][n.strftime(artdateformat)]['tasks'] = {}
            if task['ntag'].strftime(artdateformat) in arttasksdict[task['branch']][task['package']]:
                if task['task_id'] not in arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['tasks']:
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['tasks'][task['task_id']] = {}
                    for state in statestocount:
                        arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['tasks'][
                            task['task_id']][state] = 0
                if task['status'] in ('finished', 'failed'):
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['tasks'][task['task_id']][task['status']] = task['njobs']
                else:
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['tasks'][
                        task['task_id']]['active'] = task['njobs']

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')
    data = {
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'arttasks' : arttasksdict,
        'noviewurl': noviewurl,
        'ntaglist': [ntag.strftime(artdateformat) for ntag in ntagslist],
    }

    setCacheEntry(request, "artTasks", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    response = render_to_response('artTasks.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response


def artJobs(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')

    # Here we try to get cached data
    data = getCacheEntry(request, "artJobs")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artJobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    cur = connection.cursor()
    cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))" % (query['ntag_from'], query['ntag_to'], query['strcondition']))
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid','package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid', 'computingsite', 'guid', 'scope', 'lfn', 'taskstatus', 'taskmodificationtime', 'jobmodificationtime', 'result']
    jobs = [dict(zip(artJobsNames, row)) for row in jobs]

    ntagslist=list(sorted(set([x['ntag'] for x in jobs])))

    artjobsdict={}
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for job in jobs:
            if job['package'] not in artjobsdict.keys():
                artjobsdict[job['package']] = {}
            if job['branch'] not in artjobsdict[job['package']].keys():
                artjobsdict[job['package']][job['branch']] = {}
            if job['testname'] not in artjobsdict[job['package']][job['branch']].keys():
                artjobsdict[job['package']][job['branch']][job['testname']] = {}
                for n in ntagslist:
                    artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)] = {}
                    artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)]['jobs'] = {}
            if job['ntag'].strftime(artdateformat) in artjobsdict[job['package']][job['branch']][job['testname']]:
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']] = {}
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['jobstatus'] = job['jobstatus']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['origpandaid'] = job['origpandaid']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['computingsite'] = job['computingsite']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['guid'] = job['guid']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['scope'] = job['scope']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['lfn'] = job['lfn']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['jeditaskid'] = job['taskid']
                try:
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = int(re.search('.([0-9]{6}).log.', job['lfn']).group(1))
                except:
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = ''

                if len(ntagslist) == 1:
                    try:
                        job['result'] = json.loads(job['result'])
                        artjobsdict[job['package']][job['branch']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testexitcode'] = \
                        job['result']['exit_code'] if 'exit_code' in job['result'] else None
                        artjobsdict[job['package']][job['branch']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testresult'] = \
                        job['result']['result'] if 'result' in job['result'] else []
                    except:
                        artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testexitcode'] = None
                        artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testresult'] = None

    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for job in jobs:
            if job['branch'] not in artjobsdict.keys():
                artjobsdict[job['branch']] = {}
            if job['package'] not in artjobsdict[job['branch']].keys():
                artjobsdict[job['branch']][job['package']] = {}
            if job['testname'] not in artjobsdict[job['branch']][job['package']].keys():
                artjobsdict[job['branch']][job['package']][job['testname']] = {}
                for n in ntagslist:
                    artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)] = {}
                    artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)]['jobs'] = {}
            if job['ntag'].strftime(artdateformat) in artjobsdict[job['branch']][job['package']][job['testname']]:
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']] = {}
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['jobstatus'] = job['jobstatus']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['origpandaid'] = job['origpandaid']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['computingsite'] = job['computingsite']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['guid'] = job['guid']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['scope'] = job['scope']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['lfn'] = job['lfn']
                try:
                    artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = int(re.search('.([0-9]{6}).log.', job['lfn']).group(1))
                except:
                    artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = ''

                if len(ntagslist) == 1 and job['guid'] is not None and job['lfn'] is not None and job['scope'] is not None:
                    try:
                        job['result'] = json.loads(job['result'])
                        artjobsdict[job['branch']][job['package']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testexitcode'] = \
                            job['result']['exit_code'] if 'exit_code' in job['result'] else None
                        artjobsdict[job['branch']][job['package']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testresult'] = \
                            job['result']['result'] if 'result' in job['result'] else []
                    except:
                        artjobsdict[job['branch']][job['package']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testexitcode'] = None
                        artjobsdict[job['branch']][job['package']][job['testname']][
                            job['ntag'].strftime(artdateformat)]['jobs'][job['origpandaid']]['testresult'] = None

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    data = {
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'artjobs': artjobsdict,
        'noviewurl': noviewurl,
        'ntaglist': [ntag.strftime(artdateformat) for ntag in ntagslist],
    }
    setCacheEntry(request, "artJobs", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    response = render_to_response('artJobs.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response

def updateARTJobList(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')
    starttime = datetime.now()

    ### Getting full list of jobs
    cur = connection.cursor()
    cur.execute("SELECT taskid, ntag, pandaid, guid, scope, lfn, taskstatus, status as jobstatus, testname, taskmodificationtime, jobmodificationtime  FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s')) WHERE pandaid is not NULL" % (query['ntag_from'], query['ntag_to'], query['strcondition']))
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['jeditaskid', 'ntag', 'pandaid', 'guid', 'scope', 'lfn', 'taskstatus', 'jobstatus', 'testname', 'taskmodificationtime', 'jobmodificationtime']
    fulljoblist = [dict(zip(artJobsNames, row)) for row in jobs]
    ntagslist = list(sorted(set([x['ntag'] for x in fulljoblist])))

    i = 0
    ci = 0
    ii = 0
    if len(fulljoblist) > 0:
        for j in fulljoblist:
            i +=1
            get_query = {}
            get_query['jeditaskid'] = j['jeditaskid']
            get_query['testname'] = j['testname']

            blockedRowsConditions = (~Q(is_locked=1)) | Q(lock_time__lt=(timezone.now() - timedelta(minutes=30))) # This save from rerunning jobs which currnetly (first condition)
            # or recently (second condition) updated by another worker

            is_result_update = False
            existedRow = None
            try:
                existedRow = ARTResults.objects.filter(**get_query).filter(blockedRowsConditions).get()

            except:
                if getjflag(j) == 1:

                    insertRow = ARTResults(jeditaskid=j['jeditaskid'], pandaid=j['pandaid'],
                                           is_task_finished=None,
                                           is_job_finished=None, testname=j['testname'],
                                           task_flag_updated=None,
                                           job_flag_updated=None,
                                           result=None,
                                           is_locked = 1,
                                           lock_time = datetime.now())
                    insertRow.save()

                    results = getARTjobSubResults(getJobReport(j['guid'], j['lfn'], j['scope'])) if getjflag(j) == 1 else {}
                    insertRow.result = json.dumps(results)
                    insertRow.is_locked = 0
                    insertRow.lock_time = datetime.now()
                    insertRow.save(update_fields=['result', 'is_locked','lock_time'])
                    # insertRow = ARTResults(jeditaskid=j['jeditaskid'], pandaid=j['pandaid'], is_task_finished=gettflag(j),
                    #                        is_job_finished=getjflag(j), testname=j['testname'],
                    #                        task_flag_updated=datetime.now(),
                    #                        job_flag_updated=datetime.now(),
                    #                        result=json.dumps(results),
                    #                        is_locked=0,
                    #                        lock_time = None)
                else:
                    insertRow = ARTResults(jeditaskid=j['jeditaskid'], pandaid=j['pandaid'],
                                           is_task_finished=gettflag(j),
                                           is_job_finished=getjflag(j), testname=j['testname'],
                                           task_flag_updated=datetime.now(),
                                           job_flag_updated=datetime.now(),
                                           result=None)
                    insertRow.save()
                # if getjflag(j) == 1:
                #     insertRow.save(update_fields=['pandaid','is_job_finished','task_flag_updated','job_flag_updated','result','is_locked','lock_time'])
                # else:
                #     insertRow.save()

                ii += 1
                print ('%s row inserted (%s out of %s)' % (ii, i, len(fulljoblist)))

            if existedRow is not None:
                try:
                    existedResult = json.loads(existedRow.result)
                except:
                    existedResult = None
                ### check whether a job was retried
                if j['pandaid'] != existedRow.pandaid:
                    ### update pandaid -> it is needed to load json
                    existedRow.pandaid = j['pandaid']
                    if getjflag(j) == 1:
                        is_result_update = True
                    else:
                        existedRow.result = None
                        existedRow.save(update_fields=['pandaid','result'])
                elif existedResult is None:
                    ### no result in table, check whether a job finished already
                    if existedRow.is_job_finished < gettflag(j) or getjflag(j) == 1:
                        is_result_update = True
                else:
                    ### result is not empty, check whether a job was updated
                    if j['jobmodificationtime'] > existedRow.job_flag_updated:
                        ### job state was updated results needs to be updated too
                        is_result_update = True

                if is_result_update:
                    existedRow.is_locked = 1
                    existedRow.lock_time = datetime.now()
                    existedRow.save(update_fields=['is_locked','lock_time'])
                    results = getARTjobSubResults(getJobReport(j['guid'], j['lfn'], j['scope']))
                    existedRow.is_job_finished = getjflag(j)
                    existedRow.is_task_finished = gettflag(j)
                    existedRow.job_flag_updated = datetime.now()
                    existedRow.result = json.dumps(results)
                    existedRow.is_locked = 0
                    existedRow.lock_time = None
                    existedRow.save(update_fields=['pandaid', 'is_task_finished','is_job_finished', 'job_flag_updated', 'result', 'is_locked','lock_time'])

                    ci += 1
                    print ('%s row updated (%s out of %s)' % (ci,i,len(fulljoblist)))




    # ### Getting list of existed jobs
    # extra = 'jeditaskid in ( '
    # fulljoblistdict = {}
    # for job in fulljoblist:
    #     if job['jeditaskid'] not in fulljoblistdict.keys():
    #         fulljoblistdict[job['jeditaskid']] = {}
    #         extra +=  str(job['jeditaskid']) + ','
    #     fulljoblistdict[job['jeditaskid']][job['pandaid']] = []
    # if extra.endswith(','):
    #     extra = extra[:-1]
    # if extra.endswith('( '):
    #     extra = ' ( 1=1'
    # extra += ' ) '

    # existedjoblist = ARTResults.objects.extra(where=[extra]).values()
    # existedjobdict = {}
    # if len(existedjoblist) > 0:
    #     for job in existedjoblist:
    #         if job['jeditaskid'] not in existedjobdict.keys():
    #             existedjobdict[job['jeditaskid']] = {}
    #         if job['testname'] not in existedjobdict[job['jeditaskid']].keys():
    #             existedjobdict[job['jeditaskid']][job['testname']] = {}
    #         existedjobdict[job['jeditaskid']][job['testname']][job['pandaid']] = job
    #
    # tableName = 'ATLAS_PANDABIGMON.ART_RESULTS'
    # ###
    # insertData = []
    # updateData = []
    # # updateResultsData = []
    # if len(existedjoblist) > 0:
    #     print ('to be filtered')
    #
    #     for j in fulljoblist:
    #         print ('%s rows to insert' % (len(insertData)))
    #         print ('%s rows to update' % (len(updateData)))
    #         if j['jeditaskid'] in existedjobdict:
    #             ### check whether a job was retried
    #             if j['pandaid'] not in existedjobdict[j['jeditaskid']][j['testname']]:
    #                 ### add to update list
    #                 results = getARTjobSubResults(getJobReport(j['guid'], j['lfn'], j['scope']))
    #                 updateData.append((j['pandaid'], gettflag(j), getjflag(j), datetime.now().strftime(defaultDatetimeFormat), json.dumps(results), j['jeditaskid'], j['testname']))
    #             elif existedjobdict[j['jeditaskid']][j['testname']][j['pandaid']]['result'] is None or len(existedjobdict[j['jeditaskid']][j['testname']][j['pandaid']]['result']) == 0:
    #                 ### no result in table, check whether a job finished already
    #                 if existedjobdict[j['jeditaskid']][j['testname']][j['pandaid']]['is_job_finished'] < gettflag(j):
    #                     ### job state was updated results needs to be updated too
    #                     results = getARTjobSubResults(getJobReport(j['guid'],j['lfn'],j['scope']))
    #                     updateData.append((j['pandaid'], gettflag(j), getjflag(j), datetime.now().strftime(defaultDatetimeFormat), json.dumps(results), j['jeditaskid'], j['testname']))
    #             else:
    #                 ### result is not empty, check whether a job was updated
    #                 if j['jobmodificationtime'] > existedjobdict[j['jeditaskid']][j['testname']][j['pandaid']]['job_flag_updated']:
    #                     ### job state was updated results needs to be updated too
    #                     results = getARTjobSubResults(getJobReport(j['guid'],j['lfn'],j['scope']))
    #                     updateData.append((j['pandaid'], gettflag(j), getjflag(j), datetime.now().strftime(defaultDatetimeFormat), json.dumps(results), j['jeditaskid'], j['testname']))
    #         else:
    #             ### a new task that needs to be added to insert list
    #             if getjflag(j) == 1:
    #                 results = getARTjobSubResults(getJobReport(j['guid'],j['lfn'],j['scope']))
    #             else:
    #                 results = {}
    #             insertData.append((j['taskid'], j['pandaid'], gettflag(j), getjflag(j), j['testname'],
    #                                    datetime.now().strftime(defaultDatetimeFormat),
    #                                    datetime.now().strftime(defaultDatetimeFormat), json.dumps(results)))
    #
    #
    # else:
    #     print ('preparing data to insert into artresults table')
    #     for j in fulljoblist:
    #         print ('%s rows to insert' % (len(insertData)))
    #         if j['pandaid'] is not None and j['jeditaskid'] is not None:
    #             if getjflag(j) == 1:
    #                 results = getARTjobSubResults(getJobReport(j['guid'],j['lfn'],j['scope']))
    #             else:
    #                 results = {}
    #             insertData.append((j['jeditaskid'], j['pandaid'], gettflag(j), getjflag(j), j['testname'], datetime.now().strftime(defaultDatetimeFormat), datetime.now().strftime(defaultDatetimeFormat),json.dumps(results)))
    #
    #
    # if len(insertData) > 0:
    #     new_cur = connection.cursor()
    #     insert_query = """INSERT INTO """ + tableName + """(JEDITASKID,PANDAID,IS_TASK_FINISHED,IS_JOB_FINISHED,TESTNAME,TASK_FLAG_UPDATED,JOB_FLAG_UPDATED,RESULT_JSON ) VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP( %s , 'YYYY-MM-DD HH24:MI:SS' ), TO_TIMESTAMP( %s , 'YYYY-MM-DD HH24:MI:SS' ), %s)"""
    #     new_cur.executemany(insert_query, insertData)
    # print ('data inserted (%s)' % (len(insertData)))
    #
    # if len(updateData) > 0:
    #     new_cur = connection.cursor()
    #     update_query = """UPDATE """ + tableName + """ SET PANDAID = %s, IS_TASK_FINISHED = %s ,IS_JOB_FINISHED = %s , JOB_FLAG_UPDATED = TO_TIMESTAMP( %s , 'YYYY-MM-DD HH24:MI:SS' ), RESULT_JSON = %s WHERE JEDITASKID = %s AND TESTNAME = %s """
    #     new_cur.executemany(update_query, updateData)
    # print ('data updated (%s rows updated)' % (len(updateData)))



    result = True
    data = {
        'result': result,
        'strt': starttime,
        'endt': datetime.now()
    }
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='text/html')

def getJobSubResults(request):
    valid, response = initRequest(request)


    guid = request.session['requestParams']['guid'] if 'guid' in request.session['requestParams'] else ''
    lfn = request.session['requestParams']['lfn'] if 'lfn' in request.session['requestParams'] else ''
    scope = request.session['requestParams']['scope'] if 'scope' in request.session['requestParams'] else ''
    pandaid = request.session['requestParams']['pandaid'] if 'pandaid' in request.session['requestParams'] else None
    jeditaskid = request.session['requestParams']['jeditaskid'] if 'jeditaskid' in request.session['requestParams'] else None
    data = getJobReport(guid, lfn, scope)
    results = getARTjobSubResults(data)
    # if len(results) > 0:
    #     saveJobSubResults(results,jeditaskid, pandaid)

    data = {
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'jobSubResults': results
    }
    response = render_to_response('artJobSubResults.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response


def saveJobSubResults(results,jeditaskid,pandaid):
    updateData = []
    updateData.append((json.dumps(results),jeditaskid,pandaid))
    tableName = 'ATLAS_PANDABIGMON.ART_RESULTS'
    new_cur = connection.cursor()
    update_query = """UPDATE """ + tableName + """ SET RESULT_JSON = %s WHERE JEDITASKID = %s AND PANDAID = %s """
    new_cur.executemany(update_query, updateData)
    print ('data updated (%s rows updated)' % (len(updateData)))
    return True


def gettflag(job):
    return 1 if job['taskstatus'] in ('done', 'finished', 'failed', 'aborted') else 0


def getjflag(job):
    return 1 if job['jobstatus'] in ('finished', 'failed', 'cancelled', 'closed') else 0
