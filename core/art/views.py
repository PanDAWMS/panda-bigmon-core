"""
    art.views

"""
import json
import re
from datetime import datetime, timedelta
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection, transaction
from core.art.modelsART import ARTTask, ARTTasks, ARTResults
from django.db.models.functions import Concat, Substr
from django.db.models import Value as V, Sum
from core.views import initRequest, extensibleURL, removeParam
from core.views import setCacheEntry, getCacheEntry, DateEncoder, endSelfMonitor
from core.art.jobSubResults import getJobReport

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
    response = render_to_response('artMainPage.html', data, content_type='text/html')
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

    artJobsNames = ['taskid','package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid', 'computingsite', 'guid', 'scope', 'lfn', 'taskstatus']
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
                try:
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = int(re.search('.([0-9]{6}).log.', job['lfn']).group(1))
                except:
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs'][job['origpandaid']]['tarindex'] = ''
                # if len(ntagslist) == 1 and job['guid'] is not None and job['lfn'] is not None and job['scope'] is not None:
                #     try:
                #         results = getJobReport(job['guid'], job['lfn'], job['scope'])
                #
                #         # protection of format change
                #         if 'result' in results and isinstance(results['result'], list):
                #             resultlist = []
                #             for r in results['result']:
                #                 resultlist.append({'name': '', 'result': r})
                #             results['result'] = resultlist
                #
                #         artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                #             'jobs'][job['origpandaid']]['testexitcode'] = results['exit_code']
                #         artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                #             'jobs'][job['origpandaid']]['testresult'] = results['result']
                #     except:
                #         artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                #             'jobs'][job['origpandaid']]['testexitcode'] = None
                #         artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                #             'jobs'][job['origpandaid']]['testresult'] = None
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

def updateaARTJobList(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')

    ### Getting full list of jobs
    cur = connection.cursor()
    cur.execute("SELECT taskid, ntag, pandaid, taskstatus, status as jobstatus FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))" % (query['ntag_from'], query['ntag_to'], query['strcondition']))
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid', 'ntag', 'pandaid', 'taskstatus', 'jobstatus']
    fulljoblist = [dict(zip(artJobsNames, row)) for row in jobs]
    ntagslist = list(sorted(set([x['ntag'] for x in fulljoblist])))

    ### Getting list of existed jobs
    extra = 'jeditaskid in ( '
    fulljoblistdict = {}
    for job in fulljoblist:
        if job['taskid'] not in fulljoblistdict.keys():
            fulljoblistdict[job['taskid']] = {}
            extra +=  str(job['taskid']) + ','
        fulljoblistdict[job['taskid']][job['pandaid']] = []
    if extra.endswith(','):
        extra = extra[:-1]
    extra += ' ) '

    existedjoblist = ARTResults.objects.extra(where=[extra]).values()

    executionData = []

    if len(existedjoblist) > 0:
        print ('to be filtered')


    else:
        print ('preparing data to insert into artresults table')


        for j in fulljoblist:
            tflag = 0
            if j['taskstatus'] in ('done', 'finished', 'failed', 'aborted'):
                tflag = 1
            jflag = 0
            if j['taskstatus'] in ('finished', 'failed', 'cancelled', 'closed'):
                jflag = 1
            if j['pandaid'] is not None and j['taskid'] is not None:
                executionData.append((j['taskid'], j['pandaid'], tflag, jflag))


    if len(executionData) > 0:
        tmpTableName = 'ATLAS_PANDABIGMON.ART_RESULTS'
        new_cur = connection.cursor()
        query = """INSERT INTO """ + tmpTableName + """(JEDITASKID,PANDAID,IS_TASK_FINISHED,IS_JOB_FINISHED) VALUES (%s, %s, %s, %s)"""
        new_cur.executemany(query, executionData)




    result = True
    data = {
        'result': result,
    }
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='text/html')

def getJobSubResults(request):
    valid, response = initRequest(request)


    guid = request.session['requestParams']['guid'] if 'guid' in request.session['requestParams'] else ''
    lfn = request.session['requestParams']['lfn'] if 'lfn' in request.session['requestParams'] else ''
    scope = request.session['requestParams']['scope'] if 'scope' in request.session['requestParams'] else ''
    results = getJobReport(guid, lfn, scope)

    # protection of format change
    if 'result' in results and isinstance(results['result'], list):
        resultlist = []
        for r in results['result']:
            if not isinstance(r, dict):
                resultlist.append({'name': '', 'result': r})
            else:
                resultlist.append({'name': r['name'] if 'name' in r else '', 'result': r['result'] if 'result' in r else r})
        results['result'] = resultlist


    data = {
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'jobSubResults': results
    }
    response = render_to_response('artJobSubResults.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response

