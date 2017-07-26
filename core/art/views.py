"""
    art.views

"""
import json
from datetime import datetime, timedelta
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection, transaction
from core.common.models import ARTTask, ARTTasks
from django.db.models.functions import Concat, Substr
from django.db.models import Value as V, Sum
from core.views import initRequest, extensibleURL, removeParam
from core.views import setCacheEntry, getCacheEntry, DateEncoder, endSelfMonitor

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
    else:
        request.session['requestParams']['ntag'] = startdate



    querystr = ''
    if querytype == 'job':
        if 'package' in request.session['requestParams']:
            querystr += '(UPPER(PACKAGE) IN ( UPPER(\'\'' + request.session['requestParams']['package'] + '\'\'))) AND '
        if 'branch' in request.session['requestParams']:
            querystr += '(UPPER(NIGHTLY_RELEASE_SHORT || \'\'/\'\' || PLATFORM || \'\'/\'\' || PROJECT)  IN ( UPPER(\'\'' + request.session['requestParams']['branch'] + '\'\'))) AND '
        if querystr.endswith('AND '):
            querystr = querystr[:len(querystr)-4]
        else:
            querystr += '(1=1)'
        query['strcondition'] = querystr
        query['ntag_from'] = startdate.strftime(artdateformat)
        query['ntag_to'] = enddate.strftime(artdateformat)
    elif querytype == 'task':
        if 'package' in request.session['requestParams']:
            query['package'] = request.session['requestParams']['package']
        if 'branch' in request.session['requestParams']:
            query['branch'] = request.session['requestParams']['branch']
        query['ntag__range'] = [startdate.strftime(artdateformat), enddate.strftime(artdateformat)]



    return query


def art(request):
    valid, response = initRequest(request)
    tquery = {}
    packages = ARTTask.objects.filter(**tquery).values('package').distinct()
    branches = ARTTask.objects.filter(**tquery).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'platform', V('/'), 'project')).values('branch').distinct()
    ntags = ARTTask.objects.values('nightly_tag').annotate(nightly_tag_date=Substr('nightly_tag', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]

    data = {
        'packages':[p['package'] for p in packages],
        'branches':[b['branch'] for b in branches],
        'ntags':[t['nightly_tag_date'] for t in ntags]
    }
    response = render_to_response('artMainPage.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def artOverview(request):
    valid, response = initRequest(request)
    query = setupView(request, 'task')

    # Here we try to get cached data
    data = getCacheEntry(request, "artOverview")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artOverview.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    packages = ARTTasks.objects.filter(**query).values('package', 'ntag').annotate(nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
    ntagslist=list(sorted(set([x['ntag'] for x in packages])))
            
    artpackagesdict = {}
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        packages = ARTTasks.objects.filter(**query).values('package', 'ntag').annotate(
            nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
        for p in packages:
            if p['package'] not in artpackagesdict.keys():
                artpackagesdict[p['package']] = {}
                for n in ntagslist:
                    artpackagesdict[p['package']][n.strftime(artdateformat)] = {}
                    artpackagesdict[p['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
    
            if p['ntag'].strftime(artdateformat) in artpackagesdict[p['package']]:
                if 'finished' in artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)] and 'failed' in \
                        artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]:
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]['finished'] += p['nfilesfinished']
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]['failed'] += p['nfilesfailed']
                else:
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]['finished'] = p['nfilesfinished']
                    artpackagesdict[p['package']][p['ntag'].strftime(artdateformat)]['failed'] = p['nfilesfailed']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        packages = ARTTasks.objects.filter(**query).values('branch', 'ntag').annotate(
            nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
        for p in packages:
            if p['branch'] not in artpackagesdict.keys():
                artpackagesdict[p['branch']] = {}
                for n in ntagslist:
                    artpackagesdict[p['branch']][n.strftime(artdateformat)] = {}
                    artpackagesdict[p['branch']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)

            if p['ntag'].strftime(artdateformat) in artpackagesdict[p['branch']]:
                if 'finished' in artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)] and 'failed' in \
                        artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]:
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]['finished'] += p['nfilesfinished']
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]['failed'] += p['nfilesfailed']
                else:
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]['finished'] = p['nfilesfinished']
                    artpackagesdict[p['branch']][p['ntag'].strftime(artdateformat)]['failed'] = p['nfilesfailed']
        
    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    data = {
        'requestParams': request.session['requestParams'],
        'artpackages': artpackagesdict,
        'noviewurl': noviewurl
    }
    setCacheEntry(request, "artOverview", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    response = render_to_response('artOverview.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response


def artTasks(request):
    valid, response = initRequest(request)
    query = setupView(request, 'task')

    # Here we try to get cached data
    data = getCacheEntry(request, "artTasks")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artTasks.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    tasks = ARTTasks.objects.filter(**query).values('package','branch','task_id', 'ntag', 'nfilesfinished', 'nfilesfailed')
    ntagslist = list(sorted(set([x['ntag'] for x in tasks])))

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
            if task['ntag'].strftime(artdateformat) in arttasksdict[task['package']][task['branch']]:
                if len(arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]) > 1:
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['finished'] += task['nfilesfinished']
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['failed'] += task['nfilesfailed']
                else:
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['finished'] = task['nfilesfinished']
                    arttasksdict[task['package']][task['branch']][task['ntag'].strftime(artdateformat)]['failed'] = task['nfilesfailed']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for task in tasks:
            if task['branch'] not in arttasksdict.keys():
                arttasksdict[task['branch']] = {}
            if task['package'] not in arttasksdict[task['branch']].keys():
                arttasksdict[task['branch']][task['package']] = {}
                for n in ntagslist:
                    arttasksdict[task['branch']][task['package']][n.strftime(artdateformat)] = {}
                    arttasksdict[task['branch']][task['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
            if task['ntag'].strftime(artdateformat) in arttasksdict[task['branch']][task['package']]:
                if 'finished' in arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)] and 'failed' in arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]:
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['finished'] += task['nfilesfinished']
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['failed'] += task['nfilesfailed']
                else:
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['finished'] = task['nfilesfinished']
                    arttasksdict[task['branch']][task['package']][task['ntag'].strftime(artdateformat)]['failed'] = task['nfilesfailed']

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')
    data = {
        'requestParams': request.session['requestParams'],
        'arttasks' : arttasksdict,
        'noviewurl': noviewurl
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

    artJobsNames = ['taskid','package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid']
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
            if job['ntag'].strftime(artdateformat) in artjobsdict[job['package']][job['branch']][job['testname']]:
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobstatus'] = job['jobstatus']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['origpandaid'] = job['origpandaid']
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
            if job['ntag'].strftime(artdateformat) in artjobsdict[job['branch']][job['package']][job['testname']]:
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobstatus'] = job['jobstatus']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['origpandaid'] = job['origpandaid']

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    data = {
        'requestParams': request.session['requestParams'],
        'artjobs': artjobsdict,
        'noviewurl': noviewurl
    }
    setCacheEntry(request, "artJobs", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    response = render_to_response('artJobs.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response
