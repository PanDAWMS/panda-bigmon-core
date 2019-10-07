"""
    art.views
"""
import logging
import json
import re
import time
import multiprocessing
from datetime import datetime, timedelta
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
from django.template.defaulttags import register
from django.db.models.functions import Concat, Substr
from django.db.models import Value as V


from core.views import login_customrequired, initRequest, extensibleURL, removeParam
from core.views import DateEncoder, endSelfMonitor
from core.art.artMail import send_mail_art
from core.art.modelsART import ARTResults, ARTTests, ReportEmails, ARTResultsQueue
from core.art.jobSubResults import getJobReport, getARTjobSubResults, subresults_getter, save_subresults, lock_nqueuedjobs, delete_queuedjobs, clear_queue, get_final_result
from core.common.models import Filestable4, FilestableArch
from core.libs.cache import setCacheEntry, getCacheEntry
from core.pandajob.models import CombinedWaitActDefArch4, Jobsarchived

from core.art.utils import setupView

_logger = logging.getLogger('bigpandamon-error')

@register.filter(takes_context=True)
def remove_dot(value):
    return value.replace(".", "").replace('/','')

@register.filter(takes_context=True)
def get_time(value):
    return value[-5:]

artdateformat = '%Y-%m-%d'
humandateformat = '%d %b %Y'
cache_timeout = 15


@login_customrequired
def art(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # Here we try to get cached data
    data = getCacheEntry(request, "artMain")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('artMainPage.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    tquery = {}
    tquery['platform__endswith'] = 'opt'

    # limit results by N days
    N_DAYS_LIMIT = 90
    extrastr = " (TO_DATE(SUBSTR(NIGHTLY_TAG, 0, INSTR(NIGHTLY_TAG, 'T')-1), 'YYYY-MM-DD') > sysdate - {}) ".format(N_DAYS_LIMIT)

    packages = ARTTests.objects.filter(**tquery).extra(where=[extrastr]).values('package').distinct().order_by('package')
    branches = ARTTests.objects.filter(**tquery).extra(where=[extrastr]).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'project', V('/'), 'platform')).values('branch').distinct().order_by('-branch')
    ntags = ARTTests.objects.values('nightly_tag').annotate(nightly_tag_date=Substr('nightly_tag', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]

    data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'packages': [p['package'] for p in packages],
            'branches': [b['branch'] for b in branches],
            'ntags': [t['nightly_tag_date'] for t in ntags]
    }
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
        response = render_to_response('artMainPage.html', data, content_type='text/html')
    else:
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    setCacheEntry(request, "artMain", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    endSelfMonitor(request)
    return response


@login_customrequired
def artOverview(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)
    query = setupView(request, 'job')

    # Here we try to get cached data
    data = getCacheEntry(request, "artOverview")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if 'ntaglist' in data:
            if len(data['ntaglist']) > 0:
                ntags = []
                for ntag in data['ntaglist']:
                    try:
                        ntags.append(datetime.strptime(ntag, artdateformat))
                    except:
                        pass
                if len(ntags) > 1 and 'requestParams' in data:
                    data['requestParams']['ntag_from'] = min(ntags)
                    data['requestParams']['ntag_to'] = max(ntags)
                elif len(ntags) == 1:
                    data['requestParams']['ntag'] = ntags[0]
        response = render_to_response('artOverview.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    if datetime.strptime(query['ntag_from'], '%Y-%m-%d') < datetime.strptime('2018-03-20', '%Y-%m-%d'):
        query_raw = """SELECT package, branch, ntag, status, result FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])
    else:
        query_raw = """SELECT package, branch, ntag, status, result FROM table(ATLAS_PANDABIGMON.ARTTESTS_1('%s','%s','%s')) WHERE attemptmark = 0""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])

    cur = connection.cursor()
    cur.execute(query_raw)
    tasks_raw = cur.fetchall()
    cur.close()
    artJobs = ['package', 'branch','ntag', 'jobstatus', 'result']
    jobs = [dict(zip(artJobs, row)) for row in tasks_raw]
    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))

    statestocount = ['finished', 'failed', 'active', 'done']
    
    artpackagesdict = {}
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for j in jobs:
            if j['package'] not in artpackagesdict.keys():
                artpackagesdict[j['package']] = {}
                for n in ntagslist:
                    artpackagesdict[j['package']][n.strftime(artdateformat)] = {}
                    artpackagesdict[j['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    for state in statestocount:
                        artpackagesdict[j['package']][n.strftime(artdateformat)][state] = 0
    
            if j['ntag'].strftime(artdateformat) in artpackagesdict[j['package']]:
                finalresult, extraparams = get_final_result(j)
                artpackagesdict[j['package']][j['ntag'].strftime(artdateformat)][finalresult] +=1

    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for j in jobs:
            if j['branch'] not in artpackagesdict.keys():
                artpackagesdict[j['branch']] = {}
                for n in ntagslist:
                    artpackagesdict[j['branch']][n.strftime(artdateformat)] = {}
                    artpackagesdict[j['branch']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    for state in statestocount:
                        artpackagesdict[j['branch']][n.strftime(artdateformat)][state] = 0

            if j['ntag'].strftime(artdateformat) in artpackagesdict[j['branch']]:
                finalresult, extraparams = get_final_result(j)
                artpackagesdict[j['branch']][j['ntag'].strftime(artdateformat)][finalresult] += 1
        
    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        data = {
            'artpackages': artpackagesdict,
        }

        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        data = {
            'request': request,
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


@login_customrequired
def artTasks(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)
    query = setupView(request, 'job')

    # Here we try to get cached data
    data = getCacheEntry(request, "artTasks")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if 'ntaglist' in data:
            if len(data['ntaglist']) > 0:
                ntags = []
                for ntag in data['ntaglist']:
                    try:
                        ntags.append(datetime.strptime(ntag, artdateformat))
                    except:
                        pass
                if len(ntags) > 1 and 'requestParams' in data:
                    data['requestParams']['ntag_from'] = min(ntags)
                    data['requestParams']['ntag_to'] = max(ntags)
                elif len(ntags) == 1:
                    data['requestParams']['ntag'] = ntags[0]
        response = render_to_response('artTasks.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response

    cur = connection.cursor()
    if datetime.strptime(query['ntag_from'], '%Y-%m-%d') < datetime.strptime('2018-03-20', '%Y-%m-%d'):
        query_raw = """SELECT package, branch, ntag, nightly_tag, taskid, status, result FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])
    else:
        query_raw = """SELECT package, branch, ntag, nightly_tag, taskid, status, result FROM table(ATLAS_PANDABIGMON.ARTTESTS_1('%s','%s','%s')) WHERE attemptmark = 0""" % (query['ntag_from'], query['ntag_to'], query['strcondition'])

    cur.execute(query_raw)
    tasks_raw = cur.fetchall()
    cur.close()

    artJobs = ['package', 'branch', 'ntag', 'nightly_tag', 'task_id', 'jobstatus', 'result']
    jobs = [dict(zip(artJobs, row)) for row in tasks_raw]

    # tasks = ARTTasks.objects.filter(**query).values('package','branch','task_id', 'ntag', 'nfilesfinished', 'nfilesfailed')
    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))
    statestocount = ['finished', 'failed', 'active', 'done']
    arttasksdict = {}
    jeditaskids = {}
    if not 'view' in request.session['requestParams'] or ('view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for job in jobs:
            if job['package'] not in arttasksdict.keys():
                arttasksdict[job['package']] = {}
            if job['branch'] not in arttasksdict[job['package']].keys():
                arttasksdict[job['package']][job['branch']] = {}
                for n in ntagslist:
                    arttasksdict[job['package']][job['branch']][n.strftime(artdateformat)] = {}
                    arttasksdict[job['package']][job['branch']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
            if job['nightly_tag'] not in arttasksdict[job['package']][job['branch']][job['ntag'].strftime(artdateformat)]:
                arttasksdict[job['package']][job['branch']][job['ntag'].strftime(artdateformat)][job['nightly_tag']] = {}
                for state in statestocount:
                    arttasksdict[job['package']][job['branch']][job['ntag'].strftime(artdateformat)][job['nightly_tag']][state] = 0
            if job['nightly_tag'] in arttasksdict[job['package']][job['branch']][job['ntag'].strftime(artdateformat)]:
                finalresult, extraparams = get_final_result(job)
                arttasksdict[job['package']][job['branch']][job['ntag'].strftime(artdateformat)][job['nightly_tag']][finalresult] += 1

            if job['package'] not in jeditaskids.keys():
                jeditaskids[job['package']] = {}
            if job['branch'] not in jeditaskids[job['package']].keys():
                jeditaskids[job['package']][job['branch']] = []
            if job['task_id'] not in jeditaskids[job['package']][job['branch']]:
                jeditaskids[job['package']][job['branch']].append(job['task_id'])

    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for job in jobs:
            if job['branch'] not in arttasksdict.keys():
                arttasksdict[job['branch']] = {}
            if job['package'] not in arttasksdict[job['branch']].keys():
                arttasksdict[job['branch']][job['package']] = {}
                for n in ntagslist:
                    arttasksdict[job['branch']][job['package']][n.strftime(artdateformat)] = {}
                    arttasksdict[job['branch']][job['package']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
            if job['nightly_tag'] not in arttasksdict[job['branch']][job['package']][job['ntag'].strftime(artdateformat)]:
                arttasksdict[job['branch']][job['package']][job['ntag'].strftime(artdateformat)][job['nightly_tag']] = {}
                for state in statestocount:
                    arttasksdict[job['branch']][job['package']][job['ntag'].strftime(artdateformat)][job['nightly_tag']][state] = 0
            if job['nightly_tag'] in arttasksdict[job['branch']][job['package']][job['ntag'].strftime(artdateformat)]:
                finalresult, extraparams = get_final_result(job)
                arttasksdict[job['branch']][job['package']][job['ntag'].strftime(artdateformat)][job['nightly_tag']][finalresult] += 1

            if job['branch'] not in jeditaskids.keys():
                jeditaskids[job['branch']] = {}
            if job['package'] not in jeditaskids[job['branch']].keys():
                jeditaskids[job['branch']][job['package']] = []
            if job['task_id'] not in jeditaskids[job['branch']][job['package']]:
                jeditaskids[job['branch']][job['package']].append(job['task_id'])

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        data = {
            'arttasks': arttasksdict,
            'jeditaskids': jeditaskids,
        }

        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        data = {
            'request': request,
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


@login_customrequired
def artJobs(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # Here we try to get cached data
    data = getCacheEntry(request, "artJobs")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if 'ntaglist' in data:
            if len(data['ntaglist']) > 0:
                ntags = []
                for ntag in data['ntaglist']:
                    try:
                        ntags.append(datetime.strptime(ntag, artdateformat))
                    except:
                        pass
                if len(ntags) > 1 and 'requestParams' in data:
                    data['requestParams']['ntag_from'] = min(ntags)
                    data['requestParams']['ntag_to'] = max(ntags)
                elif len(ntags) == 1:
                    data['requestParams']['ntag'] = ntags[0]
        response = render_to_response('artJobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response


    query = setupView(request, 'job')

    cur = connection.cursor()
    if datetime.strptime(query['ntag_from'], '%Y-%m-%d') < datetime.strptime('2018-03-20', '%Y-%m-%d'):
        cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.ARTTESTS('%s','%s','%s'))" % (query['ntag_from'], query['ntag_to'], query['strcondition']))
    else:
        cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.ARTTESTS_1('%s','%s','%s'))" % (query['ntag_from'], query['ntag_to'], query['strcondition']))
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid','package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid', 'computingsite', 'endtime', 'starttime' , 'maxvmem', 'cpuconsumptiontime', 'guid', 'scope', 'lfn', 'taskstatus', 'taskmodificationtime', 'jobmodificationtime', 'result', 'gitlabid', 'outputcontainer', 'maxrss', 'attemptnr', 'maxattempt', 'parentid', 'attemptmark', 'inputfileid']
    jobs = [dict(zip(artJobsNames, row)) for row in jobs]

    # i=0
    # for job in jobs:
    #     i+=1
    #     print 'registering %i out of %i jobs' % (i, len(jobs))
    #     x = ArtTest(job['origpandaid'], job['testname'], job['branch'].split('/')[0], job['branch'].split('/')[2],job['branch'].split('/')[1], job['package'], job['nightly_tag'])
    #     if x.registerArtTest():
    #         print '%i job registered sucessfully out of %i' % (i, len(jobs))

    ntagslist=list(sorted(set([x['ntag'] for x in jobs])))
    jeditaskids = list(sorted(set([x['taskid'] for x in jobs])))

    testdirectories = {}
    outputcontainers = {}
    reportTo = {'mail': [], 'jira': {}}
    gitlabids = []
    gitlabids = list(sorted(set([x['gitlabid'] for x in jobs])))

    artjobsdict={}
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for job in jobs:
            if 'attemptmark' in job and job['attemptmark'] == 0:
                if job['package'] not in artjobsdict.keys():
                    artjobsdict[job['package']] = {}
                if job['branch'] not in artjobsdict[job['package']].keys():
                    artjobsdict[job['package']][job['branch']] = {}

                if job['package'] not in testdirectories.keys():
                    testdirectories[job['package']] = {}
                if job['branch'] not in testdirectories[job['package']].keys():
                    testdirectories[job['package']][job['branch']] = []

                if job['package'] not in outputcontainers.keys():
                    outputcontainers[job['package']] = {}
                if job['branch'] not in outputcontainers[job['package']].keys():
                    outputcontainers[job['package']][job['branch']] = []

                if job['testname'] not in artjobsdict[job['package']][job['branch']].keys():
                    artjobsdict[job['package']][job['branch']][job['testname']] = {}
                    for n in ntagslist:
                        artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)] = {}
                        artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                        artjobsdict[job['package']][job['branch']][job['testname']][n.strftime(artdateformat)]['jobs'] = []
                if job['ntag'].strftime(artdateformat) in artjobsdict[job['package']][job['branch']][job['testname']]:
                    jobdict = {}
                    jobdict['jobstatus'] = job['jobstatus']
                    jobdict['origpandaid'] = job['origpandaid']
                    jobdict['linktext'] = job['branch'] + '/' + job['nightly_tag'] + '/' + job['package'] + '/' + job['testname'][:-3]
                    jobdict['ntagtime'] = job['nightly_tag'][-5:]
                    jobdict['computingsite'] = job['computingsite']
                    jobdict['guid'] = job['guid']
                    jobdict['scope'] = job['scope']
                    jobdict['lfn'] = job['lfn']
                    jobdict['jeditaskid'] = job['taskid']
                    jobdict['maxvmem'] = round(job['maxvmem']*1.0/1000,1) if job['maxvmem'] is not None else '---'
                    jobdict['maxrss'] = round(job['maxrss']*1.0/1000,1) if job['maxrss'] is not None else '---'
                    jobdict['attemptnr'] = job['attemptnr']
                    jobdict['maxattempt'] = job['maxattempt']
                    jobdict['cpuconsumptiontime'] = job['cpuconsumptiontime'] if job['jobstatus'] in ('finished', 'failed') else '---'
                    jobdict['inputfileid'] = job['inputfileid']
                    if job['jobstatus'] in ('finished', 'failed'):
                        try:
                            jobdict['duration'] = job['endtime'] - job['starttime']
                        except:
                            jobdict['duration'] = '---'
                    else:
                        jobdict['duration'] = str(datetime.now() - job['starttime']).split('.')[0] if job['starttime'] is not None else "---"
                    try:
                        jobdict['tarindex'] = int(re.search('.([0-9]{6}).log.', job['lfn']).group(1))
                    except:
                        jobdict['tarindex'] = ''

                    finalresult, extraparams = get_final_result(job)

                    jobdict['finalresult'] = finalresult
                    jobdict.update(extraparams)

                    if not extraparams['testdirectory'] in testdirectories[job['package']][job['branch']] and extraparams['testdirectory'] is not None and isinstance(extraparams['testdirectory'], str):
                        testdirectories[job['package']][job['branch']].append(extraparams['testdirectory'])

                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'].append(jobdict)

                    if job['outputcontainer'] is not None and len(job['outputcontainer']) > 0:
                        for oc in job['outputcontainer'].split(','):
                            if not oc in outputcontainers[job['package']][job['branch']] and oc is not None and isinstance(oc, str):
                                outputcontainers[job['package']][job['branch']].append(oc)

                    if jobdict['reportjira'] is not None:
                        for jira, link in jobdict['reportjira'].items():
                            if jira not in reportTo['jira'].keys():
                                reportTo['jira'][jira] = link
                    if jobdict['reportmail'] is not None and jobdict['reportmail'] not in reportTo['mail']:
                            reportTo['mail'].append(jobdict['reportmail'])



    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for job in jobs:
            if 'attemptmark' in job and job['attemptmark'] == 0:
                if job['branch'] not in artjobsdict.keys():
                    artjobsdict[job['branch']] = {}
                if job['package'] not in artjobsdict[job['branch']].keys():
                    artjobsdict[job['branch']][job['package']] = {}

                if job['branch'] not in testdirectories.keys():
                    testdirectories[job['branch']] = {}
                if job['package'] not in testdirectories[job['branch']].keys():
                    testdirectories[job['branch']][job['package']] = []

                if job['testname'] not in artjobsdict[job['branch']][job['package']].keys():
                    artjobsdict[job['branch']][job['package']][job['testname']] = {}
                    for n in ntagslist:
                        artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)] = {}
                        artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                        artjobsdict[job['branch']][job['package']][job['testname']][n.strftime(artdateformat)]['jobs'] = []
                if job['ntag'].strftime(artdateformat) in artjobsdict[job['branch']][job['package']][job['testname']]:
                    jobdict = {}
                    jobdict['jobstatus'] = job['jobstatus']
                    jobdict['origpandaid'] = job['origpandaid']
                    jobdict['linktext'] = job['branch'] + '/' + job['nightly_tag'] + '/' + job['package'] + '/' + job['testname'][:-3]
                    jobdict['ntagtime'] = job['nightly_tag'][-5:]
                    jobdict['computingsite'] = job['computingsite']
                    jobdict['guid'] = job['guid']
                    jobdict['scope'] = job['scope']
                    jobdict['lfn'] = job['lfn']
                    jobdict['jeditaskid'] = job['taskid']
                    jobdict['maxvmem'] = round(job['maxvmem'] * 1.0 / 1000, 1) if job['maxvmem'] is not None else '---'
                    jobdict['maxrss'] = round(job['maxrss']*1.0/1000,1) if job['maxrss'] is not None else '---'
                    jobdict['attemptnr'] = job['attemptnr']
                    jobdict['maxattempt'] = job['maxattempt']
                    jobdict['cpuconsumptiontime'] = job['cpuconsumptiontime'] if job['jobstatus'] in ('finished', 'failed') else '---'
                    jobdict['inputfileid'] = job['inputfileid']
                    if job['jobstatus'] in ('finished', 'failed'):
                        jobdict['duration'] = job['endtime'] - job['starttime']
                    else:
                        jobdict['duration'] = str(datetime.now() - job['starttime']).split('.')[0] if job['starttime'] is not None else "---"
                    try:
                        jobdict['tarindex'] = int(re.search('.([0-9]{6}).log.', job['lfn']).group(1))
                    except:
                        jobdict['tarindex'] = ''

                    finalresult, extraparams = get_final_result(job)

                    jobdict['finalresult'] = finalresult
                    jobdict.update(extraparams)

                    if not extraparams['testdirectory'] in testdirectories[job['branch']][job['package']] and extraparams['testdirectory'] is not None and isinstance(extraparams['testdirectory'], str):
                        testdirectories[job['branch']][job['package']].append(extraparams['testdirectory'])

                    artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'].append(jobdict)

    # add links to logs of previous attempt if there is one
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for job in jobs:
            if 'attemptmark' in job and job['attemptmark'] == 1:
                jobindex = next((index for (index, d) in enumerate(
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs']) if d['inputfileid'] == job['inputfileid']), None)
                if jobindex is not None:
                    artjobsdict[job['package']][job['branch']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][jobindex]['linktopreviousattemptlogs'] = '?scope={}&guid={}&lfn={}&site={}'.format(job['scope'], job['guid'], job['lfn'], job['computingsite'])
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for job in jobs:
            if 'attemptmark' in job and job['attemptmark'] == 1:
                jobindex = next((index for (index, d) in enumerate(
                    artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)][
                        'jobs']) if d['inputfileid'] == job['inputfileid']), None)
                if jobindex is not None:
                    artjobsdict[job['branch']][job['package']][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][jobindex]['linktopreviousattemptlogs'] = '?scope={}&guid={}&lfn={}&site={}'.format(job['scope'], job['guid'], job['lfn'], job['computingsite'])

    # transform dict of tests to list of test and sort alphabetically
    artjobslist = {}
    for i, idict in artjobsdict.items():
        artjobslist[i] = {}
        for j, jdict in idict.items():
            artjobslist[i][j] = []
            for t, tdict in jdict.items():
                for ntg, jobs in tdict.items():
                    tdict[ntg]['jobs'] = sorted(jobs['jobs'], key=lambda x: (x['ntagtime'], x['origpandaid']), reverse=True)
                tdict['testname'] = t
                if len(testdirectories[i][j]) > 0:
                    if not 'view' in request.session['requestParams'] or (
                            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
                        tdict['gitlablink'] = 'https://gitlab.cern.ch/atlas/athena/blob/' + j.split('/')[0] + \
                                              testdirectories[i][j][0].split('src')[1] + '/' + t
                    else:
                        tdict['gitlablink'] = 'https://gitlab.cern.ch/atlas/athena/blob/' + i.split('/')[0] + \
                                              testdirectories[i][j][0].split('src')[1] + '/' + t
                artjobslist[i][j].append(tdict)
            artjobslist[i][j] = sorted(artjobslist[i][j], key=lambda x: x['testname'].lower())

    xurl = extensibleURL(request)
    noviewurl = removeParam(xurl, 'view', mode='extensible')

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        data = {
            'artjobs': artjobsdict,
        }

        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'artjobs': artjobslist,
            'testdirectories': testdirectories,
            'noviewurl': noviewurl,
            'ntaglist': [ntag.strftime(artdateformat) for ntag in ntagslist],
            'taskids' : jeditaskids,
            'gitlabids': gitlabids,
            'outputcontainers': outputcontainers,
            'reportto': reportTo,
        }
        setCacheEntry(request, "artJobs", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
        response = render_to_response('artJobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        endSelfMonitor(request)
        return response


def updateARTJobList(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    query = setupView(request, 'job')
    starttime = datetime.now()

    ### Adding to ART_RESULTS_QUEUE jobs with not loaded result json yet
    cur = connection.cursor()
    cur.autocommit = True
    cur.execute("""INSERT INTO atlas_pandabigmon.art_results_queue
                    (pandaid, IS_LOCKED, LOCK_TIME)
                    SELECT pandaid, 0, NULL  FROM table(ATLAS_PANDABIGMON.ARTTESTS_1('%s','%s','%s'))
                    WHERE pandaid is not NULL
                          and attemptmark = 0  
                          and result is NULL
                          and status in ('finished', 'failed')
                          and pandaid not in (select pandaid from atlas_pandabigmon.art_results_queue)"""
                % (query['ntag_from'], query['ntag_to'], query['strcondition']))

    nrows = 2

    is_queue_empty = False
    while not is_queue_empty:

        # Locking first N rows
        lock_time = lock_nqueuedjobs(cur, nrows)

        # Getting locked jobs from ART_RESULTS_QUEUE
        equery = {}
        equery['lock_time'] = lock_time
        equery['is_locked'] = 1
        ids = ARTResultsQueue.objects.filter(**equery).values()

        # Loading subresults from logs
        if len(ids) > 0:
            query = {}
            query['type'] = 'log'
            query['pandaid__in'] = [id['pandaid'] for id in ids]
            file_properties = []
            try:
                file_properties = Filestable4.objects.filter(**query).values('pandaid', 'guid', 'scope', 'lfn', 'destinationse', 'status')
            except:
                pass
            if len(file_properties) == 0:
                try:
                    file_properties.extend(FilestableArch.objects.filter(**query).values('pandaid', 'guid', 'scope', 'lfn', 'destinationse', 'status'))
                except:
                    pass

            # Forming url params to single str for request to filebrowser
            url_params = []
            if len(file_properties):
                url_params = [('&guid=' + filei['guid'] + '&lfn=' + filei['lfn'] + '&scope=' + filei['scope'] + '&pandaid=' + str(filei['pandaid'])) for filei in file_properties]

            # Loading subresults in parallel and collecting to list of dictionaries
            pool = multiprocessing.Pool(processes=nrows)
            try:
                sub_results = pool.map(subresults_getter, url_params)
            except:
                print('Exception was caught while mapping pool requests responses for next files {}'.format(str(url_params)))
                sub_results = []
            pool.close()
            pool.join()

            # list -> dict
            subResultsDict = {}
            for sr in sub_results:
                try:
                    pandaid = int(list(sr.keys())[0])
                except:
                    _logger.exception('Exception was caught while transforming pandaid from str to int.' + str(sr))
                    pandaid = -1
                if pandaid > 0:
                    subResultsDict[pandaid] = json.dumps(sr[pandaid])

            # insert subresults to special table
            save_subresults(subResultsDict)

            # deleting processed jobs from queue
            delete_queuedjobs(cur, lock_time)
        else:
            is_queue_empty = True

    # clear queue in case there are locked jobs of previously crashed requests
    clear_queue(cur)
    cur.close()

    data = {
        'strt': starttime,
        'endt': datetime.now()
    }
    endSelfMonitor(request)
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')



def getJobSubResults(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

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

@csrf_exempt
def registerARTTest(request):
    """
    API to register ART tests
    Example of curl command:
    curl -X POST -d "pandaid=XXX" -d "testname=test_XXXXX.sh" http://bigpanda.cern.ch/art/registerarttest/?json
    """
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)
    pandaid = -1
    testname = ''
    nightly_release_short = None
    platform = None
    project = None
    package = None
    nightly_tag = None

    ### Checking whether params were provided
    if 'requestParams' in request.session and 'pandaid' in request.session['requestParams'] and 'testname' in request.session['requestParams']:
            pandaid = request.session['requestParams']['pandaid']
            testname = request.session['requestParams']['testname']
    else:
        data = {'exit_code': -1, 'message': "There were not recieved any pandaid and testname"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    if 'nightly_release_short' in request.session['requestParams']:
        nightly_release_short = request.session['requestParams']['nightly_release_short']
    else:
        data = {'exit_code': -1, 'message': "No nightly_release_short provided"}
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'platform' in request.session['requestParams']:
        platform = request.session['requestParams']['platform']
    else:
        data = {'exit_code': -1, 'message': "No platform provided"}
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'project' in request.session['requestParams']:
        project = request.session['requestParams']['project']
    else:
        data = {'exit_code': -1, 'message': "No project provided"}
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'package' in request.session['requestParams']:
        package = request.session['requestParams']['package']
    else:
        data = {'exit_code': -1, 'message': "No package provided"}
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'nightly_tag' in request.session['requestParams']:
        nightly_tag = request.session['requestParams']['nightly_tag']
    else:
        data = {'exit_code': -1, 'message': "No nightly_tag provided"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Checking whether params is valid
    try:
        pandaid = int(pandaid)
    except:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    if pandaid < 0:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    if not str(testname).startswith('test_'):
        data = {'exit_code': -1, 'message': "Illegal test name was recieved"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Checking if provided pandaid exists in panda db
    query={}
    query['pandaid'] = pandaid
    values = 'pandaid', 'jeditaskid', 'jobname'
    jobs = []
    jobs.extend(CombinedWaitActDefArch4.objects.filter(**query).values(*values))
    if len(jobs) == 0:
        # check archived table
        jobs.extend(Jobsarchived.objects.filter(**query).values(*values))
    try:
       job = jobs[0]
    except:
        data = {'exit_code': -1, 'message': "Provided pandaid does not exists"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Checking whether provided pandaid is art job
    if 'jobname' in job and not job['jobname'].startswith('user.artprod'):
        data = {'exit_code': -1, 'message': "Provided pandaid is not art job"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Preparing params to register art job

    jeditaskid = job['jeditaskid']

    ### table columns:
    # pandaid
    # testname
    # nightly_release_short
    # platform
    # project
    # package
    # nightly_tag
    # jeditaskid

    ### Check whether the pandaid has been registered already
    if ARTTests.objects.filter(pandaid=pandaid).count() == 0:

        ## INSERT ROW
        try:
            insertRow = ARTTests.objects.create(pandaid=pandaid,
                                                jeditaskid=jeditaskid,
                                                testname=testname,
                                                nightly_release_short=nightly_release_short,
                                                nightly_tag=nightly_tag,
                                                project=project,
                                                platform=platform,
                                                package=package
                                                )
            insertRow.save()
            data = {'exit_code': 0, 'message': "Provided pandaid has been successfully registered"}
        except:
            data = {'exit_code': 0, 'message': "Provided pandaid is already registered (pk violated)"}
    else:
        data = {'exit_code': 0, 'message': "Provided pandaid is already registered"}


    endSelfMonitor(request)
    return HttpResponse(json.dumps(data), content_type='application/json')


def sendArtReport(request):
    """
    A view to send ART jobs status report by email
    :param request:
    :return: json
    """
    valid, response = initRequest(request)
    template = 'templated_email/artReportPackage.html'
    if 'ntag_from' not in request.session['requestParams']:
        valid = False
        errorMessage = 'No ntag provided!'
    elif 'ntag_to' not in request.session['requestParams']:
        valid = False
        errorMessage = 'No ntag provided!'
    elif request.session['requestParams']['ntag_from'] != (datetime.now() - timedelta(days=1)).strftime(artdateformat) or request.session['requestParams']['ntag_to'] != datetime.now().strftime(artdateformat):
        valid = False
        errorMessage = 'Provided ntag is not valid'
    if not valid:
        return HttpResponse(json.dumps({'errorMessage': errorMessage}), content_type='application/json')

    query = setupView(request, 'job')

    cur = connection.cursor()
    cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.ARTTESTS_1('%s','%s','%s')) WHERE attemptmark = 0" % (
        query['ntag_from'], query['ntag_to'], query['strcondition']))
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid', 'package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid',
                    'computingsite', 'endtime', 'starttime', 'maxvmem', 'cpuconsumptiontime', 'guid', 'scope', 'lfn',
                    'taskstatus', 'taskmodificationtime', 'jobmodificationtime', 'result']
    jobs = [dict(zip(artJobsNames, row)) for row in jobs]

    ### prepare data for report
    artjobsdictbranch = {}
    artjobsdictpackage = {}
    for job in jobs:
        nightly_tag_time = datetime.strptime(job['nightly_tag'].replace('T', ' '), '%Y-%m-%d %H%M')
        if nightly_tag_time > request.session['requestParams']['ntag_from'] + timedelta(hours=20):
            if job['branch'] not in artjobsdictbranch.keys():
                artjobsdictbranch[job['branch']] = {}
                artjobsdictbranch[job['branch']]['branch'] = job['branch']
                artjobsdictbranch[job['branch']]['ntag_full'] = job['nightly_tag']
                artjobsdictbranch[job['branch']]['ntag'] = job['ntag'].strftime(artdateformat)
                artjobsdictbranch[job['branch']]['packages'] = {}
            if job['package'] not in artjobsdictbranch[job['branch']]['packages'].keys():
                artjobsdictbranch[job['branch']]['packages'][job['package']] = {}
                artjobsdictbranch[job['branch']]['packages'][job['package']]['name'] = job['package']
                artjobsdictbranch[job['branch']]['packages'][job['package']]['ndone'] = 0
                artjobsdictbranch[job['branch']]['packages'][job['package']]['nfailed'] = 0
                artjobsdictbranch[job['branch']]['packages'][job['package']]['nfinished'] = 0
                artjobsdictbranch[job['branch']]['packages'][job['package']]['nactive'] = 0

            if job['package'] not in artjobsdictpackage.keys():
                artjobsdictpackage[job['package']] = {}
                artjobsdictpackage[job['package']]['branch'] = job['branch']
                artjobsdictpackage[job['package']]['ntag_full'] = job['nightly_tag']
                artjobsdictpackage[job['package']]['ntag'] = job['ntag'].strftime(artdateformat)
                artjobsdictpackage[job['package']]['link'] = 'https://bigpanda.cern.ch/art/tasks/?package={}&ntag={}'.format(
                    job['package'], job['ntag'].strftime(artdateformat))
                artjobsdictpackage[job['package']]['branches'] = {}
            if job['branch'] not in artjobsdictpackage[job['package']]['branches'].keys():
                artjobsdictpackage[job['package']]['branches'][job['branch']] = {}
                artjobsdictpackage[job['package']]['branches'][job['branch']]['name'] = job['branch']
                artjobsdictpackage[job['package']]['branches'][job['branch']]['ndone'] = 0
                artjobsdictpackage[job['package']]['branches'][job['branch']]['nfailed'] = 0
                artjobsdictpackage[job['package']]['branches'][job['branch']]['nfinished'] = 0
                artjobsdictpackage[job['package']]['branches'][job['branch']]['nactive'] = 0
                artjobsdictpackage[job['package']]['branches'][job['branch']][
                    'linktoeos'] = 'https://atlas-art-data.web.cern.ch/atlas-art-data/grid-output/{}/{}/{}/'.format(
                    job['branch'], job['nightly_tag'], job['package'])
            finalresult, extraparams = get_final_result(job)
            artjobsdictbranch[job['branch']]['packages'][job['package']]['n' + finalresult] += 1
            artjobsdictpackage[job['package']]['branches'][job['branch']]['n' + finalresult] += 1


    ### dict -> list & ordering
    for branchname, sumdict in artjobsdictbranch.items():
        sumdict['packages'] = sorted(artjobsdictbranch[branchname]['packages'].values(), key=lambda k: k['name'])
    for packagename, sumdict in artjobsdictpackage.items():
        sumdict['packages'] = sorted(artjobsdictpackage[packagename]['branches'].values(), key=lambda k: k['name'])

    summaryPerBranch = sorted(artjobsdictbranch.values(), key=lambda k: k['branch'], reverse=True)

    rquery = {}
    rquery['report'] = 'art'
    recipientslist = ReportEmails.objects.filter(**rquery).values()
    recipients = {}
    for recipient in recipientslist:
        if recipient['email'] is not None and len(recipient['email']) > 0:
            recipients[recipient['type']] = recipient['email']

    summaryPerRecipient = {}
    for package, email in recipients.items():
        if email not in summaryPerRecipient:
            summaryPerRecipient[email] = {}
        if package in artjobsdictpackage.keys():
            summaryPerRecipient[email][package] = artjobsdictpackage[package]
    subject = 'ART jobs status report'

    maxTries = 1
    for recipient, summary in summaryPerRecipient.items():
        isSent = False
        i = 0
        while not isSent:
            i +=1
            if i > 1:
                time.sleep(10)
            isSent = send_mail_art(template, subject, summary, recipient)
            if i >= maxTries:
                break


    endSelfMonitor(request)
    return HttpResponse(json.dumps({'isSent': isSent, 'nTries': i}), content_type='application/json')



