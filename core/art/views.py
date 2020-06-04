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
from core.views import DateEncoder
from core.art.artMail import send_mail_art
from core.art.modelsART import ARTTests, ReportEmails, ARTResultsQueue
from core.art.jobSubResults import subresults_getter, save_subresults, lock_nqueuedjobs, delete_queuedjobs, clear_queue, get_final_result
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
statestocount = ['finished', 'failed', 'active', 'succeeded']

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
        return response

    tquery = {}
    tquery['platform__endswith'] = 'opt'

    # limit results by N days
    N_DAYS_LIMIT = 90
    extrastr = " (TO_DATE(SUBSTR(NIGHTLY_TAG, 0, INSTR(NIGHTLY_TAG, 'T')-1), 'YYYY-MM-DD') > sysdate - {}) ".format(N_DAYS_LIMIT)

    packages = ARTTests.objects.filter(**tquery).extra(where=[extrastr]).values('package').distinct().order_by('package')
    branches = ARTTests.objects.filter(**tquery).extra(where=[extrastr]).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'project', V('/'), 'platform')).values('branch').distinct().order_by('-branch')
    ntags = ARTTests.objects.values('nightly_tag').annotate(nightly_tag_date=Substr('nightly_tag', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]

    # a workaround for a splitted DF into a lot of separate packages
    package_list = [p['package'] for p in packages]
    package_list.append('DerivationFramework*ART')

    data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'packages': sorted(package_list, key=str.lower),
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
    return response


@login_customrequired
def artOverview(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)
    
    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        art_aggr_order = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        art_aggr_order = ['branch', 'package']
    else:
        return HttpResponse(status=401)

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
        return response

    # process URL params to query params
    query = setupView(request, 'job')
    
    # quering data from dedicated SQL function
    query_raw = """
        SELECT package, branch, ntag, status, result 
        FROM table(ATLAS_PANDABIGMON.ARTTESTS_LIGHT('{}','{}','{}')) 
        WHERE attemptmark = 0
        """.format(query['ntag_from'], query['ntag_to'], query['strcondition'])
    cur = connection.cursor()
    cur.execute(query_raw)
    tasks_raw = cur.fetchall()
    cur.close()
    artJobs = ['package', 'branch','ntag', 'jobstatus', 'result']
    jobs = [dict(zip(artJobs, row)) for row in tasks_raw]
    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))
    
    artpackagesdict = {}
    for j in jobs:
        if j[art_aggr_order[0]] not in artpackagesdict.keys():
            artpackagesdict[j[art_aggr_order[0]]] = {}
            for n in ntagslist:
                artpackagesdict[j[art_aggr_order[0]]][n.strftime(artdateformat)] = {}
                artpackagesdict[j[art_aggr_order[0]]][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                for state in statestocount:
                    artpackagesdict[j[art_aggr_order[0]]][n.strftime(artdateformat)][state] = 0

        if j['ntag'].strftime(artdateformat) in artpackagesdict[j[art_aggr_order[0]]]:
            finalresult, extraparams = get_final_result(j)
            artpackagesdict[j[art_aggr_order[0]]][j['ntag'].strftime(artdateformat)][finalresult] += 1
        
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
        return response


@login_customrequired
def artTasks(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        art_aggr_order = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        art_aggr_order = ['branch', 'package']
    else:
        return HttpResponse(status=401)

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
        return response

    # process URL params to query params
    query = setupView(request, 'job')

    # quering data from dedicated SQL function
    cur = connection.cursor()
    query_raw = """
        SELECT package, branch, ntag, nightly_tag, taskid, status, result 
        FROM table(ATLAS_PANDABIGMON.ARTTESTS_LIGHT('{}','{}','{}')) 
        WHERE attemptmark = 0
        """.format(query['ntag_from'], query['ntag_to'], query['strcondition'])
    cur.execute(query_raw)
    tasks_raw = cur.fetchall()
    cur.close()

    artJobs = ['package', 'branch', 'ntag', 'nightly_tag', 'task_id', 'jobstatus', 'result']
    jobs = [dict(zip(artJobs, row)) for row in tasks_raw]

    # tasks = ARTTasks.objects.filter(**query).values('package','branch','task_id', 'ntag', 'nfilesfinished', 'nfilesfailed')
    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))
    arttasksdict = {}
    jeditaskids = {}
    for job in jobs:
        if job[art_aggr_order[0]] not in arttasksdict.keys():
            arttasksdict[job[art_aggr_order[0]]] = {}
        if job[art_aggr_order[1]] not in arttasksdict[job[art_aggr_order[0]]].keys():
            arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]] = {}
            for n in ntagslist:
                arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][n.strftime(artdateformat)] = {}
                arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
        if job['nightly_tag'] not in arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['ntag'].strftime(artdateformat)]:
            arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['ntag'].strftime(artdateformat)][job['nightly_tag']] = {}
            for state in statestocount:
                arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['ntag'].strftime(artdateformat)][job['nightly_tag']][state] = 0
        if job['nightly_tag'] in arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['ntag'].strftime(artdateformat)]:
            finalresult, extraparams = get_final_result(job)
            arttasksdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['ntag'].strftime(artdateformat)][job['nightly_tag']][finalresult] += 1

        if job[art_aggr_order[0]] not in jeditaskids.keys():
            jeditaskids[job[art_aggr_order[0]]] = {}
        if job[art_aggr_order[1]] not in jeditaskids[job[art_aggr_order[0]]].keys():
            jeditaskids[job[art_aggr_order[0]]][job[art_aggr_order[1]]] = []
        if job['task_id'] not in jeditaskids[job[art_aggr_order[0]]][job[art_aggr_order[1]]]:
            jeditaskids[job[art_aggr_order[0]]][job[art_aggr_order[1]]].append(job['task_id'])

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
        return response


@login_customrequired
def artJobs(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        art_aggr_order = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        art_aggr_order = ['branch', 'package']
    else:
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
        return response

    # process URL params to query params
    query = setupView(request, 'job')

    # querying data from dedicated SQL function
    cur = connection.cursor()
    query_raw = """
        SELECT 
            c.taskid, 
            c.package, 
            c.branch, 
            c.ntag, 
            c.nightly_tag, 
            c.testname, 
            c.status, 
            c.pandaid, 
            c.computingsite, 
            c.endtime,
            c.starttime,
            c.maxvmem, 
            c.cpuconsumptiontime, 
            c.guid, 
            c.scope, 
            c.lfn,
            c.taskstatus, 
            c.taskmodificationtime, 
            c.jobmodificationtime, 
            c.cpuconsumptionunit, 
            c.result, 
            c.gitlabid, 
            c.outputcontainer, 
            c.maxrss, 
            c.attemptnr, 
            c.maxattempt,  
            c.parentid, 
            c.attemptmark, 
            c.inputfileid,
            c.extrainfo 
        FROM table(ATLAS_PANDABIGMON.ARTTESTS('{}','{}','{}')) c
        """.format(query['ntag_from'], query['ntag_to'], query['strcondition'])
    cur.execute(query_raw)
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid','package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'origpandaid',
                    'computingsite', 'endtime', 'starttime' , 'maxvmem', 'cpuconsumptiontime', 'guid', 'scope', 'lfn',
                    'taskstatus', 'taskmodificationtime', 'jobmodificationtime', 'cpuconsumptionunit', 'result',
                    'gitlabid', 'outputcontainer', 'maxrss', 'attemptnr', 'maxattempt', 'parentid', 'attemptmark',
                    'inputfileid', 'extrainfo']
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
    gitlabids = list(sorted(set([x['gitlabid'] for x in jobs if 'gitlabid' in x and x['gitlabid'] is not None])))
    linktoplots = []

    artjobsdict={}

    for job in jobs:
        if 'attemptmark' in job and job['attemptmark'] == 0:
            if job[art_aggr_order[0]] not in artjobsdict.keys():
                artjobsdict[job[art_aggr_order[0]]] = {}
            if job[art_aggr_order[1]] not in artjobsdict[job[art_aggr_order[0]]].keys():
                artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]] = {}

            if job[art_aggr_order[0]] not in testdirectories.keys():
                testdirectories[job[art_aggr_order[0]]] = {}
            if job[art_aggr_order[1]] not in testdirectories[job[art_aggr_order[0]]].keys():
                testdirectories[job[art_aggr_order[0]]][job[art_aggr_order[1]]] = []

            if job[art_aggr_order[0]] not in outputcontainers.keys():
                outputcontainers[job[art_aggr_order[0]]] = {}
            if job[art_aggr_order[1]] not in outputcontainers[job[art_aggr_order[0]]].keys():
                outputcontainers[job[art_aggr_order[0]]][job[art_aggr_order[1]]] = []

            if job['testname'] not in artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]].keys():
                artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']] = {}
                for n in ntagslist:
                    artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][n.strftime(artdateformat)] = {}
                    artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][n.strftime(artdateformat)]['ntag_hf'] = n.strftime(humandateformat)
                    artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][n.strftime(artdateformat)]['jobs'] = []
            if job['ntag'].strftime(artdateformat) in artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']]:
                jobdict = {}
                jobdict['jobstatus'] = job['jobstatus']
                jobdict['origpandaid'] = job['origpandaid']
                jobdict['linktext'] = job[art_aggr_order[1]] + '/' + job['nightly_tag'] + '/' + job['package'] + '/' + job['testname'][:-3]
                jobdict['ntagtime'] = job['nightly_tag'][-5:]
                jobdict['computingsite'] = job['computingsite']
                jobdict['guid'] = job['guid']
                jobdict['scope'] = job['scope']
                jobdict['lfn'] = job['lfn']
                jobdict['jeditaskid'] = job['taskid']
                jobdict['maxrss'] = round(job['maxrss'] * 1.0 / 1000, 1) if job['maxrss'] is not None else '---'
                jobdict['attemptnr'] = job['attemptnr']
                jobdict['maxattempt'] = job['maxattempt']
                jobdict['cpuconsumptiontime'] = job['cpuconsumptiontime'] if job['jobstatus'] in ('finished', 'failed') else '---'
                jobdict['cpuconsumptionunit'] = job['cpuconsumptionunit'] if job['cpuconsumptionunit'] is not None else '---'
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
                # ATLINFR-3305
                if 'extrainfo' in job:
                    try:
                        job['extrainfo'] = json.loads(job['extrainfo'])
                    except:
                        job['extrainfo'] = {}
                if 'html' in job['extrainfo']:
                    jobdict['htmllink'] = jobdict['linktext'] + '/' + job['extrainfo']['html']

                finalresult, extraparams = get_final_result(job)

                jobdict['finalresult'] = finalresult
                jobdict.update(extraparams)

                if not extraparams['testdirectory'] in testdirectories[job[art_aggr_order[0]]][job[art_aggr_order[1]]] and extraparams[
                    'testdirectory'] is not None and isinstance(extraparams['testdirectory'], str):
                    testdirectories[job[art_aggr_order[0]]][job[art_aggr_order[1]]].append(extraparams['testdirectory'])

                artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'].append(jobdict)

                if job['outputcontainer'] is not None and len(job['outputcontainer']) > 0:
                    for oc in job['outputcontainer'].split(','):
                        if not oc in outputcontainers[job[art_aggr_order[0]]][job[art_aggr_order[1]]] and oc is not None and isinstance(oc, str):
                            outputcontainers[job[art_aggr_order[0]]][job[art_aggr_order[1]]].append(oc)

                if jobdict['reportjira'] is not None:
                    for jira, link in jobdict['reportjira'].items():
                        if jira not in reportTo['jira'].keys():
                            reportTo['jira'][jira] = link
                if jobdict['reportmail'] is not None and jobdict['reportmail'] not in reportTo['mail']:
                    reportTo['mail'].append(jobdict['reportmail'])

                if 'linktoplots' in extraparams and extraparams['linktoplots'] is not None:
                    linktoplots.append(extraparams['linktoplots'])

    # add links to logs of previous attempt if there is one
    for job in jobs:
        if 'attemptmark' in job and job['attemptmark'] == 1:
            jobindex = next((index for (index, d) in enumerate(
                artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][job['ntag'].strftime(artdateformat)]['jobs']) if d['inputfileid'] == job['inputfileid']), None)
            if jobindex is not None:
                artjobsdict[job[art_aggr_order[0]]][job[art_aggr_order[1]]][job['testname']][job['ntag'].strftime(artdateformat)]['jobs'][jobindex]['linktopreviousattemptlogs'] = '?scope={}&guid={}&lfn={}&site={}'.format(job['scope'], job['guid'], job['lfn'], job['computingsite'])

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
                if len(testdirectories[i][j]) > 0 and 'src' in testdirectories[i][j][0]:
                    if not 'view' in request.session['requestParams'] or (
                            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
                        tdict['gitlablink'] = 'https://gitlab.cern.ch/atlas/athena/blob/' + j.split('/')[0] + \
                                              testdirectories[i][j][0].split('src')[1] + '/' + t
                    else:
                        tdict['gitlablink'] = 'https://gitlab.cern.ch/atlas/athena/blob/' + i.split('/')[0] + \
                                              testdirectories[i][j][0].split('src')[1] + '/' + t
                artjobslist[i][j].append(tdict)
            artjobslist[i][j] = sorted(artjobslist[i][j], key=lambda x: x['testname'].lower())

    linktoplots = set(linktoplots)
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
            'linktoplots': linktoplots,
        }
        setCacheEntry(request, "artJobs", json.dumps(data, cls=DateEncoder), 60 * cache_timeout)
        response = render_to_response('artJobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


def updateARTJobList(request):
    """
    Loading sub-step results for tests from PanDA job log files managed by Rucio
    :param request: HTTP request
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    query = setupView(request, 'job')
    starttime = datetime.now()

    ### Adding to ART_RESULTS_QUEUE jobs with not loaded result json yet
    cur = connection.cursor()
    cur.autocommit = True
    cur.execute("""INSERT INTO atlas_pandabigmon.art_results_queue
                    (pandaid, IS_LOCKED, LOCK_TIME)
                    SELECT pandaid, 0, NULL  FROM table(ATLAS_PANDABIGMON.ARTTESTS_LIGHT('{}','{}','{}'))
                    WHERE pandaid is not NULL
                          and attemptmark = 0  
                          and result is NULL
                          and status in ('finished', 'failed')
                          and pandaid not in (select pandaid from atlas_pandabigmon.art_results_queue)
                """.format(query['ntag_from'], query['ntag_to'], query['strcondition']))

    # number of concurrent download requests to Rucio
    N_ROWS = 1

    is_queue_empty = False
    while not is_queue_empty:

        # Locking first N rows
        lock_time = lock_nqueuedjobs(cur, N_ROWS)

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
                file_properties = Filestable4.objects.filter(**query).values('pandaid', 'guid', 'scope', 'lfn', 'destinationse', 'status', 'fileid')
            except:
                pass
            if len(file_properties) == 0:
                try:
                    file_properties.extend(FilestableArch.objects.filter(**query).values('pandaid', 'guid', 'scope', 'lfn', 'destinationse', 'status', 'fileid'))
                except:
                    pass

            # Forming url params to single str for request to filebrowser
            url_params = []
            if len(file_properties):
                url_params = [('&guid=' + filei['guid'] + '&lfn=' + filei['lfn'] + '&scope=' + filei['scope'] + '&fileid=' + str(filei['fileid']) + '&pandaid=' + str(filei['pandaid'])) for filei in file_properties]

            # Loading subresults in parallel and collecting to list of dictionaries
            pool = multiprocessing.Pool(processes=N_ROWS)
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
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


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
    jeditaskid = -1
    testname = ''
    nightly_release_short = None
    platform = None
    project = None
    package = None
    nightly_tag = None
    extra_info = {}

    ### Checking whether params were provided
    if 'requestParams' in request.session and 'pandaid' in request.session['requestParams'] and 'testname' in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
        testname = request.session['requestParams']['testname']
    else:
        data = {'exit_code': -1, 'message': "There were not recieved any pandaid and testname"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')

    if 'nightly_release_short' in request.session['requestParams']:
        nightly_release_short = request.session['requestParams']['nightly_release_short']
    else:
        data = {'exit_code': -1, 'message': "No nightly_release_short provided"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'platform' in request.session['requestParams']:
        platform = request.session['requestParams']['platform']
    else:
        data = {'exit_code': -1, 'message': "No platform provided"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'project' in request.session['requestParams']:
        project = request.session['requestParams']['project']
    else:
        data = {'exit_code': -1, 'message': "No project provided"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'package' in request.session['requestParams']:
        package = request.session['requestParams']['package']
    else:
        data = {'exit_code': -1, 'message': "No package provided"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')
    if 'nightly_tag' in request.session['requestParams']:
        nightly_tag = request.session['requestParams']['nightly_tag']
    else:
        data = {'exit_code': -1, 'message': "No nightly_tag provided"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Processing extra params
    if 'html' in request.session['requestParams']:
        extra_info['html'] = request.session['requestParams']['html']

    ### Checking whether params is valid
    try:
        pandaid = int(pandaid)
    except:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')

    if pandaid < 0:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')

    if not str(testname).startswith('test_'):
        data = {'exit_code': -1, 'message': "Illegal test name was recieved"}
        _logger.error(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Checking if provided pandaid exists in panda db
    query={}
    query['pandaid'] = pandaid
    values = 'pandaid', 'jeditaskid', 'username'
    jobs = []
    jobs.extend(CombinedWaitActDefArch4.objects.filter(**query).values(*values))
    try:
       job = jobs[0]
    except:
        data = {'exit_code': -1, 'message': "Provided pandaid does not exists"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Checking whether provided pandaid is art job
    if 'username' in job and job['username'] != 'artprod':
        data = {'exit_code': -1, 'message': "Provided pandaid is not art job"}
        return HttpResponse(json.dumps(data), content_type='application/json')

    ### Preparing params to register art job

    if 'jeditaskid' in job:
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
    # extrainfo

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
                                                package=package,
                                                extrainfo=json.dumps(extra_info)
                                                )
            insertRow.save()
            data = {'exit_code': 0, 'message': "Provided pandaid has been successfully registered"}
            _logger.error(data['message'] + str(request.session['requestParams']))
        except:
            data = {'exit_code': 0, 'message': "Provided pandaid is already registered (pk violated)"}
            _logger.error(data['message'] + str(request.session['requestParams']))
    else:
        data = {'exit_code': 0, 'message': "Provided pandaid is already registered"}
        _logger.error(data['message'] + str(request.session['requestParams']))

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
    query_raw = """
        SELECT taskid, package, branch, ntag, nightly_tag, testname, status, result
        FROM table(ATLAS_PANDABIGMON.ARTTESTS_LIGHT('{}','{}','{}')) 
        WHERE attemptmark = 0
        """.format(query['ntag_from'], query['ntag_to'], query['strcondition'])
    cur.execute(query_raw)
    jobs = cur.fetchall()
    cur.close()

    artJobsNames = ['taskid', 'package', 'branch', 'ntag', 'nightly_tag', 'testname', 'jobstatus', 'result']
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
                for state in statestocount:
                    artjobsdictbranch[job['branch']]['packages'][job['package']]['n' + state] = 0

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
                for state in statestocount:
                    artjobsdictpackage[job['package']]['branches'][job['branch']]['n' + state] = 0
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
            i += 1
            if i > 1:
                time.sleep(10)
            isSent = send_mail_art(template, subject, summary, recipient)
            # put 10 seconds delay to bypass the message rate limit of smtp server
            time.sleep(10)
            if i >= maxTries:
                break

    return HttpResponse(json.dumps({'isSent': isSent, 'nTries': i}), content_type='application/json')



