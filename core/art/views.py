"""
    art.views

"""
import logging, re, json, commands, os, copy
import sys, traceback
from datetime import datetime, timedelta
import time
import copy
import itertools, random
import string as strm
import math
from urllib import urlencode, unquote
from urlparse import urlparse, urlunparse, parse_qs
from django.utils.decorators import available_attrs

from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render_to_response, render
from django.template import RequestContext, loader
from django.template.loader import get_template
from django.conf import settings
from django.utils.cache import patch_cache_control, patch_response_headers
from django.core.cache import cache
from django.utils import encoding
from django.conf import settings as djangosettings
from django.db import connection, transaction
from core.common.models import ARTTask, ARTTest, ProductionTask, ARTTasks, ARTTests
from django.conf import settings as djangosettings
from django.db.models.functions import Concat, Substr
from django.db.models import CharField, Value as V, Sum
from time import gmtime, strftime
from core.settings.local import dbaccess
from core.settings.local import PRODSYS
from core.settings.local import ES
from django.template.defaulttags import register

from core.views import initRequest

artdateformat = '%Y-%m-%d'

def setupView(request, querytype='task'):
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

    query['ntag_from'] = startdate.strftime(artdateformat)
    query['ntag_to'] = enddate.strftime(artdateformat)



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



    return query


def art(request):
    valid, response = initRequest(request)
    tquery = {}
    packages = ARTTask.objects.filter(**tquery).values('package').distinct()
    branches = ARTTask.objects.filter(**tquery).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'platform', V('/'), 'project')).values('branch').distinct()
    ntags = ARTTask.objects.values('nightly_tag').annotate(nightly_tag_date=Substr('nightly_tag', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]
    # taskids = []
    # taskstr = '('
    # for task in arttasks:
    #     taskids.append(task['task_id'])
    #     taskstr += str(task['task_id']) + ','



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
    query = {}

    # sqlquerystr = """select
    #                       ta.package,
    #                       ta.branch,
    #                       ta.ntag,
    #                       sum(ds.nfilesfinished) as nfilesfinished,
    #                       sum(ds.nfilesfailed) as nfilesfailed,
    #                       sum(ds.nfilesonhold) as nfilesonhold,
    #                       sum(ds.nfilesused) as nfilesused,
    #                       sum(ds.nfilestobeused) as nfilestobeused
    #                       from (
    #                         (select TASK_ID,
    #                           (NIGHTLY_RELEASE_SHORT || '/' || PLATFORM || '/' || PROJECT) as branch , package, NIGHTLY_TAG,
    #                           TO_DATE(SUBSTR(NIGHTLY_TAG, 0, INSTR(NIGHTLY_TAG, 'T')-1), 'YYYY-MM-DD') as NTAG, ART_ID
    #                          from ATLAS_DEFT.T_ART) ta
    #                         left join
    #                         (select jeditaskid, nfilesfinished, nfilesfailed, nfilesonhold, nfilesused, nfilestobeused
    #                           from ATLAS_PANDA.JEDI_DATASETS where type in ('output') ) ds
    #                           on ta.TASK_ID = ds.jeditaskid
    #                         )
    #                         where ta.ntag > (sysdate - 7)
    #                         GROUP by ta.package, ta.branch, ntag
    #                         order by ntag DESC"""
    #
    # cur = connection.cursor()
    # cur.execute(sqlquerystr)
    # packages = cur.fetchall()
    # cur.close()
    #
    # artTaskNames = ['package', 'branch', 'ntag', 'nfilesfinished', 'nfilesfailed',
    #                 'nfilesonhold', 'nfilesused', 'nfilestobeused']
    # packages = [dict(zip(artTaskNames, row)) for row in packages]

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
                    artpackagesdict[p['package']][n] = {}
    
            if p['ntag'] in artpackagesdict[p['package']]:
                if len(artpackagesdict[p['package']][p['ntag']]) > 1:
                    artpackagesdict[p['package']][p['ntag']]['finished'] += p['nfilesfinished']
                    artpackagesdict[p['package']][p['ntag']]['failed'] += p['nfilesfailed']
                    # artpackagesdict[p['package']][p['ntag']]['running'] += p['nfilesonhold']
                    # artpackagesdict[p['package']][p['ntag']]['unrecoverable'] += 0
                else:
                    artpackagesdict[p['package']][p['ntag']]['finished'] = p['nfilesfinished']
                    artpackagesdict[p['package']][p['ntag']]['failed'] = p['nfilesfailed']
                    # artpackagesdict[p['package']][p['ntag']]['running'] = p['nfilesonhold']
                    # artpackagesdict[p['package']][p['ntag']]['unrecoverable'] = 0
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        packages = ARTTasks.objects.filter(**query).values('branch', 'ntag').annotate(
            nfilesfinished=Sum('nfilesfinished'), nfilesfailed=Sum('nfilesfailed'))
        for p in packages:
            if p['branch'] not in artpackagesdict.keys():
                artpackagesdict[p['branch']] = {}
                for n in ntagslist:
                    artpackagesdict[p['branch']][n] = {}

            if p['ntag'] in artpackagesdict[p['branch']]:
                if len(artpackagesdict[p['branch']][p['ntag']]) > 1:
                    artpackagesdict[p['branch']][p['ntag']]['finished'] += p['nfilesfinished']
                    artpackagesdict[p['branch']][p['ntag']]['failed'] += p['nfilesfailed']
                    # artpackagesdict[p['branch']][p['ntag']]['running'] += p['nfilesonhold']
                    # artpackagesdict[p['branch']][p['ntag']]['unrecoverable'] += 0
                else:
                    artpackagesdict[p['branch']][p['ntag']]['finished'] = p['nfilesfinished']
                    artpackagesdict[p['branch']][p['ntag']]['failed'] = p['nfilesfailed']
                    # artpackagesdict[p['branch']][p['ntag']]['running'] = p['nfilesonhold']
                    # artpackagesdict[p['branch']][p['ntag']]['unrecoverable'] = 0
        


    data = {
        'requestParams': request.session['requestParams'],
        'artpackages': artpackagesdict
    }
    response = render_to_response('artOverview.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def artTasks(request):
    valid, response = initRequest(request)
    
    
    sqlquerystr = """select 
                      ta.package,
                      ta.branch,
                      ta.task_id,
                      ta.ntag,
                      ta.nightly_tag,
                      ds.nfilesfinished,
                      ds.nfilesfailed,
                      ds.nfilesonhold,
                      ds.nfilesused,
                      ds.nfilestobeused
                      from (
                        (select TASK_ID,
                          (NIGHTLY_RELEASE_SHORT || '/' || PLATFORM || '/' || PROJECT) as branch , package, NIGHTLY_TAG,
                          TO_DATE(SUBSTR(NIGHTLY_TAG, 0, INSTR(NIGHTLY_TAG, 'T')-1), 'YYYY-MM-DD') as NTAG, ART_ID  
                         from ATLAS_DEFT.T_ART) ta
                        left join 
                        (select jeditaskid, nfilesfinished, nfilesfailed, nfilesonhold, nfilesused, nfilestobeused
                          from ATLAS_PANDA.JEDI_DATASETS where type in ('output') ) ds
                          on ta.TASK_ID = ds.jeditaskid
                        ) 
                        where ta.ntag > (sysdate - 7) 
                        order by ntag DESC"""

    cur = connection.cursor()
    cur.execute(sqlquerystr)
    tasks = cur.fetchall()
    cur.close()

    artTaskNames = ['package', 'branch', 'task_id', 'ntag', 'nightly_tag', 'nfilesfinished', 'nfilesfailed', 'nfilesonhold', 'nfilesused', 'nfilestobeused']
    tasks = [dict(zip(artTaskNames, row)) for row in tasks]
    ntagslist = list(sorted(set([x['ntag'] for x in tasks])))

    arttasksdict = {}

    if not 'view' in request.session['requestParams'] or ('view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        for task in tasks:
            if task['package'] not in arttasksdict.keys():
                arttasksdict[task['package']] = {}
            if task['branch'] not in arttasksdict[task['package']].keys():
                arttasksdict[task['package']][task['branch']] = {}
                for n in ntagslist:
                    arttasksdict[task['package']][task['branch']][n] = {}
            if task['ntag'] in arttasksdict[task['package']][task['branch']]:
                if len(arttasksdict[task['package']][task['branch']][task['ntag']]) > 1:
                    arttasksdict[task['package']][task['branch']][task['ntag']]['finished'] += task['nfilesfinished']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['failed'] += task['nfilesfailed']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['running'] += task['nfilesonhold']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['unrecoverable'] += 0
                else:
                    arttasksdict[task['package']][task['branch']][task['ntag']]['finished'] = task['nfilesfinished']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['failed'] = task['nfilesfailed']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['running'] = task['nfilesonhold']
                    arttasksdict[task['package']][task['branch']][task['ntag']]['unrecoverable'] = 0
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for task in tasks:
            if task['branch'] not in arttasksdict.keys():
                arttasksdict[task['branch']] = {}
            if task['package'] not in arttasksdict[task['branch']].keys():
                arttasksdict[task['branch']][task['package']] = {}
                for n in ntagslist:
                    arttasksdict[task['branch']][task['package']][n] = {}
            if task['ntag'] in arttasksdict[task['branch']][task['package']]:
                if len(arttasksdict[task['branch']][task['package']][task['ntag']]) > 1:
                    arttasksdict[task['branch']][task['package']][task['ntag']]['finished'] += task['nfilesfinished']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['failed'] += task['nfilesfailed']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['running'] += task['nfilesonhold']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['unrecoverable'] += 0
                else:
                    arttasksdict[task['branch']][task['package']][task['ntag']]['finished'] = task['nfilesfinished']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['failed'] = task['nfilesfailed']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['running'] = task['nfilesonhold']
                    arttasksdict[task['branch']][task['package']][task['ntag']]['unrecoverable'] = 0
        
    data = {
        'arttasks' : arttasksdict
    }


    response = render_to_response('artTasks.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def artJobs(request):
    valid, response = initRequest(request)
    query = setupView(request, 'job')

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
                    artjobsdict[job['package']][job['branch']][job['testname']][n] = {}
            if job['ntag'] in artjobsdict[job['package']][job['branch']][job['testname']]:
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag']] = {}
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag']]['jobstatus'] = job['jobstatus']
                artjobsdict[job['package']][job['branch']][job['testname']][job['ntag']]['origpandaid'] = job['origpandaid']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        for job in jobs:
            if job['branch'] not in artjobsdict.keys():
                artjobsdict[job['branch']] = {}
            if job['package'] not in artjobsdict[job['branch']].keys():
                artjobsdict[job['branch']][job['package']] = {}
            if job['testname'] not in artjobsdict[job['branch']][job['package']].keys():
                artjobsdict[job['branch']][job['package']][job['testname']] = {}
                for n in ntagslist:
                    artjobsdict[job['branch']][job['package']][job['testname']][n] = {}
            if job['ntag'] in artjobsdict[job['branch']][job['package']][job['testname']]:
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag']] = {}
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag']]['jobstatus'] = job['jobstatus']
                artjobsdict[job['branch']][job['package']][job['testname']][job['ntag']]['origpandaid'] = job['origpandaid']



    data = {
        'artjobs': artjobsdict
    }
    response = render_to_response('artJobs.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
