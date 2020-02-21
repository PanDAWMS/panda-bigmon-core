import logging, re, subprocess, os
import sys, traceback
from datetime import datetime, timedelta
import time
import json
import copy
import itertools, random
import numpy as np
from io import BytesIO

import math

from core.pandajob.SQLLookups import CastDate
from django.db.models import DateTimeField


from urllib.parse import urlencode, urlparse, urlunparse, parse_qs
from django.utils.decorators import available_attrs
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response, redirect
from django.template import RequestContext
from django.db.models import Count, Sum, F, Value, FloatField
from django.views.decorators.csrf import csrf_exempt
import django.utils.cache as ucache
from functools import wraps

from django.utils import timezone
from django.utils.cache import patch_response_headers
from django.db.models import Q
from django.core.cache import cache
from django.utils import encoding
from django.conf import settings as djangosettings
from django.db import connection

from core.common.utils import getPrefix, getContextVariables, QuerySetChain
from core.settings import defaultDatetimeFormat
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived, \
    GetRWWithPrioJedi3DAYS, RemainedEventsPerCloud3dayswind, JobsWorldViewTaskType, CombinedWaitActDefArch4
from core.schedresource.models import Schedconfig, SchedconfigJson
from core.common.models import Filestable4
from core.common.models import Datasets
from core.common.models import Sitedata
from core.common.models import FilestableArch
from core.common.models import Users
from core.common.models import Jobparamstable
from core.common.models import JobsStatuslog
from core.common.models import Logstable
from core.common.models import Jobsdebug
from core.common.models import Cloudconfig
from core.common.models import Incidents
from core.common.models import Pandalog
from core.common.models import JediJobRetryHistory
from core.common.models import JediTasks
from core.common.models import JediTasksOrdered
from core.common.models import GetEventsForTask
from core.common.models import JediTaskparams
from core.common.models import JediEvents
from core.common.models import JediDatasets
from core.common.models import JediDatasetContents
from core.common.models import JediWorkQueue
from core.common.models import BPUser, Visits, BPUserSettings, AllRequests
from core.compare.modelsCompare import ObjectsComparison
from core.art.modelsART import ARTTests
from core.filebrowser.ruciowrapper import ruciowrapper

from core.settings.local import dbaccess
from core.settings.local import PRODSYS
from core.settings.local import ES
from core.settings.local import GRAFANA


from core.TaskProgressPlot import TaskProgressPlot
from core.ErrorCodes import ErrorCodes
import hashlib

import core.Customrenderer as Customrenderer
import collections, pickle

from threading import Thread,Lock
import base64
import urllib3
from django.views.decorators.cache import never_cache
from core import chainsql

errorFields = []
errorCodes = {}
errorStages = {}

from django.template.defaulttags import register

#from django import template
#register = template.Library()

from core.reports import MC16aCPReport, ObsoletedTasksReport, TitanProgressReport
from decimal import *

from django.contrib.auth import logout as auth_logout
from core.auth.utils import grant_rights, deny_rights

from core.libs import dropalgorithm
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table
from core.libs.cache import deleteCacheTestData, getCacheEntry, setCacheEntry
from core.libs.exlib import insert_to_temp_table, dictfetchall, is_timestamp, parse_datetime, get_job_walltime, is_job_active, get_tmp_table_name
from core.libs.task import job_summary_for_task, event_summary_for_task, input_summary_for_task, \
    job_summary_for_task_light, get_top_memory_consumers, get_harverster_workers_for_task
from core.libs.task import get_job_state_summary_for_tasklist
from core.libs.bpuser import get_relevant_links
from django.template.context_processors import csrf

@register.filter(takes_context=True)
def get_count(dict, key):
    return dict[key]['count']

@register.filter(takes_context=True)
def get_tk(dict, key):
    return dict[key]['tk']


@register.filter(takes_context=True)
def get_item(dictionary, key):
    return dictionary.get(key)

@register.simple_tag(takes_context=True)
def get_renderedrow(context, **kwargs):
    if kwargs['type']=="world_nucleussummary":
        kwargs['statelist'] = statelist
        return Customrenderer.world_nucleussummary(context, kwargs)

    if kwargs['type']=="world_computingsitesummary":
        kwargs['statelist'] = statelist
        return Customrenderer.world_computingsitesummary(context, kwargs)

    if kwargs['type']=="region_sitesummary":
        kwargs['statelist'] = statelist
        return Customrenderer.region_sitesummary(context, kwargs)


inilock = Lock()
DateTimeField.register_lookup(CastDate)

try:
    hostname = subprocess.getoutput('hostname')
    if hostname.find('.') > 0: hostname = hostname[:hostname.find('.')]
except:
    hostname = ''

callCount = 0
homeCloud = {}
objectStores = {}
objectStoresNames = {}
objectStoresSites = {}


pandaSites = {}
cloudList = ['CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'ND', 'NL', 'RU', 'TW', 'UK', 'US']

statelist = ['defined', 'waiting', 'pending', 'assigned', 'throttled',
             'activated', 'sent', 'starting', 'running', 'holding',
             'transferring', 'finished', 'failed', 'cancelled', 'merging', 'closed']
sitestatelist = ['defined', 'waiting', 'assigned', 'throttled', 'activated', 'sent', 'starting', 'running', 'holding',
                 'merging', 'transferring', 'finished', 'failed', 'cancelled', 'closed']
eventservicestatelist = ['ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed', 'fatal','merged', 'corrupted']
taskstatelist = ['registered', 'defined', 'assigning', 'ready', 'pending', 'scouting', 'scouted', 'running', 'prepared',
                 'done', 'failed', 'finished', 'aborting', 'aborted', 'finishing', 'topreprocess', 'preprocessing',
                 'tobroken', 'broken', 'toretry', 'toincexec', 'rerefine']
taskstatelist_short = ['reg', 'def', 'assgn', 'rdy', 'pend', 'scout', 'sctd', 'run', 'prep', 'done', 'fail', 'finish',
                       'abrtg', 'abrtd', 'finishg', 'toprep', 'preprc', 'tobrok', 'broken', 'retry', 'incexe', 'refine']

taskstatedict = []
for i in range(0, len(taskstatelist)):
    tsdict = {'state': taskstatelist[i], 'short': taskstatelist_short[i]}
    taskstatedict.append(tsdict)

errorcodelist = [
    {'name': 'brokerage', 'error': 'brokerageerrorcode', 'diag': 'brokerageerrordiag'},
    {'name': 'ddm', 'error': 'ddmerrorcode', 'diag': 'ddmerrordiag'},
    {'name': 'exe', 'error': 'exeerrorcode', 'diag': 'exeerrordiag'},
    {'name': 'jobdispatcher', 'error': 'jobdispatchererrorcode', 'diag': 'jobdispatchererrordiag'},
    {'name': 'pilot', 'error': 'piloterrorcode', 'diag': 'piloterrordiag'},
    {'name': 'sup', 'error': 'superrorcode', 'diag': 'superrordiag'},
    {'name': 'taskbuffer', 'error': 'taskbuffererrorcode', 'diag': 'taskbuffererrordiag'},
    {'name': 'transformation', 'error': 'transexitcode', 'diag': None},
]

_logger = logging.getLogger('bigpandamon')


notcachedRemoteAddress = ['188.184.185.129', '188.184.116.46']


LAST_N_HOURS_MAX = 0
# JOB_LIMIT = 0
# TFIRST = timezone.now()
# TLAST = timezone.now() - timedelta(hours=2400)
PLOW = 1000000
PHIGH = -1000000


standard_fields = ['processingtype', 'computingsite', 'jobstatus', 'prodsourcelabel', 'produsername', 'jeditaskid',
                   'workinggroup', 'transformation', 'cloud', 'homepackage', 'inputfileproject', 'inputfiletype',
                   'attemptnr', 'specialhandling', 'priorityrange', 'reqid', 'minramcount', 'eventservice',
                   'jobsubstatus', 'nucleus','gshare', 'resourcetype']
standard_sitefields = ['region', 'gocname', 'nickname', 'status', 'tier', 'comment_field', 'cloud', 'allowdirectaccess',
                       'allowfax', 'copytool', 'faxredirector', 'retry', 'timefloor']
standard_taskfields = ['workqueue_id', 'tasktype', 'superstatus', 'status', 'corecount', 'taskpriority', 'username', 'transuses',
                       'transpath', 'workinggroup', 'processingtype', 'cloud', 'campaign', 'project', 'stream', 'tag',
                       'reqid', 'ramcount', 'nucleus', 'eventservice', 'gshare']
standard_errorfields = ['cloud', 'computingsite', 'eventservice', 'produsername', 'jeditaskid', 'jobstatus',
                        'processingtype', 'prodsourcelabel', 'specialhandling','taskid' ,'transformation', 'workinggroup','reqid']

VOLIST = ['atlas', 'bigpanda', 'htcondor', 'core', 'aipanda']
VONAME = {'atlas': 'ATLAS', 'bigpanda': 'BigPanDA', 'htcondor': 'HTCondor', 'core': 'LSST', '': ''}
VOMODE = ' '


def login_customrequired(function):
    def wrap(request, *args, **kwargs):

        #we check here if it is a crawler:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for and x_forwarded_for in notcachedRemoteAddress:
            return function(request, *args, **kwargs)

        if request.user.is_authenticated or (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or ('json' in request.GET):
            return function(request, *args, **kwargs)
        else:
            # if '/user/' in request.path:
            #     return HttpResponseRedirect('/login/?next=' + request.get_full_path())
            # else:
            # return function(request, *args, **kwargs)
            return HttpResponseRedirect('/login/?next='+request.get_full_path())
    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


@login_customrequired
def grantRights(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'type' in request.session['requestParams']:
        rtype = request.session['requestParams']['type']
        grant_rights(request, rtype)

    return HttpResponse(status=204)


@login_customrequired
def denyRights(request):
    valid, response = initRequest(request)
    if not valid: return response

    if 'type' in request.session['requestParams']:
        rtype = request.session['requestParams']['type']
        deny_rights(request, rtype)

    return HttpResponse(status=204)


def datetime_handler(x):
    import datetime
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


def jobSuppression(request):

    extra = '(1=1)'

    if not 'notsuppress' in request.session['requestParams']:
        suppressruntime = 10
        if 'suppressruntime' in request.session['requestParams']:
            try:
                suppressruntime = int(request.session['requestParams']['suppressruntime'])
            except:
                pass
        extra = '( not( (JOBDISPATCHERERRORCODE=100 OR ' \
                'PILOTERRORCODE in (1200, 1201, 1202, 1203, 1204, 1206, 1207) ) and ((ENDTIME-STARTTIME)*24*60 < ' + str(
        suppressruntime) + ')))'
    return extra


def getObjectStoresNames():
    global objectStoresNames
    url = "http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&preset=dict&json_pretty=1&type[]=OS_ES"
    http = urllib3.PoolManager()
    data = {}
    try:
        r = http.request('GET', url)
        data = json.loads(r.data.decode('utf-8'))
    except Exception as exc:
        print (exc)
        return

    for OSname, OSdescr in data.items():
        if "resource" in OSdescr and "bucket_id" in OSdescr["resource"]:
            objectStoresNames[OSdescr["resource"]["bucket_id"]] = OSname
        objectStoresNames[OSdescr["id"]] = OSname
        objectStoresSites[OSname] = OSdescr["site"]




def escapeInput(strToEscape):
    charsToEscape = '$%^&()[]{};<>?\`~+%\'\"'
    charsToReplace = '_' * len(charsToEscape)
    tbl = str.maketrans(charsToEscape, charsToReplace)
    strToEscape = encoding.smart_str(strToEscape, encoding='ascii', errors='ignore')
    strToEscape = strToEscape.translate(tbl)
    return strToEscape


def setupSiteInfo(request):
    requestParams = {}
    if not 'requestParams' in request.session:
        request.session['requestParams'] = requestParams
    global homeCloud, objectStores, pandaSites, callCount
    callCount += 1
    if len(homeCloud) > 0 and callCount % 100 != 1 and 'refresh' not in request.session['requestParams']: return
    sflist = ('siteid', 'site', 'status', 'cloud', 'tier', 'comment_field', 'objectstore', 'catchall', 'corepower')
    sites = Schedconfig.objects.filter().exclude(cloud='CMS').values(*sflist)
    for site in sites:
        pandaSites[site['siteid']] = {}
        for f in ('siteid', 'status', 'tier', 'site', 'comment_field', 'cloud', 'corepower'):
            pandaSites[site['siteid']][f] = site[f]
        homeCloud[site['siteid']] = site['cloud']
        if (site['catchall'] != None) and (
                        site['catchall'].find('log_to_objectstore') >= 0 or site['objectstore'] != ''):
            # print 'object store site', site['siteid'], site['catchall'], site['objectstore']
            try:
                fpath = getFilePathForObjectStore(site['objectstore'], filetype="logs")
                #### dirty hack
                fpath = fpath.replace('root://atlas-objectstore.cern.ch/atlas/logs',
                                      'https://atlas-objectstore.cern.ch:1094/atlas/logs')
                if fpath != "" and fpath.startswith('http'): objectStores[site['siteid']] = fpath
            except:
                pass


def initRequest(request, callselfmon = True):
    global VOMODE, ENV, hostname
    ENV = {}
    VOMODE = ''
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
        # VOMODE = 'devtest'
    request.session['IS_TESTER'] = False

    if VOMODE == 'atlas':
        if "MELLON_SAML_RESPONSE" in request.META and base64.b64decode(request.META['MELLON_SAML_RESPONSE']):
            if "ADFS_FULLNAME" in request.META:
                request.session['ADFS_FULLNAME'] = request.META['ADFS_FULLNAME']
            if "ADFS_EMAIL" in request.META:
                request.session['ADFS_EMAIL'] = request.META['ADFS_EMAIL']
            if "ADFS_FIRSTNAME" in request.META:
                request.session['ADFS_FIRSTNAME'] = request.META['ADFS_FIRSTNAME']
            if "ADFS_LASTNAME" in request.META:
                request.session['ADFS_LASTNAME'] = request.META['ADFS_LASTNAME']
            if "ADFS_LOGIN" in request.META:
                request.session['ADFS_LOGIN'] = request.META['ADFS_LOGIN']
                user = None
                try:
                    user = BPUser.objects.get(username=request.session['ADFS_LOGIN'])
                    request.session['IS_TESTER'] = user.is_tester
                    request.session['USER_ID'] = user.id
                except BPUser.DoesNotExist:
                    user = BPUser.objects.create_user(username=request.session['ADFS_LOGIN'], email=request.session['ADFS_EMAIL'], first_name=request.session['ADFS_FIRSTNAME'], last_name=request.session['ADFS_LASTNAME'])
                    user.set_unusable_password()
                    user.save()

    # if VOMODE == 'devtest':
    #     request.session['ADFS_FULLNAME'] = ''
    #     request.session['ADFS_EMAIL'] = ''
    #     request.session['ADFS_FIRSTNAME'] = ''
    #     request.session['ADFS_LASTNAME'] = ''
    #     request.session['ADFS_LOGIN'] = ''
    #     # user = None
    #     user = BPUser.objects.get(username=request.session['ADFS_LOGIN'])
    #     request.session['IS_TESTER'] = user.is_tester
    #     request.user = user

    # print("IP Address for debug-toolbar: " + request.META['REMOTE_ADDR'])


    viewParams = {}
    # if not 'viewParams' in request.session:
    request.session['viewParams'] = viewParams

    url = request.get_full_path()
    u = urlparse(url)
    query = parse_qs(u.query)
    query.pop('timestamp', None)
    try:
        u = u._replace(query=urlencode(query, True))
    except UnicodeEncodeError:
        data = {
            'errormessage': 'Error appeared while encoding URL!'
        }
        return False, render_to_response('errorPage.html', data, content_type='text/html')
    request.session['notimestampurl'] = urlunparse(u) + ('&' if len(query) > 0 else '?')

    request.session['secureurl'] = 'https://bigpanda.cern.ch' + url

    #if 'USER' in os.environ and os.environ['USER'] != 'apache':
    #    request.session['debug'] = True
    if 'debug' in request.GET and request.GET['debug'] == 'insider':
        request.session['debug'] = True
        djangosettings.DEBUG = True
    else:
        request.session['debug'] = False
        djangosettings.DEBUG = False

    if len(hostname) > 0: request.session['hostname'] = hostname

    ##self monitor
    if callselfmon:
        initSelfMonitor(request)

    ## Set default page lifetime in the http header, for the use of the front end cache
    request.session['max_age_minutes'] = 10

    ## Is it an https connection with a legit cert presented by the user?
    if 'SSL_CLIENT_S_DN' in request.META or 'HTTP_X_SSL_CLIENT_S_DN' in request.META:
        if 'SSL_CLIENT_S_DN' in request.META:
            request.session['userdn'] = request.META['SSL_CLIENT_S_DN']
        else:
            request.session['userdn'] = request.META['HTTP_X_SSL_CLIENT_S_DN']
        userrec = Users.objects.filter(dn__startswith=request.session['userdn']).values()
        if len(userrec) > 0:
            request.session['username'] = userrec[0]['name']

    ENV['MON_VO'] = ''
    request.session['viewParams']['MON_VO'] = ''
    if 'HTTP_HOST' in request.META:
        for vo in VOLIST:
            if request.META['HTTP_HOST'].startswith(vo):
                VOMODE = vo
    else:
        VOMODE = 'atlas'

    ## If DB is Oracle, set vomode to atlas
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    ENV['MON_VO'] = VONAME[VOMODE]
    request.session['viewParams']['MON_VO'] = ENV['MON_VO']
    global errorFields, errorCodes, errorStages
    requestParams = {}
    request.session['requestParams'] = requestParams

    allowedemptyparams = ('json', 'snap', 'dt', 'dialogs', 'pandaids', 'workersstats')
    if request.method == 'POST':
        for p in request.POST:
            if p in ('csrfmiddlewaretoken',): continue
            pval = request.POST[p]
            pval = pval.replace('+', ' ')
            request.session['requestParams'][p.lower()] = pval
    else:
        for p in request.GET:
            pval = request.GET[p]
            ####if injection###
            if 'script' in pval.lower() or '</' in pval.lower() or '/>' in pval.lower():
                data = {
                    'viewParams': request.session['viewParams'],
                    'requestParams': request.session['requestParams'],
                    "errormessage": "Illegal value '%s' for %s" % (pval, p),
                }
                return False, render_to_response('errorPage.html', data, content_type='text/html')
            pval = pval.replace('+', ' ')
            pval = pval.replace("\'", '')
            if p.lower() != 'batchid':  # Special requester exception
                pval = pval.replace('#', '')
            ## is it int, if it's supposed to be?
            if p.lower() in (
            'days', 'hours', 'limit', 'display_limit', 'taskid', 'jeditaskid', 'jobsetid', 'reqid', 'corecount', 'taskpriority',
            'priority', 'attemptnr', 'statenotupdated', 'tasknotupdated','corepower','wansourcelimit','wansinklimit','nqueue','nodes','queuehours','memory','maxtime','space',
            'maxinputsize','timefloor','depthboost','idlepilotsupression','pilotlimit','transferringlimit','cachedse','stageinretry','stageoutretry','maxwdir','minmemory','maxmemory','minrss',
            'maxrss','mintime',):
                try:
                    requestVal = request.GET[p]
                    if '|' in requestVal:
                        values = requestVal.split('|')
                        for value in values:
                            i = int(value)
                    else:
                        i = int(requestVal)
                except:
                    data = {
                        'viewParams': request.session['viewParams'],
                        'requestParams': request.session['requestParams'],
                        "errormessage": "Illegal value '%s' for %s" % (pval, p),
                    }
                    return False, render_to_response('errorPage.html', data, content_type='text/html')
            if p.lower() in ('date_from', 'date_to'):
                try:
                    requestVal = request.GET[p]
                    i = parse_datetime(requestVal)
                except:
                    data = {
                        'viewParams': request.session['viewParams'],
                        'requestParams': request.session['requestParams'],
                        "errormessage": "Illegal value '%s' for %s" % (pval, p),
                    }
                    return False, render_to_response('errorPage.html', data, content_type='text/html')
            if p.lower() not in allowedemptyparams and len(pval) == 0:
                data = {
                    'viewParams': request.session['viewParams'],
                    'requestParams': request.session['requestParams'],
                    "errormessage": "Empty value '%s' for %s" % (pval, p),
                }
                return False, render_to_response('errorPage.html', data, content_type='text/html')
            request.session['requestParams'][p.lower()] = pval
    setupSiteInfo(request)

    with inilock:
        if len(errorFields) == 0:
            codes = ErrorCodes()
            errorFields, errorCodes, errorStages = codes.getErrorCodes()
    return True, None


def preprocessWildCardString(strToProcess, fieldToLookAt):
    if (len(strToProcess) == 0):
        return '(1=1)'
    isNot = False
    if strToProcess.startswith('!'):
        isNot = True
        strToProcess = strToProcess[1:]

    cardParametersRaw = strToProcess.split('*')
    cardRealParameters = [s for s in cardParametersRaw if len(s) >= 1]
    countRealParameters = len(cardRealParameters)
    countParameters = len(cardParametersRaw)

    if (countParameters == 0):
        return '(1=1)'
    currentRealParCount = 0
    currentParCount = 0
    extraQueryString = '('

    for parameter in cardParametersRaw:
        leadStar = False
        trailStar = False
        if len(parameter) > 0:

            if (currentParCount - 1 >= 0):
                #                if len(cardParametersRaw[currentParCount-1]) == 0:
                leadStar = True

            if (currentParCount + 1 < countParameters):
                #                if len(cardParametersRaw[currentParCount+1]) == 0:
                trailStar = True

            if fieldToLookAt.lower() == 'produserid':
                leadStar = True
                trailStar = True

            if fieldToLookAt.lower() == 'resourcetype':
                fieldToLookAt = 'resource_type'

            isEscape = False
            if '_' in parameter and fieldToLookAt.lower() != 'nucleus':
                parameter = parameter.replace('_', '!_')
                isEscape = True

            extraQueryString += "(UPPER(" + fieldToLookAt + ") "
            if isNot:
                extraQueryString += "NOT "
            if leadStar and trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "%%')"
            elif not leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "')"
            elif leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "')"
            elif not leadStar and trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "%%')"
            if isEscape:
                extraQueryString += " ESCAPE '!'"
            extraQueryString += ")"
            currentRealParCount += 1
            if currentRealParCount < countRealParameters:
                extraQueryString += ' AND '
        currentParCount += 1
    extraQueryString += ')'
    extraQueryString = extraQueryString.replace("%20", " ") if not '%%20' in extraQueryString else extraQueryString
    return extraQueryString


def setupView(request, opmode='', hours=0, limit=-99, querytype='job', wildCardExt=False):
    viewParams = {}
    if not 'viewParams' in request.session:
        request.session['viewParams'] = viewParams

    LAST_N_HOURS_MAX = 0

    for paramName, paramVal in request.session['requestParams'].items():
        try:
            request.session['requestParams'][paramName] = urllib.unquote(paramVal)
        except: request.session['requestParams'][paramName] = paramVal

    excludeJobNameFromWildCard = True
    if 'jobname' in request.session['requestParams']:
        jobrequest = request.session['requestParams']['jobname']
        if (('*' in jobrequest) or ('|' in jobrequest)):
            excludeJobNameFromWildCard = False
    excludeWGFromWildCard = False
    excludeSiteFromWildCard = False
    if 'workinggroup' in request.session['requestParams'] and 'preset' in request.session['requestParams'] and request.session['requestParams']['preset']=='MC':
        # if 'workinggroup' in request.session['requestParams']:
        workinggroupQuery = request.session['requestParams']['workinggroup']
        extraQueryString = '('
        for card in workinggroupQuery.split(','):
            if card[0] == '!':
                extraQueryString += ' NOT workinggroup=\''+escapeInput(card[1:])+'\' AND'
            else:
                extraQueryString += ' workinggroup=\''+escapeInput(card[0:])+'\' OR'
        if extraQueryString.endswith('AND'):
            extraQueryString = extraQueryString[:-3]
        elif extraQueryString.endswith('OR'):
            extraQueryString = extraQueryString[:-2]
        extraQueryString += ')'
        excludeWGFromWildCard = True
    elif 'workinggroup' in request.session['requestParams'] and request.session['requestParams']['workinggroup'] and \
                        '*' not in request.session['requestParams']['workinggroup'] and \
                        ',' not in request.session['requestParams']['workinggroup']:
        excludeWGFromWildCard = True

    if 'site' in request.session['requestParams'] and (request.session['requestParams']['site'] == 'hpc' or not (
                    '*' in request.session['requestParams']['site'] or '|' in request.session['requestParams']['site'])):
        excludeSiteFromWildCard = True


    wildSearchFields = []
    if querytype == 'job':
        for field in Jobsactive4._meta.get_fields():
            if (field.get_internal_type() == 'CharField'):
                if not (field.name == 'jobstatus' or field.name == 'modificationhost' #or field.name == 'batchid'
                        or (
                    excludeJobNameFromWildCard and field.name == 'jobname')):
                    wildSearchFields.append(field.name)
    if querytype == 'task':
        for field in JediTasks._meta.get_fields():
            if (field.get_internal_type() == 'CharField'):
                if not (field.name == 'modificationhost' or (excludeWGFromWildCard and field.name == 'workinggroup') or (excludeSiteFromWildCard and field.name == 'site')):
                    wildSearchFields.append(field.name)

    deepquery = False
    fields = standard_fields
    if 'limit' in request.session['requestParams']:
        request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
    elif limit != -99 and limit > 0:
        request.session['JOB_LIMIT'] = limit
    elif VOMODE == 'atlas':
        request.session['JOB_LIMIT'] = 10000
    else:
        request.session['JOB_LIMIT'] = 10000

    if VOMODE == 'atlas':
        LAST_N_HOURS_MAX = 12
    else:
        LAST_N_HOURS_MAX = 7 * 24

    if VOMODE == 'atlas':
        if 'cloud' not in fields: fields.append('cloud')
        if 'atlasrelease' not in fields: fields.append('atlasrelease')
        if 'produsername' in request.session['requestParams'] or 'jeditaskid' in request.session[
            'requestParams'] or 'user' in request.session['requestParams']:
            if 'jobsetid' not in fields: fields.append('jobsetid')
            if ('hours' not in request.session['requestParams']) and (
                'days' not in request.session['requestParams']) and (
                        'jobsetid' in request.session['requestParams'] or 'taskid' in request.session[
                    'requestParams'] or 'jeditaskid' in request.session['requestParams']):
                ## Cases where deep query is safe. Unless the time depth is specified in URL.
                if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
                    deepquery = True
        else:
            if 'jobsetid' in fields: fields.remove('jobsetid')
    else:
        fields.append('vo')

    if hours > 0:
        ## Call param overrides default hours, but not a param on the URL
        LAST_N_HOURS_MAX = hours
    ## For site-specific queries, allow longer time window

    if 'batchid' in request.session['requestParams'] and (hours is None or hours == 0):
        LAST_N_HOURS_MAX = 12
    if 'computingsite' in request.session['requestParams'] and hours is None:
        LAST_N_HOURS_MAX = 12
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        LAST_N_HOURS_MAX = 2 * 24
    ## hours specified in the URL takes priority over the above
    if 'hours' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['hours'])
    if 'days' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['days']) * 24
    ## Exempt single-job, single-task etc queries from time constraint
    if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
        if 'jeditaskid' in request.session['requestParams']: deepquery = True
        if 'taskid' in request.session['requestParams']: deepquery = True
        if 'pandaid' in request.session['requestParams']: deepquery = True
        if 'jobname' in request.session['requestParams']: deepquery = True
        #if 'batchid' in request.session['requestParams']: deepquery = True
    if deepquery:
        opmode = 'notime'
        hours = LAST_N_HOURS_MAX = 24 * 180
        request.session['JOB_LIMIT'] = 999999
    if opmode != 'notime':
        if LAST_N_HOURS_MAX <= 72 and not ('date_from' in request.session['requestParams'] or 'date_to' in request.session['requestParams']
                                           or 'earlierthan' in request.session['requestParams'] or 'earlierthandays' in request.session['requestParams']):
            request.session['viewParams']['selection'] = ", last %s hours" % LAST_N_HOURS_MAX
        else:
            request.session['viewParams']['selection'] = ", last %d days" % (float(LAST_N_HOURS_MAX) / 24.)
        # if JOB_LIMIT < 999999 and JOB_LIMIT > 0:
        #    viewParams['selection'] += ", <font style='color:#FF8040; size=-1'>Warning: limit %s per job table</font>" % JOB_LIMIT
        request.session['viewParams']['selection'] += ". <b>Params:</b> "
        # if 'days' not in requestParams:
        #    viewParams['selection'] += "hours=%s" % LAST_N_HOURS_MAX
        # else:
        #    viewParams['selection'] += "days=%s" % int(LAST_N_HOURS_MAX/24)
        if request.session['JOB_LIMIT'] < 100000 and request.session['JOB_LIMIT'] > 0:
            request.session['viewParams']['selection'] += " <b>limit=</b>%s" % request.session['JOB_LIMIT']
    else:
        request.session['viewParams']['selection'] = ""
    for param in request.session['requestParams']:
        if request.session['requestParams'][param] == 'None': continue
        if request.session['requestParams'][param] == '': continue
        if param == 'display_limit': continue
        if param == 'sortby': continue
        if param == 'limit' and request.session['JOB_LIMIT'] > 0: continue
        request.session['viewParams']['selection'] += " <b>%s=</b>%s " % (
        param, request.session['requestParams'][param])

    startdate = None
    if 'date_from' in request.session['requestParams']:
        startdate = parse_datetime(request.session['requestParams']['date_from'])
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    # startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = None

    endtime__castdate__range = None
    if 'endtimerange' in request.session['requestParams']:
        endtimerange = request.session['requestParams']['endtimerange'].split('|')
        endtime__castdate__range = [parse_datetime(endtimerange[0]).strftime(defaultDatetimeFormat),
                                    parse_datetime(endtimerange[1]).strftime(defaultDatetimeFormat)]

    if 'date_to' in request.session['requestParams']:
        enddate = parse_datetime(request.session['requestParams']['date_to'])
    if 'earlierthan' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthan']))
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if 'earlierthandays' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthandays']) * 24)
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if enddate == None:
        enddate = timezone.now()  # .strftime(defaultDatetimeFormat)
        request.session['noenddate'] = True
    else:
        request.session['noenddate'] = False

    if request.path.startswith('/running'):
        query = {}
    else:
        if not endtime__castdate__range:
            query = {
                'modificationtime__castdate__range': [startdate.strftime(defaultDatetimeFormat), enddate.strftime(defaultDatetimeFormat)]}
        else:
            query = {
                'endtime__castdate__range': [endtime__castdate__range[0], endtime__castdate__range[1]]}


    request.session['TFIRST'] = startdate  # startdate[:18]
    request.session['TLAST'] = enddate  # enddate[:18]

    ### Add any extensions to the query determined from the URL
    #query['vo'] = 'atlas'
    #for vo in ['atlas', 'core']:
    #    if 'HTTP_HOST' in request.META and request.META['HTTP_HOST'].startswith(vo):
    #        query['vo'] = vo
    for param in request.session['requestParams']:
        if param in ('hours', 'days'): continue
        if param == 'cloud' and request.session['requestParams'][param] == 'All':
            continue
        elif param == 'harvesterinstance' or param == 'harvesterid':
             if request.session['requestParams'][param] == 'all':
                 query['schedulerid__startswith'] = 'harvester'
             else:
                 val = request.session['requestParams'][param]
                 query['schedulerid'] = 'harvester-'+val
        elif param == 'schedulerid':
             if 'harvester-*' in request.session['requestParams'][param]:
                 query['schedulerid__startswith'] = 'harvester'
             else:
                 val = request.session['requestParams'][param]
                 query['schedulerid__startswith'] = val
        elif param == 'priorityrange':
            mat = re.match('([0-9]+)\:([0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['currentpriority__gte'] = plo
                query['currentpriority__lte'] = phi
        elif param == 'jobsetrange':
            mat = re.match('([0-9]+)\:([0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['jobsetid__gte'] = plo
                query['jobsetid__lte'] = phi
        elif param == 'user' or param == 'username' or param == 'produsername':
            if querytype == 'job':
                query['produsername__icontains'] = request.session['requestParams'][param].strip()
        elif param in ('project',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__istartswith'] = val
        elif param in ('outputfiletype',) and querytype != 'task':
            val = request.session['requestParams'][param]
            query['destinationdblock__icontains'] = val
        elif param in ('stream',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__icontains'] = val

        elif param == 'harvesterid':
            val = escapeInput(request.session['requestParams'][param])
            values = val.split(',')
            query['harvesterid__in'] = values


        elif param in ('tag',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__endswith'] = val


        elif param == 'reqid_from':
            val = int(request.session['requestParams'][param])
            query['reqid__gte'] = val
        elif param == 'reqid_to':
            val = int(request.session['requestParams'][param])
            query['reqid__lte'] = val
        elif param == 'processingtype' and '|' not in request.session['requestParams'][param] and '*' not in request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            query['processingtype'] = val
        elif param == 'mismatchedcloudsite' and request.session['requestParams'][param] == 'true':
            listOfCloudSitesMismatched = cache.get('mismatched-cloud-sites-list')
            if (listOfCloudSitesMismatched is None) or (len(listOfCloudSitesMismatched) == 0):
                request.session['viewParams'][
                    'selection'] += "      <b>The query can not be processed because list of mismatches is not found. Please visit %s/dash/production/?cloudview=region page and then try again</b>" % \
                                    request.session['hostname']
            else:
                try:
                    extraQueryString += ' AND ( '
                except NameError:
                    extraQueryString = '('
                for count, cloudSitePair in enumerate(listOfCloudSitesMismatched):
                    extraQueryString += ' ( (cloud=\'%s\') and (computingsite=\'%s\') ) ' % (
                    cloudSitePair[1], cloudSitePair[0])
                    if (count < (len(listOfCloudSitesMismatched) - 1)):
                        extraQueryString += ' OR '
                extraQueryString += ')'
        elif param == 'pilotversion' and request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            if val == 'Not specified':
                try:
                    extraQueryString += ' AND ( '
                except NameError:
                    extraQueryString = '('
                extraQueryString += '(pilotid not like \'%%|%%\') or (pilotid is null)'
                extraQueryString += ')'
            else:
                query['pilotid__endswith'] = val
        elif param == 'durationmin' and request.session['requestParams'][param]:
            try:
                durationrange = request.session['requestParams'][param].split('-')
            except:
                continue
            try:
                extraQueryString += ' AND ( '
            except NameError:
                extraQueryString = '('
            if durationrange[0] == '0' and durationrange[1] == '0':
                extraQueryString += ' (endtime is NULL and starttime is null) ) '
            else:
                extraQueryString += """ 
            (endtime is not NULL and starttime is not null 
            and (endtime - starttime) * 24 * 60 > {} and (endtime - starttime) * 24 * 60 < {} ) 
            or 
            (endtime is NULL and starttime is not null 
            and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 > {} and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 < {} ) 
            ) """.format(str(durationrange[0]), str(durationrange[1]), str(durationrange[0]), str(durationrange[1]))
        elif param == 'errormessage':
            errfield_map_dict = {}
            for errcode in errorcodelist:
                if errcode['name'] != 'transformation':
                    errfield_map_dict[errcode['error']] = errcode['diag']
            for parname in request.session['requestParams']:
                if parname in errfield_map_dict.keys():
                    query[errfield_map_dict[parname]] = request.session['requestParams'][param]


        if querytype == 'task':
            for field in JediTasks._meta.get_fields():
                # for param in requestParams:
                if param == field.name:
                    if param == 'ramcount':
                        if 'GB' in request.session['requestParams'][param]:
                            leftlimit, rightlimit = (request.session['requestParams'][param]).split('-')
                            rightlimit = rightlimit[:-2]
                            query['%s__range' % param] = (int(leftlimit) * 1000, int(rightlimit) * 1000 - 1)
                        else:
                            query[param] = int(request.session['requestParams'][param])
                    elif param == 'transpath':
                        query['%s__endswith' % param] = request.session['requestParams'][param]
                    elif param == 'tasktype':
                        ttype = request.session['requestParams'][param]
                        if ttype.startswith('anal'):
                            ttype = 'anal'
                        elif ttype.startswith('prod'):
                            ttype = 'prod'
                        query[param] = ttype
                    elif param == 'jeditaskid':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['jeditaskid__in'] = values
                    elif param == 'status':
                        val = escapeInput(request.session['requestParams'][param])
                        if '*' not in val and '|' not in val and '!' not in val:
                            values = val.split(',')
                            query['status__in'] = values
                    elif param == 'superstatus':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['superstatus__in'] = values
                    elif param == 'workinggroup':
                        if request.session['requestParams'][param] and \
                                '*' not in request.session['requestParams'][param] and \
                                ',' not in request.session['requestParams'][param]:
                            query[param]=request.session['requestParams'][param]
                    elif param == 'reqid':
                        val = escapeInput(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'site':
                        if request.session['requestParams'][param] != 'hpc' and excludeSiteFromWildCard:
                            query['site__contains'] = request.session['requestParams'][param]
                    elif param == 'eventservice':
                        if request.session['requestParams'][param] == 'eventservice' or \
                                        request.session['requestParams'][param] == '1':
                            query['eventservice'] = 1
                        else:
                            query['eventservice'] = 0
                    else:
                        if (param not in wildSearchFields):
                            query[param] = request.session['requestParams'][param]
        elif param == 'reqtoken':
                data = getCacheData(request, request.session['requestParams']['reqtoken'])
                if data is not None:
                    if 'pandaid' in data:
                        pid = data['pandaid']
                        if pid.find(',') >= 0:
                            pidl = pid.split(',')
                            query['pandaid__in'] = pidl
                        else:
                            query['pandaid'] = int(pid)
                    elif 'jeditaskid' in data:
                        tid = data['jeditaskid']
                        if tid.find(',') >= 0:
                            tidl = tid.split(',')
                            query['jeditaskid__in'] = tidl
                        else:
                            query['jeditaskid'] = int(tid)

                else: return 'reqtoken', None, None

        else:
            for field in Jobsactive4._meta.get_fields():
                if param == field.name:
                    if param == 'minramcount':
                        if 'GB' in request.session['requestParams'][param]:
                            leftlimit, rightlimit = (request.session['requestParams'][param]).split('-')
                            rightlimit = rightlimit[:-2]
                            query['%s__range' % param] = (int(leftlimit) * 1000, int(rightlimit) * 1000 - 1)
                        else:
                            query[param] = int(request.session['requestParams'][param])
                    elif param == 'specialhandling' and not '*' in request.session['requestParams'][param]:
                        query['specialhandling__contains'] = request.session['requestParams'][param]
                    elif param == 'reqid':
                        val = escapeInput(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'transformation' or param == 'transpath':
                        paramQuery = request.session['requestParams'][param]
                        if paramQuery[0] == '*': paramQuery = paramQuery[1:]
                        if paramQuery[-1] == '*': paramQuery = paramQuery[:-1]
                        query['%s__contains' % param] = paramQuery
                    elif param == 'modificationhost' and request.session['requestParams'][param].find('@') < 0:
                        paramQuery = request.session['requestParams'][param]
                        if paramQuery[0] == '*': paramQuery = paramQuery[1:]
                        if paramQuery[-1] == '*': paramQuery = paramQuery[:-1]
                        query['%s__contains' % param] = paramQuery
                    elif param == 'jeditaskid':
                        if request.session['requestParams']['jeditaskid'] != 'None':
                            if int(request.session['requestParams']['jeditaskid']) < 4000000:
                                query['taskid'] = request.session['requestParams'][param]
                            else:
                                query[param] = request.session['requestParams'][param]
                    elif param == 'taskid':
                        if request.session['requestParams']['taskid'] != 'None': query[param] = \
                        request.session['requestParams'][param]
                    elif param == 'pandaid':
                        try:
                            pid = request.session['requestParams']['pandaid']
                            if pid.find(',') >= 0:
                                pidl = pid.split(',')
                                query['pandaid__in'] = pidl
                            else:
                                query['pandaid'] = int(pid)
                        except:
                            query['jobname'] = request.session['requestParams']['pandaid']
                    elif param == 'jobstatus' and request.session['requestParams'][param] == 'finished' \
                            and (('mode' in  request.session['requestParams'] and request.session['requestParams']['mode'] == 'eventservice') or (
                                    'jobtype' in request.session['requestParams'] and request.session['requestParams'][
                                'jobtype'] == 'eventservice')):
                        query['jobstatus__in'] = ('finished', 'cancelled')
                    elif param == 'jobstatus':
                        val = escapeInput(request.session['requestParams'][param])
                        values = val.split('|')
                        query['jobstatus__in'] = values
                    elif param == 'eventservice':
                        if '|' in request.session['requestParams'][param]:
                            paramsstr = request.session['requestParams'][param]
                            paramsstr = paramsstr.replace('eventservice', '1')
                            paramsstr = paramsstr.replace('esmerge', '2')
                            paramsstr = paramsstr.replace('clone', '3')
                            paramsstr = paramsstr.replace('cojumbo', '5')
                            paramsstr = paramsstr.replace('jumbo', '4')
                            paramvalues = paramsstr.split('|')
                            try:
                                paramvalues = [int(p) for p in paramvalues]
                            except:
                                paramvalues = []
                            query['eventservice__in'] = paramvalues
                        else:
                            if request.session['requestParams'][param] == 'esmerge' or request.session['requestParams'][
                                param] == '2':
                                query['eventservice'] = 2
                            elif request.session['requestParams'][param] == 'clone' or request.session['requestParams'][
                                param] == '3':
                                query['eventservice'] = 3
                            elif request.session['requestParams'][param] == 'jumbo' or request.session['requestParams'][
                                param] == '4':
                                query['eventservice'] = 4
                            elif request.session['requestParams'][param] == 'cojumbo' or request.session['requestParams'][
                                param] == '5':
                                query['eventservice'] = 5
                            elif request.session['requestParams'][param] == 'eventservice' or \
                                            request.session['requestParams'][param] == '1':
                                query['eventservice'] = 1
                                try:
                                    extraQueryString += " not specialhandling like \'%%sc:%%\' "
                                except NameError:
                                    extraQueryString = " not specialhandling like \'%%sc:%%\' "

                            elif request.session['requestParams'][param] == 'not2':
                                try:
                                    extraQueryString += ' AND (eventservice != 2) '
                                except NameError:
                                    extraQueryString = '(eventservice != 2)'
                            else:
                                query['eventservice__isnull'] = True
                    elif param == 'corecount' and request.session['requestParams'][param] == '1':
                        try:
                            extraQueryString += 'AND (corecount = 1 or corecount is NULL)'
                        except:
                            extraQueryString = '(corecount = 1 or corecount is NULL) '
                    else:
                        if (param not in wildSearchFields):
                            query[param] = request.session['requestParams'][param]

    if 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = opmode
    if jobtype.startswith('anal'):
        query['prodsourcelabel__in'] = ['panda', 'user', 'prod_test', 'rc_test']
    elif jobtype.startswith('prod'):
        query['prodsourcelabel__in'] = ['managed', 'prod_test', 'rc_test']
    elif jobtype == 'groupproduction':
        query['prodsourcelabel'] = 'managed'
        query['workinggroup__isnull'] = False
    elif jobtype == 'eventservice':
        query['eventservice'] = 1
    elif jobtype == 'esmerge':
        query['eventservice'] = 2
    elif jobtype.find('test') >= 0:
        query['prodsourcelabel__icontains'] = jobtype

    if 'region' in request.session['requestParams']:
        region = request.session['requestParams']['region']
        siteListForRegion = []
        try:
            homeCloud
        except NameError:
            setupSiteInfo(request)
        else:
            setupSiteInfo(request)

        for sn, rn in homeCloud.items():
            if rn == region:
                siteListForRegion.append(str(sn))
        query['computingsite__in'] = siteListForRegion

    if (wildCardExt == False):
        return query

    try:
        extraQueryString += ' AND '
    except NameError:
        extraQueryString = ''


    wildSearchFields = (set(wildSearchFields) & set(list(request.session['requestParams'].keys())))
    wildSearchFields1 = set()
    for currenfField in wildSearchFields:
        if not (currenfField.lower() == 'transformation'):
            if not ((currenfField.lower() == 'cloud') & (
            any(card.lower() == 'all' for card in request.session['requestParams'][currenfField].split('|')))):
                if not any(currenfField in key for key, value in query.items()):
                    wildSearchFields1.add(currenfField)
    wildSearchFields = wildSearchFields1

    lenWildSearchFields = len(wildSearchFields)
    currentField = 1

    for currenfField in wildSearchFields:
        extraQueryString += '('
        wildCards = request.session['requestParams'][currenfField].split('|')
        countCards = len(wildCards)
        currentCardCount = 1
        if not ((currenfField.lower() == 'cloud') & (any(card.lower() == 'all' for card in wildCards))):
            for card in wildCards:
                extraQueryString += preprocessWildCardString(card, currenfField)
                if (currentCardCount < countCards): extraQueryString += ' OR '
                currentCardCount += 1
            extraQueryString += ')'
            if (currentField < lenWildSearchFields): extraQueryString += ' AND '
            currentField += 1

    if ('jobparam' in request.session['requestParams'].keys()):
        jobParWildCards = request.session['requestParams']['jobparam'].split('|')
        jobParCountCards = len(jobParWildCards)
        jobParCurrentCardCount = 1
        extraJobParCondition = '('
        for card in jobParWildCards:
            extraJobParCondition += preprocessWildCardString(escapeInput(card), 'JOBPARAMETERS')
            if (jobParCurrentCardCount < jobParCountCards): extraJobParCondition += ' OR '
            jobParCurrentCardCount += 1
        extraJobParCondition += ')'

        pandaIDs = []
        jobParamQuery = {'modificationtime__castdate__range': [startdate.strftime(defaultDatetimeFormat),
                                                     enddate.strftime(defaultDatetimeFormat)]}

        jobs = Jobparamstable.objects.filter(**jobParamQuery).extra(where=[extraJobParCondition]).values('pandaid')
        for values in jobs:
            pandaIDs.append(values['pandaid'])

        query['pandaid__in'] = pandaIDs

    if extraQueryString.endswith(' AND '):
        extraQueryString = extraQueryString[:-5]

    if (len(extraQueryString) < 2):
        extraQueryString = '1=1'

    return (query, extraQueryString, LAST_N_HOURS_MAX)


def saveUserSettings(request, page):

    if page == 'errors':
        errorspage_tables = ['jobattrsummary', 'errorsummary', 'siteerrorsummary', 'usererrorsummary',
                            'taskerrorsummary']
        preferences = {}
        if 'jobattr' in request.session['requestParams']:
            preferences["jobattr"] = request.session['requestParams']['jobattr'].split(",")
            try:
                del request.session['requestParams']['jobattr']
            except:
                pass
        else:
            preferences["jobattr"] = standard_errorfields
        if 'tables' in request.session['requestParams']:
            preferences['tables'] = request.session['requestParams']['tables'].split(",")
            try:
                del request.session['requestParams']['tables']
            except:
                pass
        else:
            preferences['tables'] = errorspage_tables
        query = {}
        query['page']= str(page)
        if request.user.is_authenticated:
            userids = BPUser.objects.filter(email=request.user.email).values('id')
            userid = userids[0]['id']
            try:
                userSetting = BPUserSettings.objects.get(page=page, userid=userid)
                userSetting.preferences = json.dumps(preferences)
                userSetting.save(update_fields=['preferences'])
            except BPUserSettings.DoesNotExist:
                userSetting = BPUserSettings(page=page, userid=userid, preferences=json.dumps(preferences))
                userSetting.save()


def saveSettings(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    data = {}
    if 'page' in request.session['requestParams']:
        page = request.session['requestParams']['page']
        if page == 'errors':
            errorspage_tables = ['jobattrsummary', 'errorsummary', 'siteerrorsummary', 'usererrorsummary',
                                'taskerrorsummary']
            preferences = {}
            if 'jobattr' in request.session['requestParams']:
                preferences["jobattr"] = request.session['requestParams']['jobattr'].split(",")
                try:
                    del request.session['requestParams']['jobattr']
                    request.session.pop('jobattr')
                except:
                    pass
            else:
                preferences["jobattr"] = standard_errorfields
            if 'tables' in request.session['requestParams']:
                preferences['tables'] = request.session['requestParams']['tables'].split(",")
                try:
                    del request.session['requestParams']['tables']
                    request.session.pop('tables')
                except:
                    pass
            else:
                preferences['tables'] = errorspage_tables
            query = {}
            query['page']= str(page)
            if request.user.is_authenticated:
                userids = BPUser.objects.filter(email=request.user.email).values('id')
                userid = userids[0]['id']
                try:
                    userSetting = BPUserSettings.objects.get(page=page, userid=userid)
                    userSetting.preferences = json.dumps(preferences)
                    userSetting.save(update_fields=['preferences'])
                except BPUserSettings.DoesNotExist:
                    userSetting = BPUserSettings(page=page, userid=userid, preferences=json.dumps(preferences))
                    userSetting.save()

        return HttpResponse(status=204)
    else:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

def dropRetrielsJobs(jobs, jeditaskid, isReturnDroppedPMerge):
    # dropping algorithm for jobs belong to single task
    # !!! Necessary job's attributes:
    # PANDAID
    # JOBSTATUS
    # PROCESSINGTYPE
    # JOBSETID

    droplist = []
    droppedIDs = set()
    droppedPmerge = set()



    retryquery = {}
    retryquery['jeditaskid'] = jeditaskid
    retries = JediJobRetryHistory.objects.filter(**retryquery).extra(
        where=["OLDPANDAID!=NEWPANDAID AND RELATIONTYPE IN ('', 'retry', 'pmerge', 'merge', "
               "'jobset_retry', 'es_merge', 'originpandaid')"]).order_by('newpandaid').values()
    print ('got retriels %d %d' % (len(retries),len(jobs)))
    print ('doing the drop')
    hashRetries = {}
    for retry in retries:
        hashRetries[retry['oldpandaid']] = retry

    newjobs = []
    for job in jobs:
        dropJob = 0
        pandaid = job['pandaid']
        if not isEventService(job):
            if pandaid in hashRetries:
                retry = hashRetries[pandaid]
                if retry['relationtype'] == '' or retry['relationtype'] == 'retry' or (
                            job['processingtype'] == 'pmerge' and (job['jobstatus'] == 'failed' or job['jobstatus'] == 'cancelled') and retry[
                    'relationtype'] == 'merge'):
                    dropJob = retry['newpandaid']
            else:
                if (job['jobsetid'] in hashRetries) and (
                    hashRetries[job['jobsetid']]['relationtype'] == 'jobset_retry'):
                    dropJob = 1
        else:

            if job['pandaid'] in hashRetries and job['jobstatus'] not in ('finished', 'merging'):
                if hashRetries[job['pandaid']]['relationtype'] == 'retry':
                    dropJob = 1

            # if (hashRetries[job['pandaid']]['relationtype'] == 'es_merge' and (
            #        job['jobsubstatus'] == 'es_merge')):
            #        dropJob = 1

            if (dropJob == 0):
                if (job['jobsetid'] in hashRetries) and (
                            hashRetries[job['jobsetid']]['relationtype'] == 'jobset_retry'):
                    dropJob = 1

                if (job['jobstatus'] == 'closed' and (job['jobsubstatus'] in ('es_unused', 'es_inaction'))):
                    dropJob = 1

                    #               if 'jobstatus' in request.session['requestParams'] and request.session['requestParams'][
                    #                   'jobstatus'] == 'cancelled' and job['jobstatus'] != 'cancelled':
                    #                   dropJob = 1

        if dropJob == 0 and not isReturnDroppedPMerge:
            #     and not (
            #     'processingtype' in request.session['requestParams'] and request.session['requestParams'][
            # 'processingtype'] == 'pmerge')

            if not (job['processingtype'] == 'pmerge'):
                newjobs.append(job)
            else:
                droppedPmerge.add(pandaid)
        elif (dropJob == 0):
            newjobs.append(job)
        else:
            if not pandaid in droppedIDs:
                droppedIDs.add(pandaid)
                droplist.append({'pandaid': pandaid, 'newpandaid': dropJob})
    print ('%d jobs dropped' % (len(jobs) - len(newjobs)))
    droplist = sorted(droplist, key=lambda x: -x['pandaid'])
    jobs = newjobs
    return jobs, droplist, droppedPmerge



def cleanJobListLite(request, jobl, mode='nodrop', doAddMeta=True):
    for job in jobl:
        job['duration'] = ""
        job['durationsec'] = 0
        # if job['jobstatus'] in ['finished','failed','holding']:
        if 'endtime' in job and 'starttime' in job and job['starttime']:
            starttime = job['starttime']
            if job['endtime']:
                endtime = job['endtime']
            else:
                endtime = timezone.now()

            duration = max(endtime - starttime, timedelta(seconds=0))
            ndays = duration.days
            strduration = str(timedelta(seconds=duration.seconds))
            job['duration'] = "%s:%s" % (ndays, strduration)
            job['durationsec'] = ndays * 24 * 3600 + duration.seconds

        job['waittime'] = ""
        # if job['jobstatus'] in ['running','finished','failed','holding','cancelled','transferring']:
        if 'creationtime' in job and 'starttime' in job and job['creationtime']:
            creationtime = job['creationtime']
            if job['starttime']:
                starttime = job['starttime']
            elif job['jobstatus'] in ('finished', 'failed', 'closed', 'cancelled'):
                starttime = job['modificationtime']
            else:
                starttime = datetime.now()
            wait = starttime - creationtime
            ndays = wait.days
            strwait = str(timedelta(seconds=wait.seconds))
            job['waittime'] = "%s:%s" % (ndays, strwait)
        if 'currentpriority' in job:
            plo = int(job['currentpriority']) - int(job['currentpriority']) % 100
            phi = plo + 99
            job['priorityrange'] = "%d:%d" % (plo, phi)
        if 'jobsetid' in job and job['jobsetid']:
            plo = int(job['jobsetid']) - int(job['jobsetid']) % 100
            phi = plo + 99
            job['jobsetrange'] = "%d:%d" % (plo, phi)
    return jobl




def cleanJobList(request, jobl, mode='nodrop', doAddMeta=True):
    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'drop': mode = 'drop'
    doAddMetaStill = False
    if 'fields' in request.session['requestParams']:
        fieldsStr = request.session['requestParams']['fields']
        fields = fieldsStr.split("|")
        if 'metastruct' in fields:
            doAddMetaStill = True
    if doAddMeta or doAddMetaStill:
        jobs = addJobMetadata(jobl, doAddMetaStill)
    else:
        jobs = jobl
    for job in jobs:
        if isEventService(job):
            #if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 111:
            #    job['taskbuffererrordiag'] = 'Rerun scheduled to pick up unprocessed events'
            #    job['piloterrorcode'] = 0
            #    job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
            # job['jobstatus'] = 'finished'
            #if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 112:
                #                job['taskbuffererrordiag'] = 'All events processed, merge job created'
            #    job['piloterrorcode'] = 0
            #    job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
            # job['jobstatus'] = 'finished'
            #if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 114:
            #    job['taskbuffererrordiag'] = 'No rerun to pick up unprocessed, at max attempts'
            #    job['piloterrorcode'] = 0
            #    job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
            # job['jobstatus'] = 'finished'
            # if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 115:
            #    job['taskbuffererrordiag'] = 'No events remaining, other jobs still processing'
            #    job['piloterrorcode'] = 0
            #    job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
            #    #job['jobstatus'] = 'finished'
            #if 'taskbuffererrorcode' in job and job['taskbuffererrorcode'] == 116:
            #    job['taskbuffererrordiag'] = 'No remaining event ranges to allocate'
            #    job['piloterrorcode'] = 0
            #    job['piloterrordiag'] = 'Job terminated by signal from PanDA server'
                # job['jobstatus'] = 'finished'
            if 'jobmetrics' in job:
                pat = re.compile('.*mode\=([^\s]+).*HPCStatus\=([A-Za-z0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['jobmode'] = mat.group(1)
                    job['substate'] = mat.group(2)
                pat = re.compile('.*coreCount\=([0-9]+)')
                mat = pat.match(job['jobmetrics'])
                if mat:
                    job['corecount'] = mat.group(1)
            if 'jobsubstatus' in job and job['jobstatus'] == 'closed' and job['jobsubstatus'] == 'toreassign':
                job['jobstatus'] += ':' + job['jobsubstatus']
        if 'eventservice' in job:
            if isEventService(job) and job['eventservice'] == 1:
                job['eventservice'] = 'eventservice'
            elif isEventService(job) and job['eventservice'] == 2:
                job['eventservice'] = 'esmerge'
            elif job['eventservice'] == 3:
                job['eventservice'] = 'clone'
            elif isEventService(job) and job['eventservice'] == 4:
                job['eventservice'] = 'jumbo'
            elif job['eventservice'] == 5:
                job['eventservice'] = 'cojumbo'
            else:
                job['eventservice'] = 'ordinary'
        if 'destinationdblock' in job:
            ddbfields = job['destinationdblock'].split('.')
            if len(ddbfields) == 6 and ddbfields[0] != 'hc_test':
                job['outputfiletype'] = ddbfields[4]
            elif len(ddbfields) >= 7:
                job['outputfiletype'] = ddbfields[6]
            # else:
            #     job['outputfiletype'] = None
            #     print job['destinationdblock'], job['outputfiletype'], job['pandaid']

        try:
            job['homecloud'] = homeCloud[job['computingsite']]
        except:
            job['homecloud'] = None
        if 'produsername' in job and not job['produsername']:
            if ('produserid' in job) and job['produserid']:
                job['produsername'] = job['produserid']
            else:
                job['produsername'] = 'Unknown'
        if job['transformation']: job['transformation'] = job['transformation'].split('/')[-1]
        if (job['jobstatus'] == 'failed' or job['jobstatus'] == 'cancelled') and 'brokerageerrorcode' in job:
            job['errorinfo'] = errorInfo(job, nchars=70)
        else:
            job['errorinfo'] = ''
        job['jobinfo'] = ''
        if isEventService(job):
            if 'taskbuffererrordiag' in job and job['taskbuffererrordiag'] is None:
                job['taskbuffererrordiag'] = ''
            if 'taskbuffererrordiag' in job and len(job['taskbuffererrordiag']) > 0:
                job['jobinfo'] = job['taskbuffererrordiag']
            elif 'specialhandling' in job and job['specialhandling'] == 'esmerge':
                job['jobinfo'] = 'Event service merge job'
            elif 'eventservice' in job and job['eventservice'] == 'jumbo':
                job['jobinfo'] = 'Jumbo job'
            else:
                job['jobinfo'] = 'Event service job'
        job['duration'] = ""
        job['durationsec'] = 0
        # if job['jobstatus'] in ['finished','failed','holding']:
        if 'endtime' in job and 'starttime' in job and job['starttime']:
            starttime = job['starttime']
            if job['endtime']:
                endtime = job['endtime']
            else:
                endtime = timezone.now()

            duration = max(endtime - starttime, timedelta(seconds=0))
            ndays = duration.days
            strduration = str(timedelta(seconds=duration.seconds))
            job['duration'] = "%s:%s" % (ndays, strduration)
            job['durationsec'] = ndays * 24 * 3600 + duration.seconds
            job['durationmin'] = round((ndays * 24 * 3600 + duration.seconds)/60)

        # durationmin for active jobs = now - starttime, for non-started = 0
        if not 'durationmin' in job:
            if 'starttime' in job and job['starttime'] is not None and 'endtime' in job and job['endtime'] is None:
                endtime = timezone.now()
                starttime = job['starttime']
                duration = max(endtime - starttime, timedelta(seconds=0))
                ndays = duration.days
                job['durationmin'] = round((ndays * 24 * 3600 + duration.seconds)/60)
            else:
                job['durationmin'] = 0

        job['waittime'] = ""
        # if job['jobstatus'] in ['running','finished','failed','holding','cancelled','transferring']:
        if 'creationtime' in job and 'starttime' in job and job['creationtime']:
            creationtime = job['creationtime']
            if job['starttime']:
                starttime = job['starttime']
            elif job['jobstatus'] in ('finished', 'failed', 'closed', 'cancelled'):
                starttime = job['modificationtime']
            else:
                starttime = datetime.now()
            wait = starttime - creationtime
            ndays = wait.days
            strwait = str(timedelta(seconds=wait.seconds))
            job['waittime'] = "%s:%s" % (ndays, strwait)
        if 'currentpriority' in job:
            plo = int(job['currentpriority']) - int(job['currentpriority']) % 100
            phi = plo + 99
            job['priorityrange'] = "%d:%d" % (plo, phi)
        if 'jobsetid' in job and job['jobsetid']:
                plo = int(job['jobsetid']) - int(job['jobsetid']) % 100
                phi = plo + 99
                job['jobsetrange'] = "%d:%d" % (plo, phi)
        if 'corecount' in job and job['corecount'] is None:
            job['corecount'] = 1
    ## drop duplicate jobs
    droplist = []
    job1 = {}
    newjobs = []
    for job in jobs:
        pandaid = job['pandaid']
        dropJob = 0
        if pandaid in job1:
            ## This is a duplicate. Drop it.
            dropJob = 1
        else:
            job1[pandaid] = 1
        if (dropJob == 0):
            newjobs.append(job)
    jobs = newjobs

    # if mode == 'nodrop':
    #     print ('job list cleaned')
    #     return jobs

    global PLOW, PHIGH
    # request.session['TFIRST'] = timezone.now()  # .strftime(defaultDatetimeFormat)
    # request.session['TLAST'] = (timezone.now() - timedelta(hours=2400))  # .strftime(defaultDatetimeFormat)
    PLOW = 1000000
    PHIGH = -1000000
    for job in jobs:
        # if job['modificationtime'] > request.session['TLAST']: request.session['TLAST'] = job['modificationtime']
        # if job['modificationtime'] < request.session['TFIRST']: request.session['TFIRST'] = job['modificationtime']
        if job['currentpriority'] > PHIGH: PHIGH = job['currentpriority']
        if job['currentpriority'] < PLOW: PLOW = job['currentpriority']
    jobs = sorted(jobs, key=lambda x: x['modificationtime'], reverse=True)

    print ('job list cleaned')
    return jobs


def reconstructJobsConsumersHelper(chainsDict):
    reconstructionDict = {}
    modified = False
    for pandaid, parentids in chainsDict.items():
        if parentids and parentids[-1] in chainsDict:
            if chainsDict[parentids[-1]]:
                reconstructionDict[pandaid] = parentids + chainsDict[parentids[-1]]
                modified = True
            else:
                reconstructionDict[pandaid] = parentids
        else:
            reconstructionDict[pandaid] = parentids

    if modified:
        return reconstructJobsConsumersHelper(reconstructionDict)
    else:
        return reconstructionDict



def reconstructJobsConsumers(jobsList):
    consumers = []
    jobsInheritance = {}
    chainsList = {}

    #Fill out all possible consumers
    for job in jobsList:
        jobsInheritance[job['pandaid']] = [job['parentid']]

    chains = reconstructJobsConsumersHelper(jobsInheritance)
    cleanChain = {}
    for name, value in chains.items():
        if len(value) > 1:
            cleanChain[name] = value[-2]
            for pandaid in value[:-2]:
                cleanChain[pandaid] = value[-2]

    
    for job in jobsList:
        if job['pandaid'] in cleanChain:
            job['consumer'] = cleanChain[job['pandaid']]
        else:
            job['consumer'] = None

    return jobsList
    


def cleanTaskList(request, tasks):
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"

    for task in tasks:
        if task['transpath']: task['transpath'] = task['transpath'].split('/')[-1]
        if task['statechangetime'] == None: task['statechangetime'] = task['modificationtime']

    ## Get status of input processing as indicator of task progress
    dsquery = {}
    dsquery['type__in'] = ['input', 'pseudo_input']
    dsquery['masterid__isnull'] = True
    taskl = []
    for t in tasks:
        taskl.append(t['jeditaskid'])
    # dsquery['jeditaskid__in'] = taskl

    random.seed()
    transactionKey = random.randrange(1000000)
#    connection.enter_transaction_management()
    new_cur = connection.cursor()
    for id in taskl:
        new_cur.execute("INSERT INTO %s(ID,TRANSACTIONKEY) VALUES (%i,%i)" % (
            tmpTableName, id, transactionKey))  # Backend dependable
#    connection.commit()
    dsets = JediDatasets.objects.filter(**dsquery).extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values(
        'jeditaskid', 'nfiles', 'nfilesfinished', 'nfilesfailed')
    dsinfo = {}
    if len(dsets) > 0:
        for ds in dsets:
            taskid = ds['jeditaskid']
            if taskid not in dsinfo:
                dsinfo[taskid] = []
            dsinfo[taskid].append(ds)

    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
#    connection.commit()
#   connection.leave_transaction_management()

    for task in tasks:
        if 'totevrem' not in task:
            task['totevrem'] = None
        if 'eventservice' in task:
            if task['eventservice'] == 1:
                task['eventservice'] = 'eventservice'
            else:
                task['eventservice'] = 'ordinary'
        if 'reqid' in task and task['reqid'] is not None and task['reqid'] < 100000 and task['reqid'] > 100 and task['reqid'] != 300 and (
            ('tasktype' in task) and (not task['tasktype'].startswith('anal'))):
            task['deftreqid'] = task['reqid']
        if 'corecount' in task and task['corecount'] is None:
            task['corecount'] = 1
        # if task['status'] == 'running' and task['jeditaskid'] in dsinfo:
        dstotals = {}
        dstotals['nfiles'] = 0
        dstotals['nfilesfinished'] = 0
        dstotals['nfilesfailed'] = 0
        dstotals['pctfinished'] = 0
        dstotals['pctfailed'] = 0
        if (task['jeditaskid'] in dsinfo):
            nfiles = 0
            nfinished = 0
            nfailed = 0
            for ds in dsinfo[task['jeditaskid']]:
                if int(ds['nfiles']) > 0:
                    nfiles += int(ds['nfiles'])
                    nfinished += int(ds['nfilesfinished'])
                    nfailed += int(ds['nfilesfailed'])
            if nfiles > 0:
                dstotals = {}
                dstotals['nfiles'] = nfiles
                dstotals['nfilesfinished'] = nfinished
                dstotals['nfilesfailed'] = nfailed
                dstotals['pctfinished'] = int(100. * nfinished / nfiles)
                dstotals['pctfailed'] = int(100. * nfailed / nfiles)

        task['dsinfo'] = dstotals

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
        if sortby == 'time-ascending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'])
        if sortby == 'time-descending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'], reverse=True)
        if sortby == 'statetime-descending':
            tasks = sorted(tasks, key=lambda x: x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            tasks = sorted(tasks, key=lambda x: x['taskpriority'], reverse=True)
        elif sortby == 'nfiles':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['nfiles'], reverse=True)
        elif sortby == 'pctfinished':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfinished'], reverse=True)
        elif sortby == 'pctfailed':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfailed'], reverse=True)
        elif sortby == 'taskname':
            tasks = sorted(tasks, key=lambda x: x['taskname'])
        elif sortby == 'jeditaskid' or sortby == 'taskid':
            tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])
        elif sortby == 'cloud':
            tasks = sorted(tasks, key=lambda x: x['cloud'], reverse=True)

    else:
        sortby = "jeditaskid"
        tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])

    return tasks


def jobSummaryDict(request, jobs, fieldlist=None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    if fieldlist:
        flist = fieldlist
    else:
        flist = standard_fields
    for job in jobs:
        for f in flist:
            if f == 'actualcorecount' and job[f] is None: job[f] = 1
            if f == 'pilotversion':
                if 'pilotid' in job and '|' in job['pilotid']:
                    job[f] = job['pilotid'].split('|')[-1]
                else:
                    job[f] = 'Not specified'
            if f == 'schedulerid':
                if 'schedulerid' in job and job[f] is not None:
                    if 'harvester' in job[f]:
                        job[f] = job[f].replace('harvester-','')
                    #del job[f]
                    else: del job[f]

            if f in job and job[f]:
                if f == 'taskid' and int(job[f]) < 1000000 and 'produsername' not in request.session[
                    'requestParams']: continue
                if f == 'nucleus' and job[f] is None: continue
                if f == 'specialhandling':
                    if not 'specialhandling' in sumd: sumd['specialhandling'] = {}
                    shl = job['specialhandling'].split()
                    for v in shl:
                        if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                        sumd['specialhandling'][v] += 1
                else:
                    if not f in sumd: sumd[f] = {}
                    if not job[f] in sumd[f]: sumd[f][job[f]] = 0
                    sumd[f][job[f]] += 1
        for extra in ('jobmode', 'substate', 'outputfiletype', 'durationmin'):
            if extra in job:
                if not extra in sumd: sumd[extra] = {}
                if not job[extra] in sumd[extra]: sumd[extra][job[extra]] = 0
                sumd[extra][job[extra]] += 1
    if 'schedulerid' in sumd:
        sumd['harvesterinstance'] = sumd['schedulerid']
        del sumd['schedulerid']
    ## event service
    esjobdict = {}
    esjobs = []
    for job in jobs:
        if isEventService(job):
            esjobs.append(job['pandaid'])
            # esjobdict[job['pandaid']] = {}
            # for s in eventservicestatelist:
            #    esjobdict[job['pandaid']][s] = 0
    if len(esjobs) > 0:
        sumd['eventservicestatus'] = {}

        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
        else:
            tmpTableName = "TMP_IDS1"

        transactionKey = random.randrange(1000000)

 #       connection.enter_transaction_management()
        new_cur = connection.cursor()
        executionData = []
        for id in esjobs:
            executionData.append((id, transactionKey))
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, executionData)
 #       connection.commit()

        new_cur.execute("SELECT STATUS, COUNT(STATUS) AS COUNTSTAT FROM (SELECT /*+ dynamic_sampling(TMP_IDS1 0) cardinality(TMP_IDS1 10) INDEX_RS_ASC(ev JEDI_EVENTS_PANDAID_STATUS_IDX) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */ PANDAID, STATUS FROM ATLAS_PANDA.JEDI_EVENTS ev, %s WHERE TRANSACTIONKEY = %i AND  PANDAID = ID) t1 GROUP BY STATUS" % (tmpTableName, transactionKey))

        evtable = dictfetchall(new_cur)
        new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
#        connection.commit()
 #       connection.leave_transaction_management()

        for ev in evtable:
            evstat = eventservicestatelist[ev['STATUS']]
            sumd['eventservicestatus'][evstat] = ev['COUNTSTAT']
            # for ev in evtable:
            #    evstat = eventservicestatelist[ev['STATUS']]
            #    if evstat not in sumd['eventservicestatus']:
            #        sumd['eventservicestatus'][evstat] = 0
            #    sumd['eventservicestatus'][evstat] += 1
            #    #esjobdict[ev['PANDAID']][evstat] += 1

    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = list(sumd[f].keys())
        if f == 'minramcount':
            newvalues = {}
            for ky in kys:
                roundedval = int(ky / 1000)
                if roundedval in newvalues:
                    newvalues[roundedval] += sumd[f][ky]
                else:
                    newvalues[roundedval] = sumd[f][ky]
            for ky in newvalues:
                iteml.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
            iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())
        elif f == 'durationmin':
            if len(kys) == 1 and kys[0] == 0:
                iteml.append({'kname': '0-0', 'kvalue': sumd[f][0]})
            else:
                nbinsmax = 20
                minstep = 10
                rangebounds = []
                if min(kys) == 0:
                    iteml.append({'kname': '0-0', 'kvalue': sumd[f][0]})
                    dstep = minstep if (max(kys)-min(kys)+1)/nbinsmax < minstep else int((max(kys)-min(kys)+1)/nbinsmax)
                    rangebounds.extend([lb for lb in range(min(kys)+1, max(kys)+dstep, dstep)])
                else:
                    dstep = minstep if (max(kys)-min(kys))/nbinsmax < minstep else int((max(kys)-min(kys))/nbinsmax)
                    rangebounds.extend([lb-1 for lb in range(min(kys), max(kys)+dstep, dstep)])
                if len(rangebounds) == 1:
                    rangebounds.append(rangebounds[0]+dstep)
                bins, ranges = np.histogram([job['durationmin'] for job in jobs if 'durationmin' in job], bins=rangebounds)
                for i, bin in enumerate(bins):
                    iteml.append({'kname': str(ranges[i]) + '-' + str(ranges[i+1]), 'kvalue':bin})

#        elif f == 'pilotversion':
#            pilotver =
#            iteml.append({'kname': , 'kvalue':bin})

        else:
            if f in ('priorityrange', 'jobsetrange'):
                skys = []
                for k in kys:
                    skys.append({'key': k, 'val': int(k[:k.index(':')])})
                skys = sorted(skys, key=lambda x: x['val'])
                kys = []
                for sk in skys:
                    kys.append(sk['key'])
            elif f in ('attemptnr', 'jeditaskid', 'taskid','noutputdatafiles','actualcorecount','corecount'):
                kys = sorted(kys, key=lambda x: int(x))
            else:
                kys = sorted(kys)
            for ky in kys:
                iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
            if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
                iteml = sorted(iteml, key=lambda x: x['kvalue'], reverse=True)
            elif f not in ('priorityrange', 'jobsetrange', 'attemptnr', 'jeditaskid', 'taskid','noutputdatafiles','actualcorecount'):
                iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())

        itemd['list'] = iteml
        suml.append(itemd)
        suml = sorted(suml, key=lambda x: x['field'])
    return suml, esjobdict


def siteSummaryDict(sites):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    sumd['category'] = {}
    sumd['category']['test'] = 0
    sumd['category']['production'] = 0
    sumd['category']['analysis'] = 0
    sumd['category']['multicloud'] = 0
    for site in sites:
        for f in standard_sitefields:
            if f in site:
                if not f in sumd: sumd[f] = {}
                if not site[f] in sumd[f]: sumd[f][site[f]] = 0
                sumd[f][site[f]] += 1
        isProd = True
        if site['siteid'].find('ANALY') >= 0:
            isProd = False
            sumd['category']['analysis'] += 1
        if site['siteid'].lower().find('test') >= 0:
            isProd = False
            sumd['category']['test'] += 1
        if (site['multicloud'] is not None) and (site['multicloud'] != 'None') and (
        re.match('[A-Z]+', site['multicloud'])):
            sumd['category']['multicloud'] += 1
        if isProd: sumd['category']['production'] += 1
    if VOMODE != 'atlas': del sumd['cloud']
    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()

        kys = sorted(kys, key=lambda x: (x is None, x))

        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def userSummaryDict(jobs):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    for job in jobs:
        if 'produsername' in job and job['produsername'] != None:
            user = job['produsername'].lower()
        else:
            user = 'Unknown'
        if not user in sumd:
            sumd[user] = {}
            for state in statelist:
                sumd[user][state] = 0
            sumd[user]['name'] = job['produsername']
            sumd[user]['cputime'] = 0
            sumd[user]['njobs'] = 0
            for state in statelist:
                sumd[user]['n' + state] = 0
            sumd[user]['nsites'] = 0
            sumd[user]['sites'] = {}
            sumd[user]['nclouds'] = 0
            sumd[user]['clouds'] = {}
            sumd[user]['nqueued'] = 0
            sumd[user]['latest'] = timezone.now() - timedelta(hours=2400)
            sumd[user]['pandaid'] = 0
        cloud = job['cloud']
        site = job['computingsite']
        cpu = float(job['cpuconsumptiontime']) / 1.
        state = job['jobstatus']
        if job['modificationtime'] > sumd[user]['latest']: sumd[user]['latest'] = job['modificationtime']
        if job['pandaid'] > sumd[user]['pandaid']: sumd[user]['pandaid'] = job['pandaid']
        sumd[user]['cputime'] += cpu
        sumd[user]['njobs'] += 1
        if 'n%s' % (state) not in sumd[user]:
            sumd[user]['n' + state] = 0
        sumd[user]['n' + state] += 1
        if not site in sumd[user]['sites']: sumd[user]['sites'][site] = 0
        sumd[user]['sites'][site] += 1
        if not cloud in sumd[user]['clouds']: sumd[user]['clouds'][cloud] = 0
        sumd[user]['clouds'][cloud] += 1
    for user in sumd:
        sumd[user]['nsites'] = len(sumd[user]['sites'])
        sumd[user]['nclouds'] = len(sumd[user]['clouds'])
        sumd[user]['nqueued'] = sumd[user]['ndefined'] + sumd[user]['nwaiting'] + sumd[user]['nassigned'] + sumd[user][
            'nactivated']
        sumd[user]['cputime'] = "%d" % float(sumd[user]['cputime'])
    ## convert to list ordered by username
    ukeys = list(sumd.keys())
    ukeys = sorted(ukeys)
    suml = []
    for u in ukeys:
        uitem = {}
        uitem['name'] = u
        uitem['latest'] = sumd[u]['pandaid']
        uitem['dict'] = sumd[u]
        suml.append(uitem)
    suml = sorted(suml, key=lambda x: -x['latest'])
    return suml


def taskSummaryDict(request, tasks, fieldlist=None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    logger = logging.getLogger('bigpandamon-error')

    if fieldlist:
        flist = fieldlist
    else:
        flist = copy.deepcopy(standard_taskfields)

    for task in tasks:
        for f in flist:
            if 'tasktype' in request.session['requestParams'] and request.session['requestParams'][
                'tasktype'].startswith('analy'):
                ## Remove the noisy useless parameters in analysis listings
                if flist in ('reqid', 'stream', 'tag'): continue

            if 'taskname' in task and len(task['taskname'].split('.')) == 5:
                if f == 'project':
                    try:
                        if not f in sumd: sumd[f] = {}
                        project = task['taskname'].split('.')[0]
                        if not project in sumd[f]: sumd[f][project] = 0
                        sumd[f][project] += 1
                    except:
                        pass
                if f == 'stream':
                    try:
                        if not f in sumd: sumd[f] = {}
                        stream = task['taskname'].split('.')[2]
                        if not re.match('[0-9]+', stream):
                            if not stream in sumd[f]: sumd[f][stream] = 0
                            sumd[f][stream] += 1
                    except:
                        pass
                if f == 'tag':
                    try:
                        if not f in sumd: sumd[f] = {}
                        tags = task['taskname'].split('.')[4]
                        if not tags.startswith('job_'):
                            tagl = tags.split('_')
                            tag = tagl[-1]
                            if not tag in sumd[f]: sumd[f][tag] = 0
                            sumd[f][tag] += 1



                            #                            for tag in tagl:
                            #                                if not tag in sumd[f]: sumd[f][tag] = 0
                            #                                sumd[f][tag] += 1
                    except:
                        pass
            if f in task and task[f]:
                val = task[f]
                # if val == 'anal': val = 'analy'
                if not f in sumd: sumd[f] = {}
                if not val in sumd[f]: sumd[f][val] = 0
                sumd[f][val] += 1
    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        kys =sorted(kys)
        if f != 'ramcount':
            for ky in kys:
                iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
            iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())
        else:
            newvalues = {}
            for ky in kys:
                roundedval = int(ky / 1000)
                if roundedval in newvalues:
                    newvalues[roundedval] += sumd[f][ky]
                else:
                    newvalues[roundedval] = sumd[f][ky]
            for ky in newvalues:
                iteml.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
            iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def wgTaskSummary(request, fieldname='workinggroup', view='production', taskdays=3):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    query = {}
    hours = 24 * taskdays
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query['modificationtime__castdate__range'] = [startdate, enddate]
    if fieldname == 'workinggroup': query['workinggroup__isnull'] = False
    if view == 'production':
        query['tasktype'] = 'prod'
    elif view == 'analysis':
        query['tasktype'] = 'anal'

    if 'processingtype' in request.session['requestParams']:
        query['processingtype'] = request.session['requestParams']['processingtype']

    if 'workinggroup' in request.session['requestParams']:
        query['workinggroup'] = request.session['requestParams']['workinggroup']

    if 'project' in request.session['requestParams']:
        query['taskname__istartswith'] = request.session['requestParams']['project']

    summary = JediTasks.objects.filter(**query).values(fieldname, 'status').annotate(Count('status')).order_by(
        fieldname, 'status')
    totstates = {}
    tottasks = 0
    wgsum = {}
    for state in taskstatelist:
        totstates[state] = 0
    for rec in summary:
        wg = rec[fieldname]
        status = rec['status']
        count = rec['status__count']
        if status not in taskstatelist: continue
        tottasks += count
        totstates[status] += count
        if wg not in wgsum:
            wgsum[wg] = {}
            wgsum[wg]['name'] = wg
            wgsum[wg]['count'] = 0
            wgsum[wg]['states'] = {}
            wgsum[wg]['statelist'] = []
            for state in taskstatelist:
                wgsum[wg]['states'][state] = {}
                wgsum[wg]['states'][state]['name'] = state
                wgsum[wg]['states'][state]['count'] = 0
        wgsum[wg]['count'] += count
        wgsum[wg]['states'][status]['count'] += count

    ## convert to ordered lists
    suml = []
    for f in wgsum:
        itemd = {}
        itemd['field'] = f
        itemd['count'] = wgsum[f]['count']
        kys = taskstatelist
        iteml = []
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': wgsum[f]['states'][ky]['count']})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def extensibleURL(request, xurl=''):
    """ Return a URL that is ready for p=v query extension(s) to be appended """
    if xurl == '': xurl = request.get_full_path()
    if xurl.endswith('/'):
        if 'tag' or '/job/' or '/task/' in xurl:
            xurl = xurl[0:len(xurl)]
        else:
            xurl = xurl[0:len(xurl) - 1]

    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    # if 'jobtype' in requestParams:
    #    xurl += "jobtype=%s&" % requestParams['jobtype']
    return xurl

def mainPage(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request)

    debuginfo = None
    if request.session['debug']:
        debuginfo = "<h2>Debug info</h2>"
        from django.conf import settings
        for name in dir(settings):
            debuginfo += "%s = %s<br>" % (name, getattr(settings, name))
        debuginfo += "<br>******* Environment<br>"
        for env in os.environ:
            debuginfo += "%s = %s<br>" % (env, os.environ[env])
    #TODO It should be removed in the future
    hostname = "dashb-atlas-job.cern.ch"
    port = "80"
    try:
        import socket
        host = socket.gethostbyname(hostname)
        s = socket.create_connection((host, port), 2)
        if(s):
            old_monitoring = 1
    except Exception as e:
        old_monitoring = 0



    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'debuginfo': debuginfo,
            'built': datetime.now().strftime("%H:%M:%S"),
            'old_monitoring': old_monitoring,
        }
        data.update(getContextVariables(request))
        response = render_to_response('core-mainPage.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif (('HTTP_ACCEPT' in request.META) and request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json')) or (
        'json' in request.session['requestParams']):
        return HttpResponse('json', content_type='text/html')
    else:
        return HttpResponse('not understood', content_type='text/html')


def helpPage(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request)
    del request.session['TFIRST']
    del request.session['TLAST']
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('completeHelp.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        return HttpResponse('json', content_type='text/html')
    else:
        return HttpResponse('not understood', content_type='text/html')


def errorInfo(job, nchars=300, mode='html'):
    errtxt = ''
    err1 = ''
    desc, codesDescribed = getErrorDescription(job, provideProcessedCodes=True)

    if int(job['brokerageerrorcode']) != 0 and int(job['brokerageerrorcode']) not in codesDescribed:
        errtxt += 'Brokerage error %s: %s <br>' % (job['brokerageerrorcode'], job['brokerageerrordiag'])
        if err1 == '': err1 = "Broker: %s" % job['brokerageerrordiag']
    if int(job['ddmerrorcode']) != 0 and int(job['ddmerrorcode']) not in codesDescribed:
        errtxt += 'DDM error %s: %s <br>' % (job['ddmerrorcode'], job['ddmerrordiag'])
        if err1 == '': err1 = "DDM: %s" % job['ddmerrordiag']
    if int(job['exeerrorcode']) != 0 and int(job['exeerrorcode']) not in codesDescribed:
        errtxt += 'Executable error %s: %s <br>' % (job['exeerrorcode'], job['exeerrordiag'])
        if err1 == '': err1 = "Exe: %s" % job['exeerrordiag']
    if int(job['jobdispatchererrorcode']) != 0 and int(job['jobdispatchererrorcode']) not in codesDescribed:
        errtxt += 'Dispatcher error %s: %s <br>' % (job['jobdispatchererrorcode'], job['jobdispatchererrordiag'])
        if err1 == '': err1 = "Dispatcher: %s" % job['jobdispatchererrordiag']
    if int(job['piloterrorcode']) != 0 and int(job['piloterrorcode']) not in codesDescribed:
        errtxt += 'Pilot error %s: %s <br>' % (job['piloterrorcode'], job['piloterrordiag'])
        if err1 == '': err1 = "Pilot: %s" % job['piloterrordiag']
    if int(job['superrorcode']) != 0 and int(job['superrorcode']) not in codesDescribed:
        errtxt += 'Sup error %s: %s <br>' % (job['superrorcode'], job['superrordiag'])
        if err1 == '': err1 = job['superrordiag']
    if int(job['taskbuffererrorcode']) != 0 and int(job['taskbuffererrorcode']) not in codesDescribed:
        errtxt += 'Task buffer error %s: %s <br>' % (job['taskbuffererrorcode'], job['taskbuffererrordiag'])
        if err1 == '': err1 = 'Taskbuffer: %s' % job['taskbuffererrordiag']
    if job['transexitcode'] != '' and job['transexitcode'] is not None and int(job['transexitcode']) > 0 and int(job['transexitcode']) not in codesDescribed:
        errtxt += 'Trf exit code %s.' % job['transexitcode']
        if err1 == '': err1 = 'Trf exit code %s' % job['transexitcode']
    if len(desc) > 0:
        errtxt += '%s<br>' % desc
        if err1 == '': err1 = getErrorDescription(job, mode='string')
    if len(errtxt) > nchars:
        ret = errtxt[:nchars] + '...'
    else:
        ret = errtxt[:nchars]
    if err1.find('lost heartbeat') >= 0: err1 = 'lost heartbeat'
    if err1.lower().find('unknown transexitcode') >= 0: err1 = 'unknown transexit'
    if err1.find(' at ') >= 0: err1 = err1[:err1.find(' at ') - 1]
    if errtxt.find('lost heartbeat') >= 0: err1 = 'lost heartbeat'
    err1 = err1.replace('\n', ' ')
    if mode == 'html':
        return errtxt
    else:
        return err1[:nchars]


def jobParamList(request):
    idlist = []
    if 'pandaid' in request.session['requestParams']:
        idstring = request.session['requestParams']['pandaid']
        idstringl = idstring.split(',')
        for id in idstringl:
            idlist.append(int(id))
    query = {}
    query['pandaid__in'] = idlist
    jobparams = Jobparamstable.objects.filter(**query).values()
    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        return HttpResponse(json.dumps(jobparams, cls=DateEncoder), content_type='application/json')
    else:
        return HttpResponse('not supported', content_type='text/html')


def jobSummaryDictProto(request, cutsummary, requestToken):
    esjobdict = []
    sqlRequest = "SELECT ATTR, ATTR_VALUE, NUM_OCCUR FROM ATLAS_PANDABIGMON.JOBSPAGE_CUMULATIVE_RESULT WHERE " \
                 "REQUEST_TOKEN=%s AND ATTR_VALUE <> 'END' ORDER BY ATTR, ATTR_VALUE;)" % str(requestToken)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    rawsummary = cur.fetchall()
    cur.close()

    errsByCount = []
    summaryhash = {}
    for row in rawsummary:
        if row[0] in summaryhash:
            if row[1] in summaryhash[row[0]]:
                summaryhash[row[0]][row[1]] += row[2]
            else:
                summaryhash[row[0]][row[1]] = row[2]
        else:
            item = {}
            item[row[1]] = row[2]
            summaryhash[row[0]] = item
    # second checkpoint

    shkeys = list(summaryhash.keys())
    sumd = []
    jobsToList = set()
    njobs = 0
    for shkey in shkeys:
        if shkey != 'PANDAID' and shkey != 'ErrorCode' and shkey != 'MINRAMCOUNT':
            # check this condition
            entry = {}
            entry['field'] = shkey
            entrlist = []

            if (cutsummary):
                cutlen = 5
            else:
                cutlen = len(summaryhash[shkey].keys())

            for subshkey in summaryhash[shkey].keys()[0:cutlen]:
                subentry = {}
                subentry['kname'] = subshkey
                subentry['kvalue'] = summaryhash[shkey][subshkey]
                if (shkey == 'COMPUTINGSITE'):
                    njobs += summaryhash[shkey][subshkey]
                entrlist.append(subentry)
            entry['list'] = entrlist
            sumd.append(entry)
        elif shkey == 'PANDAID':
            for subshkey in summaryhash[shkey]:
                jobsToList.add(subshkey)

        elif shkey == 'MINRAMCOUNT':
            entry = {}
            entry['field'] = shkey
            entrlist = []
            newvalues = {}

            for subshkey in summaryhash[shkey].keys():
                roundedval = int(subshkey / 1000)
                if roundedval in newvalues:
                    newvalues[roundedval] += summaryhash[shkey][subshkey]
                else:
                    newvalues[roundedval] = summaryhash[shkey][subshkey]
            for ky in newvalues:
                entrlist.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
            entrlist = sorted(entrlist, key=lambda x: str(x['kname']).lower())
            entry['list'] = entrlist
            sumd.append(entry)

        elif shkey == 'ErrorCode':
            for subshkey in summaryhash[shkey]:
                errval = {}
                errval['codename'] = subshkey.split(':')[0]
                errval['codeval'] = subshkey.split(':')[1]
                errval['count'] = summaryhash[shkey][subshkey]
                errval['error'] = subshkey
                error = [it['error'] for it in errorcodelist if it['name'] == errval['codename'].lower()]
                if len(error) > 0 and error[0] in errorCodes and int(errval['codeval']) in errorCodes[error[0]]:
                    errval['diag'] = errorCodes[error[0]][int(errval['codeval'])]
                errsByCount.append(errval)

    return sumd, esjobdict, jobsToList, njobs, errsByCount


def postpone(function):
    def decorator(*args, **kwargs):
        t = Thread(target=function, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
    return decorator


@postpone
def startDataRetrieve(request, dropmode, query, requestToken, wildCardExtension):

    plsql = "BEGIN ATLAS_PANDABIGMON.QUERY_JOBSPAGE_CUMULATIVE("
    plsql += " REQUEST_TOKEN=>"+str(requestToken)+", "
    requestFields = {}

    a = datetime.strptime(query['modificationtime__castdate__range'][0], defaultDatetimeFormat)
    b = datetime.strptime(query['modificationtime__castdate__range'][1], defaultDatetimeFormat)
    delta = b - a
    range = delta.days+delta.seconds/86400.0


    #if (range == 180.0):
    #    plsql += " RANGE_DAYS=>null, "
    #else:
    #    plsql += " RANGE_DAYS=>"+str(range)+", "

    for item in request.GET:
        requestFields[item.lower()] = request.GET[item]

    if (('jeditaskid' in requestFields) and range == 180.0): #This is a temporary patch to avoid absence of pandaids
        plsql += " RANGE_DAYS=>null, "
    else:
        plsql += " RANGE_DAYS=>" + str(range) + ", "

    if ('priorityrange' in requestFields): #This is a temporary patch to avoid absence of pandaids
        plsql += " PRIORITYRANGE=>'"+escapeInput(requestFields['priorityrange'])+"', "


    if not dropmode:
        plsql += " WITH_RETRIALS=>'Y', "
    else:
        plsql += " WITH_RETRIALS=>'N', "

    if ('noenddate' in request.session and request.session['noenddate'] == False):
        plsql += " END_DATE=>'"+str(b.date().strftime('%d-%m-%Y'))+"', "

    if ('pandaid' in requestFields):
        plsql += " PANDAID=>("
        pandaIdRequest = requestFields['pandaid'].split(',')
        for pandaID in pandaIdRequest:
            try:
                pandaID = int(pandaID)
                plsql += str(pandaID) + ','
            except:
                pass # it is better to add here wrong data handler
        plsql = plsql[:-1] +'), '
    # if ('computingsite' in requestFields):
    #     plsql += "COMPUTINGSITE=>("




    for item in standard_fields+['corecount','noutputdatafiles','actualcorecount']:
        if ((item + '__in') in query):
            plsql += " " + item.upper() + "=>'" + str(query[item+'__in'][0]) + "', "
        if ((item + '__endswith') in query and item=='transformation'):
            plsql += " " + item.upper() + "=>'" + str(query[item+'__endswith']) + "', "
        elif (item in query):
            plsql += " " + item.upper() + "=>'" + str(query[item]) + "', "
        elif (((item + '__range') in query) and (item == 'minramcount')):
            plsql += " " + item.upper() + "=>'" + str(query[item + '__range']) + "', "
        else:
            pos = wildCardExtension.find(item, 0)
            if pos > 0:
                firstc = wildCardExtension.find("'", pos) + 1
                sec = wildCardExtension.find("'", firstc)
                value = wildCardExtension[firstc: sec]
                if '%%' in value:
                    value = value.replace('%%','*')
                plsql += " "+item.upper()+"=>'"+value+"', "
    plsql = plsql[:-2]
    plsql += "); END;;"
    print (plsql)
    # Here we call stored proc to fill temporary data
    cursor = connection.cursor()
    countCalls = 0
    while (countCalls < 3):
        try:
            cursor.execute(plsql)
            countCalls += 1
        except Exception as ex:
            print (ex)
            if ex[0].code == 8103:
                pass
            else:
                break
    cursor.close()


# plsql = """BEGIN ATLAS_PANDABIGMON.QUERY_JOBSPAGE_CUMULATIVE(:REQUEST_TOKEN, :RANGE_DAYS); END;;"""
# cursor.execute(plsql, {'REQUEST_TOKEN': 54, 'RANGE_DAYS': 1})



def jobListP(request, mode=None, param=None):
    valid, response = initRequest(request)
  #  initSelfMonitor(request)
    #if 'JOB_LIMIT' in request.session:
    #    del request.session['JOB_LIMIT']
    # Hack to void limit caption in the params label
    request.session['requestParams']['limit'] = 10000000
    #is_json = False
    # Here We start Retreiving Summary and return almost empty template
    if ('requesttoken' in request.session):
        print ('Existing')
    # Get request token. This sheme of getting tokens should be more sophisticated (at least not use sequential numbers)
    requestToken = 0

    if len(list(request.GET.values())) == 0:
        requestToken = -1
    elif len(list(request.GET.values())) == 1 and 'json' in request.GET:
        requestToken = -1
    else:
        sqlRequest = "SELECT ATLAS_PANDABIGMON.PANDAMON_REQUEST_TOKEN_SEQ.NEXTVAL as my_req_token FROM dual;"
        cur = connection.cursor()
        cur.execute(sqlRequest)
        requestToken = cur.fetchall()
        cur.close()
        requestToken = requestToken[0][0]

    if (requestToken == 0):
        print ("Error in getting reuest token")
        return

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'drop': dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'nodrop': dropmode = False

    requestFields = {}
    for item in request.GET:
        requestFields[item.lower()] = request.GET[item]

    if not (requestToken == -1):
        startDataRetrieve(request, dropmode, query, requestToken, wildCardExtension)

    #request.session['viewParams']['selection'] = request.session['viewParams']['selection'][:request.session['viewParams']['selection'].index('<b>limit=</b>')]
    if 'json' not in request.session['requestParams']:
        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'requesttoken': requestToken,
            'tfirst': request.session['TFIRST'].strftime(defaultDatetimeFormat),
            'tlast': request.session['TLAST'].strftime(defaultDatetimeFormat),
            'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        del request.session['TFIRST']
        del request.session['TLAST']
        response = render_to_response('jobListWrapper.html', data, content_type='text/html')
        return response
    else:
        data = getJobList(request,requestToken)
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response






def getJobList(request,requesttoken=None):
    rawsummary={}
    newpandaIDVal = {}
    if 'requestParams' in request.session and u'display_limit' in request.session['requestParams']:
        display_limit = int(request.session['requestParams']['display_limit'])
        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    else:
        display_limit = 100
        url_nolimit = request.get_full_path()
    njobsmax = display_limit
    cur = connection.cursor()
    if 'requesttoken' in request.GET:
        sqlRequest = "SELECT * FROM ATLAS_PANDABIGMON.JOBSPAGE_CUMULATIVE_RESULT WHERE REQUEST_TOKEN=%s" % request.GET[
        'requesttoken']
        cur.execute(sqlRequest)
        rawsummary = cur.fetchall()
        # if 'requesttoken' not in request.session:
        #     request.session['requesttoken'] = request.REQUEST[
        #     'requesttoken']
    else:
        sqlRequest = "SELECT * FROM ATLAS_PANDABIGMON.JOBSPAGE_CUMULATIVE_RESULT WHERE REQUEST_TOKEN=%s" % int(requesttoken)
        cur.execute(sqlRequest)
        rawsummary = cur.fetchall()
        time.sleep(10)
        njobsmax = 1000000000
        # if 'requesttoken' not in request.session:
        #     request.session['requesttoken'] = requesttoken
    cur.close()

    #if 'requesttoken' not in request.GET:
    #    return HttpResponse('')

    errsByCount = []
    summaryhash = {}
    doRefresh = True
    for row in rawsummary:
        if row[2] == 'END':
            doRefresh = False
        else:
            if row[1] in summaryhash:
                if row[1] in summaryhash[row[1]]:
                    summaryhash[row[1]][row[2]] += row[3]
                else:
                    summaryhash[row[1]][row[2]] = row[3]
            else:
                item = {}
                item[row[2]] = row[3]
                summaryhash[row[1]] = item

    shkeys = sorted(summaryhash.keys())
    sumd = []
    jobsToList = set()
    njobs = 0
    for shkey in shkeys:
        if not shkey in ['PANDAID', 'ErrorCode', 'MINRAMCOUNT','PRIORITYRANGE','CORECOUNT','NOUTPUTDATAFILES','ACTUALCORECOUNT']:
            # check this condition
            entry = {}
            entry['field'] = shkey
            entrlist = []
            for subshkey in summaryhash[shkey].keys():
                subentry = {}
                subentry['kname'] = subshkey
                subentry['kvalue'] = summaryhash[shkey][subshkey]
                if (shkey == 'JOBSTATUS'):
                    njobs += summaryhash[shkey][subshkey]
                entrlist.append(subentry)
            entry['list'] = entrlist
            sumd.append(entry)
        elif shkey == 'PRIORITYRANGE':
            entry = {}
            entry['field'] = shkey
            entrlist = []
            sd = summaryhash[shkey].keys()
            skys = []
            for k in sd:
                skys.append({'key': k, 'val': int(k[:k.index(':')])})
            skys = sorted(skys, key=lambda x: x['val'])
            for sk in skys:
                subentry = {}
                subentry['kname'] = sk['key']
                subentry['kvalue'] = summaryhash[shkey][sk['key']]
                entrlist.append(subentry)
            entry['list'] = entrlist
            sumd.append(entry)
        elif shkey == 'MINRAMCOUNT':
            entry = {}
            entry['field'] = shkey
            entrlist = []
            newvalues = {}

            for subshkey in summaryhash[shkey].keys():
                roundedval = int( int(subshkey) / 1000)
                if roundedval in newvalues:
                    newvalues[roundedval] += summaryhash[shkey][subshkey]
                else:
                    newvalues[roundedval] = summaryhash[shkey][subshkey]
            for ky in newvalues:
                entrlist.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
            entrlist = sorted(entrlist, key=lambda x: str(x['kname']).lower())
            entry['list'] = entrlist
            sumd.append(entry)


        elif shkey == 'PANDAID':
            for subshkey in summaryhash[shkey]:
                jobsToList.add(subshkey)

        elif shkey == 'ErrorCode':
            for subshkey in summaryhash[shkey]:
                errval = {}
                errval['codename'] = subshkey.split(':')[0]
                errval['codeval'] = subshkey.split(':')[1]
                errval['count'] = summaryhash[shkey][subshkey]
                errval['error'] = subshkey
                error = [it['error'] for it in errorcodelist if it['name'] == errval['codename'].lower()]
                if len(error) > 0 and error[0] in errorCodes and int(errval['codeval']) in errorCodes[error[0]]:
                    errval['diag'] = errorCodes[error[0]][int(errval['codeval'])]
                errsByCount.append(errval)
        elif shkey in ['CORECOUNT','NOUTPUTDATAFILES','ACTUALCORECOUNT']:
            entry = {}
            entry['field'] = shkey
            entrlist = []
            for subshkey in sorted(summaryhash[shkey], key = int):
                entrlist.append({'kname': subshkey, 'kvalue': summaryhash[shkey][subshkey]})
            entry['list'] = entrlist
            sumd.append(entry)

    if sumd:
        for item in sumd:
            if item['field'] == 'JEDITASKID':
                item['list'] = sorted(item['list'], key=lambda k: k['kvalue'], reverse=True)

    jobs = []

    if not doRefresh:
        print (len(jobsToList))
        pandaIDVal = [int(val) for val in jobsToList]

        newpandaIDVal = importToken(request,pandaIDVal)
        pandaIDVal = pandaIDVal[:njobsmax]
        newquery = {}
        newquery['pandaid__in'] = pandaIDVal

        eventservice = False
        if 'requestParams' in request.session and 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
            eventservice = True
        if 'requestParams' in request.session and 'eventservice' in request.session['requestParams'] and (
                        request.session['requestParams']['eventservice'] == 'eventservice' or
                        request.session['requestParams'][
                            'eventservice'] == '1'):
            eventservice = True
        if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
                    'requestParams' in request.session and 'json' in request.session['requestParams']):
            values = [f.name for f in Jobsactive4._meta.get_fields()]
        elif eventservice:
            values = 'jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock', 'jobmetrics', 'reqid', 'minramcount', 'statechangetime', 'jobsubstatus', 'eventservice','gshare','noutputdatafiles','actualcorecount'
        else:
            values = 'jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss', 'maxrss', 'nucleus', 'eventservice','gshare','noutputdatafiles','actualcorecount'

        jobs.extend(Jobsdefined4.objects.filter(**newquery).values(*values))
        jobs.extend(Jobsactive4.objects.filter(**newquery).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**newquery).values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**newquery).values(*values))
        if (len(jobs) < njobsmax):
            jobs.extend(Jobsarchived.objects.filter(**newquery).values(*values))


    print (len(jobs))
    if 'requestParams' in request.session and 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
        if sortby == 'time-ascending':
            jobs = sorted(jobs, key=lambda x: x['modificationtime'])
        if sortby == 'time-descending':
            jobs = sorted(jobs, key=lambda x: x['modificationtime'], reverse=True)
        if sortby == 'statetime':
            jobs = sorted(jobs, key=lambda x: x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            jobs = sorted(jobs, key=lambda x: x['currentpriority'], reverse=True)
        elif sortby == 'attemptnr':
            jobs = sorted(jobs, key=lambda x: x['attemptnr'], reverse=True)
        elif sortby == 'duration-ascending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'duration-descending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'], reverse=True)
        elif sortby == 'duration':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'PandaID':
            jobs = sorted(jobs, key=lambda x: x['pandaid'], reverse=True)
    else:
        sortby = "time-descending"
        if len(jobs) > 0 and 'modificationtime' in jobs[0]:
            jobs = sorted(jobs, key=lambda x: x['modificationtime'], reverse=True)




    ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
    taskids = {}

    for job in jobs:
        if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1

    droplist = []
    droppedIDs = set()
    droppedPmerge = set()

    jobs = cleanJobListLite(request, jobs)

    print (len(jobs))
    jobtype = ''
    if 'requestParams' in request.session and 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'


    if 'requestParams' in request.session and 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
        if sortby == 'time-ascending':
            jobs = sorted(jobs, key=lambda x: x['modificationtime'])
        if sortby == 'time-descending':
            jobs = sorted(jobs, key=lambda x: x['modificationtime'], reverse=True)
        if sortby == 'statetime':
            jobs = sorted(jobs, key=lambda x: x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            jobs = sorted(jobs, key=lambda x: x['currentpriority'], reverse=True)
        elif sortby == 'attemptnr':
            jobs = sorted(jobs, key=lambda x: x['attemptnr'], reverse=True)
        elif sortby == 'duration-ascending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'duration-descending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'], reverse=True)
        elif sortby == 'duration':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'PandaID':
            jobs = sorted(jobs, key=lambda x: x['pandaid'], reverse=True)
    else:
        sortby = "time-descending"
        if len(jobs) > 0 and 'modificationtime' in jobs[0]:
            jobs = sorted(jobs, key=lambda x: x['modificationtime'], reverse=True)

    taskname = ''
    if 'requestParams' in request.session and 'jeditaskid' in request.session['requestParams']:
        taskname = getTaskName('jeditaskid', request.session['requestParams']['jeditaskid'])
    if 'requestParams' in request.session and 'taskid' in request.session['requestParams']:
        taskname = getTaskName('jeditaskid', request.session['requestParams']['taskid'])

    if 'requestParams' in request.session and 'produsername' in request.session['requestParams']:
        user = request.session['requestParams']['produsername']
    elif 'requestParams' in request.session and 'user' in request.session['requestParams']:
        user = request.session['requestParams']['user']
    else:
        user = None

    ## set up google flow diagram
    flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)

    # show warning or not
    showwarn = 0
    if 'JOB_LIMIT' in request.session:
        if njobs <= request.session['JOB_LIMIT']:
            showwarn = 0
        else:
            showwarn = 1

    jobsToShow = jobs[:njobsmax]

    for job in jobsToShow:
        if job['creationtime']:
            job['creationtime'] = job['creationtime'].strftime(defaultDatetimeFormat)
        if job['modificationtime']:
            job['modificationtime'] = job['modificationtime'].strftime(defaultDatetimeFormat)
        if job['statechangetime']:
            job['statechangetime'] = job['statechangetime'].strftime(defaultDatetimeFormat)

    if 'requestParams' in request.session and 'jeditaskid' in request.session['requestParams']:
        if len(jobs) > 0:
            for job in jobs:
                if 'maxvmem' in job:
                    if type(job['maxvmem']) is int and job['maxvmem'] > 0:
                        job['maxvmemmb'] = "%0.2f" % (job['maxvmem'] / 1000.)
                        job['avgvmemmb'] = "%0.2f" % (job['avgvmem'] / 1000.)
                if 'maxpss' in job:
                    if type(job['maxpss']) is int and job['maxpss'] > 0:
                        job['maxpss'] = "%0.2f" % (job['maxpss'] / 1024.)

    # errsByCount, errsBySite, errsByUser, errsByTask, errdSumd, errHist =

    if 'HTTP_REFERER' in request.META:
        xurl = extensibleURL(request, request.META['HTTP_REFERER'])
    else:
        xurl = request.META['PATH_INFO'] + "?"+ request.META['QUERY_STRING']
    print (xurl)
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    nosorturl = removeParam(nosorturl, 'display_limit', mode='extensible')

    TFIRST = None
    TLAST = None
    if 'TFIRST' in request.session:
        TFIRST = request.session['TFIRST'].strftime(defaultDatetimeFormat)
        del request.session['TFIRST']
    if 'TLAST' in request.session:
        TLAST = request.session['TLAST'].strftime(defaultDatetimeFormat)
        del request.session['TLAST']
    if 'viewParams' in request.session and 'limit' in request.session['viewParams']:
        del request.session['viewParams']['limit']
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        nodropPartURL = cleanURLFromDropPart(xurl)
    #sumd = None
    #errsByCount = None

        data = {
            'errsByCount': errsByCount,
            'newpandaIDVal': newpandaIDVal,
            #        'errdSumd': errdSumd,
            'request': request,
            'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
            'requestParams': request.session['requestParams'] if 'requestParams' in request.session else None,
            'jobList': jobsToShow[:njobsmax],
            'jobtype': jobtype,
            'njobs': njobs,
            'user': user,
            'sumd': sumd,
            'xurl': xurl,
            # 'droplist': droplist,
            # 'ndrops': len(droplist) if len(droplist) > 0 else (- len(droppedPmerge)),
            'ndrops': 0,
            'tfirst': TFIRST,
            'tlast': TLAST,
            'plow': PLOW,
            'phigh': PHIGH,
            'joblimit': request.session['JOB_LIMIT'] if 'JOB_LIMIT' in request.session else None,
            'limit': 0,
            #        'totalJobs': totalJobs,
            #        'showTop': showTop,
            'url_nolimit': url_nolimit,
            'display_limit': display_limit,
            'sortby': sortby,
            'nosorturl': nosorturl,
            'taskname': taskname,
            'flowstruct': flowstruct,
            'nodropPartURL': nodropPartURL,
            'doRefresh': doRefresh,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
    else:
        if (('fields' in request.session['requestParams']) and (len(jobs) > 0)):
            fields = request.session['requestParams']['fields'].split(',')
            fields = (set(fields) & set(jobs[0].keys()))

            for job in jobs:
                for field in list(job.keys()):
                    if field in fields:
                        pass
                    else:
                        del job[field]
        if doRefresh == True:
            data=getJobList(request, int(requesttoken))
        else:
            print (len(jobs))
            data = {
            "selectionsummary": sumd,
            "jobs": jobs,
            "errsByCount": errsByCount,
            }

    return data

def jobListPDiv(request, mode=None, param=None):
    initRequest(request, False)
    data = getCacheEntry(request, "jobListWrapper")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('jobListWrapper.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    data = getJobList(request)


    if 'requesttoken' in request.GET:
        requesttoken = request.GET['requesttoken']


    startdate = timezone.now() - timedelta(hours=2)
    enddate = timezone.now()
    query = {'qtime__range': [startdate, enddate],
             'url__contains': requesttoken,
             'urlview': '/jobssupt/',
             }

    if AllRequests.objects.filter(**query).count() > 100:
        data['doRefresh'] = False

    data.update(getContextVariables(request))
    setCacheEntry(request, "jobListWrapper", json.dumps(data, cls=DateEncoder), 60 * 20)

    #    if eventservice:
    #        response = render_to_response('jobListESProto.html', data, RequestContext(request))
    #    else:

    response = render_to_response('jobListContent.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def cache_filter(timeout):
    # This function provides splitting cache keys depending on conditions above the parameters specified in the URL
    def decorator(view_func):
        @wraps(view_func, assigned=available_attrs(view_func))
        def _wrapped_view(request, *args, **kwargs):
            is_json = False
            request._cache_update_cache = False

            # here we can apply any conditions to separate cache streams
            if ((('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) or (
                        'json' in request.GET)):
                is_json = True

            key_prefix = "%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX)

            cache_key = ucache.get_cache_key(request, key_prefix, 'GET', cache)
            if cache_key is None:
                # we should add saving result
                responce = (view_func)(request, *args, **kwargs)
                cache_key = ucache.learn_cache_key(request, responce, timeout, key_prefix, cache)
                cache.set(cache_key, responce, timeout)
                return responce
            responce = cache.get(cache_key, None)
            if responce is None:
                responce = (view_func)(request, *args, **kwargs)
                cache_key = ucache.learn_cache_key(request, responce, timeout, key_prefix, cache)
                cache.set(cache_key, responce, timeout)
            return responce
        return _wrapped_view
    return decorator

@login_customrequired
def jobList(request, mode=None, param=None):
    start_time = time.time()
    valid, response = initRequest(request)
    if not valid: return response
    dkey = digkey(request)
    thread = None
    isEventTask = False
    #Here we try to get data from cache
    data = getCacheEntry(request, "jobList")
    if data is not None:
        data = json.loads(data)
        try:
            data = deleteCacheTestData(request,data)
        except: pass
        data['request'] = request
        if data['eventservice'] == True:
            response = render_to_response('jobListES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'dump' in request.session['requestParams'] and request.session['requestParams']['dump'] == 'parameters':
        return jobParamList(request)
    eventservice = False
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        eventservice = True
    if 'eventservice' in request.session['requestParams'] and (
            request.session['requestParams']['eventservice'] == 'eventservice' or request.session['requestParams'][
        'eventservice'] == '1' or request.session['requestParams']['eventservice'] == '4' or request.session['requestParams']['eventservice'] == 'jumbo'):
        eventservice = True
    elif 'eventservice' in request.session['requestParams'] and (
        '1' in request.session['requestParams']['eventservice'] or '2' in request.session['requestParams']['eventservice'] or
        '4' in request.session['requestParams']['eventservice'] or '5' in request.session['requestParams']['eventservice']):
        eventservice = True

    noarchjobs = False
    if ('noarchjobs' in request.session['requestParams'] and request.session['requestParams']['noarchjobs'] == '1'):
        noarchjobs = True
    warning = {}
    extraquery_files = ' '
    if 'fileid' in request.session['requestParams'] or 'ecstate' in request.session['requestParams']:
        if 'fileid' in request.session['requestParams'] and request.session['requestParams']['fileid']:
            fileid = request.session['requestParams']['fileid']
        else:
            fileid = None
        if 'datasetid' in request.session['requestParams'] and request.session['requestParams']['datasetid']:
            datasetid = request.session['requestParams']['datasetid']
        else:
            datasetid = None
        if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
            jeditaskid = request.session['requestParams']['jeditaskid']
        else:
            jeditaskid = None
        if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk']:
            tk = request.session['requestParams']['tk']
            del request.session['requestParams']['tk']
        else:
            tk = None

        if jeditaskid and datasetid and fileid:
            extraquery_files += """
                pandaid in (
                (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                union all
                (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                ) """.format(jeditaskid, datasetid, fileid, jeditaskid, datasetid, fileid)

        if 'ecstate' in request.session['requestParams'] and tk and datasetid:
            extraquery_files += """
                pandaid in (
                    (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid in ( {} ) and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    union all 
                    (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid in ( {} ) and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    ) """.format(jeditaskid, datasetid, tk, jeditaskid, datasetid, tk)
        # warning['jobsforfiles'] = 'Only jobs for last 4 days are shown. Support of filtering older jobs associated with files will be implemented soon.'
    elif 'jeditaskid' in request.session['requestParams'] and 'datasetid' in request.session['requestParams']:
        fileid = None
        if 'datasetid' in request.session['requestParams'] and request.session['requestParams']['datasetid']:
            datasetid = request.session['requestParams']['datasetid']
        else:
            datasetid = None
        if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
            jeditaskid = request.session['requestParams']['jeditaskid']
        else:
            jeditaskid = None
        if datasetid and jeditaskid:
            extraquery_files += """
                pandaid in (
                (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid = {} )
                union all
                (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid = {})
                ) """.format(jeditaskid, datasetid, jeditaskid, datasetid)
    else:
        fileid = None

    _logger.debug('Specific params processing: {}'.format(time.time()-start_time))

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)

    _logger.debug('Setup view: {}'.format(time.time() - start_time))

    if len(extraquery_files) > 1:
        wildCardExtension += ' AND ' + extraquery_files

    if query == 'reqtoken' and wildCardExtension is None and LAST_N_HOURS_MAX is None:
        return render_to_response('message.html', {'desc':'Request token is not found or data is outdated. Please reload the original page.'}, content_type='text/html')

    jobs = []

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        values = [f.name for f in Jobsactive4._meta.get_fields()]
    elif eventservice:
        values = 'corecount','jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'maxattempt',  'jobname', 'proddblock', 'destinationdblock', 'jobmetrics', 'reqid', 'minramcount', 'statechangetime', 'jobsubstatus', 'eventservice' , 'nevents','gshare','noutputdatafiles','parentid','attemptnr','actualcorecount', 'resourcetype', 'schedulerid', 'pilotid'
    else:
        values = 'corecount','jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'maxattempt', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount', 'statechangetime', 'avgvmem', 'maxvmem', 'maxpss', 'maxrss', 'nucleus', 'eventservice', 'nevents','gshare','noutputdatafiles','parentid','attemptnr','actualcorecount', 'resourcetype','schedulerid', 'pilotid'

    JOB_LIMITS = request.session['JOB_LIMIT']
    totalJobs = 0
    showTop = 0

    if 'limit' in request.session['requestParams']:
        request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])

    droppedList =[]
    if request.user.is_authenticated and request.user.is_tester:
        taskids = {}
        tk = 0
        if 'eventservice' in request.session['requestParams']:
            isEventTask = True
            print ('Event Service!')
        else:
            isEventTask = False
        if 'jeditaskid' in request.session['requestParams']:
            taskids[request.session['requestParams']['jeditaskid']] = 1
        dropmode = True
        if 'mode' in request.session['requestParams'] and request.session['requestParams'][
            'mode'] == 'drop': dropmode = True
        if 'mode' in request.session['requestParams'] and request.session['requestParams'][
            'mode'] == 'nodrop': dropmode = False
        isReturnDroppedPMerge=False
        if 'processingtype' in request.session['requestParams'] and \
            request.session['requestParams']['processingtype'] == 'pmerge': isReturnDroppedPMerge=True
        isJumbo = False
        if dropmode and (len(taskids) == 1) and 'eventservice' in request.session['requestParams']:
            if request.session['requestParams']['eventservice'] != '4' and request.session['requestParams']['eventservice'] != 'jumbo':
                tk,droppedList,wildCardExtension = dropalgorithm.dropRetrielsJobs(list(taskids.keys())[0],wildCardExtension,isEventTask)
            else:
                isJumbo = True

    _logger.debug('Prepare list of retriels if dropmode [Aleksandr method]: {}'.format(time.time() - start_time))

    harvesterjobstatus = ''

    from core.harvester.views import getHarvesterJobs,getCeHarvesterJobs

    if 'jobstatus' in request.session['requestParams']:
        harvesterjobstatus = request.session['requestParams']['jobstatus']
    if 'transferringnotupdated' in request.session['requestParams']:
        jobs = stateNotUpdated(request, state='transferring', values=values, wildCardExtension=wildCardExtension)
    elif 'statenotupdated' in request.session['requestParams']:
        jobs = stateNotUpdated(request, values=values, wildCardExtension=wildCardExtension)
    elif 'harvesterinstance' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request, instance=request.session['requestParams']['harvesterinstance'],
                                workerid=request.session['requestParams']['workerid'], jobstatus=harvesterjobstatus)
    elif 'harvesterid' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request, instance=request.session['requestParams']['harvesterid'],
                                workerid=request.session['requestParams']['workerid'], jobstatus=harvesterjobstatus)
    elif ('harvesterinstance' not in request.session['requestParams'] and 'harvesterid' not in request.session['requestParams']) and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request, workerid=request.session['requestParams']['workerid'], jobstatus=harvesterjobstatus)
    elif 'harvesterce' in request.session['requestParams']:
        jobs = getCeHarvesterJobs(request, computingelment=request.session['requestParams']['harvesterce'])
    else:
        excludedTimeQuery = copy.deepcopy(query)
        if ('modificationtime__castdate__range' in excludedTimeQuery and len(set(['date_to', 'hours']).intersection(
                request.session['requestParams'].keys())) == 0) or (
                    'jobstatus' in request.session['requestParams'] and is_job_active(
                        request.session['requestParams']['jobstatus'])):
            del excludedTimeQuery['modificationtime__castdate__range']
            warning['notimelimit'] = "no time window limitting was applied for active jobs in this selection"

        jobs.extend(Jobsdefined4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
                    :request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
                    :request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
                    :request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension])[
                    :request.session['JOB_LIMIT']].values(*values))
        listJobs = [Jobsarchived4,Jobsactive4,Jobswaiting4,Jobsdefined4]
        if not noarchjobs:
            queryFrozenStates = []
            if 'jobstatus' in request.session['requestParams']:
                if isEventTask:
                    queryFrozenStates = list(filter(set(request.session['requestParams']['jobstatus'].split('|')).__contains__,
                                           ['finished', 'failed', 'cancelled', 'closed', 'merging']))
                else:
                    queryFrozenStates = list(filter(set(request.session['requestParams']['jobstatus'].split('|')).__contains__,
                                           ['finished', 'failed', 'cancelled', 'closed']))
            ##hard limit is set to 2K
            if ('jobstatus' not in request.session['requestParams'] or len(queryFrozenStates) > 0):

                if ('limit' not in request.session['requestParams'] and 'jeditaskid' not in request.session[
                    'requestParams']):
                    request.session['JOB_LIMIT'] = 20000
                    JOB_LIMITS = 20000
                    showTop = 1
                elif ('limit' not in request.session['requestParams'] and 'jeditaskid' in request.session[
                    'requestParams']):
                    request.session['JOB_LIMIT'] = 200000
                    JOB_LIMITS = 200000
                else:
                    request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
                    JOB_LIMITS = int(request.session['requestParams']['limit'])
                if 'modificationtime__castdate__range' in query and (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0],
                                                         "%Y-%m-%d %H:%M:%S")).days > 2) or \
                            ((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
                                                                 "%Y-%m-%d %H:%M:%S")).days > 2)):
                    if 'jeditaskid' in request.session['requestParams'] and 'json' in request.session['requestParams'] \
                            and ('fulllist' in request.session['requestParams'] and
                                              request.session['requestParams']['fulllist'] == 'true'):
                        del query['modificationtime__castdate__range']
                    archJobs = Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[
                           :request.session['JOB_LIMIT']].values(*values)
                    listJobs.append(Jobsarchived)
                    totalJobs = len(archJobs)
                    jobs.extend(archJobs)
        if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                    'json' not in request.session['requestParams'])):
            thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension,dkey))
            thread.start()
        else:
            thread = None

    _logger.debug('Got jobs: {}'.format(time.time() - start_time))

    ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
    taskids = {}

    for job in jobs:
        if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1
    dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'drop': dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'nodrop': dropmode = False
    isReturnDroppedPMerge=False
    if 'processingtype' in request.session['requestParams'] and \
        request.session['requestParams']['processingtype'] == 'pmerge': isReturnDroppedPMerge=True
    droplist = []
    newdroplist = []
    droppedPmerge = set()
    newdroppedPmerge = set()
    cntStatus = []
    if dropmode and (len(taskids) == 1):
        start = time.time()
        jobs, droplist, droppedPmerge = dropRetrielsJobs(jobs,list(taskids.keys())[0],isReturnDroppedPMerge)
        end = time.time()
        print(end - start)
        if request.user.is_authenticated and request.user.is_tester:
            _logger.debug('Started deepcopy: {}'.format(time.time() - start_time))

            newjobs = copy.deepcopy(jobs)

            _logger.debug('Finished deep copy: {}'.format(time.time() - start_time))
            if 'eventservice' in request.session['requestParams']:
                isEventTask = True
                print ('Event Service!')
            else:
                isEventTask = False
            start = time.time()
            if isJumbo == False:
                newjobs,newdroppedPmerge,newdroplist = dropalgorithm.clearDropRetrielsJobs(tk=tk,droplist=droppedList,jobs=newjobs,isEventTask=isEventTask,isReturnDroppedPMerge=isReturnDroppedPMerge)
            end = time.time()
            print(end - start)

    _logger.debug('Done droppping if was requested: {}'.format(time.time() - start_time))

    #get attemps of file if fileid in request params
    files_attempts_dict = {}
    files_attempts = []
    if fileid:
        if fileid and jeditaskid and datasetid:
            fquery = {}
            fquery['pandaid__in'] = [job['pandaid'] for job in jobs if len(jobs) > 0]
            fquery['fileid'] = fileid
            files_attempts.extend(Filestable4.objects.filter(**fquery).values('pandaid', 'attemptnr'))
            files_attempts.extend(FilestableArch.objects.filter(**fquery).values('pandaid', 'attemptnr'))
            if len(files_attempts) > 0:
                files_attempts_dict = dict(zip([f['pandaid'] for f in files_attempts], [ff['attemptnr'] for ff in files_attempts]))

            jfquery = {'jeditaskid': jeditaskid, 'datasetid': datasetid, 'fileid': fileid}
            jedi_file = JediDatasetContents.objects.filter(**jfquery).values('attemptnr', 'maxattempt', 'failedattempt', 'maxfailure')
            if jedi_file and len(jedi_file) > 0:
                jedi_file = jedi_file[0]
            if len(files_attempts_dict) > 0:
                for job in jobs:
                    if job['pandaid'] in files_attempts_dict:
                        job['fileattemptnr'] = files_attempts_dict[job['pandaid']]
                    else:
                        job['fileattemptnr'] = None
                    if jedi_file and 'maxattempt' in jedi_file:
                        job['filemaxattempts'] = jedi_file['maxattempt']

    _logger.debug('Got file attempts: {}'.format(time.time() - start_time))

    jobs = cleanJobList(request, jobs, doAddMeta=False)

    _logger.debug('Cleaned job list: {}'.format(time.time() - start_time))

    jobs = reconstructJobsConsumers(jobs)

    _logger.debug('Reconstructed consumers: {}'.format(time.time() - start_time))

    njobs = len(jobs)
    jobtype = ''
    if 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'

    if u'display_limit' in request.session['requestParams']:
        if int(request.session['requestParams']['display_limit']) > njobs:
            display_limit = njobs
        else:
            display_limit = int(request.session['requestParams']['display_limit'])
        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    else:
        display_limit = 1000
        url_nolimit = request.get_full_path()
    njobsmax = display_limit

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']

        if sortby == 'time-ascending':
            jobs = sorted(jobs, key=lambda x:x['modificationtime'] if not x['modificationtime'] is None else datetime(1900, 1, 1))
        if sortby == 'time-descending':
            jobs = sorted(jobs, key=lambda x:x['modificationtime'] if not x['modificationtime'] is None else datetime(1900, 1, 1), reverse=True)
        if sortby == 'statetime':
            jobs = sorted(jobs, key=lambda x:x['statechangetime'] if not x['statechangetime'] is None else datetime(1900, 1, 1), reverse=True)
        elif sortby == 'priority':
            jobs = sorted(jobs, key=lambda x:x['currentpriority'] if not x['currentpriority'] is None else 0, reverse=True)
        elif sortby == 'attemptnr':
            jobs = sorted(jobs, key=lambda x: x['attemptnr'], reverse=True)
        elif sortby == 'duration-ascending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'duration-descending':
            jobs = sorted(jobs, key=lambda x: x['durationsec'], reverse=True)
        elif sortby == 'duration':
            jobs = sorted(jobs, key=lambda x: x['durationsec'])
        elif sortby == 'PandaID':
            jobs = sorted(jobs, key=lambda x: x['pandaid'], reverse=True)
    elif fileid:
        sortby = "fileattemptnr-descending"
        jobs = sorted(jobs, key=lambda x: x['fileattemptnr'], reverse=True)
    else:
        sortby = "attemptnr-descending,pandaid-descending"
        jobs = sorted(jobs, key=lambda x: [-x['attemptnr'],-x['pandaid']])

    _logger.debug('Sorted joblist: {}'.format(time.time() - start_time))

    taskname = ''
    if 'jeditaskid' in request.session['requestParams']:
        taskname = getTaskName('jeditaskid', request.session['requestParams']['jeditaskid'])
    if 'taskid' in request.session['requestParams']:
        taskname = getTaskName('jeditaskid', request.session['requestParams']['taskid'])

    if 'produsername' in request.session['requestParams']:
        user = request.session['requestParams']['produsername']
    elif 'user' in request.session['requestParams']:
        user = request.session['requestParams']['user']
    else:
        user = None

    _logger.debug('Got task names: {}'.format(time.time() - start_time))

    ## set up google flow diagram
    flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)

    _logger.debug('Built google flow diagram: {}'.format(time.time() - start_time))

    if ('datasets' in request.session['requestParams']) and (request.session['requestParams']['datasets'] == 'yes') and ((
        ('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or ('json' in request.session['requestParams'])):
        for job in jobs:
            files = []
            pandaid = job['pandaid']
            files.extend(JediDatasetContents.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
            ninput = 0

            dsquery = Q()
            counter = 0
            if len(files) > 0:
                for f in files:
                    if f['type'] == 'input': ninput += 1
                    f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)

                    f['DSQuery'] = {'jeditaskid': job['jeditaskid'], 'datasetid': f['datasetid']}
                    dsquery = dsquery | Q(Q(jeditaskid=job['jeditaskid']) & Q(datasetid=f['datasetid']))
                    counter += 1
                    if counter == 30:
                        break

                dsets = JediDatasets.objects.filter(dsquery).extra(select={"dummy1": '/*+ INDEX_RS_ASC(ds JEDI_DATASETS_PK) */ 1 '}).values()
                if len(dsets) > 0:
                    for ds in dsets:
                        for file in files:
                            if 'DSQuery' in file and file['DSQuery']['jeditaskid'] == ds['jeditaskid'] and \
                                    file['DSQuery']['datasetid'] == ds['datasetid']:
                                file['dataset'] = ds['datasetname']
                                del file['DSQuery']

                    #dsets = JediDatasets.objects.filter(jeditaskid=job['jeditaskid'], datasetid=f['datasetid']).extra(select={"dummy1" : '/*+ INDEX_RS_ASC(ds JEDI_DATASETS_PK) */ 1 '}).values()
                    #if len(dsets) > 0:
                    #    f['datasetname'] = dsets[0]['datasetname']


            if True:
                # if ninput == 0:
                files.extend(Filestable4.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
                if len(files) == 0:
                    files.extend(FilestableArch.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
                if len(files) > 0:
                    for f in files:
                        if 'creationdate' not in f: f['creationdate'] = f['modificationtime']
                        if 'fileid' not in f: f['fileid'] = f['row_id']
                        if 'datasetname' not in f and 'dataset' in f: f['datasetname'] = f['dataset']
                        if 'modificationtime' in f: f['oldfiletable'] = 1
                        if 'destinationdblock' in f and f['destinationdblock'] is not None:
                            f['destinationdblock_vis'] = f['destinationdblock'].split('_')[-1]
            files = sorted(files, key=lambda x: x['type'])
            nfiles = len(files)
            logfile = {}
            for file in files:
                if file['type'] == 'log':
                    logfile['lfn'] = file['lfn']
                    logfile['guid'] = file['guid']
                    if 'destinationse' in file:
                        logfile['site'] = file['destinationse']
                    else:
                        logfilerec = Filestable4.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                        if len(logfilerec) == 0:
                            logfilerec = FilestableArch.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                        if len(logfilerec) > 0:
                            logfile['site'] = logfilerec[0]['destinationse']
                            logfile['guid'] = logfilerec[0]['guid']
                    logfile['scope'] = file['scope']
                file['fsize'] = int(file['fsize'] / 1000000)
            job['datasets'] = files

    _logger.debug('Got datasets info if requested: {}'.format(time.time() - start_time))

    # show warning or not
    if njobs <= request.session['JOB_LIMIT']:
        showwarn = 0
    else:
        showwarn = 1

    # Sort in order to see the most important tasks
    sumd, esjobdict = jobSummaryDict(request, jobs, standard_fields+['corecount','noutputdatafiles','actualcorecount','schedulerid', 'pilotversion'])
    if sumd:
        for item in sumd:
            if item['field'] == 'jeditaskid':
                item['list'] = sorted(item['list'], key=lambda k: k['kvalue'], reverse=True)

    _logger.debug('Built standard params attributes summary: {}'.format(time.time() - start_time))

    if 'jeditaskid' in request.session['requestParams']:
        if len(jobs) > 0:
            for job in jobs:
                if 'maxvmem' in job:
                    if type(job['maxvmem']) is int and job['maxvmem'] > 0:
                        job['maxvmemmb'] = "%0.2f" % (job['maxvmem'] / 1000.)
                        job['avgvmemmb'] = "%0.2f" % (job['avgvmem'] / 1000.)
                if 'maxpss' in job:
                    if type(job['maxpss']) is int and job['maxpss'] > 0:
                        job['maxpss'] = "%0.2f" % (job['maxpss'] / 1024.)


    testjobs = False
    if 'prodsourcelabel' in request.session['requestParams'] and request.session['requestParams'][
        'prodsourcelabel'].lower().find('test') >= 0:
        testjobs = True
    tasknamedict = taskNameDict(jobs)
    errsByCount, errsBySite, errsByUser, errsByTask, errdSumd, errHist = errorSummaryDict(request, jobs, tasknamedict,
                                                                                          testjobs)

    errsByMessage = get_error_message_summary(jobs)

    _logger.debug('Built error summary: {}'.format(time.time() - start_time))

    # Here we getting extended data for site
    jobsToShow = jobs[:njobsmax]
    from core.libs import exlib
    try:
        jobsToShow = exlib.fileList(jobsToShow)
    except Exception as e:
        logger = logging.getLogger('bigpandamon-error')
        logger.error(e)
    ###RESERVE
    distinctComputingSites = []
    for job in jobsToShow:
        distinctComputingSites.append(job['computingsite'])
    distinctComputingSites = list(set(distinctComputingSites))
    query = {}
    query['siteid__in'] = distinctComputingSites
    siteres = Schedconfig.objects.filter(**query).exclude(cloud='CMS').extra().values('siteid', 'status',
                                                                                      'comment_field')
    siteHash = {}
    for site in siteres:
        siteHash[site['siteid']] = (site['status'], site['comment_field'])
    for job in jobsToShow:
        if job['computingsite'] in siteHash.keys():
            job['computingsitestatus'] = siteHash[job['computingsite']][0]
            job['computingsitecomment'] = siteHash[job['computingsite']][1]

    _logger.debug('Got extra params for sites: {}'.format(time.time() - start_time))

    if thread != None:
        try:
            thread.join()
            jobsTotalCount = sum(tcount[dkey])
            print(dkey)
            print(tcount[dkey])
            del tcount[dkey]
            print(tcount)
            print(jobsTotalCount)
        except:
            jobsTotalCount = -1
    else: jobsTotalCount = -1

    listPar =[]
    for key, val in request.session['requestParams'].items():
        if (key!='limit' and key!='display_limit'):
            listPar.append(key + '=' + str(val))
    if len(listPar)>0:
        urlParametrs = '&'.join(listPar)+'&'
    else:
        urlParametrs = None
    print(listPar)
    del listPar
    if (math.fabs(njobs-jobsTotalCount)<1000 or jobsTotalCount == -1):
        jobsTotalCount=None
    else:
        jobsTotalCount = int(math.ceil((jobsTotalCount+10000)/10000)*10000)

    _logger.debug('Total jobs count thread finished: {}'.format(time.time() - start_time))

    for job in jobsToShow:
        if job['creationtime']:
            job['creationtime'] = job['creationtime'].strftime(defaultDatetimeFormat)
        if job['modificationtime']:
            job['modificationtime'] = job['modificationtime'].strftime(defaultDatetimeFormat)
        if job['statechangetime']:
            job['statechangetime'] = job['statechangetime'].strftime(defaultDatetimeFormat)

    isincomparisonlist = False
    clist = []
    if request.user.is_authenticated and request.user.is_tester:

        cquery = {}
        cquery['object'] = 'job'
        cquery['userid'] = request.user.id
        try:
            jobsComparisonList = ObjectsComparison.objects.get(**cquery)
        except ObjectsComparison.DoesNotExist:
            jobsComparisonList = None

        if jobsComparisonList:
            try:
                clist = json.loads(jobsComparisonList.comparisonlist)
                newlist = []
                for ce in clist:
                    try:
                        ceint = int(ce)
                        newlist.append(ceint)
                    except:
                        pass
                clist = newlist
            except:
                clist = []

    _logger.debug('Got comparison job list for user: {}'.format(time.time() - start_time))

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):

        xurl = extensibleURL(request)
        time_locked_url = removeParam(removeParam(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
                          'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
                          '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
        nodurminurl = removeParam(xurl, 'durationmin', mode='extensible')
        print (xurl)
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')
        nosorturl = removeParam(nosorturl, 'display_limit', mode='extensible')
        #nosorturl = removeParam(nosorturl, 'harvesterinstance', mode='extensible')
        xurl = removeParam(nosorturl, 'mode', mode='extensible')

        TFIRST = request.session['TFIRST'].strftime(defaultDatetimeFormat)
        TLAST = request.session['TLAST'].strftime(defaultDatetimeFormat)
        del request.session['TFIRST']
        del request.session['TLAST']
        errsByCount = importToken(request,errsByCount=errsByCount)
        nodropPartURL = cleanURLFromDropPart(xurl)
        difDropList = dropalgorithm.compareDropAlgorithm(droplist,newdroplist)

        _logger.debug('Extra data preporation done: {}'.format(time.time() - start_time))

        data = {
            'prefix': getPrefix(request),
            'errsByCount': errsByCount,
            'errsByMessage': json.dumps(errsByMessage),
            'errdSumd': errdSumd,
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'jobList': jobsToShow,
            'jobtype': jobtype,
            'njobs': njobs,
            'user': user,
            'sumd': sumd,
            'xurl': xurl,
            'xurlnopref': xurl[5:],
            'droplist': droplist,
            'ndrops': len(droplist) if len(droplist) > 0 else (- len(droppedPmerge)),
            'tfirst': TFIRST,
            'tlast': TLAST,
            'plow': PLOW,
            'phigh': PHIGH,
            'showwarn': showwarn,
            'joblimit': request.session['JOB_LIMIT'],
            'limit': JOB_LIMITS,
            'totalJobs': totalJobs,
            'showTop': showTop,
            'url_nolimit': url_nolimit,
            'display_limit': display_limit,
            'sortby': sortby,
            'nosorturl': nosorturl,
            'nodurminurl': nodurminurl,
            'time_locked_url': time_locked_url,
            'taskname': taskname,
            'flowstruct': flowstruct,
            'nodropPartURL': nodropPartURL,
            'eventservice': eventservice,
            'jobsTotalCount': jobsTotalCount,
            'requestString': urlParametrs,
            'built': datetime.now().strftime("%H:%M:%S"),
            'newndrop_test': len(newdroplist) if len(newdroplist) > 0 else (- len(newdroppedPmerge)),
            'cntStatus_test': cntStatus,
            'ndropPmerge_test':len(newdroppedPmerge),
            'droppedPmerge2_test':newdroppedPmerge,
            'pandaIDList_test':newdroplist,
            'difDropList_test':difDropList,
            'clist': clist,
            'warning': warning,
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobList", json.dumps(data, cls=DateEncoder), 60 * 20)

        _logger.debug('Cache was set: {}'.format(time.time() - start_time))

        if eventservice:
            response = render_to_response('jobListES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobList.html', data, content_type='text/html')

        _logger.debug('Rendered template: {}'.format(time.time() - start_time))

        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        if (('fields' in request.session['requestParams']) and (len(jobs) > 0)):
            fields = request.session['requestParams']['fields'].split(',')
            fields = (set(fields) & set(jobs[0].keys()))
            if 'pandaid' not in fields:
                list(fields).append('pandaid')
            for job in jobs:
                for field in list(job.keys()):
                    if field in fields:
                        pass
                    else:
                        del job[field]

        data = {
            "selectionsummary": sumd,
            "jobs": jobs,
            "errsByCount": errsByCount,
        }
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

def importToken(request,errsByCount):
    newErrsByCount = []
    random.seed()
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
    else:
        tmpTableName = "TMP_IDS1DEBUG"
    isListPID = False
    new_cur = connection.cursor()
    transactionKey = random.randrange(1000000)
    for item in errsByCount:
        executionData = []
        if (type(item) is not int):
            transactionKey = random.randrange(1000000)
            if ('pandalist' in item):
                item['tk'] = transactionKey
                for key,values in item['pandalist'].items():
            #print item['error'] , key , values
                    executionData.append((key, transactionKey, timezone.now().strftime(defaultDatetimeFormat)))
            #setCacheEntry(request,transactionKey,executionData,60*60, isData=True)
        else:
            executionData.append((item, transactionKey, timezone.now().strftime(defaultDatetimeFormat)))
            if isListPID == False:
                isListPID = True
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY,INS_TIME) VALUES (%s, %s, %s)"""
        new_cur.executemany(query, executionData)
        newErrsByCount.append(item)
    if isListPID == True:
        #newErrsByCount =  {'tk':transactionKey, 'pandalist':newErrsByCount}
        newErrsByCount = transactionKey
        #print newErrsByCount['tk']
        print (len(errsByCount))
    return newErrsByCount


@login_customrequired
def summaryErrorsList(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    message = {}
    isReloadData = False
    notTkLive = 0
    errorsList = []

    if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk']:
        transactionkey = request.session['requestParams']['tk']
    else:
        transactionkey = None
    if 'codename' in request.session['requestParams'] and request.session['requestParams']['codename']:
        codename = request.session['requestParams']['codename']
    else:
        codename = None
    if 'codeval' in request.session['requestParams'] and request.session['requestParams']['codeval']:
        codeval = request.session['requestParams']['codeval']
    else:
        codeval = None

    if transactionkey and codename and codeval:
        checkTKeyQuery = '''
            SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}
            '''
        try:
            sqlRequestFull = checkTKeyQuery.format(transactionkey)
            cur = connection.cursor()
            cur.execute(sqlRequestFull)
            errorsList = cur.fetchall()
        except:
            message['warning'] = """The data is outdated or not found. 
                You should close this page, refresh jobs page and try again."""

        if len(errorsList) == 0 or errorsList == '':
            data = getCacheEntry(request, transactionkey, isData=True)
            if dbaccess['default']['ENGINE'].find('oracle') >= 0:
                tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
            else:
                tmpTableName = "TMP_IDS1DEBUG"
            new_cur = connection.cursor()
            query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY,INS_TIME) VALUES (%s, %s, %s)"""
            if data is not None:
                new_cur.executemany(query, data)
            else:
                message['warning'] = """The data is outdated or not found. 
                                You should close this page, refresh jobs page and try again."""


    xurl = extensibleURL(request)
    print(xurl)

    data = {
        'prefix': getPrefix(request),
        'tk': transactionkey,
        'codename':codename,
        'codeval':codeval,
        'request': request,
        'message': message,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    data.update(getContextVariables(request))
    response = render_to_response('errorSummaryList.html', data, content_type='text/html')
    return response


def summaryErrorMessagesListJSON(request):
    """
    JSON for Datatables errors
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'codename' in request.session['requestParams'] and request.session['requestParams']['codename']:
        codename = request.session['requestParams']['codename']
    else:
        codename = None
    if 'codeval' in request.session['requestParams'] and request.session['requestParams']['codeval']:
        codeval = request.session['requestParams']['codeval']
    else:
        codeval = None

    fullListErrors = []
    errorcode2diag = {}
    for er in errorcodelist:
        if er['error'] == request.session['requestParams']['codename']:
            errorcode = er['name'] + ':' + request.session['requestParams']['codeval']
        if er['name'] == str(request.session['requestParams']['codename']):
            codename = er['error']
            errorcode = er['name'] + ':' + request.session['requestParams']['codeval']
        errorcode2diag[er['error']] = er['diag']

    condition = request.session['requestParams']['tk']
    sqlRequest = """
    SELECT DISTINCT PANDAID, JEDITASKID, COMMANDTOPILOT, 
        concat('transformation:',TRANSEXITCODE) AS TRANSEXITCODE, 
        concat('pilot:',PILOTERRORCODE) AS PILOTERRORCODE, 
        PILOTERRORDIAG, 
        concat('exe:',EXEERRORCODE) AS EXEERRORCODE, 
        EXEERRORDIAG, 
        concat('sup:',SUPERRORCODE) AS SUPERRORCODE, 
        SUPERRORDIAG,
        concat('ddm:',DDMERRORCODE) AS DDMERRORCODE,
        DDMERRORDIAG,
        concat('brokerage:',BROKERAGEERRORCODE) AS BROKERAGEERRORCODE,
        BROKERAGEERRORDIAG,
        concat('jobdispatcher:',JOBDISPATCHERERRORCODE) AS JOBDISPATCHERERRORCODE,
        JOBDISPATCHERERRORDIAG,
        concat('taskbuffer:',TASKBUFFERERRORCODE) AS TASKBUFFERERRORCODE,
        TASKBUFFERERRORDIAG 
    FROM (
        SELECT PANDAID,JEDITASKID, 
            COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,
            SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,
            JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG 
        FROM ATLAS_PANDA.JOBSARCHIVED4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE 
        WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSARCHIVED4.PANDAID
        UNION ALL
        SELECT PANDAID,JEDITASKID, 
            COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,
            SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,
            JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG 
        FROM ATLAS_PANDA.JOBSACTIVE4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE 
        WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSACTIVE4.PANDAID
        UNION ALL 
        SELECT PANDAID,JEDITASKID, 
            COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,
            SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,
            JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG 
        FROM ATLAS_PANDA.JOBSDEFINED4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE 
        WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSDEFINED4.PANDAID
        UNION ALL 
        SELECT PANDAID,JEDITASKID, 
            COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,
            SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,
            JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG 
        FROM ATLAS_PANDA.JOBSWAITING4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE 
        WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSWAITING4.PANDAID
        UNION ALL
        SELECT PANDAID,JEDITASKID, 
            COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,
            SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,
            BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG 
        FROM ATLAS_PANDAARCH.JOBSARCHIVED, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE 
        WHERE PIDACTIVE.ID=ATLAS_PANDAARCH.JOBSARCHIVED.PANDAID
        )
    """

    sqlRequest += ' WHERE ' + codename + '=' + codeval
    sqlRequestFull = sqlRequest.format(condition)
    cur = connection.cursor()
    cur.execute(sqlRequestFull)
    errors_tuple = cur.fetchall()
    errors_header = [s.lower() for s in ['PANDAID', 'JEDITASKID', 'COMMANDTOPILOT', 'TRANSEXITCODE',
             'PILOTERRORCODE', 'PILOTERRORDIAG', 'EXEERRORCODE', 'EXEERRORDIAG', 'SUPERRORCODE', 'SUPERRORDIAG',
             'DDMERRORCODE', 'DDMERRORDIAG', 'BROKERAGEERRORCODE', 'BROKERAGEERRORDIAG',
             'JOBDISPATCHERERRORCODE', 'JOBDISPATCHERERRORDIAG', 'TASKBUFFERERRORCODE', 'TASKBUFFERERRORDIAG']]
    errors_list = [dict(zip(errors_header, row)) for row in errors_tuple]

    # group by error diag message, counting unique messages and store top N pandaids and by errorcode for full list table
    N_SAMPLEJOBS = 5
    errorMessages = {}
    for error in errors_list:
        if errorcode in error.values():
            if error[errorcode2diag[codename]] not in errorMessages:
                errorMessages[error[errorcode2diag[codename]]] = {'count': 0, 'pandaids': []}
            errorMessages[error[errorcode2diag[codename]]]['count'] += 1
            if len(errorMessages[error[errorcode2diag[codename]]]['pandaids']) < N_SAMPLEJOBS:
                errorMessages[error[errorcode2diag[codename]]]['pandaids'].append(error['pandaid'])

    # transform dict -> list
    error_messages = []
    for key, value in errorMessages.items():
        error_messages.append({'desc': key, 'count': value['count'], 'pandaids': value['pandaids']})

    return HttpResponse(json.dumps(error_messages), content_type='application/json')


def summaryErrorsListJSON(request):
    initRequest(request)

    codename = request.session['requestParams']['codename']
    codeval = request.session['requestParams']['codeval']
    fullListErrors = []
    #isJobsss = False
    print (request.session['requestParams'])
    for er in errorcodelist:
        if er['error'] == request.session['requestParams']['codename']:
            errorcode = er['name'] + ':' + request.session['requestParams']['codeval']
        if er['name'] == str(request.session['requestParams']['codename']):
            codename = er['error']
            errorcode = er['name'] + ':' + request.session['requestParams']['codeval']
            #isJobsss=True
    #d = dict((k, v) for k, v in errorcodelist if v >= request.session['requestParams']['codename'])


    condition = request.session['requestParams']['tk']
    sqlRequest = '''
SELECT DISTINCT PANDAID,JEDITASKID, COMMANDTOPILOT, concat('transformation:',TRANSEXITCODE) AS TRANSEXITCODE, concat('pilot:',PILOTERRORCODE) AS PILOTERRORCODE, PILOTERRORDIAG, concat('exe:',EXEERRORCODE) AS EXEERRORCODE, EXEERRORDIAG, concat('sup:',SUPERRORCODE) AS SUPERRORCODE,SUPERRORDIAG,concat('ddm:',DDMERRORCODE) AS DDMERRORCODE,DDMERRORDIAG,concat('brokerage:',BROKERAGEERRORCODE) AS BROKERAGEERRORCODE,BROKERAGEERRORDIAG,concat('jobdispatcher:',JOBDISPATCHERERRORCODE) AS JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,concat('taskbuffer:',TASKBUFFERERRORCODE) AS TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM
(SELECT PANDAID,JEDITASKID, COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM ATLAS_PANDA.JOBSARCHIVED4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSARCHIVED4.PANDAID
UNION ALL
SELECT PANDAID,JEDITASKID, COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM ATLAS_PANDA.JOBSACTIVE4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSACTIVE4.PANDAID
UNION ALL 
SELECT PANDAID,JEDITASKID, COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM ATLAS_PANDA.JOBSDEFINED4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSDEFINED4.PANDAID
UNION ALL 
SELECT PANDAID,JEDITASKID, COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM ATLAS_PANDA.JOBSWAITING4, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE WHERE PIDACTIVE.ID=ATLAS_PANDA.JOBSWAITING4.PANDAID
UNION ALL
SELECT PANDAID,JEDITASKID, COMMANDTOPILOT, TRANSEXITCODE,PILOTERRORCODE, PILOTERRORDIAG,EXEERRORCODE, EXEERRORDIAG,SUPERRORCODE,SUPERRORDIAG,DDMERRORCODE,DDMERRORDIAG,BROKERAGEERRORCODE,BROKERAGEERRORDIAG,JOBDISPATCHERERRORCODE,JOBDISPATCHERERRORDIAG,TASKBUFFERERRORCODE,TASKBUFFERERRORDIAG FROM ATLAS_PANDAARCH.JOBSARCHIVED, (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1DEBUG WHERE TRANSACTIONKEY={0}) PIDACTIVE WHERE PIDACTIVE.ID=ATLAS_PANDAARCH.JOBSARCHIVED.PANDAID)
    '''
    #if isJobsss:
    sqlRequest += ' WHERE '+ codename + '='+codeval
    # INPUT_EVENTS, TOTAL_EVENTS, STEP
    shortListErrors = []
    sqlRequestFull = sqlRequest.format(condition)
    cur = connection.cursor()
    cur.execute(sqlRequestFull)
    errorsList = cur.fetchall()
    for error in errorsList:
        if (errorcode in error):
            try:
                errnum = int(codeval)
                if str(error[error.index(errorcode) + 1]) !='' and 'transformation' not in errorcode:
                    descr = str(error[error.index(errorcode) + 1])
                else:
                    if codename in errorCodes and errnum in errorCodes[codename]:
                        descr = errorCodes[codename][errnum]
                    else:
                        descr = 'None'
            except:
                pass
            rowDict = {"taskid": error[1], "pandaid": error[0], "desc": descr}
            fullListErrors.append(rowDict)
    return HttpResponse(json.dumps(fullListErrors), content_type='application/json')



def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif (obj == 0): return 0
    elif (obj == 'None'): return -1

def isEventService(job):

    if 'eventservice' in job and job['eventservice'] is not None:
        if 'specialhandling' in job and job['specialhandling'] and (
                    job['specialhandling'].find('eventservice') >= 0 or job['specialhandling'].find('esmerge') >= 0 or (
                job['eventservice'] != 'ordinary' and job['eventservice'])) and job['specialhandling'].find('sc:') == -1:
                return True
        else:
            return False
    else:
        return False



def cleanURLFromDropPart(url):
    posDropPart = url.find('mode')
    if (posDropPart == -1):
        return url
    else:
        if url[posDropPart - 1] == '&':
            posDropPart -= 1
        nextAmp = url.find('&', posDropPart + 1)
        if nextAmp == -1:
            return url[0:posDropPart]
        else:
            return url[0:posDropPart] + url[nextAmp + 1:]


def getSequentialRetries(pandaid, jeditaskid, countOfInvocations):
    retryquery = {}
    countOfInvocations.append(1)
    retryquery['jeditaskid'] = jeditaskid
    retryquery['newpandaid'] = pandaid
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('oldpandaid').reverse().values()
        newretries.extend(retries)
        for retry in retries:
            if retry['relationtype'] in ['merge', 'retry']:
                jsquery = {}
                jsquery['jeditaskid'] = jeditaskid
                jsquery['pandaid'] = retry['oldpandaid']
                values = ['pandaid', 'jobstatus', 'jeditaskid']
                jsjobs = []
                jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
                jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
                for job in jsjobs:
                    if job['jobstatus'] == 'failed':
                        for retry in newretries:
                            if (retry['oldpandaid'] == job['pandaid']):
                                retry['relationtype'] = 'retry'
                        newretries.extend(getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations))

    outlist = []
    added_keys = set()
    for row in newretries:
        lookup = row['oldpandaid']
        if lookup not in added_keys:
            outlist.append(row)
            added_keys.add(lookup)

    return outlist


def getSequentialRetries_ES(pandaid, jobsetid, jeditaskid, countOfInvocations, recurse=0):
    retryquery = {}
    retryquery['jeditaskid'] = jeditaskid
    retryquery['newpandaid'] = jobsetid
    retryquery['relationtype'] = 'jobset_retry'
    countOfInvocations.append(1)
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('oldpandaid').reverse().values()
        newretries.extend(retries)
        for retry in retries:
            jsquery = {}
            jsquery['jeditaskid'] = jeditaskid
            jsquery['jobstatus'] = 'failed'
            jsquery['jobsetid'] = retry['oldpandaid']
            values = ['pandaid', 'jobstatus', 'jobsetid', 'jeditaskid']
            jsjobs = []
            jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
            for job in jsjobs:
                if job['jobstatus'] == 'failed':
                    for retry in newretries:
                        if (retry['oldpandaid'] == job['jobsetid']):
                            retry['relationtype'] = 'retry'
                            retry['jobid'] = job['pandaid']

                        newretries.extend(getSequentialRetries_ES(job['pandaid'],
                                                                  jobsetid, job['jeditaskid'], countOfInvocations,
                                                                  recurse + 1))
    outlist = []
    added_keys = set()
    for row in newretries:
        if 'jobid' in row:
            lookup = row['jobid']
            if lookup not in added_keys:
                outlist.append(row)
                added_keys.add(lookup)
    return outlist


def getSequentialRetries_ESupstream(pandaid, jobsetid, jeditaskid, countOfInvocations, recurse=0):
    retryquery = {}
    retryquery['jeditaskid'] = jeditaskid
    retryquery['oldpandaid'] = jobsetid
    retryquery['relationtype'] = 'jobset_retry'
    countOfInvocations.append(1)
    newretries = []

    if (len(countOfInvocations) < 100):
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').values()
        newretries.extend(retries)
        for retry in retries:
            jsquery = {}
            jsquery['jeditaskid'] = jeditaskid
            jsquery['jobsetid'] = retry['newpandaid']
            values = ['pandaid', 'jobstatus', 'jobsetid', 'jeditaskid']
            jsjobs = []
            jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
            jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
            for job in jsjobs:
                for retry in newretries:
                    if (retry['newpandaid'] == job['jobsetid']):
                        retry['relationtype'] = 'retry'
                        retry['jobid'] = job['pandaid']

    outlist = []
    added_keys = set()
    for row in newretries:
        if 'jobid' in row:
            lookup = row['jobid']
            if lookup not in added_keys:
                outlist.append(row)
                added_keys.add(lookup)
    return outlist


def descendentjoberrsinfo(request):
    valid, response = initRequest(request)
    if not valid: return response
    data = {}

    job_pandaid = job_jeditaskid = -1
    if 'pandaid' in request.session['requestParams']:
        job_pandaid = int(request.session['requestParams']['pandaid'])
    if 'jeditaskid' in request.session['requestParams']:
        job_jeditaskid = int(request.session['requestParams']['jeditaskid'])

    if (job_pandaid == -1) or (job_jeditaskid == -1):
        data = {"error": "no pandaid or jeditaskid supplied"}
        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    query = setupView(request, hours=365 * 24)
    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).values())
    jobs.extend(Jobsactive4.objects.filter(**query).values())
    jobs.extend(Jobswaiting4.objects.filter(**query).values())
    jobs.extend(Jobsarchived4.objects.filter(**query).values())
    if len(jobs) == 0:
        jobs.extend(Jobsarchived.objects.filter(**query).values())

    if len(jobs) == 0:
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {"error": "job not found"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    job = jobs[0]
    countOfInvocations = []
    if not isEventService(job):
        retryquery = {}
        retryquery['jeditaskid'] = job['jeditaskid']
        retryquery['oldpandaid'] = job['pandaid']
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values()
        pretries = getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations)
    else:
        retryquery = {}
        retryquery['jeditaskid'] = job['jeditaskid']
        retryquery['oldpandaid'] = job['jobsetid']
        retryquery['relationtype'] = 'jobset_retry'
        retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values()
        pretries = getSequentialRetries_ES(job['pandaid'], job['jobsetid'], job['jeditaskid'], countOfInvocations)

    query = {'jeditaskid': job_jeditaskid}
    jobslist = []
    for retry in pretries:
        jobslist.append(retry['oldpandaid'])
    for retry in retries:
        jobslist.append(retry['oldpandaid'])
    query['pandaid__in'] = jobslist

    jobs = []
    jobs.extend(Jobsdefined4.objects.filter(**query).values())
    jobs.extend(Jobsactive4.objects.filter(**query).values())
    jobs.extend(Jobswaiting4.objects.filter(**query).values())
    jobs.extend(Jobsarchived4.objects.filter(**query).values())
    jobs.extend(Jobsarchived.objects.filter(**query).values())
    jobs = cleanJobList(request, jobs, mode='nodrop')

    errors = {}

    for job in jobs:

        errors[job['pandaid']] = getErrorDescription(job, mode='txt')
    del request.session['TFIRST']
    del request.session['TLAST']
    response = render_to_response('descentJobsErrors.html', {'errors': errors}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def eventsInfo(request, mode=None, param=None):
    if not 'jeditaskid' in request.GET:
        data = {}
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

    jeditaskid = request.GET['jeditaskid']

    cur = connection.cursor()
    cur.execute(
        "select sum(decode(c.startevent,NULL,c.nevents,endevent-startevent+1)) nevents,c.status from atlas_panda.jedi_datasets d,atlas_panda.jedi_dataset_contents c where d.jeditaskid=c.jeditaskid and d.datasetid=c.datasetid and d.jeditaskid=%s and d.type in ('input','pseudo_input') and d.masterid is null group by c.status;" % (
        jeditaskid))
    events = cur.fetchall()
    cur.close()
    data = {}

    for ev in events:
        data[ev[1]] = ev[0]
    data['jeditaskid'] = jeditaskid

    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

@login_customrequired
@csrf_exempt
def jobInfo(request, pandaid=None, batchid=None, p2=None, p3=None, p4=None):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "jobInfo")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if data['eventservice'] == True:
            response = render_to_response('jobInfoES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    eventservice = False
    query = setupView(request, hours=365 * 24)
    jobid = ''
    if 'creator' in request.session['requestParams']:
        ## Find the job that created the specified file.
        fquery = {}
        fquery['lfn'] = request.session['requestParams']['creator']
        fquery['type'] = 'output'
        fileq = []
        fileq.extend(Filestable4.objects.filter(**fquery).values('pandaid', 'type', 'status'))
        if len(fileq) > 0:
            try:
                pandaid = next(filei['pandaid'] for filei in fileq if filei['status'] != 'failed')
            except:
                pandaid = None
        if not pandaid or len(fileq) == 0:
            fileq.extend(FilestableArch.objects.filter(**fquery).values('pandaid', 'type', 'status'))
            if fileq and len(fileq) > 0:
                try:
                    pandaid = next(filei['pandaid'] for filei in fileq if filei['status'] != 'failed')
                except:
                    pandaid = None

    if pandaid:
        jobid = pandaid
        try:
            query['pandaid'] = int(pandaid)
        except:
            query['jobname'] = pandaid
    if batchid:
        jobid = batchid
        query['batchid'] = batchid
    if 'pandaid' in request.session['requestParams']:
        try:
            pandaid = int(request.session['requestParams']['pandaid'])
        except ValueError:
            pandaid = 0
        jobid = pandaid
        query['pandaid'] = pandaid
    elif 'batchid' in request.session['requestParams']:
        batchid = request.session['requestParams']['batchid']
        jobid = "'" + batchid + "'"
        query['batchid'] = batchid
    elif 'jobname' in request.session['requestParams']:
        jobid = request.session['requestParams']['jobname']
        query['jobname'] = jobid

    jobs = []
    if pandaid or batchid:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
        jobs.extend(Jobsdefined4.objects.filter(**query).values())
        jobs.extend(Jobsactive4.objects.filter(**query).values())
        jobs.extend(Jobswaiting4.objects.filter(**query).values())
        jobs.extend(Jobsarchived4.objects.filter(**query).values())
        if len(jobs) == 0:
            try:
                del query['modificationtime__castdate__range']
            except:
                pass
            jobs.extend(Jobsarchived.objects.filter(**query).values())
        jobs = cleanJobList(request, jobs, mode='nodrop')

    if len(jobs) == 0:
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'prefix': getPrefix(request),
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'pandaid': pandaid,
            'job': None,
            'jobid': jobid,
        }
        response = render_to_response('jobInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    job = {}
    colnames = []
    columns = []
    harvesterInfo = {}
    rucioUserName = []

    if 'produserid' in jobs[0]:
        if 'prodsourcelabel' in jobs[0] and jobs[0]['prodsourcelabel'] == 'user':
            dn = jobs[0]['produserid']
            try:
                CNs = dn.split("/CN=")
                if len(CNs) > 1:
                    int(CNs[-1])
                    dn = dn[:-(len(CNs[-1])+4)]
            except ValueError:
                pass
            rw = ruciowrapper()
            rucioUserName = rw.getRucioAccountByDN(dn)
        else:
            rucioUserName = [jobs[0]['produserid']]

    try:
        job = jobs[0]
        tquery = {}
        tquery['jeditaskid'] = job['jeditaskid']
        tquery['storagetoken__isnull'] = False
        storagetoken = JediDatasets.objects.filter(**tquery).values('storagetoken')
        if storagetoken:
            job['destinationse'] = storagetoken[0]['storagetoken']
        ###Harvester section####
        from core.harvester.views import isHarvesterJob

        harvesterInfo = isHarvesterJob(job['pandaid'])
        if harvesterInfo == False:
           harvesterInfo = {}

        pandaid = job['pandaid']
        colnames = job.keys()
        colnames = sorted(colnames)
        produsername = ''
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = job[k].strftime(defaultDatetimeFormat)
                except:
                    val = job[k]
            else:
                val = job[k]
            if job[k] == None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
            if k == 'produsername':
                produsername = job[k]
    except IndexError:
        job = {}

    try:
        ## Check for logfile extracts
        logs = Logstable.objects.filter(pandaid=pandaid)
        if logs:
            logextract = logs[0].log1
        else:
            logextract = None
    except:
        traceback.print_exc(file=sys.stderr)
        logextract = None

    files = []
    fileids = []
    typeFiles = {}
    fileSummary = ''
    inputFilesSize = 0
    if 'nofiles' not in request.session['requestParams']:
        ## Get job files. First look in JEDI datasetcontents
        print ("Pulling file info")
        files.extend(Filestable4.objects.filter(pandaid=pandaid).order_by('type').values())
        ninput = 0
        noutput = 0
        npseudo_input = 0
        if len(files) > 0:
            for f in files:
                f['destination'] = ' '
                if f['type'] == 'input':
                    ninput += 1
                    inputFilesSize += f['fsize'] / 1048576.
                if f['type'] in typeFiles:
                    typeFiles[f['type']] += 1
                else:
                    typeFiles[f['type']] = 1
                if f['type'] == 'output':
                    noutput += 1
                    if len(jobs[0]['jobmetrics']) > 0:
                        for s in jobs[0]['jobmetrics'].split(' '):
                            if 'logBucketID' in s:
                                logBucketID = int(s.split('=')[1])
                                if logBucketID in [45, 41, 105, 106, 42, 61, 103, 2, 82, 101, 117,
                                                   115]:  # Bucket Codes for S3 destination
                                    f['destination'] = 'S3'

                                    # if len(jobs[0]['jobmetrics'])  > 0:
                                    #    jobmetrics = dict(s.split('=') for s in jobs[0]['jobmetrics'].split(' '))
                                    #    if 'logBucketID' in jobmetrics:
                                    #        if int(jobmetrics['logBucketID']) in [3, 21, 45, 46, 104, 41, 105, 106, 42, 61, 21, 102, 103, 2, 82, 81, 82, 101]: #Bucket Codes for S3 destination
                                    #            f['destination'] = 'S3'
                if f['type'] == 'pseudo_input': npseudo_input += 1
                f['fsizemb'] = "%0.2f" % (f['fsize'] / 1048576.)
                dsets = JediDatasets.objects.filter(datasetid=f['datasetid']).values()
                if len(dsets) > 0:
                    if  f['scope']+":" in f['dataset']:
                        f['datasetname'] = dsets[0]['datasetname']
                        f['ruciodatasetname'] = dsets[0]['datasetname'].split(":")[1]
                    else:
                        f['ruciodatasetname'] = dsets[0]['datasetname']
                        f['datasetname'] = dsets[0]['datasetname']
                    if job['computingsite'] in pandaSites.keys():
                        _, _, computeSvsAtlasS = getAGISSites()
                        f['ddmsite'] = computeSvsAtlasS.get(job['computingsite'], "")

                if 'dst' in f['destinationdblocktoken']:
                    parced = f['destinationdblocktoken'].split("_")
                    f['ddmsite'] = parced[0][4:]
                    f['dsttoken'] = parced[1]


            files = [x for x in files if x['destination'] != 'S3']


        if len(typeFiles) > 0:
            inputFilesSize = "%0.2f" % inputFilesSize
            for i in typeFiles:
                fileSummary += str(i) + ': ' + str(typeFiles[i])
                if (i == 'input'): fileSummary += ', size: ' + inputFilesSize + '(MB)'
                fileSummary += '; '
            fileSummary = fileSummary[:-2]
        if len(files) == 0:
            files.extend(FilestableArch.objects.filter(pandaid=pandaid).order_by('type').values())
        if len(files) > 0:
            for f in files:
                if 'creationdate' not in f: f['creationdate'] = f['modificationtime']
                if 'fileid' not in f: f['fileid'] = f['row_id']
                if 'datasetname' not in f:
                    if  f['scope']+":" in f['dataset']:
                        f['datasetname'] = f['dataset']
                        f['ruciodatasetname'] = f['dataset'].split(":")[1]
                    else:
                        f['datasetname'] = f['dataset']
                        f['ruciodatasetname'] = f['dataset']
                if 'modificationtime' in f: f['oldfiletable'] = 1
                if 'destinationdblock' in f and f['destinationdblock'] is not None:
                    f['destinationdblock_vis'] = f['destinationdblock'].split('_')[-1]

                fileids.append(f['fileid'])

            dcquery = {}
            dcquery['pandaid'] = pandaid
            dcquery['fileid__in'] = fileids
            dcfiles = JediDatasetContents.objects.filter(**dcquery).values()
            dcfilesDict = {}
            if len(dcfiles) > 0:
                for dcf in dcfiles:
                    dcfilesDict[dcf['fileid']] = dcf
        files = sorted(files, key=lambda x: x['type'])
    nfiles = len(files)
    logfile = {}
    for file in files:
        if file['type'] == 'log':
            logfile['lfn'] = file['lfn']
            logfile['guid'] = file['guid']
            if 'destinationse' in file:
                logfile['site'] = file['destinationse']
            else:
                logfilerec = Filestable4.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                if len(logfilerec) == 0:
                    logfilerec = FilestableArch.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
                if len(logfilerec) > 0:
                    logfile['site'] = logfilerec[0]['destinationse']
                    logfile['guid'] = logfilerec[0]['guid']
            logfile['scope'] = file['scope']
            logfile['fileid'] = file['fileid']
        file['fsize'] = int(file['fsize'])
        if file['type'] == 'input':
            file['attemptnr'] = dcfilesDict[file['fileid']]['attemptnr'] if file['fileid'] in dcfilesDict else file['attemptnr']
            file['maxattempt'] = dcfilesDict[file['fileid']]['maxattempt'] if file['fileid'] in dcfilesDict else None

    if 'pilotid' in job and job['pilotid'] is not None and job['pilotid'].startswith('http'):
        stdout = job['pilotid'].split('|')[0]
        stderr = stdout.replace('.out', '.err')
        stdlog = stdout.replace('.out', '.log')
        stdjdl = stdout.replace('.out', '.jdl')
    else:
        stdout = stderr = stdlog = stdjdl = None

    # input,pseudo_input,output,log and alphabetically within those please

    filesSorted = []
    filesSorted.extend(sorted([file for file in files if file['type'] == 'input'], key=lambda x: x['lfn']))
    filesSorted.extend(sorted([file for file in files if file['type'] == 'pseudo_input'], key=lambda x: x['lfn']))
    filesSorted.extend(sorted([file for file in files if file['type'] == 'output'], key=lambda x: x['lfn']))
    filesSorted.extend(sorted([file for file in files if file['type'] == 'log'], key=lambda x: x['lfn']))
    files = filesSorted

    inputfiles = [{'jeditaskid':file['jeditaskid'], 'datasetid':file['datasetid'], 'fileid':file['fileid']} for file in files if file['type'] == 'input']

    ## Check for object store based log
    oslogpath = None
    if 'computingsite' in job and job['computingsite'] in objectStores:
        ospath = objectStores[job['computingsite']]
        if 'lfn' in logfile:
            if ospath.endswith('/'):
                oslogpath = ospath + logfile['lfn']
            else:
                oslogpath = ospath + '/' + logfile['lfn']

    ## Check for debug info
    if 'specialhandling' in job and not job['specialhandling'] is None and job['specialhandling'].find('debug') >= 0:
        debugmode = True
    else:
        debugmode = False
    debugstdout = None
    if debugmode:
        if 'showdebug' in request.session['requestParams']:
            debugstdoutrec = Jobsdebug.objects.filter(pandaid=pandaid).values()
            if len(debugstdoutrec) > 0:
                if 'stdout' in debugstdoutrec[0]: debugstdout = debugstdoutrec[0]['stdout']

    if 'transformation' in job and job['transformation'] is not None and job['transformation'].startswith('http'):
        job['transformation'] = "<a href='%s'>%s</a>" % (job['transformation'], job['transformation'].split('/')[-1])

    if 'metastruct' in job:
        job['metadata'] = json.dumps(job['metastruct'], sort_keys=True, indent=4, separators=(',', ': '))

    ## Get job parameters
    print ("getting job parameters")
    jobparamrec = Jobparamstable.objects.filter(pandaid=pandaid)
    jobparams = None
    if len(jobparamrec) > 0:
        jobparams = jobparamrec[0].jobparameters
    # else:
    #    jobparamrec = JobparamstableArch.objects.filter(pandaid=pandaid)
    #    if len(jobparamrec) > 0:
    #        jobparams = jobparamrec[0].jobparameters

    dsfiles = []
    countOfInvocations = []
    ## If this is a JEDI job, look for job retries
    if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0:
        print ("looking for retries")
        ## Look for retries of this job


        if not isEventService(job):
            retryquery = {}
            retryquery['jeditaskid'] = job['jeditaskid']
            retryquery['oldpandaid'] = job['pandaid']
            retries = []
            retries.extend(JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values())
            pretries = getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations)
        else:
            retryquery = {}
            retryquery['jeditaskid'] = job['jeditaskid']
            retryquery['oldpandaid'] = job['jobsetid']
            retryquery['relationtype'] = 'jobset_retry'
            # retries = JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values()
            retries = getSequentialRetries_ESupstream(job['pandaid'], job['jobsetid'], job['jeditaskid'],
                                                      countOfInvocations)
            pretries = getSequentialRetries_ES(job['pandaid'], job['jobsetid'], job['jeditaskid'], countOfInvocations)
    else:
        retries = None
        pretries = None

    countOfInvocations = len(countOfInvocations)

    ## jobset info
    libjob = None
    runjobs = []
    mergejobs = []
    if 'jobset' in request.session['requestParams'] and 'jobsetid' in job and job['jobsetid'] > 0:
        print ("jobset info")
        jsquery = {}
        jsquery['jobsetid'] = job['jobsetid']
        jsquery['produsername'] = job['produsername']
        values = ['pandaid', 'prodsourcelabel', 'processingtype', 'transformation']
        jsjobs = []
        jsjobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsactive4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*values))
        jsjobs.extend(Jobsarchived.objects.filter(**jsquery).values(*values))
        if len(jsjobs) > 0:
            for j in jsjobs:
                id = j['pandaid']
                if j['transformation'].find('runAthena') >= 0:
                    runjobs.append(id)
                elif j['transformation'].find('buildJob') >= 0:
                    libjob = id
                if j['processingtype'] == 'usermerge':
                    mergejobs.append(id)


    esjobstr = ''
    if isEventService(job):
        ## for ES jobs, prepare the event table
        esjobdict = {}
        for s in eventservicestatelist:
            esjobdict[s] = 0
        evalues = 'fileid', 'datasetid', 'def_min_eventid', 'def_max_eventid', 'processed_upto_eventid', 'status', 'job_processid', 'attemptnr', 'eventoffset'
        evtable = JediEvents.objects.filter(pandaid=job['pandaid']).order_by('-def_min_eventid').values(*evalues)
        fileids = {}
        datasetids = {}
        # for evrange in evtable:
        #    fileids[int(evrange['fileid'])] = {}
        #    datasetids[int(evrange['datasetid'])] = {}
        flist = []
        for f in fileids:
            flist.append(f)
        dslist = []
        for ds in datasetids:
            dslist.append(ds)
        # datasets = JediDatasets.objects.filter(datasetid__in=dslist).values()
        dsfiles = JediDatasetContents.objects.filter(fileid__in=flist).values()
        # for ds in datasets:
        #    datasetids[int(ds['datasetid'])]['dict'] = ds
        # for f in dsfiles:
        #    fileids[int(f['fileid'])]['dict'] = f

        for evrange in evtable:
            # evrange['fileid'] = fileids[int(evrange['fileid'])]['dict']['lfn']
            # evrange['datasetid'] = datasetids[evrange['datasetid']]['dict']['datasetname']
            evrange['status'] = eventservicestatelist[evrange['status']]
            esjobdict[evrange['status']] += 1
            evrange['attemptnr'] = 10 - evrange['attemptnr']

        esjobstr = ''
        for s in esjobdict:
            if esjobdict[s] > 0:
                esjobstr += " %s(%s) " % (s, esjobdict[s])
    else:
        evtable = []

    runesjobs = []
    mergeesjobs = []
    if isEventService(job) and 'jobsetid' in job and job['jobsetid'] > 0:
        print ("jobset info")
        esjsquery = {}
        esjsquery['jobsetid'] = job['jobsetid']
        esjsquery['produsername'] = job['produsername']
        values = ['pandaid', 'eventservice']
        esjsjobs = []
        esjsjobs.extend(Jobsdefined4.objects.filter(**esjsquery).values(*values))
        esjsjobs.extend(Jobsactive4.objects.filter(**esjsquery).values(*values))
        esjsjobs.extend(Jobswaiting4.objects.filter(**esjsquery).values(*values))
        esjsjobs.extend(Jobsarchived4.objects.filter(**esjsquery).values(*values))
        esjsjobs.extend(Jobsarchived.objects.filter(**esjsquery).values(*values))
        if len(esjsjobs) > 0:
            for j in esjsjobs:
                if j['eventservice'] == 1:
                    runesjobs.append(j['pandaid'])
                if j['eventservice'] == 2:
                    mergeesjobs.append(j['pandaid'])

    ## For CORE, pick up parameters from jobparams
    if VOMODE == 'core' or ('vo' in job and job['vo'] == 'core'):
        coreData = {}
        if jobparams:
            coreParams = re.match(
                '.*PIPELINE_TASK\=([a-zA-Z0-9]+).*PIPELINE_PROCESSINSTANCE\=([0-9]+).*PIPELINE_STREAM\=([0-9\.]+)',
                jobparams)
            if coreParams:
                coreData['pipelinetask'] = coreParams.group(1)
                coreData['processinstance'] = coreParams.group(2)
                coreData['pipelinestream'] = coreParams.group(3)
    else:
        coreData = None

    if 'jobstatus' in job and (job['jobstatus'] == 'failed' or job['jobstatus'] == 'holding'):
        errorinfo = getErrorDescription(job)
        if len(errorinfo) > 0:
            job['errorinfo'] = errorinfo
    delta = -1
    if job['creationtime']:
        creationtime = job['creationtime']
        now = datetime.now()
        tdelta = now - creationtime
        delta = int(tdelta.days) + 1
        job['creationtime'] = job['creationtime'].strftime(defaultDatetimeFormat)
    if job['modificationtime']:
        job['modificationtime'] = job['modificationtime'].strftime(defaultDatetimeFormat)
    if job['statechangetime']:
        job['statechangetime'] = job['statechangetime'].strftime(defaultDatetimeFormat)

    isincomparisonlist = False
    clist = []
    if request.user.is_authenticated and request.user.is_tester:
        cquery = {}
        cquery['object'] = 'job'
        cquery['userid'] = request.user.id
        try:
            jobsComparisonList = ObjectsComparison.objects.get(**cquery)
        except ObjectsComparison.DoesNotExist:
            jobsComparisonList = None

        if jobsComparisonList:
            try:
                clist = json.loads(jobsComparisonList.comparisonlist)
                newlist = []
                for ce in clist:
                    try:
                        ceint = int(ce)
                        newlist.append(ceint)
                    except:
                        pass
                clist = newlist
            except:
                clist = []
            if job['pandaid'] in clist:
                isincomparisonlist = True


    # if it is ART test, get test name
    art_test = []
    artqueue = {'pandaid': pandaid}
    art_test.extend(ARTTests.objects.filter(**artqueue).values())


    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'pandaid': pandaid,
            'job': job,
            'columns': columns,
            'arttest': art_test,
            'files': files,
            'dsfiles': dsfiles,
            'nfiles': nfiles,
            'logfile': logfile,
            'oslogpath': oslogpath,
            'stdout': stdout,
            'stderr': stderr,
            'stdlog': stdlog,
            'stdjdl': stdjdl,
            'jobparams': jobparams,
            'jobid': jobid,
            'coreData': coreData,
            'logextract': logextract,
            'retries': retries,
            'pretries': pretries,
            'countOfInvocations': countOfInvocations,
            'eventservice': isEventService(job),
            'evtable': evtable[:100],
            'debugmode': debugmode,
            'debugstdout': debugstdout,
            'libjob': libjob,
            'runjobs': runjobs,
            'mergejobs': mergejobs,
            'runesjobs': runesjobs,
            'mergeesjobs': mergeesjobs,
            'esjobstr': esjobstr,
            'fileSummary': fileSummary,
            'built': datetime.now().strftime("%H:%M:%S"),
            'produsername':produsername,
            'harvesterInfo':harvesterInfo,
            'isincomparisonlist': isincomparisonlist,
            'clist': clist,
            'timedelta': delta,
            'inputfiles': inputfiles,
            'rucioUserName':rucioUserName
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobInfo", json.dumps(data, cls=DateEncoder), 60 * 20)
        if isEventService(job):
            response = render_to_response('jobInfoES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif (
        ('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {'files': files,
                'job': job,
                'dsfiles': dsfiles,
                }

        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse('not understood', content_type='text/html')


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

@login_customrequired
def userList(request):
    valid, response = initRequest(request)
    if not valid: return response
    nhours = 90 * 24

    setupView(request, hours=nhours, limit=-99)
    if VOMODE == 'atlas':
        view = 'database'
    else:
        view = 'dynamic'
    if 'view' in request.session['requestParams']:
        view = request.session['requestParams']['view']
    sumd = []
    jobsumd = []
    userdb = []
    userdbl = []
    userstats = {}
    if view == 'database':
        startdate = timezone.now() - timedelta(hours=nhours)
        startdate = startdate.strftime(defaultDatetimeFormat)
        enddate = timezone.now().strftime(defaultDatetimeFormat)
        query = {'lastmod__range': [startdate, enddate]}
        # viewParams['selection'] = ", last %d days" % (float(nhours)/24.)
        ## Use the users table
        if 'sortby' in request.session['requestParams']:
            sortby = request.session['requestParams']['sortby']
            if sortby == 'name':
                userdb = Users.objects.filter(**query).order_by('name')
            elif sortby == 'njobs':
                userdb = Users.objects.filter(**query).order_by('njobsa').reverse()
            elif sortby == 'date':
                userdb = Users.objects.filter(**query).order_by('lastmod').reverse()
            elif sortby == 'cpua1':
                userdb = Users.objects.filter(**query).order_by('cpua1').reverse()
            elif sortby == 'cpua7':
                userdb = Users.objects.filter(**query).order_by('cpua7').reverse()
            elif sortby == 'cpup1':
                userdb = Users.objects.filter(**query).order_by('cpup1').reverse()
            elif sortby == 'cpup7':
                userdb = Users.objects.filter(**query).order_by('cpup7').reverse()
            else:
                userdb = Users.objects.filter(**query).order_by('name')
        else:
            userdb = Users.objects.filter(**query).order_by('name')

        anajobs = 0
        n1000 = 0
        n10k = 0
        nrecent3 = 0
        nrecent7 = 0
        nrecent30 = 0
        nrecent90 = 0
        ## Move to a list of dicts and adjust CPU unit
        for u in userdb:
            u.latestjob = u.lastmod
            udict = {}
            udict['name'] = u.name
            udict['njobsa'] = u.njobsa
            if u.cpua1: udict['cpua1'] = "%0.1f" % (int(u.cpua1) / 3600.)
            if u.cpua7: udict['cpua7'] = "%0.1f" % (int(u.cpua7) / 3600.)
            if u.cpup1: udict['cpup1'] = "%0.1f" % (int(u.cpup1) / 3600.)
            if u.cpup7: udict['cpup7'] = "%0.1f" % (int(u.cpup7) / 3600.)
            if u.latestjob:
                udict['latestjob'] = u.latestjob.strftime(defaultDatetimeFormat)
                udict['lastmod'] = u.lastmod.strftime(defaultDatetimeFormat)
            userdbl.append(udict)
            if u.njobsa is not None:
                if u.njobsa > 0: anajobs += u.njobsa
                if u.njobsa >= 1000: n1000 += 1
                if u.njobsa >= 10000: n10k += 1
            if u.latestjob != None:
                latest = timezone.now() - u.latestjob
                if latest.days < 4: nrecent3 += 1
                if latest.days < 8: nrecent7 += 1
                if latest.days < 31: nrecent30 += 1
                if latest.days < 91: nrecent90 += 1
        userstats['anajobs'] = anajobs
        userstats['n1000'] = n1000
        userstats['n10k'] = n10k
        userstats['nrecent3'] = nrecent3
        userstats['nrecent7'] = nrecent7
        userstats['nrecent30'] = nrecent30
        userstats['nrecent90'] = nrecent90
    else:
        if VOMODE == 'atlas':
            nhours = 12
        else:
            nhours = 7 * 24
        query = setupView(request, hours=nhours, limit=5000)
        ## dynamically assemble user summary info
        values = 'eventservice', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'processingtype', 'workinggroup', 'currentpriority'
        jobs = QuerySetChain( \
            Jobsdefined4.objects.filter(**query).order_by('-modificationtime')[:request.session['JOB_LIMIT']].values(
                *values),
            Jobsactive4.objects.filter(**query).order_by('-modificationtime')[:request.session['JOB_LIMIT']].values(
                *values),
            Jobswaiting4.objects.filter(**query).order_by('-modificationtime')[:request.session['JOB_LIMIT']].values(
                *values),
            Jobsarchived4.objects.filter(**query).order_by('-modificationtime')[:request.session['JOB_LIMIT']].values(
                *values),
        )
        jobs = cleanJobList(request, jobs, doAddMeta=False)
        sumd = userSummaryDict(jobs)
        for user in sumd:
            if user['dict']['latest']:
                user['dict']['latest'] = user['dict']['latest'].strftime(defaultDatetimeFormat)
        sumparams = ['jobstatus', 'prodsourcelabel', 'specialhandling', 'transformation', 'processingtype',
                     'workinggroup', 'priorityrange', 'jobsetrange']
        if VOMODE == 'atlas':
            sumparams.append('atlasrelease')
        else:
            sumparams.append('vo')

        jobsumd = jobSummaryDict(request, jobs, sumparams)[0]

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        TFIRST = request.session['TFIRST']
        TLAST = request.session['TLAST']
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': extensibleURL(request),
            'url': request.path,
            'sumd': sumd,
            'jobsumd': jobsumd,
            'userdb': userdbl,
            'userstats': userstats,
            'tfirst': TFIRST.strftime(defaultDatetimeFormat),
            'tlast': TLAST.strftime(defaultDatetimeFormat),
            'plow': PLOW,
            'phigh': PHIGH,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('userList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif (
        ('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = sumd
        return HttpResponse(json.dumps(resp), content_type='application/json')

#@login_required(login_url='loginauth2')
@login_customrequired
def userInfo(request, user=''):
    valid, response = initRequest(request)
    if not valid: return response
    fullname = ''
    login = ''
    userQueryTask = None
    userQueryJobs = None

    if user == '':
        if 'user' in request.session['requestParams']: user = request.session['requestParams']['user']
        if 'produsername' in request.session['requestParams']: user = request.session['requestParams']['produsername']

        # Here we serve only personal user pages. No user parameter specified
        if user == '':
            if request.user.is_authenticated:
                login = user = request.user.username
                fullname = request.user.first_name.replace('\'', '') + ' ' + request.user.last_name
                userQueryTask = Q(username=login) | Q(username__startswith=fullname)
                userQueryJobs = Q(produsername=login) | Q(produsername__startswith=fullname)

    if 'days' in request.session['requestParams']:
        days = int(request.session['requestParams']['days'])
    else:
        days = 7

    requestParams = {}
    for param in request.session['requestParams']:
        requestParams[escapeInput(param.strip())] = escapeInput(request.session['requestParams'][param.strip()].strip())
    request.session['requestParams'] = requestParams

    ## Tasks owned by the user
    query = setupView(request, hours=72, limit=999999, querytype='task')
    startdate = timezone.now() - timedelta(hours=days * 24)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)

    if 'date_from' in request.session['requestParams']:
        time_from_struct = time.strptime(request.session['requestParams']['date_from'], '%Y-%m-%d')
        startdate = datetime.utcfromtimestamp(time.mktime(time_from_struct))
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    # startdate = startdate.strftime(defaultDatetimeFormat)
    if 'date_to' in request.session['requestParams']:
        time_from_struct = time.strptime(request.session['requestParams']['date_to'], '%Y-%m-%d')
        enddate = datetime.utcfromtimestamp(time.mktime(time_from_struct))
    if 'earlierthan' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthan']))
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if 'earlierthandays' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthandays']) * 24)
    # enddate = enddate.strftime(defaultDatetimeFormat)
    if enddate == None:
        enddate = timezone.now()  # .strftime(defaultDatetimeFormat)

    query['modificationtime__castdate__range'] = [startdate, enddate]

    if userQueryTask is None:
        query['username__icontains'] = user.strip()
        tasks = JediTasks.objects.filter(**query).values()
    else:
        tasks = JediTasks.objects.filter(**query).filter(userQueryTask).values()


    tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])
    tasks = cleanTaskList(request, tasks)
    ntasks = len(tasks)
    tasksumd = taskSummaryDict(request, tasks)

    if 'display_limit_tasks' not in request.session['requestParams']:
        display_limit_tasks = 100
    else:
        display_limit_tasks = int(request.session['requestParams']['display_limit_tasks'])
    ntasksmax = display_limit_tasks
    url_nolimit_tasks = request.get_full_path() + "&display_limit_tasks=" + str(ntasks)

    tasks = getTaskScoutingInfo(tasks, ntasksmax)

    timestamp_vars = ['modificationtime', 'statechangetime', 'starttime']
    for task in tasks:
        for tsv in timestamp_vars:
            if tsv in task and task[tsv]:
                task[tsv] = task[tsv].strftime(defaultDatetimeFormat)

    ## Jobs
    limit = 5000
    query = setupView(request, hours=72, limit=limit, querytype='job')
    jobs = []
    values = 'eventservice', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock',

    if userQueryJobs is None:
        query['produsername__icontains'] = user.strip()
        jobs.extend(Jobsdefined4.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))
        if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
            jobs.extend(Jobsarchived.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))
    else:
        jobs.extend(Jobsdefined4.objects.filter(**query).filter(userQueryJobs)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**query).filter(userQueryJobs)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query).filter(userQueryJobs)[:request.session['JOB_LIMIT']].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query).filter(userQueryJobs)[:request.session['JOB_LIMIT']].values(*values))


        # Here we go to an archive. Separation OR condition is done to enforce Oracle to perform indexed search.
        if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
            query['produsername__startswith'] = user.strip() #.filter(userQueryJobs)
            archjobs = []
            # This two filters again to force Oracle search
            archjobs.extend(Jobsarchived.objects.filter(**query).filter(Q(produsername=user.strip()))[:request.session['JOB_LIMIT']].values(*values))
            if len(archjobs) > 0:
                jobs = jobs+archjobs
            elif len(fullname) > 0:
                #del query['produsername']
                query['produsername__startswith'] = fullname
                jobs.extend(Jobsarchived.objects.filter(**query)[:request.session['JOB_LIMIT']].values(*values))


    jobsetids = None
    # if len(jobs) < limit and ntasks == 0:
    #             ## try at least to find some old jobsets
    #             startdate = timezone.now() - timedelta(hours=30*24)
    #             startdate = startdate.strftime(defaultDatetimeFormat)
    #             enddate = timezone.now().strftime(defaultDatetimeFormat)
    #             query = { 'modificationtime__range' : [startdate, enddate] }
    #             query['produsername'] = user
    #             jobsetids = Jobsarchived.objects.filter(**query).values('jobsetid').distinct()

    jobs = cleanJobList(request, jobs, doAddMeta=False)
    if fullname != '':
        query = {'name': fullname}
    else:
        query = {'name__icontains': user.strip()}
    userdb = Users.objects.filter(**query).values()
    if len(userdb) > 0:
        userstats = userdb[0]
        user = userstats['name']
        for field in ['cpua1', 'cpua7', 'cpup1', 'cpup7']:
            try:
                userstats[field] = "%0.1f" % (float(userstats[field]) / 3600.)
            except:
                userstats[field] = '-'
        for timefield in ['cachetime', 'firstjob', 'lastmod', 'latestjob']:
            try:
                userstats[timefield] = userstats[timefield].strftime(defaultDatetimeFormat)
            except:
                userstats[timefield] = userstats[timefield]
    else:
        userstats = None

    ## Divide up jobs by jobset and summarize
    jobsets = {}
    for job in jobs:
        if 'jobsetid' not in job or job['jobsetid'] == None: continue
        if job['jobsetid'] not in jobsets:
            jobsets[job['jobsetid']] = {}
            jobsets[job['jobsetid']]['jobsetid'] = job['jobsetid']
            jobsets[job['jobsetid']]['jobs'] = []
        jobsets[job['jobsetid']]['jobs'].append(job)
    for jobset in jobsets:
        jobsets[jobset]['sum'] = jobStateSummary(jobsets[jobset]['jobs'])
        jobsets[jobset]['njobs'] = len(jobsets[jobset]['jobs'])
        tfirst = timezone.now()
        tlast = timezone.now() - timedelta(hours=2400)
        plow = 1000000
        phigh = -1000000
        for job in jobsets[jobset]['jobs']:
            if job['modificationtime'] > tlast: tlast = job['modificationtime']
            if job['modificationtime'] < tfirst: tfirst = job['modificationtime']
            if job['currentpriority'] > phigh: phigh = job['currentpriority']
            if job['currentpriority'] < plow: plow = job['currentpriority']
        jobsets[jobset]['tfirst'] = tfirst.strftime(defaultDatetimeFormat)
        jobsets[jobset]['tlast'] = tlast.strftime(defaultDatetimeFormat)
        jobsets[jobset]['plow'] = plow
        jobsets[jobset]['phigh'] = phigh
    jobsetl = []
    jsk = jobsets.keys()
    jsk = sorted(jsk, reverse=True)
    for jobset in jsk:
        jobsetl.append(jobsets[jobset])

    njobsmax = len(jobs)
    if 'display_limit_jobs' in request.session['requestParams'] and int(
            request.session['requestParams']['display_limit_jobs']) < len(jobs):
        display_limit_jobs = int(request.session['requestParams']['display_limit_jobs'])
        njobsmax = display_limit_jobs
        url_nolimit_jobs = removeParam(request.get_full_path(), 'display_limit_jobs') + 'display_limit_jobs=' + str(len(jobs))
    else:
        display_limit_jobs = 100
        njobsmax = display_limit_jobs
        url_nolimit_jobs = request.get_full_path() + 'display_limit_jobs=' + str(len(jobs))

    links = ''
    # getting most relevant links based on visit statistics
    if request.user.is_authenticated:
        userids = BPUser.objects.filter(email=request.user.email).values('id')
        userid = userids[0]['id']
        fields = {'job': standard_fields, 'task': copy.deepcopy(standard_taskfields), 'site': standard_sitefields}
        links = get_relevant_links(userid, fields)

    sumd = userSummaryDict(jobs)
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        flist = ['jobstatus', 'prodsourcelabel', 'processingtype', 'specialhandling', 'transformation', 'jobsetid',
                 'jeditaskid', 'computingsite', 'cloud', 'workinggroup', 'homepackage', 'inputfileproject',
                 'inputfiletype', 'attemptnr', 'priorityrange', 'jobsetrange']
        if VOMODE != 'atlas':
            flist.append('vo')
        else:
            flist.append('atlasrelease')
        jobsumd, esjobssumd = jobSummaryDict(request, jobs, flist)
        njobsetmax = 100
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')

        timestamp_vars = ['modificationtime', 'creationtime']
        for job in jobs:
            for tsv in timestamp_vars:
                if tsv in job and job[tsv]:
                    job[tsv] = job[tsv].strftime(defaultDatetimeFormat)

        TFIRST = request.session['TFIRST']
        TLAST = request.session['TLAST']
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'xurl': xurl,
            'nosorturl': nosorturl,
            'user': user,
            'sumd': sumd,
            'jobsumd': jobsumd,
            'jobList': jobs[:njobsmax],
            'njobs': len(jobs),
            'display_limit_jobs': display_limit_jobs,
            'url_nolimit_jobs': url_nolimit_jobs,
            'query': query,
            'userstats': userstats,
            'tfirst': TFIRST.strftime(defaultDatetimeFormat),
            'tlast': TLAST.strftime(defaultDatetimeFormat),
            'plow': PLOW,
            'phigh': PHIGH,
            'jobsets': jobsetl[:njobsetmax - 1],
            'njobsetmax': njobsetmax,
            'njobsets': len(jobsetl),
            'url_nolimit_tasks': url_nolimit_tasks,
            'display_limit_tasks': display_limit_tasks,
            'tasks': tasks[:ntasksmax],
            'ntasks': ntasks,
            'tasksumd': tasksumd,
            'built': datetime.now().strftime("%H:%M:%S"),
            'links' : links,
        }
        data.update(getContextVariables(request))
        response = render_to_response('userInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = sumd
        return HttpResponse(json.dumps(resp,default=datetime_handler),content_type='application/json')

@login_customrequired
def siteList(request):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "siteList")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    for param in request.session['requestParams']:
        request.session['requestParams'][param] = escapeInput(request.session['requestParams'][param])
    setupView(request, opmode='notime')
    query = {}
    ### Add any extensions to the query determined from the URL
    if VOMODE == 'core': query['siteid__contains'] = 'CORE'
    prod = False
    extraParCondition = '1=1'
    for param in request.session['requestParams']:
        if param == 'category' and request.session['requestParams'][param] == 'multicloud':
            query['multicloud__isnull'] = False
        if param == 'category' and request.session['requestParams'][param] == 'analysis':
            query['siteid__contains'] = 'ANALY'
        if param == 'category' and request.session['requestParams'][param] == 'test':
            query['siteid__icontains'] = 'test'
        if param == 'category' and request.session['requestParams'][param] == 'production':
            prod = True
        if param == 'catchall':
            wildCards = request.session['requestParams'][param].split('|')
            countCards = len(wildCards)
            currentCardCount = 1
            extraParCondition = '('
            for card in wildCards:
                extraParCondition += preprocessWildCardString(escapeInput(card), 'catchall')
                if (currentCardCount < countCards): extraParCondition += ' OR '
                currentCardCount += 1
            extraParCondition += ')'

        for field in Schedconfig._meta.get_fields():
            if param == field.name and not (param == 'catchall'):
                query[param] = escapeInput(request.session['requestParams'][param])

    siteres = Schedconfig.objects.filter(**query).exclude(cloud='CMS').extra(where=[extraParCondition]).values()
    mcpres = Schedconfig.objects.filter(status='online').exclude(cloud='CMS').exclude(siteid__icontains='test').values(
        'siteid', 'multicloud', 'cloud').order_by('siteid')
    sites = []
    for site in siteres:
        if 'category' in request.session['requestParams'] and request.session['requestParams'][
            'category'] == 'multicloud':
            if (site['multicloud'] == 'None') or (not re.match('[A-Z]+', site['multicloud'])): continue
        sites.append(site)
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'maxmemory':
        sites = sorted(sites, key=lambda x: -x['maxmemory'])
    elif 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'maxtime':
        sites = sorted(sites, key=lambda x: -x['maxtime'])
    elif 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'gocname':
        sites = sorted(sites, key=lambda x: x['gocname'])
    else:
        sites = sorted(sites, key=lambda x: x['siteid'])
    if prod:
        newsites = []
        for site in sites:
            if site['siteid'].find('ANALY') >= 0:
                pass
            elif site['siteid'].lower().find('test') >= 0:
                pass
            else:
                newsites.append(site)
        sites = newsites
    for site in sites:
        if site['maxtime'] and (site['maxtime'] > 0): site['maxtime'] = "%.1f" % (float(site['maxtime']) / 3600.)
        site['space'] = "%d" % (site['space'] / 1000.)

    if VOMODE == 'atlas' and (
            len(request.session['requestParams']) == 0 or 'cloud' in request.session['requestParams']):
        clouds = Cloudconfig.objects.filter().exclude(name='CMS').exclude(name='OSG').values()
        clouds = sorted(clouds, key=lambda x: x['name'])
        mcpsites = {}
        for cloud in clouds:
            cloud['display'] = True
            if 'cloud' in request.session['requestParams'] and request.session['requestParams']['cloud'] != cloud[
                'name']: cloud['display'] = False
            mcpsites[cloud['name']] = []
            for site in sites:
                if site['siteid'] == cloud['tier1']:
                    cloud['space'] = site['space']
                    cloud['tspace'] = site['tspace'].strftime("%m-%d %H:%M")
            for site in mcpres:
                mcpclouds = site['multicloud'].split(',')
                if cloud['name'] in mcpclouds or cloud['name'] == site['cloud']:
                    sited = {}
                    sited['name'] = site['siteid']
                    sited['cloud'] = site['cloud']
                    if site['cloud'] == cloud['name']:
                        sited['type'] = 'home'
                    else:
                        sited['type'] = 'mcp'
                    mcpsites[cloud['name']].append(sited)
            cloud['mcpsites'] = ''
            for s in mcpsites[cloud['name']]:
                if s['type'] == 'home':
                    cloud['mcpsites'] += "<b>%s</b>     " % s['name']
                else:
                    cloud['mcpsites'] += "%s     " % s['name']
            if cloud['modtime']:
                cloud['modtime'] = cloud['modtime'].strftime("%m-%d %H:%M")
    else:
        clouds = None
    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        sumd = siteSummaryDict(sites)
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'sites': sites,
            'clouds': clouds,
            'sumd': sumd,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        if 'cloud' in request.session['requestParams']: data['mcpsites'] = mcpsites[
            request.session['requestParams']['cloud']]
        # data.update(getContextVariables(request))
        ##self monitor
        setCacheEntry(request, "siteList", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render_to_response('siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = sites
        return HttpResponse(json.dumps(resp, cls=DateEncoder), content_type='application/json')

def get_panda_resource(siterec):
    url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas"
    http = urllib3.PoolManager()
    data = {}
    try:
        r = http.request('GET', url)
        data = json.loads(r.data.decode('utf-8'))
        for cs in data.keys():
            # if (data[cs]['siteid']!=data[cs]['panda_resource']):
            #     print data[cs]['siteid']
            if (data[cs] and siterec.siteid == data[cs]['siteid']):
            #     # resourcesDict.setdefault(data[cs]['resourcetype'], []).append(cs)
            #     # resourcesDictSites[data[cs]['siteid']] = data[cs]['panda_resource']
                return data[cs]['panda_resource']
    except Exception as exc:
        print (exc)
    #return resourcesDictSites

@login_customrequired
def siteInfo(request, site=''):
    valid, response = initRequest(request)
    if not valid: return response
    if site == '' and 'site' in request.session['requestParams']: site = request.session['requestParams']['site']
    setupView(request)
    LAST_N_HOURS_MAX = 12
    startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query = {'siteid__iexact': site}
    sites = Schedconfig.objects.filter(**query)
    colnames = []
    try:
        siterec = sites[0]
        colnames = siterec.get_all_fields()
        if sites[0].lastmod:
            sites[0].lastmod = sites[0].lastmod.strftime(defaultDatetimeFormat)
    except IndexError:
        siterec = None
    if len(sites) > 1:
        for queue in sites:
            if queue['lastmod']:
                queue['lastmod'] = queue['lastmod'].strftime(defaultDatetimeFormat)


    # get data from new schedconfig_json table
    panda_queue = []
    pqquery = {'pandaqueue': site}
    panda_queues = SchedconfigJson.objects.filter(**pqquery).values()
    if len(panda_queues) > 0:
        panda_queue_dict = json.loads(panda_queues[0]['data'])
        for par, val in panda_queue_dict.items():
            val = ', '.join([str(subpar) + ' = ' + str(subval) for subpar, subval in val.items()]) if isinstance(val, dict) else val
            panda_queue.append({'param': par, 'value': val})

    panda_queue = sorted(panda_queue, key=lambda x: x['param'])

    HPC = False
    njobhours = 12
    try:
        if siterec.catchall.find('HPC') >= 0:
            HPC = True
            njobhours = 48
    except AttributeError:
        pass
    panda_resource = get_panda_resource(siterec)
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        attrs = []
        if siterec:
            attrs.append({'name': 'GOC name', 'value': siterec.gocname})
            if HPC: attrs.append(
                {'name': 'HPC', 'value': 'This is a High Performance Computing (HPC) supercomputer queue'})
            if siterec.catchall and siterec.catchall.find('log_to_objectstore') >= 0:
                attrs.append({'name': 'Object store logs', 'value': 'Logging to object store is enabled'})
            if siterec.objectstore and len(siterec.objectstore) > 0:
                fields = siterec.objectstore.split('|')
                nfields = len(fields)
                for nf in range(0, len(fields)):
                    if nf == 0:
                        attrs.append({'name': 'Object store location', 'value': fields[0]})
                    else:
                        fields2 = fields[nf].split('^')
                        if len(fields2) > 1:
                            ostype = fields2[0]
                            ospath = fields2[1]
                            attrs.append({'name': 'Object store %s path' % ostype, 'value': ospath})

            if siterec.nickname != site:
                attrs.append({'name': 'Queue (nickname)', 'value': siterec.nickname})
            if len(sites) > 1:
                attrs.append({'name': 'Total queues for this site', 'value': len(sites)})
            attrs.append({'name': 'Status', 'value': siterec.status})
            if siterec.comment_field and len(siterec.comment_field) > 0:
                attrs.append({'name': 'Comment', 'value': siterec.comment_field})
            attrs.append({'name': 'Cloud', 'value': siterec.cloud})
            if siterec.multicloud and len(siterec.multicloud) > 0:
                attrs.append({'name': 'Multicloud', 'value': siterec.multicloud})
            attrs.append({'name': 'Tier', 'value': siterec.tier})
            attrs.append({'name': 'DDM endpoint', 'value': siterec.ddm})
            attrs.append({'name': 'Max rss', 'value': "%.1f GB" % (float(siterec.maxrss) / 1000.)})
            attrs.append({'name': 'Min rss', 'value': "%.1f GB" % (float(siterec.minrss) / 1000.)})
            if siterec.maxtime > 0:
                attrs.append({'name': 'Maximum time', 'value': "%.1f hours" % (float(siterec.maxtime) / 3600.)})
            attrs.append({'name': 'Space', 'value': "%d TB as of %s" % (
            (float(siterec.space) / 1000.), siterec.tspace.strftime('%m-%d %H:%M'))})
            attrs.append({'name': 'Last modified', 'value': "%s" % (siterec.lastmod.strftime('%Y-%m-%d %H:%M'))})

            iquery = {}

            startdate = timezone.now() - timedelta(hours=24 * 30)
            startdate = startdate.strftime(defaultDatetimeFormat)
            enddate = timezone.now().strftime(defaultDatetimeFormat)
            iquery['at_time__range'] = [startdate, enddate]
            cloudQuery = Q(description__contains='queue=%s' % siterec.nickname) | Q(
                description__contains='queue=%s' % siterec.siteid)
            incidents = Incidents.objects.filter(**iquery).filter(cloudQuery).order_by('at_time').reverse().values()
            for inc in incidents:
                if inc['at_time']:
                    inc['at_time'] = inc['at_time'].strftime(defaultDatetimeFormat)
        else:
            incidents = []
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'site': siterec,
            'panda_resource':panda_resource,
            'queues': sites,
            'colnames': colnames,
            'attrs': attrs,
            'incidents': incidents,
            'name': site,
            'njobhours': njobhours,
            'built': datetime.now().strftime("%H:%M:%S"),
            'pandaqueue': panda_queue,
        }
        data.update(getContextVariables(request))
        response = render_to_response('siteInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = []
        for job in jobList:
            resp.append({'pandaid': job.pandaid, 'status': job.jobstatus, 'prodsourcelabel': job.prodsourcelabel,
                         'produserid': job.produserid})
        return HttpResponse(json.dumps(resp), content_type='application/json')


def updateCacheWithListOfMismatchedCloudSites(mismatchedSites):
    try:
        listOfCloudSitesMismatched = cache.get('mismatched-cloud-sites-list')
    except:
        listOfCloudSitesMismatched = None
    if (listOfCloudSitesMismatched is None) or (len(listOfCloudSitesMismatched) == 0):
        cache.set('mismatched-cloud-sites-list', mismatchedSites, 31536000)
    else:
        listOfCloudSitesMismatched.extend(mismatchedSites)
        listOfCloudSitesMismatched = sorted(listOfCloudSitesMismatched)
        cache.set('mismatched-cloud-sites-list', list(listOfCloudSitesMismatched for listOfCloudSitesMismatched, _ in
                                                      itertools.groupby(listOfCloudSitesMismatched)), 31536000)


def getListOfFailedBeforeSiteAssignedJobs(query, mismatchedSites, notime=True):
    jobs = []
    querynotime = copy.deepcopy(query)
    if notime: del querynotime['modificationtime__castdate__range']
    siteCondition = ''
    for site in mismatchedSites:
        siteQuery = Q(computingsite=site[0]) & Q(cloud=site[1])
        siteCondition = siteQuery if (siteCondition == '') else (siteCondition | siteQuery)
    jobs.extend(Jobsactive4.objects.filter(siteCondition).filter(**querynotime).values('pandaid'))
    jobs.extend(Jobsdefined4.objects.filter(siteCondition).filter(**querynotime).values('pandaid'))
    jobs.extend(Jobswaiting4.objects.filter(siteCondition).filter(**querynotime).values('pandaid'))
    jobs.extend(Jobsarchived4.objects.filter(siteCondition).filter(**query).values('pandaid'))
    jobsString = ''
    if (len(jobs) > 0):
        jobsString = '&pandaid='
        for job in jobs:
            jobsString += str(job['pandaid']) + ','
    jobsString = jobsString[:-1]
    return jobsString


def siteSummary(query, notime=True, extra="(1=1)"):
    summary = []
    querynotime = copy.deepcopy(query)
    summaryResources=[]
    if notime:
        if 'modificationtime__castdate__range' in querynotime:
            del querynotime['modificationtime__castdate__range']
    # summary.extend(Jobsactive4.objects.filter(**querynotime).values('cloud', 'computingsite', 'jobstatus').extra(where=[extra]).annotate(
    #     Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    # summary.extend(Jobsdefined4.objects.filter(**querynotime).values('cloud', 'computingsite', 'jobstatus').extra(where=[extra]).annotate(
    #     Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    # summary.extend(Jobswaiting4.objects.filter(**querynotime).values('cloud', 'computingsite', 'jobstatus').extra(where=[extra]).annotate(
    #     Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    # summary.extend(Jobsarchived4.objects.filter(**query).values('cloud', 'computingsite', 'jobstatus').extra(where=[extra]).annotate(
    #     Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    summaryResources.extend(
        Jobsactive4.objects.filter(**querynotime).values('cloud', 'computingsite', 'jobstatus', 'resourcetype','corecount').extra(
            where=[extra]).annotate(
            Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    summaryResources.extend(
        Jobsdefined4.objects.filter(**querynotime).values('cloud', 'computingsite', 'jobstatus', 'resourcetype','corecount').extra(
            where=[extra]).annotate(
            Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    summaryResources.extend(Jobswaiting4.objects.filter(**querynotime).values('cloud', 'computingsite','jobstatus', 'resourcetype','corecount').extra(where=[extra]).annotate(
        Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))
    summaryResources.extend(
        Jobsarchived4.objects.filter(**query).values('cloud', 'computingsite', 'jobstatus', 'resourcetype','corecount').extra(
            where=[extra]).annotate(
            Count('jobstatus')).order_by('cloud', 'computingsite', 'jobstatus'))

    summaryResourcesDict = {}
    actualcorecount = 0
    for sumS in summaryResources:
        if sumS['corecount'] is None:
            actualcorecount = 1
        else:
            actualcorecount = sumS['corecount']
        if sumS['cloud'] not in summaryResourcesDict:
            summaryResourcesDict[sumS['cloud']] ={}
            if sumS['computingsite'] not in summaryResourcesDict[sumS['cloud']]:
                summaryResourcesDict[sumS['cloud']][sumS['computingsite']] = {}
                if sumS['jobstatus'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']]:
                    summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']] = {}
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
                else:
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][
                        sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
        else:
            if sumS['computingsite'] not in summaryResourcesDict[sumS['cloud']]:
                summaryResourcesDict[sumS['cloud']][sumS['computingsite']] = {}
                if sumS['jobstatus'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']]:
                    summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']] = {}
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
                else:
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][
                        sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
            else:
                if sumS['jobstatus'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']]:
                    summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']] = {}
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][
                        sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
                else:
                    if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][
                        sumS['jobstatus']]:
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {}
                        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][
                            sumS['resourcetype']] = {'jobstatus__count':sumS['jobstatus__count'],'corecount':actualcorecount}
    summaryList = []
    obj = {}
    for cloud in summaryResourcesDict.keys():
        for site in summaryResourcesDict[cloud].keys():
            for jobstatus in summaryResourcesDict[cloud][site].keys():
                jobscount =0
                obj['resource'] = {}
                for i,resource in enumerate(summaryResourcesDict[cloud][site][jobstatus]):
                    # obj['resource'].append({resource:summaryResourcesDict[cloud][site][jobstatus][resource]})
                    #obj['resource'][resource] = summaryResourcesDict[cloud][site][jobstatus][resource]
                    if resource not in obj['resource']:
                        obj['resource'][resource] = {}
                        obj['resource'][resource]['jobstatus__count'] = {}
                    if resource not in obj['resource']:
                        obj['resource'][resource] = {}
                        obj['resource'][resource]['corecount'] = {}
                    obj['resource'][resource]['jobstatus__count'] = summaryResourcesDict[cloud][site][jobstatus][resource]['jobstatus__count']
                    obj['resource'][resource]['corecount'] = summaryResourcesDict[cloud][site][jobstatus][resource]['corecount']
                    jobscount += summaryResourcesDict[cloud][site][jobstatus][resource]['jobstatus__count']
                    if i == len(summaryResourcesDict[cloud][site][jobstatus]) - 1:

                        obj['cloud'] = cloud
                        obj['computingsite'] = site
                        obj['jobstatus'] = jobstatus
                        obj['jobstatus__count'] = jobscount
                        summaryList.append(obj)
                        obj = {}
    return summaryList


def taskSummaryData(request, query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    summary.extend(
        Jobsactive4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(Count('jobstatus')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(
        Jobsdefined4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(Count('jobstatus')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(
        Jobswaiting4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(Count('jobstatus')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(
        Jobsarchived4.objects.filter(**query).values('taskid', 'jobstatus').annotate(Count('jobstatus')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    summary.extend(
        Jobsarchived4.objects.filter(**query).values('jeditaskid', 'jobstatus').annotate(Count('jobstatus')).order_by(
            'jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    return summary


def voSummary(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    return summary


def wgSummary(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    summary.extend(
        Jobsdefined4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobsactive4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobswaiting4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobsarchived4.objects.filter(**query).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    return summary


def wnSummary(query):
    summary = []
    querynotime = query
    # del querynotime['modificationtime__range']    ### creates inconsistency with job lists. Stick to advertised 12hrs
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).order_by('modificationhost', 'jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).order_by('modificationhost', 'jobstatus'))
    return summary

@login_customrequired
def wnInfo(request, site, wnname='all'):
    """ Give worker node level breakdown of site activity. Spot hot nodes, error prone nodes. """

    if 'hours' in request.GET:
        hours = int(request.GET['hours'])
    elif 'days' in request.GET:
        hours = 24*int(request.GET['days'])
    else:
        hours = 12

    valid, response = initRequest(request)
    if not valid: return response

    if site and site not in pandaSites:
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'alert': {'title': 'This site does not exist!',
                      'message': 'There is no {} registered in the system, please check spelling.'.format(site)},
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        response = render_to_response('wnInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    # Here we try to get cached data
    data = getCacheEntry(request, "wnInfo")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('wnInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    errthreshold = 15

    wnname_rgx = None
    if 'wnname' in request.session['requestParams'] and request.session['requestParams']['wnname']:
        wnname_rgx = request.session['requestParams']['wnname']

    query = setupView(request, hours=hours, limit=999999)
    if wnname != 'all':
        query['modificationhost__endswith'] = wnname
    elif wnname_rgx is not None:
        query['modificationhost__contains'] = wnname_rgx.replace('*', '')
    query['computingsite'] = site
    wnsummarydata = wnSummary(query)
    totstates = {}
    totjobs = 0
    wns = {}
    wnPlotFailed = {}
    wnPlotFinished = {}
    for state in sitestatelist:
        totstates[state] = 0
    for rec in wnsummarydata:
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        wnfull = rec['modificationhost']
        wnsplit = wnfull.split('@')
        if len(wnsplit) == 2:
            if wnname == 'all':
                wn = wnsplit[1]
            else:
                wn = wnfull
            slot = wnsplit[0]
        else:
            wn = wnfull
            slot = '1'
        if wn.startswith('aipanda'): continue
        if jobstatus == 'failed':
            if not wn in wnPlotFailed: wnPlotFailed[wn] = 0
            wnPlotFailed[wn] += count
        elif jobstatus == 'finished':
            if not wn in wnPlotFinished: wnPlotFinished[wn] = 0
            wnPlotFinished[wn] += count
        totjobs += count
        if jobstatus not in totstates:
            totstates[jobstatus] = 0
        totstates[jobstatus] += count
        if wn not in wns:
            wns[wn] = {}
            wns[wn]['name'] = wn
            wns[wn]['count'] = 0
            wns[wn]['states'] = {}
            wns[wn]['slotd'] = {}
            wns[wn]['statelist'] = []
            for state in sitestatelist:
                wns[wn]['states'][state] = {}
                wns[wn]['states'][state]['name'] = state
                wns[wn]['states'][state]['count'] = 0
        if slot not in wns[wn]['slotd']: wns[wn]['slotd'][slot] = 0
        wns[wn]['slotd'][slot] += 1
        wns[wn]['count'] += count
        if jobstatus not in wns[wn]['states']:
            wns[wn]['states'][jobstatus] = {}
            wns[wn]['states'][jobstatus]['count'] = 0
        wns[wn]['states'][jobstatus]['count'] += count

    ## Remove None wn from failed jobs plot if it is in system, add warning banner
    warning = {}
    if 'None' in wnPlotFailed:
        warning['message'] = '%i failed jobs are excluded from "Failed jobs per WN slot" plot because of None value of modificationhost.' % (wnPlotFailed['None'])
        try:
            del wnPlotFailed['None']
        except: pass

    ## Convert dict to summary list
    wnkeys = wns.keys()
    wnkeys = sorted(wnkeys)
    wntot = len(wnkeys)
    fullsummary = []

    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allwns = {}
    allwns['name'] = 'All'
    allwns['count'] = totjobs
    allwns['states'] = totstates
    allwns['statelist'] = []
    for state in sitestatelist:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allwns['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allwns['pctfail'] = int(100. * float(allstated['failed']) / (allstated['finished'] + allstated['failed']))
    else:
        allwns['pctfail'] = 0
    if wnname == 'all': fullsummary.append(allwns)
    avgwns = {}
    avgwns['name'] = 'Average'
    if wntot > 0:
        avgwns['count'] = "%0.2f" % (totjobs / wntot)
    else:
        avgwns['count'] = ''
    avgwns['states'] = totstates
    avgwns['statelist'] = []
    avgstates = {}
    for state in sitestatelist:
        if wntot > 0:
            avgstates[state] = totstates[state] / wntot
        else:
            avgstates[state] = ''
        allstate = {}
        allstate['name'] = state
        if wntot > 0:
            allstate['count'] = "%0.2f" % (int(totstates[state]) / wntot)
            allstated[state] = "%0.2f" % (int(totstates[state]) / wntot)
        else:
            allstate['count'] = ''
            allstated[state] = ''
        avgwns['statelist'].append(allstate)
        avgwns['pctfail'] = allwns['pctfail']
    if wnname == 'all': fullsummary.append(avgwns)

    for wn in wnkeys:
        outlier = ''
        wns[wn]['slotcount'] = len(wns[wn]['slotd'])
        wns[wn]['pctfail'] = 0
        for state in sitestatelist:
            wns[wn]['statelist'].append(wns[wn]['states'][state])
        if wns[wn]['states']['finished']['count'] + wns[wn]['states']['failed']['count'] > 0:
            wns[wn]['pctfail'] = int(100. * float(wns[wn]['states']['failed']['count']) / (
            wns[wn]['states']['finished']['count'] + wns[wn]['states']['failed']['count']))
        if float(wns[wn]['states']['finished']['count']) < float(avgstates['finished']) / 5.:
            outlier += " LowFinished "
        if float(wns[wn]['states']['failed']['count']) > max(float(avgstates['failed']) * 3., 5.):
            outlier += " HighFailed "
        wns[wn]['outlier'] = outlier
        fullsummary.append(wns[wn])

    if 'sortby' in request.session['requestParams']:
        if request.session['requestParams']['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x: x['states'][request.session['requestParams']['sortby']] if not isinstance(x['states'][request.session['requestParams']['sortby']], dict) else x['states'][request.session['requestParams']['sortby']]['count'],
                                 reverse=True)
        elif request.session['requestParams']['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)

    wnPlotFailedL = [[k, v] for k, v in wnPlotFailed.items()]
    wnPlotFailedL = sorted(wnPlotFailedL, key=lambda x: x[0])

    kys = wnPlotFinished.keys()
    kys = sorted(kys)
    wnPlotFinishedL = []
    for k in kys:
        wnPlotFinishedL.append([k, wnPlotFinished[k]])

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'xurl': xurl,
            'site': site,
            'wnname': wnname,
            'user': None,
            'summary': fullsummary,
            'wnPlotFailed': wnPlotFailedL,
            'wnPlotFinished': wnPlotFinishedL,
            'hours': hours,
            'errthreshold': errthreshold,
            'warning': warning,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        response = render_to_response('wnInfo.html', data, content_type='text/html')
        setCacheEntry(request, "wnInfo", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'site': site,
            'wnname': wnname,
            'user': None,
            'summary': fullsummary,
            'wnPlotFailed': wnPlotFailedL,
            'wnPlotFinished': wnPlotFinishedL,
            'hours': hours,
            'errthreshold': errthreshold,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')


def checkUcoreSite(site, usites):
    isUsite = False
    if site in usites:
       isUsite = True
    return isUsite

def getAGISSites():
    sitesUcore = cache.get('sitesUcore')
    sitesHarvester = cache.get('sitesHarvester')
    computevsAtlasCE = cache.get('computevsAtlasCE')

    if not (sitesUcore and sitesHarvester and computevsAtlasCE):
        sitesUcore, sitesHarvester = [], []
        computevsAtlasCE = {}
        url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas"
        http = urllib3.PoolManager()
        data = {}
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for cs in data.keys():
                if 'unifiedPandaQueue' in data[cs]['catchall'] or 'ucore' in data[cs]['capability']:
                    sitesUcore.append(data[cs]['siteid'])
                if 'harvester' in data[cs] and len(data[cs]['harvester']) != 0:
                    sitesHarvester.append(data[cs]['siteid'])
                if 'panda_site' in data[cs]:
                    computevsAtlasCE[cs] = data[cs]['atlas_site']
        except Exception as exc:
            print (exc)

        cache.set('sitesUcore', sitesUcore, 3600)
        cache.set('sitesHarvester', sitesHarvester, 3600)
        cache.set('computevsAtlasCE', computevsAtlasCE, 3600)

    return sitesUcore, sitesHarvester, computevsAtlasCE


def dashSummary(request, hours, limit=999999, view='all', cloudview='region', notime=True):
    start_time = time.time()
    pilots = getPilotCounts(view)
    query = setupView(request, hours=hours, limit=limit, opmode=view)
    ucoreComputingSites, harvesterComputingSites, _ = getAGISSites()

    _logger.debug('[dashSummary] Got AGIS json: {}'.format(time.time() - start_time))

    if VOMODE == 'atlas' and len(request.session['requestParams']) == 0:
        cloudinfol = Cloudconfig.objects.filter().exclude(name='CMS').exclude(name='OSG').values('name', 'status')
    else:
        cloudinfol = []
    cloudinfo = {}
    for c in cloudinfol:
        cloudinfo[c['name']] = c['status']

    siteinfol = Schedconfig.objects.filter().exclude(cloud='CMS').values('siteid', 'status')
    siteinfo = {}
    for s in siteinfol:
        siteinfo[s['siteid']] = s['status']

    _logger.debug('[dashSummary] Got list of sites: {}'.format(time.time() - start_time))

    extra = "(1=1)"
    if 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'TRUE':
        extra = "(not eventservice is null and eventservice in (1, 5) and not specialhandling like '%%sc:%%')"
    elif 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'FALSE':
        extra = "(not (not eventservice is null and eventservice in (1, 5) and not specialhandling like '%%sc:%%'))"
    elif 'esmerge' in request.session['requestParams'] and request.session['requestParams'][
        'esmerge'].upper() == 'TRUE':
        extra = "(not eventservice is null and eventservice=2 and not specialhandling like '%%sc:%%')"

    sitesummarydata = siteSummary(query, notime, extra)
    nojobabs = Sitedata.objects.filter(hours=3).values('site').annotate(dcount=Sum('nojobabs'))
    nojobabshash = {}
    for item in nojobabs:
        nojobabshash[item['site']] = item['dcount']

    _logger.debug('[dashSummary] Got njobsabs for for sites: {}'.format(time.time() - start_time))

    mismatchedSites = []
    clouds = {}
    totstates = {}
    totjobs = 0
    cloudsresources = {}
    for state in sitestatelist:
        totstates[state] = 0
    for rec in sitesummarydata:

        if cloudview == 'region':
            if rec['computingsite'] in homeCloud:
                cloud = homeCloud[rec['computingsite']]
            else:
                print ("ERROR cloud not known", rec)
                mismatchedSites.append([rec['computingsite'], rec['cloud']])
                cloud = ''
        else:
            cloud = rec['cloud']
        site = rec['computingsite']
        if view.find('test') < 0:
            if view != 'analysis' and site.startswith('ANALY'): continue
            if view == 'analysis' and not site.startswith('ANALY'): continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        resources = rec['resource']
        if jobstatus not in sitestatelist: continue
        totjobs += count
        totstates[jobstatus] += count

        if cloud not in clouds:
            print ("Cloud:" + cloud)
            clouds[cloud] = {}
            clouds[cloud]['name'] = cloud
            if cloud in cloudinfo: clouds[cloud]['status'] = cloudinfo[cloud]
            clouds[cloud]['count'] = 0
            clouds[cloud]['pilots'] = 0
            clouds[cloud]['nojobabs'] = 0
            clouds[cloud]['sites'] = {}
            clouds[cloud]['states'] = {}
            clouds[cloud]['statelist'] = []
            cloudsresources[cloud] = {}
            cloudsresources[cloud]['sites'] = {}
            for state in sitestatelist:
                clouds[cloud]['states'][state] = {}
                clouds[cloud]['states'][state]['name'] = state
                clouds[cloud]['states'][state]['count'] = 0
        clouds[cloud]['count'] += count
        clouds[cloud]['states'][jobstatus]['count'] += count
        if site not in clouds[cloud]['sites']:
            clouds[cloud]['sites'][site] = {}
            cloudsresources[cloud]['sites'][site] = {}
            cloudsresources[cloud]['sites'][site]['sumres'] = set()
            clouds[cloud]['sites'][site]['name'] = site
            if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
            clouds[cloud]['sites'][site]['count'] = 0
            if site in pilots:
                clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                clouds[cloud]['pilots'] += pilots[site]['count']
            else:
                clouds[cloud]['sites'][site]['pilots'] = 0

            if site in nojobabshash:
                clouds[cloud]['sites'][site]['nojobabs'] = nojobabshash[site]
                clouds[cloud]['nojobabs'] += nojobabshash[site]
            else:
                clouds[cloud]['sites'][site]['nojobabs'] = 0

            if site in harvesterComputingSites:
                clouds[cloud]['sites'][site]['isHarvester'] = True

            clouds[cloud]['sites'][site]['states'] = {}
            for state in sitestatelist:
                clouds[cloud]['sites'][site]['states'][state] = {}
                clouds[cloud]['sites'][site]['states'][state]['name'] = state
                clouds[cloud]['sites'][site]['states'][state]['count'] = 0
        clouds[cloud]['sites'][site]['count'] += count
        clouds[cloud]['sites'][site]['states'][jobstatus]['count'] += count

        if checkUcoreSite(site,ucoreComputingSites):
            if 'resources' not in clouds[cloud]['sites'][site]['states'][jobstatus]:
                clouds[cloud]['sites'][site]['states'][jobstatus]['resources'] = {}
                clouds[cloud]['sites'][site]['states'][jobstatus]['resources'] = resources

                for reshash in resources.keys():
                    ressite = site + ' ' + reshash
                    if ressite not in clouds[cloud]['sites']:
                        clouds[cloud]['sites'][ressite] = {}
                        clouds[cloud]['sites'][ressite]['states'] = {}
                        clouds[cloud]['sites'][ressite]['resource'] = reshash
                        for parentjobstatus in clouds[cloud]['sites'][site]['states']:
                            if parentjobstatus not in clouds[cloud]['sites'][ressite]['states']:
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus] = {}
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['count'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['corecount'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['name'] = parentjobstatus
                        clouds[cloud]['sites'][ressite]['count'] = resources[reshash][
                            'jobstatus__count']

                        clouds[cloud]['sites'][ressite]['name'] = ressite
                        clouds[cloud]['sites'][ressite]['nojobabs'] = -1
                        clouds[cloud]['sites'][ressite]['parent'] = site
                        if site in siteinfo:
                            clouds[cloud]['sites'][ressite]['status'] = siteinfo[site]
                        else:
                            clouds[cloud]['sites'][ressite]['status'] = ''
                        clouds[cloud]['sites'][ressite]['pilots'] = -1
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash][
                            'corecount']

                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash][
                            'jobstatus__count']
                    else:
                        clouds[cloud]['sites'][ressite]['states'][jobstatus] = {}
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash][
                             'jobstatus__count']
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['name'] = jobstatus
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash][
                                'corecount']
                        clouds[cloud]['sites'][ressite]['count'] += resources[reshash][
                            'jobstatus__count']
            else:
                hashreskeys = clouds[cloud]['sites'][site]['states'][jobstatus]['resources'].keys()
                for reshash in resources.keys():
                    if reshash in hashreskeys:
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['jobstatus__count'] += resources[reshash]['jobstatus__count']
                    else:
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash] = {}
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['jobstatus__count'] = resources[reshash]['jobstatus__count']
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['corecount'] = resources[reshash]['corecount']
                    ressite = site + ' ' + reshash
                    if ressite not in clouds[cloud]['sites']:
                        clouds[cloud]['sites'][ressite] = {}
                        clouds[cloud]['sites'][ressite]['states'] = {}
                        clouds[cloud]['sites'][ressite]['resource'] = reshash
                        for parentjobstatus in clouds[cloud]['sites'][site]['states']:
                            if parentjobstatus not in clouds[cloud]['sites'][ressite]['states']:
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus] = {}
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['count'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['corecount'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['name'] = parentjobstatus
                        clouds[cloud]['sites'][ressite]['count'] = resources[reshash][
                            'jobstatus__count']

                        clouds[cloud]['sites'][ressite]['name'] = ressite
                        clouds[cloud]['sites'][ressite]['nojobabs'] = -1
                        clouds[cloud]['sites'][ressite]['parent'] = site
                        if site in siteinfo:
                            clouds[cloud]['sites'][ressite]['status'] = siteinfo[site]
                        else:
                            clouds[cloud]['sites'][ressite]['status'] = ''
                        clouds[cloud]['sites'][ressite]['pilots'] = -1
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash][
                            'corecount']

                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash][
                            'jobstatus__count']
                    else:
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] += resources[reshash][
                            'jobstatus__count']
                        clouds[cloud]['sites'][ressite]['count'] += resources[reshash][
                            'jobstatus__count']
            if 'sumres' not in clouds[cloud]['sites'][site]:
                clouds[cloud]['sites'][site]['sumres'] = set()
                for res in resources.keys():
                    clouds[cloud]['sites'][site]['sumres'].add(res)
                    cloudsresources[cloud]['sites'][site]['sumres'].add(res)
            else:
                for res in resources.keys():
                    clouds[cloud]['sites'][site]['sumres'].add(res)
                    cloudsresources[cloud]['sites'][site]['sumres'].add(res)

    for cloud in clouds.keys():
        for site in clouds[cloud]['sites'].keys():
            if 'sumres' in clouds[cloud]['sites'][site]:
                clouds[cloud]['sites'][site]['sumres']=list(clouds[cloud]['sites'][site]['sumres'])
            for jobstate in clouds[cloud]['sites'][site]['states'].keys():
                if 'resources' in clouds[cloud]['sites'][site]['states'][jobstate]:
                    for res in cloudsresources[cloud]['sites'][site]['sumres']:
                        if res not in clouds[cloud]['sites'][site]['states'][jobstate]['resources'].keys():
                            clouds[cloud]['sites'][site]['states'][jobstate]['resources'][res] = {'jobstatus__count':0, 'corecount':0}

    _logger.debug('[dashSummary] Precessed data for site summary: {}'.format(time.time() - start_time))

    updateCacheWithListOfMismatchedCloudSites(mismatchedSites)

    _logger.debug('[dashSummary] Updated Cache with  mistmatched cloud|sites : {}'.format(time.time() - start_time))

    ## Go through the sites, add any that are missing (because they have no jobs in the interval)
    if cloudview != 'cloud':
        for site in pandaSites:
            if view.find('test') < 0:
                if view != 'analysis' and site.startswith('ANALY'): continue
                if view == 'analysis' and not site.startswith('ANALY'): continue
            cloud = pandaSites[site]['cloud']
            if cloud not in clouds:
                ## Bail. Adding sites is one thing; adding clouds is another
                continue
            if site not in clouds[cloud]['sites']:
                clouds[cloud]['sites'][site] = {}
                clouds[cloud]['sites'][site]['name'] = site
                if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
                clouds[cloud]['sites'][site]['count'] = 0
                clouds[cloud]['sites'][site]['pctfail'] = 0

                if site in nojobabshash:
                    clouds[cloud]['sites'][site]['nojobabs'] = nojobabshash[site]
                    clouds[cloud]['nojobabs'] += nojobabshash[site]
                else:
                    clouds[cloud]['sites'][site]['nojobabs'] = 0

                if site in pilots:
                    clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                    clouds[cloud]['pilots'] += pilots[site]['count']
                else:
                    clouds[cloud]['sites'][site]['pilots'] = 0

                clouds[cloud]['sites'][site]['states'] = {}
                for state in sitestatelist:
                    clouds[cloud]['sites'][site]['states'][state] = {}
                    clouds[cloud]['sites'][site]['states'][state]['name'] = state
                    clouds[cloud]['sites'][site]['states'][state]['count'] = 0

    ## Convert dict to summary list
    cloudkeys = clouds.keys()
    cloudkeys = sorted(cloudkeys)
    fullsummary = []
    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allclouds = {}
    allclouds['name'] = 'All'
    allclouds['count'] = totjobs
    allclouds['pilots'] = 0
    allclouds['nojobabs'] = 0

    allclouds['sites'] = {}
    allclouds['states'] = totstates
    allclouds['statelist'] = []
    for state in sitestatelist:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allclouds['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allclouds['pctfail'] = int(100. * float(allstated['failed']) / (allstated['finished'] + allstated['failed']))
    else:
        allclouds['pctfail'] = 0
    for cloud in cloudkeys:
        allclouds['pilots'] += clouds[cloud]['pilots']
    fullsummary.append(allclouds)

    for cloud in cloudkeys:
        for state in sitestatelist:
            clouds[cloud]['statelist'].append(clouds[cloud]['states'][state])
        sites = clouds[cloud]['sites']
        sitekeys = list(sites.keys())
        sitekeys = sorted(sitekeys)
        cloudsummary = []
        for site in sitekeys:
            sitesummary = []
            for state in sitestatelist:
                sitesummary.append(sites[site]['states'][state])
            sites[site]['summary'] = sitesummary
            if sites[site]['states']['finished']['count'] + sites[site]['states']['failed']['count'] > 0:
                sites[site]['pctfail'] = int(100. * float(sites[site]['states']['failed']['count']) / (
                sites[site]['states']['finished']['count'] + sites[site]['states']['failed']['count']))
            else:
                sites[site]['pctfail'] = 0

            cloudsummary.append(sites[site])
        clouds[cloud]['summary'] = cloudsummary
        if clouds[cloud]['states']['finished']['count'] + clouds[cloud]['states']['failed']['count'] > 0:
            clouds[cloud]['pctfail'] = int(100. * float(clouds[cloud]['states']['failed']['count']) / (
                clouds[cloud]['states']['finished']['count'] + clouds[cloud]['states']['failed']['count']))
        else:
            clouds[cloud]['pctfail'] = 0

        fullsummary.append(clouds[cloud])

    _logger.debug('[dashSummary] Finished cloud|sites summary: {}'.format(time.time() - start_time))

    if 'sortby' in request.session['requestParams']:
        if request.session['requestParams']['sortby'] in statelist:
            #fullsummary = sorted(fullsummary, key=lambda x: x['states'][request.session['requestParams']['sortby']]['count'],
            #                     reverse=True)
            #cloudsummary = sorted(cloudsummary, key=lambda x: x['states'][request.session['requestParams']['sortby']],
            #                      reverse=True)
            for cloud in clouds:
                clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'],
                                                  key=lambda x: x['states'][request.session['requestParams']['sortby']][
                                                      'count'], reverse=True)
        elif request.session['requestParams']['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)
            cloudsummary = sorted(cloudsummary, key=lambda x: x['pctfail'], reverse=True)
            for cloud in clouds:
                clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'], key=lambda x: x['pctfail'], reverse=True)

    _logger.debug('[dashSummary] Sorted cloud|sites summary: {}'.format(time.time() - start_time))

    return fullsummary


def dashTaskSummary(request, hours, limit=999999, view='all'):
    query = setupView(request, hours=hours, limit=limit, opmode=view)
    tasksummarydata = taskSummaryData(request, query)
    tasks = {}
    totstates = {}
    totjobs = 0
    for state in sitestatelist:
        totstates[state] = 0

    taskids = []
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskids.append({'jeditaskid': rec['jeditaskid']})
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskids.append({'taskid': rec['taskid']})
    tasknamedict = taskNameDict(taskids)
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskid = rec['jeditaskid']
            tasktype = 'JEDI'
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskid = rec['taskid']
            tasktype = 'old'
        else:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in sitestatelist: continue
        totjobs += count
        totstates[jobstatus] += count
        if taskid not in tasks:
            tasks[taskid] = {}
            tasks[taskid]['taskid'] = taskid
            if taskid in tasknamedict:
                tasks[taskid]['name'] = tasknamedict[taskid]
            else:
                tasks[taskid]['name'] = str(taskid)
            tasks[taskid]['count'] = 0
            tasks[taskid]['states'] = {}
            tasks[taskid]['statelist'] = []
            for state in sitestatelist:
                tasks[taskid]['states'][state] = {}
                tasks[taskid]['states'][state]['name'] = state
                tasks[taskid]['states'][state]['count'] = 0
        tasks[taskid]['count'] += count
        tasks[taskid]['states'][jobstatus]['count'] += count
    if view == 'analysis':
        ## Show only tasks starting with 'user.'
        kys = list(tasks.keys())
        for t in kys:
            if not str(tasks[t]['name'].encode('ascii', 'ignore')).startswith('user.'): del tasks[t]
    ## Convert dict to summary list
    taskkeys = list(tasks.keys())
    taskkeys = sorted(taskkeys)
    fullsummary = []
    for taskid in taskkeys:
        for state in sitestatelist:
            tasks[taskid]['statelist'].append(tasks[taskid]['states'][state])
        if tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count'] > 0:
            tasks[taskid]['pctfail'] = int(100. * float(tasks[taskid]['states']['failed']['count']) / (
            tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count']))

        fullsummary.append(tasks[taskid])
    if 'sortby' in request.session['requestParams']:
        if request.session['requestParams']['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x: x['states'][request.session['requestParams']['sortby']],
                                 reverse=True)
        elif request.session['requestParams']['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)
    return fullsummary


def preProcess(request):
    ''' todo:
    0. Decide tables structure and parameters aggregates approach
    1. Get List of Jobs modified later than previosly saved last modified job
    2. For each of them calculate output variables of Error summary.
    Factorize using set of request parameters causing different flow.
    3. Save new variables in the dedicated table in form - jobid ~ variable
    4. When a new query comes, select from job tables correspondent ids.
    5. Select variables from the transistent table.
    6. Merge them and display output.

    '''

    #    data = {}
    #    dashTaskSummary_preprocess(request)
    #    response = render_to_response('preprocessLog.html', data, RequestContext(request))
    #    patch_response_headers(response, cache_timeout=-1)

    return None


# class prepDashTaskSummary:






def dashTaskSummary_preprocess(request):
    #    query = setupView(request,hours=hours,limit=limit,opmode=view)
    query = {'modificationtime__castdate__range': [timezone.now() - timedelta(hours=LAST_N_HOURS_MAX), timezone.now()]}

    tasksummarydata = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    tasksummarydata.extend(
        Jobsactive4.objects.filter(**querynotime).values('taskid', 'jobstatus', 'computingsite', 'produsername',
                                                         'transexitcode', 'piloterrorcode', 'processingtype',
                                                         'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                     Count('computingsite'),
                                                                                     Count('produsername'),
                                                                                     Count('transexitcode'),
                                                                                     Count('piloterrorcode'),
                                                                                     Count('processingtype'),
                                                                                     Count('prodsourcelabel')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobsdefined4.objects.filter(**querynotime).values('taskid', 'jobstatus', 'computingsite', 'produsername',
                                                          'transexitcode', 'piloterrorcode', 'processingtype',
                                                          'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                      Count('computingsite'),
                                                                                      Count('produsername'),
                                                                                      Count('transexitcode'),
                                                                                      Count('piloterrorcode'),
                                                                                      Count('processingtype'), Count(
                'prodsourcelabel')).order_by('taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobswaiting4.objects.filter(**querynotime).values('taskid', 'jobstatus', 'computingsite', 'produsername',
                                                          'transexitcode', 'piloterrorcode', 'processingtype',
                                                          'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                      Count('computingsite'),
                                                                                      Count('produsername'),
                                                                                      Count('transexitcode'),
                                                                                      Count('piloterrorcode'),
                                                                                      Count('processingtype'), Count(
                'prodsourcelabel')).order_by('taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobsarchived4.objects.filter(**query).values('taskid', 'jobstatus', 'computingsite', 'produsername',
                                                     'transexitcode', 'piloterrorcode', 'processingtype',
                                                     'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                 Count('computingsite'),
                                                                                 Count('produsername'),
                                                                                 Count('transexitcode'),
                                                                                 Count('piloterrorcode'),
                                                                                 Count('processingtype'),
                                                                                 Count('prodsourcelabel')).order_by(
            'taskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobsactive4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus', 'computingsite', 'produsername',
                                                         'transexitcode', 'piloterrorcode', 'processingtype',
                                                         'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                     Count('computingsite'),
                                                                                     Count('produsername'),
                                                                                     Count('transexitcode'),
                                                                                     Count('piloterrorcode'),
                                                                                     Count('processingtype'),
                                                                                     Count('prodsourcelabel')).order_by(
            'jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobsdefined4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus', 'computingsite', 'produsername',
                                                          'transexitcode', 'piloterrorcode', 'processingtype',
                                                          'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                      Count('computingsite'),
                                                                                      Count('produsername'),
                                                                                      Count('transexitcode'),
                                                                                      Count('piloterrorcode'),
                                                                                      Count('processingtype'), Count(
                'prodsourcelabel')).order_by('jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobswaiting4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus', 'computingsite', 'produsername',
                                                          'transexitcode', 'piloterrorcode', 'processingtype',
                                                          'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                      Count('computingsite'),
                                                                                      Count('produsername'),
                                                                                      Count('transexitcode'),
                                                                                      Count('piloterrorcode'),
                                                                                      Count('processingtype'), Count(
                'prodsourcelabel')).order_by('jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])
    tasksummarydata.extend(
        Jobsarchived4.objects.filter(**query).values('jeditaskid', 'jobstatus', 'computingsite', 'produsername',
                                                     'transexitcode', 'piloterrorcode', 'processingtype',
                                                     'prodsourcelabel').annotate(Count('jobstatus'),
                                                                                 Count('computingsite'),
                                                                                 Count('produsername'),
                                                                                 Count('transexitcode'),
                                                                                 Count('piloterrorcode'),
                                                                                 Count('processingtype'),
                                                                                 Count('prodsourcelabel')).order_by(
            'jeditaskid', 'jobstatus')[:request.session['JOB_LIMIT']])

    '''
    tasks = {}
    totstates = {}
    totjobs = 0
    for state in sitestatelist:
        totstates[state] = 0

    taskids = []
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskids.append( { 'jeditaskid' : rec['jeditaskid'] } )
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0 :
            taskids.append( { 'taskid' : rec['taskid'] } )
    tasknamedict = taskNameDict(taskids)
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskid = rec['jeditaskid']
            tasktype = 'JEDI'
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0 :
            taskid = rec['taskid']
            tasktype = 'old'
        else:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in sitestatelist: continue
        totjobs += count
        totstates[jobstatus] += count
        if taskid not in tasks:
            tasks[taskid] = {}
            tasks[taskid]['taskid'] = taskid
            if taskid in tasknamedict:
                tasks[taskid]['name'] = tasknamedict[taskid]
            else:
                tasks[taskid]['name'] = str(taskid)
            tasks[taskid]['count'] = 0
            tasks[taskid]['states'] = {}
            tasks[taskid]['statelist'] = []
            for state in sitestatelist:
                tasks[taskid]['states'][state] = {}
                tasks[taskid]['states'][state]['name'] = state
                tasks[taskid]['states'][state]['count'] = 0
        tasks[taskid]['count'] += count
        tasks[taskid]['states'][jobstatus]['count'] += count
    if view == 'analysis':
        ## Show only tasks starting with 'user.'
        kys = tasks.keys()
        for t in kys:
            if not str(tasks[t]['name'].encode('ascii','ignore')).startswith('user.'): del tasks[t]
    ## Convert dict to summary list
    taskkeys = tasks.keys()
    taskkeys = sorted(taskkeys)
    fullsummary = []
    for taskid in taskkeys:
        for state in sitestatelist:
            tasks[taskid]['statelist'].append(tasks[taskid]['states'][state])
        if tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count'] > 0:
            tasks[taskid]['pctfail'] =  int(100.*float(tasks[taskid]['states']['failed']['count'])/(tasks[taskid]['states']['finished']['count']+tasks[taskid]['states']['failed']['count']))

        fullsummary.append(tasks[taskid])
    if 'sortby' in request.session['requestParams']:
        if request.session['requestParams']['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x:x['states'][request.session['requestParams']['sortby']],reverse=True)
        elif request.session['requestParams']['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x:x['pctfail'],reverse=True)
    '''

    return -1


# https://github.com/PanDAWMS/panda-jedi/blob/master/pandajedi/jedicore/JediCoreUtils.py
def getEffectiveFileSize(fsize, startEvent, endEvent, nEvents):
    inMB = 1024 * 1024
    if fsize in [None, 0]:
        # use dummy size for pseudo input
        effectiveFsize = inMB
    elif nEvents != None and startEvent != None and endEvent != None:
        # take event range into account
        effectiveFsize = np.long(float(fsize) * float(endEvent - startEvent + 1) / float(nEvents))
    else:
        effectiveFsize = fsize
    # use dummy size if input is too small
    if effectiveFsize == 0:
        effectiveFsize = inMB
    # in MB
    effectiveFsize = float(effectiveFsize) / inMB
    # return
    return effectiveFsize


def calculateRWwithPrio_JEDI(query):
    # query = {}
    retRWMap = {}
    retNREMJMap = {}
    values = ['jeditaskid', 'datasetid', 'modificationtime', 'cloud', 'nrem', 'walltime', 'fsize', 'startevent',
              'endevent', 'nevents']
    ###TODO Rework it
    if 'schedulerid' in query.keys():
        del query['schedulerid']
    elif 'schedulerid__startswith' in query.keys():
        del query['schedulerid__startswith']
    progressEntries = []
    progressEntries.extend(GetRWWithPrioJedi3DAYS.objects.filter(**query).values(*values))
    allCloudsRW = 0;
    allCloudsNREMJ = 0;

    if len(progressEntries) > 0:
        for progrEntry in progressEntries:
            if progrEntry['fsize'] != None:
                effectiveFsize = getEffectiveFileSize(progrEntry['fsize'], progrEntry['startevent'],
                                                      progrEntry['endevent'], progrEntry['nevents'])
                tmpRW = progrEntry['nrem'] * effectiveFsize * progrEntry['walltime']
                if not progrEntry['cloud'] in retRWMap:
                    retRWMap[progrEntry['cloud']] = 0
                retRWMap[progrEntry['cloud']] += tmpRW
                allCloudsRW += tmpRW
                if not progrEntry['cloud'] in retNREMJMap:
                    retNREMJMap[progrEntry['cloud']] = 0
                retNREMJMap[progrEntry['cloud']] += progrEntry['nrem']
                allCloudsNREMJ += progrEntry['nrem']
    retRWMap['All'] = allCloudsRW
    retNREMJMap['All'] = allCloudsNREMJ
    for cloudName, rwValue in retRWMap.items():
        retRWMap[cloudName] = int(rwValue / 24 / 3600)
    return retRWMap, retNREMJMap


def dashWorldAnalysis(request):
    return worldjobs(request, view='analysis')


def dashWorldProduction(request):
    return worldjobs(request, view='production')


@login_customrequired
def worldjobs(request, view='production'):
    valid, response = initRequest(request)

    data = getCacheEntry(request, "worldjobs")
    #data = None

    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('worldjobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    query = {}
    values = ['nucleus', 'computingsite', 'jobstatus', 'countjobsinstate']
    worldTasksSummary = []

    if view=='production':
        query['tasktype'] = 'prod'
    else:
        query['tasktype'] = 'anal'

    worldTasksSummary.extend(JobsWorldViewTaskType.objects.filter(**query).values(*values))
    nucleus = {}
    statelist1 = statelist
    #    del statelist1[statelist1.index('jclosed')]
    #    del statelist1[statelist1.index('pending')]

    if len(worldTasksSummary) > 0:
        for jobs in worldTasksSummary:
            if jobs['nucleus'] in nucleus:
                if jobs['computingsite'] in nucleus[jobs['nucleus']]:
                    nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] = jobs['countjobsinstate']
                else:
                    nucleus[jobs['nucleus']][jobs['computingsite']] = {}
                    for state in statelist1:
                        nucleus[jobs['nucleus']][jobs['computingsite']][state] = 0
                    nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] = jobs['countjobsinstate']
            else:
                nucleus[jobs['nucleus']] = {}
                nucleus[jobs['nucleus']][jobs['computingsite']] = {}
                for state in statelist1:
                    nucleus[jobs['nucleus']][jobs['computingsite']][state] = 0
                nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] = jobs['countjobsinstate']

    nucleusSummary = {}
    for nucleusInfo in nucleus:
        nucleusSummary[nucleusInfo] = {}
        for site in nucleus[nucleusInfo]:
            for state in nucleus[nucleusInfo][site]:
                if state in nucleusSummary[nucleusInfo]:
                    nucleusSummary[nucleusInfo][state] += nucleus[nucleusInfo][site][state]
                else:
                    nucleusSummary[nucleusInfo][state] = nucleus[nucleusInfo][site][state]

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')
        #        del request.session['TFIRST']
        #        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'nucleuses': nucleus,
            'nucleussummary': nucleusSummary,
            'statelist': statelist1,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'user': None,
            'built': datetime.now().strftime("%H:%M:%S"),
            'hours':48,
        }
        setCacheEntry(request, "worldjobs", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render_to_response('worldjobs.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        #        del request.session['TFIRST']
        #        del request.session['TLAST']

        data = {
        }

        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


@login_customrequired
def worldhs06s(request):
    valid, response = initRequest(request)

    # Here we try to get cached data
    data = getCacheEntry(request, "worldhs06s")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('worldHS06s.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    roundflag = False
    condition = ''
    for param in request.session['requestParams']:
        if param == 'reqid':
            condition += ('t.reqid=' + str(request.session['requestParams']['reqid']))
        if param == 'jeditaskid' and len(condition) > 1:
            condition += (' AND t.jeditaskid=' + str(request.session['requestParams']['jeditaskid']))
        elif param == 'jeditaskid':
            condition += ('t.jeditaskid=' + str(request.session['requestParams']['jeditaskid']))
    if len(condition) < 1:
        condition = '(1=1)'
        roundflag = True

    cur = connection.cursor()
    cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.GETHS06SSUMMARY('%s'))" % condition)
    hspersite = cur.fetchall()
    cur.close()

    newcur = connection.cursor()
    newcur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.GETHS06STOTSUMMARY('%s'))" % condition)
    hspernucleus = newcur.fetchall()
    newcur.close()

    keys = ['nucleus', 'computingsite', 'usedhs06spersite', 'failedhs06spersite']
    totkeys = ['nucleus', 'ntaskspernucleus', 'toths06spernucleus']

    worldHS06sSummary = [dict(zip(keys, row)) for row in hspersite]
    worldHS06sTotSummary = [dict(zip(totkeys, row)) for row in hspernucleus]
    worldHS06sSummaryByNucleus = {}
    nucleus = {}
    totnucleus = {}

    for nucl in worldHS06sTotSummary:
        totnucleus[nucl['nucleus']] = {}
        totnucleus[nucl['nucleus']]['ntaskspernucleus'] = nucl['ntaskspernucleus']
        if roundflag:
            totnucleus[nucl['nucleus']]['toths06spernucleus'] = round(nucl['toths06spernucleus'] / 1000. / 3600 / 24,
                                                                      2) if nucl[
                                                                                'toths06spernucleus'] is not None else 0
        else:
            totnucleus[nucl['nucleus']]['toths06spernucleus'] = nucl['toths06spernucleus'] if nucl[
                                                                                                  'toths06spernucleus'] is not None else 0

    for site in worldHS06sSummary:
        if site['nucleus'] not in nucleus:
            nucleus[site['nucleus']] = []
        dictsite = {}
        dictsite['computingsite'] = site['computingsite']
        dictsite['usedhs06spersite'] = site['usedhs06spersite'] if site['usedhs06spersite'] else 0
        dictsite['failedhs06spersite'] = site['failedhs06spersite'] if site['failedhs06spersite'] else 0
        dictsite['failedhs06spersitepct'] = 100 * dictsite['failedhs06spersite'] / dictsite['usedhs06spersite'] if (
        site['usedhs06spersite'] and site['usedhs06spersite'] > 0) else 0
        nucleus[site['nucleus']].append(dictsite)

    for nuc in nucleus:
        worldHS06sSummaryByNucleus[nuc] = {}
        worldHS06sSummaryByNucleus[nuc]['usedhs06spernucleus'] = sum(
            [site['usedhs06spersite'] for site in nucleus[nuc]])
        worldHS06sSummaryByNucleus[nuc]['failedhs06spernucleus'] = sum(
            [site['failedhs06spersite'] for site in nucleus[nuc]])
        if roundflag:
            worldHS06sSummaryByNucleus[nuc]['usedhs06spernucleus'] = round(
                worldHS06sSummaryByNucleus[nuc]['usedhs06spernucleus'] / 1000. / 3600 / 24, 2)
            worldHS06sSummaryByNucleus[nuc]['failedhs06spernucleus'] = round(
                worldHS06sSummaryByNucleus[nuc]['failedhs06spernucleus'] / 1000. / 3600 / 24, 2)
        worldHS06sSummaryByNucleus[nuc]['failedhs06spernucleuspct'] = int(
            100 * worldHS06sSummaryByNucleus[nuc]['failedhs06spernucleus'] / worldHS06sSummaryByNucleus[nuc][
                'usedhs06spernucleus']) if worldHS06sSummaryByNucleus[nuc]['usedhs06spernucleus'] and \
                                           worldHS06sSummaryByNucleus[nuc]['usedhs06spernucleus'] > 0 else 0
        if nuc in totnucleus:
            worldHS06sSummaryByNucleus[nuc]['ntaskspernucleus'] = totnucleus[nuc]['ntaskspernucleus']
            worldHS06sSummaryByNucleus[nuc]['toths06spernucleus'] = totnucleus[nuc]['toths06spernucleus']

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
        reverseflag = False
        if request.session['requestParams']['sortby'] == 'used-desc':
            sortcol = 'usedhs06spersite'
            reverseflag = True
        elif request.session['requestParams']['sortby'] == 'used-asc':
            sortcol = 'usedhs06spersite'
        elif request.session['requestParams']['sortby'] == 'failed-desc':
            sortcol = 'failedhs06spersite'
            reverseflag = True
        elif request.session['requestParams']['sortby'] == 'failed-asc':
            sortcol = 'failedhs06spersite'
        elif request.session['requestParams']['sortby'] == 'failedpct-desc':
            sortcol = 'failedhs06spersitepct'
            reverseflag = True
        elif request.session['requestParams']['sortby'] == 'failedpct-asc':
            sortcol = 'failedhs06spersitepct'
        elif request.session['requestParams']['sortby'] == 'satellite-desc':
            sortcol = 'computingsite'
            reverseflag = True
        else:
            sortcol = 'computingsite'
        for nuc in nucleus:
            nucleus[nuc] = sorted(nucleus[nuc], key=lambda x: x[sortcol], reverse=reverseflag)
    else:
        sortby = 'satellite-asc'

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'user': None,
            'hssitesum': nucleus,
            'hsnucleussum': worldHS06sSummaryByNucleus,
            'roundflag': roundflag,
            'sortby': sortby,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        ##self monitor
        setCacheEntry(request, "worldhs06s", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render_to_response('worldHS06s.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:

        data = {
        }

        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

@login_customrequired
def dashboard(request, view='all'):
    valid, response = initRequest(request)
    if not valid: return response

#    data = getCacheEntry(request, "dashboard", skipCentralRefresh=True)
    data = getCacheEntry(request, "dashboard")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        template = data['template']
        response = render_to_response(template, data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    taskdays = 3
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        hours = 24 * taskdays
    else:
        hours = 12

    hoursSinceUpdate = 36
    estailtojobslinks = ''
    if view == 'production':
        extra = "(1=1)"
        if 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'TRUE':
            extra = "(not eventservice is null and eventservice in (1, 5) and not specialhandling like '%%sc:%%')"
            estailtojobslinks = '&eventservice=eventservice'
        elif 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'FALSE':
            extra = "(not (not eventservice is null and eventservice in (1, 5) and not specialhandling like '%%sc:%%'))"
        elif 'esmerge' in request.session['requestParams'] and request.session['requestParams'][
            'esmerge'].upper() == 'TRUE':
            extra = "(not eventservice is null and eventservice=2 and not specialhandling like '%%sc:%%')"
            estailtojobslinks = '&eventservice=2'
        noldtransjobs, transclouds, transrclouds = stateNotUpdated(request, state='transferring',
                                                                   hoursSinceUpdate=hoursSinceUpdate, count=True, wildCardExtension=extra)
    elif view == 'analysis':
        hours = 3
        noldtransjobs = 0
        transclouds = []
        transrclouds = []
    else:
        hours = 12
        noldtransjobs = 0
        transclouds = []
        transrclouds = []

    errthreshold = 10

    query = setupView(request, hours=hours, limit=999999, opmode=view)
    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'task':
        return dashTasks(request, hours, view)

    if VOMODE != 'atlas':
        vosummarydata = voSummary(query)
        vos = {}
        for rec in vosummarydata:
            vo = rec['vo']
            # if vo == None: vo = 'Unassigned'
            if vo == None: continue
            jobstatus = rec['jobstatus']
            count = rec['jobstatus__count']
            if vo not in vos:
                vos[vo] = {}
                vos[vo]['name'] = vo
                vos[vo]['count'] = 0
                vos[vo]['states'] = {}
                vos[vo]['statelist'] = []
                for state in sitestatelist:
                    vos[vo]['states'][state] = {}
                    vos[vo]['states'][state]['name'] = state
                    vos[vo]['states'][state]['count'] = 0
            vos[vo]['count'] += count
            vos[vo]['states'][jobstatus]['count'] += count
        ## Convert dict to summary list
        vokeys = list(vos.keys())
        vokeys = sorted(vokeys)
        vosummary = []
        for vo in vokeys:
            for state in sitestatelist:
                vos[vo]['statelist'].append(vos[vo]['states'][state])
                if int(vos[vo]['states']['finished']['count']) + int(vos[vo]['states']['failed']['count']) > 0:
                    vos[vo]['pctfail'] = int(100. * float(vos[vo]['states']['failed']['count']) / (
                    vos[vo]['states']['finished']['count'] + vos[vo]['states']['failed']['count']))
            vosummary.append(vos[vo])

        if 'sortby' in request.session['requestParams']:
            if request.session['requestParams']['sortby'] in statelist:
                vosummary = sorted(vosummary, key=lambda x: x['states'][request.session['requestParams']['sortby']],
                                   reverse=True)
            elif request.session['requestParams']['sortby'] == 'pctfail':
                vosummary = sorted(vosummary, key=lambda x: x['pctfail'], reverse=True)

    else:
        if view == 'production':
            errthreshold = 5
        elif view == 'analysis':
            errthreshold = 15
        else:
            errthreshold = 10
        vosummary = []

    cloudview = 'region'
    if 'cloudview' in request.session['requestParams']:
        cloudview = request.session['requestParams']['cloudview']
    if view == 'analysis':
        cloudview = 'region'
    elif view != 'production' and view != 'all':
        cloudview = 'N/A'
    if view == 'production' and (cloudview == 'world' or cloudview == 'cloud'): #cloud view is the old way of jobs distributing;
        # just to avoid redirecting
        if 'modificationtime__castdate__range' in query:
            query = {'modificationtime__castdate__range': query['modificationtime__castdate__range']}
        else:
            query = {}
        values = ['nucleus', 'computingsite', 'jobstatus']
        worldJobsSummary = []
        estailtojobslinks = ''

        if 'days' in request.session['requestParams']:
            hours = int(request.session['requestParams']['days'])*24
        if 'hours' in request.session['requestParams']:
            hours = int(request.session['requestParams']['hours'])

        extra = '(1=1)'

        if view == 'production':
            query['tasktype'] = 'prod'
        elif view == 'analysis':
            query['tasktype'] = 'anal'


        if 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'TRUE':
            query['es__in'] = [1, 5]
            estailtojobslinks = '&eventservice=eventservice|cojumbo'
            extra = jobSuppression(request)

        if 'es' in request.session['requestParams'] and request.session['requestParams']['es'].upper() == 'FALSE':
            query['es'] = 0


        # This is done for compartibility with /jobs/ results
        excludedTimeQuery = copy.deepcopy(query)
        jobsarch4statuses = ['finished', 'failed', 'cancelled', 'closed']
        if ('modificationtime__castdate__range' in excludedTimeQuery and not 'date_to' in request.session['requestParams']):
            del excludedTimeQuery['modificationtime__castdate__range']
        worldJobsSummary.extend(CombinedWaitActDefArch4.objects.filter(**excludedTimeQuery).values(*values).extra(where=[extra]).exclude(isarchive=1).annotate(countjobsinstate=Count('jobstatus')).annotate(counteventsinstate=Sum('nevents')))
        worldJobsSummary.extend(CombinedWaitActDefArch4.objects.filter(**query).values(*values).extra(where=[extra]).exclude(isarchive=0).annotate(countjobsinstate=Count('jobstatus')).annotate(counteventsinstate=Sum('nevents')))
        nucleus = {}
        statelist1 = statelist
        #    del statelist1[statelist1.index('jclosed')]
        #    del statelist1[statelist1.index('pending')]

        if len(worldJobsSummary) > 0:
            for jobs in worldJobsSummary:
                if jobs['nucleus'] in nucleus:
                    if jobs['computingsite'] in nucleus[jobs['nucleus']]:
                        nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] += jobs['countjobsinstate']
                        if (jobs['jobstatus'] in ('finished', 'failed','merging')):
                            nucleus[jobs['nucleus']][jobs['computingsite']]['events'+ jobs['jobstatus']] += jobs['counteventsinstate']

                    else:
                        nucleus[jobs['nucleus']][jobs['computingsite']] = {}
                        for state in statelist1:
                            nucleus[jobs['nucleus']][jobs['computingsite']][state] = 0
                            if (state in ('finished', 'failed','merging')):
                                nucleus[jobs['nucleus']][jobs['computingsite']]['events'+ state] = 0

                        nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] = jobs['countjobsinstate']
                        if (state in ('finished', 'failed', 'merging')):
                            nucleus[jobs['nucleus']][jobs['computingsite']]['events'+ state] = jobs['counteventsinstate']

                else:
                    nucleus[jobs['nucleus']] = {}
                    nucleus[jobs['nucleus']][jobs['computingsite']] = {}
                    for state in statelist1:
                        nucleus[jobs['nucleus']][jobs['computingsite']][state] = 0
                        if (state in ('finished', 'failed', 'merging')):
                            nucleus[jobs['nucleus']][jobs['computingsite']]['events'+ state] = 0

                    nucleus[jobs['nucleus']][jobs['computingsite']][jobs['jobstatus']] = jobs['countjobsinstate']
                    if (state in ('finished', 'failed', 'merging')):
                        nucleus[jobs['nucleus']][jobs['computingsite']]['events'+ jobs['jobstatus']] = jobs['counteventsinstate']


        nucleusSummary = {}
        for nucleusInfo in nucleus:
            nucleusSummary[nucleusInfo] = {}
            for site in nucleus[nucleusInfo]:
                for state in nucleus[nucleusInfo][site]:
                    if state in nucleusSummary[nucleusInfo]:
                        nucleusSummary[nucleusInfo][state] += nucleus[nucleusInfo][site][state]
                    else:
                        nucleusSummary[nucleusInfo][state] = nucleus[nucleusInfo][site][state]

        if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                    'json' not in request.session['requestParams'])):
            xurl = extensibleURL(request)
            nosorturl = removeParam(xurl, 'sortby', mode='extensible')
            if 'TFIRST' in request.session: del request.session['TFIRST']
            if 'TLAST' in request.session: del request.session['TLAST']
            data = {
                'request': request,
                'viewParams': request.session['viewParams'],
                'requestParams': request.session['requestParams'],
                'url': request.path,
                'nucleuses': nucleus,
                'nucleussummary': nucleusSummary,
                'statelist': statelist1,
                'xurl': xurl,
                'estailtojobslinks':estailtojobslinks,
                'nosorturl': nosorturl,
                'user': None,
                'hours': hours,
                'template': 'worldjobs.html',
                'built': datetime.now().strftime("%m-%d %H:%M:%S"),
            }
            ##self monitor
            response = render_to_response('worldjobs.html', data, content_type='text/html')
            setCacheEntry(request, "dashboard", json.dumps(data, cls=DateEncoder), 60 * 30)
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response
        else:
            data = {
                'nucleuses': nucleus,
                'nucleussummary': nucleusSummary,
                'statelist': statelist1,
            }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

    elif view == 'objectstore':
        global objectStoresNames
        if len(objectStoresNames) == 0:
            getObjectStoresNames()

        sqlRequest = """SELECT JOBSTATUS, COUNT(JOBSTATUS) as COUNTJOBSINSTATE, COMPUTINGSITE, OBJSE, RTRIM(XMLAGG(XMLELEMENT(E,PANDAID,',').EXTRACT('//text()') ORDER BY PANDAID).GetClobVal(),',') AS PANDALIST FROM 
          (SELECT DISTINCT t1.PANDAID, NUCLEUS, COMPUTINGSITE, JOBSTATUS, TASKTYPE, ES, CASE WHEN t2.OBJSTORE_ID > 0 THEN TO_CHAR(t2.OBJSTORE_ID) ELSE t3.destinationse END AS OBJSE 
          FROM ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4 t1 
          LEFT JOIN ATLAS_PANDA.JEDI_EVENTS t2 ON t1.PANDAID=t2.PANDAID and t1.JEDITASKID =  t2.JEDITASKID and (t2.ziprow_id>0 or t2.OBJSTORE_ID > 0) 
          LEFT JOIN ATLAS_PANDA.filestable4 t3 ON (t3.pandaid = t2.pandaid and  t3.JEDITASKID = t2.JEDITASKID and t3.row_id=t2.ziprow_id) WHERE t1.ES in (1) and t1.CLOUD='WORLD' and t1.MODIFICATIONTIME > (sysdate - interval '13' hour) 
          AND t3.MODIFICATIONTIME >  (sysdate - interval '13' hour)) WHERE NOT OBJSE IS NULL GROUP BY JOBSTATUS, JOBSTATUS, COMPUTINGSITE, OBJSE order by OBJSE;"""

        cur = connection.cursor()
        cur.execute(sqlRequest)
        rawsummary = fixLob(cur)
        #rawsummary = cur.fetchall()
        mObjectStores = {}
        mObjectStoresTk = {}
        if len(rawsummary) > 0:
            for row in rawsummary:
                id = -1
                try:
                    id = int(row[3])
                except ValueError:
                    pass

                if not row[3] is None and id in objectStoresNames:
                    osName = objectStoresNames[id]
                else:
                    osName = "Not defined"
                compsite = row[2]
                status = row[0]
                count = row[1]

                tk = setCacheData(request, pandaid = row[4],compsite = row[2])
                if osName in mObjectStores:
                    if not compsite in mObjectStores[osName]:
                        mObjectStores[osName][compsite] = {}
                        for state in sitestatelist + ["closed"]:
                            mObjectStores[osName][compsite][state] = {'count': 0, 'tk': 0}
                    mObjectStores[osName][compsite][status] = {'count': count, 'tk': tk}
                    if not status in mObjectStoresTk[osName]:
                        mObjectStoresTk[osName][status] = []
                    mObjectStoresTk[osName][status].append(tk)
                else:
                    mObjectStores[osName] = {}
                    mObjectStores[osName][compsite] = {}
                    mObjectStoresTk[osName]={}
                    mObjectStoresTk[osName][status]=[]
                    for state in sitestatelist + ["closed"]:
                        mObjectStores[osName][compsite][state] = {'count': 0, 'tk': 0}
                    mObjectStores[osName][compsite][status] = {'count': count, 'tk': tk}
                    mObjectStoresTk[osName][status].append(tk)
        ### Getting tk's for parents ####
        for osName in mObjectStoresTk:
            for state in mObjectStoresTk[osName]:
                mObjectStoresTk[osName][state] = setCacheData(request, childtk=','.join(mObjectStoresTk[osName][state]))

        mObjectStoresSummary = {}
        for osName in mObjectStores:
            mObjectStoresSummary[osName] ={}
            for site in mObjectStores[osName]:
                for state in mObjectStores[osName][site]:
                    if state in mObjectStoresSummary[osName]:
                        mObjectStoresSummary[osName][state]['count'] += mObjectStores[osName][site][state]['count']
                        mObjectStoresSummary[osName][state]['tk'] = 0

                    else:
                        mObjectStoresSummary[osName][state] = {}
                        mObjectStoresSummary[osName][state]['count'] = mObjectStores[osName][site][state]['count']
                        mObjectStoresSummary[osName][state]['tk'] = 0
        for osName in mObjectStoresSummary:
            for state in mObjectStoresSummary[osName]:
                if (mObjectStoresSummary[osName][state]['count'] > 0):
                    mObjectStoresSummary[osName][state]['tk'] = mObjectStoresTk[osName][state]
        data = {
            'mObjectStoresSummary': mObjectStoresSummary,
            'mObjectStores': mObjectStores,
            'viewParams': request.session['viewParams'],
            'statelist': sitestatelist + ["closed"],
            'template': 'dashObjectStore.html',
            'built': datetime.now().strftime("%m-%d %H:%M:%S"),
        }
        response = render_to_response('dashObjectStore.html', data, content_type='text/html')
        setCacheEntry(request, "dashboard", json.dumps(data, cls=DateEncoder), 60 * 25)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    else:

        notime = True
        if len({'date_to', 'hours'}.intersection(request.session['requestParams'].keys())) > 0:
            notime = False

        fullsummary = dashSummary(request, hours=hours, view=view, cloudview=cloudview, notime=notime)
        cloudTaskSummary = wgTaskSummary(request, fieldname='cloud', view=view, taskdays=taskdays)
        jobsLeft = {}
        rw = {}

        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            rwData, nRemJobs = calculateRWwithPrio_JEDI(query)
            for cloud in fullsummary:
                if cloud['name'] in nRemJobs.keys():
                    jobsLeft[cloud['name']] = nRemJobs[cloud['name']]
                if cloud['name'] in rwData.keys():
                    rw[cloud['name']] = rwData[cloud['name']]

        request.session['max_age_minutes'] = 6
        if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
            'json' not in request.session['requestParams'])) or ('keephtml' in request.session['requestParams']):
            xurl = extensibleURL(request)
            nosorturl = removeParam(xurl, 'sortby', mode='extensible')
            del request.session['TFIRST']
            del request.session['TLAST']
            data = {
                'request': request,
                'viewParams': request.session['viewParams'],
                'requestParams': request.session['requestParams'],
                'url': request.path,
                'xurl': xurl,
                'nosorturl': nosorturl,
                'user': None,
                'summary': fullsummary,
                'vosummary': vosummary,
                'view': view,
                'mode': 'site',
                'cloudview': cloudview,
                'hours': hours,
                'errthreshold': errthreshold,
                'cloudTaskSummary': cloudTaskSummary,
                'taskstates': taskstatedict,
                'taskdays': taskdays,
                'estailtojobslinks':estailtojobslinks,
                'noldtransjobs': noldtransjobs,
                'transclouds': transclouds,
                'transrclouds': transrclouds,
                'hoursSinceUpdate': hoursSinceUpdate,
                'jobsLeft': jobsLeft,
                'rw': rw,
                'template': 'dashboard.html',
                'built': datetime.now().strftime("%H:%M:%S"),
            }
            ##self monitor
            response = render_to_response('dashboard.html', data, content_type='text/html')
            setCacheEntry(request, "dashboard", json.dumps(data, cls=DateEncoder), 60 * 60)
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response
        else:
            del request.session['TFIRST']
            del request.session['TLAST']

            data = {
                'summary': fullsummary,
                'vosummary': vosummary,
                'view': view,
                'mode': 'site',
                'cloudview': cloudview,
                'hours': hours,
                'errthreshold': errthreshold,
                'cloudTaskSummary': cloudTaskSummary,
                'taskstates': taskstatedict,
                'taskdays': taskdays,
                'noldtransjobs': noldtransjobs,
                'transclouds': transclouds,
                'transrclouds': transrclouds,
                'hoursSinceUpdate': hoursSinceUpdate,
                'jobsLeft': jobsLeft,
                'rw': rw,
                'built': datetime.now().strftime("%H:%M:%S"),
            }

            return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

@login_customrequired
def dashAnalysis(request):
    return dashboard(request, view='analysis')

@login_customrequired
def dashProduction(request):
    return dashboard(request, view='production')

@login_customrequired
def dashObjectStore(request):
    return dashboard(request, view='objectstore')

def dashTasks(request, hours, view='production'):
    valid, response = initRequest(request)
    if not valid: return response

    if view == 'production':
        errthreshold = 5
    else:
        errthreshold = 15

    if 'days' in request.session['requestParams']:
        taskdays = int(request.session['requestParams']['days'])
    else:
        taskdays = 7
    hours = taskdays * 24
    query = setupView(request, hours=hours, limit=999999, opmode=view, querytype='task')

    cloudTaskSummary = wgTaskSummary(request, fieldname='cloud', view=view, taskdays=taskdays)

    # taskJobSummary = dashTaskSummary(request, hours, view)     not particularly informative
    taskJobSummary = []

    if 'display_limit' in request.session['requestParams']:
        try:
            display_limit = int(request.session['requestParams']['display_limit'])
        except:
            display_limit = 300
    else:
        display_limit = 300

    cloudview = 'cloud'
    if 'cloudview' in request.session['requestParams']:
        cloudview = request.session['requestParams']['cloudview']
    if view == 'analysis':
        cloudview = 'region'
    elif view != 'production':
        cloudview = 'N/A'

    fullsummary = dashSummary(request, hours=hours, view=view, cloudview=cloudview)

    jobsLeft = {}
    rw = {}
    rwData, nRemJobs = calculateRWwithPrio_JEDI(query)
    for cloud in fullsummary:
        leftCount = 0
        if cloud['name'] in nRemJobs.keys():
            jobsLeft[cloud['name']] = nRemJobs[cloud['name']]
        if cloud['name'] in rwData.keys():
            rw[cloud['name']] = rwData[cloud['name']]

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'user': None,
            'view': view,
            'mode': 'task',
            'hours': hours,
            'errthreshold': errthreshold,
            'cloudTaskSummary': cloudTaskSummary,
            'taskstates': taskstatedict,
            'taskdays': taskdays,
            'taskJobSummary': taskJobSummary[:display_limit],
            'display_limit': display_limit,
            'jobsLeft': jobsLeft,
            'estailtojobslinks': '',
            'rw': rw,
            'template': 'dashboard.html',
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        setCacheEntry(request, "dashboard", json.dumps(data, cls=DateEncoder), 60 * 60)
        response = render_to_response('dashboard.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        remainingEvents = RemainedEventsPerCloud3dayswind.objects.values('cloud', 'nrem')
        remainingEventsSet = {}
        for remev in remainingEvents:
            remainingEventsSet[remev['cloud']] = remev['nrem']
        data = {
            'jobsLeft': jobsLeft,
            'remainingWeightedEvents': remainingEventsSet,
        }
        return HttpResponse(json.dumps(data), content_type='application/json')


def taskESExtendedInfo(request):
    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return HttpResponse("Not jeditaskid supplied", content_type='text/html')

    eventsdict=[]
    equery = {'jeditaskid': jeditaskid}
    eventsdict.extend(
        JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
    for state in eventsdict: state['statusname'] = eventservicestatelist[state['status']]

    estaskstr = ''
    for s in eventsdict:
        estaskstr += " %s(%s) " % (s['statusname'], s['count'])
    return HttpResponse(estaskstr, content_type='text/html')


def removeDublicates(inlist, key):

    ids = set([item[key] for item in inlist])
    outlist = []
    for item in inlist:
        if item[key] in ids:
            outlist.append(item)
            ids.remove(item[key])
    return outlist


@login_customrequired
def getCSRFToken(request):
    c = {}
    user = request.user
    if user.is_authenticated:
        c.update(csrf(request))
        return render_to_response("csrftoken.html", c)
    else:
        resp = {"detail": "User not authenticated. Please login to bigpanda"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response


@login_customrequired
@csrf_exempt
def taskList(request):
    valid, response = initRequest(request)
    if not valid: return response

    thread = None
    dkey = digkey(request)
    # Here we try to get cached data
    data = getCacheEntry(request, "taskList")
    #data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if data['eventservice'] == True:
            response = render_to_response('taskListES.html', data, content_type='text/html')
        else:
            response = render_to_response('taskList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])
    else:
        limit = 1000
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'pctfailed':
            limit = 50000

    if 'tasktype' in request.session['requestParams'] and request.session['requestParams']['tasktype'].startswith(
            'anal'):
        hours = 3 * 24
    else:
        hours = 7 * 24
    eventservice = False
    if 'eventservice' in request.session['requestParams'] and (
            request.session['requestParams']['eventservice'] == 'eventservice' or request.session['requestParams'][
        'eventservice'] == '1'):
        eventservice = True
        hours = 7 * 24
    extraquery = ''
    if 'hashtag' in request.session['requestParams']:
        hashtagsrt = request.session['requestParams']['hashtag']
        if ',' in hashtagsrt:
            hashtaglistquery = ''.join("'" + ht + "' ," for ht in hashtagsrt.split(','))
        elif '|' in hashtagsrt:
            hashtaglistquery = ''.join("'" + ht + "' ," for ht in hashtagsrt.split('|'))
        else:
            hashtaglistquery = "'" + request.session['requestParams']['hashtag'] + "'"
        hashtaglistquery = hashtaglistquery[:-1] if hashtaglistquery[-1] == ',' else hashtaglistquery
        extraquery = """JEDITASKID IN ( SELECT HTT.TASKID FROM ATLAS_DEFT.T_HASHTAG H, ATLAS_DEFT.T_HT_TO_TASK HTT WHERE JEDITASKID = HTT.TASKID AND H.HT_ID = HTT.HT_ID AND H.HASHTAG IN ( %s ))""" % (hashtaglistquery)

    if 'tape' in  request.session['requestParams']:
        extraquery = """JEDITASKID IN (SELECT TASKID FROM ATLAS_DEFT.t_production_task where PRIMARY_INPUT in (select DATASET FROM ATLAS_DEFT.T_DATASET_STAGING) )"""

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='task', wildCardExt=True)

    tmpTableName = 'ATLAS_PANDABIGMON.TMP_IDS1Debug'
    if 'jeditaskid__in' in query:
        taskl = query['jeditaskid__in']
        if len(taskl) > 20:
            transactionKey = insert_to_temp_table(taskl)
            selectTail = """jeditaskid in (SELECT tmp.id FROM %s tmp where TRANSACTIONKEY=%i)""" % (tmpTableName, transactionKey)
            extraquery = selectTail if len(extraquery) == 0 else extraquery + ' AND ' + selectTail
            del query['jeditaskid__in']

    if len(extraquery) > 0:
        if len(wildCardExtension) > 0:
            wildCardExtension += ' AND ( ' + extraquery + ' )'
        else:
            wildCardExtension = extraquery
    listTasks = []
    if 'statenotupdated' in request.session['requestParams']:
        tasks = taskNotUpdated(request, query, wildCardExtension)
    else:
        #wildCardExtension = "(((UPPER(taskname)  LIKE UPPER('%%.%%')) AND (UPPER(taskname)  LIKE UPPER('%%mc%%')) AND (UPPER(taskname)  LIKE UPPER('%%.CAOD_HIGG5D1.%%')) AND (UPPER(taskname)  LIKE UPPER('%%.32-07-8/'))))"
        tasks = JediTasksOrdered.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values()
        listTasks.append(JediTasksOrdered)
        if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                    'json' not in request.session['requestParams'])):
            pass
            thread = Thread(target=totalCount, args=(listTasks, query, wildCardExtension, dkey))
            thread.start()
        else:
            thread = None
    # Getting hashtags for task selection
    taskl = []
    for task in tasks:
        taskl.append(task['jeditaskid'])

    transactionKey = insert_to_temp_table(taskl)

    # For tasks plots
    setCacheEntry(request, transactionKey, taskl, 60 * 20, isData=True)
    new_cur = connection.cursor()
    new_cur.execute(
        """
        select htt.TASKID,
            LISTAGG(h.hashtag, ',') within GROUP (order by htt.taskid) as hashtags
        from ATLAS_DEFT.T_HASHTAG h, ATLAS_DEFT.T_HT_TO_TASK htt , %s tmp
        where TRANSACTIONKEY=%i and h.ht_id = htt.ht_id and tmp.id = htt.taskid
        GROUP BY htt.TASKID
        """ % (tmpTableName, transactionKey)
    )
    taskhashtags = dictfetchall(new_cur)

    datasetstage = []
    if 'tape' in  request.session['requestParams']:
        stagesource = ''
        if 'stagesource' in request.session['requestParams'] and request.session['requestParams']['stagesource']!='Unknown':
            stagesource = " and t1.SOURCE_RSE='" + request.session['requestParams']['stagesource'].strip().replace("'","''")+"\'"
        elif 'stagesource' in request.session['requestParams'] and request.session['requestParams']['stagesource']=='Unknown':
            stagesource = ' and t1.SOURCE_RSE is null'
            

        new_cur.execute(
            """
            SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE, t1.TOTAL_FILES, 
                t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID FROM ATLAS_DEFT.T_DATASET_STAGING t1
                INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID %s and taskid in (SELECT tmp.id FROM %s tmp where TRANSACTIONKEY=%i)
            """ % (stagesource, tmpTableName, transactionKey)
        )
        datasetstage = dictfetchall(new_cur)
        taskslistfiltered = set()
        for datasetstageitem in datasetstage:
            taskslistfiltered.add(datasetstageitem['TASKID'])
            if datasetstageitem['START_TIME']:
                datasetstageitem['START_TIME'] = datasetstageitem['START_TIME'].strftime(defaultDatetimeFormat)
            else:
                datasetstageitem['START_TIME'] = ''

            if datasetstageitem['END_TIME']:
                datasetstageitem['END_TIME'] = datasetstageitem['END_TIME'].strftime(defaultDatetimeFormat)
            else:
                datasetstageitem['END_TIME'] = ''

            if not datasetstageitem['SOURCE_RSE']:
                datasetstageitem['SOURCE_RSE'] = 'Unknown'


            if datasetstageitem['UPDATE_TIME']:
                datasetstageitem['UPDATE_TIME'] = datasetstageitem['UPDATE_TIME'].strftime(defaultDatetimeFormat)
            else:
                datasetstageitem['UPDATE_TIME'] = ''

        if 'stagesource' in request.session['requestParams']:
            newtasks = []
            newtaskl = []

            for task in tasks:
                if task['jeditaskid'] in taskslistfiltered:
                    newtaskl.append(task['jeditaskid'])
                    newtasks.append(task)
            tasks =  newtasks
            taskl = newtaskl


    eventInfoDict = {}
    if eventservice:
        #we get here events data
        tquery = {}
        tasksEventInfo = GetEventsForTask.objects.filter(**tquery).extra(
            where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values('jeditaskid', 'totevrem', 'totev')

        #We do it because we intermix raw and queryset queries. With next new_cur.execute tasksEventInfo cleares
        for tasksEventInfoItem in tasksEventInfo:
            listItem = {}
            listItem["jeditaskid"] = tasksEventInfoItem["jeditaskid"]
            listItem["totevrem"] = tasksEventInfoItem["totevrem"]
            listItem["totev"] = tasksEventInfoItem["totev"]
            eventInfoDict[tasksEventInfoItem["jeditaskid"]] = listItem


    # clean temporary table
    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))

    taskids = {}
    for taskid in taskhashtags:
        taskids[taskid['TASKID']] = taskid['HASHTAGS']


    # Filtering tasks if there are a few hashtahgs with 'AND' operand in query
    if 'hashtagsrt' in locals() and ',' in hashtagsrt:
        thashtags = hashtagsrt.split(',')
        newtasks = []
        for task in tasks:
            if task['jeditaskid'] in taskids.keys():
                if all(ht+',' in taskids[task['jeditaskid']]+',' for ht in thashtags):
                    newtasks.append(task)
        tasks = newtasks

    hashtags = []
    for task in tasks:
        # Forming hashtag list for summary attribute table
        if task['jeditaskid'] in taskids.keys():
            task['hashtag'] = taskids[task['jeditaskid']]
            for hashtag in taskids[task['jeditaskid']].split(','):
                if hashtag not in hashtags:
                    hashtags.append(hashtag)

        if eventservice:
            # Addind event data
            if task['jeditaskid'] in eventInfoDict.keys():
                task['eventsData'] = eventInfoDict[task['jeditaskid']]

    if len(hashtags) > 0:
        hashtags = sorted(hashtags, key=lambda h: h.lower())

    tasks = cleanTaskList(request, tasks)
    ntasks = len(tasks)
    nmax = ntasks

    #    if 'display_limit' in request.session['requestParams']:
    #            and int(request.session['requestParams']['display_limit']) < nmax:
    #        display_limit = int(request.session['requestParams']['display_limit'])
    #        nmax = display_limit
    #        url_nolimit = removeParam(request.get_full_path(), 'display_limit')
    #    else:
    #        display_limit = 300
    #        nmax = display_limit
    #        url_nolimit = request.get_full_path()


    if 'display_limit' not in request.session['requestParams']:
        display_limit = 100
        nmax = display_limit
        url_nolimit = request.get_full_path() + "&display_limit=" + str(nmax)
    else:
        display_limit = int(request.session['requestParams']['display_limit'])
        nmax = display_limit
        url_nolimit = request.get_full_path() + "&display_limit=" + str(nmax)

    # from django.db import connection
    # print 'SQL query:', connection.queries

    tasks = getTaskScoutingInfo(tasks, nmax)

    if 'tape' in  request.session['requestParams'] and len(datasetstage)>0:
        datasetRSEsHash = {}
        for dataset in datasetstage:
            datasetRSEsHash[dataset['TASKID']] = dataset['SOURCE_RSE']

        for task in tasks:
            task['stagesource'] = datasetRSEsHash.get(task['jeditaskid'], 'Unknown')



    ## For event service, pull the jobs and event ranges

    doESCalc = False

    if eventservice and doESCalc:
        taskl = []
        for task in tasks:
            taskl.append(task['jeditaskid'])
        jquery = {}
        jquery['jeditaskid__in'] = taskl
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**jquery).values('pandaid', 'jeditaskid'))
        jobs.extend(Jobsarchived4.objects.filter(**jquery).values('pandaid', 'jeditaskid'))
        taskdict = {}
        for job in jobs:
            taskdict[job['pandaid']] = job['jeditaskid']
        estaskdict = {}
        esjobs = []
        for job in jobs:
            esjobs.append(job['pandaid'])

        random.seed()

        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
        else:
            tmpTableName = "TMP_IDS1"

        transactionKey = random.randrange(1000000)
#        connection.enter_transaction_management()
        new_cur = connection.cursor()
        executionData = []
        for id in esjobs:
            executionData.append((id, transactionKey))
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, executionData)

#        connection.commit()
        new_cur.execute(
            """
            SELECT /*+ dynamic_sampling(TMP_IDS1 0) cardinality(TMP_IDS1 10) INDEX_RS_ASC(ev JEDI_EVENTS_PANDAID_STATUS_IDX) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  PANDAID,STATUS FROM ATLAS_PANDA.JEDI_EVENTS ev, %s WHERE TRANSACTIONKEY=%i AND PANDAID = ID
            """ % (tmpTableName, transactionKey)
        )
        evtable = dictfetchall(new_cur)

        #        esquery = {}
        #        esquery['pandaid__in'] = esjobs
        #        evtable = JediEvents.objects.filter(**esquery).values('pandaid','status')

        new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
 #       connection.commit()
 #       connection.leave_transaction_management()

        for ev in evtable:
            taskid = taskdict[ev['PANDAID']]
            if taskid not in estaskdict:
                estaskdict[taskid] = {}
                for s in eventservicestatelist:
                    estaskdict[taskid][s] = 0
            evstat = eventservicestatelist[ev['STATUS']]
            estaskdict[taskid][evstat] += 1
        for task in tasks:
            taskid = task['jeditaskid']
            if taskid in estaskdict:
                estaskstr = ''
                for s in estaskdict[taskid]:
                    if estaskdict[taskid][s] > 0:
                        estaskstr += " %s(%s) " % (s, estaskdict[taskid][s])
                task['estaskstr'] = estaskstr

    ## set up google flow diagram
    flowstruct = buildGoogleFlowDiagram(request, tasks=tasks)
    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    nohashtagurl = removeParam(xurl, 'hashtag', mode='extensible')
    if thread!=None:
        try:
            thread.join()
            tasksTotalCount = sum(tcount[dkey])
            print (dkey)
            print (tcount[dkey])
            del tcount[dkey]
            print (tcount)
            print (tasksTotalCount)
        except:
            tasksTotalCount = -1
    else: tasksTotalCount = -1

    listPar = []
    for key, val in request.session['requestParams'].items():
        if (key != 'limit' and key != 'display_limit'):
            listPar.append(key + '=' + str(val))
    if len(listPar) > 0:
        urlParametrs = '&'.join(listPar) + '&'
    else:
        urlParametrs = None
    print (listPar)
    del listPar
    if (math.fabs(ntasks - tasksTotalCount) < 1000 or tasksTotalCount == -1):
        tasksTotalCount = None
    else:
        tasksTotalCount = int(math.ceil((tasksTotalCount + 10000) / 10000) * 10000)
    tasksToShow = tasks[:nmax]
    for task in tasksToShow:
        if task['creationdate']:
            task['creationdate'] = task['creationdate'].strftime(defaultDatetimeFormat)
        if task['modificationtime']:
            task['modificationtime'] = task['modificationtime'].strftime(defaultDatetimeFormat)
        if task['starttime']:
            task['starttime'] = task['starttime'].strftime(defaultDatetimeFormat)
        if task['statechangetime']:
            task['statechangetime'] = task['statechangetime'].strftime(defaultDatetimeFormat)
        if task['ttcrequested']:
            task['ttcrequested'] = task['ttcrequested'].strftime(defaultDatetimeFormat)



    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        ## Add info to the json dump if the request is for a single task
        if len(tasks) == 1:
            id = tasks[0]['jeditaskid']
            dsquery = {'jeditaskid': id, 'type__in': ['input', 'output']}
            dsets = JediDatasets.objects.filter(**dsquery).values()
            dslist = []
            for ds in dsets:
                dslist.append(ds)
            tasks[0]['datasets'] = dslist
        else:
            for task in tasks:
                id = task['jeditaskid']
                dsquery = {'jeditaskid': id, 'type__in': ['input', 'output']}
                dsets = JediDatasets.objects.filter(**dsquery).values()
                dslist = []
                for ds in dsets:
                    dslist.append(ds)
                task['datasets'] = dslist

        # getting jobs metadata if it is requested in URL [ATLASPANDA-492]
        if 'extra' in request.session['requestParams'] and 'metastruct' in request.session['requestParams']['extra']:
            jeditaskids = list(set([task['jeditaskid'] for task in tasks]))
            MAX_N_TASKS = 100 #protection against DB overloading
            if len(jeditaskids) <= MAX_N_TASKS:
                job_pids = []
                jobQuery = {
                    'jobstatus__in': ['finished', 'failed', 'transferring', 'merging', 'cancelled', 'closed', 'holding'],
                    'jeditaskid__in': jeditaskids
                }
                job_pids.extend(Jobsarchived4.objects.filter(**jobQuery).values('pandaid', 'jeditaskid', 'jobstatus', 'creationtime'))
                job_pids.extend(Jobsarchived.objects.filter(**jobQuery).values('pandaid', 'jeditaskid', 'jobstatus', 'creationtime'))
                if len(job_pids) > 0:
                    jobs = addJobMetadata(job_pids, require=True)
                    taskMetadata = {}
                    for job in jobs:
                        if not job['jeditaskid'] in taskMetadata:
                            taskMetadata[job['jeditaskid']] = {}
                        if 'metastruct' in job:
                            taskMetadata[job['jeditaskid']][job['pandaid']] = job['metastruct']

                    for task in tasks:
                        if task['jeditaskid'] in taskMetadata:
                            task['jobs_metadata'] = taskMetadata[task['jeditaskid']]

        if 'extra' in request.session['requestParams'] and 'jobstatecount' in request.session['requestParams']['extra']:
            js_count_bytask_dict = get_job_state_summary_for_tasklist(tasks)
            for task in tasks:
                if task['jeditaskid'] in js_count_bytask_dict:
                    task['job_state_count'] = js_count_bytask_dict[task['jeditaskid']]
                else:
                    task['job_state_count'] = {}
        dump = json.dumps(tasks, cls=DateEncoder)
        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse(dump, content_type='application/json')
    else:
        #tasks = removeDublicates(tasks, "jeditaskid")
        sumd = taskSummaryDict(request, tasks, copy.deepcopy(standard_taskfields) +
                                               ['stagesource'] if 'tape' in  request.session['requestParams'] else copy.deepcopy(standard_taskfields))
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'tasks': tasksToShow,
            'datasetstage': json.dumps(datasetstage, cls=DateEncoder),
            'ntasks': ntasks,
            'sumd': sumd,
            'hashtags': hashtags,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'nohashtagurl': nohashtagurl,
            'url_nolimit': url_nolimit,
            'display_limit': nmax,
            'flowstruct': flowstruct,
            'eventservice': eventservice,
            'requestString': urlParametrs,
            'tasksTotalCount': tasksTotalCount,
            'built': datetime.now().strftime("%H:%M:%S"),
            'idtasks': transactionKey,
        }

        setCacheEntry(request, "taskList", json.dumps(data, cls=DateEncoder), 60 * 20)
        if eventservice:
            response = render_to_response('taskListES.html', data, content_type='text/html')
        else:
            response = render_to_response('taskList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

@never_cache
def killtasks(request):
    valid, response = initRequest(request)
    if not valid: return response
    taskid = -1
    action = -1
    if 'task' in request.session['requestParams']:
        taskid = int(request.session['requestParams']['task'])
    if 'action' in request.session['requestParams']:
        action = int(request.session['requestParams']['action'])

    prodsysHost = None
    prodsysToken = None
    prodsysUrl = None
    username = None
    fullname = None

    if 'prodsysHost' in PRODSYS:
        prodsysHost = PRODSYS['prodsysHost']
    if 'prodsysToken' in PRODSYS:
        prodsysToken = PRODSYS['prodsysToken']

    if action == 0:
        prodsysUrl = '/prodtask/task_action_ext/finish/'
    elif action == 1:
        prodsysUrl = '/prodtask/task_action_ext/abort/'
    else:
        resp = {"detail": "Action is not recognized"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response

    user = request.user
    if user.is_authenticated and (not user.social_auth is None) and (not user.social_auth.get(provider='cernauth2') is None) \
            and (not user.social_auth.get(provider='cernauth2').extra_data is None) and ('username' in user.social_auth.get(provider='cernauth2').extra_data):
        username = user.social_auth.get(provider='cernauth2').extra_data['username']
        fullname = user.social_auth.get(provider='cernauth2').extra_data['name']

    else:
        resp = {"detail": "User not authenticated. Please login to bigpanda mon with CERN"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response

    if action == 1:
        postdata = {"username": username, "task": taskid, "userfullname":fullname}
    else:
        postdata = {"username": username, "task": taskid, "parameters":[1], "userfullname":fullname}


    headers = {'Content-Type':'application/json', 'Accept': 'application/json', 'Authorization': 'Token '+prodsysToken}

    conn = urllib3.HTTPSConnectionPool(prodsysHost, timeout=100)
    resp = None

#    if request.session['IS_TESTER']:
    resp = conn.urlopen('POST', prodsysUrl, body=json.dumps(postdata, cls=DateEncoder), headers=headers, retries=1, assert_same_host=False)
#    else:
#        resp = {"detail": "You are not allowed to test. Sorry"}
#        dump = json.dumps(resp, cls=DateEncoder)
#        response = HttpResponse(dump, mimetype='text/plain')
#        return response


    if resp and len(resp.data) > 0:
        try:
            resp = json.loads(resp.data)
            if resp['result'] == "FAILED":
                resp['detail'] = 'Result:' + resp['result'] + ' with reason:' + resp['exception']
            elif resp['result'] == "OK":
                resp['detail'] = 'Action peformed successfully, details: ' + resp['details']
        except:
            resp = {"detail":"prodsys responce could not be parced"}
    else:
        resp = {"detail": "Error with sending request to prodsys"}
    dump = json.dumps(resp, cls=DateEncoder)
    response = HttpResponse(dump, content_type='application/json')
    return response


def getTaskScoutingInfo(tasks, nmax):
    taskslToBeDisplayed = tasks[:nmax]
    tasksIdToBeDisplayed = [task['jeditaskid'] for task in taskslToBeDisplayed]
    tquery = {}

    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"

    transactionKey = random.randrange(1000000)
    new_cur = connection.cursor()
    executionData = []
    for id in tasksIdToBeDisplayed:
        executionData.append((id, transactionKey))
    query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
    new_cur.executemany(query, executionData)

    tasksEventInfo = GetEventsForTask.objects.filter(**tquery).extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values('jeditaskid', 'totevrem', 'totev')

    #We do it because we intermix raw and queryset queries. With next new_cur.execute tasksEventInfo cleares
    tasksEventInfoList = []
    for tasksEventInfoItem in tasksEventInfo:
        listItem = {}
        listItem["jeditaskid"] = tasksEventInfoItem["jeditaskid"]
        listItem["totevrem"] = tasksEventInfoItem["totevrem"]
        listItem["totev"] = tasksEventInfoItem["totev"]
        tasksEventInfoList.append(listItem)

    tasksEventInfoList.reverse()

    failedInScouting = JediDatasets.objects.filter(**tquery).extra(where=["NFILESFAILED > NFILESTOBEUSED AND JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey) ]).values('jeditaskid')

    taskStatuses = dict((task['jeditaskid'], task['status']) for task in tasks)

    failedInScouting = [item['jeditaskid'] for item in failedInScouting if
                        (taskStatuses[item['jeditaskid']] in ('failed', 'broken'))]

    # scoutingHasCritFailures
    tquery['nfilesfailed__gt'] = 0
    scoutingHasCritFailures = JediDatasets.objects.filter(**tquery).extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values('jeditaskid')
    scoutingHasCritFailures = [item['jeditaskid'] for item in scoutingHasCritFailures if
                               (taskStatuses[item['jeditaskid']] in ('scouting'))]

    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
    transactionKey = random.randrange(1000000)
    executionData = []
    for id in scoutingHasCritFailures:
        executionData.append((id, transactionKey))
    query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
    new_cur.executemany(query, executionData)

    tquery = {}
    tquery['nfilesfailed'] = 0
    scoutingHasNonCritFailures = JediDatasets.objects.filter(**tquery).extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values('jeditaskid')
    scoutingHasNonCritFailures = [item['jeditaskid'] for item in scoutingHasNonCritFailures if (
    taskStatuses[item['jeditaskid']] == 'scouting' and item['jeditaskid'] not in scoutingHasCritFailures)]


    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
    transactionKey = random.randrange(1000000)
    executionData = []
    for id in scoutingHasNonCritFailures:
        executionData.append((id, transactionKey))
    query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
    new_cur.executemany(query, executionData)

    tquery = {}
    tquery['relationtype'] = 'retry'
    scoutingHasNonCritFailures = JediJobRetryHistory.objects.filter(**tquery).extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values('jeditaskid')
    scoutingHasNonCritFailures = [item['jeditaskid'] for item in scoutingHasNonCritFailures]

    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))

    for task in taskslToBeDisplayed:
        correspondendEventInfo = []
        if tasksEventInfoList and len(tasksEventInfoList) > 0:
            correspondendEventInfo = [item for item in tasksEventInfoList if item["jeditaskid"]==task['jeditaskid']] #filter(lambda n: n.get('jeditaskid') == task['jeditaskid'], tasksEventInfo)
        if len(correspondendEventInfo) > 0:
            task['totevrem'] = int(correspondendEventInfo[0]['totevrem'])
            task['totev'] = correspondendEventInfo[0]['totev']
        else:
            task['totevrem'] = 0
            task['totev'] = 0
        if (task['jeditaskid'] in failedInScouting):
            task['failedscouting'] = True
        if (task['jeditaskid'] in scoutingHasCritFailures):
            task['scoutinghascritfailures'] = True
        if (task['jeditaskid'] in scoutingHasNonCritFailures):
            task['scoutinghasnoncritfailures'] = True

    return tasks

def getErrorSummaryForEvents(request):
    valid, response = initRequest(request)
    if not valid: return response
    data = {}
    eventsErrors = []
    print ('getting error summary for events')
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    else:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
    if 'mode' in request.session['requestParams']:
        mode = request.session['requestParams']['mode']
    else:
        mode = 'drop'
    if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk'] > 0:
        transactionKey = int(request.session['requestParams']['tk'])
    else:
        transactionKey = None
    if 'tkdj' in request.session['requestParams'] and request.session['requestParams']['tkdj'] > 0:
        transactionKeyDJ = int(request.session['requestParams']['tkdj'])
    else:
        transactionKeyDJ = None
    equery = {}
    equery['jeditaskid']=jeditaskid
    equery['error_code__isnull'] = False

    if mode == 'drop':
        eventsErrors = []

        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
        else:
            tmpTableName = "TMP_IDS1DEBUG"

        new_cur = connection.cursor()
        if transactionKey:
            eequery = """
            select error_code,
              sum(neventsinjob) as nevents,
              sum(nerrorsinjob) as nerrors,
              count(pandaid) as njobs,
              LISTAGG(case when aff <= 10 then pandaid end,',' ) WITHIN group (order by error_code, aff) as pandaidlist
            from (
              select  pandaid, error_code, neventsinjob, nerrorsinjob,
                row_number() over (partition by error_code ORDER BY neventsinjob desc) as aff
              from (
                  (select pandaid, error_code,
                    sum(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) as neventsinjob,
                    count(*) as nerrorsinjob
                  from ATLAS_PANDA.Jedi_events
                  where jeditaskid={} and ERROR_CODE is not null
                  group by error_code, pandaid ) e
                join
                  (select ID from {} where TRANSACTIONKEY={} ) j
                on e.pandaid = j.ID))
            group by error_code""".format(jeditaskid, tmpTableName, transactionKey)
        elif transactionKeyDJ:
            eequery = """
            select error_code,
              sum(neventsinjob) as nevents,
              sum(nerrorsinjob) as nerrors,
              count(pandaid) as njobs,
              LISTAGG(case when aff <= 10 then pandaid end,',' ) WITHIN group (order by error_code, aff) as pandaidlist
            from (
              select  pandaid, error_code, neventsinjob, nerrorsinjob,
                row_number() over (partition by error_code ORDER BY neventsinjob desc) as aff
              from (
                  (select pandaid, error_code,
                    sum(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) as neventsinjob,
                    count(*) as nerrorsinjob
                  from ATLAS_PANDA.Jedi_events
                  where jeditaskid={} and ERROR_CODE is not null 
                    and pandaid not in ( select ID from {} where TRANSACTIONKEY={} )
                  group by error_code, pandaid ) e
                ))
            group by error_code""".format(jeditaskid, tmpTableName, transactionKeyDJ)
        else:
            data = {"error": "no failed events found"}
            return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
        new_cur.execute(eequery)
        eventsErrorsUP = dictfetchall(new_cur)
    elif mode == 'nodrop':
        # eventsErrors = JediEvents.objects.filter(**equery).values('error_code').annotate(njobs=Count('pandaid',distinct=True),nevents=Sum('def_max_eventid', field='def_max_eventid-def_min_eventid+1'))
        new_cur = connection.cursor()
        new_cur.execute(
            """select error_code, sum(neventsinjob) as nevents, sum(nerrorsinjob) as nerrors , count(pandaid) as njobs,
                  LISTAGG(case when aff <= 10 then pandaid end,',' ) WITHIN group (order by error_code, aff) as pandaidlist
                  from (select pandaid, error_code,
                        sum(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) as neventsinjob,
                        count(*) as nerrorsinjob,
                        row_number() over (partition by error_code ORDER BY sum(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) desc) as aff
                          from ATLAS_PANDA.Jedi_events
                          where jeditaskid=%i and ERROR_CODE is not null
                          group by error_code, pandaid)
                  group by error_code
            """ % (jeditaskid)
        )
        eventsErrorsUP = dictfetchall(new_cur)
    else:
        data = {"error": "wrong mode specified"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')


    for error in eventsErrorsUP:
        line = dict()
        for key, value in error.items():
            line[key.lower()] = value
        eventsErrors.append(line)

    for eventserror in eventsErrors:
        try:
            eventserror['error_code']=int(eventserror['error_code'])
            if eventserror['error_code'] in errorCodes['piloterrorcode'].keys():
                eventserror['error_description'] = errorCodes['piloterrorcode'][eventserror['error_code']]
            else:
                eventserror['error_description'] = ''
        except:
            eventserror['error_description'] = ''
        if eventserror['pandaidlist'] and len(eventserror['pandaidlist']) > 0:
            eventserror['pandaidlist'] = eventserror['pandaidlist'].split(',')



    data = {'errors' : eventsErrors}

    response = render_to_response('eventsErrorSummary.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def getSummaryForTaskList(request):
    valid, response = initRequest(request)
    if not valid: return response
    data = {}

    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])
    else:
        limit = 5000

    if not valid: return response
    if 'tasktype' in request.session['requestParams'] and request.session['requestParams']['tasktype'].startswith(
            'anal'):
        hours = 3 * 24
    else:
        hours = 7 * 24
    eventservice = False
    if 'eventservice' in request.session['requestParams'] and (
                    request.session['requestParams']['eventservice'] == 'eventservice' or
                    request.session['requestParams']['eventservice'] == '1'): eventservice = True
    if eventservice: hours = 7 * 24
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='task',
                                                           wildCardExt=True)
    if 'statenotupdated' in request.session['requestParams']:
        tasks = taskNotUpdated(request, query, wildCardExtension)
    else:
        tasks = JediTasks.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values('jeditaskid',
                                                                                                  'status',
                                                                                                  'creationdate',
                                                                                                  'modificationtime')
    taskl = []
    for t in tasks:
        taskl.append(t['jeditaskid'])

    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"
    taskEvents = []
    random.seed()
    transactionKey = random.randrange(1000000)
#    connection.enter_transaction_management()
    new_cur = connection.cursor()
    for id in taskl:
        new_cur.execute("INSERT INTO %s(ID,TRANSACTIONKEY) VALUES (%i,%i)" % (
        tmpTableName, id, transactionKey))  # Backend dependable
#    connection.commit()
    taske = GetEventsForTask.objects.extra(
        where=["JEDITASKID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (tmpTableName, transactionKey)]).values()
    for task in taske:
        taskEvents.append(task)
    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
#    connection.commit()
#    connection.leave_transaction_management()

    nevents = {'neventstot': 0, 'neventsrem': 0}
    for task in taskEvents:
        if 'totev' in task and task['totev'] is not None:
            nevents['neventstot'] += task['totev']
        if 'totevrem' in task and task['totevrem'] is not None:
            nevents['neventsrem'] += task['totevrem']

    del request.session['TFIRST']
    del request.session['TLAST']
    response = render_to_response('taskListSummary.html', {'nevents': nevents}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

@never_cache
def report(request):
    initRequest(request)
    step = 0
    response = None

    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16':
        reportGen = MC16aCPReport.MC16aCPReport()
        response = reportGen.prepareReportJEDI(request)
        return response

    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16C':
        reportGen = MC16aCPReport.MC16aCPReport()
        response = reportGen.prepareReportJEDIMC16c(request)
        return response


    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16A' and 'type' in request.session['requestParams'] and request.session['requestParams']['type'].upper() == 'DCC':
        reportGen = MC16aCPReport.MC16aCPReport()
        resp = reportGen.getDKBEventsSummaryRequestedBreakDownHashTag(request)
        dump = json.dumps(resp, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')


    if 'requestParams' in request.session and 'obstasks' in request.session['requestParams']:
        reportGen = ObsoletedTasksReport.ObsoletedTasksReport()
        response = reportGen.prepareReport(request)
        return response

    if 'requestParams' in request.session and 'titanreport' in request.session['requestParams']:
        reportGen = TitanProgressReport.TitanProgressReport()
        response = reportGen.prepareReport(request)
        return response

    if 'requestParams' in request.session and 'step' in request.session['requestParams']:
        step = int(request.session['requestParams']['step'])
    if step == 0:
        response = render_to_response('reportWizard.html', {'nevents': 0}, content_type='text/html')
    else:
        if 'reporttype' in request.session['requestParams'] and request.session['requestParams']['reporttype'] == 'rep0':
            reportGen = MC16aCPReport.MC16aCPReport()
            response = reportGen.prepareReport()
    return response


def getBrokerageLog(request):
    iquery = {}
    iquery['type'] = 'prod_brokerage'
    iquery['name'] = 'panda.mon.jedi'
    if 'taskid' in request.session['requestParams']:
        iquery['message__startswith'] = request.session['requestParams']['taskid']
    if 'jeditaskid' in request.session['requestParams']:
        iquery['message__icontains'] = "jeditaskid=%s" % request.session['requestParams']['jeditaskid']
    if 'hours' not in request.session['requestParams']:
        hours = 72
    else:
        hours = int(request.session['requestParams']['hours'])
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    iquery['bintime__range'] = [startdate, enddate]
    records = Pandalog.objects.filter(**iquery).order_by('bintime').reverse()[:request.session['JOB_LIMIT']].values()
    sites = {}
    for record in records:
        message = records['message']
        print (message)


def taskprofileplot(request):
    jeditaskid = 0
    if 'jeditaskid' in request.GET: jeditaskid = int(request.GET['jeditaskid'])
    image = None
    if jeditaskid != 0:
        dp = TaskProgressPlot()
        image = dp.get_task_profile(taskid=jeditaskid)
    if image is not None:
        return HttpResponse(image, content_type="image/png")
    else:
        return HttpResponse('')
        # response = HttpResponse(content_type="image/jpeg")
        # red.save(response, "JPEG")
        # return response


def taskESprofileplot(request):
    jeditaskid = 0
    if 'jeditaskid' in request.GET: jeditaskid = int(request.GET['jeditaskid'])
    image = None
    if jeditaskid != 0:
        dp = TaskProgressPlot()
        image = dp.get_es_task_profile(taskid=jeditaskid)
    if image is not None:
        return HttpResponse(image, content_type="image/png")
    else:
        return HttpResponse('')
        # response = HttpResponse(content_type="image/jpeg")
        # red.save(response, "JPEG")
        # return response

@login_customrequired
def taskInfo(request, jeditaskid=0):
    try:
        jeditaskid = int(jeditaskid)
    except:
        jeditaskid = re.findall("\d+", jeditaskid)
        jdtstr =""
        for jdt in jeditaskid:
            jdtstr = jdtstr+str(jdt)
        return redirect('/task/'+jdtstr)
    valid, response = initRequest(request)
    furl = request.get_full_path()
    nomodeurl = removeParam(furl, 'mode')
    nomodeurl = extensibleURL(request, nomodeurl)
    if not valid: return response
    # Here we try to get cached data. We get any cached data is available
    # data = None

    if ('dt' in request.session['requestParams'] and 'transkey' in request.session['requestParams']):
        tk = request.session['requestParams']['transkey']
        datasets = getCacheEntry(request, tk, isData=True)
        if datasets is None:
            request.META['QUERY_STRING'] = ''
            commondata = getCacheEntry(request, "taskInfo", skipCentralRefresh=True)
            if commondata is not None:
                commondata = json.loads(commondata)
                if commondata is not None:
                    datasets = commondata['datasets'] if 'datasets' in commondata else None
                    datasets = json.dumps(datasets)
        if datasets is None:
            _logger.error('No datasets data found for task in cache!!! Request: {}'.format(str(request.get_full_path())))
        return HttpResponse(datasets, content_type='application/json')
    data = getCacheEntry(request, "taskInfo", skipCentralRefresh=True)

    # data = None #temporarily turm off caching

    if data is not None:
        data = json.loads(data)
        if data is not None:

            if 'built' in data and data['built'] is not None:
                builtDate = datetime.strptime('2019-'+data['built'], defaultDatetimeFormat)
                if builtDate < datetime.strptime('2018-02-27 12:00:00', defaultDatetimeFormat):
                    data = None
                    setCacheEntry(request, "taskInfo", json.dumps(data, cls=DateEncoder), 1)

            if 'eventservice' in data and data['eventservice'] is not None:
                if data['eventservice'] == True and (
                    'version' not in request.session['requestParams'] or (
                        'version' in request.session['requestParams'] and request.session['requestParams']['version'] != 'old' )):
                    return redirect('/tasknew/'+str(jeditaskid))

            try:
                data = deleteCacheTestData(request, data)
            except: pass
            doRefresh = False

        plotDict = {}
        if 'plotsDict' in data:
            oldPlotDict = data['plotsDict']
            if isinstance(oldPlotDict, dict):
                for plotName, plotData in oldPlotDict.items():
                    if 'sites' in plotData and 'ranges' in plotData:
                        plotDict[str(plotName)] = {'sites': {}, 'ranges': plotData['ranges'], 'stats': plotData['stats']}
                        for dictSiteName, listValues in plotData['sites'].items():
                            try:
                                plotDict[str(plotName)]['sites'][str(dictSiteName)] = []
                                plotDict[str(plotName)]['sites'][str(dictSiteName)] += listValues
                            except:
                                pass
                data['plotsDict'] = plotDict

            #We still want to refresh tasks if request came from central crawler and task not in the frozen state
            if (('REMOTE_ADDR' in request.META) and (request.META['REMOTE_ADDR'] in notcachedRemoteAddress) and
                    data['task'] and data['task']['status'] not in ['broken', 'aborted']):
                doRefresh = True

            # we check here whether task status didn't changed for both (user or crawler request)
            if data['task'] and data['task']['status'] and data['task']['status'] in ['done', 'finished', 'failed']:
                if 'jeditaskid' in request.session['requestParams']: jeditaskid = int(
                    request.session['requestParams']['jeditaskid'])
                if jeditaskid != 0:
                    query = {'jeditaskid': jeditaskid}
                    values = ['status','superstatus','modificationtime']
                    tasks = JediTasks.objects.filter(**query).values(*values)[:1]
                    if len(tasks) > 0:
                        task = tasks[0]
                        if (task['status'] == data['task']['status'] and task['superstatus'] == data['task']['superstatus'] and
                                    task['modificationtime'].strftime(defaultDatetimeFormat) == data['task']['modificationtime']):
                            doRefresh = False
                        else:
                            doRefresh = True
                    else:
                        doRefresh = True
            # doRefresh = True

            ### This is a temporary fix in order of avoiding 500 error for cached tasks not compartible to a new template
            if not isinstance(data['jobscoutids']['ramcountscoutjob'], list):
                if 'ramcountscoutjob' in data['jobscoutids']: del data['jobscoutids']['ramcountscoutjob']
                if 'iointensityscoutjob' in data['jobscoutids']: del data['jobscoutids']['iointensityscoutjob']
                if 'outdiskcountscoutjob' in data['jobscoutids']: del data['jobscoutids']['outdiskcountscoutjob']

            if not doRefresh:
                data['request'] = request
                if data['eventservice'] == True:
                    response = render_to_response('taskInfoES.html', data, content_type='text/html')
                else:
                    response = render_to_response('taskInfo.html', data, content_type='text/html')
                patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
                return response

    if 'taskname' in request.session['requestParams'] and request.session['requestParams']['taskname'].find('*') >= 0:
        return taskList(request)
    setupView(request, hours=365 * 24, limit=999999999, querytype='task')
    eventservice = False
    query = {}
    tasks = []
    taskrec = None
    colnames = []
    columns = []
    jobsummary = []
    maxpss = []
    walltime = []
    jobsummaryESMerge = []
    jobsummaryPMERGE = []
    jobsummaryBuild = []
    eventsdict=[]
    objectStoreDict=[]
    eventsChains = []
    currentlyRunningDataSets = []
    warning = {}

    newjobsummary =[]
    newjobsummaryESMerge = []
    newjobsummaryPMERGE = []
    neweventsdict =[]

    if 'jeditaskid' in request.session['requestParams']: jeditaskid = int(
        request.session['requestParams']['jeditaskid'])
    if jeditaskid == 0:
        return redirect('/tasks')
    if jeditaskid != 0:

        query = {'jeditaskid': jeditaskid}
        tasks = JediTasks.objects.filter(**query).values()
        if len(tasks) > 0:
            if 'eventservice' in tasks[0] and tasks[0]['eventservice'] == 1: eventservice = True

        if eventservice:
            if 'version' not in request.session['requestParams'] or (
                    'version' in request.session['requestParams'] and request.session['requestParams']['version'] != 'old'):
                return redirect('/tasknew/' + str(jeditaskid))

            mode = 'drop'
            if 'mode' in request.session['requestParams'] and request.session['requestParams'][
                'mode'] == 'drop': mode = 'drop'
            if 'mode' in request.session['requestParams'] and request.session['requestParams'][
                'mode'] == 'nodrop': mode = 'nodrop'

            extra = jobSuppression(request)

            auxiliaryDict = {}

            plotsDict, jobsummary, eventssummary, transactionKey, jobScoutIDs, hs06sSum = jobSummary2(
                request, query, exclude={}, mode=mode, isEventServiceFlag=True, substatusfilter='non_es_merge', algorithm='isOld')
            plotsDictESMerge, jobsummaryESMerge, eventssummaryESM, transactionKeyESM, jobScoutIDsESM, hs06sSumESM = jobSummary2(
                request, query, exclude={}, mode=mode, isEventServiceFlag=True, substatusfilter='es_merge', algorithm='isOld')
            if request.user.is_authenticated and request.user.is_tester:
                tk, droppedList, extra = dropalgorithm.dropRetrielsJobs(jeditaskid, extra, eventservice)
                newplotsDict, newjobsummary, neweventssummary, newtransactionKey, newjobScoutIDs, newhs06sSum = jobSummary2(
                    request, query, exclude={}, extra=extra, mode=mode, isEventServiceFlag=True, substatusfilter='non_es_merge', algorithm='isNew')
                newplotsDictESMerge, newjobsummaryESMerge, neweventssummaryESM, newtransactionKeyESM, newjobScoutIDsESM, newhs06sSumESM = jobSummary2(
                    request, query, exclude={}, extra=extra, mode=mode, isEventServiceFlag=True, substatusfilter='es_merge', algorithm='isNew')
                for state in eventservicestatelist:
                    eventstatus = {}
                    eventstatus['statusname'] = state
                    eventstatus['count'] = neweventssummary[state]
                    neweventsdict.append(eventstatus)

            for state in eventservicestatelist:
                eventstatus = {}
                eventstatus['statusname'] = state
                eventstatus['count'] = eventssummary[state]
                eventsdict.append(eventstatus)

            if mode=='nodrop':
                sqlRequest = """
                WITH jedi_ev AS 
                (
                SELECT /*+ INDEX_RS_ASC(e JEDI_EVENTS_PK) NO_INDEX_FFS(e JEDI_EVENTS_PK) */ 
                pandaid, e.objstore_id, e.status, count(*) as nevents 
                FROM atlas_panda.jedi_events e 
                WHERE jeditaskid = {}
                GROUP BY pandaid, e.objstore_id, e.status 
                )
                SELECT 
                jobs.computingsite, jobs.COMPUTINGELEMENT, jedi_ev.objstore_id, jedi_ev.status, SUM(jedi_ev.nevents) 
                FROM jedi_ev, 
                (
                select computingsite, computingelement,pandaid
                from ATLAS_PANDA.JOBSARCHIVED4
                where jeditaskid={} 
                UNION
                select computingsite, computingelement,pandaid
                from ATLAS_PANDAARCH.JOBSARCHIVED
                where jeditaskid={} 
                ) jobs
                WHERE 
                jobs.pandaid = jedi_ev.pandaid 
                GROUP BY 
                jobs.computingsite, jobs.COMPUTINGELEMENT, jedi_ev.objstore_id, jedi_ev.status
                """.format(jeditaskid, jeditaskid, jeditaskid)
                cur = connection.cursor()
                cur.execute(sqlRequest)
                ossummary = cur.fetchall()
                cur.close()

                ossummarynames = ['computingsite', 'computingelement', 'objectstoreid', 'statusindex', 'nevents']
                objectStoreDict = [dict(zip(ossummarynames, row)) for row in ossummary]
                for row in objectStoreDict: row['statusname'] = eventservicestatelist[row['statusindex']]

        elif len(tasks) > 0 and 'tasktype' in tasks[0] and tasks[0]['tasktype']  == 'anal':
            # Divide jobs into 3 categories: run, build, merge
            extra = '(1=1)'
            jbquery = copy.deepcopy(query)
            jbquery['transformation__icontains'] = 'build'
            exclude = {'processingtype': 'pmerge'}
            jextra = "transformation NOT LIKE \'%%build%%\'"
            mode = 'drop'
            if 'mode' in request.session['requestParams']:
                mode = request.session['requestParams']['mode']
            plotsDict, jobsummary, eventssummary, transactionKey, jobScoutIDs, hs06sSum = jobSummary2(
                request, query, exclude=exclude, extra=jextra, mode=mode,algorithm='isOld')
            plotsDictBuild, jobsummaryBuild, eventssummaryBuild, transactionKeyBuild, jobScoutIDsBuild, hs06sSumBuild = jobSummary2(
                request, jbquery, exclude={}, extra=extra,  mode=mode, algorithm='isOld')
            plotsDictPMERGE, jobsummaryPMERGE, eventssummaryPM, transactionKeyPM, jobScoutIDsPMERGE, hs06sSumPMERGE = jobSummary2(
                request, query, exclude={},extra=extra,  mode=mode, processingtype='pmerge',algorithm='isOld')
        else:
            extra = '(1=1)'
            ## Exclude merge jobs. Can be misleading. Can show failures with no downstream successes.
            exclude = {'processingtype': 'pmerge'}
            mode = 'drop'
            if 'mode' in request.session['requestParams']:
                mode = request.session['requestParams']['mode']
            plotsDict, jobsummary, eventssummary, transactionKey, jobScoutIDs, hs06sSum = jobSummary2(
                request, query, exclude=exclude,extra=extra, mode=mode,algorithm='isOld')
            plotsDictPMERGE, jobsummaryPMERGE, eventssummaryPM, transactionKeyPM, jobScoutIDsPMERGE, hs06sSumPMERGE = jobSummary2(
                request, query, exclude={},extra=extra,  mode=mode, processingtype='pmerge',algorithm='isOld')
            if request.user.is_authenticated and request.user.is_tester:
                if mode == 'drop':
                    tk, droppedList, extra = dropalgorithm.dropRetrielsJobs(jeditaskid, extra=None, isEventTask=False)
                newplotsDict, newjobsummary, neweventssummary, newtransactionKey, newjobScoutIDs, newhs06sSum = jobSummary2(
                    request, query, exclude=exclude,extra=extra , mode=mode, algorithm='isNew')
                newplotsDictPMERGE, newjobsummaryPMERGE, neweventssummaryPM, newtransactionKeyPM, newjobScoutIDsPMERGE, newhs06sSumPMERGE = jobSummary2(request, query=query, exclude={},extra=extra , mode=mode, processingtype='pmerge', algorithm='isNew')


    elif 'taskname' in request.session['requestParams']:
        querybyname = {'taskname': request.session['requestParams']['taskname']}
        tasks = JediTasks.objects.filter(**querybyname).values()
        if len(tasks) > 0:
            jeditaskid = tasks[0]['jeditaskid']
        query = {'jeditaskid': jeditaskid}

    nonzeroPMERGE = 0
    for status in jobsummaryPMERGE:
        if status['count'] > 0:
            nonzeroPMERGE += 1
            break

    if nonzeroPMERGE == 0:
        jobsummaryPMERGE = None

    maxpssave = 0
    maxpsscount = 0
    for maxpssjob in maxpss:
        if maxpssjob > 0:
            maxpssave += maxpssjob
            maxpsscount += 1
    if maxpsscount > 0:
        maxpssave = maxpssave / maxpsscount
    else:
        maxpssave = ''

    tasks = cleanTaskList(request, tasks)
    try:
        taskrec = tasks[0]
        colnames = taskrec.keys()
        colnames = sorted(colnames)
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = taskrec[k].strftime(defaultDatetimeFormat)
                except:
                    val = taskrec[k]
            else:
                val = taskrec[k]
            if taskrec[k] == None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
    except IndexError:
        taskrec = None

    taskpars = JediTaskparams.objects.filter(**query).values()[:1000]
    jobparams = None
    taskparams = None
    taskparaml = None
    jobparamstxt = []
    if len(taskpars) > 0:
        taskparams = taskpars[0]['taskparams']
        try:
            taskparams = json.loads(taskparams)
            tpkeys = taskparams.keys()
            tpkeys = sorted(tpkeys)
            taskparaml = []
            for k in tpkeys:
                rec = {'name': k, 'value': taskparams[k]}
                taskparaml.append(rec)
            jobparams = taskparams['jobParameters']
            if 'log' in taskparams:
                jobparams.append(taskparams['log'])
            for p in jobparams:
                if p['type'] == 'constant':
                    ptxt = p['value']
                elif p['type'] == 'template':
                    ptxt = "<i>%s template:</i> value='%s' " % (p['param_type'], p['value'])
                    for v in p:
                        if v in ['type', 'param_type', 'value']: continue
                        ptxt += "  %s='%s'" % (v, p[v])
                else:
                    ptxt = '<i>unknown parameter type %s:</i> ' % p['type']
                    for v in p:
                        if v in ['type', ]: continue
                        ptxt += "  %s='%s'" % (v, p[v])
                jobparamstxt.append(ptxt)
            jobparamstxt = sorted(jobparamstxt, key=lambda x: x.lower())

        except ValueError:
            pass

    if taskrec and 'ticketsystemtype' in taskrec and taskrec['ticketsystemtype'] == '' and taskparams != None:
        if 'ticketID' in taskparams: taskrec['ticketid'] = taskparams['ticketID']
        if 'ticketSystemType' in taskparams: taskrec['ticketsystemtype'] = taskparams['ticketSystemType']

    if taskrec:
        taskname = taskrec['taskname']
    elif 'taskname' in request.session['requestParams']:
        taskname = request.session['requestParams']['taskname']
    else:
        taskname = ''

    logtxt = None
    if taskrec and taskrec['errordialog']:
        mat = re.match('^.*"([^"]+)"', taskrec['errordialog'])
        if mat:
            errurl = mat.group(1)
            cmd = "curl -s -f --compressed '%s'" % errurl
            logpfx = u"logtxt: %s\n" % cmd
            logout = subprocess.getoutput(cmd)
            if len(logout) > 0:
                loglist = (logout.splitlines())[::-1]
                logtxt = '\n'.join(loglist)

    dsquery = {}
    dsquery['jeditaskid'] = jeditaskid

    dsets = JediDatasets.objects.filter(**dsquery).values()
    dsinfo = None
    nfiles = 0
    nfinished = 0
    nfailed = 0
    neventsTot = 0
    neventsUsedTot = 0
    scope = ''
    newdslist = []
    if len(dsets) > 0:
        for ds in dsets:
            if len (ds['datasetname']) > 0:
                if not str(ds['datasetname']).startswith('user'):
                    scope = str(ds['datasetname']).split('.')[0]
                else:
                    scope = '.'.join(str(ds['datasetname']).split('.')[:2])
                if ':' in scope:
                    scope = str(scope).split(':')[0]
                ds['scope'] = scope
            newdslist.append(ds)
            if ds['type'] not in ['input', 'pseudo_input']: continue
            if ds['masterid']: continue
            if not ds['nevents'] is None and int(ds['nevents']) > 0:
                neventsTot += int(ds['nevents'])
            if not ds['neventsused'] is None and int(ds['neventsused']) > 0:
                neventsUsedTot += int(ds['neventsused'])

            if int(ds['nfiles']) > 0:
                ds['percentfinished'] = int(100. * int(ds['nfilesfinished']) / int(ds['nfiles']))
                nfiles += int(ds['nfiles'])
                nfinished += int(ds['nfilesfinished'])
                nfailed += int(ds['nfilesfailed'])

        dsets = newdslist
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())
        if nfiles > 0:
            dsinfo = {}
            dsinfo['nfiles'] = nfiles
            dsinfo['nfilesfinished'] = nfinished
            dsinfo['nfilesfailed'] = nfailed
            dsinfo['pctfinished'] = int(100. * nfinished / nfiles)
            dsinfo['pctfailed'] = int(100. * nfailed / nfiles)
    else: ds = []
    if taskrec: taskrec['dsinfo'] = dsinfo

    ## get dataset types
    dstypesd = {}
    for ds in dsets:
        dstype = ds['type']
        if dstype not in dstypesd: dstypesd[dstype] = 0
        dstypesd[dstype] += 1
    dstkeys = dstypesd.keys()
    dstkeys = sorted(dstkeys)
    dstypes = []
    for dst in dstkeys:
        dstd = {'type': dst, 'count': dstypesd[dst]}
        dstypes.append(dstd)

    ## get input containers
    inctrs = []
    if taskparams and 'dsForIN' in taskparams:
        inctrs = taskparams['dsForIN'].split(',')

    ## get output containers
    cquery = {}
    cquery['jeditaskid'] = jeditaskid
    cquery['type__in'] = ('output', 'log')
    outctrs = []
    outctrs.extend(JediDatasets.objects.filter(**cquery).values_list('containername', flat=True).distinct())
    if len(outctrs) == 0 or outctrs[0] == '':
        outctrs = None
    if isinstance(outctrs, str):
       outctrs = [outctrs]

    # getBrokerageLog(request)

    # neventsTot = 0
    # neventsUsedTot = 0

    if taskrec:
        taskrec['totev'] = neventsTot
        taskrec['totevproc'] = neventsUsedTot
        taskrec['pctfinished'] = (100 * taskrec['totevproc'] / taskrec['totev']) if (taskrec['totev'] > 0) else ''
        taskrec['totevhs06'] = (neventsTot) * taskrec['cputime'] if (
        taskrec['cputime'] is not None and neventsTot > 0) else None
        # if taskrec['pctfinished']<=20 or hs06sSum['total']==0:
        #     taskrec['totevhs06'] = (neventsTot)*taskrec['cputime'] if (taskrec['cputime'] is not None and neventsTot > 0) else None
        # else:
        #     taskrec['totevhs06'] = int(hs06sSum['total']*neventsTot)
        taskrec['totevprochs06'] = int(hs06sSum['finished'])
        taskrec['failedevprochs06'] = int(hs06sSum['failed'])
        taskrec['currenttotevhs06'] = int(hs06sSum['total'])

        taskrec['maxpssave'] = maxpssave
        if 'creationdate' in taskrec:
            taskrec['kibanatimefrom'] = taskrec['creationdate'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimefrom'] = None
        if taskrec['status'] in ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']:
            taskrec['kibanatimeto'] = taskrec['modificationtime'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimeto'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    tquery = {}
    tquery['jeditaskid'] = jeditaskid
    tquery['storagetoken__isnull'] = False
    storagetoken = JediDatasets.objects.filter(**tquery).values('storagetoken')

    taskbrokerage = 'prod_brokerage' if (taskrec != None and taskrec['tasktype'] == 'prod') else 'analy_brokerage'

    if storagetoken:
        if taskrec:
            taskrec['destination'] = storagetoken[0]['storagetoken']

    if (taskrec != None and taskrec['cloud'] == 'WORLD'):
        taskrec['destination'] = taskrec['nucleus']

    showtaskprof = False
    countfailed = [val['count'] for val in jobsummary if val['name'] == 'finished']
    if len(countfailed) > 0 and countfailed[0] > 0:
        showtaskprof = True


    if taskrec:

        if 'tasktype' in taskrec and taskrec['tasktype'] == 'anal':
            tmcj_list = get_top_memory_consumers(taskrec)
            if len(tmcj_list) > 0 and len([True for job in tmcj_list if job['maxrssratio'] >= 1]) > 0:
                warning['memoryleaksuspicion'] = {}
                warning['memoryleaksuspicion'][
                    'message'] = 'Some jobs in this task consumed a lot of memory. We suspect there might be memory leaks.'
                warning['memoryleaksuspicion']['jobs'] = tmcj_list

        if taskrec['creationdate']:
            if taskrec['creationdate'] < datetime.strptime('2018-02-07', '%Y-%m-%d'):
                warning['dropmode'] = 'The drop mode is unavailable since the data of job retries was cleaned up. The data shown on the page is in nodrop mode.'
            taskrec['creationdate'] = taskrec['creationdate'].strftime(defaultDatetimeFormat)
        if taskrec['modificationtime']:
            taskrec['modificationtime'] = taskrec['modificationtime'].strftime(defaultDatetimeFormat)
        if taskrec['starttime']:
            taskrec['starttime'] = taskrec['starttime'].strftime(defaultDatetimeFormat)
        if taskrec['statechangetime']:
            taskrec['statechangetime'] = taskrec['statechangetime'].strftime(defaultDatetimeFormat)
        if taskrec['ttcrequested']:
            taskrec['ttcrequested'] = taskrec['ttcrequested'].strftime(defaultDatetimeFormat)

    for dset in dsets:
        dset['creationtime'] = dset['creationtime'].strftime(defaultDatetimeFormat)
        dset['modificationtime'] = dset['modificationtime'].strftime(defaultDatetimeFormat)
        if dset['statechecktime'] is not None:
            dset['statechecktime'] = dset['statechecktime'].strftime(defaultDatetimeFormat)

    ### Putting list of datasets to cache separately for dataTables plugin
    transKey = random.randrange(100000000)
    setCacheEntry(request, transKey, json.dumps(dsets, cls=DateEncoder), 60 * 30, isData=True)

    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        del tasks
        del columns
        del ds
        if taskrec:
            taskrec['creationdate'] = taskrec['creationdate']
            taskrec['modificationtime'] = taskrec['modificationtime']
            taskrec['starttime'] = taskrec['starttime']
            taskrec['statechangetime'] = taskrec['statechangetime']

        data = {
            'task': taskrec,
            'taskparams': taskparams,
            'datasets': dsets,
        }

        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        attrs = []
        do_redirect = False
        try:
            if int(jeditaskid) > 0 and int(jeditaskid) < 4000000:
                do_redirect = True
        except:
            pass
        if taskrec:
            attrs.append({'name': 'Status', 'value': taskrec['status']})
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'furl': furl,
            'nomodeurl': nomodeurl,
            'mode': mode,
            'showtaskprof': showtaskprof,
            'jobsummaryESMerge': jobsummaryESMerge,
            'jobsummaryPMERGE': jobsummaryPMERGE,
            'jobsummaryBuild': jobsummaryBuild,
            'plotsDict': plotsDict,
            'taskbrokerage': taskbrokerage,
            'jobscoutids' : jobScoutIDs,
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'task': taskrec,
            'taskname': taskname,
            'taskparams': taskparams,
            'taskparaml': taskparaml,
            'jobparams': jobparamstxt,
            'columns': columns,
            'attrs': attrs,
            'jobsummary': jobsummary,
            'eventssummary': eventsdict,
            'ossummary': objectStoreDict,
            'jeditaskid': jeditaskid,
            'logtxt': logtxt,
            'datasets': dsets,
            'dstypes': dstypes,
            'inctrs': inctrs,
            'outctrs': outctrs,
            'vomode': VOMODE,
            'eventservice': eventservice,
            'tk': transactionKey,
            'transkey': transKey,
            'built': datetime.now().strftime("%m-%d %H:%M:%S"),
            'newjobsummary_test': newjobsummary,
            'newjobsummaryPMERGE_test':newjobsummaryPMERGE,
            'newjobsummaryESMerge_test': newjobsummaryESMerge,
            'neweventssummary_test': neweventsdict,
            'warning': warning,
        }
        data.update(getContextVariables(request))
        cacheexpiration = 60*20 #second/minute * minutes
        if taskrec and 'status' in taskrec:
            totaljobs = 0
            for state in jobsummary:
                totaljobs += state['count']
            if taskrec['status'] in ['broken','aborted','done','finished','failed'] and totaljobs > 5000:
                cacheexpiration = 3600*24*31 # we store such data a month
        setCacheEntry(request, "taskInfo", json.dumps(data, cls=DateEncoder), cacheexpiration)

        if eventservice:
            response = render_to_response('taskInfoES.html', data, content_type='text/html')
        else:
            response = render_to_response('taskInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


def taskInfoNew(request, jeditaskid=0):
    try:
        jeditaskid = int(jeditaskid)
    except:
        jeditaskid = re.findall("\d+", jeditaskid)
        jdtstr =""
        for jdt in jeditaskid:
            jdtstr = jdtstr+str(jdt)
        return redirect('/task/'+jdtstr)
    valid, response = initRequest(request)
    furl = request.get_full_path()
    nomodeurl = removeParam(furl, 'mode')
    nomodeurl = extensibleURL(request, nomodeurl)
    if not valid: return response
    # Here we try to get cached data. We get any cached data is available

    # return json for dataTables if dt in request params
    if 'dt' in request.session['requestParams'] and 'tkiec' in request.session['requestParams']:
        tkiec = request.session['requestParams']['tkiec']
        data = getCacheEntry(request, tkiec, isData=True)
        return HttpResponse(data, content_type='application/json')


    data = getCacheEntry(request, "taskInfoNew", skipCentralRefresh=True)
    # data = None
    # Temporary protection
    if data is not None:
        data = json.loads(data)

        if 'built' in data:
            builtDate = datetime.strptime('2018-'+data['built'], defaultDatetimeFormat)
            if builtDate < datetime.strptime('2018-02-27 12:00:00', defaultDatetimeFormat):
                data = None
                setCacheEntry(request, "taskInfoNew", json.dumps(data, cls=DateEncoder), 1)

    if data is not None:
        doRefresh = False

        #We still want to refresh tasks if request came from central crawler and task not in the frozen state
        if (('REMOTE_ADDR' in request.META) and (request.META['REMOTE_ADDR'] in notcachedRemoteAddress) and
                data['task'] and data['task']['status'] not in ['broken', 'aborted']):
            doRefresh = True

        # we check here whether task status didn't changed for both (user or crawler request)
        if data['task'] and data['task']['status'] and data['task']['status'] in ['done', 'finished', 'failed']:
            if 'jeditaskid' in request.session['requestParams']: jeditaskid = int(
                request.session['requestParams']['jeditaskid'])
            if jeditaskid != 0:
                query = {'jeditaskid': jeditaskid}
                values = ['status','superstatus','modificationtime']
                tasks = JediTasks.objects.filter(**query).values(*values)[:1]
                if len(tasks) > 0:
                    task = tasks[0]
                    if (task['status'] == data['task']['status'] and task['superstatus'] == data['task']['superstatus'] and
                                task['modificationtime'].strftime(defaultDatetimeFormat) == data['task']['modificationtime']):
                        doRefresh = False
                    else:
                        doRefresh = True
                else:
                    doRefresh = True
        # temp turning on refresh of all tasks to rewrite cache
        # doRefresh = True

        if not doRefresh:
            data['request'] = request
            if data['eventservice'] == True:
                response = render_to_response('taskInfoESNew.html', data, content_type='text/html')
            else:
                response = render_to_response('taskInfo.html', data, content_type='text/html')
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response

    if 'taskname' in request.session['requestParams'] and request.session['requestParams']['taskname'].find('*') >= 0:
        return taskList(request)
    setupView(request, hours=365 * 24, limit=999999999, querytype='task')
    eventservice = False
    query = {}
    tasks = []
    taskrec = None
    colnames = []
    columns = []
    jobsummary = []
    maxpss = []
    walltime = []
    jobsummaryMerge = []
    jobsummaryESMerge = []
    jobsummaryPMERGE = []
    eventsdict = []
    objectStoreDict=[]
    eventsChains = []
    currentlyRunningDataSets = []
    warning = {}
    neventsProcTot = 0


    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == 0:
        return redirect('/tasks')
    if jeditaskid != 0:

        query = {'jeditaskid': jeditaskid}
        tasks = JediTasks.objects.filter(**query).values()
        if len(tasks) > 0:
            if 'eventservice' in tasks[0] and tasks[0]['eventservice'] == 1: eventservice = True

        if not eventservice:
            return redirect('/task/' + str(jeditaskid))

        mode = 'nodrop'
        if 'mode' in request.session['requestParams'] and request.session['requestParams'][
            'mode'] == 'drop': mode = 'drop'
        if 'mode' in request.session['requestParams'] and request.session['requestParams'][
            'mode'] == 'nodrop': mode = 'nodrop'
        extra = '(1=1)'
        transactionKeyDJ = -1
        if mode == 'drop':
            start = time.time()
            extra, transactionKeyDJ = insert_dropped_jobs_to_tmp_table(query, extra)
            end = time.time()
            print("Inserting dropped jobs: {} sec".format(end - start))
            print('tk of dropped jobs: {}'.format(transactionKeyDJ))
        #
        # plotsDict, jobsummary, jobsummaryMerge, jobScoutIDs, hs06sSum = jobSummary3(
        #     request, query, extra=extra, isEventServiceFlag=eventservice)

        if eventservice:
            start = time.time()
            eventsdict = event_summary_for_task(mode, query, transactionKeyDJ)
            end = time.time()
            print("Events states summary: {} sec".format(end - start))
            for entry in eventsdict:
                status = entry.get("statusname", "-")
                if status in ['finished','done','merged']:
                    neventsProcTot += entry.get("count", 0)


    start = time.time()

    maxpssave = 0
    maxpsscount = 0
    for maxpssjob in maxpss:
        if maxpssjob > 0:
            maxpssave += maxpssjob
            maxpsscount += 1
    if maxpsscount > 0:
        maxpssave = maxpssave / maxpsscount
    else:
        maxpssave = ''

    tasks = cleanTaskList(request, tasks)
    try:
        taskrec = tasks[0]
        colnames = taskrec.keys()
        colnames = sorted(colnames)
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = taskrec[k].strftime(defaultDatetimeFormat)
                except:
                    val = taskrec[k]
            else:
                val = taskrec[k]
            if taskrec[k] == None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
    except IndexError:
        taskrec = None

    taskpars = JediTaskparams.objects.filter(**query).values()[:1000]
    jobparams = None
    taskparams = None
    taskparaml = None
    jobparamstxt = []
    if len(taskpars) > 0:
        taskparams = taskpars[0]['taskparams']
        try:
            taskparams = json.loads(taskparams)
            tpkeys = taskparams.keys()
            tpkeys = sorted(tpkeys)
            taskparaml = []
            for k in tpkeys:
                rec = {'name': k, 'value': taskparams[k]}
                taskparaml.append(rec)
            jobparams = taskparams['jobParameters']
            if 'log' in taskparams:
                jobparams.append(taskparams['log'])
            for p in jobparams:
                if p['type'] == 'constant':
                    ptxt = p['value']
                elif p['type'] == 'template':
                    ptxt = "<i>%s template:</i> value='%s' " % (p['param_type'], p['value'])
                    for v in p:
                        if v in ['type', 'param_type', 'value']: continue
                        ptxt += "  %s='%s'" % (v, p[v])
                else:
                    ptxt = '<i>unknown parameter type %s:</i> ' % p['type']
                    for v in p:
                        if v in ['type', ]: continue
                        ptxt += "  %s='%s'" % (v, p[v])
                jobparamstxt.append(ptxt)
            jobparamstxt = sorted(jobparamstxt, key=lambda x: x.lower())

        except ValueError:
            pass

    if taskrec and 'ticketsystemtype' in taskrec and taskrec['ticketsystemtype'] == '' and taskparams != None:
        if 'ticketID' in taskparams: taskrec['ticketid'] = taskparams['ticketID']
        if 'ticketSystemType' in taskparams: taskrec['ticketsystemtype'] = taskparams['ticketSystemType']

    if taskrec:
        taskname = taskrec['taskname']
    elif 'taskname' in request.session['requestParams']:
        taskname = request.session['requestParams']['taskname']
    else:
        taskname = ''

    logtxt = None
    if taskrec and taskrec['errordialog']:
        mat = re.match('^.*"([^"]+)"', taskrec['errordialog'])
        if mat:
            errurl = mat.group(1)
            cmd = "curl -s -f --compressed '%s'" % errurl
            logpfx = u"logtxt: %s\n" % cmd
            logout = subprocess.getoutput(cmd)
            if len(logout) > 0: logtxt = logout

    dsquery = {}
    dsquery['jeditaskid'] = jeditaskid

    dsets = JediDatasets.objects.filter(**dsquery).values()
    dsinfo = None
    nfiles = 0
    nfinished = 0
    nfailed = 0
    neventsTot = 0
    neventsUsedTot = 0

    scope = ''
    newdslist = []
    if len(dsets) > 0:
        for ds in dsets:
            if len (ds['datasetname']) > 0:
               scope = str(ds['datasetname']).split('.')[0]
               if ':' in scope:
                   scope = str(scope).split(':')[0]
               ds['scope']=scope
            newdslist.append(ds)
            if ds['type'] not in ['input', 'pseudo_input']: continue
            if ds['masterid']: continue
            if not ds['nevents'] is None and int(ds['nevents']) > 0:
                neventsTot += int(ds['nevents'])
                neventsUsedTot += int(ds['neventsused'])

            if int(ds['nfiles']) > 0:
                nfiles += int(ds['nfiles'])
                nfinished += int(ds['nfilesfinished'])
                nfailed += int(ds['nfilesfailed'])
        dsets = newdslist
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())
        if nfiles > 0:
            dsinfo = {}
            dsinfo['nfiles'] = nfiles
            dsinfo['nfilesfinished'] = nfinished
            dsinfo['nfilesfailed'] = nfailed
            dsinfo['pctfinished'] = int(100. * nfinished / nfiles)
            dsinfo['pctfailed'] = int(100. * nfailed / nfiles)
    else: ds = []
    if taskrec: taskrec['dsinfo'] = dsinfo

    ## get dataset types
    dstypesd = {}
    for ds in dsets:
        dstype = ds['type']
        if dstype not in dstypesd: dstypesd[dstype] = 0
        dstypesd[dstype] += 1
    dstkeys = dstypesd.keys()
    dstkeys = sorted(dstkeys)
    dstypes = []
    for dst in dstkeys:
        dstd = {'type': dst, 'count': dstypesd[dst]}
        dstypes.append(dstd)

    ## get input containers
    inctrs = []
    if taskparams and 'dsForIN' in taskparams:
        inctrs = [taskparams['dsForIN'], ]

    ## get output containers
    cquery = {}
    cquery['jeditaskid'] = jeditaskid
    cquery['type__in'] = ('output', 'log')
    outctrs = []
    outctrs.extend(JediDatasets.objects.filter(**cquery).values_list('containername', flat=True).distinct())
    if len(outctrs) == 0 or outctrs[0] == '':
        outctrs = None
    if isinstance(outctrs, str):
       outctrs = [outctrs]

    # getBrokerageLog(request)

    # neventsTot = 0
    # neventsUsedTot = 0

    if taskrec:
        taskrec['totev'] = neventsTot
        taskrec['totevproc'] = neventsUsedTot
        taskrec['totevproc_evst'] = neventsProcTot
        taskrec['pcttotevproc_evst'] = (100 * neventsProcTot / neventsTot) if (taskrec['totev'] > 0) else ''
        taskrec['pctfinished'] = (100 * taskrec['totevproc'] / taskrec['totev']) if (taskrec['totev'] > 0) else ''
        taskrec['totevhs06'] = (neventsTot) * taskrec['cputime'] if (
        taskrec['cputime'] is not None and neventsTot > 0) else None
        taskrec['maxpssave'] = maxpssave
        if 'creationdate' in taskrec:
            taskrec['kibanatimefrom'] = taskrec['creationdate'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimefrom'] = None
        if taskrec['status'] in ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']:
            taskrec['kibanatimeto'] = taskrec['modificationtime'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimeto'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    tquery = {}
    tquery['jeditaskid'] = jeditaskid
    tquery['storagetoken__isnull'] = False
    storagetoken = JediDatasets.objects.filter(**tquery).values('storagetoken')

    taskbrokerage = 'prod_brokerage' if (taskrec != None and taskrec['tasktype'] == 'prod') else 'analy_brokerage'

    if storagetoken:
        if taskrec:
            taskrec['destination'] = storagetoken[0]['storagetoken']

    if (taskrec != None and taskrec['cloud'] == 'WORLD'):
        taskrec['destination'] = taskrec['nucleus']

    showtaskprof = False
    countfailed = [val['count'] for val in jobsummary if val['name'] == 'finished']
    if len(countfailed) > 0 and countfailed[0] > 0:
        showtaskprof = True

    end = time.time()
    print("Loading task params and datasets info: {} sec".format(end - start))

    transactionKeyIEC = -1
    ifs_summary = []
    if eventservice:
        start = time.time()
        # getting inputs states summary
        inputfiles_list, ifs_summary, ifs_tk = input_summary_for_task(taskrec, dsets)

        # Putting list of inputs IDs to tmp table for connection with jobList
        for tk, ids_list in ifs_tk.items():
            tk = insert_to_temp_table(ids_list, tk)


        # Putting list of inputs to cache separately for dataTables plugin
        transactionKeyIEC = random.randrange(100000000)
        setCacheEntry(request, transactionKeyIEC, json.dumps(inputfiles_list, cls=DateEncoder), 60 * 30, isData=True)

        end = time.time()
        print("Inputs states summary: {} sec".format(end - start))

    # get sum of hs06sec grouped by status
    hquery = {}
    hquery['jeditaskid'] = jeditaskid
    hquery['jobstatus__in'] = ('finished', 'failed')
    hs06sec_sum = []
    hs06sec_sum.extend(Jobsarchived.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))
    hs06sec_sum.extend(Jobsarchived4.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))

    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}
    if len(hs06sec_sum) > 0:
        for hs in hs06sec_sum:
            if hs['jobstatus'] == 'finished':
                hs06sSum['finished'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
            elif hs['jobstatus'] == 'failed':
                hs06sSum['failed'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0

    # get corecount and normalized corecount values
    ccquery = {}
    ccquery['jeditaskid'] = jeditaskid
    ccquery['jobstatus'] = 'running'
    accsum = Jobsactive4.objects.filter(**ccquery).aggregate(accsum=Sum('actualcorecount'))
    naccsum = Jobsactive4.objects.filter(**ccquery).aggregate(naccsum=Sum(F('actualcorecount')*F('hs06')/F('corecount')/Value(10), output_field=FloatField()))

    # get lighted job summary
    jobsummarylight, jobsummarylightsplitted = job_summary_for_task_light(taskrec)

    if taskrec:
        taskrec['totevprochs06'] = int(hs06sSum['finished'])
        taskrec['failedevprochs06'] = int(hs06sSum['failed'])
        taskrec['currenttotevhs06'] = int(hs06sSum['total'])
        taskrec['accsum'] = accsum['accsum'] if 'accsum' in accsum else 0
        taskrec['naccsum'] = naccsum['naccsum'] if 'naccsum' in naccsum else 0

    # datetime type -> str in order to avoid encoding cached on template
    datetime_task_param_names = ['creationdate', 'modificationtime', 'starttime', 'statechangetime', 'ttcrequested']
    datetime_dataset_param_names = ['statechecktime', 'creationtime', 'modificationtime']

    if taskrec:
        for dtp in datetime_task_param_names:
            if taskrec[dtp]:
                taskrec[dtp] = taskrec[dtp].strftime(defaultDatetimeFormat)

    for dset in dsets:
        for dtp in datetime_dataset_param_names:
            if dset[dtp]:
                dset[dtp] = dset[dtp].strftime(defaultDatetimeFormat)


    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):

        del tasks
        del columns
        del ds
        if taskrec:
            taskrec['creationdate'] = taskrec['creationdate']
            taskrec['modificationtime'] = taskrec['modificationtime']
            taskrec['starttime'] = taskrec['starttime']
            taskrec['statechangetime'] = taskrec['statechangetime']

        data = {
            'task': taskrec,
            'taskparams': taskparams,
            'datasets': dsets,
        }

        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        attrs = []
        do_redirect = False
        try:
            if int(jeditaskid) > 0 and int(jeditaskid) < 4000000:
                do_redirect = True
        except:
            pass
        if taskrec:
            attrs.append({'name': 'Status', 'value': taskrec['status']})
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'furl': furl,
            'nomodeurl': nomodeurl,
            'mode': mode,
            'showtaskprof': showtaskprof,
            'taskbrokerage': taskbrokerage,
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'task': taskrec,
            'taskname': taskname,
            'taskparams': taskparams,
            'taskparaml': taskparaml,
            'jobparams': jobparamstxt,
            'columns': columns,
            'attrs': attrs,
            'jobsummary': jobsummary,
            'jobsummarylight': jobsummarylight,
            'jobsummarylightsplitted': jobsummarylightsplitted,
            'eventssummary': eventsdict,
            'ossummary': objectStoreDict,
            'jeditaskid': jeditaskid,
            'logtxt': logtxt,
            'datasets': dsets,
            'dstypes': dstypes,
            'inctrs': inctrs,
            'outctrs': outctrs,
            'vomode': VOMODE,
            'eventservice': eventservice,
            'tkdj': transactionKeyDJ,
            'tkiec': transactionKeyIEC,
            'iecsummary': ifs_summary,
            'built': datetime.now().strftime("%m-%d %H:%M:%S"),
            'warning': warning,
        }
        data.update(getContextVariables(request))
        cacheexpiration = 60*20 #second/minute * minutes
        if taskrec and 'status' in taskrec:
            totaljobs = 0
            for state in jobsummary:
                totaljobs += state['count']
            if taskrec['status'] in ['broken','aborted','done','finished','failed'] and totaljobs > 5000:
                cacheexpiration = 3600*24*31 # we store such data a month
        setCacheEntry(request, "taskInfoNew", json.dumps(data, cls=DateEncoder), cacheexpiration)

        if eventservice:
            response = render_to_response('taskInfoESNew.html', data, content_type='text/html')
        else:
            response = render_to_response('taskInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


def getEventsDetails(request, mode='drop', jeditaskid=0):
    """
    A view for ES task Info page to get events details in different states
    """
    valid, response = initRequest(request)
    if not valid: return response

    tmpTableName = 'ATLAS_PANDABIGMON.TMP_IDS1DEBUG'

    if 'jeditaskid' in  request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
        jeditaskid = request.session['requestParams']['jeditaskid']
        try:
            jeditaskid = int(jeditaskid)
        except:
            return HttpResponse(status=404)

    extrastr = ''
    if mode == 'drop':
        if 'tkd' in request.session['requestParams'] and request.session['requestParams']['tkd']:
            transactionKey = request.session['requestParams']['tkd']
            extrastr += " AND pandaid not in ( select id from {0} where TRANSACTIONKEY = {1})".format(tmpTableName,
                                                                                               transactionKey)
        else:
            return HttpResponse(status=404)
    sqlRequest = """
        select /*+ INDEX_RS_ASC(e JEDI_EVENTS_PK) NO_INDEX_FFS(e JEDI_EVENTS_PK) */
          j.computingsite, j.COMPUTINGELEMENT,e.objstore_id,e.status,count(e.status) as nevents
          from atlas_panda.jedi_events e
            join
                (select computingsite, computingelement,pandaid from ATLAS_PANDA.JOBSARCHIVED4 where jeditaskid={} {}
                UNION
                select computingsite, computingelement,pandaid from ATLAS_PANDAARCH.JOBSARCHIVED where jeditaskid={} {}
                ) j
            on (e.jeditaskid={} and e.pandaid=j.pandaid)
        group by j.computingsite, j.COMPUTINGELEMENT, e.objstore_id, e.status""".format(jeditaskid, extrastr, jeditaskid, extrastr, jeditaskid)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    ossummary = cur.fetchall()
    cur.close()

    ossummarynames = ['computingsite', 'computingelement', 'objectstoreid', 'statusindex', 'nevents']
    objectStoreDict = [dict(zip(ossummarynames, row)) for row in ossummary]
    for row in objectStoreDict: row['statusname'] = eventservicestatelist[row['statusindex']]

    return HttpResponse(json.dumps(objectStoreDict, cls=DateEncoder), content_type='application/json')


def taskchain(request):
    valid, response = initRequest(request)

    jeditaskid = -1
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == -1:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    new_cur = connection.cursor()
    taskChainSQL = "SELECT * FROM table(ATLAS_PANDABIGMON.GETTASKSCHAIN_TEST(%i))" % jeditaskid
    new_cur.execute(taskChainSQL)
    taskChain = new_cur.fetchall()
    results = ["".join(map(str, r)) for r in taskChain]
    ts = "".join(results)

    data = {
        'viewParams': request.session['viewParams'],
        'taskChain': ts,
        'jeditaskid': jeditaskid
    }
    response = render_to_response('taskchain.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def ganttTaskChain(request):
    from django.db import connections
    valid, response = initRequest(request)
    jeditaskid = -1
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == -1:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    new_cur = connections["deft_adcr"].cursor()
    sql_request_str = chainsql.query.replace('%i', str(jeditaskid))
    new_cur.execute(sql_request_str)
    results = new_cur.fetchall()
    results_list = ["".join(map(str, r)) for r in results]
    results_str = results_list[0].replace("\n", "")
    substr_end = results_str.index(">")

    data = {
        'viewParams': request.session['viewParams'],
        'ganttTaskChain': results_str[substr_end+1:],
        'jeditaskid': jeditaskid,
        'request': request,
    }
    response = render_to_response('ganttTaskChain.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def jobSummary2(request, query, exclude={}, extra = "(1=1)", mode='drop', isEventServiceFlag=False,
                substatusfilter='', processingtype='', auxiliaryDict = None, algorithm = 'isOld'):
    jobs = []
    jobScoutIDs = {}
    jobScoutIDs['cputimescoutjob'] = []
    jobScoutIDs['walltimescoutjob'] = []
    jobScoutIDs['ramcountscoutjob'] = []
    jobScoutIDs['iointensityscoutjob'] = []
    jobScoutIDs['outdiskcountscoutjob'] = []
    newquery = copy.deepcopy(query)
    isESMerge = False
    if substatusfilter != '':
        if (substatusfilter == 'es_merge'):
            newquery['eventservice'] = 2
            isESMerge = True
        else:
            exclude['eventservice'] = 2
    isReturnDroppedPMerge=False
    if processingtype != '':
        newquery['processingtype'] = 'pmerge'
        isReturnDroppedPMerge=True

    values = 'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid', \
             'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', \
             'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid','attemptnr', 'transformation'
    # newquery['jobstatus'] = 'finished'

    # Here we apply sort for implem rule about two jobs in Jobsarchived and Jobsarchived4 with 'finished' and closed statuses
    print (algorithm)
    start = time.time()
    jobs.extend(Jobsarchived.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))

    jobs.extend(Jobsdefined4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    end = time.time()
    print(end - start)

    ## drop duplicate jobs
    job1 = {}
    newjobs = []
    for job in jobs:
        pandaid = job['pandaid']
        dropJob = 0
        if pandaid in job1:
            ## This is a duplicate. Drop it.
            dropJob = 1
        else:
            job1[pandaid] = 1
        if (dropJob == 0):
            newjobs.append(job)
    jobs = newjobs


    jobsSet = {}
    newjobs = []

    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}
    cpuTimeCurrent = []
    for job in jobs:

        if not auxiliaryDict is None:
            auxiliaryDict[job['pandaid']] = job['jobsetid']

        if not job['pandaid'] in jobsSet:
            jobsSet[job['pandaid']] = job['jobstatus']
            newjobs.append(job)
        elif jobsSet[job['pandaid']] == 'closed' and job['jobstatus'] == 'finished':
            jobsSet[job['pandaid']] = job['jobstatus']
            newjobs.append(job)
        if 'scout=cpuTime' in job['jobmetrics'] or (
                'scout=' in job['jobmetrics'] and 'cpuTime' in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
            jobScoutIDs['cputimescoutjob'].append(job['pandaid'])
        if 'scout=ioIntensity' in job['jobmetrics'] or (
                'scout=' in job['jobmetrics'] and 'ioIntensity' in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
            jobScoutIDs['iointensityscoutjob'].append(job['pandaid'])
        if 'scout=outDiskCount' in job['jobmetrics'] or (
                'scout=' in job['jobmetrics'] and 'outDiskCount' in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
            jobScoutIDs['outdiskcountscoutjob'].append(job['pandaid'])
        if 'scout=ramCount' in job['jobmetrics'] or (
                'scout=' in job['jobmetrics'] and 'ramCount' in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
            jobScoutIDs['ramcountscoutjob'].append(job['pandaid'])
        if 'scout=walltime' in job['jobmetrics'] or (
                'scout=' in job['jobmetrics'] and 'walltime' in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
            jobScoutIDs['walltimescoutjob'].append(job['pandaid'])
        if 'actualcorecount' in job and job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if job['jobstatus'] in ['finished', 'failed'] and 'endtime' in job and 'starttime' in job and job[
            'starttime'] and job['endtime']:
            duration = max(job['endtime'] - job['starttime'], timedelta(seconds=0))
            job['duration'] = duration.days * 24 * 3600 + duration.seconds
            if job['hs06sec'] is None:
                if job['computingsite'] in pandaSites:
                    job['hs06sec'] = (job['duration']) * float(pandaSites[job['computingsite']]['corepower']) * job[
                    'actualcorecount']
                else:
                    job['hs06sec'] = 0
            if job['nevents'] and job['nevents'] > 0:
                cpuTimeCurrent.append(job['hs06sec'] / job['nevents'])
                job['walltimeperevent'] = round(job['duration'] * job['actualcorecount'] / (job['nevents']*1.0), 2)
            hs06sSum['finished'] += job['hs06sec'] if job['jobstatus'] == 'finished' else 0
            hs06sSum['failed'] += job['hs06sec'] if job['jobstatus'] == 'failed' else 0
            hs06sSum['total'] += job['hs06sec']

    jobs = newjobs

    if mode == 'drop' and len(jobs) < 400000:
        print ('filtering retries')
        if algorithm == 'isNew':
            print('new algorithm!')
            start = time.time()
            #jobs, cntStatus, droplist, droppedPMerge = dropRetrielsJobsV2(newjobs, newquery['jeditaskid'], isReturnDroppedPMerge, isEventTask=True)
            jobs, droppedPMerge,droplist = dropalgorithm.clearDropRetrielsJobs(tk=0, jobs=newjobs, isEventTask=isEventServiceFlag, isReturnDroppedPMerge=isReturnDroppedPMerge)
            end = time.time()
            print(end - start)

        else:
            start = time.time()
            jobs, droplist, droppedPMerge = dropRetrielsJobs(jobs, newquery['jeditaskid'], isReturnDroppedPMerge)
            end = time.time()
            print(end - start)
    elif len(jobs) >= 400000:
        request.session['requestParams']['warning'] = 'Task has more than 400 000 jobs. Dropping was not done to avoid timeout error!'

    plotsNames = ['maxpss', 'maxpsspercore', 'nevents', 'walltime', 'walltimeperevent', 'hs06s', 'cputime', 'cputimeperevent', 'maxpssf', 'maxpsspercoref', 'walltimef', 'hs06sf', 'cputimef', 'cputimepereventf']
    plotsDict = {}

    for pname in plotsNames:
        plotsDict[pname] = {'sites': {}, 'ranges': {}}

    for job in jobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if job['maxpss'] is not None and job['maxpss'] != -1:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['maxpsspercore']['sites']:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['nevents']['sites']:
                    plotsDict['nevents']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpss']['sites']:
                    plotsDict['maxpss']['sites'][job['computingsite']] = []

                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']].append(job['maxpss'] / 1024 / job['actualcorecount'])
                plotsDict['maxpss']['sites'][job['computingsite']].append(job['maxpss'] / 1024)
                plotsDict['nevents']['sites'][job['computingsite']].append(job['nevents'])
            elif job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['maxpsspercoref']['sites']:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpssf']['sites']:
                    plotsDict['maxpssf']['sites'][job['computingsite']] = []
                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']].append(job['maxpss'] / 1024 / job['actualcorecount'])
                plotsDict['maxpssf']['sites'][job['computingsite']].append(job['maxpss'] / 1024)
        if 'duration' in job and job['duration']:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['walltime']['sites']:
                    plotsDict['walltime']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['hs06s']['sites']:
                    plotsDict['hs06s']['sites'][job['computingsite']] = []
                plotsDict['walltime']['sites'][job['computingsite']].append(job['duration'])
                plotsDict['hs06s']['sites'][job['computingsite']].append(job['hs06sec'])
                if 'walltimeperevent' in job:
                    if job['computingsite'] not in plotsDict['walltimeperevent']['sites']:
                        plotsDict['walltimeperevent']['sites'][job['computingsite']] = []
                    plotsDict['walltimeperevent']['sites'][job['computingsite']].append(job['walltimeperevent'])
            if job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['walltimef']['sites']:
                    plotsDict['walltimef']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['hs06sf']['sites']:
                    plotsDict['hs06sf']['sites'][job['computingsite']] = []
                plotsDict['walltimef']['sites'][job['computingsite']].append(job['duration'])
                plotsDict['hs06sf']['sites'][job['computingsite']].append(job['hs06sec'])
        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['cputime']['sites']:
                    plotsDict['cputime']['sites'][job['computingsite']] = []
                plotsDict['cputime']['sites'][job['computingsite']].append(job['cpuconsumptiontime'])
                if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                    if job['computingsite'] not in plotsDict['cputimeperevent']['sites']:
                        plotsDict['cputimeperevent']['sites'][job['computingsite']] = []
                    plotsDict['cputimeperevent']['sites'][job['computingsite']].append(round(job['cpuconsumptiontime']/(job['nevents']*1.0), 2))
            if job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['cputimef']['sites']:
                    plotsDict['cputimef']['sites'][job['computingsite']] = []
                plotsDict['cputimef']['sites'][job['computingsite']].append(job['cpuconsumptiontime'])
                if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                    if job['computingsite'] not in plotsDict['cputimepereventf']['sites']:
                        plotsDict['cputimepereventf']['sites'][job['computingsite']] = []
                    plotsDict['cputimepereventf']['sites'][job['computingsite']].append(round(job['cpuconsumptiontime']/(job['nevents']*1.0), 2))

    nbinsmax = 100
    for pname in plotsNames:
        rawdata = []
        for k,d in plotsDict[pname]['sites'].items():
            rawdata.extend(d)
        if len(rawdata) > 0:
            plotsDict[pname]['stats'] = []
            plotsDict[pname]['stats'].append(np.average(rawdata))
            plotsDict[pname]['stats'].append(np.std(rawdata))
            bins, ranges = np.histogram(rawdata, bins='auto')
            if len(ranges) > nbinsmax + 1:
                bins, ranges = np.histogram(rawdata, bins=nbinsmax)
            if pname not in ('walltimeperevent', 'cputimeperevent'):
                plotsDict[pname]['ranges'] = list(np.ceil(ranges))
            else:
                plotsDict[pname]['ranges'] = list(ranges)
            for site in plotsDict[pname]['sites'].keys():
                sitedata = [x for x in plotsDict[pname]['sites'][site]]
                plotsDict[pname]['sites'][site] = list(np.histogram(sitedata, ranges)[0])
        else:
            try:
                del(plotsDict[pname])
            except:
                pass

    jobstates = []
    global statelist
    for state in statelist:
        statecount = {}
        statecount['name'] = state
        statecount['count'] = 0
        for job in jobs:
            # if isEventService and job['jobstatus'] == 'cancelled':
            #    job['jobstatus'] = 'finished'
            if job['jobstatus'] == state:
                statecount['count'] += 1
                continue
        jobstates.append(statecount)
    essummary = dict((key, 0) for key in eventservicestatelist)
    transactionKey = -1
    if isEventServiceFlag and not isESMerge:
        print ('getting events states summary')
        if mode == 'drop' and len(jobs) < 400000:
            esjobs = []
            for job in jobs:
                esjobs.append(job['pandaid'])

            random.seed()

            if dbaccess['default']['ENGINE'].find('oracle') >= 0:
                tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
            else:
                tmpTableName = "TMP_IDS1DEBUG"

            transactionKey = random.randrange(1000000)
#            connection.enter_transaction_management()
            new_cur = connection.cursor()
            executionData = []
            for id in esjobs:
                executionData.append((id, transactionKey, timezone.now().strftime(defaultDatetimeFormat) ))
            query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY,INS_TIME) VALUES (%s, %s, %s)"""
            new_cur.executemany(query, executionData)
#            connection.commit()
            jeditaskidstr = str(newquery['jeditaskid'])
            new_cur.execute(
                """
                SELECT /*+ cardinality(tmp 10) INDEX_RS_ASC(ev JEDI_EVENTS_PK) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  
                    SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS EVCOUNT, STATUS 
                FROM ATLAS_PANDA.JEDI_EVENTS ev, %s 
                WHERE TRANSACTIONKEY=%i AND PANDAID = ID AND JEDITASKID=%s 
                GROUP BY STATUS
                """ % (tmpTableName, transactionKey, jeditaskidstr)
            )

            evtable = dictfetchall(new_cur)
            # new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
#            connection.commit()
#            connection.leave_transaction_management()
            for ev in evtable:
                essummary[eventservicestatelist[ev['STATUS']]] += ev['EVCOUNT']
        eventsdict=[]
        if mode == 'nodrop':
            equery = {'jeditaskid': newquery['jeditaskid']}
            eventsdict.extend(
                JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
            for state in eventsdict:
                essummary[eventservicestatelist[state['status']]]=state['count']

    return plotsDict, jobstates, essummary, transactionKey, jobScoutIDs, hs06sSum


def getJobSummaryForTask(request, jeditaskid=-1):
    valid, response = initRequest(request)
    if not valid: return response

    try:
        jeditaskid = int(jeditaskid)
    except:
        return HttpResponse(status=404)

    if jeditaskid == -1:
        return HttpResponse(status=404)

    # possible values of infotype are jobssummary, plots, scouts. Provided type will be returned, other put in cache.
    if 'infotype' in request.session['requestParams'] and request.session['requestParams']['infotype']:
        infotype = request.session['requestParams']['infotype']
    else:
        return response

    if 'es' in request.session['requestParams'] and request.session['requestParams']['es'] == 'True':
        es = True
    else:
        es = False

    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'drop':
        mode = 'drop'
    else:
        mode = 'nodrop'

    data = getCacheEntry(request, "jobSummaryForTask"+str(jeditaskid)+mode, isData=True)
    # data = None

    if data is not None:
        data = json.loads(data)

        plotDict = {}
        if 'plotsDict' in data:
            oldPlotDict = data['plotsDict']
            for plotName, plotData in oldPlotDict.items():
                if 'sites' in plotData and 'ranges' in plotData:
                    plotDict[str(plotName)] = {'sites': {}, 'ranges': plotData['ranges'], 'stats': plotData['stats']}
                    for dictSiteName, listValues in plotData['sites'].items():
                        try:
                            plotDict[str(plotName)]['sites'][str(dictSiteName)] = []
                            plotDict[str(plotName)]['sites'][str(dictSiteName)] += listValues
                        except:
                            pass
                elif plotName == 'neventsbysite':
                    plotDict[str(plotName)] = plotData

            data['plotsDict'] = plotDict

        data['request'] = request

        if infotype == 'jobsummary':
            response = render_to_response('jobSummaryForTask.html', data, content_type='text/html')
        elif infotype == 'scouts':
            response = render_to_response('scoutsForTask.html', data, content_type='text/html')
        elif infotype == 'plots':
            response = HttpResponse(json.dumps(data['plotsDict'], cls=DateEncoder), content_type='application/json')
        else:
            response = None
        return response


    extra = '(1=1)'
    query = {}
    query['jeditaskid'] = jeditaskid

    if mode == 'drop':
        start = time.time()
        extra, transactionKeyDJ = insert_dropped_jobs_to_tmp_table(query, extra)
        end = time.time()
        print("Inserting dropped jobs: {} sec".format(end - start))
        print('tk of dropped jobs: {}'.format(transactionKeyDJ))

    plotsDict, jobsummary, jobsummaryMerge, jobScoutIDs = job_summary_for_task(
        request, query, pandaSites, statelist, extra=extra, isEventServiceFlag=es)


    nonzeroMERGE = 0
    for status in jobsummaryMerge:
        if status['count'] > 0:
            nonzeroMERGE += 1
            break

    if nonzeroMERGE == 0:
        jobsummaryMerge = None

    alldata = {
        'jeditaskid': jeditaskid,
        'request': request,
        'jobsummary': jobsummary,
        'jobsummaryMerge': jobsummaryMerge,
        'jobScoutIDs': jobScoutIDs,
        'plotsDict': plotsDict,
    }
    setCacheEntry(request, 'jobSummaryForTask'+str(jeditaskid)+mode, json.dumps(alldata, cls=DateEncoder), 60 * 10, isData=True)

    if infotype == 'jobsummary':
        data = {
            'jeditaskid': jeditaskid,
            'request': request,
            'jobsummary': jobsummary,
            'jobsummaryMerge': jobsummaryMerge,
        }
        response = render_to_response('jobSummaryForTask.html', data, content_type='text/html')
    elif infotype == 'scouts':
        data = {
            'jeditaskid': jeditaskid,
            'request': request,
            'jobscoutids': request,
        }
        response = render_to_response('scoutsForTask.html', data, content_type='text/html')
    elif infotype == 'plots':
        response = HttpResponse(json.dumps(plotsDict, cls=DateEncoder), content_type='application/json')
    else:
        response = HttpResponse(status=204)
    if response:
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response




def jobStateSummary(jobs):
    global statelist
    statecount = {}
    for state in statelist:
        statecount[state] = 0
    for job in jobs:
        statecount[job['jobstatus']] += 1
    return statecount


def errorSummaryDict(request, jobs, tasknamedict, testjobs):
    """ take a job list and produce error summaries from it """
    errsByCount = {}
    errsBySite = {}
    errsByUser = {}
    errsByTask = {}

    sumd = {}
    ## histogram of errors vs. time, for plotting
    errHist = {}

    flist = standard_errorfields
    print (len(jobs))
    for job in jobs:
        if not testjobs:
            if job['jobstatus'] not in ['failed', 'holding']: continue
        site = job['computingsite']
        #        if 'cloud' in request.session['requestParams']:
        #            if site in homeCloud and homeCloud[site] != request.session['requestParams']['cloud']: continue
        user = job['produsername']
        taskname = ''
        if job['jeditaskid'] is not None and job['jeditaskid'] > 0:
            taskid = job['jeditaskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'jeditaskid'
        else:
            taskid = job['taskid'] if not job['taskid'] is None else 0
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'taskid'

        if 'modificationtime' in job and job['jobstatus'] == 'failed':
            tm = job['modificationtime']
            if tm is not None:
                tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
                if not tm in errHist: errHist[tm] = 1
                else:
                    errHist[tm] += 1

        ## Overall summary
        for f in flist:
            if job[f]:
                if f == 'taskid' and job[f] < 1000000 and 'produsername' not in request.session['requestParams']:
                    pass
                else:
                    if not f in sumd: sumd[f] = {}
                    if not job[f] in sumd[f]: sumd[f][job[f]] = 0
                    sumd[f][job[f]] += 1
        if job['specialhandling']:
            if not 'specialhandling' in sumd: sumd['specialhandling'] = {}
            shl = job['specialhandling'].split()
            for v in shl:
                if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                sumd['specialhandling'][v] += 1
        errsByList = {}
        #errsByCount[errcode]['list'] = {}
        for err in errorcodelist:
            if job[err['error']] != 0 and job[err['error']] != '' and job[err['error']] != None:
                errval = job[err['error']]
                ## error code of zero is not an error
                if errval == 0 or errval == '0' or errval == None: continue
                errdiag = ''
                try:
                    errnum = int(errval)
                    if err['error'] in errorCodes and errnum in errorCodes[err['error']]:
                        errdiag = errorCodes[err['error']][errnum]
                except:
                    errnum = errval
                errcode = "%s:%s" % (err['name'], errnum)
                if err['diag']:
                    errdiag = job[err['diag']]
                errsByList[job['pandaid']]=errdiag

                if errcode not in errsByCount:
                    errsByCount[errcode] = {}
                    errsByCount[errcode]['error'] = errcode
                    errsByCount[errcode]['codename'] = err['error']
                    errsByCount[errcode]['codeval'] = errnum
                    errsByCount[errcode]['diag'] = errdiag
                    errsByCount[errcode]['count'] = 0
                    errsByCount[errcode]['pandalist'] = {}
                errsByCount[errcode]['count'] += 1
                errsByCount[errcode]['pandalist'].update(errsByList)
                if user not in errsByUser:
                    errsByUser[user] = {}
                    errsByUser[user]['name'] = user
                    errsByUser[user]['errors'] = {}
                    errsByUser[user]['toterrors'] = 0
                if errcode not in errsByUser[user]['errors']:
                    errsByUser[user]['errors'][errcode] = {}
                    errsByUser[user]['errors'][errcode]['error'] = errcode
                    errsByUser[user]['errors'][errcode]['codename'] = err['error']
                    errsByUser[user]['errors'][errcode]['codeval'] = errnum
                    errsByUser[user]['errors'][errcode]['diag'] = errdiag
                    errsByUser[user]['errors'][errcode]['count'] = 0
                errsByUser[user]['errors'][errcode]['count'] += 1
                errsByUser[user]['toterrors'] += 1

                if site not in errsBySite:
                    errsBySite[site] = {}
                    errsBySite[site]['name'] = site
                    errsBySite[site]['errors'] = {}
                    errsBySite[site]['toterrors'] = 0
                    errsBySite[site]['toterrjobs'] = 0
                if errcode not in errsBySite[site]['errors']:
                    errsBySite[site]['errors'][errcode] = {}
                    errsBySite[site]['errors'][errcode]['error'] = errcode
                    errsBySite[site]['errors'][errcode]['codename'] = err['error']
                    errsBySite[site]['errors'][errcode]['codeval'] = errnum
                    errsBySite[site]['errors'][errcode]['diag'] = errdiag
                    errsBySite[site]['errors'][errcode]['count'] = 0
                errsBySite[site]['errors'][errcode]['count'] += 1
                errsBySite[site]['toterrors'] += 1

                if tasktype == 'jeditaskid' or (taskid is not None and taskid > 1000000) or 'produsername' in request.session['requestParams']:
                    if taskid not in errsByTask:
                        errsByTask[taskid] = {}
                        errsByTask[taskid]['name'] = taskid
                        errsByTask[taskid]['longname'] = taskname
                        errsByTask[taskid]['errors'] = {}
                        errsByTask[taskid]['toterrors'] = 0
                        errsByTask[taskid]['toterrjobs'] = 0
                        errsByTask[taskid]['tasktype'] = tasktype
                    if errcode not in errsByTask[taskid]['errors']:
                        errsByTask[taskid]['errors'][errcode] = {}
                        errsByTask[taskid]['errors'][errcode]['error'] = errcode
                        errsByTask[taskid]['errors'][errcode]['codename'] = err['error']
                        errsByTask[taskid]['errors'][errcode]['codeval'] = errnum
                        errsByTask[taskid]['errors'][errcode]['diag'] = errdiag
                        errsByTask[taskid]['errors'][errcode]['count'] = 0
                    errsByTask[taskid]['errors'][errcode]['count'] += 1
                    errsByTask[taskid]['toterrors'] += 1

        if site in errsBySite: errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask: errsByTask[taskid]['toterrjobs'] += 1

    ## reorganize as sorted lists
    errsByCountL = []
    errsBySiteL = []
    errsByUserL = []
    errsByTaskL = []
    v = {}
    esjobs = []
    kys = errsByCount.keys()
    kys = sorted(kys)
    for err in kys:
        v = {}
        for key, value in sorted(errsByCount[err]['pandalist'].items()):
            if value == '':
                value = 'None'
            esjobs.append(key)
           # if err == 'jobdispatcher:100':
           #     value = re.sub("(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})", "*", value)
           # elif err == 'exe:68':
           #     #value = re.sub("","*",value)
           #     value = value
          #  elif err == 'pilot:1099':
          #      if ('STAGEIN FAILED: Get error: Staging input file failed' in value):
         #           value = 'STAGEIN FAILED: Get error: Staging input file failed'
          #  v.setdefault(value, []).append(key)
        #errsByCount[err]['pandalist'] = v
        errsByCountL.append(errsByCount[err])



       # random.seed()

        #if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        #    tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
        #else:
        #    tmpTableName = "TMP_IDS1DEBUG"

        #transactionKey = random.randrange(1000000)
        #            connection.enter_transaction_management()
        #new_cur = connection.cursor()
        #executionData = []
        #for id in esjobs:
        #    executionData.append((id, transactionKey, timezone.now().strftime(defaultDatetimeFormat)))
        #query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY,INS_TIME) VALUES (%s, %s, %s)"""
        #new_cur.executemany(query, executionData)
        #            connection.commit()

    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByCountL = sorted(errsByCountL, key=lambda x: -x['count'])

    kys = list(errsByUser.keys())
    kys = sorted(kys)
    for user in kys:
        errsByUser[user]['errorlist'] = []
        errkeys = errsByUser[user]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsByUser[user]['errorlist'].append(errsByUser[user]['errors'][err])
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
            errsByUser[user]['errorlist'] = sorted(errsByUser[user]['errorlist'], key=lambda x: -x['count'])
        errsByUserL.append(errsByUser[user])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByUserL = sorted(errsByUserL, key=lambda x: -x['toterrors'])


    kys = list(errsBySite.keys())
    kys = sorted(kys)
    for site in kys:
        errsBySite[site]['errorlist'] = []
        errkeys = errsBySite[site]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsBySite[site]['errorlist'].append(errsBySite[site]['errors'][err])
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
            errsBySite[site]['errorlist'] = sorted(errsBySite[site]['errorlist'], key=lambda x: -x['count'])
        errsBySiteL.append(errsBySite[site])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsBySiteL = sorted(errsBySiteL, key=lambda x: -x['toterrors'])

    kys = list(errsByTask.keys())
    kys = sorted(kys)
    for taskid in kys:
        errsByTask[taskid]['errorlist'] = []
        errkeys = errsByTask[taskid]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsByTask[taskid]['errorlist'].append(errsByTask[taskid]['errors'][err])
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
            errsByTask[taskid]['errorlist'] = sorted(errsByTask[taskid]['errorlist'], key=lambda x: -x['count'])
        errsByTaskL.append(errsByTask[taskid])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByTaskL = sorted(errsByTaskL, key=lambda x: -x['toterrors'])

    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sorted(sumd[f].keys())

        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])

    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        for item in suml:
            item['list'] = sorted(item['list'], key=lambda x: -x['kvalue'])

    kys = errHist.keys()
    kys = sorted(kys)
    errHistL = []
    for k in kys:
        errHistL.append([k, errHist[k]])

    return errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, errHistL


def getTaskName(tasktype, taskid):
    taskname = ''
    if tasktype == 'taskid':
        taskname = ''
    elif tasktype == 'jeditaskid' and taskid and taskid != 'None':
        tasks = JediTasks.objects.filter(jeditaskid=taskid).values('taskname')
        if len(tasks) > 0:
            taskname = tasks[0]['taskname']
    return taskname


def get_error_message_summary(jobs):
    """
    Aggregation of error messages for each error code
    :param jobs: list of job dicts including error codees, error messages, timestamps of job start and end, corecount
    :return: list of rows for datatable
    """
    error_message_summary_list = []
    errorMessageSummary = {}
    N_SAMPLE_JOBS = 3
    for job in jobs:
        for errortype in errorcodelist:
            if errortype['error'] in job and job[errortype['error']] is not None and job[errortype['error']] != '' and int(job[errortype['error']]) > 0:
                errorcodestr = errortype['name'] + ':' + str(job[errortype['error']])
                if not errorcodestr in errorMessageSummary:
                    errorMessageSummary[errorcodestr] = {'count': 0, 'walltimeloss': 0, 'messages': {}}
                errorMessageSummary[errorcodestr]['count'] += 1
                try:
                    corecount = int(job['actualcorecount'])
                except:
                    corecount = 1
                try:
                    walltime = int(get_job_walltime(job))
                except:
                    walltime = 0
                errorMessageSummary[errorcodestr]['walltimeloss'] += walltime * corecount
                # transexitcode has no diag field in DB, so we get it from ErrorCodes class
                if errortype['name'] != 'transformation':
                    errordiag = job[errortype['diag']] if len(job[errortype['diag']]) > 0 else '---'
                else:
                    try:
                        errordiag = errorCodes[errortype['error']][int(job[errortype['error']])]
                    except:
                        errordiag = '--'
                if not errordiag in errorMessageSummary[errorcodestr]['messages']:
                    errorMessageSummary[errorcodestr]['messages'][errordiag] = {'count': 0, 'pandaids': []}
                errorMessageSummary[errorcodestr]['messages'][errordiag]['count'] += 1
                if len(errorMessageSummary[errorcodestr]['messages'][errordiag]['pandaids']) < N_SAMPLE_JOBS:
                    errorMessageSummary[errorcodestr]['messages'][errordiag]['pandaids'].append(job['pandaid'])

    # form a dict for mapping error code name and field in panda db in order to prepare links to job selection
    errname2dbfield = {}
    for errortype in errorcodelist:
        errname2dbfield[errortype['name']] = errortype['error']

    # dict -> list
    for errcode, errinfo in errorMessageSummary.items():
        errcodename = errname2dbfield[errcode.split(':')[0]]
        errcodeval = errcode.split(':')[1]
        for errmessage, errmessageinfo in errinfo['messages'].items():
            error_message_summary_list.append({
                'errcode': errcode,
                'errcodename': errcodename,
                'errcodeval': errcodeval,
                'errcodecount': errinfo['count'],
                'errcodewalltimeloss': round(errinfo['walltimeloss']/60.0/60.0/24.0/360.0, 2),
                'errmessage': errmessage,
                'errmessagecount': errmessageinfo['count'],
                'pandaids': list(errmessageinfo['pandaids'])
            })

    return error_message_summary_list


tcount = {}
lock = Lock()

def totalCount(panJobList, query, wildCardExtension, dkey):
    print ('Thread started')
    lock.acquire()
    try:
        tcount.setdefault(dkey,[])
        for panJob in panJobList:
            wildCardExtension = wildCardExtension.replace('%20', ' ')
            wildCardExtension = wildCardExtension.replace('%2520', ' ')
            wildCardExtension = wildCardExtension.replace('%252540', '@')
            wildCardExtension = wildCardExtension.replace('%2540', '@')
            wildCardExtension = wildCardExtension.replace('+', ' ')
            wildCardExtension = wildCardExtension.replace('%', ' ')
            tcount[dkey].append(panJob.objects.filter(**query).extra(where=[wildCardExtension]).count())
    finally:
        lock.release()
    print ('Thread finished')

def digkey (rq):
    sk = rq.session.session_key
    qt = rq.session['qtime']
    if sk is None:
        sk = random.randrange(1000000)
    hashkey = hashlib.sha256((str(sk) + ' ' + qt).encode('utf-8'))
    return hashkey.hexdigest()


@login_customrequired
def errorSummary(request):
    start_time = time.time()
    valid, response = initRequest(request)
    thread = None
    dkey = digkey(request)
    if not valid:
        return response

    _logger.debug('Initialized request: {}'.format(time.time() - start_time))

    # Here we try to get cached data
    data = getCacheEntry(request, "errorSummary")
    if data is not None:
        _logger.debug('Got cached data: {}'.format(time.time() - start_time))
        data = json.loads(data)
        data['request'] = request
        # Filtering data due to user settings
        # if 'ADFS_LOGIN' in request.session and request.session['ADFS_LOGIN'] and 'IS_TESTER' in request.session and request.session['IS_TESTER']:
        if request.user.is_authenticated and request.user.is_tester:
            data = filterErrorData(request, data)
        if data['errHist']:
            for list in data['errHist']:
                try:
                    list[0] = datetime.strptime(list[0],"%Y-%m-%dT%H:%M:%S")
                except:
                    pass
        _logger.debug('Processed cached data: {}'.format(time.time() - start_time))
        response = render_to_response('errorSummary.html', data, content_type='text/html')
        _logger.debug('Rendered template from cached data: {}'.format(time.time() - start_time))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    # if 'jobattr' in request.session['requestParams'] or 'tables' in request.session['requestParams']:
    #     saveUserSettings(request,'errors')
        # if request.GET:
        #     addGetRequestParams(request)

    testjobs = False
    if 'prodsourcelabel' in request.session['requestParams'] and request.session['requestParams'][
        'prodsourcelabel'].lower().find('test') >= 0:
        testjobs = True

    jobtype = ''
    if 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'
    elif testjobs:
        jobtype = 'rc_test'

    if jobtype == '':
        hours = 3
        limit = 100000
    elif jobtype.startswith('anal'):
        hours = 6
        limit = 100000
    elif 'JOB_LIMIT' in request.session:
        hours = 6
        limit = request.session['JOB_LIMIT']
    else:
        hours = 12
        limit = 100000

    if 'hours' in request.session['requestParams']:
        hours = int(request.session['requestParams']['hours'])

    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])

    if 'display_limit' in request.session['requestParams']:
        display_limit = int(request.session['requestParams']['display_limit'])
    else:
        display_limit = 9999999


    xurlsubst = extensibleURL(request)
    xurlsubstNoSite = xurlsubst

    # Preprocess request to cover all sites for cloud to view jobs assigned to the World
    if ('cloud' in request.session['requestParams']) and ('computingsite' not in request.session['requestParams']) and (
        request.session['requestParams']['cloud'] != 'WORLD') and (
        '|' not in request.session['requestParams']['cloud']):
        cloud = request.session['requestParams']['cloud']
        del request.session['requestParams']['cloud']
        sites = set([site['site'] for site in pandaSites.values() if site['cloud'] == cloud])
        siteStr = ""
        for site in sites:
            siteStr += "|" + site
        siteStr = siteStr[1:]
        request.session['requestParams']['computingsite'] = siteStr

        # this substitution is nessessary to propagate update in the xurl
        updatedRequest = ""
        updatedRequestNoSite = ""

        for param in request.session['requestParams']:
            updatedRequest += '&' + param + '=' + request.session['requestParams'][param]
            if param != 'computingsite':
                updatedRequestNoSite += '&' + param + '=' + request.session['requestParams'][param]

        updatedRequest = updatedRequest[1:]
        updatedRequestNoSite = updatedRequestNoSite[1:]
        xurlsubst = '/errors/?' + updatedRequest + '&'
        xurlsubstNoSite = '/errors/?' + updatedRequestNoSite + '&'

    _logger.debug('Processed specific params: {}'.format(time.time() - start_time))

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=limit, wildCardExt=True)

    _logger.debug('Finished set up view: {}'.format(time.time() - start_time))

    if not testjobs and 'jobstatus' not in request.session['requestParams']:
        query['jobstatus__in'] = ['failed', 'holding']
    jobs = []
    values = (
        'eventservice', 'produsername', 'produserid', 'pandaid', 'cloud', 'computingsite', 'cpuconsumptiontime',
        'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime',
        'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'starttime',
        'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode',
        'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag',
        'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode',
        'destinationse', 'currentpriority', 'computingelement', 'gshare', 'reqid', 'actualcorecount'
    )

    if testjobs:
        jobs.extend(
            Jobsdefined4.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values(*values))
        jobs.extend(
            Jobswaiting4.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values(*values))
    listJobs = Jobsactive4, Jobsarchived4, Jobsdefined4, Jobswaiting4

    jobs.extend(
        Jobsactive4.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values(*values))
    jobs.extend(
        Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values(*values))

    if (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0], "%Y-%m-%d %H:%M:%S")).days > 1) or \
                ((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
                                                     "%Y-%m-%d %H:%M:%S")).days > 1)):
        jobs.extend(
            Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values(
                *values))
        listJobs = Jobsactive4, Jobsarchived4, Jobsdefined4, Jobswaiting4, Jobsarchived
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension,dkey))
        thread.start()
    else:
        thread = None

    _logger.debug('Got jobs: {}'.format(time.time() - start_time))

    jobs = cleanJobList(request, jobs, mode='nodrop', doAddMeta=False)

    _logger.debug('Cleaned jobs list: {}'.format(time.time() - start_time))

    error_message_summary = get_error_message_summary(jobs)

    _logger.debug('Prepared new error message summary: {}'.format(time.time() - start_time))

    njobs = len(jobs)
    tasknamedict = taskNameDict(jobs)

    _logger.debug('Got taskname for jobs: {}'.format(time.time() - start_time))

    ## Build the error summary.
    errsByCount, errsBySite, errsByUser, errsByTask, sumd, errHist = errorSummaryDict(request, jobs, tasknamedict,
                                                                                      testjobs)

    _logger.debug('Error summary built: {}'.format(time.time() - start_time))

    ## Build the state summary and add state info to site error summary
    # notime = True
    # if testjobs: notime = False
    notime = False  #### behave as it used to before introducing notime for dashboards. Pull only 12hrs.
    statesummary = dashSummary(request, hours, limit=limit, view=jobtype, cloudview='region', notime=notime)
    sitestates = {}
    savestates = ['finished', 'failed', 'cancelled', 'holding', ]
    for cloud in statesummary:
        for site in cloud['sites']:
            sitename = cloud['sites'][site]['name']
            sitestates[sitename] = {}
            for s in savestates:
                sitestates[sitename][s] = cloud['sites'][site]['states'][s]['count']
            sitestates[sitename]['pctfail'] = cloud['sites'][site]['pctfail']

    for site in errsBySite:
        sitename = site['name']
        if sitename in sitestates:
            for s in savestates:
                if s in sitestates[sitename]: site[s] = sitestates[sitename][s]
            if 'pctfail' in sitestates[sitename]: site['pctfail'] = sitestates[sitename]['pctfail']

    _logger.debug('Built errors by site summary: {}'.format(time.time() - start_time))

    taskname = ''
    if not testjobs:
        ## Build the task state summary and add task state info to task error summary
        taskstatesummary = dashTaskSummary(request, hours, limit=limit, view=jobtype)

        _logger.debug('Prepared data for errors by task summary: {}'.format(time.time() - start_time))

        taskstates = {}
        for task in taskstatesummary:
            taskid = task['taskid']
            taskstates[taskid] = {}
            for s in savestates:
                taskstates[taskid][s] = task['states'][s]['count']
            if 'pctfail' in task: taskstates[taskid]['pctfail'] = task['pctfail']
        for task in errsByTask:
            taskid = task['name']
            if taskid in taskstates:
                for s in savestates:
                    if s in taskstates[taskid]: task[s] = taskstates[taskid][s]
                if 'pctfail' in taskstates[taskid]: task['pctfail'] = taskstates[taskid]['pctfail']
        if 'jeditaskid' in request.session['requestParams']:
            taskname = getTaskName('jeditaskid', request.session['requestParams']['jeditaskid'])

    _logger.debug('Built errors by task summary: {}'.format(time.time() - start_time))

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'alpha'
    flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)

    _logger.debug('Built google diagram: {}'.format(time.time() - start_time))

    if thread!=None:
        try:
            thread.join()
            jobsErrorsTotalCount = sum(tcount[dkey])
            print (dkey)
            print (tcount[dkey])
            del tcount[dkey]
            print (tcount)
            print (jobsErrorsTotalCount)
        except: jobsErrorsTotalCount = -1
    else: jobsErrorsTotalCount = -1

    _logger.debug('Finished thread counting total number of jobs: {}'.format(time.time() - start_time))

    listPar =[]
    for key, val in request.session['requestParams'].items():
        if (key!='limit' and key!='display_limit'):
            listPar.append(key + '=' + str(val))
    if len(listPar)>0:
        urlParametrs = '&'.join(listPar)+'&'
    else:
        urlParametrs = None
    print (listPar)
    del listPar
    if (math.fabs(njobs-jobsErrorsTotalCount)<1000):
        jobsErrorsTotalCount = None
    else:
        jobsErrorsTotalCount = int(math.ceil((jobsErrorsTotalCount+10000)/10000)*10000)
    request.session['max_age_minutes'] = 6

    _logger.debug('Formed list of params: {}'.format(time.time() - start_time))

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        nosorturl = removeParam(request.get_full_path(), 'sortby')
        xurl = extensibleURL(request)
        time_locked_url = removeParam(removeParam(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
                          'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
                          '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
        jobsurl = xurlsubst.replace('/errors/', '/jobs/')
        jobsurlNoSite = xurlsubstNoSite.replace('/errors/', '')

        errsByCount = importToken(request,errsByCount=errsByCount)
        TFIRST = request.session['TFIRST'].strftime(defaultDatetimeFormat)
        TLAST = request.session['TLAST'].strftime(defaultDatetimeFormat)
        del request.session['TFIRST']
        del request.session['TLAST']

        _logger.debug('Extra data preparation for template: {}'.format(time.time() - start_time))

        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'requestString': urlParametrs,
            'jobtype': jobtype,
            'njobs': njobs,
            'hours': LAST_N_HOURS_MAX,
            'limit': limit,
            'user': None,
            'xurl': xurl,
            'xurlsubst': xurlsubst,
            'xurlsubstNoSite': xurlsubstNoSite,
            'jobsurlNoSite': jobsurlNoSite,
            'jobsurl': jobsurl,
            'nosorturl': nosorturl,
            'time_locked_url': time_locked_url,
            'errsByCount': errsByCount,
            'errsBySite': errsBySite[:display_limit] if len(errsBySite) > display_limit else errsBySite,
            'errsByUser': errsByUser[:display_limit] if len(errsByUser) > display_limit else errsByUser,
            'errsByTask': errsByTask[:display_limit] if len(errsByTask) > display_limit else errsByTask,
            'sumd': sumd,
            'errHist': errHist,
            'errsByMessage': json.dumps(error_message_summary),
            'tfirst': TFIRST,
            'tlast': TLAST,
            'sortby': sortby,
            'taskname': taskname,
            'flowstruct': flowstruct,
            'jobsErrorsTotalCount': jobsErrorsTotalCount,
            'display_limit': display_limit,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "errorSummary", json.dumps(data, cls=DateEncoder), 60 * 20)

        _logger.debug('Set cache: {}'.format(time.time() - start_time))

        # Filtering data due to user settings
        if request.user.is_authenticated and request.user.is_tester:
            data = filterErrorData(request, data)
        response = render_to_response('errorSummary.html', data, content_type='text/html')

        _logger.debug('Rendered template: {}'.format(time.time() - start_time))

        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif 'fields' in request.session['requestParams'] and request.session['requestParams']['fields']:
        del request.session['TFIRST']
        del request.session['TLAST']
        fields = request.session['requestParams']['fields'].split(',')
        data = {}
        if 'jobSummary' in fields:
            data['jobSummary'] = sumd
        if 'errsByCount' in fields:
            data['errsByCount'] = errsByCount
        if 'errsBySite' in fields:
            data['errsBySite'] = errsBySite
        if 'errsByUser' in fields:
            data['errsByUser'] = errsByUser
        if 'errsByTask' in fields:
            data['errsByTask'] = errsByTask
        return HttpResponse(json.dumps(data), content_type='application/json')
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = []
        for job in jobs:
            resp.append({'pandaid': job['pandaid'], 'status': job['jobstatus'], 'prodsourcelabel': job['prodsourcelabel'],
                         'produserid': job['produserid']})
        return HttpResponse(json.dumps(resp), content_type='application/json')


def filterErrorData(request, data):
    defaultErrorsPreferences = {}
    defaultErrorsPreferences['jobattr'] = standard_errorfields

    defaultErrorsPreferences['tables'] = {
        'jobattrsummary' : 'Job attribute summary',
        'errorsummary': 'Overall error summary',
        'siteerrorsummary' : 'Site error summary',
        'usererrorsummary' : 'User error summary',
        'taskerrorsummary' : 'Task error summary'
    }
    userids = BPUser.objects.filter(email=request.user.email).values('id')
    userid = userids[0]['id']
    try:
        userSetting = BPUserSettings.objects.get(page='errors', userid=userid)
        userPreferences = json.loads(userSetting.preferences)
    except:
        saveUserSettings(request, 'errors')
        userSetting = BPUserSettings.objects.get(page='errors', userid=userid)
        userPreferences = json.loads(userSetting.preferences)
        # userPreferences = defaultErrorsPreferences
    userPreferences['defaulttables'] = defaultErrorsPreferences['tables']
    userPreferences['defaultjobattr'] = defaultErrorsPreferences['jobattr']
   ###TODO Temporary fix. Need to redesign
    userPreferences['jobattr'].append('reqid')

    data['userPreferences'] = userPreferences
    if 'tables' in userPreferences:
        if 'jobattrsummary' in userPreferences['tables']:
            if 'jobattr' in userPreferences:
                sumd_new = []
                for attr in userPreferences['jobattr']:
                    for field in data['sumd']:
                        if attr == field['field']:
                            sumd_new.append(field)
                            continue
                data['sumd'] = sorted(sumd_new, key=lambda x: x['field'])
        else:
            try:
                del data['sumd']
            except:
                pass
        if 'errorsummary' not in userPreferences['tables']:
            try:
                del data['errsByCount']
            except:
                pass
        if 'siteerrorsummary' not in userPreferences['tables']:
            try:
                del data['errsBySite']
            except:
                pass
        if 'usererrorsummary' not in userPreferences['tables']:
            try:
                del data['errsByUser']
            except:
                pass
        if 'taskerrorsummary' not in userPreferences['tables']:
            try:
                del data['errsByTask']
            except:
                pass

    return data


def removeParam(urlquery, parname, mode='complete'):
    """Remove a parameter from current query"""
    urlquery = urlquery.replace('&&', '&')
    urlquery = urlquery.replace('?&', '?')
    pstr = '.*(%s=[a-zA-Z0-9\.\-\_\,\:]*).*' % parname
    pat = re.compile(pstr)
    mat = pat.match(urlquery)
    if mat:
        pstr = mat.group(1)
        urlquery = urlquery.replace(pstr, '')
        urlquery = urlquery.replace('&&', '&')
        urlquery = urlquery.replace('?&', '?')
        if mode != 'extensible':
            if urlquery.endswith('?') or urlquery.endswith('&'): urlquery = urlquery[:len(urlquery) - 1]
    return urlquery

@login_customrequired
def incidentList(request):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "incidents")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('incidents.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'days' in request.session['requestParams']:
        hours = int(request.session['requestParams']['days']) * 24
    else:
        if 'hours' not in request.session['requestParams']:
            hours = 24 * 3
        else:
            hours = int(request.session['requestParams']['hours'])
    setupView(request, hours=hours, limit=9999999)
    iquery = {}
    cloudQuery = Q()
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    iquery['at_time__range'] = [startdate, enddate]
    if 'site' in request.session['requestParams']:
        iquery['description__contains'] = 'queue=%s' % request.session['requestParams']['site']
    if 'category' in request.session['requestParams']:
        iquery['description__startswith'] = '%s:' % request.session['requestParams']['category']
    if 'comment' in request.session['requestParams']:
        iquery['description__contains'] = '%s' % request.session['requestParams']['comment']
    if 'notifier' in request.session['requestParams']:
        iquery['description__contains'] = 'DN=%s' % request.session['requestParams']['notifier']
    if 'cloud' in request.session['requestParams']:
        sites = [site for site, cloud in homeCloud.items() if cloud == request.session['requestParams']['cloud']]
        for site in sites:
            cloudQuery = cloudQuery | Q(description__contains='queue=%s' % site)
    incidents = []
    incidents.extend(Incidents.objects.filter(**iquery).filter(cloudQuery).order_by('at_time').reverse().values())
    sumd = {}
    pars = {}
    incHist = {}
    for inc in incidents:
        desc = inc['description']
        desc = desc.replace('   ', ' ')
        parsmat = re.match('^([a-z\s]+):\s+queue=([^\s]+)\s+DN=(.*)\s\s\s*([A-Za-z^ \.0-9]*)$', desc)
        tm = inc['at_time']
        tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
        if not tm in incHist: incHist[tm] = 0
        incHist[tm] += 1
        if parsmat:
            pars['category'] = parsmat.group(1)
            pars['site'] = parsmat.group(2)
            pars['notifier'] = parsmat.group(3)
            pars['type'] = inc['typekey']
            if pars['site'] in  homeCloud:
                pars['cloud'] = homeCloud[pars['site']]
            if parsmat.group(4): pars['comment'] = parsmat.group(4)
        else:
            parsmat = re.match('^([A-Za-z\s]+):.*$', desc)
            if parsmat:
                pars['category'] = parsmat.group(1)
            else:
                pars['category'] = desc[:10]
        for p in pars:
            if p not in sumd:
                sumd[p] = {}
                sumd[p]['param'] = p
                sumd[p]['vals'] = {}
            if pars[p] not in sumd[p]['vals']:
                sumd[p]['vals'][pars[p]] = {}
                sumd[p]['vals'][pars[p]]['name'] = pars[p]
                sumd[p]['vals'][pars[p]]['count'] = 0
            sumd[p]['vals'][pars[p]]['count'] += 1
        ## convert incident components to URLs. Easier here than in the template.
        if 'site' in pars:
            inc['description'] = re.sub('queue=[^\s]+', 'queue=<a href="%ssite=%s">%s</a>' % (
            extensibleURL(request), pars['site'], pars['site']), inc['description'])
        inc['at_time'] = inc['at_time'].strftime(defaultDatetimeFormat)

    ## convert to ordered lists
    suml = []
    for p in sumd:
        itemd = {}
        itemd['param'] = p
        iteml = []
        kys = sumd[p]['vals'].keys()
        kys.sort(key=lambda y: y.lower())
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[p]['vals'][ky]['count']})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['param'].lower())
    kys = incHist.keys()
    kys = sorted(kys)
    incHistL = []
    for k in kys:
        incHistL.append([k, incHist[k]])

    del request.session['TFIRST']
    del request.session['TLAST']

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'user': None,
            'incidents': incidents,
            'sumd': suml,
            'incHist': incHistL,
            'xurl': extensibleURL(request),
            'hours': hours,
            'ninc': len(incidents),
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        setCacheEntry(request, "incidents", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render_to_response('incidents.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        clearedInc = []
        for inc in incidents:
            entry = {}
            entry['at_time'] = inc['at_time'].isoformat()
            entry['typekey'] = inc['typekey']
            entry['description'] = inc['description']
            clearedInc.append(entry)
        jsonResp = json.dumps(clearedInc)
        return HttpResponse(jsonResp, content_type='application/json')


def esatlasPandaLoggerJson(request):
    valid, response = initRequest(request)
    if not valid: return response
    from elasticsearch import Elasticsearch

    esHost = None
    esPort = None
    esUser = None
    esPassword = None

    if 'esHost' in ES:
        esHost = ES['esHost']
    if 'esPort' in ES:
        esPort = ES['esPort']
    if 'esUser' in ES:
        esUser = ES['esUser']
    if 'esPassword' in ES:
        esPassword = ES['esPassword']

    es = Elasticsearch(
        [{'host': esHost, 'port': int(esPort)}],
        http_auth=(esUser,esPassword),
        use_ssl=True,
        verify_certs=False,
        timeout=30,
        max_retries=10,
        retry_on_timeout=True,
    )
    today = time.strftime("%Y.%m.%d")
    jedi = {}
    logindexjedi = 'atlas_jedilogs-'
    res = es.search(index=logindexjedi + str(today), stored_fields=['jediTaskID', 'type', 'logLevel'], body={
        "aggs": {
            "jediTaskID": {
                "terms": {"field": "jediTaskID", "size": 100000000},
                "aggs": {
                    "type": {
                        "terms": {"field": "fields.type.keyword", "size": 100},
                        "aggs": {
                            "logLevel": {
                                "terms": {"field": "logLevel.keyword"}
                            }
                        }
                    }
                }
            }
        }
    })

    jdListFinal = []
    for agg in res['aggregations']['jediTaskID']['buckets']:
        jdlist = {}
        name = agg['key']
        for types in agg['type']['buckets']:
            jdlist = {}
            type = types['key']
            for levelnames in types['logLevel']['buckets']:
                jdlist = {}
                levelname = levelnames['key']
                jdlist['jediTaskID'] = str(name)
                jdlist['Type'] = type
                jdlist['LevelName'] = levelname
                jdlist['Count'] = levelnames['doc_count']
                jdListFinal.append(jdlist)

    return HttpResponse(json.dumps(jdListFinal), content_type='application/json')


def esatlasPandaLogger(request):
    valid, response = initRequest(request)
    if not valid: return response
    from elasticsearch import Elasticsearch
    esHost = None
    esPort = None
    esUser = None
    esPassword = None

    if 'esHost' in ES:
        esHost = ES['esHost']
    if 'esPort' in ES:
        esPort = ES['esPort']
    if 'esUser' in ES:
        esUser = ES['esUser']
    if 'esPassword' in ES:
        esPassword = ES['esPassword']

    es = Elasticsearch(
        [{'host': esHost, 'port': int(esPort)}],
        http_auth=(esUser,esPassword),
        use_ssl=True,
        verify_certs=False,
        timeout=30,
        max_retries=10,
        retry_on_timeout=True,
    )
    today = time.strftime("%Y.%m.%d")
    pandaDesc = {
        "panda.log.RetrialModule":["cat1","Retry module to apply rules on failed jobs"],

        "panda.log.Serveraccess":["cat2","Apache request log"],
        "panda.log.Servererror":["cat2","Apache errors"],
        "panda.log.PilotRequests": ["cat2", "Pilot requests"],
        "panda.log.Entry":["cat2","Entry point to the PanDA server"],
        "panda.log.UserIF": ["cat2", "User interface"],
        "panda.log.DBProxy": ["cat2", "Filtered messages of DB interactions"],



        "panda.log.Adder": ["cat3", "Add output files to datasets and trigger output aggregation"],
        "panda.log.Finisher": ["cat3", "Finalization procedures for jobs"],
        "panda.log.Closer": ["cat3", "Close internal datasets once all associated jobs are done"],
        "panda.log.Setupper": ["cat3", "Setup internal datasets for data transfers"],
        "panda.log.copyArchive": ["cat3", "Various actions, such as kill and poll based on timeout parameters"],
        "panda.log.DynDataDistributer": ["cat3", "PD2P"],
        "panda.log.Activator": ["cat3", "Activates jobs based on input transfers"],
        "panda.log.datasetManager": ["cat3", "Manages datasets states"],
        "panda.log.Watcher": ["cat3", "Watchdog for jobs, e.g. lost heartbeat"],

        "panda.log.broker": ["cat4", "Brokerage"],
        "panda.log.runRebro": ["cat4", "Identifies jobs to rebroker"],
        "panda.log.prioryMassage": ["cat4", "Priority management for user jobs"],

        "panda.log.Initializer": ["cat8", "Initializes connections to the DB"],
        "panda.log.ConBridge": ["cat5", "DB connections"],

        "panda.log.ProcessLimiter": ["cat6", "Limit number of forked processes in PanDA"],

        "panda.log.Utils": ["cat8", "Aux functions"],
        "panda.log.Notifier": ["cat7", "User email notification agent"],

        "panda.log.Panda": ["cat8", "Some messages are redirected here"],
    }
    pandaCat = ['cat1','cat2','cat3','cat4','cat5','cat6','cat7','cat8']

    jediDesc = {
        "panda.log.AtlasProdTaskBroker":["cat1","Production task brokerage"],
        "panda.log.TaskBroker":["cat7","Task brokerage factory"],
        "panda.log.AtlasProdJobBroker":["cat1","Production job brokerage"],
        "panda.log.AtlasAnalJobBroker": ["cat1", "Analysis job brokerage"],
        "panda.log.JobBroker":["cat7","Job brokerage factory"],


        "panda.log.AtlasProdJobThrottler": ["cat2", "Throttles generation of production jobs based on defined limits"],
        "panda.log.JobThrottler": ["cat7", "Job throttler factory"],
        "panda.log.JobGenerator": ["cat2", "Generates job for a task"],
        "panda.log.JobSplitter": ["cat2", "Job splitter, used by the job generator"],

        "panda.log.TaskRefiner": ["cat3", "Generates tasks in JEDI from definitions found in DEFT"],
        "panda.log.TaskSetupper": ["cat7", "Procedures for task setup. Base class"],
        "panda.log.AtlasTaskSetupper": ["cat3", "ATLAS procedures for task setup"],
        "panda.log.TaskCommando": ["cat3", "Executes task commands from DEFT"],
        "panda.log.PostProcessor": ["cat3", "Post processes tasks"],

        "panda.log.Activator": ["cat4", "Activates jobs based on DDM messages"],
        "panda.log.Closer": ["cat4", "Closes internal datasets once all associated jobs are done"],
        "panda.log.ContentsFeeder": ["cat4", "Feeds file contents for tasks"],
        "panda.log.AtlasDDMClient": ["cat4", "DDM client"],

        "panda.log.AtlasProdWatchDog": ["cat5", "Production task watchdog"],
        "panda.log.AtlasAnalWatchDog": ["cat5", "Analysis task watchdog"],
        "panda.log.WatchDog": ["cat5", "Watchdog"],

        "panda.log.JediDBProxy": ["cat6", "Filtered JEDI DB interactions"],
        "panda.log.TaskBuffer": ["cat7", "PanDA server task buffer"],
        "panda.log.JediTaskBuffer": ["cat7", "JEDI task buffer"],
        "panda.log.DBProxyPool": ["cat7", "DB connection pool interactions"],
    }
    jediCat = ['cat1','cat2','cat3','cat4','cat5','cat6','cat7']

    logindexpanda = 'atlas_pandalogs-'
    logindexjedi = 'atlas_jedilogs-'

    indices = [logindexpanda,logindexjedi]

    panda = {}
    jedi = {}
    for index in indices:
        res = es.search(index=index + str(today), stored_fields=['logName', 'type', 'logLevel'], body={
            "aggs": {
                "logName": {
                    "terms": {"field": "logName.keyword","size": 100},
                    "aggs": {
                        "type": {
                            "terms": {"field": "fields.type.keyword","size": 100},
                            "aggs": {
                                "logLevel": {
                                    "terms": {"field": "logLevel.keyword"}
                                }
                            }
                        }
                 }
                }
            }
        }
        )

        if index == "atlas_pandalogs-":
            for cat in pandaCat:
                panda[cat]={}
            for agg in res['aggregations']['logName']['buckets']:
                if agg['key'] not in pandaDesc:
                    pandaDesc[agg['key']] = [panda.keys()[-1], "New log type. No description"]
                cat = pandaDesc[agg['key']][0]
                    #desc = pandaDesc[agg['key']][1]
                name = agg['key']
                panda[cat][name] = {}
                #panda[cat][name]['desc'] = pandaDesc[agg['key']][1]
                for types in agg['type']['buckets']:
                    type = types['key']
                    panda[cat][name][type] = {}
                    for levelnames in types['logLevel']['buckets']:
                        levelname = levelnames['key']
                        panda[cat][name][type][levelname] = {}
                        panda[cat][name][type][levelname]['logLevel'] = levelname
                        panda[cat][name][type][levelname]['lcount'] = str(levelnames['doc_count'])
        elif index == "atlas_jedilogs-":
            for cat in jediCat:
                jedi[cat]={}
            for agg in res['aggregations']['logName']['buckets']:
                if agg['key'] not in jediDesc:
                    jediDesc[agg['key']] = [jedi.keys()[-1], "New log type. No description"]
                cat = jediDesc[agg['key']][0]
                name = agg['key']
                jedi[cat][name] = {}
                for types in agg['type']['buckets']:
                    type = types['key']
                    jedi[cat][name][type] = {}
                    for levelnames in types['logLevel']['buckets']:
                        levelname = levelnames['key']
                        jedi[cat][name][type][levelname] = {}
                        jedi[cat][name][type][levelname]['logLevel'] = levelname
                        jedi[cat][name][type][levelname]['lcount'] = str(levelnames['doc_count'])
    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'user': None,
        'panda': panda,
        'pandadesc':pandaDesc,
        'jedi': jedi,
        'jedidesc':jediDesc,
        'time': time.strftime("%Y-%m-%d"),
    }

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        response = render_to_response('esatlasPandaLogger.html', data, content_type='text/html')
        return response

def esPandaLogger(request):
    valid, response = initRequest(request)
    if not valid: return response
    from elasticsearch import Elasticsearch

    es = Elasticsearch(
        hosts=[{'host': 'aianalytics01.cern.ch', 'port': 9200}],
        use_ssl=False,
        retry_on_timeout=True,
        max_retries=3
    )

    today = time.strftime("%Y-%m-%d")
    logindex = 'pandalogger-' + str(today)
    logindexdev = 'pandaloggerdev-' + str(today)

    # check if dev index exists
    indexdev = es.indices.exists(index=logindexdev)

    if indexdev:
        indices = [logindex, logindexdev]
    else:
        indices = [logindex]
    res = es.search(index=indices, stored_fields=['@message.name', '@message.Type', '@message.levelname'], body={
        "aggs": {
            "name": {
                "terms": {"field": "@message.name"},
                "aggs": {
                    "type": {
                        "terms": {"field": "@message.Type"},
                        "aggs": {
                            "levelname": {
                                "terms": {"field": "@message.levelname"}
                            }
                        }
                    }
                }
            }
        }
    }
                    )

    log = {}
    for agg in res['aggregations']['name']['buckets']:
        name = agg['key']
        log[name] = {}
        for types in agg['type']['buckets']:
            type = types['key']
            log[name][type] = {}
            for levelnames in types['levelname']['buckets']:
                levelname = levelnames['key']
                log[name][type][levelname] = {}
                log[name][type][levelname]['levelname'] = levelname
                log[name][type][levelname]['lcount'] = str(levelnames['doc_count'])
    # print log
    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'user': None,
        'log': log,
    }

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        response = render_to_response('esPandaLogger.html', data, content_type='text/html')
        return response


def pandaLogger(request):
    valid, response = initRequest(request)
    if not valid: return response
    getrecs = False
    iquery = {}
    if 'category' in request.session['requestParams']:
        iquery['name'] = request.session['requestParams']['category']
        getrecs = True
    if 'type' in request.session['requestParams']:
        val = escapeInput(request.session['requestParams']['type'])
        iquery['type__in'] = val.split('|')
        getrecs = True
    if 'level' in request.session['requestParams']:
        iquery['levelname'] = request.session['requestParams']['level'].upper()
        getrecs = True
    if 'taskid' in request.session['requestParams']:
        iquery['message__startswith'] = request.session['requestParams']['taskid']
        getrecs = True
    if 'jeditaskid' in request.session['requestParams']:
        iquery['message__icontains'] = "jeditaskid=%s" % request.session['requestParams']['jeditaskid']
        getrecs = True
    if 'site' in request.session['requestParams']:
        iquery['message__icontains'] = "site=%s " % request.session['requestParams']['site']
        getrecs = True
    if 'pandaid' in request.session['requestParams']:
        iquery['pid'] = request.session['requestParams']['pandaid']
        getrecs = True
    if 'hours' not in request.session['requestParams']:
        if getrecs:
            hours = 72
        else:
            hours = 24
    else:
        hours = int(request.session['requestParams']['hours'])
    setupView(request, hours=hours, limit=9999999)

    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    if 'startdate' in request.session['requestParams'] and len(request.session['requestParams']['startdate']) > 1:
        startdate = request.session['requestParams']['startdate']

    enddate = timezone.now().strftime(defaultDatetimeFormat)
    if 'enddate' in request.session['requestParams'] and len(request.session['requestParams']['startdate']) > 1:
        enddate = request.session['requestParams']['enddate']

    iquery['bintime__range'] = [startdate, enddate]
    print (iquery)
    counts = Pandalog.objects.filter(**iquery).values('name', 'type', 'levelname').annotate(
        Count('levelname')).order_by('name', 'type', 'levelname')
    if getrecs:
        records = Pandalog.objects.filter(**iquery).order_by('bintime').reverse()[
                  :request.session['JOB_LIMIT']].values()
        ## histogram of logs vs. time, for plotting
        logHist = {}
        for r in records:
            r['message'] = r['message'].replace('<', '')
            r['message'] = r['message'].replace('>', '')
            r['levelname'] = r['levelname'].lower()
            tm = r['bintime']
            tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
            if not tm in logHist: logHist[tm] = 0
            logHist[tm] += 1
        kys = logHist.keys()
        kys = sorted(kys)
        logHistL = []
        for k in kys:
            logHistL.append([k, logHist[k]])
    else:
        records = None
        logHistL = None
    logs = {}
    totcount = 0
    for inc in counts:
        name = inc['name']
        type = inc['type']
        level = inc['levelname']
        count = inc['levelname__count']
        totcount += count
        if name not in logs:
            logs[name] = {}
            logs[name]['name'] = name
            logs[name]['count'] = 0
            logs[name]['types'] = {}
        logs[name]['count'] += count
        if type not in logs[name]['types']:
            logs[name]['types'][type] = {}
            logs[name]['types'][type]['name'] = type
            logs[name]['types'][type]['count'] = 0
            logs[name]['types'][type]['levels'] = {}
        logs[name]['types'][type]['count'] += count
        if level not in logs[name]['types'][type]['levels']:
            logs[name]['types'][type]['levels'][level] = {}
            logs[name]['types'][type]['levels'][level]['name'] = level.lower()
            logs[name]['types'][type]['levels'][level]['count'] = 0
        logs[name]['types'][type]['levels'][level]['count'] += count

    ## convert to ordered lists
    logl = []
    for l in logs:
        itemd = {}
        itemd['name'] = logs[l]['name']
        itemd['types'] = []
        for t in logs[l]['types']:
            logs[l]['types'][t]['levellist'] = []
            for v in logs[l]['types'][t]['levels']:
                logs[l]['types'][t]['levellist'].append(logs[l]['types'][t]['levels'][v])
            logs[l]['types'][t]['levellist'] = sorted(logs[l]['types'][t]['levellist'], key=lambda x: x['name'])
            typed = {}
            typed['name'] = logs[l]['types'][t]['name']
            itemd['types'].append(logs[l]['types'][t])
        itemd['types'] = sorted(itemd['types'], key=lambda x: x['name'])
        logl.append(itemd)
    logl = sorted(logl, key=lambda x: x['name'])

    del request.session['TFIRST']
    del request.session['TLAST']
    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'user': None,
        'logl': logl,
        'records': records,
        'ninc': totcount,
        'logHist': logHistL,
        'xurl': extensibleURL(request),
        'hours': hours,
        'getrecs': getrecs,
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        response = render_to_response('pandaLogger.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        resp = data
        return HttpResponse(json.dumps(resp, cls=DateEncoder), content_type='application/json')


# def percentile(N, percent, key=lambda x:x):
#     """
#     Find the percentile of a list of values.
#
#     @parameter N - is a list of values. Note N MUST BE already sorted.
#     @parameter percent - a float value from 0.0 to 1.0.
#     @parameter key - optional key function to compute value from each element of N.
#
#     @return - the percentile of the values
#     """
#     if not N:
#         return None
#     k = (len(N)-1) * percent
#     f = math.floor(k)
#     c = math.ceil(k)
#     if f == c:
#         return key(N[int(k)])
#     d0 = key(N[int(f)]) * (c-k)
#     d1 = key(N[int(c)]) * (k-f)
#     return d0+d1


def ttc(request):
    valid, response = initRequest(request)
    if not valid: return response
    data = {}

    jeditaskid = -1
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == -1:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    query = {'jeditaskid': jeditaskid}
    task = JediTasks.objects.filter(**query).values('jeditaskid', 'taskname', 'workinggroup', 'tasktype',
                                                    'processingtype', 'ttcrequested', 'starttime', 'endtime',
                                                    'creationdate', 'status')
    if len(task) == 0:
        data = {"error": ("jeditaskid " + str(jeditaskid) + " does not exist")}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
    taskrec = task[0]

    if taskrec['tasktype'] != 'prod' or taskrec['ttcrequested'] == None:
        data = {"error": "TTC for this type of task has not implemented yet"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    if taskrec['ttcrequested']:
        taskrec['ttc'] = taskrec['ttcrequested']

    taskevents = GetEventsForTask.objects.filter(**query).values('jeditaskid', 'totev', 'totevrem')

    taskev = None
    if len(taskevents) > 0:
        taskev = taskevents[0]
    cur = connection.cursor()
    cur.execute("SELECT * FROM table(ATLAS_PANDABIGMON.GETTASKPROFILE('%s'))" % taskrec['jeditaskid'])
    taskprofiled = cur.fetchall()
    cur.close()

    keys = ['endtime', 'starttime', 'nevents', 'njob']
    taskprofile = [{'endtime': taskrec['starttime'], 'starttime': taskrec['starttime'], 'nevents': 0, 'njob': 0}]
    taskprofile = taskprofile + [dict(zip(keys, row)) for row in taskprofiled]
    maxt = (taskrec['ttc'] - taskrec['starttime']).days * 3600 * 24 + (taskrec['ttc'] - taskrec['starttime']).seconds
    neventsSum = 0
    for job in taskprofile:
        job['ttccoldline'] = 100. - ((job['endtime'] - taskrec['starttime']).days * 3600 * 24 + (
        job['endtime'] - taskrec['starttime']).seconds) * 100 / float(maxt)
        job['endtime'] = job['endtime'].strftime("%Y-%m-%d %H:%M:%S")
        job['ttctime'] = job['endtime']
        job['starttime'] = job['starttime'].strftime("%Y-%m-%d %H:%M:%S")
        neventsSum += job['nevents']
        if taskev and taskev['totev'] > 0:
            job['tobedonepct'] = 100. - neventsSum * 100. / taskev['totev']
        else:
            job['tobedonepct'] = None
    taskprofile.insert(len(taskprofile), {'endtime': taskprofile[len(taskprofile) - 1]['endtime'],
                                          'starttime': taskprofile[len(taskprofile) - 1]['starttime'],
                                          'ttctime': taskrec['ttc'].strftime("%Y-%m-%d %H:%M:%S"),
                                          'tobedonepct': taskprofile[len(taskprofile) - 1]['tobedonepct'],
                                          'ttccoldline': 0})

    progressForBar = []
    if taskev['totev'] > 0:
        taskrec['percentage'] = ((neventsSum) * 100 / taskev['totev'])
    else:  taskrec['percentage'] = None
    if taskrec['percentage'] != None:
     taskrec['percentageok'] = taskrec['percentage'] - 5
    else:  taskrec['percentageok'] = None
    if taskrec['status'] == 'running':
        taskrec['ttcbasedpercentage'] = ((datetime.now() - taskrec['starttime']).days * 24 * 3600 + (
        datetime.now() - taskrec['starttime']).seconds) * 100 / (
                                        (taskrec['ttcrequested'] - taskrec['creationdate']).days * 24 * 3600 + (
                                        taskrec['ttcrequested'] - taskrec['creationdate']).seconds) if datetime.now() < \
                                                                                                       taskrec[
                                                                                                           'ttc'] else 100
        progressForBar = [100, taskrec['percentage'], taskrec['ttcbasedpercentage']]

    if taskrec['ttc']:
        taskrec['ttc'] = taskrec['ttc'].strftime(defaultDatetimeFormat)
    if taskrec['creationdate']:
        taskrec['creationdate'] = taskrec['creationdate'].strftime(defaultDatetimeFormat)
    if taskrec['starttime']:
        taskrec['starttime'] = taskrec['starttime'].strftime(defaultDatetimeFormat)
    if taskrec['endtime']:
        taskrec['endtime'] = taskrec['endtime'].strftime(defaultDatetimeFormat)

    data = {
        'request': request,
        'task': taskrec,
        'progressForBar': progressForBar,
        'profile': taskprofile,
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    response = render_to_response('ttc.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


#@cache_page(60 * 20)
@login_customrequired
def workingGroups(request):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "workingGroups")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('workingGroups.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    taskdays = 3
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        days = 30
    else:
        days = taskdays
    hours = days * 24
    query = setupView(request, hours=hours, limit=999999)
    query['workinggroup__isnull'] = False

    ## WG task summary
    tasksummary = wgTaskSummary(request, view='working group', taskdays=taskdays)

    ## WG job summary
    if 'workinggroup' in request.session['requestParams'] and request.session['requestParams']['workinggroup']:
        query['workinggroup'] = request.session['requestParams']['workinggroup']
    wgsummarydata = wgSummary(query)
    wgs = {}
    for rec in wgsummarydata:
        wg = rec['workinggroup']
        if wg == None: continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if wg not in wgs:
            wgs[wg] = {}
            wgs[wg]['name'] = wg
            wgs[wg]['count'] = 0
            wgs[wg]['states'] = {}
            wgs[wg]['statelist'] = []
            for state in statelist:
                wgs[wg]['states'][state] = {}
                wgs[wg]['states'][state]['name'] = state
                wgs[wg]['states'][state]['count'] = 0
        wgs[wg]['count'] += count
        wgs[wg]['states'][jobstatus]['count'] += count

    errthreshold = 15
    ## Convert dict to summary list
    wgkeys = wgs.keys()
    wgkeys = sorted(wgkeys)
    wgsummary = []
    for wg in wgkeys:
        for state in statelist:
            wgs[wg]['statelist'].append(wgs[wg]['states'][state])
            if int(wgs[wg]['states']['finished']['count']) + int(wgs[wg]['states']['failed']['count']) > 0:
                wgs[wg]['pctfail'] = int(100. * float(wgs[wg]['states']['failed']['count']) / (
                wgs[wg]['states']['finished']['count'] + wgs[wg]['states']['failed']['count']))

        wgsummary.append(wgs[wg])
    if len(wgsummary) == 0: wgsummary = None

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'xurl': xurl,
            'user': None,
            'wgsummary': wgsummary,
            'taskstates': taskstatedict,
            'tasksummary': tasksummary,
            'hours': hours,
            'days': days,
            'errthreshold': errthreshold,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        setCacheEntry(request, "workingGroups", json.dumps(data, cls=DateEncoder), 60 * 20)

        response = render_to_response('workingGroups.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        resp = []
        return HttpResponse(json.dumps(resp), content_type='application/json')


@login_customrequired
def datasetInfo(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    dsets = []
    dsrec = None
    colnames = []
    columns = []
    if 'datasetname' in request.session['requestParams']:
        dataset = request.session['requestParams']['datasetname']
        query['datasetname'] = request.session['requestParams']['datasetname']
    elif 'datasetid' in request.session['requestParams']:
        dataset = request.session['requestParams']['datasetid']
        query['datasetid'] = request.session['requestParams']['datasetid']
    else:
        dataset = None

    if 'jeditaskid' in request.session['requestParams']:
        query['jeditaskid'] = int(request.session['requestParams']['jeditaskid'])

    if dataset:
        dsets = JediDatasets.objects.filter(**query).values()
        if len(dsets) == 0:
            startdate = timezone.now() - timedelta(hours=30 * 24)
            startdate = startdate.strftime(defaultDatetimeFormat)
            enddate = timezone.now().strftime(defaultDatetimeFormat)
            query = {'modificationdate__range': [startdate, enddate]}
            if 'datasetname' in request.session['requestParams']:
                query['name'] = request.session['requestParams']['datasetname']
            elif 'datasetid' in request.session['requestParams']:
                query['vuid'] = request.session['requestParams']['datasetid']
            moredsets = Datasets.objects.filter(**query).values()
            if len(moredsets) > 0:
                dsets = moredsets
                for ds in dsets:
                    ds['datasetname'] = ds['name']
                    ds['creationtime'] = ds['creationdate'].strftime(defaultDatetimeFormat)
                    ds['modificationtime'] = ds['modificationdate'].strftime(defaultDatetimeFormat)
                    ds['nfiles'] = ds['numberfiles']
                    ds['datasetid'] = ds['vuid']
    if len(dsets) > 0:
        dsrec = dsets[0]
        dataset = dsrec['datasetname']
        colnames = dsrec.keys()
        colnames = sorted(colnames)
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = dsrec[k].strftime(defaultDatetimeFormat)
                except:
                    val = dsrec[k]
            else:
                val = dsrec[k]
            if dsrec[k] == None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
    del request.session['TFIRST']
    del request.session['TLAST']

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'dsrec': dsrec,
            'datasetname': dataset,
            'columns': columns,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('datasetInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(dsrec, cls=DateEncoder), content_type='application/json')

@login_customrequired
def datasetList(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    dsets = []
    for par in ('jeditaskid', 'containername'):
        if par in request.session['requestParams']:
            query[par] = request.session['requestParams'][par]

    if len(query) > 0:
        dsets = JediDatasets.objects.filter(**query).values()
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())

    del request.session['TFIRST']
    del request.session['TLAST']
    for ds in dsets:
        ds['creationtime'] = ds['creationtime'].strftime(defaultDatetimeFormat)
        ds['modificationtime'] = ds['modificationtime'].strftime(defaultDatetimeFormat)
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        data = {
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'datasets': dsets,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('datasetList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(dsets), content_type='application/json')

@login_customrequired
def fileInfo(request):
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        JediDatasetsTableName = "ATLAS_PANDA.JEDI_DATASETS"
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        JediDatasetsTableName = "JEDI_DATASETS"
        tmpTableName = "TMP_IDS1"

    random.seed()
    transactionKey = random.randrange(1000000)

    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    files = []
    frec = None
    colnames = []
    columns = []
    if 'filename' in request.session['requestParams']:
        file = request.session['requestParams']['filename']
        query['lfn'] = request.session['requestParams']['filename']
    elif 'lfn' in request.session['requestParams']:
        file = request.session['requestParams']['lfn']
        query['lfn'] = request.session['requestParams']['lfn']
    elif 'fileid' in request.session['requestParams']:
        file = request.session['requestParams']['fileid']
        query['fileid'] = request.session['requestParams']['fileid']
    elif 'guid' in request.session['requestParams']:
        file = request.session['requestParams']['guid']
        query['guid'] = request.session['requestParams']['guid']
    else:
        file = None

    startdate = None
    if 'date_from' in request.session['requestParams']:
        time_from_struct = time.strptime(request.session['requestParams']['date_from'], '%Y-%m-%d')
        startdate = datetime.utcfromtimestamp(time.mktime(time_from_struct))
    if not startdate:
        startdate = timezone.now() - timedelta(hours=365 * 24)
    # startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = None
    if 'date_to' in request.session['requestParams']:
        time_from_struct = time.strptime(request.session['requestParams']['date_to'], '%Y-%m-%d')
        enddate = datetime.utcfromtimestamp(time.mktime(time_from_struct))
    if enddate == None:
        enddate = timezone.now()  # .strftime(defaultDatetimeFormat)

    query['creationdate__range'] = [startdate.strftime(defaultDatetimeFormat), enddate.strftime(defaultDatetimeFormat)]

    if 'pandaid' in request.session['requestParams'] and request.session['requestParams']['pandaid'] != '':
        query['pandaid'] = request.session['requestParams']['pandaid']
    if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid'] != '':
        query['jeditaskid'] = request.session['requestParams']['jeditaskid']
    if 'scope' in request.session['requestParams']:
        query['scope'] = request.session['requestParams']['scope']
    if 'datasetid' in request.session['requestParams']:
        query['datasetid'] = request.session['requestParams']['datasetid']

    if file or ('pandaid' in query and query['pandaid'] is not None) or ('jeditaskid' in query and query['jeditaskid'] is not None):
        files = JediDatasetContents.objects.filter(**query).values()
        if len(files) == 0:
            del query['creationdate__range']
            query['modificationtime__castdate__range'] = [startdate.strftime(defaultDatetimeFormat),
                                                enddate.strftime(defaultDatetimeFormat)]
            morefiles = Filestable4.objects.filter(**query).values()
            if len(morefiles) == 0:
                morefiles = FilestableArch.objects.filter(**query).values()
            if len(morefiles) > 0:
                files = morefiles
                for f in files:
                    f['creationdate'] = f['modificationtime']
                    f['fileid'] = f['row_id']
                    f['datasetname'] = f['dataset']
                    f['oldfiletable'] = 1

#        connection.enter_transaction_management()
        new_cur = connection.cursor()
        executionData = []
        for id in files:
            executionData.append((id['datasetid'], transactionKey))
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, executionData)
#        connection.commit()


        new_cur.execute(
            "SELECT DATASETNAME,DATASETID FROM %s WHERE DATASETID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (
                JediDatasetsTableName, tmpTableName, transactionKey))
        mrecs = dictfetchall(new_cur)
        mrecsDict = {}
        for mrec in mrecs:
            mrecsDict[mrec['DATASETID']] = mrec['DATASETNAME']

        for f in files:
            f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
            if 'datasetid' in f and f['datasetid'] in mrecsDict and mrecsDict[f['datasetid']]:
                f['datasetname'] = mrecsDict[f['datasetid']]

    if len(files) > 0:

        files = sorted(files, key=lambda x: x['pandaid'] if x['pandaid'] is not None else False, reverse=True)
        frec = files[0]
        file = frec['lfn']
        colnames = frec.keys()
        colnames = sorted(colnames)
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = frec[k].strftime(defaultDatetimeFormat)
                except:
                    val = frec[k]
            else:
                val = frec[k]
            if frec[k] == None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
    del request.session['TFIRST']
    del request.session['TLAST']

    for file_ in files:
        if 'startevent' in file_:
            if (file_['startevent'] != None):
                file_['startevent'] += 1
        if 'endevent' in file_:
            if (file_['endevent'] != None):
                file_['endevent'] += 1

    if ((len(files) > 0) and ('jeditaskid' in files[0]) and ('startevent' in files[0]) and (
        files[0]['jeditaskid'] != None)):
        files = sorted(files, key=lambda k: (-k['jeditaskid'], k['startevent']))
    if frec and 'creationdate' in frec and frec['creationdate'] is None:
        frec['creationdate'] = frec['creationdate'].strftime(defaultDatetimeFormat)
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'frec': frec,
            'files': files,
            'filename': file,
            'columns': columns,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('fileInfo.html', data, RequestContext(request))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        data = {
            'frec': frec,
            'files': files,
            'filename': file,
            'columns': columns,
        }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

@login_customrequired
def fileList(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    files = []
    defaultlimit = 1000
    frec = None
    colnames = []
    columns = []
    datasetname = ''
    datasetid = 0

    #### It's dangerous when dataset name is not unique over table
    if 'datasetname' in request.session['requestParams']:
        datasetname = request.session['requestParams']['datasetname']
        dsets = JediDatasets.objects.filter(datasetname=datasetname).values()
        if len(dsets) > 0:
            datasetid = dsets[0]['datasetid']
    elif 'datasetid' in request.session['requestParams']:
        datasetid = request.session['requestParams']['datasetid']
        dsets = JediDatasets.objects.filter(datasetid=datasetid).values()
        if len(dsets) > 0:
            datasetname = dsets[0]['datasetname']

    extraparams = ''
    if 'procstatus' in request.session['requestParams'] and request.session['requestParams']['procstatus']:
        query['procstatus'] = request.session['requestParams']['procstatus']
        extraparams += '&procstatus=' + request.session['requestParams']['procstatus']

    if int(datasetid) > 0:
        query['datasetid'] = datasetid
        nfilestotal = JediDatasetContents.objects.filter(**query).count()
        nfilesunique = JediDatasetContents.objects.filter(**query).values('lfn').distinct().count()

    del request.session['TFIRST']
    del request.session['TLAST']
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        xurl = extensibleURL(request)
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'limit': defaultlimit,
            'datasetid': datasetid,
            'nfilestotal': nfilestotal,
            'nfilesunique': nfilesunique,
            'extraparams': extraparams,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('fileList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(files), content_type='application/json')


def loadFileList(request, datasetid=-1):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    files = []
    limit = 1000
    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])

    if 'procstatus' in request.session['requestParams'] and request.session['requestParams']['procstatus']:
        query['procstatus'] = request.session['requestParams']['procstatus']


    sortOrder = 'lfn'

    if int(datasetid) > 0:
        query['datasetid'] = datasetid
        files.extend(JediDatasetContents.objects.filter(**query).values().order_by(sortOrder)[:limit])

    pandaids = []
    for f in files:
        pandaids.append(f['pandaid'])

    query = {}
    filesFromFileTable = []
    filesFromFileTableDict = {}
    query['pandaid__in'] = pandaids
    # JEDITASKID, DATASETID, FILEID
    filesFromFileTable.extend(
        Filestable4.objects.filter(**query).values('fileid', 'dispatchdblock', 'scope', 'destinationdblock'))
    if len(filesFromFileTable) == 0:
        filesFromFileTable.extend(
            FilestableArch.objects.filter(**query).values('fileid', 'dispatchdblock', 'scope', 'destinationdblock'))
    if len(filesFromFileTable) > 0:
        for f in filesFromFileTable:
            filesFromFileTableDict[f['fileid']] = f

    ## Count the number of distinct files
    filed = {}
    for f in files:
        filed[f['lfn']] = 1
        f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
        ruciolink = ""
        if f['fileid'] in filesFromFileTableDict:
            if len(filesFromFileTableDict[f['fileid']]['dispatchdblock']) > 0:
                ruciolink = 'https://rucio-ui.cern.ch/did?scope=' + filesFromFileTableDict[f['fileid']][
                        'scope'] + '&name=' + filesFromFileTableDict[f['fileid']]['dispatchdblock']
            else:
                if len(filesFromFileTableDict[f['fileid']]['destinationdblock']) > 0:
                    ruciolink = 'https://rucio-ui.cern.ch/did?scope=' + filesFromFileTableDict[f['fileid']][
                        'scope'] + '&name=' + filesFromFileTableDict[f['fileid']]['destinationdblock']
        f['ruciolink'] = ruciolink
        f['creationdatecut'] = f['creationdate'].strftime('%Y-%m-%d')
        f['creationdate'] = f['creationdate'].strftime(defaultDatetimeFormat)

    dump = json.dumps(files, cls=DateEncoder)
    return HttpResponse(dump, content_type='application/json')

@login_customrequired
def workQueues(request):
    valid, response = initRequest(request)
    data = getCacheEntry(request, "workQueues")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('workQueues.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    if not valid: return response
    setupView(request, hours=180 * 24, limit=9999999)
    query = {}
    for param in request.session['requestParams']:
        for field in JediWorkQueue._meta.get_fields():
            if param == field.name:
                query[param] = request.session['requestParams'][param]
    queues = []
    queues.extend(JediWorkQueue.objects.filter(**query).order_by('queue_type', 'queue_order').values())

    del request.session['TFIRST']
    del request.session['TLAST']
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'queues': queues,
            'xurl': extensibleURL(request),
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        response = render_to_response('workQueues.html', data, content_type='text/html')
        setCacheEntry(request, "workQueues", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(queues), content_type='application/json')


def stateNotUpdated(request, state='transferring', hoursSinceUpdate=36, values=standard_fields, count=False,
                    wildCardExtension='(1=1)'):
    valid, response = initRequest(request)
    if not valid: return response
    query = setupView(request, opmode='notime', limit=99999999)
    if 'jobstatus' in request.session['requestParams']: state = request.session['requestParams']['jobstatus']
    if 'transferringnotupdated' in request.session['requestParams']: hoursSinceUpdate = int(
        request.session['requestParams']['transferringnotupdated'])
    if 'statenotupdated' in request.session['requestParams']: hoursSinceUpdate = int(
        request.session['requestParams']['statenotupdated'])
    moddate = timezone.now() - timedelta(hours=hoursSinceUpdate)
    moddate = moddate.strftime(defaultDatetimeFormat)
    mindate = timezone.now() - timedelta(hours=24 * 30)
    mindate = mindate.strftime(defaultDatetimeFormat)
    query['statechangetime__lte'] = moddate
    # query['statechangetime__gte'] = mindate
    query['jobstatus'] = state
    if count:
        jobs = []
        jobs.extend(
            Jobsactive4.objects.filter(**query).extra(where=[wildCardExtension]).values('cloud', 'computingsite',
                                                                                        'jobstatus').annotate(
                Count('jobstatus')))
        jobs.extend(
            Jobsdefined4.objects.filter(**query).extra(where=[wildCardExtension]).values('cloud', 'computingsite',
                                                                                         'jobstatus').annotate(
                Count('jobstatus')))
        jobs.extend(
            Jobswaiting4.objects.filter(**query).extra(where=[wildCardExtension]).values('cloud', 'computingsite',
                                                                                         'jobstatus').annotate(
                Count('jobstatus')))
        ncount = 0
        perCloud = {}
        perRCloud = {}
        for cloud in cloudList:
            perCloud[cloud] = 0
            perRCloud[cloud] = 0
        for job in jobs:
            site = job['computingsite']
            if site in homeCloud:
                cloud = homeCloud[site]
                if not cloud in perCloud:
                    perCloud[cloud] = 0
                perCloud[cloud] += job['jobstatus__count']
            cloud = job['cloud']
            if not cloud in perRCloud:
                perRCloud[cloud] = 0
            perRCloud[cloud] += job['jobstatus__count']
            ncount += job['jobstatus__count']
        perCloudl = []
        for c in perCloud:
            pcd = {'name': c, 'count': perCloud[c]}
            perCloudl.append(pcd)
        perCloudl = sorted(perCloudl, key=lambda x: x['name'])
        perRCloudl = []
        for c in perRCloud:
            pcd = {'name': c, 'count': perRCloud[c]}
            perRCloudl.append(pcd)
        perRCloudl = sorted(perRCloudl, key=lambda x: x['name'])
        return ncount, perCloudl, perRCloudl
    else:
        jobs = []
        jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[wildCardExtension]).values(*values))
        jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[wildCardExtension]).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[wildCardExtension]).values(*values))
        return jobs


def taskNotUpdated(request, query, state='submitted', hoursSinceUpdate=36, values=[], count=False,
                   wildCardExtension='(1=1)'):
    valid, response = initRequest(request)
    if not valid: return response
    # query = setupView(request, opmode='notime', limit=99999999)
    if 'status' in request.session['requestParams']: state = request.session['requestParams']['status']
    if 'statenotupdated' in request.session['requestParams']: hoursSinceUpdate = int(
        request.session['requestParams']['statenotupdated'])
    moddate = timezone.now() - timedelta(hours=hoursSinceUpdate)
    moddate = moddate.strftime(defaultDatetimeFormat)
    mindate = timezone.now() - timedelta(hours=24 * 30)
    mindate = mindate.strftime(defaultDatetimeFormat)
    query['statechangetime__lte'] = moddate
    # query['statechangetime__gte'] = mindate
    query['status'] = state
    job = ''
    if count:
        tasks = JediTasks.objects.filter(**query).extra(where=[wildCardExtension]).values('name', 'status').annotate(
            Count('status'))
        statecounts = {}
        for s in taskstatelist:
            statecounts[s] = {}
            statecounts[s]['count'] = 0
            statecounts[s]['name'] = s
        ncount = 0
        for task in tasks:
            state = task['status']
            statecounts[state]['count'] += task['status__count']
            ncount += job['status__count']
        return ncount, statecounts
    else:
        tasks = JediTasks.objects.filter(**query).extra(where=[wildCardExtension]).values()
        return tasks


def getErrorDescription(job, mode='html', provideProcessedCodes = False):
    txt = ''
    codesDescribed = []

    if 'metastruct' in job:
        if type(job['metastruct']) is np.unicode:
            try:
                meta = json.loads(job['metastruct'])
            except:
                print ('Meta type: '+str(type(job['metastruct'])))
                meta = job['metastruct']
            if 'exitCode' in meta and meta['exitCode'] != 0:
                txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
                if provideProcessedCodes:
                    return txt, codesDescribed
                else:
                    return txt
            else:
                if provideProcessedCodes:
                    return '-', codesDescribed
                else:
                    return '-'
        else:
            meta = job['metastruct']
            if 'exitCode' in meta and meta['exitCode'] != 0:
                txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
                if provideProcessedCodes:
                    return txt, codesDescribed
                else:
                    return txt

    for errcode in errorCodes.keys():
        errval = 0
        if errcode in job:
            errval = job[errcode]
            if errval != 0 and errval != '0' and errval != None and errval != '':
                try:
                    errval = int(errval)
                except:
                    pass  # errval = -1
                codesDescribed.append(errval)
                errdiag = errcode.replace('errorcode', 'errordiag')
                if errcode.find('errorcode') > 0:
                    if job[errdiag] is not None:
                        diagtxt = str(job[errdiag])
                    else:
                        diagtxt = ''
                else:
                    diagtxt = ''
                if len(diagtxt) > 0:
                    desc = diagtxt
                elif errval in errorCodes[errcode]:
                    desc = errorCodes[errcode][errval]
                else:
                    desc = "Unknown %s error code %s" % (errcode, errval)
                errname = errcode.replace('errorcode', '')
                errname = errname.replace('exitcode', '')
                if mode == 'html':
                    txt += " <b>%s, %d:</b> %s" % (errname, errval, desc)
                else:
                    txt = "%s, %d: %s" % (errname, errval, desc)
    if provideProcessedCodes:
        return txt, codesDescribed
    else:
        return txt


def getPilotCounts(view):
    query = {}
    query['flag'] = view
    query['hours'] = 3
    rows = Sitedata.objects.filter(**query).values()
    pilotd = {}
    try:
        for r in rows:
            site = r['site']
            if not site in pilotd: pilotd[site] = {}
            pilotd[site]['count'] = r['getjob'] + r['updatejob']
            pilotd[site]['time'] = r['lastmod']
    except:
        pass
    return pilotd


def taskNameDict(jobs):
    ## Translate IDs to names. Awkward because models don't provide foreign keys to task records.
    taskids = {}
    jeditaskids = {}
    for job in jobs:
        if 'taskid' in job and job['taskid'] and job['taskid'] > 0:
            taskids[job['taskid']] = 1
        if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0: jeditaskids[job['jeditaskid']] = 1
    taskidl = taskids.keys()
    jeditaskidl = jeditaskids.keys()

    # Write ids to temp table to avoid too many bind variables oracle error
    tasknamedict = {}
    if len(jeditaskidl) > 0:
        random.seed()
        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
        else:
            tmpTableName = "TMP_IDS1"
        transactionKey = random.randrange(1000000)
        jeditaskidl = [(tid, transactionKey) for tid in jeditaskidl]
        new_cur = connection.cursor()
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, jeditaskidl)
        connection.commit()

        extraqueue = 'JEDITASKID IN (SELECT ID FROM %s WHERE TRANSACTIONKEY = %s)' % (tmpTableName, transactionKey)
        jeditasks = JediTasks.objects.extra(where=[extraqueue]).values('taskname', 'jeditaskid')
        for t in jeditasks:
            tasknamedict[t['jeditaskid']] = t['taskname']

    # if len(taskidl) > 0:
    #    tq = { 'taskid__in' : taskidl }
    #    oldtasks = Etask.objects.filter(**tq).values('taskname', 'taskid')
    #    for t in oldtasks:
    #        tasknamedict[t['taskid']] = t['taskname']
    return tasknamedict


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def getFilePathForObjectStore(objectstore, filetype="logs"):
    """ Return a proper file path in the object store """

    # For single object stores
    # root://atlas-objectstore.cern.ch/|eventservice^/atlas/eventservice|logs^/atlas/logs
    # For multiple object stores
    # eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://atlas-objectstore.bnl.gov//atlas/logs

    basepath = ""

    # Which form of the schedconfig.objectstore field do we currently have?
    if objectstore != "":
        _objectstore = objectstore.split("|")
        if "^" in _objectstore[0]:
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
        else:
            _objectstore = objectstore.split("|")
            url = _objectstore[0]
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
            if basepath != "":
                if url.endswith('/') and basepath.startswith('/'):
                    basepath = url + basepath[1:]
                else:
                    basepath = url + basepath

        if basepath == "":
            print ("Object store path could not be extracted using file type \'%s\' from objectstore=\'%s\'" % (
            filetype, objectstore))

    else:
        print ("Object store not defined in queuedata")

    return basepath


def buildGoogleFlowDiagram(request, jobs=[], tasks=[]):
    ## set up google flow diagram
    if 'requestParams' not in request.session or 'flow' not in request.session['requestParams']: return None
    flowstruct = {}
    if len(jobs) > 0:
        flowstruct['maxweight'] = len(jobs)
        flowrows = buildGoogleJobFlow(jobs)
    elif len(tasks) > 0:
        flowstruct['maxweight'] = len(tasks)
        flowrows = buildGoogleTaskFlow(request, tasks)
    else:
        return None
    flowstruct['columns'] = [['string', 'From'], ['string', 'To'], ['number', 'Weight']]
    flowstruct['rows'] = flowrows[:3000]
    return flowstruct


def buildGoogleJobFlow(jobs):
    cloudd = {}
    mcpcloudd = {}
    mcpshownd = {}
    errd = {}
    errshownd = {}
    sited = {}
    statd = {}
    errcountd = {}
    sitecountd = {}
    siteshownd = {}
    ptyped = {}
    ptypecountd = {}
    ptypeshownd = {}
    for job in jobs:
        errinfo = errorInfo(job, nchars=40, mode='string')
        jobstatus = job['jobstatus']
        for js in ('finished', 'holding', 'merging', 'running', 'cancelled', 'transferring', 'starting'):
            if jobstatus == js: errinfo = js
        if errinfo not in errcountd: errcountd[errinfo] = 0
        errcountd[errinfo] += 1
        cloud = job['homecloud']
        mcpcloud = job['cloud']
        ptype = job['processingtype']
        if ptype not in ptypecountd: ptypecountd[ptype] = 0
        ptypecountd[ptype] += 1
        site = job['computingsite']
        if site not in sitecountd: sitecountd[site] = 0
        sitecountd[site] += 1

        if cloud not in cloudd: cloudd[cloud] = {}
        if site not in cloudd[cloud]: cloudd[cloud][site] = 0
        cloudd[cloud][site] += 1

        if mcpcloud not in mcpcloudd: mcpcloudd[mcpcloud] = {}
        if cloud not in mcpcloudd[mcpcloud]: mcpcloudd[mcpcloud][cloud] = 0
        mcpcloudd[mcpcloud][cloud] += 1

        if jobstatus not in errd: errd[jobstatus] = {}
        if errinfo not in errd[jobstatus]: errd[jobstatus][errinfo] = 0
        errd[jobstatus][errinfo] += 1

        if site not in sited: sited[site] = {}
        if errinfo not in sited[site]: sited[site][errinfo] = 0
        sited[site][errinfo] += 1

        if jobstatus not in statd: statd[jobstatus] = {}
        if errinfo not in statd[jobstatus]: statd[jobstatus][errinfo] = 0
        statd[jobstatus][errinfo] += 1

        if ptype not in ptyped: ptyped[ptype] = {}
        if errinfo not in ptyped[ptype]: ptyped[ptype][errinfo] = 0
        ptyped[ptype][errinfo] += 1

    flowrows = []

    for mcpcloud in mcpcloudd:
        for cloud in mcpcloudd[mcpcloud]:
            n = mcpcloudd[mcpcloud][cloud]
            if float(n) / len(jobs) > 0.0:
                mcpshownd[mcpcloud] = 1
                flowrows.append(["%s MCP" % mcpcloud, cloud, n])

    othersited = {}
    othersiteErrd = {}

    for cloud in cloudd:
        if cloud not in mcpshownd: continue
        for e in cloudd[cloud]:
            n = cloudd[cloud][e]
            if float(sitecountd[e]) / len(jobs) > .01:
                siteshownd[e] = 1
                flowrows.append([cloud, e, n])
            else:
                flowrows.append([cloud, 'Other sites', n])
                othersited[e] = n
    # for jobstatus in errd:
    #    for errinfo in errd[jobstatus]:
    #        flowrows.append( [ errinfo, jobstatus, errd[jobstatus][errinfo] ] )
    for e in errcountd:
        if float(errcountd[e]) / len(jobs) > .01:
            errshownd[e] = 1

    for site in sited:
        nother = 0
        for e in sited[site]:
            n = sited[site][e]
            if site in siteshownd:
                sitename = site
            else:
                sitename = "Other sites"
            if e in errshownd:
                errname = e
            else:
                errname = 'Other errors'
            flowrows.append([sitename, errname, n])
            if errname not in othersiteErrd: othersiteErrd[errname] = 0
            othersiteErrd[errname] += n

    # for e in othersiteErrd:
    #    if e in errshownd:
    #        flowrows.append( [ 'Other sites', e, othersiteErrd[e] ] )

    for ptype in ptyped:
        if float(ptypecountd[ptype]) / len(jobs) > .05:
            ptypeshownd[ptype] = 1
            ptname = ptype
        else:
            ptname = "Other processing types"
        for e in ptyped[ptype]:
            n = ptyped[ptype][e]
            if e in errshownd:
                flowrows.append([e, ptname, n])
            else:
                flowrows.append(['Other errors', ptname, n])

    return flowrows


def buildGoogleTaskFlow(request, tasks):
    analysis = False
    if 'requestParams' in request.session:
        analysis = 'tasktype' in request.session['requestParams'] and request.session['requestParams'][
            'tasktype'].startswith('anal')
    ptyped = {}
    reqd = {}
    statd = {}
    substatd = {}
    trfd = {}
    filestatd = {}
    cloudd = {}
    reqsized = {}
    reqokd = {}
    ## count the reqid's. Use only the biggest (in file count) if too many.
    for task in tasks:
        if not analysis and 'deftreqid' not in task: continue
        req = int(task['reqid'])
        dsinfo = task['dsinfo']
        nfiles = dsinfo['nfiles']
        if req not in reqsized: reqsized[req] = 0
        reqsized[req] += nfiles
        ## Veto requests that are all done etc.
        if task['superstatus'] != 'done': reqokd[req] = 1

    if not analysis:
        for req in reqsized:
            # de-prioritize requests not specifically OK'd for inclusion
            if req not in reqokd: reqsized[req] = 0

        nmaxreq = 10
        if len(reqsized) > nmaxreq:
            reqkeys = reqsized.keys()
            reqsortl = sorted(reqkeys, key=reqsized.__getitem__, reverse=True)
            reqsortl = reqsortl[:nmaxreq - 1]
        else:
            reqsortl = reqsized.keys()

    for task in tasks:
        ptype = task['processingtype']
        # if 'jedireqid' not in task: continue
        req = int(task['reqid'])
        if not analysis and req not in reqsortl: continue
        stat = task['superstatus']
        substat = task['status']
        # trf = task['transpath']
        trf = task['taskname']
        cloud = task['cloud']
        if cloud == '': cloud = 'No cloud assigned'
        dsinfo = task['dsinfo']
        nfailed = dsinfo['nfilesfailed']
        nfinished = dsinfo['nfilesfinished']
        nfiles = dsinfo['nfiles']
        npending = nfiles - nfailed - nfinished

        if ptype not in ptyped: ptyped[ptype] = {}
        if req not in ptyped[ptype]: ptyped[ptype][req] = 0
        ptyped[ptype][req] += nfiles

        if req not in reqd: reqd[req] = {}
        if stat not in reqd[req]: reqd[req][stat] = 0
        reqd[req][stat] += nfiles

        if trf not in trfd: trfd[trf] = {}
        if stat not in trfd[trf]: trfd[trf][stat] = 0
        trfd[trf][stat] += nfiles

        if stat not in statd: statd[stat] = {}
        if substat not in statd[stat]: statd[stat][substat] = 0
        statd[stat][substat] += nfiles

        if substat not in substatd: substatd[substat] = {}
        if 'finished' not in substatd[substat]:
            for filestat in ('finished', 'failed', 'pending'):
                substatd[substat][filestat] = 0
        substatd[substat]['finished'] += nfinished
        substatd[substat]['failed'] += nfailed
        substatd[substat]['pending'] += npending

        if cloud not in cloudd: cloudd[cloud] = {}
        if 'finished' not in cloudd[cloud]:
            for filestat in ('finished', 'failed', 'pending'):
                cloudd[cloud][filestat] = 0
        cloudd[cloud]['finished'] += nfinished
        cloudd[cloud]['failed'] += nfailed
        cloudd[cloud]['pending'] += npending

    flowrows = []

    if analysis:
        ## Don't include request, task for analysis
        for trf in trfd:
            for stat in trfd[trf]:
                n = trfd[trf][stat]
                flowrows.append([trf, 'Task %s' % stat, n])
    else:
        for ptype in ptyped:
            for req in ptyped[ptype]:
                n = ptyped[ptype][req]
                flowrows.append([ptype, 'Request %s' % req, n])

        for req in reqd:
            for stat in reqd[req]:
                n = reqd[req][stat]
                flowrows.append(['Request %s' % req, 'Task %s' % stat, n])

    for stat in statd:
        for substat in statd[stat]:
            n = statd[stat][substat]
            flowrows.append(['Task %s' % stat, 'Substatus %s' % substat, n])

    for substat in substatd:
        for filestat in substatd[substat]:
            if filestat not in substatd[substat]: continue
            n = substatd[substat][filestat]
            flowrows.append(['Substatus %s' % substat, 'File status %s' % filestat, n])

    for cloud in cloudd:
        for filestat in cloudd[cloud]:
            if filestat not in cloudd[cloud]: continue
            n = cloudd[cloud][filestat]
            flowrows.append(['File status %s' % filestat, cloud, n])

    return flowrows



# This function created backend dependable for avoiding numerous arguments in metadata query.
# Transaction and cursors used due to possible issues with django connection pooling
def addJobMetadata(jobs, require=False):
    print ('adding metadata')
    pids = []

    useMetaArch = False
    useMeta = False

    for job in jobs:
        if (job['jobstatus'] == 'failed' or require):
            pids.append(job['pandaid'])

        if 'creationtime' in job:
            tdelta = datetime.now() - job['creationtime']
            delta = int(tdelta.days) + 1
            if delta > 3:
                useMetaArch = True
            else:
                useMeta = True

    query = {}
    query['pandaid__in'] = pids
    mdict = {}
    ## Get job metadata

    random.seed()

    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        metaTableName = "ATLAS_PANDA.METATABLE"
        metaTableNameArch = "ATLAS_PANDAARCH.METATABLE_ARCH"
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"

    else:
        metaTableName = "METATABLE"
        metaTableNameArch = "METATABLE_ARCH"
        tmpTableName = "TMP_IDS1"

    transactionKey = random.randrange(1000000)
    new_cur = connection.cursor()
    for id in pids:
        new_cur.execute("INSERT INTO %s(ID,TRANSACTIONKEY) VALUES (%i,%i)" % (
        tmpTableName, id, transactionKey))  # Backend dependable

    mrecs = []
    if useMeta:
        new_cur.execute(
            "SELECT METADATA,MODIFICATIONTIME,PANDAID FROM %s WHERE PANDAID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (
            metaTableName, tmpTableName, transactionKey))
        mrecs = dictfetchall(new_cur)
    if useMetaArch:
        new_cur.execute(
            "SELECT METADATA,MODIFICATIONTIME,PANDAID FROM %s WHERE PANDAID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (
            metaTableNameArch, tmpTableName, transactionKey))

        mrecs.extend(dictfetchall(new_cur))

    if mrecs:
        for m in mrecs:
            try:
                mdict[m['PANDAID']] = m['METADATA']
            except:
                pass
    for job in jobs:
        if job['pandaid'] in mdict:
            try:
                job['metastruct'] = json.loads(mdict[job['pandaid']].read())
            except:
                pass
                # job['metadata'] = mdict[job['pandaid']]
    print ('added metadata')
    new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))
 #   connection.commit()
 #   connection.leave_transaction_management()
    return jobs


##self monitor

def g4exceptions(request):
    valid, response = initRequest(request)
    setupView(request, hours=365 * 24, limit=999999999)
    if 'hours' in request.session['requestParams']:
        hours = int(request.session['requestParams']['hours'])
    else:
        hours = 3

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, wildCardExt=True)
    query['jobstatus__in'] = ['failed', 'holding']
    query['exeerrorcode'] = 68
    query['exeerrordiag__icontains'] = 'G4 exception'
    values = 'pandaid', 'atlasrelease', 'exeerrorcode', 'exeerrordiag', 'jobstatus', 'transformation'

    jobs = []
    jobs.extend(
        Jobsactive4.objects.filter(**query).extra(where=[wildCardExtension])[:request.session['JOB_LIMIT']].values(
            *values))
    jobs.extend(
        Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension])[:request.session['JOB_LIMIT']].values(
            *values))
    if (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0], "%Y-%m-%d %H:%M:%S")).days > 1) or \
                ((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
                                                     "%Y-%m-%d %H:%M:%S")).days > 1)):
        jobs.extend(
            Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[:request.session['JOB_LIMIT']].values(
                *values))

    if 'amitag' in request.session['requestParams']:

        if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
        else:
            tmpTableName = "TMP_IDS1"

        transactionKey = random.randrange(1000000)
#        connection.enter_transaction_management()
        new_cur = connection.cursor()
        for job in jobs:
            new_cur.execute("INSERT INTO %s(ID,TRANSACTIONKEY) VALUES (%i,%i)" % (
            tmpTableName, job['pandaid'], transactionKey))  # Backend dependable
 #       connection.commit()
        new_cur.execute(
            "SELECT JOBPARAMETERS, PANDAID FROM ATLAS_PANDA.JOBPARAMSTABLE WHERE PANDAID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (
            tmpTableName, transactionKey))
        mrecs = dictfetchall(new_cur)
#        connection.commit()
#        connection.leave_transaction_management()
        jobsToRemove = set()
        for rec in mrecs:
            acceptJob = True
            parameters = rec['JOBPARAMETERS'].read()
            tagName = "--AMITag"
            startPos = parameters.find(tagName)
            if startPos == -1:
                acceptJob = False
            endPos = parameters.find(" ", startPos)
            AMITag = parameters[startPos + len(tagName) + 1:endPos]
            if AMITag != request.session['requestParams']['amitag']:
                acceptJob = False
            if acceptJob == False:
                jobsToRemove.add(rec['PANDAID'])

        jobs = filter(lambda x: not x['pandaid'] in jobsToRemove, jobs)

    jobs = addJobMetadata(jobs, True)
    errorFrequency = {}
    errorJobs = {}

    for job in jobs:
        if (job['metastruct']['executor'][0]['logfileReport']['countSummary']['FATAL'] > 0):
            message = job['metastruct']['executor'][0]['logfileReport']['details']['FATAL'][0]['message']
            exceptMess = message[message.find("G4Exception :") + 14: message.find("issued by :") - 1]
            if exceptMess not in errorFrequency:
                errorFrequency[exceptMess] = 1
            else:
                errorFrequency[exceptMess] += 1

            if exceptMess not in errorJobs:
                errorJobs[exceptMess] = []
                errorJobs[exceptMess].append(job['pandaid'])
            else:
                errorJobs[exceptMess].append(job['pandaid'])

    resp = {'errorFrequency': errorFrequency, 'errorJobs': errorJobs}

    del request.session['TFIRST']
    del request.session['TLAST']
    return HttpResponse(json.dumps(resp), content_type='application/json')


def initSelfMonitor(request):
    import psutil
    if 'hostname' in request.session:
        server = request.session['hostname']
    else: server = '-'

    if 'HTTP_X_FORWARDED_FOR' in request.META:
        remote = request.META['HTTP_X_FORWARDED_FOR']
    else:
        remote = request.META['REMOTE_ADDR']

    urlProto = request.META['wsgi.url_scheme']
    if 'HTTP_X_FORWARDED_PROTO' in request.META:
        urlProto = request.META['HTTP_X_FORWARDED_PROTO']
    urlProto = str(urlProto) + "://"

    try:
        urls = urlProto + request.META['SERVER_NAME'] + request.META['REQUEST_URI']
    except:
        if 'SERVER_PORT' in request.META:
            port =':' + request.META['SERVER_PORT']
        else: port = ''
        if 'PATH_INFO' in request.META:
            path = request.META['PATH_INFO']
        else: path=''
        if 'QUERY_STRING' in request.META and request.META['QUERY_STRING']!="":
            qstring= '?'+request.META['QUERY_STRING']
        else: qstring =''
        urls = urlProto + request.META['SERVER_NAME'] + port + path + qstring
    print (urls)
    qtime = str(timezone.now())
    load = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory().percent
    if 'HTTP_REFERER' in request.META:
        refferer = request.META['HTTP_REFERER']
    else:
        refferer = '-'
    if 'HTTP_USER_AGENT' in request.META:
        useragent = request.META['HTTP_USER_AGENT']
    else:
        useragent = '-'
    request.session["qtime"] = qtime
    request.session["load"] = load
    request.session["remote"] = remote
    request.session["mem"] = mem
    request.session["urls"] = urls
    request.session["refferer"] = refferer
    request.session["useragent"] = useragent


@never_cache
def statpixel(request):
    valid, response = initRequest(request, callselfmon=False)

    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    if 'HTTP_REFERER' in request.META:
        url = request.META['HTTP_REFERER']
        service = 0
        userid = -1
        if request.user.is_authenticated:
            userids = BPUser.objects.filter(email=request.user.email).values('id')
            userid = userids[0]['id']
        Visits.objects.create(url=url, service=service, remote=ip, time=str(timezone.now()), userid=userid)

    #user = BPUser.objects.create_user(username=request.session['ADFS_LOGIN'], email=request.session['ADFS_EMAIL'],
    #                                  first_name=request.session['ADFS_FIRSTNAME'],
    #                                  last_name=request.session['ADFS_LASTNAME'])

    #this is a transparent gif pixel
    pixel_= "\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b"
    response = HttpResponse(pixel_, content_type='image/gif')
    return response

#@cache_page(60 * 20)

# taken from https://raw.githubusercontent.com/PanDAWMS/panda-server/master/pandaserver/taskbuffer/OraDBProxy.py
# retrieve global shares

from PIL import Image
import urllib.request
import io

whitelist = ["triumf.ca", "cern.ch"]
def image(request):
    if ('url' in request.GET):
        param = request.build_absolute_uri()
        url = param[param.index("=")+1:len(param)]
        for urlw in whitelist:
            pattern = "^((http[s]?):\/)?\/?([^:\/\s]+"+urlw+")"
            urlConfim = re.findall(pattern,url)
            if (len(urlConfim)>0):
                break
        if (len(urlConfim)==0):
            return redirect('/static/images/22802286-denied-red-grunge-stamp.png')
        try:
            data = getCacheEntry(request, "imagewrap")
            if data is not None:
                data = base64.b64decode(data)
                response = HttpResponse(data, content_type='image/jpg')
                patch_response_headers(response, cache_timeout=10 * 60)
                return response
            else:
                with urllib.request.urlopen(url) as fd:
                    image_file = io.BytesIO(fd.read())
                    im = Image.open(image_file)
                    rgb_im = im.convert('RGB')
                    response = HttpResponse(content_type='image/jpg')
                    rgb_im.save(response, "JPEG")
                    byte_io = BytesIO()
                    rgb_im.save(byte_io, 'JPEG')
                    data = base64.b64encode(byte_io.getvalue())
                    setCacheEntry(request, "imagewrap", data, 60 * 10)
                    patch_response_headers(response, cache_timeout=10 * 60)
                    return response

        except Exception as ex:
            print(ex)
            return redirect('/static/images/404-not-found-site.gif')
    else:
        return redirect('/static/images/error_z0my4n.png')

def grafana_image(request):
    if ('url' in request.GET):
        param = request.build_absolute_uri()
        url = param[param.index("=")+1:len(param)]
        for urlw in whitelist:
            pattern = "^((http[s]?):\/)?\/?([^:\/\s]+"+urlw+")"
            urlConfim = re.findall(pattern,url)
            if (len(urlConfim)>0):
                break
        if (len(urlConfim)==0):
            return redirect('/static/images/22802286-denied-red-grunge-stamp.png')
        try:
            data = getCacheEntry(request, "grafanaimagewrap")
            if data is not None:
                data = base64.b64decode(data)
                response = HttpResponse(data, content_type='image/jpg')
                patch_response_headers(response, cache_timeout=10 * 60)
                return response
            if 'Authorization' in GRAFANA:
                grafana_token = GRAFANA['Authorization']
            import requests
            headers = {"Authorization": grafana_token}
            r = requests.get(url, headers = headers)
            r.raise_for_status()
            with io.BytesIO(r.content) as f:
                with Image.open(f) as img:
                    rgb_im = img.convert('RGB')
                    response = HttpResponse(content_type='image/jpg')
                    rgb_im.save(response, "JPEG")
                    byte_io = BytesIO()
                    rgb_im.save(byte_io, 'JPEG')
                    data = base64.b64encode(byte_io.getvalue())
                    setCacheEntry(request, "grafanaimagewrap", data, 60 * 60)
                    return response
        except Exception as ex:
            return redirect('/static/images/404-not-found-site.gif')
    else:
        return redirect('/static/images/error_z0my4n.png')

def handler500(request):
    response = render_to_response('500.html', {},
                                  context_instance=RequestContext(request))
    response.status_code = 500
    return response
#### URL Section ####
import uuid
from collections import defaultdict

def setCacheData(request,lifetime=60*120,**parametrlist):
    #transactionKey = random.getrandbits(128)
    transactionKey = uuid.uuid4().hex
    dictinoary = {}
    dictinoary[transactionKey] = {}
    keys = parametrlist.keys()
    for key in keys:
        dictinoary[transactionKey][key] = str(parametrlist[key])
    data = json.dumps(dictinoary, cls=DateEncoder)
    setCacheEntry(request, str(transactionKey), data, lifetime,isData=True)

    return transactionKey

def getCacheData(request,requestid):
    data = getCacheEntry(request,str(requestid),isData=True)
    if data is not None:
        data = json.loads(data)
        if 'childtk'in data[requestid]:
            tklist = defaultdict(list)
            data = str(data[requestid]['childtk']).split(',')
            if data is not None:
                for child in data:
                    ch = getCacheEntry(request,str(child),isData=True)
                    if ch is not None:
                        ch = json.loads(ch)
                        ### merge data
                        for k, v in ch[child].items():
                            tklist[k].append(v)
                data = {}
                for k,v in tklist.items():
                    data[k] =','.join(v)
        else: data = data[requestid]
        return data
    else:
        return None

def fixLob(cur):
    fixRowsList = []
    for row in cur:
        newRow = []
        for col in row:
            if type(col).__name__ == 'LOB':
                newRow.append(str(col))
            else:
                newRow.append(col)
        fixRowsList.append(tuple(newRow))
    return fixRowsList
############################


#@never_cache
def getBadEventsForTask(request):

    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return HttpResponse("Not jeditaskid supplied", content_type='text/html')

    mode = 'drop'
    if 'mode' in request.GET and request.GET['mode'] == 'nodrop':
        mode = 'nodrop'

    global errorFields, errorCodes, errorStages
    if len(errorFields) == 0:
        codes = ErrorCodes.ErrorCodes()
        errorFields, errorCodes, errorStages = codes.getErrorCodes()

    data = []
    cursor = connection.cursor()

    plsql = """select DATASETID, ERROR_CODE, RTRIM(XMLAGG(XMLELEMENT(E,DEF_MIN_EVENTID,',').EXTRACT('//text()') 
            ORDER BY DEF_MIN_EVENTID).GetClobVal(),',') as bb,
            RTRIM(XMLAGG(XMLELEMENT(E,PANDAID,',').EXTRACT('//text()') ORDER BY PANDAID).GetClobVal(),',') AS PANDAIDS, 
            count(*) from 
            atlas_panda.jedi_events where jeditaskid=%d and attemptnr = 1 group by DATASETID, ERROR_CODE """ % jeditaskid

    if mode == 'drop':
        plsql = """select DATASETID, ERROR_CODE, RTRIM(XMLAGG(XMLELEMENT(E,DEF_MIN_EVENTID,',').EXTRACT('//text()') 
            ORDER BY DEF_MIN_EVENTID).GetClobVal(),',') as bb, 
            RTRIM(XMLAGG(XMLELEMENT(E,PANDAID,',').EXTRACT('//text()') ORDER BY PANDAID).GetClobVal(),',') AS PANDAIDS,
            count(*) from 
            atlas_panda.jedi_events where jeditaskid=%d and attemptnr = 1 
            and PANDAID IN (SELECT PANDAID FROM ATLAS_PANDA.JEDI_DATASET_CONTENTS where jeditaskid=%d and type in ('input', 'pseudo_input'))
            group by DATASETID, ERROR_CODE """ % (jeditaskid, jeditaskid)

    cursor.execute(plsql)
    evtable = cursor.fetchall()

    for row in evtable:
        dataitem = {}
        dataitem['DATASETID'] = row[0]
        dataitem['ERROR_CODE'] = (errorCodes['piloterrorcode'][row[1]] + " (" +str(row[1])+ ")") if row[1] in errorCodes['piloterrorcode'] else row[1]
        dataitem['EVENTS'] = list(set(  str(row[2].read()).split(',')   )) if not row[2] is None else None
        dataitem['PANDAIDS'] = list(set(  str(row[3].read()).split(',')   )) if not row[3] is None else None
        if dataitem['EVENTS']: dataitem['EVENTS'] = sorted(dataitem['EVENTS'])
        dataitem['COUNT'] = row[4]
        data.append(dataitem)
    cursor.close()
    return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')


def getEventsChunks(request):
    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return HttpResponse("Not jeditaskid supplied", content_type='text/html')

    # We reconstruct here jobsets retries

    sqlRequest = """SELECT OLDPANDAID, NEWPANDAID, MAX(LEV) as LEV, MIN(PTH) as PTH FROM (
    SELECT OLDPANDAID, NEWPANDAID, LEVEL as LEV, CONNECT_BY_ISLEAF as IL, SYS_CONNECT_BY_PATH(OLDPANDAID, ',') PTH FROM (
    SELECT OLDPANDAID, NEWPANDAID FROm ATLAS_PANDA.JEDI_JOB_RETRY_HISTORY WHERE JEDITASKID=%s and RELATIONTYPE='jobset_retry')t1 CONNECT BY OLDPANDAID=PRIOR NEWPANDAID
    )t2 GROUP BY OLDPANDAID, NEWPANDAID;""" % str(jeditaskid)

    cur = connection.cursor()
    cur.execute(sqlRequest)
    datasetsChunks = cur.fetchall()
    cur.close()

    jobsetretries = {}
    eventsChunks = []

    for datasetsChunk in datasetsChunks:
        jobsetretries[datasetsChunk[1]] = datasetsChunk[3].split(',')[1:]

    eventsChunksValues = 'lfn', 'attemptnr', 'startevent', 'endevent', 'pandaid', 'status', 'jobsetid', 'failedattempt', 'maxfailure', 'maxattempt'
    queryChunks = {'jeditaskid': jeditaskid, 'startevent__isnull': False, 'type': 'input'}
    eventsChunks.extend(
        JediDatasetContents.objects.filter(**queryChunks).order_by('attemptnr').reverse().values(*eventsChunksValues))

    for eventsChunk in eventsChunks:
        if eventsChunk['jobsetid'] in jobsetretries:
            eventsChunk['prevAttempts'] = jobsetretries[eventsChunk['jobsetid']]
            eventsChunk['attemptnrDS'] = len(jobsetretries[eventsChunk['jobsetid']])
        else:
            eventsChunk['prevAttempts'] = []
            eventsChunk['attemptnrDS'] = 0

    return HttpResponse(json.dumps(eventsChunks, cls=DateTimeEncoder), content_type='application/json')


def getJobStatusLog(request, pandaid = None):
    """
    A view to asynchronously load job states changes history
    :param request:
    :param pandaid:
    :return: json contained job states changes history
    """
    valid, response = initRequest(request)
    if not valid: return response

    try:
        pandaid = int(pandaid)
    except:
        HttpResponse(status=404, content_type='text/html')

    squery = {}
    squery['pandaid'] = pandaid
    statusLog = []
    statusLog.extend(JobsStatuslog.objects.filter(**squery).order_by('modiftime_extended').values())

    mtimeparam = 'modiftime_extended'
    if len(statusLog) > 0 and None in set([sl['modiftime_extended'] for sl in statusLog]):
        mtimeparam = 'modificationtime'
        statusLog = sorted(statusLog, key=lambda x: x[mtimeparam])

    if len(statusLog) > 0:
        for c, item in enumerate(statusLog):
            if c < len(statusLog)-1:
                if statusLog[c+1][mtimeparam] is not None and statusLog[c][mtimeparam] is not None:
                    duration = statusLog[c+1][mtimeparam] - statusLog[c][mtimeparam]
                    ndays = duration.days
                    strduration = str(timedelta(seconds=duration.seconds))
                    statusLog[c]['duration'] = "%s:%s" % (ndays, strduration)
                else:
                    statusLog[c]['duration'] = "---"
            else:
                statusLog[c]['duration'] = "---"

    for sl in statusLog:
        sl['modiftime_str'] = sl[mtimeparam].strftime(defaultDatetimeFormat) if sl[mtimeparam] is not None else "---"
    response = render_to_response('jobStatusLog.html', {'statusLog': statusLog}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
### API ###
def getSites(request):
    if request.is_ajax():
        try:
            q = request.GET.get('term', '')
            sites = Schedconfig.objects.filter(siteid__icontains=q).exclude(cloud='CMS').values("siteid")
            results = []
            for site in sites:
                results.append(site['siteid'])
            data = json.dumps(results)
        except:
            data = 'fail'
    else:
        data = 'fail'
    mimetype = 'application/json'
    return HttpResponse(data, mimetype)

def serverStatusHealth(request):
    """
    This function dymanically calculates status of a particular server in order to make it idle and give an opportunity
    to restart wsgi daemon.

    WSGIDaemonProcess: inactivity-timeout=60 (this is for nginx health) restart-interval=14400 (the last one is for guarging from blocking requests)

    Nginx: https://www.nginx.com/resources/admin-guide/http-health-check/


    match server_ok {
        status 200;
        header Content-Type = text/html;
        body ~ "Normal operation";
    }


    location / {
        proxy_pass http://backend;
        health_check match=server_ok uri=/serverstatushealth/ interval=600 fails=10000 passes=1;
    }

    Then healthping = 10 min,
    """

    initRequest(request)
    periodOfAllServWorkRestart = 15 #minutes.
    restartTimeWindow = 5

    debug = True

    # Here we should load all the servers from the settingsdjangosettings.
    # next is just for tests

    data = getCacheEntry(request, "StatusHealth")

    print ("serverStatusHealth ", datetime.now(), " runninghost:", request.session["hostname"], " ", data)

    if data is None:
        q = collections.deque()
        q.append("aipanda100")
        q.append("aipanda105")
        q.append("aipanda106")
        q.append("aipanda115")
        q.append("aipanda116")
        q.append("aipanda107")
        q.append("aipanda108")
        lastupdate = datetime.now()
        data['q'] = pickle.dumps(q)
        data['lastupdate'] = lastupdate
        setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
    else:
        data = json.loads(data)
        q = pickle.loads(data['q'])
        lastupdate = datetime.strptime(data['lastupdate'], defaultDatetimeFormat)

    # end of test filling

    currenthost = q.popleft()
    runninghost = request.session["hostname"]

    if (currenthost == runninghost):
        if (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart)) and \
                        (datetime.now() - lastupdate) < timedelta(minutes=(periodOfAllServWorkRestart+restartTimeWindow)):
            return HttpResponse("Awaiting restart", content_type='text/html')
        elif (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart)) and \
                        (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart+restartTimeWindow)):
            data = {}
            q.append(currenthost)
            data['q'] = pickle.dumps(q)
            data['lastupdate'] = datetime.now().strftime(defaultDatetimeFormat)
            setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
            return HttpResponse("Normal operation", content_type='text/html')



    # rows = subprocess.check_output('ps -eo cmd,lstart --sort=start_time | grep httpd', shell=True).split('\n')[:-2]
    # print "serverStatusHealth ", datetime.now(), " rows:", rows
    #
    # if (currenthost == runninghost) and (datetime.now() - lastupdate) > timedelta(minutes=periodOfAllServWorkRestart):
    #
    #     if len(rows) > 0:
    #         httpdStartTime = list(datefinder.find_dates(rows[0]))[0]
    #         if (datetime.now() - httpdStartTime) < timedelta(minutes=periodOfAllServWorkRestart):
    #
    #             print "serverStatusHealth ", "httpdStartTime", httpdStartTime
    #
    #             data = {}
    #             data['q'] = pickle.dumps(q)
    #             data['lastupdate'] = datetime.now().strftime(defaultDatetimeFormat)
    #             setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
    #
    #             print "serverStatusHealth ", "Normal operation0"
    #             return HttpResponse("Normal operation", content_type='text/html')
    #             # We think that wsgi daemon recently restarted and we can change order to the next server
    #             # q.put(currenthost)
    #             # q. put to cache
    #             # lastupdate put to cache
    #             # return success
    #
    #     # we return failed by default
    #     print "serverStatusHealth ", "Awaiting restart"
    #     return HttpResponse("Awaiting restart", content_type='text/html')
    #
    # print "serverStatusHealth ", "Normal operations1"
    return HttpResponse("Normal operation", content_type='text/html')


def getHarversterWorkersForTask(request):
    valid, response = initRequest(request)
    if not valid: return response
    if 'requestParams' in request.session and 'jeditaskid' in request.session['requestParams']:
        try:
            jeditaskid = int(request.session['requestParams']['jeditaskid'])
        except:
            return HttpResponse(status=400)

        data = get_harverster_workers_for_task(jeditaskid)
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    return HttpResponse(status=400)


#import logging
#logging.basicConfig()
@never_cache
def loginauth2(request):
    if 'next' in request.GET:
        next = str(request.GET['next'])
    elif 'HTTP_REFERER' in request.META:
        next = extensibleURL(request, request.META['HTTP_REFERER'])
    else:
        next = '/'
    response = render_to_response('login.html', {'request': request, 'next':next,}, content_type='text/html')
    response.delete_cookie('sessionid')
    return response

def loginerror(request):
    warning = """The login to BigPanDA monitor is failed. Cleaning of your browser cookies might help. 
                 If the error is persistent, please write to """
    response = render_to_response('login.html', {'request': request, 'warning': warning}, content_type='text/html')
    #patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@login_customrequired
def testauth(request):
    response = render_to_response('testauth.html', {'request': request,}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

def logout(request):
    """Logs out user"""
    auth_logout(request)
    return redirect('/')

def testip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    # if x_forwarded_for:
    #     ip = x_forwarded_for.split(',')[0]
    # else:
    #     ip = request.META.get('REMOTE_ADDR')
    return HttpResponse(json.dumps(x_forwarded_for, cls=DateTimeEncoder), content_type='application/json')
