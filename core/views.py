import logging
import re
import subprocess
import os
import sys
import traceback
import time
import json
import copy
import random
import numpy as np
import pandas as pd
import math
import base64
import urllib3
import hashlib

from datetime import datetime, timedelta
from threading import Thread, Lock
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, unquote_plus
from elasticsearch_dsl import Search

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render_to_response, redirect
from django.template import RequestContext
from django.db.models import Count, Sum, F, Value, FloatField, Q, DateTimeField
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.cache import never_cache
from django.utils import timezone
from django.utils.cache import patch_response_headers
from django.core.cache import cache
from django.db import connection
from django.template.loaders.app_directories import get_app_template_dirs
from django.template.defaulttags import register
from django.template.context_processors import csrf

from core import chainsql
import core.constants as const
import core.Customrenderer as Customrenderer
from core.common.utils import getPrefix, getContextVariables
from core.pandajob.SQLLookups import CastDate
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived, \
    GetRWWithPrioJedi3DAYS, RemainedEventsPerCloud3dayswind, CombinedWaitActDefArch4, PandaJob
from core.schedresource.models import Schedconfig, SchedconfigJson
from core.common.models import Filestable4
from core.common.models import Datasets
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
from core.common.models import TasksStatusLog
from core.common.models import GetEventsForTask
from core.common.models import JediEvents
from core.common.models import JediDatasets
from core.common.models import JediDatasetContents
from core.common.models import JediWorkQueue
from core.oauth.models import BPUser
from core.compare.modelsCompare import ObjectsComparison
from core.filebrowser.ruciowrapper import ruciowrapper

from django.conf import settings

from core.libs.TaskProgressPlot import TaskProgressPlot
from core.libs.UserProfilePlot import UserProfilePlot
from core.libs.TasksErrorCodesAnalyser import TasksErrorCodesAnalyser

from core.oauth.utils import login_customrequired

from core.utils import is_json_request, extensibleURL, complete_request, is_wildcards, removeParam
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table, drop_job_retries
from core.libs.cache import getCacheEntry, setCacheEntry, set_cache_timeout, getCacheData
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, create_temporary_table
from core.libs.exlib import is_timestamp, get_file_info, convert_bytes, convert_hs06, dictfetchall
from core.libs.eventservice import event_summary_for_task
from core.libs.task import input_summary_for_task, datasets_for_task, \
    get_task_params, humanize_task_params, get_hs06s_summary_for_task, cleanTaskList, get_task_flow_data, \
    get_datasets_for_tasklist, get_task_name_by_taskid
from core.libs.task import get_dataset_locality, is_event_service_task, \
    get_prod_slice_by_taskid, get_task_timewindow, get_task_time_archive_flag, get_logs_by_taskid, task_summary_dict, \
    wg_task_summary
from core.libs.job import is_event_service, get_job_list, calc_jobs_metrics, add_job_category, \
    job_states_count_by_param, is_job_active, get_job_queuetime, get_job_walltime, job_state_count, \
    getSequentialRetries, getSequentialRetries_ES, getSequentialRetries_ESupstream, is_debug_mode, clean_job_list
from core.libs.eventservice import job_suppression
from core.libs.jobmetadata import addJobMetadata
from core.libs.error import errorInfo, getErrorDescription, get_job_error_desc
from core.libs.site import get_pq_metrics
from core.libs.bpuser import get_relevant_links, filterErrorData
from core.libs.user import prepare_user_dash_plots, get_panda_user_stats, humanize_metrics
from core.libs.elasticsearch import create_esatlas_connection, get_payloadlog
from core.libs.sqlcustom import escape_input, preprocess_wild_card_string
from core.libs.datetimestrings import datetime_handler, parse_datetime
from core.libs.jobconsumers import reconstruct_job_consumers
from core.libs.DateEncoder import DateEncoder
from core.libs.DateTimeEncoder import DateTimeEncoder

from core.pandajob.summary_error import errorSummaryDict, get_error_message_summary
from core.pandajob.summary_task import task_summary, job_summary_for_task, job_summary_for_task_light, \
    get_job_state_summary_for_tasklist, get_top_memory_consumers
from core.pandajob.summary_site import cloud_site_summary, vo_summary, site_summary_dict
from core.pandajob.summary_wg import wg_summary
from core.pandajob.summary_wn import wn_summary
from core.pandajob.summary_os import objectstore_summary
from core.pandajob.summary_user import user_summary_dict
from core.pandajob.utils import job_summary_dict

from core.iDDS.algorithms import checkIfIddsTask
from core.dashboards.jobsummaryregion import get_job_summary_region, prepare_job_summary_region, prettify_json_output
from core.dashboards.jobsummarynucleus import get_job_summary_nucleus, prepare_job_summary_nucleus, get_world_hs06_summary
from core.dashboards.eventservice import get_es_job_summary_region, prepare_es_job_summary_region
from core.schedresource.utils import get_pq_atlas_sites, get_panda_queues, get_basic_info_for_pqs, \
    get_panda_resource, get_pq_clouds, get_pq_object_store_path


tcount = {}
lock = Lock()
DateTimeField.register_lookup(CastDate)

try:
    hostname = subprocess.getoutput('hostname')
    if hostname.find('.') > 0: hostname = hostname[:hostname.find('.')]
except:
    hostname = ''

cloudList = ['CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'ND', 'NL', 'RU', 'TW', 'UK', 'US']

statelist = ['pending', 'defined', 'waiting', 'assigned', 'throttled',
             'activated', 'sent', 'starting', 'running', 'holding',
             'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']
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


LAST_N_HOURS_MAX = 0
PLOW = 1000000
PHIGH = -1000000


standard_fields = ['processingtype', 'computingsite', 'jobstatus', 'prodsourcelabel', 'produsername', 'jeditaskid',
                   'workinggroup', 'transformation', 'cloud', 'homepackage', 'inputfileproject', 'inputfiletype',
                   'attemptnr', 'specialhandling', 'priorityrange', 'reqid', 'minramcount', 'eventservice',
                   'jobsubstatus', 'nucleus','gshare', 'resourcetype']
standard_sitefields = ['region', 'gocname', 'nickname', 'status', 'tier', 'comment_field', 'cloud', 'allowdirectaccess',
                       'allowfax', 'copytool', 'faxredirector', 'retry', 'timefloor']
standard_taskfields = [
    'workqueue_id', 'tasktype', 'superstatus', 'status', 'corecount', 'taskpriority', 'currentpriority', 'username',
    'transuses', 'transpath', 'workinggroup', 'processingtype', 'cloud', 'campaign', 'project', 'stream', 'tag',
    'reqid', 'ramcount', 'nucleus', 'eventservice', 'gshare', 'container_name', 'attemptnr', 'site']
standard_errorfields = ['cloud', 'computingsite', 'eventservice', 'produsername', 'jeditaskid', 'jobstatus',
                        'processingtype', 'prodsourcelabel', 'specialhandling', 'taskid', 'transformation',
                        'workinggroup', 'reqid', 'computingelement']

VONAME = {'atlas': 'ATLAS', 'bigpanda': 'BigPanDA', 'htcondor': 'HTCondor', 'core': 'LSST', '': ''}
VOMODE = ' '


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

    if kwargs['type'] == "region_sitesummary":
        kwargs['statelist'] = statelist
        return Customrenderer.region_sitesummary(context, kwargs)


def initRequest(request, callselfmon=True):
    global VOMODE, ENV, hostname
    ENV = {}
    VOMODE = ''
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        VOMODE = 'atlas'
        # VOMODE = 'devtest'

    request.session['req_init_time'] = time.time()
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

    # creating a dict in session to store long urls as it will not be saved to session storage
    # Session is NOT modified, because this alters sub dict
    request.session['urls_cut'] = {}
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
    request.session['urls_cut']['notimestampurl'] = urlunparse(u) + ('&' if len(query) > 0 else '?')

    notimerangeurl = extensibleURL(request)
    timerange_params = [
        'days', 'hours',
        'date_from', 'date_to',
        'endtimerange', 'endtime_from', 'endtime_to',
        'earlierthan', 'earlierthandays'
    ]
    for trp in timerange_params:
        notimerangeurl = removeParam(notimerangeurl, trp, mode='extensible')
    request.session['urls_cut']['notimerangeurl'] = notimerangeurl

    if 'timerange' in request.session:
        del request.session['timerange']

    #if 'USER' in os.environ and os.environ['USER'] != 'apache':
    #    request.session['debug'] = True
    if 'debug' in request.GET and request.GET['debug'] == 'insider':
        request.session['debug'] = True
        settings.DEBUG = True
    elif settings.DEBUG is True:
        request.session['debug'] = True
    else:
        request.session['debug'] = False
        settings.DEBUG = False

    if len(hostname) > 0: request.session['hostname'] = hostname

    #self monitor
    if callselfmon:
        initSelfMonitor(request)

    # Set default page lifetime in the http header, for the use of the front end cache
    set_cache_timeout(request)

    # Is it an https connection with a legit cert presented by the user?
    if 'SSL_CLIENT_S_DN' in request.META or 'HTTP_X_SSL_CLIENT_S_DN' in request.META:
        if 'SSL_CLIENT_S_DN' in request.META:
            request.session['userdn'] = request.META['SSL_CLIENT_S_DN']
        else:
            request.session['userdn'] = request.META['HTTP_X_SSL_CLIENT_S_DN']
        userrec = Users.objects.filter(dn__startswith=request.session['userdn']).values()
        if len(userrec) > 0:
            request.session['username'] = userrec[0]['name']

    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        VOMODE = 'atlas'
        request.session['viewParams']['MON_VO'] = 'ATLAS'
    else:
        VOMODE =settings.DEPLOYMENT
        #request.session['viewParams']['MON_VO'] = DEPLOYMENT

    # remove xurls from session if it is kept from previous requests
    if 'xurls' in request.session:
        try:
            del request.session['xurls']
        except:
            pass

    requestParams = {}
    request.session['requestParams'] = requestParams

    allowedemptyparams = ('json', 'snap', 'dt', 'dialogs', 'pandaids', 'workersstats', 'keephtml')
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
            if 'script' in pval.lower() and ('</' in pval.lower() or '/>' in pval.lower()):
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
                'days', 'hours', 'limit', 'display_limit', 'taskid', 'jeditaskid', 'jobsetid', 'reqid', 'corecount',
                'taskpriority', 'priority', 'attemptnr', 'statenotupdated', 'tasknotupdated', 'corepower',
                'wansourcelimit', 'wansinklimit', 'nqueue', 'nodes', 'queuehours', 'memory', 'maxtime', 'space',
                'maxinputsize', 'timefloor', 'depthboost', 'idlepilotsupression', 'pilotlimit', 'transferringlimit',
                'cachedse', 'stageinretry', 'stageoutretry', 'maxwdir', 'minmemory', 'maxmemory', 'minrss',
                'maxrss', 'mintime', 'nlastnightlies'):
                try:
                    requestVal = request.GET[p]
                    if '|' in requestVal:
                        values = requestVal.split('|')
                        for value in values:
                            i = int(value)
                    elif requestVal == 'Not specified':
                        # allow 'Not specified' value for int parameters
                        i = requestVal
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
            if p.lower() in ('jobname', 'taskname', ) and len(pval) > 0 and ('%' in pval or '%s' in pval):
                data = {
                    'viewParams': request.session['viewParams'],
                    'requestParams': request.session['requestParams'],
                    "errormessage": "Use * symbol for pattern search instead of % for {}".format(p),
                }
                return False, render_to_response('errorPage.html', data, content_type='text/html')
            request.session['requestParams'][p.lower()] = pval

    return True, None


def setupView(request, opmode='', hours=0, limit=-99, querytype='job', wildCardExt=False):
    viewParams = {}
    if not 'viewParams' in request.session:
        request.session['viewParams'] = viewParams

    extraQueryString = '(1=1) '
    extraQueryFields = []  # params that goes directly to the wildcards processing

    LAST_N_HOURS_MAX = 0

    for paramName, paramVal in request.session['requestParams'].items():
        try:
            request.session['requestParams'][paramName] = unquote_plus(paramVal)
        except:
            request.session['requestParams'][paramName] = paramVal

    excludeJobNameFromWildCard = True
    if 'jobname' in request.session['requestParams']:
        if is_wildcards(request.session['requestParams']['jobname']):
            excludeJobNameFromWildCard = False

    processor_type = request.session['requestParams'].get('processor_type', None)
    if processor_type:
        if processor_type.lower() == 'cpu':
            extraQueryString += " AND (cmtconfig not like '%%gpu%%')"
        if processor_type.lower() == 'gpu':
            extraQueryString += " AND (cmtconfig like '%%gpu%%')"

    if 'site' in request.session['requestParams'] and (
            request.session['requestParams']['site'] == 'hpc' or not is_wildcards(request.session['requestParams']['site'])):
        extraQueryFields.append('site')

    wildSearchFields = []
    if querytype == 'job':
        for field in Jobsactive4._meta.get_fields():
            if field.get_internal_type() == 'CharField':
                if not (field.name == 'jobstatus' or field.name == 'modificationhost'
                        or (excludeJobNameFromWildCard and field.name == 'jobname')):
                    wildSearchFields.append(field.name)
    if querytype == 'task':
        for field in JediTasks._meta.get_fields():
            if field.get_internal_type() == 'CharField':
                if not (field.name == 'modificationhost' or field.name in extraQueryFields):
                    wildSearchFields.append(field.name)

    deepquery = False
    fields = standard_fields
    if 'limit' in request.session['requestParams']:
        request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
    elif limit != -99 and limit > 0:
        request.session['JOB_LIMIT'] = limit
    elif VOMODE == 'atlas':
        request.session['JOB_LIMIT'] = 20000
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
        if param == 'timestamp': continue
        if param == 'limit' and request.session['JOB_LIMIT'] > 0: continue
        request.session['viewParams']['selection'] += " <b>%s=</b>%s " % (
        param, request.session['requestParams'][param])

    startdate = None
    if 'date_from' in request.session['requestParams']:
        startdate = parse_datetime(request.session['requestParams']['date_from'])
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    # startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = None

    endtime__castdate__range = None
    if 'endtimerange' in request.session['requestParams']:
        endtimerange = request.session['requestParams']['endtimerange'].split('|')
        endtime__castdate__range = [parse_datetime(endtimerange[0]).strftime(settings.DATETIME_FORMAT),
                                    parse_datetime(endtimerange[1]).strftime(settings.DATETIME_FORMAT)]

    if 'date_to' in request.session['requestParams']:
        enddate = parse_datetime(request.session['requestParams']['date_to'])
    if 'earlierthan' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthan']))
    # enddate = enddate.strftime(settings.DATETIME_FORMAT)
    if 'earlierthandays' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthandays']) * 24)
    # enddate = enddate.strftime(settings.DATETIME_FORMAT)
    if enddate == None:
        enddate = timezone.now()  # .strftime(settings.DATETIME_FORMAT)
        request.session['noenddate'] = True
    else:
        request.session['noenddate'] = False

    if request.path.startswith('/running'):
        query = {}
    else:
        if not endtime__castdate__range:
            query = {
                'modificationtime__castdate__range': [startdate.strftime(settings.DATETIME_FORMAT), enddate.strftime(settings.DATETIME_FORMAT)]}
        else:
            query = {
                'endtime__castdate__range': [endtime__castdate__range[0], endtime__castdate__range[1]]}


    request.session['TFIRST'] = startdate  # startdate[:18]
    request.session['TLAST'] = enddate  # enddate[:18]

    request.session['PLOW'] = 1000000
    request.session['PHIGH'] = -1000000

    ### Add any extensions to the query determined from the URL
    #query['vo'] = 'atlas'
    #for vo in ['atlas', 'core']:
    #    if 'HTTP_HOST' in request.META and request.META['HTTP_HOST'].startswith(vo):
    #        query['vo'] = vo
    for param in request.session['requestParams']:
        if param in ('hours', 'days'): continue
        elif param == 'cloud' and request.session['requestParams'][param] == 'All':
            continue
        elif param == 'workinggroup':
            if request.session['requestParams'][param] and not is_wildcards(request.session['requestParams'][param]):
                query[param] = request.session['requestParams'][param]
        elif param == 'harvesterinstance' or param == 'harvesterid':
            val = request.session['requestParams'][param]
            if val == 'Not specified':
                extraQueryString += " AND ((schedulerid not like 'harvester%%') or (schedulerid = '') or (schedulerid is null))"
            elif val == 'all':
                query['schedulerid__startswith'] = 'harvester'
            else:
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
            val = escape_input(request.session['requestParams'][param])
            values = val.split(',')
            query['harvesterid__in'] = values

        elif param in ('tag',):
            val = request.session['requestParams'][param]
            query['taskname__endswith'] = val

        elif param == 'reqid_from':
            val = int(request.session['requestParams'][param])
            query['reqid__gte'] = val
        elif param == 'reqid_to':
            val = int(request.session['requestParams'][param])
            query['reqid__lte'] = val
        elif param == 'processingtype' and '|' not in request.session['requestParams'][param] and '*' not in request.session['requestParams'][param] and '!' not in request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            query['processingtype'] = val
        elif param == 'mismatchedcloudsite' and request.session['requestParams'][param] == 'true':
            listOfCloudSitesMismatched = cache.get('mismatched-cloud-sites-list')
            if (listOfCloudSitesMismatched is None) or (len(listOfCloudSitesMismatched) == 0):
                request.session['viewParams'][
                    'selection'] += "      <b>The query can not be processed because list of mismatches is not found. Please visit %s/dash/production/?cloudview=region page and then try again</b>" % \
                                    request.session['hostname']
            else:
                for count, cloudSitePair in enumerate(listOfCloudSitesMismatched):
                    extraQueryString += 'AND ( ( (cloud=\'%s\') and (computingsite=\'%s\') ) ' % (
                    cloudSitePair[1], cloudSitePair[0])
                    if (count < (len(listOfCloudSitesMismatched) - 1)):
                        extraQueryString += ' OR '
                extraQueryString += ')'
        elif param == 'pilotversion' and request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            if val == 'Not specified':
                extraQueryString += ' AND ( (pilotid not like \'%%|%%\') or (pilotid is null) )'
            else:
                query['pilotid__endswith'] = val
        elif param == 'durationmin' and request.session['requestParams'][param]:
            try:
                durationrange = request.session['requestParams'][param].split('-')
            except:
                continue
            if durationrange[0] == '0' and durationrange[1] == '0':
                extraQueryString += ' AND (  (endtime is NULL and starttime is null) ) '
            else:
                extraQueryString += """ AND (
            (endtime is not NULL and starttime is not null 
            and (endtime - starttime) * 24 * 60 > {} and (endtime - starttime) * 24 * 60 < {} ) 
            or 
            (endtime is NULL and starttime is not null 
            and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 > {} and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 < {} ) 
            ) """.format(str(durationrange[0]), str(durationrange[1]), str(durationrange[0]), str(durationrange[1]))
        elif param == 'neventsrange' and request.session['requestParams'][param]:
            try:
                neventsrange = request.session['requestParams'][param].split('-')
            except:
                continue
            if neventsrange and len(neventsrange) == 2:
                query['nevents__gte'] = neventsrange[0]
                query['nevents__lte'] = neventsrange[1]
        elif param == 'errormessage':
            errfield_map_dict = {}
            for errcode in errorcodelist:
                if errcode['name'] != 'transformation':
                    errfield_map_dict[errcode['error']] = errcode['diag']
            for parname in request.session['requestParams']:
                if parname in errfield_map_dict.keys():
                    query[errfield_map_dict[parname]] = request.session['requestParams'][param]

        elif param == 'container_name' and request.session['requestParams']['container_name'] == 'all':
            extraQueryString += " AND (container_name IS NOT NULL ) "
            # remove from wildcard search fields
            wildSearchFields.remove('container_name')
            # add a new no_container_name xurl to request session
            if 'xurl' not in request.session:
                request.session['xurls'] = {}
            request.session['xurls']['container_name'] = removeParam(extensibleURL(request), 'container_name', mode='extensible')
            continue
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
            else:
                return 'reqtoken', None, None

        if querytype == 'task':
            if param == 'category':
                if request.session['requestParams'][param] == 'group production':
                    query['workinggroup__icontains'] = 'GP_'
                elif request.session['requestParams'][param] == 'production':
                    query['tasktype'] = 'prod'
                    query['workinggroup__icontains'] = 'AP_'
                elif request.session['requestParams'][param] == 'group analysis':
                    query['tasktype'] = 'anal'
                    query['workinggroup__isnull'] = False
                    extraQueryString += " AND username not in ('artprod', 'atlevind', 'gangarbt') "
                elif request.session['requestParams'][param] == 'user analysis':
                    query['tasktype'] = 'anal'
                    query['workinggroup__isnull'] = True
                    extraQueryString += " AND username not in ('artprod', 'atlevind', 'gangarbt') "
                elif request.session['requestParams'][param] == 'service':
                    query['username__in'] = ('artprod', 'atlevind', 'gangarbt')
            for field in JediTasks._meta.get_fields():
                # for param in requestParams:
                if param == field.name:
                    if request.session['requestParams'][param] == 'Not specified':
                        extraQueryString += " AND ( {0} is NULL or {0} = '' ) ".format(param)
                        extraQueryFields.append(param)
                        continue
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
                        val = escape_input(request.session['requestParams'][param])
                        values = val.split('|')
                        query['jeditaskid__in'] = values
                    elif param == 'status':
                        val = escape_input(request.session['requestParams'][param])
                        if '*' not in val and '|' not in val and '!' not in val:
                            values = val.split(',')
                            query['status__in'] = values
                    elif param == 'superstatus':
                        val = escape_input(request.session['requestParams'][param])
                        values = val.split('|')
                        query['superstatus__in'] = values
                    elif param == 'reqid':
                        val = escape_input(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'site':
                        if request.session['requestParams'][param] != 'hpc' and param in extraQueryFields:
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
        else:
            if param == 'jobtype':
                jobtype = request.session['requestParams']['jobtype']
                if jobtype.startswith('anal'):
                    query['prodsourcelabel__in'] = ['panda', 'user', 'rc_alrb', 'rc_test2']
                    query['transformation__startswith'] = 'http'
                elif jobtype.startswith('prod'):
                    query['prodsourcelabel__in'] = ['managed', 'prod_test', 'ptest', 'rc_alrb', 'rc_test2']
                    query['transformation__endswith'] = '.py'
                elif jobtype == 'groupproduction':
                    query['prodsourcelabel'] = 'managed'
                    query['workinggroup__isnull'] = False
                elif jobtype == 'eventservice':
                    query['eventservice'] = 1
                elif jobtype == 'esmerge':
                    query['eventservice'] = 2
                elif jobtype == 'test' or jobtype.find('test') >= 0:
                    query['produsername'] = 'gangarbt'

            for field in Jobsactive4._meta.get_fields():
                if param == field.name:
                    if request.session['requestParams'][param] == 'Not specified':
                        extraQueryString += " AND ( {0} is NULL or {0} = '' ) ".format(param)
                        extraQueryFields.append(param)
                        continue
                    if param == 'minramcount':
                        if 'GB' in request.session['requestParams'][param]:
                            leftlimit, rightlimit = (request.session['requestParams'][param]).split('-')
                            rightlimit = rightlimit[:-2]
                            query['%s__range' % param] = (int(leftlimit) * 1000, int(rightlimit) * 1000 - 1)
                        else:
                            query[param] = int(request.session['requestParams'][param])
                    elif param == 'specialhandling' and not '*' in request.session['requestParams'][param]:
                        query['specialhandling__contains'] = request.session['requestParams'][param]
                    elif param == 'prodsourcelabel':
                        query['prodsourcelabel'] = request.session['requestParams'][param]
                    elif param == 'reqid':
                        val = escape_input(request.session['requestParams'][param])
                        if val.find('|') >= 0:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query['reqid__in'] = values
                        else:
                            query['reqid'] = int(val)
                    elif param == 'transformation' or param == 'transpath':
                        # we cut the transformation path and show only tail
                        query[param + '__contains'] = request.session['requestParams'][param].replace('*', '')
                    elif param == 'modificationhost' and request.session['requestParams'][param].find('@') < 0:
                        paramQuery = request.session['requestParams'][param]
                        if paramQuery[0] == '*': paramQuery = paramQuery[1:]
                        if paramQuery[-1] == '*': paramQuery = paramQuery[:-1]
                        query['%s__contains' % param] = paramQuery
                    elif param == 'jeditaskid' or param == 'taskid':
                        val = escape_input(request.session['requestParams'][param])
                        if '|' in val:
                            values = val.split('|')
                            values = [int(val) for val in values]
                            query[param + '__in'] = values
                        else:
                            query[param] = int(val)
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
                        val = escape_input(request.session['requestParams'][param])
                        values = val.split('|') if '|' in val else val.split(',')
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
                                extraQueryString += " AND not specialhandling like \'%%sc:%%\' "
                            elif request.session['requestParams'][param] == 'not2':
                                extraQueryString += ' AND (eventservice != 2) '
                            elif request.session['requestParams'][param] == 'all':
                                query['eventservice__isnull'] = False
                                continue
                            else:
                                query['eventservice__isnull'] = True
                    elif param == 'corecount' and request.session['requestParams'][param] == '1':
                        extraQueryString += ' AND (corecount = 1 or corecount is NULL) '

                    elif param == 'resourcetype' and request.session['requestParams'][param]:
                        if '|' in request.session['requestParams'][param]:
                            rtypes = request.session['requestParams'][param].split('|')
                            query['resourcetype__in'] = rtypes
                        else:
                            query['resourcetype'] = request.session['requestParams'][param]
                    else:
                        if (param not in wildSearchFields):
                            query[param] = request.session['requestParams'][param]

    if 'region' in request.session['requestParams']:
        region = request.session['requestParams']['region']
        pq_clouds = get_pq_clouds()
        siteListForRegion = []
        for sn, rn in pq_clouds.items():
            if rn == region:
                siteListForRegion.append(str(sn))
        query['computingsite__in'] = siteListForRegion

    if opmode in ['analysis', 'production'] and querytype == 'job':
        if opmode.startswith('analy'):
            query['prodsourcelabel__in'] = ['panda', 'user']
        elif opmode.startswith('prod'):
            query['prodsourcelabel__in'] = ['managed']

    if wildCardExt == False:
        return query

    try:
        extraQueryString += ' AND '
    except NameError:
        extraQueryString = ''

    # wild cards handling
    wildSearchFields = (set(wildSearchFields) & set(list(request.session['requestParams'].keys())))
    # filter out fields that already in query dict
    wildSearchFields1 = set()
    for currenfField in wildSearchFields:
        if not (currenfField.lower() == 'transformation'):
            if not ((currenfField.lower() == 'cloud') & (
            any(card.lower() == 'all' for card in request.session['requestParams'][currenfField].split('|')))):
                if not any(currenfField in key for key, value in query.items()) and currenfField not in extraQueryFields:
                    wildSearchFields1.add(currenfField)
    wildSearchFields = wildSearchFields1

    for i_field, field_name in enumerate(wildSearchFields, start=1):
        extraQueryString += '('
        wildCardsOr = request.session['requestParams'][field_name].split('|')
        if not ((field_name.lower() == 'cloud') & (any(card.lower() == 'all' for card in wildCardsOr))):
            for i_or, card_or in enumerate(wildCardsOr, start=1):
                if ',' in card_or:
                    extraQueryString += '('
                    wildCardsAnd = card_or.split(',')
                    for i_and, card_and in enumerate(wildCardsAnd, start=1):
                        extraQueryString += preprocess_wild_card_string(card_and, field_name)
                        if i_and < len(wildCardsAnd):
                            extraQueryString += ' AND '
                    extraQueryString += ')'
                else:
                    extraQueryString += preprocess_wild_card_string(card_or, field_name)
                if i_or < len(wildCardsOr):
                    extraQueryString += ' OR '

            extraQueryString += ')'
            if i_field < len(wildSearchFields):
                extraQueryString += ' AND '

    if 'jobparam' in request.session['requestParams']:
        jobParWildCards = request.session['requestParams']['jobparam'].split('|')
        jobParCountCards = len(jobParWildCards)
        jobParCurrentCardCount = 1
        extraJobParCondition = '('
        for card in jobParWildCards:
            extraJobParCondition += preprocess_wild_card_string(escape_input(card), 'JOBPARAMETERS')
            if (jobParCurrentCardCount < jobParCountCards): extraJobParCondition += ' OR '
            jobParCurrentCardCount += 1
        extraJobParCondition += ')'

        pandaIDs = []
        jobParamQuery = {'modificationtime__castdate__range': [
            startdate.strftime(settings.DATETIME_FORMAT),
            enddate.strftime(settings.DATETIME_FORMAT)]}

        jobs = Jobparamstable.objects.filter(**jobParamQuery).extra(where=[extraJobParCondition]).values('pandaid')
        for values in jobs:
            pandaIDs.append(values['pandaid'])

        query['pandaid__in'] = pandaIDs

    if extraQueryString.endswith(' AND '):
        extraQueryString = extraQueryString[:-5]

    if (len(extraQueryString) < 2):
        extraQueryString = '1=1'

    return (query, extraQueryString, LAST_N_HOURS_MAX)


def mainPage(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    setupView(request)

    debuginfo = None
    if request.session['debug']:
        debuginfo = "<h2>Debug info</h2>"
        for name in dir(settings):
            debuginfo += "%s = %s<br>" % (name, getattr(settings, name))
        debuginfo += "<br>******* Environment<br>"
        for env in os.environ:
            debuginfo += "%s = %s<br>" % (env, os.environ[env])

    if not is_json_request(request):
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'debuginfo': debuginfo,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render_to_response('core-mainPage.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse('json', content_type='text/html')


def helpPage(request):
    valid, response = initRequest(request)
    if not valid: return response
    setupView(request)
    del request.session['TFIRST']
    del request.session['TLAST']

    acronyms = {
        'panda': 'PanDA',
        'art': 'ART',
        'api': 'API',
        'qa': 'Q&A',
        'idds': 'iDDS',
        'gs': 'Global Shares',
        'wn': 'WN',
    }

    # find all help templates
    template_files = []
    for template_dir in (tuple(settings.TEMPLATES[0]['DIRS']) + get_app_template_dirs('templates')):
        for dir, dirnames, filenames in os.walk(template_dir):
            for filename in filenames:
                if filename.endswith('Help.html'):
                    template_files.append(filename)
    template_files = sorted(list(set(template_files)))
    # group by object
    camel_case_regex = "(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])"
    help_template_dict = {}
    for tfn in template_files:
        tfn_words = re.split(camel_case_regex, tfn)
        tfn_words_humanized = []
        for w in tfn_words:
            if w.lower() in acronyms:
                tfn_words_humanized.append(acronyms[w.lower()])
            else:
                tfn_words_humanized.append(w.title())
        if tfn_words[0] not in help_template_dict:
            help_template_dict[tfn_words[0]] = {
                'key': tfn_words[0],
                'template_names': [],
                'anchor': tfn_words[0],
                'title': tfn_words_humanized[0],
            }
        help_template_dict[tfn_words[0]]['template_names'].append({
            'name': tfn,
            'title': ' '.join([word for word in tfn_words_humanized[:-1]]),
            'anchor': tfn.replace('.html', '')
        })
    help_template_list = list(help_template_dict.values())
    # move introduction help to the beginning
    help_template_list.insert(0, help_template_list.pop(min([i for i, d in enumerate(help_template_list) if d['key'].lower() == 'introduction'])))

    if not is_json_request(request):
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'templates': help_template_list,
        }
        response = render_to_response('help.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse('json', content_type='text/html')


def jobParamList(request):
    idlist = []
    if 'pandaid' in request.session['requestParams']:
        idstring = request.session['requestParams']['pandaid']
        idstringl = idstring.split(',')
        for pid in idstringl:
            idlist.append(int(pid))
    query = {'pandaid__in': idlist}
    jobparams = Jobparamstable.objects.filter(**query).values()
    if is_json_request(request):
        return HttpResponse(json.dumps(jobparams, cls=DateEncoder), content_type='application/json')
    else:
        return HttpResponse('not supported', content_type='text/html')


@login_customrequired
def jobList(request, mode=None, param=None):
    valid, response = initRequest(request)
    if not valid:
        return response

    dkey = digkey(request)
    thread = None

    # Here we try to get data from cache
    data = getCacheEntry(request, "jobList")
    if data is not None:
        data = json.loads(data)
        if 'istestmonitor' in request.session['requestParams'] and request.session['requestParams']['istestmonitor'] == 'yes':
            return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        data['request'] = request
        if data['eventservice'] == True:
            response = render_to_response('jobListES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobList.html', data, content_type='text/html')
        _logger.info('Rendered template with data from cache: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'dump' in request.session['requestParams'] and request.session['requestParams']['dump'] == 'parameters':
        return jobParamList(request)

    is_job_meta_required = False
    if 'fields' in request.session['requestParams'] and request.session['requestParams']['fields'] and 'metastruct' in request.session['requestParams']['fields']:
        is_job_meta_required = True

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
    elif 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
        try:
            jeditaskid = int(request.session['requestParams']['jeditaskid'])
        except:
            jeditaskid = None
        if jeditaskid:
            eventservice = is_event_service_task(jeditaskid)

    noarchjobs = False
    if 'noarchjobs' in request.session['requestParams'] and request.session['requestParams']['noarchjobs'] == '1':
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
                (select pandaid from atlas_panda.filestable4 
                    where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                union all
                (select pandaid from atlas_pandaarch.filestable_arch 
                    where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                ) """.format(jeditaskid, datasetid, fileid, jeditaskid, datasetid, fileid)

        if 'ecstate' in request.session['requestParams'] and tk and datasetid:
            extraquery_files += """
                pandaid in (
                    (select pandaid from atlas_panda.filestable4 where jeditaskid = {} and datasetid in ( {} ) 
                        and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    union all 
                    (select pandaid from atlas_pandaarch.filestable_arch where jeditaskid = {} and datasetid in ( {} ) 
                        and fileid in (select id from atlas_pandabigmon.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    ) """.format(jeditaskid, datasetid, tk, jeditaskid, datasetid, tk)
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

    extraquery_tasks = ' '
    if 'taskname' in request.session['requestParams'] and 'username' in request.session['requestParams']:
        taskname = request.session['requestParams']['taskname']
        taskusername = request.session['requestParams']['username']

        if taskname.find('*') != -1:
           taskname = taskname.replace('*', '%%')

        if taskusername.find('*') != -1:
           taskusername = taskusername.replace('*', '%%')

        extraquery_tasks += """
            jeditaskid in (
            select jeditaskid from atlas_panda.jedi_tasks where taskname like '{}' and username like '{}'
            ) """.format(taskname, taskusername)

    _logger.debug('Specific params processing: {}'.format(time.time() - request.session['req_init_time']))

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)

    _logger.debug('Setup view: {}'.format(time.time() - request.session['req_init_time']))

    if len(extraquery_files) > 1:
        wildCardExtension += ' AND ' + extraquery_files

    if len(extraquery_tasks) > 1:
        wildCardExtension += ' AND ' + extraquery_tasks

    if query == 'reqtoken' and wildCardExtension is None and LAST_N_HOURS_MAX is None:
        data = {
            'desc': 'Request token is not found or data is outdated. Please reload the original page.',
        }
        return render_to_response('message.html', data, content_type='text/html')

    jobs = []

    if is_json_request(request):
        values = [f.name for f in Jobsactive4._meta.get_fields()]
    else:
        values = [
            'corecount', 'jobsubstatus', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus',
            'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease',
            'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime',
            'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag',
            'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode',
            'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag',
            'transexitcode', 'destinationse', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr',
            'maxattempt', 'jobname', 'computingelement', 'proddblock', 'destinationdblock', 'reqid', 'minramcount',
            'statechangetime',  'nevents', 'jobmetrics',
            'noutputdatafiles', 'parentid', 'actualcorecount', 'schedulerid', 'pilotid', 'commandtopilot',
            'cmtconfig', 'maxpss']
    if not eventservice:
        values.extend(['avgvmem', 'maxvmem', 'maxrss'])

    if settings.DEPLOYMENT != "POSTGRES":
        values.append('nucleus')
        values.append('eventservice')
        values.append('gshare')
        values.append('resourcetype')
        values.append('container_name')

    totalJobs = 0
    showTop = 0
    if 'limit' in request.session['requestParams']:
        request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
    JOB_LIMIT = request.session['JOB_LIMIT']
    job_final_states = ['finished', 'failed', 'cancelled', 'closed', 'merging']
    harvesterjobstatus = ''

    from core.harvester.views import getHarvesterJobs, getCeHarvesterJobs

    if 'jobstatus' in request.session['requestParams']:
        harvesterjobstatus = request.session['requestParams']['jobstatus']
    if 'transferringnotupdated' in request.session['requestParams']:
        jobs = stateNotUpdated(request, state='transferring', values=values, wildCardExtension=wildCardExtension)
    elif 'statenotupdated' in request.session['requestParams']:
        jobs = stateNotUpdated(request, values=values, wildCardExtension=wildCardExtension)
    elif 'harvesterinstance' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request,
                                instance=request.session['requestParams']['harvesterinstance'],
                                workerid=request.session['requestParams']['workerid'],
                                jobstatus=harvesterjobstatus,
                                fields=values)
    elif 'harvesterid' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request,
                                instance=request.session['requestParams']['harvesterid'],
                                workerid=request.session['requestParams']['workerid'],
                                jobstatus=harvesterjobstatus,
                                fields=values)
    elif ('harvesterinstance' not in request.session['requestParams'] and 'harvesterid' not in request.session['requestParams']) and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(request,
                                workerid=request.session['requestParams']['workerid'],
                                jobstatus=harvesterjobstatus,
                                fields=values)
    elif 'harvesterce' in request.session['requestParams']:
        jobs = getCeHarvesterJobs(request, computingelment=request.session['requestParams']['harvesterce'])
    else:
        # exclude time from query for DB tables with active jobs
        etquery = copy.deepcopy(query)
        if ('modificationtime__castdate__range' in etquery and len(set(['date_to', 'hours']).intersection(request.session['requestParams'].keys())) == 0) or (
                'jobstatus' in request.session['requestParams'] and is_job_active(request.session['requestParams']['jobstatus'])):
            del etquery['modificationtime__castdate__range']
            warning['notimelimit'] = "no time window limiting was applied for active jobs in this selection"

        jobs.extend(Jobsdefined4.objects.filter(**etquery).extra(where=[wildCardExtension])[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**etquery).extra(where=[wildCardExtension])[:JOB_LIMIT].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**etquery).extra(where=[wildCardExtension])[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension])[:JOB_LIMIT].values(*values))
        listJobs = [Jobsarchived4, Jobsactive4, Jobswaiting4, Jobsdefined4]
        if not noarchjobs:
            queryFrozenStates = []
            if 'jobstatus' in request.session['requestParams']:
                queryFrozenStates = list(set(request.session['requestParams']['jobstatus'].split('|')).intersection(job_final_states))
            # hard limit is set to 20K
            if 'jobstatus' not in request.session['requestParams'] or len(queryFrozenStates) > 0:
                if 'limit' not in request.session['requestParams']:
                    if 'jeditaskid' not in request.session['requestParams']:
                        request.session['JOB_LIMIT'] = 20000
                        JOB_LIMIT = 20000
                        showTop = 1
                    else:
                        request.session['JOB_LIMIT'] = 200000
                        JOB_LIMIT = 200000
                else:
                    request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
                    JOB_LIMIT = int(request.session['requestParams']['limit'])

                if 'modificationtime__castdate__range' in query and (
                        (datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0], settings.DATETIME_FORMAT)).days > 2 or
                        (datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1], settings.DATETIME_FORMAT)).days > 2):
                    if 'jeditaskid' in request.session['requestParams'] or (is_json_request(request) and (
                            'fulllist' in request.session['requestParams'] and request.session['requestParams']['fulllist'] == 'true')):
                        del query['modificationtime__castdate__range']
                    archJobs = Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[:JOB_LIMIT].values(*values)
                    listJobs.append(Jobsarchived)
                    totalJobs = len(archJobs)
                    jobs.extend(archJobs)
        if not is_json_request(request):
            thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension, dkey))
            thread.start()
        else:
            thread = None

    _logger.info('Got jobs: {}'.format(time.time() - request.session['req_init_time']))

    # If the list is for a particular JEDI task, filter out the jobs superseded by retries
    taskids = {}
    for job in jobs:
        if 'jeditaskid' in job:
            taskids[job['jeditaskid']] = 1

    # if ES -> nodrop by default
    dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'drop':
        dropmode = True
    if ('mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'nodrop') or eventservice:
        dropmode = False

    isReturnDroppedPMerge = False
    if 'processingtype' in request.session['requestParams'] and request.session['requestParams']['processingtype'] == 'pmerge':
        isReturnDroppedPMerge = True

    droplist = []
    droppedPmerge = set()
    cntStatus = []
    if dropmode and (len(taskids) == 1):
        jobs, droplist, droppedPmerge = drop_job_retries(jobs, list(taskids.keys())[0], is_return_dropped_jobs= isReturnDroppedPMerge)
        _logger.debug('Done droppping if was requested: {}'.format(time.time() - request.session['req_init_time']))

    # get attempts of file if fileid in request params
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
        _logger.debug('Got file attempts: {}'.format(time.time() - request.session['req_init_time']))

    jobs = clean_job_list(request, jobs, do_add_metadata=is_job_meta_required, do_add_errorinfo=True)
    _logger.debug('Cleaned job list: {}'.format(time.time() - request.session['req_init_time']))

    jobs = reconstruct_job_consumers(jobs)
    _logger.debug('Reconstructed consumers: {}'.format(time.time() - request.session['req_init_time']))

    njobs = len(jobs)
    jobtype = ''
    if 'jobtype' in request.session['requestParams']:
        jobtype = request.session['requestParams']['jobtype']
    elif '/analysis' in request.path:
        jobtype = 'analysis'
    elif '/production' in request.path:
        jobtype = 'production'

    if 'display_limit' in request.session['requestParams']:
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

        if sortby == 'create-ascending':
            jobs = sorted(jobs, key=lambda x:x['creationtime'] if not x['creationtime'] is None else datetime(1900, 1, 1))
        if sortby == 'create-descending':
            jobs = sorted(jobs, key=lambda x:x['creationtime'] if not x['creationtime'] is None else datetime(1900, 1, 1), reverse=True)
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
    elif 'computingsite' in request.session['requestParams']:
        sortby = 'time-descending'
        jobs = sorted(jobs, key=lambda x: x['modificationtime'] if x['modificationtime'] is not None else datetime(1900, 1, 1), reverse=True)
    else:
        sortby = "attemptnr-descending,pandaid-descending"
        jobs = sorted(jobs, key=lambda x: [-x['attemptnr'],-x['pandaid']])
    _logger.debug('Sorted joblist: {}'.format(time.time() - request.session['req_init_time']))

    taskname = ''
    if 'jeditaskid' in request.session['requestParams'] and '|' not in request.session['requestParams']['jeditaskid']:
        taskname = get_task_name_by_taskid(request.session['requestParams']['jeditaskid'])
    if 'taskid' in request.session['requestParams'] and '|' not in request.session['requestParams']['taskid']:
        taskname = get_task_name_by_taskid(request.session['requestParams']['taskid'])

    if 'produsername' in request.session['requestParams']:
        user = request.session['requestParams']['produsername']
    elif 'user' in request.session['requestParams']:
        user = request.session['requestParams']['user']
    else:
        user = None
    _logger.debug('Got task and user names: {}'.format(time.time() - request.session['req_init_time']))

    # show warning or not
    if njobs <= request.session['JOB_LIMIT']:
        showwarn = 0
    else:
        showwarn = 1

    # Sort in order to see the most important tasks
    sumd, esjobdict = job_summary_dict(request, jobs, standard_fields+['corecount', 'noutputdatafiles', 'actualcorecount', 'schedulerid', 'pilotversion', 'computingelement', 'container_name', 'nevents'])
    if sumd:
        for item in sumd:
            if item['field'] == 'jeditaskid':
                item['list'] = sorted(item['list'], key=lambda k: k['kvalue'], reverse=True)

    _logger.debug('Built standard params attributes summary: {}'.format(time.time() - request.session['req_init_time']))

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
    if 'prodsourcelabel' in request.session['requestParams'] and request.session['requestParams']['prodsourcelabel'].lower().find('test') >= 0:
        testjobs = True

    errsByCount, _, _, _, errdSumd, _ = errorSummaryDict(request, jobs, testjobs, output=['errsByCount', 'errdSumd'])
    _logger.debug('Built error summary: {}'.format(time.time() - request.session['req_init_time']))
    errsByMessage = get_error_message_summary(jobs)
    _logger.debug('Built error message summary: {}'.format(time.time() - request.session['req_init_time']))

    if not is_json_request(request):

        # Here we getting extended data for list of jobs to be shown
        jobsToShow = jobs[:njobsmax]
        from core.libs import exlib
        try:
            jobsToShow = exlib.fileList(jobsToShow)
        except Exception as e:
            _logger.error(e)
        _logger.debug(
            'Got file info for list of jobs to be shown: {}'.format(time.time() - request.session['req_init_time']))

        # Getting PQ status for for list of jobs to be shown
        pq_dict = get_panda_queues()
        for job in jobsToShow:
            if job['computingsite'] in pq_dict:
                job['computingsitestatus'] = pq_dict[job['computingsite']]['status']
                job['computingsitecomment'] = pq_dict[job['computingsite']]['comment']
        _logger.debug('Got extra params for sites: {}'.format(time.time() - request.session['req_init_time']))

        # closing thread for counting total jobs in DB without limiting number of rows selection
        if thread is not None:
            try:
                thread.join()
                jobsTotalCount = sum(tcount[dkey])
                _logger.debug(dkey)
                _logger.debug(tcount[dkey])
                del tcount[dkey]
                _logger.debug(tcount)
                _logger.info("Total number of jobs in DB: {}".format(jobsTotalCount))
            except:
                jobsTotalCount = -1
        else:
            jobsTotalCount = -1

        listPar = []
        for key, val in request.session['requestParams'].items():
            if key not in ('limit', 'display_limit'):
                listPar.append(key + '=' + str(val))
        if len(listPar) > 0:
            urlParametrs = '&'.join(listPar) + '&'
        else:
            urlParametrs = None
        _logger.info(listPar)
        del listPar
        if math.fabs(njobs - jobsTotalCount) < 1000 or jobsTotalCount == -1:
            jobsTotalCount = None
        else:
            jobsTotalCount = int(math.ceil((jobsTotalCount + 10000) / 10000) * 10000)
        _logger.debug('Total jobs count thread finished: {}'.format(time.time() - request.session['req_init_time']))

        # datetime type -> str in order to avoid encoding errors on template
        datetime_job_param_names = ['creationtime', 'modificationtime', 'starttime', 'statechangetime', 'endtime']
        for job in jobsToShow:
            for dtp in datetime_job_param_names:
                if job[dtp]:
                    job[dtp] = job[dtp].strftime(settings.DATETIME_FORMAT)

        # comparison of objects
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
        _logger.debug('Got comparison job list for user: {}'.format(time.time() - request.session['req_init_time']))

        # set up google flow diagram
        flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)
        _logger.debug('Built google flow diagram: {}'.format(time.time() - request.session['req_init_time']))

        xurl = extensibleURL(request)
        time_locked_url = removeParam(removeParam(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
                          'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
                          '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
        nodurminurl = removeParam(xurl, 'durationmin', mode='extensible')
        nosorturl = removeParam(xurl, 'sortby', mode='extensible')
        nosorturl = removeParam(nosorturl, 'display_limit', mode='extensible')
        #xurl = removeParam(nosorturl, 'mode', mode='extensible')
        xurl = nosorturl

        # check if there are jobs exceeding timewindow and add warning message
        if math.floor((request.session['TLAST'] - request.session['TFIRST']).total_seconds()) > LAST_N_HOURS_MAX * 3600:
            warning['timelimitexceeding'] = """
            Some of jobs in this listing are outside of the default 'last {} hours' time window, 
            because this limit was applied to jobs in final state only. Please explicitly add &hours=N to URL, 
            if you want to force applying the time window limit on active jobs also.""".format(LAST_N_HOURS_MAX)
        _logger.debug('Extra data preporation done: {}'.format(time.time() - request.session['req_init_time']))

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
            'tfirst': request.session['TFIRST'].strftime(settings.DATETIME_FORMAT),
            'tlast': request.session['TLAST'].strftime(settings.DATETIME_FORMAT),
            'plow': request.session['PLOW'],
            'phigh': request.session['PHIGH'],
            'showwarn': showwarn,
            'joblimit': request.session['JOB_LIMIT'],
            'limit': JOB_LIMIT,
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
            'eventservice': eventservice,
            'jobsTotalCount': jobsTotalCount,
            'requestString': urlParametrs,
            'built': datetime.now().strftime("%H:%M:%S"),
            'clist': clist,
            'warning': warning,
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobList", json.dumps(data, cls=DateEncoder), 60 * 20)

        _logger.debug('Cache was set: {}'.format(time.time() - request.session['req_init_time']))

        if eventservice:
            response = render_to_response('jobListES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobList.html', data, content_type='text/html')

        _logger.info('Rendered template: {}'.format(time.time() - request.session['req_init_time']))
        request = complete_request(request)

        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        if 'datasets' in request.session['requestParams']:
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

                    dsets = JediDatasets.objects.filter(dsquery).extra(
                        select={"dummy1": '/*+ INDEX_RS_ASC(ds JEDI_DATASETS_PK) */ 1 '}).values()
                    if len(dsets) > 0:
                        for ds in dsets:
                            for file in files:
                                if 'DSQuery' in file and file['DSQuery']['jeditaskid'] == ds['jeditaskid'] and \
                                        file['DSQuery']['datasetid'] == ds['datasetid']:
                                    file['dataset'] = ds['datasetname']
                                    del file['DSQuery']

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
            _logger.info('Got dataset and file info if requested: {}'.format(time.time() - request.session['req_init_time']))

        if 'fields' in request.session['requestParams'] and len(jobs) > 0:
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
        # cache json response for particular usage (HC test monitor for RU)
        if 'istestmonitor' in request.session['requestParams'] and request.session['requestParams']['istestmonitor'] == 'yes':
            setCacheEntry(request, "jobList", json.dumps(data, cls=DateEncoder), 60 * 10)
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

@never_cache
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
    if not is_event_service(job):
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
    jobs = clean_job_list(request, jobs, do_add_metadata=False, do_add_errorinfo=True)

    errors = {}
    for job in jobs:
        errors[job['pandaid']] = job['errorinfo']

    response = render_to_response('jobDescentErrors.html', {'errors': errors}, content_type='text/html')
    request = complete_request(request)
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
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "jobInfo")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if data['eventservice'] is True:
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
        jobs = clean_job_list(request, jobs, do_add_metadata=True, do_add_errorinfo=True)

    if len(jobs) == 0:
        data = {
            'prefix': getPrefix(request),
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'pandaid': pandaid,
            'job': None,
            'jobid': jobid,
        }
        response = render_to_response('jobInfo.html', data, content_type='text/html')
        request = complete_request(request)
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
            if len(rucioUserName) > 1:
                rucio_username_unique = {}
                for un in rucioUserName:
                    if isinstance(un, dict):
                        if 'rucio_account' in un and un['rucio_account']:
                            rucio_username_unique[un['rucio_account']] = 1
                    elif isinstance(un, str):
                        rucio_username_unique[un] = 1
                rucioUserName = list(rucio_username_unique.keys())
        else:
            rucioUserName = [jobs[0]['produserid']]

    job = {}
    try:
        job = jobs[0]
    except IndexError:
        _logger.info('No job found for: {}'.format(jobid))

    tquery = {}
    tquery['jeditaskid'] = job['jeditaskid']
    tquery['storagetoken__isnull'] = False
    storagetoken = JediDatasets.objects.filter(**tquery).values('storagetoken')
    if storagetoken:
        job['destinationse'] = storagetoken[0]['storagetoken']

    pandaid = job['pandaid'] if 'pandaid' in job else -1
    colnames = job.keys()
    colnames = sorted(colnames)
    produsername = ''
    for k in colnames:
        if is_timestamp(k):
            try:
                val = job[k].strftime(settings.DATETIME_FORMAT)
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

    # get Harvester info
    if 'core.harvester' in settings.INSTALLED_APPS:
        from core.harvester.utils import isHarvesterJob
        job['harvesterInfo'] = isHarvesterJob(job['pandaid'])
    if 'harvesterInfo' in job and job['harvesterInfo'] and len(job['harvesterInfo']) > 0:
        job['harvesterInfo'] = job['harvesterInfo'][0]
    else:
        job['harvesterInfo'] = {}

    try:
        # Check for logfile extracts
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
    panda_queues = get_panda_queues()
    computeSvsAtlasS = get_pq_atlas_sites()
    if 'nofiles' not in request.session['requestParams']:
        # Get job files. First look in JEDI datasetcontents
        _logger.info("Pulling file info")
        files.extend(Filestable4.objects.filter(pandaid=pandaid).order_by('type').values())
        if len(files) == 0:
            files.extend(FilestableArch.objects.filter(pandaid=pandaid).order_by('type').values())
        ninput = 0
        noutput = 0
        npseudo_input = 0
        if len(files) > 0:
            dquery = {}
            dquery['datasetid__in'] = [f['datasetid'] for f in files]
            dsets = JediDatasets.objects.filter(**dquery).values('datasetid', 'datasetname')
            datasets_dict = {ds['datasetid']: ds['datasetname'] for ds in dsets}

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
                                if logBucketID in [45, 41, 105, 106, 42, 61, 103, 2, 82, 101, 117, 115]:  # Bucket Codes for S3 destination
                                    f['destination'] = 'S3'
                if f['type'] == 'pseudo_input': npseudo_input += 1
                f['fsizemb'] = round(convert_bytes(f['fsize'], output_unit='MB'), 2)

                if f['datasetid'] in datasets_dict:
                    f['datasetname'] = datasets_dict[f['datasetid']]
                    if f['scope'] + ":" in f['datasetname']:
                        f['ruciodatasetname'] = f['datasetname'].split(":")[1]
                    else:
                        f['ruciodatasetname'] = f['datasetname']
                    if job['computingsite'] in panda_queues:
                        if job['computingsite'] in ('CERN-P1'):
                            f['ddmsite'] = panda_queues[job['computingsite']]['gocname']
                        else:
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
                if i == 'input':
                    fileSummary += ', size: ' + inputFilesSize + ' (MB)'
                fileSummary += '; '
            fileSummary = fileSummary[:-2]

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
    inputfiles = []
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
            inputfiles.append({'jeditaskid': file['jeditaskid'], 'datasetid': file['datasetid'], 'fileid': file['fileid']})

    if 'pilotid' in job and job['pilotid'] and job['pilotid'].startswith('http') and '{' not in job['pilotid']:
        stdout = job['pilotid'].split('|')[0]
        if stdout.endswith('pilotlog.txt'):
           stdlog = stdout.replace('pilotlog.txt', 'payload.stdout')
           stderr = stdout.replace('pilotlog.txt', 'payload.stderr')
           stdjdl = None
        else:
            stderr = stdout.replace('.out', '.err')
            stdlog = stdout.replace('.out', '.log')
            stdjdl = stdout.replace('.out', '.jdl')
            stdlog = stdout.replace('.out', '.log')
    elif len(job['harvesterInfo']) > 0 and 'batchlog' in job['harvesterInfo'] and job['harvesterInfo']['batchlog']:
        stdlog = job['harvesterInfo']['batchlog']
        stderr = stdlog.replace('.log', '.err')
        stdout = stdlog.replace('.log', '.out')
        stdjdl = stdlog.replace('.log', '.jdl')
    else:
        stdout = stderr = stdlog = stdjdl = None

    # Check for object store based log
    oslogpath = None
    pq_object_store_paths = get_pq_object_store_path()
    if 'computingsite' in job and job['computingsite'] in pq_object_store_paths:
        ospath = pq_object_store_paths[job['computingsite']]
        if 'lfn' in logfile:
            if ospath.endswith('/'):
                oslogpath = ospath + logfile['lfn']
            else:
                oslogpath = ospath + '/' + logfile['lfn']

    # Check for debug info
    debugmode = is_debug_mode(job)
    debugstdout = None
    if debugmode:
        if 'showdebug' in request.session['requestParams']:
            debugstdoutrec = Jobsdebug.objects.filter(pandaid=pandaid).values()
            if len(debugstdoutrec) > 0:
                if 'stdout' in debugstdoutrec[0]:
                    debugstdout = debugstdoutrec[0]['stdout']

    # Get job parameters
    _logger.info("getting job parameters")
    jobparamrec = Jobparamstable.objects.filter(pandaid=pandaid)
    jobparams = None
    if len(jobparamrec) > 0:
        jobparams = jobparamrec[0].jobparameters
    # else:
    #    jobparamrec = JobparamstableArch.objects.filter(pandaid=pandaid)
    #    if len(jobparamrec) > 0:
    #        jobparams = jobparamrec[0].jobparameters

    esjobstr = ''
    evtable = []
    if is_event_service(job):
        # for ES jobs, prepare the event table
        esjobdict = {}
        for s in eventservicestatelist:
            esjobdict[s] = 0
        evalues = 'fileid', 'datasetid', 'def_min_eventid', 'def_max_eventid', 'processed_upto_eventid', 'status', 'job_processid', 'attemptnr', 'eventoffset'
        evtable.extend(JediEvents.objects.filter(pandaid=job['pandaid']).order_by('-def_min_eventid').values(*evalues))
        for evrange in evtable:
            evrange['status'] = eventservicestatelist[evrange['status']]
            esjobdict[evrange['status']] += evrange['def_max_eventid'] - evrange['def_min_eventid'] + 1
            evrange['attemptnr'] = 10 - evrange['attemptnr']

        esjobstr = ''
        for s in esjobdict:
            if esjobdict[s] > 0:
                esjobstr += " {} ({}) ".format(s, esjobdict[s])
    else:
        evtable = []

    # jobset info
    jobsetinfo = {}
    if ('jobset' in request.session['requestParams'] or is_event_service(job)) and 'jobsetid' in job and job['jobsetid'] > 0:
        jobs = []
        jsquery = {
            'jobsetid': job['jobsetid'],
            'produsername': job['produsername'],
        }
        jvalues = ['pandaid', 'prodsourcelabel', 'processingtype', 'transformation', 'eventservice', 'jobstatus']
        jobs.extend(Jobsdefined4.objects.filter(**jsquery).values(*jvalues))
        jobs.extend(Jobsactive4.objects.filter(**jsquery).values(*jvalues))
        jobs.extend(Jobswaiting4.objects.filter(**jsquery).values(*jvalues))
        jobs.extend(Jobsarchived4.objects.filter(**jsquery).values(*jvalues))
        jobs.extend(Jobsarchived.objects.filter(**jsquery).values(*jvalues))

        jobs = add_job_category(jobs)
        job_summary_list = job_states_count_by_param(jobs, param='category')
        for row in job_summary_list:
            jobsetinfo[row['value']] = sum([jss['count'] for jss in row['job_state_counts']])

    # For CORE, pick up parameters from jobparams
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

    if 'transformation' in job and job['transformation'] is not None and job['transformation'].startswith('http'):
        job['transformation'] = "<a href='%s'>%s</a>" % (job['transformation'], job['transformation'].split('/')[-1])

    if 'metastruct' in job:
        job['metadata'] = json.dumps(job['metastruct'], sort_keys=True, indent=4, separators=(',', ': '))

    if job['creationtime']:
        creationtime = job['creationtime']
        now = datetime.now()
        tdelta = now - creationtime
        job['days_since_creation'] = int(tdelta.days) + 1

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
    if 'core.art' in settings.INSTALLED_APPS and settings.DEPLOYMENT == 'ORACLE_ATLAS':
        try:
            from core.art.modelsART import ARTTests
            artqueue = {'pandaid': pandaid}
            art_test.extend(ARTTests.objects.filter(**artqueue).values('pandaid', 'testname'))
        except ImportError:
            _logger.exception('Failed to import ARTTests model')

    # datetime type -> str in order to avoid encoding errors on template
    datetime_job_param_names = ['creationtime', 'modificationtime', 'starttime', 'statechangetime', 'endtime']
    datetime_file_param_names = ['creationdate', 'modificationtime']
    if job:
        for dtp in datetime_job_param_names:
            if job[dtp]:
                job[dtp] = job[dtp].strftime(settings.DATETIME_FORMAT)
    for f in files:
        for fp, fpv in f.items():
            if fp in datetime_file_param_names and fpv is not None:
                f[fp] = f[fp].strftime(settings.DATETIME_FORMAT)
            if fpv is None:
                f[fp] = ''

    prmon_logs = {}
    if settings.PRMON_LOGS_DIRECTIO_LOCATION and job.get('jobstatus') in ('finished', 'failed'):
        prmon_logs['prmon_summary'] = settings.PRMON_LOGS_DIRECTIO_LOCATION.format(
            queue_name=job.get('computingsite'),
            panda_id=pandaid) + '/memory_monitor_summary.json'
        prmon_logs['prmon_details'] = settings.PRMON_LOGS_DIRECTIO_LOCATION.format(
            queue_name=job.get('computingsite'),
            panda_id=pandaid) + '/memory_monitor_output.txt'

    if not is_json_request(request):
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
            'eventservice': is_event_service(job),
            'evtable': evtable[:1000],
            'debugmode': debugmode,
            'debugstdout': debugstdout,
            'jobsetinfo': jobsetinfo,
            'esjobstr': esjobstr,
            'fileSummary': fileSummary,
            'built': datetime.now().strftime("%H:%M:%S"),
            'produsername': produsername,
            'isincomparisonlist': isincomparisonlist,
            'clist': clist,
            'inputfiles': inputfiles,
            'rucioUserName': rucioUserName,
            'prmon_logs': prmon_logs
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobInfo", json.dumps(data, cls=DateEncoder), 60 * 20)
        if is_event_service(job):
            response = render_to_response('jobInfoES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    elif is_json_request(request):
        del request.session['TFIRST']
        del request.session['TLAST']

        dsfiles = []
        if len(evtable) > 0:
            fileids = {}
            for evrange in evtable:
               fileids[int(evrange['fileid'])] = {}
            flist = []
            for f in fileids:
                flist.append(f)
            dsfiles.extend(JediDatasetContents.objects.filter(fileid__in=flist).values())

        data = {
            'files': files,
            'job': job,
            'dsfiles': dsfiles,
        }

        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')
    else:
        del request.session['TFIRST']
        del request.session['TLAST']
        return HttpResponse('not understood', content_type='text/html')


@never_cache
def get_job_relationships(request, pandaid=-1):
    """
    Getting job relationships in both directions: downstream (further retries); upstream (past retries).
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    direction = ''
    if 'direction' in  request.session['requestParams'] and request.session['requestParams']['direction']:
        direction = request.session['requestParams']['direction']

    job = {}
    jobs = []
    jquery = {
        'pandaid': pandaid,
    }
    jvalues = ['pandaid', 'jeditaskid', 'jobsetid', 'specialhandling', 'eventservice']
    jobs.extend(Jobsdefined4.objects.filter(**jquery).values(*jvalues))
    jobs.extend(Jobsactive4.objects.filter(**jquery).values(*jvalues))
    jobs.extend(Jobswaiting4.objects.filter(**jquery).values(*jvalues))
    jobs.extend(Jobsarchived4.objects.filter(**jquery).values(*jvalues))
    if len(jobs) == 0:
        jobs.extend(Jobsarchived.objects.filter(**jquery).values(*jvalues))
    try:
        job = jobs[0]
    except IndexError:
        _logger.exception('No job found with pandaid: {}'.format(pandaid))

    message = ''
    job_relationships = []

    countOfInvocations = []
    # look for job retries
    if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0:
        if direction == 'downstream':
            retries = []
            if not is_event_service(job):
                retryquery = {
                    'jeditaskid': job['jeditaskid'],
                    'oldpandaid': job['pandaid'],
                }
                job_relationships.extend(JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values())
            else:
                job_relationships = getSequentialRetries_ESupstream(job['pandaid'], job['jobsetid'], job['jeditaskid'], countOfInvocations)
        elif direction == 'upstream':
            if not is_event_service(job):
                job_relationships = getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations)
            else:
                job_relationships = getSequentialRetries_ES(job['pandaid'], job['jobsetid'], job['jeditaskid'], countOfInvocations)
        else:
            message = 'Wrong direction provided, it should be up or down stream.'
    else:
        job_relationships = None

    countOfInvocations = len(countOfInvocations)

    data = {
        'retries': job_relationships,
        'direction': direction,
        'message': message,
        'countOfInvocations': countOfInvocations,
    }
    response = render_to_response('jobRelationships.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=-1)
    return response


@login_customrequired
def userList(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "userList")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('userList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

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
        startdate = startdate.strftime(settings.DATETIME_FORMAT)
        query = {'lastmod__gte': startdate}
        userdb.extend(Users.objects.filter(**query).values())
        anajobs = 0
        n1000 = 0
        n10k = 0
        nrecent3 = 0
        nrecent7 = 0
        nrecent30 = 0
        nrecent90 = 0
        ## Move to a list of dicts and adjust CPU unit
        for u in userdb:
            u['latestjob'] = u['lastmod']
            udict = {}
            udict['name'] = u['name']
            udict['njobsa'] = u['njobsa'] if u['njobsa'] is not None else 0
            udict['cpua1'] = round(u['cpua1'] / 3600.) if u['cpua1'] is not None else 0
            udict['cpua7'] = round(u['cpua7'] / 3600.) if u['cpua7'] is not None else 0
            udict['cpup1'] = round(u['cpup1'] / 3600.) if u['cpup1'] is not None else 0
            udict['cpup7'] = round(u['cpup7'] / 3600.) if u['cpup7'] is not None else 0
            if u['latestjob']:
                udict['latestjob'] = u['latestjob'].strftime(settings.DATETIME_FORMAT)
                udict['lastmod'] = u['lastmod'].strftime(settings.DATETIME_FORMAT)
            userdbl.append(udict)

            if u['njobsa'] is not None:
                if u['njobsa'] > 0:
                    anajobs += u['njobsa']
                if u['njobsa'] >= 1000:
                    n1000 += 1
                if u['njobsa'] >= 10000:
                    n10k += 1
            if u['latestjob'] is not None:
                latest = timezone.now() - u['latestjob']
                if latest.days < 4:
                    nrecent3 += 1
                if latest.days < 8:
                    nrecent7 += 1
                if latest.days < 31:
                    nrecent30 += 1
                if latest.days < 91:
                    nrecent90 += 1
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
        query = setupView(request, hours=nhours, limit=999999)
        # looking into user analysis jobs only
        query['prodsourcelabel'] = 'user'
        # dynamically assemble user summary info
        values = ('eventservice', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus',
                  'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'pandaid',
                  'starttime', 'endtime', 'modificationtime',
                  'atlasrelease', 'processingtype', 'workinggroup', 'currentpriority', 'container_name', 'cmtconfig')
        jobs = []
        jobs.extend(Jobsdefined4.objects.filter(**query).values(*values))
        jobs.extend(Jobsactive4.objects.filter(**query).values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**query).values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query).values(*values))

        jobs = clean_job_list(request, jobs, do_add_metadata=False, do_add_errorinfo=False)
        sumd = user_summary_dict(jobs)
        for user in sumd:
            if user['dict']['latest']:
                user['dict']['latest'] = user['dict']['latest'].strftime(settings.DATETIME_FORMAT)
        sumparams = ['jobstatus', 'prodsourcelabel', 'specialhandling', 'transformation', 'processingtype',
                     'workinggroup', 'priorityrange']
        if VOMODE == 'atlas':
            sumparams.append('atlasrelease')
        else:
            sumparams.append('vo')

        jobsumd = job_summary_dict(request, jobs, sumparams)[0]

    if not is_json_request(request):
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
            'tfirst': request.session['TFIRST'].strftime(settings.DATETIME_FORMAT),
            'tlast': request.session['TLAST'].strftime(settings.DATETIME_FORMAT),
            'plow': request.session['PLOW'],
            'phigh': request.session['PHIGH'],
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "userList", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render_to_response('userList.html', data, content_type='text/html')
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        request = complete_request(request)
        return HttpResponse(json.dumps(sumd), content_type='application/json')


@login_customrequired
def userInfo(request, user=''):
    valid, response = initRequest(request)
    if not valid:
        return response
    fullname = ''
    login = ''
    is_prepare_history_links = False
    userQueryTask = None
    userQueryJobs = None

    if user == '':
        if 'user' in request.session['requestParams']: user = request.session['requestParams']['user']
        if 'produsername' in request.session['requestParams']: user = request.session['requestParams']['produsername']
        if request.user.is_authenticated and user == request.user.first_name.replace('\'', '') + ' ' + request.user.last_name:
            is_prepare_history_links = True

        # Here we serve only personal user pages. No user parameter specified
        if user == '':
            if request.user.is_authenticated:
                login = user = request.user.username
                fullname = request.user.first_name.replace('\'', '') + ' ' + request.user.last_name
                userQueryTask = Q(username=login) | Q(username__startswith=fullname)
                userQueryJobs = Q(produsername=login) | Q(produsername__startswith=fullname)
                is_prepare_history_links = True

    if 'days' in request.session['requestParams']:
        days = int(request.session['requestParams']['days'])
    else:
        days = 7
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = None

    requestParams = {}
    for param in request.session['requestParams']:
        requestParams[escape_input(param.strip())] = escape_input(request.session['requestParams'][param.strip()].strip())
    request.session['requestParams'] = requestParams

    # getting most relevant links based on visit statistics
    links = {}
    if is_prepare_history_links:
        userids = BPUser.objects.filter(email=request.user.email).values('id')
        userid = userids[0]['id']
        fields = {
            'job': copy.deepcopy(standard_fields),
            'task': copy.deepcopy(standard_taskfields),
            'site': copy.deepcopy(standard_sitefields),
        }
        links = get_relevant_links(userid, fields)

    # Tasks owned by the user
    query = setupView(request, hours=days*24, limit=999999, querytype='task')

    if userQueryTask is None:
        query['username__icontains'] = user.strip()
        tasks = JediTasks.objects.filter(**query).values()
    else:
        tasks = JediTasks.objects.filter(**query).filter(userQueryTask).values()
    _logger.info('Got {} tasks: {}'.format(len(tasks), time.time() - request.session['req_init_time']))

    tasks = cleanTaskList(tasks, sortby=sortby, add_datasets_info=True)
    _logger.info('Cleaned tasks and loading datasets info: {}'.format(time.time() - request.session['req_init_time']))

    # consumed cpu hours stats for a user
    if len(tasks) > 0:
        panda_user_name = list(set([t['username'] for t in tasks]))[0]
    else:
        panda_user_name = fullname if fullname != '' else user.strip()
    userstats = get_panda_user_stats(panda_user_name)
    _logger.info('Got user statistics: {}'.format(time.time() - request.session['req_init_time']))

    # old classic page
    if 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'classic':
        if 'display_limit_tasks' not in request.session['requestParams']:
            display_limit_tasks = 100
        else:
            display_limit_tasks = int(request.session['requestParams']['display_limit_tasks'])
        ntasksmax = display_limit_tasks
        url_nolimit_tasks = removeParam(extensibleURL(request), 'display_limit_tasks', mode='extensible') + "display_limit_tasks=" + str(len(tasks))

        tasks = getTaskScoutingInfo(tasks, ntasksmax)
        _logger.info('Tasks scouting info loaded: {}'.format(time.time() - request.session['req_init_time']))

        ntasks = len(tasks)
        tasksumd = task_summary_dict(request, tasks)
        _logger.info('Tasks summary generated: {}'.format(time.time() - request.session['req_init_time']))

        # Jobs
        limit = 5000
        query, extra_query_str, LAST_N_HOURS_MAX = setupView(request, hours=72, limit=limit, querytype='job', wildCardExt=True)
        jobs = []
        values = 'eventservice', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock', 'container_name', 'cmtconfig'

        if userQueryJobs is None:
            query['produsername__icontains'] = user.strip()
            jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
                jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
        else:
            jobs.extend(Jobsdefined4.objects.filter(**query).filter(userQueryJobs).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsactive4.objects.filter(**query).filter(userQueryJobs).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobswaiting4.objects.filter(**query).filter(userQueryJobs).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsarchived4.objects.filter(**query).filter(userQueryJobs).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))

            # Here we go to an archive. Separation OR condition is done to enforce Oracle to perform indexed search.
            if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
                query['produsername__startswith'] = user.strip() #.filter(userQueryJobs)
                archjobs = []
                # This two filters again to force Oracle search
                archjobs.extend(Jobsarchived.objects.filter(**query).filter(Q(produsername=user.strip())).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
                if len(archjobs) > 0:
                    jobs = jobs+archjobs
                elif len(fullname) > 0:
                    #del query['produsername']
                    query['produsername__startswith'] = fullname
                    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))

        jobs = clean_job_list(request, jobs, do_add_metadata=False, do_add_errorinfo=True)

        # Divide up jobs by jobset and summarize
        jobsets = {}
        for job in jobs:
            if 'jobsetid' not in job or job['jobsetid'] == None: continue
            if job['jobsetid'] not in jobsets:
                jobsets[job['jobsetid']] = {}
                jobsets[job['jobsetid']]['jobsetid'] = job['jobsetid']
                jobsets[job['jobsetid']]['jobs'] = []
            jobsets[job['jobsetid']]['jobs'].append(job)
        for jobset in jobsets:
            jobsets[jobset]['sum'] = job_state_count(jobsets[jobset]['jobs'])
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
            jobsets[jobset]['tfirst'] = tfirst.strftime(settings.DATETIME_FORMAT)
            jobsets[jobset]['tlast'] = tlast.strftime(settings.DATETIME_FORMAT)
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
        else:
            display_limit_jobs = 100
        njobsmax = display_limit_jobs
        url_nolimit_jobs = removeParam(extensibleURL(request), 'display_limit_jobs', mode='extensible') + 'display_limit_jobs=' + str(len(jobs))

        sumd = user_summary_dict(jobs)

        if not is_json_request(request):
            flist = ['jobstatus', 'prodsourcelabel', 'processingtype', 'specialhandling', 'transformation', 'jobsetid',
                     'jeditaskid', 'computingsite', 'cloud', 'workinggroup', 'homepackage', 'inputfileproject',
                     'inputfiletype', 'attemptnr', 'priorityrange', 'jobsetrange']
            if VOMODE != 'atlas':
                flist.append('vo')
            else:
                flist.append('atlasrelease')
            jobsumd, esjobssumd = job_summary_dict(request, jobs, flist)
            njobsetmax = 100
            xurl = extensibleURL(request)
            nosorturl = removeParam(xurl, 'sortby', mode='extensible')

            timestamp_vars = ['modificationtime', 'statechangetime', 'starttime', 'creationdate', 'resquetime',
                              'endtime', 'lockedtime', 'frozentime', 'ttcpredictiondate']
            for task in tasks:
                for tp in task:
                    if tp in timestamp_vars and task[tp] is not None:
                        task[tp] = task[tp].strftime(settings.DATETIME_FORMAT)
                    if task[tp] is None:
                        task[tp] = ''

            timestamp_vars = ['modificationtime', 'creationtime']
            for job in jobs:
                for tsv in timestamp_vars:
                    if tsv in job and job[tsv]:
                        job[tsv] = job[tsv].strftime(settings.DATETIME_FORMAT)

            data = {
                'request': request,
                'viewParams': request.session['viewParams'],
                'requestParams': request.session['requestParams'],
                'xurl': xurl,
                'nosorturl': nosorturl,
                'user': panda_user_name,
                'sumd': sumd,
                'jobsumd': jobsumd,
                'jobList': jobs[:njobsmax],
                'njobs': len(jobs),
                'display_limit_jobs': display_limit_jobs,
                'url_nolimit_jobs': url_nolimit_jobs,
                'query': query,
                'userstats': userstats,
                'tfirst': request.session['TFIRST'].strftime(settings.DATETIME_FORMAT),
                'tlast': request.session['TLAST'].strftime(settings.DATETIME_FORMAT),
                'plow': request.session['PLOW'],
                'phigh': request.session['PHIGH'],
                'jobsets': jobsetl[:njobsetmax - 1],
                'njobsetmax': njobsetmax,
                'njobsets': len(jobsetl),
                'url_nolimit_tasks': url_nolimit_tasks,
                'display_limit_tasks': display_limit_tasks,
                'tasks': tasks[:ntasksmax],
                'ntasks': ntasks,
                'tasksumd': tasksumd,
                'built': datetime.now().strftime("%H:%M:%S"),
                'links': links,
            }
            data.update(getContextVariables(request))
            response = render_to_response('userInfo.html', data, content_type='text/html')
            request = complete_request(request)
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response
        else:
            request = complete_request(request)
            resp = sumd
            return HttpResponse(json.dumps(resp, default=datetime_handler), content_type='application/json')

    else:
        # user dashboard
        if query and 'modificationtime__castdate__range' in query:
            request.session['timerange'] = query['modificationtime__castdate__range']

        plots = prepare_user_dash_plots(tasks)

        # put list of tasks to cache for further usage
        tk_taskids = random.randrange(100000000)
        setCacheEntry(request, tk_taskids, json.dumps(tasks, cls=DateTimeEncoder), 60 * 30, isData=True)

        metrics_total = {}
        if userstats:
            metrics_total['cpua7'] = userstats['cpua7'] if 'cpua7' in userstats else 0
            metrics_total['cpup7'] = userstats['cpup7'] if 'cpup7' in userstats else 0
        metrics_total = humanize_metrics(metrics_total)

        if is_json_request(request):
            return HttpResponse(json.dumps({'tasks': tasks}, default=datetime_handler), content_type='application/json')
        else:
            xurl = extensibleURL(request)
            url_noview = removeParam(xurl, 'view', mode='extensible')

            data = {
                'request': request,
                'viewParams': request.session['viewParams'],
                'requestParams': request.session['requestParams'],
                'timerange': request.session['timerange'],
                'built': datetime.now().strftime("%H:%M:%S"),
                'tk': tk_taskids,
                'xurl': xurl,
                'urlnoview': url_noview,
                'user': user,
                'links': links,
                'ntasks': len(tasks),
                'plots': plots,
                'metrics': metrics_total,
                'userstats': userstats,
            }
            response = render_to_response('userDash.html', data, content_type='text/html')
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response


def userDashApi(request, agg=None):
    """

    :param agg: str: type of aggregation to return
    :return: JSON
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    AVAILABLE_AGGS = ['initial', 'cons_plots', 'overall_errors']
    data = {'msg': '', 'data': {}}

    if agg is None or agg not in AVAILABLE_AGGS:
        data['msg'] += 'ERROR! Invalid agg passed.'
        return HttpResponse(json.dumps(data, default=datetime_handler), content_type='application/json')

    tk = None
    if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk']:
        tk = int(request.session['requestParams']['tk'])
    else:
        data['msg'] += 'ERROR! Invalid transaction key passed. Please try to reload the page.'
        return HttpResponse(json.dumps(data, default=datetime_handler), content_type='application/json')

    # getting top errors by task and metrics for labels
    if agg == 'initial':
        # get taskids from cache
        tasks_str = getCacheEntry(request, tk, isData=True)
        if tasks_str is not None:
            tasks = json.loads(tasks_str)
        else:
            tasks = []
        _logger.info('Got {} tasks from cache: {}'.format(len(tasks), time.time() - request.session['req_init_time']))

        # jobs summary
        jquery = {
            'jeditaskid__in': [t['jeditaskid'] for t in tasks if 'jeditaskid' in t]
        }
        err_fields = [
            'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag',
            'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag',
            'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode',
            'produsername'
        ]
        jobs = get_job_list(jquery, values=err_fields)
        _logger.info('Got jobs: {}'.format(time.time() - request.session['req_init_time']))

        errs_by_code, _, _, errs_by_task, _, _ = errorSummaryDict(request, jobs, False, flist=[], sortby='count')
        errs_by_task_dict = {}
        for err in errs_by_task:
            if err['name'] not in errs_by_task_dict:
                errs_by_task_dict[err['name']] = err
        _logger.info('Got error summaries: {}'.format(time.time() - request.session['req_init_time']))

        metrics = calc_jobs_metrics(jobs, group_by='jeditaskid')
        _logger.info('Calculated jobs metrics: {}'.format(time.time() - request.session['req_init_time']))

        job_state_summary = job_states_count_by_param(jobs, param='category')
        job_state_summary_total = {}
        for cat in job_state_summary:
            for js in cat['job_state_counts']:
                if js['name'] not in job_state_summary_total:
                    job_state_summary_total[js['name']] = 0
                job_state_summary_total[js['name']] += js['count']
        data['data']['plots'] = [{
            'name': 'n_jobs_by_status',
            'type': 'pie',
            'data': [[js, count] for js, count in job_state_summary_total.items() if count > 0],
            'title': 'N jobs by status',
            'options': {'legend_position': 'bottom', 'size_mp': 0.2, 'color_scheme': 'job_states',}
            },]
        _logger.info('Got job status summary: {}'.format(time.time() - request.session['req_init_time']))


        for t in tasks:
            for metric in metrics:
                if t['jeditaskid'] in metrics[metric]['group_by']:
                    t['job_' + metric] = metrics[metric]['group_by'][t['jeditaskid']]
                else:
                    t['job_' + metric] = ''
            if t['jeditaskid'] in errs_by_task_dict and t['superstatus'] != 'done':
                link_jobs_base = '/jobs/?mode=nodrop&jeditaskid={}&'.format(t['jeditaskid'])
                link_logs_base = '/filebrowser/?'
                t['top_errors'] = '<br>'.join(
                    ['<a href="{}{}={}">{}</a> [{}] "{}" <a href="{}pandaid={}">[<i class="fi-link"></i>]</a>'.format(
                        link_jobs_base, err['codename'], err['codeval'], err['count'], err['error'], err['diag'],
                        link_logs_base, err['example_pandaid'],
                    ) for err in errs_by_task_dict[t['jeditaskid']]['errorlist']][:2])
            else:
                t['top_errors'] = -1
        _logger.info('Jobs metrics added to tasks: {}'.format(time.time() - request.session['req_init_time']))

        # prepare relevant metrics to show
        metrics_total = {m: v['total'] for m, v in metrics.items() if 'total' in v}
        metrics_total = humanize_metrics(metrics_total)

        data['data']['metrics'] = metrics_total
        data['data']['tasks_metrics'] = tasks

        # prepare data for datatable
        timestamp_vars = ['modificationtime', 'statechangetime', 'starttime', 'creationdate', 'resquetime',
                          'endtime', 'lockedtime', 'frozentime', 'ttcpredictiondate', 'ttcrequested']
        for task in tasks:
            for tp in task:
                if tp in timestamp_vars and task[tp] is not None and isinstance(task[tp], datetime):
                    task[tp] = task[tp].strftime(settings.DATETIME_FORMAT)
                if task[tp] is None:
                    task[tp] = ''
                if task[tp] is True:
                    task[tp] = 'true'
                if task[tp] is False:
                    task[tp] = 'false'

        task_list_table_headers = [
            'jeditaskid', 'creationdate', 'attemptnr', 'tasktype', 'taskname', 'nfiles', 'nfilesfinished', 'nfilesfailed', 'pctfinished',
            'superstatus', 'status', 'age',
            'job_queuetime', 'job_walltime', 'job_maxpss_per_actualcorecount', 'job_efficiency', 'job_attemptnr',
            'errordialog', 'job_failed', 'top_errors',
        ]
        tasks_to_show = []
        for t in tasks:
            tmp_list = []
            for h in task_list_table_headers:
                if h in t:
                    tmp_list.append(t[h])
                else:
                    tmp_list.append("-")
            tasks_to_show.append(tmp_list)
        data['data']['tasks_metrics'] = tasks_to_show

    return HttpResponse(json.dumps(data, default=datetime_handler), content_type='application/json')


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
        request.session['requestParams'][param] = escape_input(request.session['requestParams'][param])
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
                extraParCondition += preprocess_wild_card_string(escape_input(card), 'catchall')
                if (currentCardCount < countCards): extraParCondition += ' OR '
                currentCardCount += 1
            extraParCondition += ')'

        for field in Schedconfig._meta.get_fields():
            if param == field.name and not (param == 'catchall'):
                query[param] = escape_input(request.session['requestParams'][param])

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
    if not is_json_request(request):
        sumd = site_summary_dict(sites, VOMODE=VOMODE)
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


@login_customrequired
def siteInfo(request, site=''):
    valid, response = initRequest(request)
    if not valid:
        return response
    if site == '' and 'site' in request.session['requestParams']:
        site = request.session['requestParams']['site']
    setupView(request)
    query = {'siteid__iexact': site}
    sites = Schedconfig.objects.filter(**query)
    colnames = []
    try:
        siterec = sites[0]
        colnames = siterec.get_all_fields()
        if sites[0].lastmod:
            sites[0].lastmod = sites[0].lastmod.strftime(settings.DATETIME_FORMAT)
    except IndexError:
        siterec = None
    if len(sites) > 1:
        for queue in sites:
            if queue['lastmod']:
                queue['lastmod'] = queue['lastmod'].strftime(settings.DATETIME_FORMAT)

    # get data from new schedconfig_json table
    panda_queue = []
    pqquery = {'pandaqueue': site}
    panda_queues = SchedconfigJson.objects.filter(**pqquery).values()
    panda_queue_type = None
    if len(panda_queues) > 0:
        panda_queue_dict = json.loads(panda_queues[0]['data'])
        panda_queue_type = panda_queue_dict['type']
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

    if not is_json_request(request):
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
            attrs.append({'name': 'Space', 'value': "%d TB as of %s" % ((float(siterec.space) / 1000.), siterec.tspace.strftime('%m-%d %H:%M'))})
            attrs.append({'name': 'Last modified', 'value': "%s" % (siterec.lastmod.strftime('%Y-%m-%d %H:%M'))})

            # get calculated metrics
            try:
                metrics = get_pq_metrics(siterec.nickname)
            except Exception as ex:
                metrics = {}
                _logger.exception('Failed to get metrics for {}\n {}'.format(siterec.nickname, ex))
            if len(metrics) > 0:
                for pq, m_dict in metrics.items():
                    for m in m_dict:
                        colnames.append({'label': m, 'name': m, 'value': m_dict[m]})

        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'site': siterec,
            'panda_resource': panda_resource,
            'queues': sites,
            'colnames': colnames,
            'attrs': attrs,
            'name': site,
            'pq_type': panda_queue_type,
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

        return HttpResponse(json.dumps(panda_queue), content_type='application/json')



@login_customrequired
def wnInfo(request, site, wnname='all'):
    """ Give worker node level breakdown of site activity. Spot hot nodes, error prone nodes. """
    valid, response = initRequest(request)
    if not valid:
        return response

    jobs_url = '?computingsite={}&mode=nodrop'.format(site)
    if 'hours' in request.session['requestParams']:
        hours = int(request.session['requestParams']['hours'])
    elif 'days' in request.session['requestParams']:
        hours = 24*int(request.session['requestParams']['days'])
    elif 'date_from' in request.session['requestParams'] and 'date_to' in request.session['requestParams']:
        hours = 0
    else:
        hours = 12
        jobs_url += '&hours={}'.format(hours)

    exclude_params = ['timestamp', 'wnname', ]
    for p, v in request.session['requestParams'].items():
        if p not in exclude_params:
            jobs_url += '&{}={}'.format(p, v)

    panda_queues = get_panda_queues()
    if site and site not in panda_queues:
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

    fullsummary, plots_data = wn_summary(wnname, query)

    if 'sortby' in request.session['requestParams']:
        if request.session['requestParams']['sortby'] in sitestatelist:
            fullsummary = sorted(fullsummary, key=lambda x: x['states'][request.session['requestParams']['sortby']] if not isinstance(x['states'][request.session['requestParams']['sortby']], dict) else x['states'][request.session['requestParams']['sortby']]['count'],
                                 reverse=True)
        elif request.session['requestParams']['sortby'] == 'pctfail':
            fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)

    # Remove None wn from failed jobs plot if it is in system, add warning banner
    warning = {}
    if 'None' in plots_data['failed']:
        warning['message'] = '%i failed jobs are excluded from "Failed jobs per WN slot" plot because of None value of modificationhost.' % (plots_data['failed']['None'])
        try:
            del plots_data['failed']['None']
        except:
            pass

    wnPlotFailedL = sorted([[k, v] for k, v in plots_data['failed'].items()], key=lambda x: x[0])

    kys = plots_data['finished'].keys()
    kys = sorted(kys)
    wnPlotFinishedL = []
    for k in kys:
        wnPlotFinishedL.append([k, plots_data['finished'][k]])

    if not is_json_request(request):
        xurl = extensibleURL(request)
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'url': request.path,
            'xurl': xurl,
            'jurl': jobs_url,
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


@login_customrequired
def dashboard(request, view='all'):
    valid, response = initRequest(request)
    if not valid:
        return response

    # if it is region|cloud view redirect to new dash
    cloudview = 'region'
    if 'cloudview' in request.session['requestParams']:
        cloudview = request.session['requestParams']['cloudview']
    if view == 'analysis':
        cloudview = 'region'
    elif view != 'production' and view != 'all':
        cloudview = 'N/A'

    if ('version' not in request.session['requestParams'] or request.session['requestParams']['version'] != 'old') \
            and view in ('all', 'production', 'analysis') and cloudview in ('region', 'world') \
            and 'es' not in request.session['requestParams'] and 'mode' not in request.session['requestParams'] \
            and not is_json_request(request):
        # do redirect
        if cloudview == 'world':
            return redirect('/dash/world/')
        elif cloudview == 'region':
            if view == 'production':
                return redirect('/dash/region/?jobtype=prod&splitby=jobtype')
            elif view == 'analysis':
                return redirect('/dash/region/?jobtype=analy&splitby=jobtype')
            elif view == 'all':
                return redirect('/dash/region/')

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
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        hours = 24 * taskdays
    else:
        hours = 12

    hoursSinceUpdate = 36
    estailtojobslinks = ''
    extra = "(1=1)"
    if view == 'production':
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
        sortby = 'name'
        if 'sortby' in request.session['requestParams']:
            sortby = request.session['requestParams']['sortby']
        vosummary = vo_summary(query, sortby=sortby)

    else:
        if view == 'production':
            errthreshold = 5
        elif view == 'analysis':
            errthreshold = 15
        else:
            errthreshold = 10
        vosummary = []

    if view == 'production' and (cloudview == 'world' or cloudview == 'cloud'): # cloud view is the old way of jobs distributing;
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
            extra = job_suppression(request)

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
                        if (jobs['jobstatus'] in ('finished','failed','merging')):
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

        if not is_json_request(request):
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
            # self monitor
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

        mObjectStores, mObjectStoresSummary = objectstore_summary(request, hours=hours)
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

        fullsummary = cloud_site_summary(query, extra=extra, view=view, cloudview=cloudview, notime=notime)
        cloudTaskSummary = wg_task_summary(request, fieldname='cloud', view=view, taskdays=taskdays)
        jobsLeft = {}
        rw = {}

        if settings.DEPLOYMENT == 'ORACLE_ATLAS':
            rwData, nRemJobs = calculateRWwithPrio_JEDI(query)
            for cloud in fullsummary:
                if cloud['name'] in nRemJobs.keys():
                    jobsLeft[cloud['name']] = nRemJobs[cloud['name']]
                if cloud['name'] in rwData.keys():
                    rw[cloud['name']] = rwData[cloud['name']]

        if not is_json_request(request) or 'keephtml' in request.session['requestParams']:
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
            # self monitor
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
def dashRegion(request):
    """
    A new job summary dashboard for regions that allows to split jobs in Grand Unified Queue
    by analy|prod and resource types
    Regions column order:
        region, status, job type, resource type, Njobstotal, [Njobs by status]
    Queues column order:
        queue name, type [GU, U, Simple], region, status, job type, resource type, Njobstotal, [Njobs by status]
    :param request: request
    :return: HTTP response
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    if request.path.startswith('/new/dash/'):
        return redirect(request.get_full_path().replace('/new/dash/', '/dash/region/'))

    # Here we try to get cached data
    data = getCacheEntry(request, "JobSummaryRegion")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('JobSummaryRegion.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'splitby' in request.session['requestParams'] and request.session['requestParams']['splitby']:
        split_by = request.session['requestParams']['splitby']
    else:
        split_by = None

    if 'region' in request.session['requestParams'] and request.session['requestParams']['region']:
        region = request.session['requestParams']['region']
    else:
        region = 'all'
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        jobtype = request.session['requestParams']['jobtype']
    else:
        jobtype = 'all'
    if 'resourcetype' in request.session['requestParams'] and request.session['requestParams']['resourcetype']:
        resourcetype = request.session['requestParams']['resourcetype']
    else:
        resourcetype = 'all'

    jquery, extra_str, hours = setupView(request, limit=9999999, querytype='job', wildCardExt=True)

    # add queue related request params to query dict
    if 'queuetype' in request.session['requestParams'] and request.session['requestParams']['queuetype']:
        jquery['queuetype'] = request.session['requestParams']['queuetype']
    if 'queuestatus' in request.session['requestParams'] and request.session['requestParams']['queuestatus']:
        jquery['queuestatus'] = request.session['requestParams']['queuestatus']
    if 'site' in request.session['requestParams'] and request.session['requestParams']['site'] != 'all':
        jquery['queuegocname'] = request.session['requestParams']['site']

    # get job summary data
    jsr_queues_dict, jsr_regions_dict = get_job_summary_region(jquery,
                                                               extra=extra_str,
                                                               region=region, 
                                                               jobtype=jobtype,
                                                               resourcetype=resourcetype,
                                                               split_by=split_by)

    if is_json_request(request):
        extra_info_params = ['links', ]
        extra_info = {ep: False for ep in extra_info_params}
        if 'extra' in request.session['requestParams'] and 'links' in request.session['requestParams']['extra']:
            extra_info['links'] = True
        jsr_queues_dict, jsr_regions_dict = prettify_json_output(jsr_queues_dict, jsr_regions_dict, hours=hours, extra=extra_info)
        data = {
            'regions': jsr_regions_dict,
            'queues': jsr_queues_dict,
        }
        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # transform dict to list and filter out rows depending on split by request param
        jsr_queues_list, jsr_regions_list = prepare_job_summary_region(jsr_queues_dict, jsr_regions_dict,
                                                                       split_by=split_by)

        # prepare lists of unique values for drop down menus
        select_params_dict = {}
        select_params_dict['queuetype'] = sorted(list(set([pq[1] for pq in jsr_queues_list])))
        select_params_dict['queuestatus'] = sorted(list(set([pq[3] for pq in jsr_queues_list])))

        pq_info_basic = get_basic_info_for_pqs([])
        unique_sites_dict = {}
        for pq in pq_info_basic:
            if pq['site'] not in unique_sites_dict:
                unique_sites_dict[pq['site']] = pq['region']
        select_params_dict['site'] = sorted([{'site': site, 'region': reg} for site, reg in unique_sites_dict.items()],
                                            key=lambda x: x['site'])
        select_params_dict['region'] = sorted(list(set([reg for site, reg in unique_sites_dict.items()])))

        xurl = request.get_full_path()
        if xurl.find('?') > 0:
            xurl += '&'
        else:
            xurl += '?'

        # overwrite view selection params
        view_params_str = '<b>Manually entered params</b>: '
        supported_params = {f.verbose_name: '' for f in PandaJob._meta.get_fields()}
        interactive_params = ['hours', 'days', 'date_from', 'date_to', 'timestamp',
                              'queuetype', 'queuestatus', 'jobtype', 'resourcetype', 'splitby', 'region', 'site']
        for pn, pv in request.session['requestParams'].items():
            if pn not in interactive_params and pn in supported_params:
                view_params_str += '<b>{}=</b>{} '.format(str(pn), str(pv))
        request.session['viewParams']['selection'] = view_params_str if not view_params_str.endswith(': ') else ''
        request.session['timerange'] = jquery['modificationtime__castdate__range']

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'timerange': request.session['timerange'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'hours': hours,
            'xurl': xurl,
            'selectParams': select_params_dict,
            'jobstates': statelist,
            'regions': jsr_regions_list,
            'queues': jsr_queues_list,
            'show': 'all',
        }

        response = render_to_response('JobSummaryRegion.html', data, content_type='text/html')
        setCacheEntry(request, "JobSummaryRegion", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def dashNucleus(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "JobSummaryNucleus")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('JobSummaryNucleus.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
    else:
        hours = 12

    query, extra, nhours = setupView(request, hours=hours, limit=999999, wildCardExt=True)

    # get summary data
    jsn_nucleus_dict, jsn_satellite_dict = get_job_summary_nucleus(
        query,
        extra=extra,
        job_states_order=copy.deepcopy(statelist),
        hs06s=True
    )

    get_world_hs06_summary(query, extra=extra)

    if is_json_request(request):
        data = {
            'nucleuses': jsn_satellite_dict,
            'nucleussummary': jsn_nucleus_dict,
            'statelist': copy.deepcopy(statelist),
            'built': datetime.now().strftime(settings.DATETIME_FORMAT),
        }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        # convert dict -> list
        jsn_nucleus_list, jsn_satellite_list = prepare_job_summary_nucleus(
            jsn_nucleus_dict,
            jsn_satellite_dict,
            job_states_order=copy.deepcopy(statelist)
        )

        xurl = request.get_full_path()
        if xurl.find('?') > 0:
            xurl += '&'
        else:
            xurl += '?'

        # overwrite view selection params
        view_params_str = '<b>Params</b>: '
        supported_params = {f.verbose_name: '' for f in PandaJob._meta.get_fields()}
        for pn, pv in request.session['requestParams'].items():
            if pn in supported_params:
                view_params_str += '<b>{}=</b>{} '.format(str(pn), str(pv))
        request.session['viewParams']['selection'] = view_params_str if not view_params_str.endswith(': ') else ''
        request.session['timerange'] = query['modificationtime__castdate__range']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'timerange': request.session['timerange'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'jobstates': statelist,
            'show': 'all',
            'hours': hours,
            'xurl': xurl,
            'nuclei': jsn_nucleus_list,
            'satellites': jsn_satellite_list,
        }
        response = render_to_response('JobSummaryNucleus.html', data, content_type='text/html')
        setCacheEntry(request, "JobSummaryNucleus", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def dashES(request):
    """
    A new ES job summary dashboard
    :param request: request
    :return: HTTP response
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "JobSummaryRegion")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('EventService.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'splitby' in request.session['requestParams'] and request.session['requestParams']['splitby']:
        split_by = request.session['requestParams']['splitby']
    else:
        split_by = None

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
    else:
        hours = 12

    jquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='job', wildCardExt=True)

    # add queue related request params to  pqquery dict
    pqquery = dict()
    if 'queuetype' in request.session['requestParams'] and request.session['requestParams']['queuetype']:
        pqquery['queuetype'] = request.session['requestParams']['queuetype']
    if 'queuestatus' in request.session['requestParams'] and request.session['requestParams']['queuestatus']:
        pqquery['queuestatus'] = request.session['requestParams']['queuestatus']

    # get job summary data
    jsr_queues_dict, jsr_regions_dict = get_es_job_summary_region(jquery, extra=wildCardExtension, pqquery=pqquery)

    if is_json_request(request):
        extra_info_params = ['links', ]
        extra_info = {ep: False for ep in extra_info_params}
        if 'extra' in request.session['requestParams'] and 'links' in request.session['requestParams']['extra']:
            extra_info['links'] = True
        jsr_queues_dict, jsr_regions_dict = prettify_json_output(jsr_queues_dict, jsr_regions_dict, hours=hours, extra=extra_info)
        data = {
            'regions': jsr_regions_dict,
            'queues': jsr_queues_dict,
        }
        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # transform dict to list and filter out rows depending on split by request param
        jsr_queues_list, jsr_regions_list = prepare_es_job_summary_region(jsr_queues_dict, jsr_regions_dict,
                                                                       split_by=split_by)

        # prepare lists of unique values for drop down menus
        select_params_dict = {}
        select_params_dict['region'] = sorted(list(set([r[0] for r in jsr_regions_list])))
        select_params_dict['queuetype'] = sorted(list(set([pq[1] for pq in jsr_queues_list])))
        select_params_dict['queuestatus'] = sorted(list(set([pq[3] for pq in jsr_queues_list])))

        xurl = request.get_full_path()
        if xurl.find('?') > 0:
            xurl += '&'
        else:
            xurl += '?'

        # overwrite view selection params
        view_params_str = '<b>Manually entered params</b>: '
        supported_params = {f.verbose_name: '' for f in PandaJob._meta.get_fields()}
        interactive_params = ['hours', 'days', 'date_from', 'date_to', 'timestamp',
                              'queuetype', 'queuestatus', 'jobtype', 'resourcetype', 'splitby', 'region']
        for pn, pv in request.session['requestParams'].items():
            if pn not in interactive_params and pn in supported_params:
                view_params_str += '<b>{}=</b>{} '.format(str(pn), str(pv))
        request.session['viewParams']['selection'] = view_params_str if not view_params_str.endswith(': ') else ''
        request.session['timerange'] = jquery['modificationtime__castdate__range']
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'timerange': request.session['timerange'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'hours': hours,
            'xurl': xurl,
            'selectParams': select_params_dict,
            'jobstates': statelist,
            'regions': jsr_regions_list,
            'queues': jsr_queues_list,
            'show': 'all',
        }

        response = render_to_response('EventService.html', data, content_type='text/html')
        setCacheEntry(request, "EventService", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

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

    cloudTaskSummary = wg_task_summary(request, fieldname='cloud', view=view, taskdays=taskdays)

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

    fullsummary = cloud_site_summary(query, view=view, cloudview=cloudview)

    jobsLeft = {}
    rw = {}
    rwData, nRemJobs = calculateRWwithPrio_JEDI(query)
    for cloud in fullsummary:
        leftCount = 0
        if cloud['name'] in nRemJobs.keys():
            jobsLeft[cloud['name']] = nRemJobs[cloud['name']]
        if cloud['name'] in rwData.keys():
            rw[cloud['name']] = rwData[cloud['name']]

    if not is_json_request(request):
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
        limit = 5000
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'pctfailed':
            limit = 50000

    if 'tasktype' in request.session['requestParams'] and request.session['requestParams']['tasktype'].startswith(
            'anal'):
        hours = 3 * 24
    else:
        hours = 7 * 24
    sortby = "jeditaskid-desc"
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby']:
        sortby = request.session['requestParams']['sortby']
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

    tmpTableName = get_tmp_table_name()

    if 'jeditaskid__in' in query:
        taskl = query['jeditaskid__in']
        if len(taskl) > settings.DB_N_MAX_IN_QUERY:
            transactionKey = insert_to_temp_table(taskl)
            selectTail = """jeditaskid in (SELECT tmp.id FROM %s tmp where TRANSACTIONKEY=%i)""" % (tmpTableName, transactionKey)
            extraquery = selectTail if len(extraquery) == 0 else extraquery + ' AND ' + selectTail
            del query['jeditaskid__in']
            if 'modificationtime__castdate__range' in query:
                del query['modificationtime__castdate__range']

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

    error_codes_analyser = TasksErrorCodesAnalyser()
    error_codes_analyser.schedule_preprocessing(tasks)

    transactionKey = insert_to_temp_table(taskl)

    # For tasks plots
    setCacheEntry(request, transactionKey, taskl, 60 * 20, isData=True)

    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
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
    else:
        taskhashtags = []

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
                datasetstageitem['START_TIME'] = datasetstageitem['START_TIME'].strftime(settings.DATETIME_FORMAT)
            else:
                datasetstageitem['START_TIME'] = ''

            if datasetstageitem['END_TIME']:
                datasetstageitem['END_TIME'] = datasetstageitem['END_TIME'].strftime(settings.DATETIME_FORMAT)
            else:
                datasetstageitem['END_TIME'] = ''

            if not datasetstageitem['SOURCE_RSE']:
                datasetstageitem['SOURCE_RSE'] = 'Unknown'


            if datasetstageitem['UPDATE_TIME']:
                datasetstageitem['UPDATE_TIME'] = datasetstageitem['UPDATE_TIME'].strftime(settings.DATETIME_FORMAT)
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

        # We do it because we intermix raw and queryset queries. With next new_cur.execute tasksEventInfo cleares
        for tasksEventInfoItem in tasksEventInfo:
            listItem = {}
            listItem["jeditaskid"] = tasksEventInfoItem["jeditaskid"]
            listItem["totevrem"] = tasksEventInfoItem["totevrem"]
            listItem["totev"] = tasksEventInfoItem["totev"]
            eventInfoDict[tasksEventInfoItem["jeditaskid"]] = listItem

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

    tasks = cleanTaskList(tasks, sortby=sortby, add_datasets_info=True)
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
        tmpTableName = get_tmp_table_name()

        tk_es_jobs = random.randrange(1000000)
#        connection.enter_transaction_management()
        new_cur = connection.cursor()
        if settings.DEPLOYMENT == "POSTGRES":
            create_temporary_table(new_cur, tmpTableName)
        executionData = []
        for id in esjobs:
            executionData.append((id, tk_es_jobs))
        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, executionData)

#        connection.commit()
        new_cur.execute(
            """
            SELECT /*+ dynamic_sampling(TMP_IDS1 0) cardinality(TMP_IDS1 10) INDEX_RS_ASC(ev JEDI_EVENTS_PANDAID_STATUS_IDX) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  PANDAID,STATUS FROM ATLAS_PANDA.JEDI_EVENTS ev, %s WHERE TRANSACTIONKEY=%i AND PANDAID = ID
            """ % (tmpTableName, tk_es_jobs)
        )
        evtable = dictfetchall(new_cur)

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
    nohashtagurl = removeParam(xurl, 'hashtag', mode='extensible')
    noerrordialogurl = removeParam(xurl, 'hashtag', mode='errordialog')

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
            task['creationdate'] = task['creationdate'].strftime(settings.DATETIME_FORMAT)
        if task['modificationtime']:
            task['modificationtime'] = task['modificationtime'].strftime(settings.DATETIME_FORMAT)
        if task['starttime']:
            task['starttime'] = task['starttime'].strftime(settings.DATETIME_FORMAT)
        if task['statechangetime']:
            task['statechangetime'] = task['statechangetime'].strftime(settings.DATETIME_FORMAT)
        if task['ttcrequested']:
            task['ttcrequested'] = task['ttcrequested'].strftime(settings.DATETIME_FORMAT)

    error_summary_table = error_codes_analyser.get_errors_table()
    error_summary_table = json.dumps(error_summary_table, cls=DateEncoder)

    if is_json_request(request):
        # Add datasets info to the json dump
        tasks = get_datasets_for_tasklist(tasks)

        # getting jobs metadata if it is requested in URL [ATLASPANDA-492]
        if 'extra' in request.session['requestParams'] and 'metastruct' in request.session['requestParams']['extra']:
            jeditaskids = list(set([task['jeditaskid'] for task in tasks]))
            MAX_N_TASKS = 100  # protection against DB overloading
            if len(jeditaskids) <= MAX_N_TASKS:
                job_pids = []
                jobQuery = {
                    'jobstatus__in': ['finished', 'failed', 'transferring', 'merging', 'cancelled', 'closed', 'holding'],
                    'jeditaskid__in': jeditaskids
                }
                job_pids.extend(Jobsarchived4.objects.filter(**jobQuery).values('pandaid', 'jeditaskid', 'jobstatus', 'creationtime'))
                job_pids.extend(Jobsarchived.objects.filter(**jobQuery).values('pandaid', 'jeditaskid', 'jobstatus', 'creationtime'))
                if len(job_pids) > 0:
                    jobs = addJobMetadata(job_pids)
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
        del request.session
        return JsonResponse(tasks, encoder=DateEncoder, safe=False)
    else:
        sumd = task_summary_dict(request, tasks, copy.deepcopy(standard_taskfields) +
                                               ['stagesource'] if 'tape' in request.session['requestParams'] else copy.deepcopy(standard_taskfields))
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
            'noerrordialogurl':noerrordialogurl,
            'url_nolimit': url_nolimit,
            'display_limit': nmax,
            'flowstruct': flowstruct,
            'eventservice': eventservice,
            'requestString': urlParametrs,
            'tasksTotalCount': tasksTotalCount,
            'built': datetime.now().strftime("%H:%M:%S"),
            'idtasks': transactionKey,
            'error_summary_table': error_summary_table
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
    if not valid:
        return response

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

    if 'prodsysHost' in settings.PRODSYS:
        prodsysHost = settings.PRODSYS['prodsysHost']
    if 'prodsysToken' in settings.PRODSYS:
        prodsysToken = settings.PRODSYS['prodsysToken']

    if action == 0:
        prodsysUrl = '/prodtask/task_action_ext/finish/'
    elif action == 1:
        prodsysUrl = '/prodtask/task_action_ext/abort/'
    else:
        resp = {"detail": "Action is not recognized"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response

    cern_auth_provider = None
    user = request.user
    # TODO
    # temporary while both old and new CERN auth supported
    if user.is_authenticated and user.social_auth is not None:
        if len(user.social_auth.filter(provider='cernauth2')) > 0:
            cern_auth_provider = 'cernauth2'
        elif len(user.social_auth.filter(provider='cernoidc')) > 0:
            cern_auth_provider = 'cernoidc'

    if cern_auth_provider and user.social_auth.get(provider=cern_auth_provider).extra_data is not None and (
            'username' in user.social_auth.get(provider=cern_auth_provider).extra_data):
        username = user.social_auth.get(provider=cern_auth_provider).extra_data['username']
        fullname = user.social_auth.get(provider=cern_auth_provider).extra_data['name']

    else:
        resp = {"detail": "User not authenticated. Please login to BigPanDAmon with CERN"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response

    if action == 1:
        postdata = {"username": username, "task": taskid, "userfullname": fullname}
    else:
        postdata = {"username": username, "task": taskid, "parameters": [1], "userfullname": fullname}

    headers = {
        'Content-Type':'application/json',
        'Accept': 'application/json',
        'Authorization': 'Token ' + prodsysToken
    }
    conn = urllib3.HTTPSConnectionPool(prodsysHost, timeout=100)
    resp = None

    # if request.session['IS_TESTER']:
    resp = conn.urlopen('POST', prodsysUrl, body=json.dumps(postdata, cls=DateEncoder), headers=headers, retries=1, assert_same_host=False)
    # else:
    #    resp = {"detail": "You are not allowed to test. Sorry"}
    #    dump = json.dumps(resp, cls=DateEncoder)
    #    response = HttpResponse(dump, mimetype='text/plain')
    #    return response

    if resp and len(resp.data) > 0:
        try:
            resp = json.loads(resp.data)
            if resp['result'] == "FAILED":
                resp['detail'] = 'Result:' + resp['result'] + ' with reason:' + resp['exception']
            elif resp['result'] == "OK":
                resp['detail'] = 'Action peformed successfully, details: ' + resp['details']
        except:
            resp = {"detail": "prodsys responce could not be parced"}
    else:
        resp = {"detail": "Error with sending request to prodsys"}
    dump = json.dumps(resp, cls=DateEncoder)
    response = HttpResponse(dump, content_type='application/json')
    return response


def getTaskScoutingInfo(tasks, nmax):
    taskslToBeDisplayed = tasks[:nmax]
    tasksIdToBeDisplayed = [task['jeditaskid'] for task in taskslToBeDisplayed]
    tquery = {}

    tmpTableName = get_tmp_table_name()
    transactionKey = random.randrange(1000000)
    new_cur = connection.cursor()
    if settings.DEPLOYMENT == "POSTGRES":
        create_temporary_table(new_cur, tmpTableName)
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
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), status=404, content_type='application/json')
    if 'mode' in request.session['requestParams']:
        mode = request.session['requestParams']['mode']
    else:
        mode = 'drop'
    transactionKey = None
    if 'tk' in request.session['requestParams'] and request.session['requestParams']['tk']:
        try:
            transactionKey = int(request.session['requestParams']['tk'])
            transactionKey = transactionKey if transactionKey > 0 else None
        except:
            _logger.debug('Transaction key is not integer, pass it as None')
    transactionKeyDJ = None
    if 'tkdj' in request.session['requestParams'] and request.session['requestParams']['tkdj']:
        try:
            transactionKeyDJ = int(request.session['requestParams']['tkdj'])
            transactionKeyDJ = transactionKeyDJ if transactionKeyDJ > 0 else None
        except:
            _logger.debug('Transaction key DJ is not integer, pass it as None')

    equery = {}
    equery['jeditaskid']=jeditaskid
    equery['error_code__isnull'] = False

    if mode == 'drop':
        eventsErrors = []
        tmpTableName = get_tmp_table_name()
        new_cur = connection.cursor()
        if settings.DEPLOYMENT == "POSTGRES":
            create_temporary_table(new_cur, tmpTableName)
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

    error_codes = get_job_error_desc()

    for eventserror in eventsErrors:
        try:
            eventserror['error_code'] = int(eventserror['error_code'])
            if eventserror['error_code'] in error_codes['piloterrorcode'].keys():
                eventserror['error_description'] = error_codes['piloterrorcode'][eventserror['error_code']]
            else:
                eventserror['error_description'] = ''
        except:
            eventserror['error_description'] = ''
        if eventserror['pandaidlist'] and len(eventserror['pandaidlist']) > 0:
            eventserror['pandaidlist'] = eventserror['pandaidlist'].split(',')

    data = {'errors': eventsErrors}

    response = render_to_response('eventsErrorSummary.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
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
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = timezone.now().strftime(settings.DATETIME_FORMAT)
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
def taskProfile(request, jeditaskid=0):
    """A wrapper page for task profile plot"""
    valid, response = initRequest(request)
    if not valid:
        return response

    try:
        jeditaskid = int(jeditaskid)
    except ValueError:
        msg = 'Provided jeditaskid: {} is not valid, it must be numerical'.format(jeditaskid)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

    if jeditaskid > 0:
        task_profile = TaskProgressPlot()
        task_profile_start = task_profile.get_task_start(taskid=jeditaskid)
        if 'starttime' in task_profile_start:
            request.session['viewParams']['selection'] = ', started at ' + task_profile_start['starttime'].strftime(settings.DATETIME_FORMAT)
        else:
            msg = 'A task with provided jeditaskid does not exist'.format(jeditaskid)
            _logger.exception(msg)
            response = HttpResponse(json.dumps(msg), status=400)
    else:
        msg = 'Not valid jeditaskid provided: {}'.format(jeditaskid)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'jeditaskid': jeditaskid,
    }
    response = render_to_response('taskProgressMonitor.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


# @login_customrequired
@never_cache
def taskProfileData(request, jeditaskid=0):
    """A view that returns data for task profile plot"""
    valid, response = initRequest(request)
    if not valid:
        return response

    try:
        jeditaskid = int(jeditaskid)
    except ValueError:
        msg = 'Provided jeditaskid: {} is not valid, it must be numerical'.format(jeditaskid)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        request_job_types = request.session['requestParams']['jobtype'].split(',')
    else:
        request_job_types = None
    if 'jobstatus' in request.session['requestParams'] and request.session['requestParams']['jobstatus']:
        request_job_states = request.session['requestParams']['jobstatus'].split(',')
    else:
        request_job_states = None
    if 'progressunit' in request.session['requestParams'] and request.session['requestParams']['progressunit']:
        request_progress_unit = request.session['requestParams']['progressunit']
    else:
        request_progress_unit = 'jobs'

    # get raw profile data
    if jeditaskid > 0:
        task_profile = TaskProgressPlot()
        task_profile_dict = task_profile.get_raw_task_profile_full(taskid=jeditaskid)
    else:
        msg = 'Not valid jeditaskid provided: {}'.format(jeditaskid)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

    # filter raw data corresponding to request params
    if request_job_types is not None and len(request_job_types) > 0:
        for jt, values in task_profile_dict.items():
            if jt not in request_job_types:
                task_profile_dict[jt] = []
    if request_job_states is not None and len(request_job_states) > 0:
        for jt, values in task_profile_dict.items():
            temp = []
            for v in values:
                if v['jobstatus'] in request_job_states:
                    temp.append(v)
            task_profile_dict[jt] = temp

    # convert raw data to format acceptable by chart.js library
    job_time_names = ['end', 'start', 'creation']
    job_types = ['build', 'run', 'merge']
    job_states = ['finished', 'failed', 'closed', 'cancelled']
    colors = {
        'creation': {'finished': 'RGBA(162,198,110,1)', 'failed': 'RGBA(255,176,176,1)',
                     'closed': 'RGBA(214,214,214,1)', 'cancelled': 'RGBA(255,227,177,1)'},
        'start': {'finished': 'RGBA(70,181,117,0.8)', 'failed': 'RGBA(235,0,0,0.8)',
                  'closed': 'RGBA(100,100,100,0.8)', 'cancelled': 'RGBA(255,165,0,0.8)'},
        'end': {'finished': 'RGBA(2,115,0,0.8)', 'failed': 'RGBA(137,0,0,0.8)',
                'closed': 'RGBA(0,0,0,0.8)', 'cancelled': 'RGBA(157,102,0,0.8)'},
    }
    markers = {'build': 'triangle', 'run': 'circle', 'merge': 'crossRot'}
    order_mpx = {
        'creation': 1,
        'start': 2,
        'end': 3,
        'finished': 7,
        'failed': 6,
        'closed': 5,
        'cancelled': 4,
    }
    order_dict = {}
    for jtn in job_time_names:
        for js in job_states:
            order_dict[jtn+'_'+js] = order_mpx[js] * order_mpx[jtn]

    task_profile_data_dict = {}
    for jt in job_types:
        if len(task_profile_dict[jt]) > 0:
            for js in list(set(job_states) & set([r['jobstatus'] for r in task_profile_dict[jt]])):
                for jtmn in job_time_names:
                    task_profile_data_dict['_'.join((jtmn, js, jt))] = {
                        'name': '_'.join((jtmn, js, jt)),
                        'label': jtmn.capitalize() + ' time of a ' + js + ' ' + jt + ' job',
                        'pointRadius': round(1 + 3.0 * math.exp(-0.0004*len(task_profile_dict[jt]))),
                        'backgroundColor': colors[jtmn][js],
                        'borderColor': colors[jtmn][js],
                        'pointStyle': markers[jt],
                        'data': [],
                    }

    for jt in job_types:
        if jt in task_profile_dict:
            rdata = task_profile_dict[jt]
            for r in rdata:
                for jtn in job_time_names:
                    task_profile_data_dict['_'.join((jtn, r['jobstatus'], jt))]['data'].append({
                        't': r[jtn].strftime(settings.DATETIME_FORMAT),
                        'y': r['indx'] if request_progress_unit == 'jobs' else r['nevents'],
                        'label': r['pandaid'],
                    })

    # deleting point groups if data is empty
    group_to_remove = []
    for group in task_profile_data_dict:
        if len(task_profile_data_dict[group]['data']) == 0:
            group_to_remove.append(group)
    for group in group_to_remove:
        try:
            del task_profile_data_dict[group]
        except:
            _logger.info('failed to remove key from dict')

    # dict -> list
    task_profile_data = [v for k, v in task_profile_data_dict.items()]

    data = {'plotData': task_profile_data, 'error': ''}
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


@login_customrequired
def userProfile(request, username=""):
    """A wrapper page for task profile plot"""
    valid, response = initRequest(request)
    if not valid:
        return response

    try:
        username = str(username)
    except ValueError:
        msg = 'Provided username: {} is not valid, it must be string'.format(username)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)
        return response

    if len(username) > 0:
        query = setupView(request, hours=24 * 7, querytype='task', wildCardExt=False)
        query['username__icontains'] = username.strip()
        tasks = JediTasks.objects.filter(**query).values('jeditaskid')

        if len(list(tasks)) > 0:
            msg = 'The username exist: {}'.format(username)
        else:
            msg = 'The username do not exist or no tasks found: {}'.format(username)
            response = HttpResponse(json.dumps(msg), status=400)
            return response

        if query and 'modificationtime__castdate__range' in query:
            request.session['timerange'] = query['modificationtime__castdate__range']

    else:
        msg = 'Not valid username provided: {}'.format(username)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)
        return response

    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'username': username,
        'timerange': request.session['timerange'],
    }
    response = render_to_response('userProfile.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@never_cache
def userProfileData(request):
    """A view that returns data for task profile plot"""
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "userProfileData", isData=True)
    data = None
    if data is not None:
        data = json.loads(data)
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')

    if 'username' in request.session['requestParams'] and request.session['requestParams']['username']:
        username = str(request.session['requestParams']['username'])
    else:
        msg = 'No username provided: {} is not valid, it must be string'
        _logger.warning(msg)
        response = HttpResponse(json.dumps(msg), status=400)
        return response

    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype']:
        request_job_types = request.session['requestParams']['jobtype'].split(',')
    else:
        request_job_types = None
    if 'jobstatus' in request.session['requestParams'] and request.session['requestParams']['jobstatus']:
        request_job_states = request.session['requestParams']['jobstatus'].split(',')
        if 'active' in request.session['requestParams']['jobstatus']:
            # replace active with list of real job states
            request.session['requestParams']['jobstatus'] = request.session['requestParams']['jobstatus'].replace(
                'active',
                ','.join(list(set(const.JOB_STATES) - set(const.JOB_STATES_FINAL))))
    else:
        request_job_states = None

    # get raw profile data
    if len(username) > 0:
        query = setupView(request, hours=24 * 7, querytype='job', wildCardExt=False)
        user_Dataprofile = UserProfilePlot(username)
        user_Dataprofile_dict = user_Dataprofile.get_raw_data_profile_full(query)
    else:
        msg = 'Not valid username provided: {}'.format(username)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

    # filter raw data corresponding to request params
    if request_job_types is not None and len(request_job_types) > 0:
        for jt, values in user_Dataprofile_dict.items():
            if jt not in request_job_types:
                user_Dataprofile_dict[jt] = []
    if request_job_states is not None and len(request_job_states) > 0:
        for jt, values in user_Dataprofile_dict.items():
            temp = []
            for v in values:
                if v['jobstatus'] in request_job_states:
                    temp.append(v)
            user_Dataprofile_dict[jt] = temp

    # convert raw data to format acceptable by chart.js library
    job_time_names = ['end', 'start', 'creation']
    job_types = ['build', 'run', 'merge']
    job_states = ['active', 'finished', 'failed', 'closed', 'cancelled']

    colors = {
        'creation': {'active': 'RGBA(0,169,255,0.75)', 'finished': 'RGBA(162,198,110,0.75)', 'failed': 'RGBA(255,176,176,0.75)',
                     'closed': 'RGBA(214,214,214,0.75)', 'cancelled': 'RGBA(255,227,177,0.75)'},
        'start': {'active': 'RGBA(0,85,183,0.75)', 'finished': 'RGBA(70,181,117,0.8)', 'failed': 'RGBA(235,0,0,0.75)',
                  'closed': 'RGBA(100,100,100,0.75)', 'cancelled': 'RGBA(255,165,0,0.75)'},
        'end': {'active': 'RGBA(0,0,141,0.75)', 'finished': 'RGBA(0,100,0,0.75)', 'failed': 'RGBA(137,0,0,0.75)',
                'closed': 'RGBA(0,0,0,0.75)', 'cancelled': 'RGBA(157,102,0,0.75)'},
    }
    markers = {'build': 'triangle', 'run': 'circle', 'merge': 'crossRot'}
    order_mpx = {
        'creation': 1,
        'start': 2,
        'end': 3,
        'finished': 4,
        'failed': 3,
        'closed': 2,
        'cancelled': 1,
        'active': 5,
    }
    order_dict = {}
    for jtn in job_time_names:
        for js in job_states:
            order_dict[jtn+'_'+js] = order_mpx[js] * order_mpx[jtn]

    user_Dataprofile_data_dict = {}
    for jt in job_types:
        if len(user_Dataprofile_dict[jt]) > 0:
            for js in list(set(job_states) & set([r['jobstatus'] for r in user_Dataprofile_dict[jt]])):
                for jtmn in job_time_names:
                    user_Dataprofile_data_dict['_'.join((jtmn, js, jt))] = {
                        'name': '_'.join((jtmn, js, jt)),
                        'label': jtmn.capitalize() + ' time of a ' + js + ' ' + jt + ' job',
                        'pointRadius': round(1 + 4.0 * math.exp(-0.0004*len(user_Dataprofile_dict[jt]))),
                        'backgroundColor': colors[jtmn][js],
                        'borderColor': colors[jtmn][js],
                        'pointStyle': markers[jt],
                        'data': [],
                    }

    for jt in job_types:
        if jt in user_Dataprofile_dict:
            rdata = user_Dataprofile_dict[jt]
            for r in rdata:
                for jtn in job_time_names:
                    if jtn in r and r[jtn] is not None:
                        user_Dataprofile_data_dict['_'.join((jtn, r['jobstatus'], jt))]['data'].append({
                            't': r[jtn].strftime(settings.DATETIME_FORMAT),
                            'y': r['indx'],
                            'label': r['pandaid'],
                        })

    # deleting point groups if data is empty
    group_to_remove = []
    for group in user_Dataprofile_data_dict:
        if len(user_Dataprofile_data_dict[group]['data']) == 0:
            group_to_remove.append(group)
    for group in group_to_remove:
        try:
            del user_Dataprofile_data_dict[group]
        except:
            _logger.info('failed to remove key from dict')

    # dict -> list
    user_Dataprofile_data = [v for k, v in user_Dataprofile_data_dict.items()]

    data = {'plotData': user_Dataprofile_data, 'error': ''}
    setCacheEntry(request, "userProfileData", json.dumps(data, cls=DateEncoder), 60 * 30, isData=True)
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


@login_customrequired
def taskInfo(request, jeditaskid=0):
    try:
        jeditaskid = int(jeditaskid)
    except:
        jeditaskid = re.findall("\d+", jeditaskid)
        jdtstr = ""
        for jdt in jeditaskid:
            jdtstr = jdtstr+str(jdt)
        return redirect('/task/'+jdtstr)
    valid, response = initRequest(request)
    if not valid:
        return response

    # return json for dataTables if dt in request params
    if 'dt' in request.session['requestParams'] and 'tkiec' in request.session['requestParams']:
        tkiec = request.session['requestParams']['tkiec']
        data = getCacheEntry(request, tkiec, isData=True)
        return HttpResponse(data, content_type='application/json')

    # Here we try to get cached data. We get any cached data is available
    data = getCacheEntry(request, "taskInfo", skipCentralRefresh=True)

    # temporarily turn off caching
    # data = None
    if data is not None:
        data = json.loads(data)
        if data is not None:
            doRefresh = False

            # check the build date of cached data, since data structure changed on 2021-03-22 and
            # we need to refresh cached data for ended tasks which we store for 1 month
            if 'built' in data and data['built'] is not None:
                try:
                    builtDate = datetime.strptime('2021-'+data['built'], settings.DATETIME_FORMAT)
                    if builtDate < datetime.strptime('2021-03-22 14:00:00', settings.DATETIME_FORMAT):
                        doRefresh = True
                except:
                    doRefresh = True

            # We still want to refresh tasks if request came from central crawler and task not in the frozen state
            if (('REMOTE_ADDR' in request.META) and (request.META['REMOTE_ADDR'] in settings.CACHING_CRAWLER_HOSTS) and
                    data['task'] and data['task']['status'] not in ['broken', 'aborted']):
                doRefresh = True

            # we check here whether task status didn't changed for both (user or crawler request)
            if data['task'] and data['task']['status'] and data['task']['status'] in ['done', 'finished', 'failed']:
                if 'jeditaskid' in request.session['requestParams']: jeditaskid = int(
                    request.session['requestParams']['jeditaskid'])
                if jeditaskid != 0:
                    query = {'jeditaskid': jeditaskid}
                    values = ['status', 'superstatus', 'modificationtime']
                    tasks = JediTasks.objects.filter(**query).values(*values)[:1]
                    if len(tasks) > 0:
                        task = tasks[0]
                        if (task['status'] == data['task']['status'] and task['superstatus'] == data['task']['superstatus'] and
                                task['modificationtime'].strftime(settings.DATETIME_FORMAT) == data['task']['modificationtime']):
                            doRefresh = False
                        else:
                            doRefresh = True
                    else:
                        doRefresh = True
            # doRefresh = True

            if not doRefresh:
                data['request'] = request
                if data['eventservice']:
                    if 'version' not in request.session['requestParams'] or (
                            'version' in request.session['requestParams'] and request.session['requestParams']['version'] != 'old'):
                        response = render_to_response('taskInfoESNew.html', data, content_type='text/html')
                    else:
                        response = render_to_response('taskInfoES.html', data, content_type='text/html')
                else:
                    response = render_to_response('taskInfo.html', data, content_type='text/html')
                patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
                return response

    if 'taskname' in request.session['requestParams'] and request.session['requestParams']['taskname'].find('*') >= 0:
        return taskList(request)

    setupView(request, hours=365 * 24, limit=999999999, querytype='task')
    tasks = []
    warning = {}

    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])

    mode = 'drop'
    if 'mode' in request.session['requestParams']:
        mode = request.session['requestParams']['mode']

    # if no jeditaskid provided, try to find by task name
    if jeditaskid < 1:
        if 'taskname' in request.session['requestParams']:
            querybyname = {'taskname': request.session['requestParams']['taskname']}
            tasks.extend(JediTasks.objects.filter(**querybyname).values())
            if len(tasks) > 0:
                jeditaskid = tasks[0]['jeditaskid']
        else:
            return redirect('/tasks/')

    # getting task info
    taskrec = None
    query = {'jeditaskid': jeditaskid}
    extra = '(1=1)'
    tasks.extend(JediTasks.objects.filter(**query).values())

    tasks = cleanTaskList(tasks, add_datasets_info=False)
    try:
        taskrec = tasks[0]
    except IndexError:
        _logger.exception('No task with jeditaskid={} found'.format(jeditaskid))
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'columns': None,
        }
        return render_to_response('taskInfo.html', data, content_type='text/html')

    eventservice = False
    if 'eventservice' in taskrec and (taskrec['eventservice'] == 1 or taskrec['eventservice'] == 'eventservice'):
        eventservice = True
        mode = 'nodrop'

    # nodrop only for tasks older than 2 years
    if get_task_timewindow(taskrec, format_out='datetime')[0] <= datetime.now() - timedelta(days=365*3):
        warning['dropmode'] = """The drop mode is unavailable since the data of job retries was cleaned up. 
                    The data shown on the page is in nodrop mode."""
        mode = 'nodrop'
        warning['archive'] = "The jobs data is moved to the archive, so the links to jobs page is unavailable"

    # iDDS section
    task_type = checkIfIddsTask(taskrec)
    idds_info = None
    if task_type == 'hpo':
        mode = 'nodrop'
        idds_info = {'task_type': 'hpo'}
    else:
        idds_info = {'task_type': 'idds'}

    # prepare ordered list of task params
    columns = []
    for k, val in taskrec.items():
        if is_timestamp(k):
            try:
                val = taskrec[k].strftime(settings.DATETIME_FORMAT)
            except:
                val = str(taskrec[k])
        if val is None:
            val = '-'
            # do not add params with empty value
            continue
        pair = {'name': k, 'value': val}
        columns.append(pair)
    columns = sorted(columns, key=lambda x: x['name'].lower())

    # get task params
    taskparams = get_task_params(jeditaskid)
    _logger.info('Got task info: {}'.format(time.time() - request.session['req_init_time']))

    # load logtxt
    logtxt = None
    if taskrec and taskrec['errordialog']:
        mat = re.match('^.*"([^"]+)"', taskrec['errordialog'])
        if mat:
            errurl = mat.group(1)
            cmd = "curl -s -f --compressed '{}'".format(errurl)
            logout = subprocess.getoutput(cmd)
            if len(logout) > 0:
                loglist = (logout.splitlines())[::-1]
                logtxt = '\n'.join(loglist)
            _logger.info("Loaded error log using '{}': {}".format(cmd, time.time() - request.session['req_init_time']))

    # get datasets list and containers
    dsets, dsinfo = datasets_for_task(jeditaskid)
    if taskrec:
        taskrec['dsinfo'] = dsinfo
        taskrec['totev'] = dsinfo['neventsTot']
        taskrec['totevproc'] = dsinfo['neventsUsedTot']
        taskrec['pctfinished'] = (100 * taskrec['totevproc'] / taskrec['totev']) if (taskrec['totev'] > 0) else ''
        taskrec['totevhs06'] = round(dsinfo['neventsTot'] * convert_hs06(taskrec['cputime'], taskrec['cputimeunit'])) if (taskrec['cputime'] and taskrec['cputimeunit'] and dsinfo['neventsTot'] > 0) else None
        taskrec['totevoutput'] = dsinfo['neventsOutput'] if 'neventsOutput' in dsinfo else 0
    # get input and output containers
    inctrs = []
    outctrs = []
    if 'dsForIN' in taskparams and taskparams['dsForIN'] and isinstance(taskparams['dsForIN'], str):
        inctrs = [{
            'containername': cin,
            'nfiles': 0,
            'nfilesfinished': 0,
            'nfilesfailed': 0, 'pct': 0
        } for cin in taskparams['dsForIN'].split(',')]
        # fill the list of input containers with progress info
        for inc in inctrs:
            for ds in dsets:
                if ds['containername'] == inc['containername']:
                    inc['nfiles'] += ds['nfiles'] if ds['nfiles'] else 0
                    inc['nfilesfinished'] += ds['nfilesfinished'] if ds['nfilesfinished'] else 0
                    inc['nfilesfailed'] += ds['nfilesfailed'] if ds['nfilesfailed'] else 0
                    inc['pct'] = math.floor(100.0*inc['nfilesfinished']/inc['nfiles']) if ds['nfiles'] and ds['nfiles'] > 0 else inc['pct']

    outctrs.extend(list(set([ds['containername'] for ds in dsets if ds['type'] in ('output', 'log') and ds['containername']])))
    # get dataset locality
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        dataset_locality = get_dataset_locality(jeditaskid)
    else:
        dataset_locality = {}
    for ds in dsets:
        if jeditaskid in dataset_locality and ds['datasetid'] in dataset_locality[jeditaskid]:
            ds['rse'] = ', '.join([item['rse'] for item in dataset_locality[jeditaskid][ds['datasetid']]])
    _logger.info("Loading datasets info: {}".format(time.time() - request.session['req_init_time']))

    # getBrokerageLog(request)

    # get sum of hs06sec grouped by status
    # creating a jquery with timewindow
    jquery = copy.deepcopy(query)
    jquery['modificationtime__castdate__range'] = get_task_timewindow(taskrec, format_out='str')
    if settings.DEPLOYMENT != 'POSTGRES':
        hs06sSum = get_hs06s_summary_for_task(jquery)
    else:
        hs06sSum = {}
    _logger.info("Loaded sum of hs06sec grouped by status: {}".format(time.time() - request.session['req_init_time']))

    eventssummary = []
    if eventservice:
        # insert dropped jobs to temporary table if drop mode
        transactionKeyDJ = -1
        if mode == 'drop':
            extra, transactionKeyDJ = insert_dropped_jobs_to_tmp_table(query, extra)
            _logger.info("Inserting dropped jobs: {}".format(time.time() - request.session['req_init_time']))
            _logger.info('tk of dropped jobs: {}'.format(transactionKeyDJ))

        # getting events summary for a ES task
        taskrec['totevproc_evst'] = 0
        equery = copy.deepcopy(query)
        # set timerange for better use of partitioned JOBSARCHIVED
        equery['creationdate__range'] = get_task_timewindow(taskrec, format_out='str')
        eventssummary = event_summary_for_task(mode, equery, tk_dj=transactionKeyDJ)
        for entry in eventssummary:
            if 'count' in entry and 'totev' in taskrec and taskrec['totev'] > 0:
                entry['pct'] = round(entry['count'] * 100. / taskrec['totev'], 2)
            else:
                entry['pct'] = 0
            status = entry.get("statusname", "-")
            if status in ['finished', 'done', 'merged']:
                taskrec['totevproc_evst'] += entry.get("count", 0)
        # update task dict in data with more accurate events data
        if taskrec:
            taskrec['pcttotevproc_evst'] = round(100. * taskrec['totevproc_evst'] / taskrec['totev'], 2) if taskrec['totev'] > 0 else ''
            taskrec['pctfinished'] = round(100. * taskrec['totevproc'] / taskrec['totev'], 2) if taskrec['totev'] > 0 else ''
        _logger.info("Events states summary: {}".format(time.time() - request.session['req_init_time']))

        # get corecount and normalized corecount values
        ccquery = {
            'jeditaskid': jeditaskid,
            'jobstatus': 'running',
        }
        accsum = Jobsactive4.objects.filter(**ccquery).aggregate(accsum=Sum('actualcorecount'))
        naccsum = Jobsactive4.objects.filter(**ccquery).aggregate(
            naccsum=Sum(F('actualcorecount') * F('hs06') / F('corecount') / Value(10), output_field=FloatField()))
        taskrec['accsum'] = accsum['accsum'] if 'accsum' in accsum else 0
        taskrec['naccsum'] = naccsum['naccsum'] if 'naccsum' in naccsum else 0
        _logger.info("Loaded corecount and normalized corecount: {}".format(time.time() - request.session['req_init_time']))

    # update taskrec dict
    if taskrec:
        if 'tasktype' in taskrec and taskrec['tasktype']:
            tmcj_list = get_top_memory_consumers(taskrec)
            if len(tmcj_list) > 0 and len([True for job in tmcj_list if job['maxrssratio'] >= 1]) > 0:
                warning['memoryleaksuspicion'] = {}
                warning['memoryleaksuspicion']['message'] = 'Some jobs in this task consumed a lot of memory. '
                warning['memoryleaksuspicion']['message'] += 'We suspect there might be memory leaks or some misconfiguration.'
                warning['memoryleaksuspicion']['jobs'] = tmcj_list

        if task_type is not None and idds_info is not None:
            for itn in idds_info:
                if itn in idds_info and isinstance(idds_info[itn], datetime):
                    idds_info[itn] = idds_info[itn].strftime(settings.DATETIME_FORMAT)
            taskrec['idds_info'] = idds_info

        if 'ticketsystemtype' in taskrec and taskrec['ticketsystemtype'] == '' and taskparams is not None:
            if 'ticketID' in taskparams:
                taskrec['ticketid'] = taskparams['ticketID']
            if 'ticketSystemType' in taskparams:
                taskrec['ticketsystemtype'] = taskparams['ticketSystemType']

        if 'creationdate' in taskrec:
            taskrec['kibanatimefrom'] = taskrec['creationdate'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimefrom'] = None
        if taskrec['status'] in ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']:
            taskrec['kibanatimeto'] = taskrec['modificationtime'].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            taskrec['kibanatimeto'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        if len(set([ds['storagetoken'] for ds in dsets if 'storagetoken' in ds and ds['storagetoken']])) > 0:
            taskrec['destination'] = list(set([ds['storagetoken'] for ds in dsets if ds['storagetoken']]))[0]
        elif taskrec['cloud'] == 'WORLD':
            taskrec['destination'] = taskrec['nucleus']

        if hs06sSum:
            taskrec['totevprochs06'] = int(hs06sSum['finished']) if 'finished' in hs06sSum else None
            taskrec['failedevprochs06'] = int(hs06sSum['failed']) if 'failed' in hs06sSum else None
            taskrec['currenttotevhs06'] = int(hs06sSum['total']) if 'total' in hs06sSum else None

        taskrec['brokerage'] = 'prod_brokerage' if taskrec['tasktype'] == 'prod' else 'analy_brokerage'
        if settings.DEPLOYMENT == 'ORACLE_ATLAS':
            taskrec['slice'] = get_prod_slice_by_taskid(jeditaskid) if taskrec['tasktype'] == 'prod' else None

    # datetime type -> str in order to avoid encoding errors in template
    datetime_task_param_names = ['creationdate', 'modificationtime', 'starttime', 'statechangetime', 'ttcrequested']
    datetime_dataset_param_names = ['statechecktime', 'creationtime', 'modificationtime']
    if taskrec:
        for dtp in datetime_task_param_names:
            if taskrec[dtp]:
                taskrec[dtp] = taskrec[dtp].strftime(settings.DATETIME_FORMAT)
    for dset in dsets:
        for dsp, dspv in dset.items():
            if dsp in datetime_dataset_param_names and dspv is not None:
                dset[dsp] = dset[dsp].strftime(settings.DATETIME_FORMAT)
            if dspv is None:
                dset[dsp] = ''

    try:
        del request.session['TFIRST']
        del request.session['TLAST']
    except:
        _logger.exception('Failed to delete TFIRST and TLAST from request session')

    if is_json_request(request):

        del tasks
        del columns

        data = {
            'task': taskrec,
            'taskparams': taskparams,
            'datasets': dsets,
        }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:

        taskparams, jobparams = humanize_task_params(taskparams)
        furl = request.get_full_path()
        nomodeurl = extensibleURL(request, removeParam(furl, 'mode'))

        # decide on data caching time [seconds]
        cacheexpiration = 60 * 20  # second/minute * minutes
        if taskrec and 'status' in taskrec and taskrec['status'] in const.TASK_STATES_FINAL and (
                'dsinfo' in taskrec and 'nfiles' in taskrec['dsinfo'] and isinstance(taskrec['dsinfo']['nfiles'], int) and taskrec['dsinfo']['nfiles'] > 10000):
            cacheexpiration = 3600 * 24 * 31  # we store such data a month

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'furl': furl,
            'nomodeurl': nomodeurl,
            'mode': mode,
            'task': taskrec,
            'taskparams': taskparams,
            'jobparams': jobparams,
            'columns': columns,
            'jeditaskid': jeditaskid,
            'logtxt': logtxt,
            'datasets': dsets,
            'inctrs': inctrs,
            'outctrs': outctrs,
            'vomode': VOMODE,
            'eventservice': eventservice,
            'built': datetime.now().strftime("%m-%d %H:%M:%S"),
            'warning': warning,
        }
        data.update(getContextVariables(request))

        if eventservice:
            data['eventssummary'] = eventssummary
            if 'version' not in request.session['requestParams'] or (
                    'version' in request.session['requestParams'] and request.session['requestParams']['version'] != 'old'):
                # prepare input-centric ES taskInfo
                _logger.info("This is input-centric ES taskInfo request")
                # get input files state summary
                transactionKeyIEC = -1
                ifs_summary = []
                inputfiles_list, ifs_summary, ifs_tk = input_summary_for_task(taskrec, dsets)

                # Putting list of inputs IDs to tmp table for connection with jobList
                for tk, ids_list in ifs_tk.items():
                    tk = insert_to_temp_table(ids_list, tk)

                # Putting list of inputs to cache separately for dataTables plugin
                transactionKeyIEC = random.randrange(100000000)
                setCacheEntry(request, transactionKeyIEC, json.dumps(inputfiles_list, cls=DateTimeEncoder), 60 * 30, isData=True)
                _logger.info("Inputs states summary: {}".format(time.time() - request.session['req_init_time']))

                # get lighted job summary
                jobsummarylight, jobsummarylightsplitted = job_summary_for_task_light(taskrec)
                _logger.info("Loaded lighted job summary: {}".format(time.time() - request.session['req_init_time']))

                data['iecsummary'] = ifs_summary
                data['tkiec'] = transactionKeyIEC
                data['jobsummarylight'] = jobsummarylight
                data['jobsummarylightsplitted'] = jobsummarylightsplitted
                data['tkdj'] = transactionKeyDJ
                setCacheEntry(request, "taskInfo", json.dumps(data, cls=DateEncoder), cacheexpiration)
                response = render_to_response('taskInfoESNew.html', data, content_type='text/html')
            else:
                _logger.info("This old style ES taskInfo request")
                # getting job summary and plots
                plotsDict, jobsummary, scouts, metrics = job_summary_for_task(
                    jquery, '(1=1)',
                    mode=mode,
                    task_archive_flag=get_task_time_archive_flag(get_task_timewindow(taskrec, format_out='datatime')))
                data['jobsummary'] = jobsummary
                data['plotsDict'] = plotsDict
                data['jobscoutids'] = scouts
                setCacheEntry(request, "taskInfo", json.dumps(data, cls=DateEncoder), cacheexpiration)
                response = render_to_response('taskInfoES.html', data, content_type='text/html')
        else:
            _logger.info("This is ordinary non-ES task")
            # getting job summary and plots
            plotsDict, jobsummary, scouts, metrics = job_summary_for_task(
                jquery, '(1=1)',
                mode=mode,
                task_archive_flag=get_task_time_archive_flag(get_task_timewindow(taskrec, format_out='datatime')))
            data['jobsummary'] = jobsummary
            data['plotsDict'] = plotsDict
            data['jobscoutids'] = scouts
            data['task'].update(metrics)
            setCacheEntry(request, "taskInfo", json.dumps(data, cls=DateEncoder), cacheexpiration)
            response = render_to_response('taskInfo.html', data, content_type='text/html')
        _logger.info('Rendered template: {}'.format(time.time() - request.session['req_init_time']))
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


def getJobSummaryForTask(request, jeditaskid=-1):
    valid, response = initRequest(request)
    if not valid:
        return response

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
        return HttpResponse(status=404)

    if 'es' in request.session['requestParams'] and request.session['requestParams']['es'] == 'True':
        es = True
    else:
        es = False

    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'drop':
        mode = 'drop'
    else:
        mode = 'nodrop'

    data = getCacheEntry(request, "jobSummaryForTask"+str(jeditaskid)+mode, isData=True)
    data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request

        if infotype == 'jobsummary':
            response = render_to_response('jobSummaryForTask.html', data, content_type='text/html')
        elif infotype == 'scouts':
            response = render_to_response('scoutsForTask.html', data, content_type='text/html')
        elif infotype == 'plots':
            response = HttpResponse(json.dumps(data['plotsDict'], cls=DateEncoder), content_type='application/json')
        else:
            response = HttpResponse(status=404)
        return response

    extra = '(1=1)'
    query = {
        'jeditaskid': jeditaskid,
    }

    if mode == 'drop':
        start = time.time()
        extra, transactionKeyDJ = insert_dropped_jobs_to_tmp_table(query, extra)
        end = time.time()
        _logger.info("Inserting dropped jobs: {} sec".format(end - start))
        _logger.debug('tk of dropped jobs: {}'.format(transactionKeyDJ))

    # pass mode='nodrop' as we already took dropping into account in extra query str
    plotsDict, jobsummary, jobScoutIDs, metrics = job_summary_for_task(query, extra=extra, mode='nodrop')

    alldata = {
        'jeditaskid': jeditaskid,
        'request': request,
        'jobsummary': jobsummary,
        'jobScoutIDs': jobScoutIDs,
        'plotsDict': plotsDict,
    }
    setCacheEntry(request, 'jobSummaryForTask'+str(jeditaskid)+mode, json.dumps(alldata, cls=DateEncoder), 60 * 10, isData=True)

    if infotype == 'jobsummary':
        data = {
            'jeditaskid': jeditaskid,
            'mode': mode,
            'jobsummary': jobsummary,
        }
        response = render_to_response('jobSummaryForTask.html', data, content_type='text/html')
    elif infotype == 'scouts':
        data = {
            'jeditaskid': jeditaskid,
            'jobscoutids': jobScoutIDs,
        }
        response = render_to_response('scoutsForTask.html', data, content_type='text/html')
    elif infotype == 'plots':
        response = HttpResponse(json.dumps(plotsDict, cls=DateEncoder), content_type='application/json')
    else:
        response = HttpResponse(status=204)
    if response:
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def taskFlowDiagram(request, jeditaskid=-1):
    """
    Prepare data for task flow chart
    :param request:
    :param jeditaskid:
    :return:
    """
    data = {'data': ''}
    try:
        jeditaskid = int(jeditaskid)
    except:
        jeditaskid = 0
        _logger.exception('jeditaskid={} must be int'.format(jeditaskid))

    if jeditaskid < 0:
        data['error'] = 'No jeditaskid provided'
    elif jeditaskid == 0:
        data['error'] = 'Not valid jeditaskid provided'
    else:
        data['data'] = get_task_flow_data(jeditaskid)

    response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    return response


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

    _logger.info('Initialized request: {}'.format(time.time() - request.session['req_init_time']))

    # Here we try to get cached data
    data = getCacheEntry(request, "errorSummary")
    if data is not None:
        _logger.info('Got cached data: {}'.format(time.time() - request.session['req_init_time']))
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
        _logger.info('Processed cached data: {}'.format(time.time() - request.session['req_init_time']))
        response = render_to_response('errorSummary.html', data, content_type='text/html')
        _logger.info('Rendered template from cached data: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    testjobs = False
    if 'prodsourcelabel' in request.session['requestParams'] and request.session['requestParams']['prodsourcelabel'].lower().find('test') >= 0:
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
        panda_queues = get_panda_queues()
        sites = set([site['site'] for site in panda_queues.values() if site['cloud'] == cloud])
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

    _logger.info('Processed specific params: {}'.format(time.time() - request.session['req_init_time']))

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=limit, wildCardExt=True)

    _logger.info('Finished set up view: {}'.format(time.time() - request.session['req_init_time']))

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
        'destinationse', 'currentpriority', 'computingelement', 'gshare', 'reqid', 'actualcorecount', 'computingelement'
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

    if not is_json_request(request):
        thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension, dkey))
        thread.start()
    else:
        thread = None

    _logger.info('Got jobs: {}'.format(time.time() - request.session['req_init_time']))

    jobs = clean_job_list(request, jobs, do_add_metadata=False, do_add_errorinfo=True)

    _logger.info('Cleaned jobs list: {}'.format(time.time() - request.session['req_init_time']))

    error_message_summary = get_error_message_summary(jobs)

    _logger.info('Prepared new error message summary: {}'.format(time.time() - request.session['req_init_time']))

    njobs = len(jobs)

    # Build the error summary.
    errsByCount, errsBySite, errsByUser, errsByTask, sumd, errHist = errorSummaryDict(request, jobs, testjobs, errHist=True)

    _logger.info('Error summary built: {}'.format(time.time() - request.session['req_init_time']))

    # Build the state summary and add state info to site error summary
    notime = False  # behave as it used to before introducing notime for dashboards. Pull only 12hrs.
    statesummary = cloud_site_summary(query, extra=wildCardExtension, view=jobtype, cloudview='region', notime=notime)
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

    _logger.info('Built errors by site summary: {}'.format(time.time() - request.session['req_init_time']))

    taskname = ''
    if not testjobs:
        # Build the task state summary and add task state info to task error summary
        taskstatesummary = task_summary(query, limit=limit, view=jobtype)

        _logger.info('Prepared data for errors by task summary: {}'.format(time.time() - request.session['req_init_time']))

        taskstates = {}
        for task in taskstatesummary:
            taskid = task['taskid']
            taskstates[taskid] = {}
            for s in savestates:
                taskstates[taskid][s] = task['states'][s]['count']
            if 'pctfail' in task:
                taskstates[taskid]['pctfail'] = task['pctfail']
        for task in errsByTask:
            taskid = task['name']
            if taskid in taskstates:
                for s in savestates:
                    if s in taskstates[taskid]:
                        task[s] = taskstates[taskid][s]
                if 'pctfail' in taskstates[taskid]:
                    task['pctfail'] = taskstates[taskid]['pctfail']
        if 'jeditaskid' in request.session['requestParams']:
            taskname = get_task_name_by_taskid('jeditaskid', request.session['requestParams']['jeditaskid'])

    _logger.info('Built errors by task summary: {}'.format(time.time() - request.session['req_init_time']))

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'alpha'
    flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)

    _logger.info('Built google diagram: {}'.format(time.time() - request.session['req_init_time']))

    if thread is not None:
        try:
            thread.join()
            jobsErrorsTotalCount = sum(tcount[dkey])
            print(dkey)
            print(tcount[dkey])
            del tcount[dkey]
            print(tcount)
            print(jobsErrorsTotalCount)
        except:
            jobsErrorsTotalCount = -1
    else:
        jobsErrorsTotalCount = -1

    _logger.info('Finished thread counting total number of jobs: {}'.format(time.time() - request.session['req_init_time']))

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
    _logger.info('Formed list of params: {}'.format(time.time() - request.session['req_init_time']))

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
        'json' not in request.session['requestParams'])):
        nosorturl = removeParam(request.get_full_path(), 'sortby')
        xurl = extensibleURL(request)
        time_locked_url = removeParam(removeParam(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
                          'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
                          '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
        jobsurl = xurlsubst.replace('/errors/', '/jobs/')
        jobsurlNoSite = xurlsubstNoSite.replace('/errors/', '')

        TFIRST = request.session['TFIRST'].strftime(settings.DATETIME_FORMAT)
        TLAST = request.session['TLAST'].strftime(settings.DATETIME_FORMAT)
        del request.session['TFIRST']
        del request.session['TLAST']

        _logger.info('Extra data preparation for template: {}'.format(time.time() - request.session['req_init_time']))

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

        _logger.info('Set cache: {}'.format(time.time() - request.session['req_init_time']))

        # Filtering data due to user settings
        if request.user.is_authenticated and request.user.is_tester:
            data = filterErrorData(request, data)
        response = render_to_response('errorSummary.html', data, content_type='text/html')

        _logger.info('Rendered template: {}'.format(time.time() - request.session['req_init_time']))

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
    pq_clouds = get_pq_clouds()
    iquery = {}
    cloudQuery = Q()
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = timezone.now().strftime(settings.DATETIME_FORMAT)
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
        sites = [site for site, cloud in pq_clouds.items() if cloud == request.session['requestParams']['cloud']]
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
            if pars['site'] in pq_clouds:
                pars['cloud'] = pq_clouds[pars['site']]
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
        inc['at_time'] = inc['at_time'].strftime(settings.DATETIME_FORMAT)

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

    if not valid:
        return response

    connection = create_esatlas_connection()

    s = Search(using=connection, index='atlas_jedilogs-*')

    s.aggs.bucket('jediTaskID', 'terms', field='jediTaskID', size=1000)\
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    res = s.execute()

    print('query completed')

    jdListFinal = []

    for agg in res['aggregations']['jediTaskID']['buckets']:
        name = agg['key']
        for types in agg['type']['buckets']:
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
    if not valid:
        return response

    connection = create_esatlas_connection()

    today = time.strftime("%Y.%m.%d")

    pandaDesc = {
        "panda.log.RetrialModule": ["cat1","Retry module to apply rules on failed jobs"],

        "panda.log.Serveraccess": ["cat2","Apache request log"],
        "panda.log.Servererror": ["cat2","Apache errors"],
        "panda.log.PilotRequests": ["cat2", "Pilot requests"],
        "panda.log.Entry": ["cat2","Entry point to the PanDA server"],
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
    pandaCat = ['cat1', 'cat2', 'cat3', 'cat4', 'cat5', 'cat6', 'cat7', 'cat8']

    jediDesc = {
        "panda.log.AtlasProdTaskBroker": ["cat1","Production task brokerage"],
        "panda.log.TaskBroker": ["cat7","Task brokerage factory"],
        "panda.log.AtlasProdJobBroker": ["cat1","Production job brokerage"],
        "panda.log.AtlasAnalJobBroker": ["cat1", "Analysis job brokerage"],
        "panda.log.JobBroker": ["cat7","Job brokerage factory"],


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

    indices = ['atlas_pandalogs-', 'atlas_jedilogs-']

    panda = {}
    jedi = {}

    for index in indices:
        s = Search(using=connection, index=index + str(today))

        s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
            .bucket('type', 'terms', field='fields.type.keyword', size=1000) \
            .bucket('logLevel', 'terms', field='logLevel.keyword')

        res = s.execute()

        if index == "atlas_pandalogs-":
            for cat in pandaCat:
                panda[cat] = {}
            for agg in res['aggregations']['logName']['buckets']:
                if agg['key'] not in pandaDesc:
                    pandaDesc[agg['key']] = [list(panda.keys())[-1], "New log type. No description"]
                cat = pandaDesc[agg['key']][0]
                name = agg['key']
                panda[cat][name] = {}
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
                jedi[cat] = {}
            for agg in res['aggregations']['logName']['buckets']:
                if agg['key'] not in jediDesc:
                    jediDesc[agg['key']] = [list(jedi.keys())[-1], "New log type. No description"]
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


def pandaLogger(request):
    valid, response = initRequest(request)
    if not valid: return response
    getrecs = False
    iquery = {}
    if 'category' in request.session['requestParams']:
        iquery['name'] = request.session['requestParams']['category']
        getrecs = True
    if 'type' in request.session['requestParams']:
        val = escape_input(request.session['requestParams']['type'])
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
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    if 'startdate' in request.session['requestParams'] and len(request.session['requestParams']['startdate']) > 1:
        startdate = request.session['requestParams']['startdate']

    enddate = timezone.now().strftime(settings.DATETIME_FORMAT)
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
        taskrec['ttc'] = taskrec['ttc'].strftime(settings.DATETIME_FORMAT)
    if taskrec['creationdate']:
        taskrec['creationdate'] = taskrec['creationdate'].strftime(settings.DATETIME_FORMAT)
    if taskrec['starttime']:
        taskrec['starttime'] = taskrec['starttime'].strftime(settings.DATETIME_FORMAT)
    if taskrec['endtime']:
        taskrec['endtime'] = taskrec['endtime'].strftime(settings.DATETIME_FORMAT)

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
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "workingGroups")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('workingGroups.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    taskdays = 3
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        VOMODE = 'atlas'
    else:
        VOMODE = ''
    if VOMODE != 'atlas':
        days = 30
    else:
        days = taskdays

    errthreshold = 15
    hours = days * 24
    query = setupView(request, hours=hours, limit=999999)
    query['workinggroup__isnull'] = False

    # WG task summary
    tasksummary = wg_task_summary(request, view='working group', taskdays=taskdays)

    # WG job summary
    if 'workinggroup' in request.session['requestParams'] and request.session['requestParams']['workinggroup']:
        query['workinggroup'] = request.session['requestParams']['workinggroup']
    wgsummary = wg_summary(query)

    if not is_json_request(request):
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
    wild_card_str = '(1=1)'
    if 'datasetname' in request.session['requestParams']:
        dataset = request.session['requestParams']['datasetname']
        if '*' not in dataset:
            query['datasetname'] = request.session['requestParams']['datasetname']
        else:
            wild_card_str += ' AND ' + preprocess_wild_card_string(dataset, 'datasetname', case_sensitivity=True)
    elif 'datasetid' in request.session['requestParams']:
        dataset = request.session['requestParams']['datasetid']
        query['datasetid'] = request.session['requestParams']['datasetid']
    else:
        dataset = None

    if 'jeditaskid' in request.session['requestParams']:
        query['jeditaskid'] = int(request.session['requestParams']['jeditaskid'])

    if dataset:
        dsets.extend(JediDatasets.objects.filter(**query).extra(where=[wild_card_str]).values())
        if len(dsets) == 0:
            startdate = timezone.now() - timedelta(hours=30 * 24)
            startdate = startdate.strftime(settings.DATETIME_FORMAT)
            enddate = timezone.now().strftime(settings.DATETIME_FORMAT)
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
                    ds['creationtime'] = ds['creationdate'].strftime(settings.DATETIME_FORMAT)
                    ds['modificationtime'] = ds['modificationdate'].strftime(settings.DATETIME_FORMAT)
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
                    val = dsrec[k].strftime(settings.DATETIME_FORMAT)
                except:
                    val = dsrec[k]
            else:
                val = dsrec[k]
            if dsrec[k] is None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)
    del request.session['TFIRST']
    del request.session['TLAST']

    if not is_json_request(request):
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
    if not valid:
        return response
    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    wild_card_str = '(1=1)'

    if 'datasetname' in request.session['requestParams']:
        if ':' in request.session['requestParams']['datasetname']:
            request.session['requestParams']['datasetname'] = '*' + request.session['requestParams']['datasetname'].split(':')[1]
        if '*' in request.session['requestParams']['datasetname']:
            wild_card_str += ' AND ' + preprocess_wild_card_string(
                request.session['requestParams']['datasetname'],
                'datasetname',
                case_sensitivity=True)
        else:
            query['datasetname'] = request.session['requestParams']['datasetname']
    if 'containername' in request.session['requestParams']:
        query['datasetname'] = request.session['requestParams']['containername']
    if 'jeditaskid' in request.session['requestParams']:
        query['jeditaskid'] = int(request.session['requestParams']['jeditaskid'])

    dsets = []
    if len(query) > 0 or len(wild_card_str) > 5:
        dsets.extend(JediDatasets.objects.filter(**query).extra(where=[wild_card_str]).values())
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())

    del request.session['TFIRST']
    del request.session['TLAST']

    if not is_json_request(request):
        # redirect to datasetInfo if only one dataset found
        if len(dsets) == 1:
            return redirect('/datasetInfo/?datasetname=' + dsets[0]['datasetname'])

        timestamp_vars = ['modificationtime', 'statechangetime', 'starttime', 'creationdate', 'resquetime',
                          'endtime', 'lockedtime', 'frozentime', 'creationtime', 'statechecktime']
        for ds in dsets:
            for p in ds:
                if p in timestamp_vars and ds[p] is not None:
                    ds[p] = ds[p].strftime(settings.DATETIME_FORMAT)
                if ds[p] is None:
                    ds[p] = ''
                if ds[p] is True:
                    ds[p] = 'true'
                if ds[p] is False:
                    ds[p] = 'false'
        data = {
            'request': request,
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
        return HttpResponse(json.dumps(dsets, cls=DateEncoder), content_type='application/json')


@login_customrequired
def fileInfo(request):

    valid, response = initRequest(request)
    if not valid:
        return response

    files = []
    frec = None
    columns = []

    tquery = setupView(request, hours=365 * 24, limit=999999999, wildCardExt=False)
    query = {'creationdate__castdate__range': tquery['modificationtime__castdate__range']}

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
            fquery = {k: v for k, v in query.items() if k != 'creationdate__castdate__range' }
            fquery['modificationtime__castdate__range'] = tquery['modificationtime__castdate__range']

            morefiles = Filestable4.objects.filter(**fquery).values()
            if len(morefiles) == 0:
                morefiles = FilestableArch.objects.filter(**fquery).values()
            if len(morefiles) > 0:
                files = morefiles
                for f in files:
                    f['creationdate'] = f['modificationtime']
                    f['fileid'] = f['row_id']
                    f['datasetname'] = f['dataset']
                    f['oldfiletable'] = 1

    if len(files) > 0:
        # get dataset names for files
        dids = list(set([f['datasetid'] for f in files]))
        dquery = {}
        extra = ' (1=1) '
        if len(dids) < settings.DB_N_MAX_IN_QUERY:
            dquery['datasetid__in'] = dids
        else:
            random.seed()
            transactionKey = random.randrange(1000000)
            tmpTableName = get_tmp_table_name()
            insert_to_temp_table(dids, transactionKey)
            extra += 'AND DATASETID in (SELECT ID FROM {} WHERE TRANSACTIONKEY={})'.format(tmpTableName, transactionKey)

        datasets = JediDatasets.objects.filter(**dquery).extra(where=[extra]).values('datasetname', 'datasetid')
        dataset_names_dict = {}
        for d in datasets:
            dataset_names_dict[d['datasetid']] = d['datasetname']

        for f in files:
            f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
            if 'datasetid' in f and f['datasetid'] in dataset_names_dict and dataset_names_dict[f['datasetid']]:
                f['datasetname'] = dataset_names_dict[f['datasetid']]
            else:
                f['datasetname'] = ''

        # filter out files if dataset name in request params
        if 'datasetname' in request.session['requestParams'] and request.session['requestParams']['datasetname']:
            files = [f for f in files if f['datasetname'] == request.session['requestParams']['datasetname']]

        files = sorted(files, key=lambda x: x['pandaid'] if x['pandaid'] is not None else False, reverse=True)
        frec = files[0]
        file = frec['lfn']
        colnames = frec.keys()
        colnames = sorted(colnames)
        for k in colnames:
            if is_timestamp(k):
                try:
                    val = frec[k].strftime(settings.DATETIME_FORMAT)
                except:
                    val = frec[k]
            else:
                val = frec[k]
            if frec[k] is None:
                val = ''
                continue
            pair = {'name': k, 'value': val}
            columns.append(pair)

        for f in files:
            f['startevent'] = f['startevent'] + 1 if 'startevent' in f and f['startevent'] is not None else -1
            f['endevent'] = f['endevent'] + 1 if 'endevent' in f and f['endevent'] is not None else -1
            for p in ('maxattempt', 'attemptnr', 'pandaid'):
                f[p] = f[p] if p in f and f[p] is not None else -1
            if 'creationdate' in f and f['creationdate'] is not None:
                f['creationdate'] = f['creationdate'].strftime(settings.DATETIME_FORMAT)

    if not is_json_request(request):
        del request.session['TFIRST']
        del request.session['TLAST']

        if frec and 'creationdate' in frec and frec['creationdate'] is None:
            frec['creationdate'] = frec['creationdate'].strftime(settings.DATETIME_FORMAT)

        files_list = []
        plot_data = []
        if len(files) > 0:
            # filter files params for a table
            file_param_names = [
                'lfn', 'datasetname', 'jeditaskid', 'pandaid', 'type', 'status', 'procstatus', 'creationdate',
                'startevent', 'endevent', 'attemptnr', 'maxattempt'
            ]
            files_list = [{k: v for k, v in f.items() if k in file_param_names} for f in files]

            # prepare data for a plot
            plot_data = {
                'data': [],
                'options': {
                    'timeFormat': '%Y-%m-%d',
                    'labels': ['Date', 'Number of occurrences, daily']
                }
            }
            df = pd.DataFrame([{'creationdate': f['creationdate'], 'pandaid': f['pandaid']} for f in files_list])
            df['creationdate'] = pd.to_datetime(df['creationdate'])
            df = df.groupby(pd.Grouper(freq='1D', key='creationdate')).count()
            plot_data['data'] = [df.reset_index()['creationdate'].tolist(), df['pandaid'].values.tolist()]
            plot_data['data'][0] = [t.strftime('%Y-%m-%d') for t in plot_data['data'][0]]
            plot_data['data'][0].insert(0, plot_data['options']['labels'][0])
            plot_data['data'][1].insert(0, plot_data['options']['labels'][1])

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'frec': frec,
            'files': files_list,
            'filename': file,
            'columns': columns,
            'built': datetime.now().strftime("%H:%M:%S"),
            'plotData': plot_data,
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
    else:
        data = {
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            "errormessage": "No datasetid or datasetname was provided",
        }
        return render_to_response('errorPage.html', data, content_type='text/html')

    extraparams = ''
    if 'procstatus' in request.session['requestParams'] and request.session['requestParams']['procstatus']:
        query['procstatus'] = request.session['requestParams']['procstatus']
        extraparams += '&procstatus=' + request.session['requestParams']['procstatus']

    dataset = []
    nfilestotal = 0
    nfilesunique = 0
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
            'datasetname': datasetname,
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
    extra_str = '(1=1)'
    files_ft = []
    files_ft_dict = {}
    if len(pandaids) > settings.DB_N_MAX_IN_QUERY:
        tk = insert_to_temp_table(pandaids)
        extra_str = 'pandaid in (select id from {} where transactionkey={} )'.format(get_tmp_table_name(), tk)
    else:
        query['pandaid__in'] = pandaids

    # JEDITASKID, DATASETID, FILEID
    files_ft.extend(
        Filestable4.objects.filter(**query).extra(where=[extra_str]).values('fileid', 'dispatchdblock', 'scope', 'destinationdblock'))
    if len(files_ft) == 0:
        files_ft.extend(
            FilestableArch.objects.filter(**query).extra(where=[extra_str]).values('fileid', 'dispatchdblock', 'scope', 'destinationdblock'))
    if len(files_ft) > 0:
        for f in files_ft:
            files_ft_dict[f['fileid']] = f

    for f in files:
        f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
        ruciolink_base = 'https://rucio-ui.cern.ch/did?scope='
        f['ruciolink'] = ''
        if f['fileid'] in files_ft_dict:
            name_param = ''
            if len(files_ft_dict[f['fileid']]['dispatchdblock']) > 0:
                name_param = 'dispatchdblock'
            elif len(files_ft_dict[f['fileid']]['destinationdblock']) > 0:
                name_param = 'destinationdblock'
            if len(name_param) > 0:
                if files_ft_dict[f['fileid']][name_param].startswith(files_ft_dict[f['fileid']]['scope']):
                    ruciolink_base += files_ft_dict[f['fileid']]['scope']
                else:
                    ruciolink_base += files_ft_dict[f['fileid']][name_param].split('.')[0]
                f['ruciolink'] = ruciolink_base + '&name=' + files_ft_dict[f['fileid']][name_param]
        f['creationdatecut'] = f['creationdate'].strftime('%Y-%m-%d')
        f['creationdate'] = f['creationdate'].strftime(settings.DATETIME_FORMAT)

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
    if not valid:
        return response
    query = setupView(request, opmode='notime', limit=99999999)
    pq_clouds = get_pq_clouds()
    if 'jobstatus' in request.session['requestParams']:
        state = request.session['requestParams']['jobstatus']
    if 'transferringnotupdated' in request.session['requestParams']:
        hoursSinceUpdate = int(request.session['requestParams']['transferringnotupdated'])
    if 'statenotupdated' in request.session['requestParams']:
        hoursSinceUpdate = int(request.session['requestParams']['statenotupdated'])
    moddate = timezone.now() - timedelta(hours=hoursSinceUpdate)
    moddate = moddate.strftime(settings.DATETIME_FORMAT)
    mindate = timezone.now() - timedelta(hours=24 * 30)
    mindate = mindate.strftime(settings.DATETIME_FORMAT)
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
            if site in pq_clouds:
                cloud = pq_clouds[site]
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
    moddate = moddate.strftime(settings.DATETIME_FORMAT)
    mindate = timezone.now() - timedelta(hours=24 * 30)
    mindate = mindate.strftime(settings.DATETIME_FORMAT)
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

        tmpTableName = get_tmp_table_name()

        transactionKey = random.randrange(1000000)
        new_cur = connection.cursor()
        if settings.DEPLOYMENT == "POSTGRES":
            create_temporary_table(new_cur, tmpTableName)
        for job in jobs:
            new_cur.execute("INSERT INTO %s(ID,TRANSACTIONKEY) VALUES (%i,%i)" % (
            tmpTableName, job['pandaid'], transactionKey))  # Backend dependable
        new_cur.execute(
            "SELECT JOBPARAMETERS, PANDAID FROM ATLAS_PANDA.JOBPARAMSTABLE WHERE PANDAID in (SELECT ID FROM %s WHERE TRANSACTIONKEY=%i)" % (
            tmpTableName, transactionKey))
        mrecs = dictfetchall(new_cur)
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

    jobs = addJobMetadata(jobs)
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


def handler500(request):
    response = render_to_response('500.html', {},
                                  context_instance=RequestContext(request))
    response.status_code = 500
    return response


def getBadEventsForTask(request):

    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return HttpResponse("Not jeditaskid supplied", content_type='text/html')

    mode = 'drop'
    if 'mode' in request.GET and request.GET['mode'] == 'nodrop':
        mode = 'nodrop'

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

    errorCodes = get_job_error_desc()

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


@never_cache
def getJobStatusLog(request, pandaid = None):
    """
    A view to asynchronously load job states changes history
    :param request:
    :param pandaid:
    :return: json contained job states changes history
    """
    valid, response = initRequest(request)
    if not valid:
        return response

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
        sl['modiftime_str'] = sl[mtimeparam].strftime(settings.DATETIME_FORMAT) if sl[mtimeparam] is not None else "---"

    if is_json_request(request):
        response = JsonResponse(statusLog, safe=False)
    else:
        response = render_to_response('jobStatusLog.html', {'statusLog': statusLog}, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@never_cache
def getTaskStatusLog(request, jeditaskid=None):
    """
    A view to asynchronously load task states changes history
    :param request:
    :param jeditaskid:
    :return: json contained task states changes history
    """
    valid, response = initRequest(request)
    if not valid: return response

    try:
        jeditaskid = int(jeditaskid)
    except:
        HttpResponse(status=404, content_type='text/html')

    mtimeparam = 'modificationtime'
    squery = {}
    squery['jeditaskid'] = jeditaskid
    statusLog = []
    statusLog.extend(TasksStatusLog.objects.filter(**squery).order_by(mtimeparam).values())

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
        sl['modiftime_str'] = sl[mtimeparam].strftime(settings.DATETIME_FORMAT) if sl[mtimeparam] is not None else "---"
    if is_json_request(request):
        response = HttpResponse(json.dumps(statusLog, cls=DateEncoder), content_type='application/json')
    else:
        response = render_to_response('taskStatusLog.html', {'statusLog': statusLog}, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@never_cache
def getTaskLogs(request, jeditaskid=None):
    """
    A view to asynchronously load task logs from ElasticSearch storage
    :param request:
    :param jeditaskid:
    :return: json
    """
    valid, response = initRequest(request)

    if not valid: return response

    try:
        jeditaskid = int(jeditaskid)
    except:
        HttpResponse(status=404, content_type='text/html')

    tasklogs = get_logs_by_taskid(jeditaskid)

    response = HttpResponse(json.dumps(tasklogs, cls=DateEncoder), content_type='application/json')

    # if is_json_request(request):
    #     response = HttpResponse(json.dumps(tasklogs, cls=DateEncoder), content_type='application/json')
    # else:
    #     HttpResponse(status=404, content_type='text/html')

    return response


def getSites(request):
    """
    List of sites for auto-complete options in the search by site in top bar
    :param request:
    :return:
    """
    if request.is_ajax():
        try:
            q = request.GET.get('term', '')
            pq_dict = get_panda_queues()
            results = []
            for pq_name in pq_dict:
                if q in pq_name.lower():
                    results.append(pq_name)
            data = json.dumps(results)
        except:
            data = 'fail'
    else:
        data = 'fail'
    mimetype = 'application/json'
    return HttpResponse(data, mimetype)


@never_cache
def get_hc_tests(request):
    """
    API for getting list of HammerCloud Tests
    :param request:
    :return: JSON response
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    jobs = []
    tests = []
    panda_queues = []

    pilot_timings_names = ['timegetjob', 'timestagein', 'timepayload', 'timestageout', 'timetotal_setup']
    error_fields = [
        'brokerageerrorcode', 'brokerageerrordiag',
        'ddmerrorcode', 'ddmerrordiag',
        'exeerrorcode', 'exeerrordiag',
        'jobdispatchererrorcode', 'jobdispatchererrordiag',
        'piloterrorcode', 'piloterrordiag',
        'superrorcode', 'superrordiag',
        'taskbuffererrorcode', 'taskbuffererrordiag',
        'transexitcode',
    ]
    fields = [
        'pandaid',
        'produsername',
        'prodsourcelabel',
        'processingtype',
        'transformation',
        'atlasrelease',
        'proddblock',
        'destinationdblock',
        'destinationse',
        'homepackage',
        'inputfileproject',
        'inputfiletype',
        'jobname',
        'cloud',
        'nucleus',
        'computingsite',
        'computingelement',
        'gshare',
        'schedulerid',
        'pilotid',
        'jobstatus',
        'creationtime',
        'starttime',
        'endtime',
        'statechangetime',
        'modificationtime',
        'actualcorecount',
        'minramcount',
        'maxvmem',
        'maxpss',
        'maxrss',
        'cpuconsumptiontime',
        'nevents',
        'hs06sec',
        'noutputdatafiles',
        'resourcetype',
        'eventservice',
        'transformation',
        'modificationhost',
        'batchid'
        ]

    jvalues = ['pilottiming',]
    jvalues.extend(fields)
    jvalues.extend(error_fields)

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    query['produsername'] = 'gangarbt'
    query['cloud'] = 'RU'
    excluded_time_query = copy.deepcopy(query)

    if 'modificationtime__castdate__range' in excluded_time_query:
        del excluded_time_query['modificationtime__castdate__range']

    # we change time param from modificationtime to :
    timeparamname = 'statechangetime'
    if 'modificationtime__castdate__range' in query:
        query[timeparamname + '__castdate__range'] = query['modificationtime__castdate__range']
        del query['modificationtime__castdate__range']

    is_archive_only = False
    is_archive = False
    timerange = [parse_datetime(mt) for mt in query[timeparamname + '__castdate__range']]
    if timerange[0] < datetime.utcnow()-timedelta(days=4) and timerange[1] < datetime.utcnow()-timedelta(days=4):
        is_archive_only = True
    if timerange[0] < datetime.utcnow() - timedelta(days=3):
        is_archive = True

    if not is_archive_only:
        jobs.extend(Jobsdefined4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
        jobs.extend(Jobsactive4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
        jobs.extend(Jobswaiting4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
        jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension]).values(*jvalues))
    if is_archive_only or is_archive:
        jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension]).values(*jvalues))
    _logger.info('Got jobs: {}'.format(time.time() - request.session['req_init_time']))

    panda_queues_info = get_panda_queues()
    _logger.info('Got PQ info: {}'.format(time.time() - request.session['req_init_time']))

    # getting input file info for jobs
    try:
        jobs = get_file_info(jobs, type='input', is_archive=is_archive)
    except:
        _logger.warning('Failed to get info of input files')
    _logger.info('Got input file info for jobs: {}'.format(time.time() - request.session['req_init_time']))

    errorCodes = get_job_error_desc()

    for job in jobs:
        test = {}
        test['errorinfo'] = errorInfo(job, errorCodes=errorCodes)
        try:
            hctestid = job['destinationdblock'].split('.')[2][2:]
        except:
            hctestid = None
        test['hctestid'] = hctestid
        try:
            pilot_timings = [int(pti) for pti in job['pilottiming'].split('|')]
        except:
            pilot_timings = [0] * 5

        test.update(dict(zip(pilot_timings_names, pilot_timings)))

        test['inputfilename'] = job['inputfilename'] if 'inputfilename' in job else None
        test['inputfilesizemb'] = round(job['inputfilesize'] / 1000000., 2) if 'inputfilesize' in job and isinstance(job['inputfilesize'], int) else None

        wallclocktime = get_job_walltime(job)
        queuetime = get_job_queuetime(job)

        if wallclocktime is not None:
            test['wallclocktime'] = wallclocktime
            if wallclocktime > 0:
                test['cpuefficiency'] = round(job['cpuconsumptiontime']/test['wallclocktime'], 3)
            else:
                test['cpuefficiency'] = 0
        else:
            test['wallclocktime'] = 0
            test['cpuefficiency'] = 0

        if queuetime is not None:
            test['queuetime'] = queuetime
        else:
            test['queuetime'] = 0

        for f in fields:
            test[f] = job[f]

        if 'computingsite' in job and job['computingsite'] in panda_queues_info:
            for f in ('siteid', 'gocname', 'status', 'cloud', 'tier', 'corepower'):
                if f in panda_queues_info[job['computingsite']]:
                    if f == 'gocname':
                        test['site'] = panda_queues_info[job['computingsite']][f]
                    else:
                        test[f] = panda_queues_info[job['computingsite']][f]
        tests.append(test)

    data = {'tests': tests}
    response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    return response


@csrf_exempt
def getPayloadLog(request):
    """
    A view to asynchronously load pilot logs from ElasticSearch storage by pandaid or taskid
    :param request:
    :param id:
    :return: json
    """
    valid, response = initRequest(request)

    connection = create_esatlas_connection()

    if not valid: return response

    mode = 'pandaid'

    log_content = {}
    if request.POST and "pandaid" in request.POST:
        try:
            id = int(request.POST['pandaid'])
            start_var = int(request.POST['start'])
            length_var = int(request.POST['length'])
            draw_var = int(request.POST['draw'])
            sort = request.POST['order[0][dir]']
            search_string = request.POST['search[value]']
        except:
            HttpResponse(status=404, content_type='text/html')
    else:
        HttpResponse(status=404, content_type='text/html')

    payloadlog, job_running_flag, total = get_payloadlog(id, connection, start=start_var, length=length_var, mode=mode,
                                                         sort=sort, search_string=search_string)

    log_content['payloadlog'] = payloadlog
    log_content['flag'] = job_running_flag
    log_content['recordsTotal'] = total
    log_content['recordsFiltered'] = total
    log_content['draw'] = draw_var

    response = HttpResponse(json.dumps(log_content, cls=DateEncoder), content_type='application/json')

    return response





