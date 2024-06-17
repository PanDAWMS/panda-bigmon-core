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
import pandas as pd
import math
import base64
import urllib3
import hashlib

from datetime import datetime, timedelta
from threading import Thread, Lock
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, unquote_plus
from opensearchpy import Search

from django.http import HttpResponse, JsonResponse, UnreadablePostError
from django.shortcuts import render, redirect
from django.template import RequestContext
from django.db.models import Count, Sum, F, Value, FloatField, Q, DateTimeField
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.cache import never_cache
from django.utils import timezone
from django.utils.cache import patch_response_headers
from django.db import connection
from django.template.loaders.app_directories import get_app_template_dirs
from django.template.defaulttags import register
from django.template.context_processors import csrf

import core.constants as const
from core.common.utils import getPrefix, getContextVariables
from core.pandajob.SQLLookups import CastDate
from core.pandajob.models import Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived, PandaJob
from core.schedresource.models import SchedconfigJson
from core.common.models import Filestable4
from core.common.models import Datasets
from core.common.models import FilestableArch
from core.common.models import Users
from core.common.models import Jobparamstable
from core.common.models import JobsStatuslog
from core.common.models import Logstable
from core.common.models import Jobsdebug
from core.common.models import JediJobRetryHistory
from core.common.models import JediTasks
from core.common.models import TasksStatusLog
from core.common.models import JediEvents
from core.common.models import JediDatasets
from core.common.models import JediDatasetContents
from core.common.models import JediWorkQueue
from core.oauth.models import BPUser
from core.compare.modelsCompare import ObjectsComparison
from core.filebrowser.ruciowrapper import ruciowrapper
from core.filebrowser.utils import get_log_provider

from django.conf import settings

from core.libs.TaskProgressPlot import TaskProgressPlot
from core.libs.UserProfilePlot import UserProfilePlot
from core.libs.TasksErrorCodesAnalyser import TasksErrorCodesAnalyser

from core.oauth.utils import login_customrequired, get_auth_provider, is_expert

from core.utils import is_json_request, extensibleURL, complete_request, is_wildcards, removeParam, is_xss, error_response
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table, drop_job_retries
from core.libs.cache import getCacheEntry, setCacheEntry, set_cache_timeout, getCacheData
from core.libs.deft import get_task_chain, hashtags_for_tasklist, extend_view_deft, staging_info_for_tasklist, \
    get_prod_slice_by_taskid
from core.libs.exlib import insert_to_temp_table, get_tmp_table_name, create_temporary_table, dictfetchall, is_timestamp
from core.libs.exlib import convert_to_si_prefix, get_file_info, convert_bytes, convert_hs06, round_to_n_digits, \
    convert_grams
from core.libs.eventservice import event_summary_for_task, add_event_summary_to_tasklist
from core.libs.flowchart import buildGoogleFlowDiagram
from core.libs.task import input_summary_for_task, datasets_for_task, \
    get_task_params, humanize_task_params, get_job_metrics_summary_for_task, cleanTaskList, get_task_flow_data, \
    get_datasets_for_tasklist, get_task_name_by_taskid
from core.libs.task import get_dataset_locality, is_event_service_task, \
    get_task_timewindow, get_task_time_archive_flag, get_logs_by_taskid, task_summary_dict, tasks_not_updated
from core.libs.taskparams import analyse_task_submission_options
from core.libs.job import is_event_service, get_job_list, calc_jobs_metrics, add_job_category, \
    job_states_count_by_param, is_job_active, get_job_queuetime, get_job_walltime, job_state_count, \
    getSequentialRetries, getSequentialRetries_ES, getSequentialRetries_ESupstream, is_debug_mode, clean_job_list, \
    add_files_info_to_jobs
from core.libs.jobmetadata import addJobMetadata
from core.libs.error import errorInfo, getErrorDescription, get_job_error_desc
from core.libs.site import get_pq_metrics
from core.libs.bpuser import get_relevant_links, filterErrorData
from core.libs.user import prepare_user_dash_plots, get_panda_user_stats, humanize_metrics
from core.libs.elasticsearch import create_os_connection, get_payloadlog, get_split_rule_info
from core.libs.sqlcustom import escape_input, preprocess_wild_card_string
from core.libs.datetimestrings import datetime_handler, parse_datetime, stringify_datetime_fields
from core.libs.jobconsumers import reconstruct_job_consumers
from core.libs.DateEncoder import DateEncoder
from core.libs.DateTimeEncoder import DateTimeEncoder

from core.pandajob.summary_error import errorSummaryDict, get_error_message_summary
from core.pandajob.summary_task import task_summary, job_summary_for_task, job_summary_for_task_light, \
    get_job_state_summary_for_tasklist, get_top_memory_consumers
from core.pandajob.summary_site import site_summary_dict
from core.pandajob.summary_wn import wn_summary
from core.pandajob.summary_user import user_summary_dict
from core.pandajob.utils import job_summary_dict

from core.iDDS.algorithms import checkIfIddsTask
from core.iDDS.utils import add_idds_info_to_tasks
from core.dashboards.jobsummaryregion import get_job_summary_region, prepare_job_summary_region, prettify_json_output
from core.dashboards.jobsummarynucleus import get_job_summary_nucleus, prepare_job_summary_nucleus
from core.dashboards.eventservice import get_es_job_summary_region, prepare_es_job_summary_region
from core.schedresource.utils import get_pq_atlas_sites, get_panda_queues, get_basic_info_for_pqs, \
    get_panda_resource, get_pq_clouds, get_pq_object_store_path, filter_pq_json

tcount = {}
lock = Lock()
DateTimeField.register_lookup(CastDate)

try:
    full_hostname = subprocess.getoutput('hostname')
    if full_hostname.find('.') > 0:
        hostname = full_hostname[:full_hostname.find('.')]
    else:
        hostname = full_hostname
except:
    full_hostname = ''
    hostname = ''

cloudList = ['CA', 'CERN', 'DE', 'ES', 'FR', 'IT', 'ND', 'NL', 'RU', 'TW', 'UK', 'US']

statelist = ['pending', 'defined', 'waiting', 'assigned', 'throttled',
             'activated', 'sent', 'starting', 'running', 'holding',
             'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']
sitestatelist = ['defined', 'waiting', 'assigned', 'throttled', 'activated', 'sent', 'starting', 'running', 'holding',
                 'merging', 'transferring', 'finished', 'failed', 'cancelled', 'closed']
eventservicestatelist = ['ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed', 'fatal',
                         'merged', 'corrupted']
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
                   'jobsubstatus', 'nucleus', 'gshare', 'resourcetype']
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
def get_count(input_dict, key):
    return input_dict[key]['count']


@register.filter(takes_context=True)
def get_tk(input_dict, key):
    return input_dict[key]['tk']


@register.filter(takes_context=True)
def get_item(input_dict, key):
    return input_dict.get(key)


def initRequest(request, callselfmon=True):
    global VOMODE, ENV, hostname, full_hostname
    ENV = {}
    VOMODE = ''
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        VOMODE = 'atlas'

    request.session['meta'] = {
        'version': settings.VERSION,
    }

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
                    user = BPUser.objects.create_user(username=request.session['ADFS_LOGIN'],
                                                      email=request.session['ADFS_EMAIL'],
                                                      first_name=request.session['ADFS_FIRSTNAME'],
                                                      last_name=request.session['ADFS_LASTNAME'])
                    user.set_unusable_password()
                    user.save()

    request.session['viewParams'] = {}

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
        return False, error_response(request, message='Error appeared while encoding URL!', status=400)

    # if injection -> 400
    if is_xss(url):
        return False, error_response(request, message="Illegal request", status=400)

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
    request.session['urls_cut']['xurl'] = extensibleURL(request)
    request.session['urls_cut']['nolimiturl'] = removeParam(extensibleURL(request), 'limit', mode='extensible')
    request.session['urls_cut']['nodisplaylimiturl'] = removeParam(extensibleURL(request), 'display_limit', mode='extensible')
    request.session['urls_cut']['nosorturl'] = removeParam(extensibleURL(request), 'sortby', mode='extensible')

    if 'timerange' in request.session:
        del request.session['timerange']

    if len(hostname) > 0:
        request.session['hostname'] = hostname
        request.session['full_hostname'] = full_hostname

    # self monitor
    if callselfmon:
        initSelfMonitor(request)

    # Set default page lifetime in the http header, for the use of the front end cache
    set_cache_timeout(request)

    # Is it a https connection with a legit cert presented by the user?
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
        VOMODE = settings.DEPLOYMENT
        if '_' in settings.DEPLOYMENT:
            request.session['viewParams']['MON_VO'] = settings.DEPLOYMENT.split('_')[1]
        elif hasattr(settings, 'MON_VO'):
            request.session['viewParams']['MON_VO'] = settings.MON_VO
        else:
            request.session['viewParams']['MON_VO'] = ''

    # add CRIC URL base to session
    if settings.CRIC_API_URL:
        request.session['crichost'] = urlparse(settings.CRIC_API_URL).hostname
    if settings.RUCIO_UI_URL:
        request.session['rucio_ui'] = settings.RUCIO_UI_URL

    # add installed apps to session
    request.session['installed_apps'] = list(settings.INSTALLED_APPS)

    # remove xurls from session if it is kept from previous requests
    if 'xurls' in request.session:
        try:
            del request.session['xurls']
        except:
            pass

    requestParams = {}
    request.session['requestParams'] = requestParams

    allowedemptyparams = ('json', 'snap', 'dt', 'dialogs', 'pandaids', 'workersstats')
    if request.method == 'POST':
        # check of POST request complete
        try:
            len(request.POST)
        except UnreadablePostError:
            _logger.exception("Something wrong with POST request, returning 400")
            return False, JsonResponse({'error': 'Failed to read request body'}, status=400)
        except Exception as ex:
            _logger.exception(f"Exception thrown while trying get length of request body \n{ex}")
            return False, JsonResponse({'error': 'Failed to read request body'}, status=400)

        if len(request.POST) > 0:
            for p in request.POST:
                if p in ('csrfmiddlewaretoken',): continue
                pval = request.POST[p]
                pval = pval.replace('+', ' ')
                request.session['requestParams'][p.lower()] = pval
        else:
            try:
                post_params = json.loads(request.body)
            except Exception as ex:
                post_params = None
                _logger.exception(f"Failed to decode params in body of POST request:\n{ex}")
            if isinstance(post_params, dict):
                if 'params' in post_params and isinstance(post_params['params'], dict):
                    request.session['requestParams'].update(post_params['params'])
                else:
                    request.session['requestParams'].update(post_params)
    else:
        for p in request.GET:
            pval = request.GET[p]
            pval = pval.replace('+', ' ')
            pval = pval.replace("\'", '')
            if p.lower() != 'batchid':  # Special requester exception
                pval = pval.replace('#', '')

            # is it int, if it's supposed to be?
            if p.lower() in (
                    'days', 'hours', 'limit', 'display_limit', 'pandaid', 'taskid', 'jeditaskid', 'jobsetid', 'reqid',
                    'datasetid', 'fileid', 'corecount', 'taskpriority', 'priority', 'attemptnr', 'statenotupdated', 'corepower',
                    'wansourcelimit', 'wansinklimit', 'nqueue', 'nodes', 'queuehours', 'memory', 'maxtime', 'space',
                    'maxinputsize', 'timefloor', 'depthboost', 'pilotlimit', 'transferringlimit',
                    'cachedse', 'stageinretry', 'stageoutretry', 'maxwdir', 'minmemory', 'maxmemory', 'minrss',
                    'maxrss', 'mintime', 'nlastnightlies'):
                try:
                    if '|' in pval:
                        values = pval.split('|')
                        for value in values:
                            int(value)
                    elif ',' in pval:
                        values = pval.split(',')
                        for value in values:
                            int(value)
                    elif pval == 'Not specified':
                        pass  # allow 'Not specified' value for int parameters
                    else:
                        int(pval)
                except:
                    return False, error_response(request, message=f"Illegal value '{pval}' for {p}", status=400)
            if p.lower() in ('date_from', 'date_to'):
                try:
                    parse_datetime(pval)
                except:
                    return False, error_response(request, message=f"Illegal value '{pval}' for {p}, expected YYYY-MM-DD", status=400)
            if p.lower() not in allowedemptyparams and len(pval) == 0:
                return False, error_response(request, message=f"Empty value '{pval}' for {p}", status=400)
            if p.lower() in ('jobname', 'taskname',) and len(pval) > 0 and ('%' in pval or '%s' in pval):
                return False, error_response(request, message=f"Use * symbol for pattern search instead of % for {p}", status=400)
            request.session['requestParams'][p.lower()] = pval

    return True, None


def setupView(request, opmode='', hours=0, limit=-99, querytype='job', wildCardExt=False):
    """
    Transform HTTP request params into query params for ORM
    :param request:
    :param opmode: str: 'optional mode', e.g. notime to remove time limit in the query
    :param hours:
    :param limit:
    :param querytype: job|task
    :param wildCardExt: flag if process wildcards to str part of 'where' clause and return it
    :return:
    """
    viewParams = {}
    if not 'viewParams' in request.session:
        request.session['viewParams'] = viewParams

    extraQueryString = '(1=1) '
    extraQueryFields = []  # params that goes directly to the wildcards processing

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
            extraQueryString += " AND (jobmetrics not like '%%nGPU=%%')"
        if processor_type.lower() == 'gpu':
            extraQueryString += " AND (jobmetrics like '%%nGPU=%%')"

    if 'site' in request.session['requestParams'] and (
            request.session['requestParams']['site'] == 'hpc' or not is_wildcards(
        request.session['requestParams']['site'])):
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
        if 'cloud' not in fields:
            fields.append('cloud')
        if 'atlasrelease' not in fields:
            fields.append('atlasrelease')
        if 'produsername' in request.session['requestParams'] or 'jeditaskid' in request.session['requestParams'] or (
                'user' in request.session['requestParams']):
            if 'jobsetid' not in fields:
                fields.append('jobsetid')
            if ('hours' not in request.session['requestParams']) and (
                    'days' not in request.session['requestParams']) and (
                    'jobsetid' in request.session['requestParams'] or 'taskid' in request.session[
                'requestParams'] or 'jeditaskid' in request.session['requestParams']):
                # Cases where deep query is safe. Unless the time depth is specified in URL.
                if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
                    deepquery = True
        else:
            if 'jobsetid' in fields:
                fields.remove('jobsetid')
    else:
        fields.append('vo')

    if hours > 0:
        # Call param overrides default hours, but not a param on the URL
        LAST_N_HOURS_MAX = hours
    # For site-specific queries, allow longer time window
    if 'batchid' in request.session['requestParams'] and (hours is None or hours == 0):
        LAST_N_HOURS_MAX = 12
    if 'computingsite' in request.session['requestParams'] and hours is None:
        LAST_N_HOURS_MAX = 12
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        LAST_N_HOURS_MAX = 2 * 24
    # hours specified in the URL takes priority over the above
    if 'hours' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['hours'])
    if 'days' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['days']) * 24
    # Exempt single-job, single-task etc queries from time constraint
    if 'hours' not in request.session['requestParams'] and 'days' not in request.session['requestParams']:
        if ('jeditaskid' in request.session['requestParams'] or
                'taskid' in request.session['requestParams'] or
                'pandaid' in request.session['requestParams'] or
                'jobname' in request.session['requestParams'] or
                'batchid' in request.session['requestParams'] or (
                        querytype == 'user'
                        and 'extra' in request.session['requestParams']
                        and 'notimelimit' in request.session['requestParams']['extra'])):
            deepquery = True
    if deepquery:
        opmode = 'notime'
        hours = LAST_N_HOURS_MAX = 24 * 180
        request.session['JOB_LIMIT'] = 999999
    if opmode != 'notime':
        if (LAST_N_HOURS_MAX <= 72 and
                not ('date_from' in request.session['requestParams'] or
                     'date_to' in request.session['requestParams'] or
                     'earlierthan' in request.session['requestParams'] or
                     'earlierthandays' in request.session['requestParams'])):
            request.session['viewParams']['selection'] = ", last %s hours" % LAST_N_HOURS_MAX
        else:
            request.session['viewParams']['selection'] = ", last %d days" % (float(LAST_N_HOURS_MAX) / 24.)
        if querytype == 'job' and 100000 > request.session['JOB_LIMIT'] > 0:
            request.session['viewParams']['selection'] += " <b>limit=</b>%s" % request.session['JOB_LIMIT']
    else:
        request.session['viewParams']['selection'] = ". <b>Params:</b> "
    for param in request.session['requestParams']:
        if request.session['requestParams'][param] == 'None':
            continue
        if request.session['requestParams'][param] == '':
            continue
        if param == 'display_limit':
            continue
        if param == 'sortby':
            continue
        if param == 'timestamp':
            continue
        if param == 'limit' and request.session['JOB_LIMIT'] > 0:
            continue
        request.session['viewParams']['selection'] += " <b>%s=</b>%s " % (
            param, request.session['requestParams'][param])

    startdate = None
    if 'date_from' in request.session['requestParams']:
        startdate = parse_datetime(request.session['requestParams']['date_from'])
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)

    enddate = None
    endtime__castdate__range = None
    if 'endtimerange' in request.session['requestParams']:
        endtimerange = request.session['requestParams']['endtimerange'].split('|')
        endtime__castdate__range = [
            parse_datetime(endtimerange[0]).strftime(settings.DATETIME_FORMAT),
            parse_datetime(endtimerange[1]).strftime(settings.DATETIME_FORMAT)
        ]

    if 'date_to' in request.session['requestParams']:
        enddate = parse_datetime(request.session['requestParams']['date_to'])
    if 'earlierthan' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthan']))
    if 'earlierthandays' in request.session['requestParams']:
        enddate = timezone.now() - timedelta(hours=float(request.session['requestParams']['earlierthandays']) * 24)

    if enddate is None:
        enddate = timezone.now()
        request.session['noenddate'] = True
    else:
        request.session['noenddate'] = False

    if not endtime__castdate__range:
        query = {
            'modificationtime__castdate__range': [
                startdate.strftime(settings.DATETIME_FORMAT),
                enddate.strftime(settings.DATETIME_FORMAT)]
        }
    else:
        query = {
            'endtime__castdate__range': [
                endtime__castdate__range[0],
                endtime__castdate__range[1]]
        }

    # add min/max values to session
    request.session['TFIRST'] = startdate
    request.session['TLAST'] = enddate

    request.session['PLOW'] = 1000000
    request.session['PHIGH'] = -1000000

    # Add any extensions to the query determined from the URL
    for param in request.session['requestParams']:
        if param in ('hours', 'days'):
            continue
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
                query['schedulerid'] = 'harvester-' + val
        elif param == 'schedulerid':
            if 'harvester-*' in request.session['requestParams'][param]:
                query['schedulerid__startswith'] = 'harvester'
            else:
                val = request.session['requestParams'][param]
                query['schedulerid__startswith'] = val
        elif param == 'priorityrange':
            mat = re.match('(-?[0-9]+):(-?[0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['currentpriority__gte'] = plo
                query['currentpriority__lte'] = phi
        elif param == 'jobsetrange':
            mat = re.match('([0-9]+):([0-9]+)', request.session['requestParams'][param])
            if mat:
                plo = int(mat.group(1))
                phi = int(mat.group(2))
                query['jobsetid__gte'] = plo
                query['jobsetid__lte'] = phi
        elif param == 'user' or param == 'username' or param == 'produsername' and not \
                is_wildcards(request.session['requestParams'][param]):
            if querytype == 'job':
                query['produsername__icontains'] = request.session['requestParams'][param].strip()
        elif param in ('project',) and querytype == 'task':
            val = request.session['requestParams'][param]
            query['taskname__istartswith'] = val
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
        elif param == 'processingtype' and '|' not in request.session['requestParams'][param] and '*' not in \
                request.session['requestParams'][param] and '!' not in request.session['requestParams'][param]:
            val = request.session['requestParams'][param]
            query['processingtype'] = val
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
            and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 > {} 
            and (CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - starttime) * 24 * 60 < {} ) 
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
            request.session['xurls']['container_name'] = removeParam(
                extensibleURL(request),
                'container_name',
                mode='extensible'
            )
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
                        if '|' in val or ',' in val:
                            values = val.split('|') if '|' in val else val.split(',')
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
                        extraQueryString += " AND ( {0} is NULL or {0} = '' ) ".format(field.db_column)
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
                            and (('mode' in request.session['requestParams'] and request.session['requestParams'][
                        'mode'] == 'eventservice') or (
                                         'jobtype' in request.session['requestParams'] and
                                         request.session['requestParams'][
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
                            paramsstr = paramsstr.replace('jumbo', '4')
                            paramsstr = paramsstr.replace('cojumbo', '5')
                            paramsstr = paramsstr.replace('finegrained', '6')
                            paramvalues = paramsstr.split('|')
                            try:
                                paramvalues = [int(p) for p in paramvalues]
                            except:
                                paramvalues = []
                            query['eventservice__in'] = paramvalues
                        else:
                            param_val = request.session['requestParams'][param]
                            if param_val == 'esmerge' or param_val == '2':
                                query['eventservice'] = 2
                            elif param_val == 'clone' or param_val == '3':
                                query['eventservice'] = 3
                            elif param_val == 'jumbo' or param_val == '4':
                                query['eventservice'] = 4
                            elif param_val == 'cojumbo' or param_val == '5':
                                query['eventservice'] = 5
                            elif param_val == 'finegrained' or param_val == '6':
                                query['eventservice'] = 6
                            elif param_val == 'eventservice' or param_val == '1':
                                query['eventservice'] = 1
                                extraQueryString += " AND not specialhandling like \'%%sc:%%\' "
                            elif param_val == 'not2':
                                extraQueryString += ' AND (eventservice != 2) '
                            elif param_val == 'all':
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
                        if param not in wildSearchFields:
                            query[param] = request.session['requestParams'][param]

    # process queue related params if any
    if querytype == 'job':
        if 'region' in request.session['requestParams']:
            request.session['requestParams']['queuecloud'] = request.session['requestParams']['region']
        if 'site' in request.session['requestParams']:
            request.session['requestParams']['queueatlas_site'] = request.session['requestParams']['site']
        # check if queue params are provided and computingsite not explicitly specified
        if any(key.startswith('queue') for key, value in request.session['requestParams'].items()) and (
            'computingsite' not in request.session['requestParams']
        ):
            pqs_dict = filter_pq_json(request)
            if len(pqs_dict) > 0:
                if 'computingsite__in' in query:
                    # unite lists
                    query['computingsite__in'] = list(set(query['computingsite__in']) & set([k for k in pqs_dict]))
                else:
                    query['computingsite__in'] = list(pqs_dict.keys())

    if opmode in ['analysis', 'production'] and querytype == 'job':
        if opmode.startswith('analy'):
            query['prodsourcelabel__in'] = ['panda', 'user']
        elif opmode.startswith('prod'):
            query['prodsourcelabel__in'] = ['managed']

    if not wildCardExt:
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
                if not any(
                        currenfField in key for key, value in query.items()) and currenfField not in extraQueryFields:
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

    if not is_json_request(request):
        del request.session['TFIRST']
        del request.session['TLAST']
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render(request, 'core-mainPage.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    else:
        response = JsonResponse({})
    request = complete_request(request)
    return response


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
    help_template_list.insert(0, help_template_list.pop(
        min([i for i, d in enumerate(help_template_list) if d['key'].lower() == 'introduction'])))

    if not is_json_request(request):
        data = {
            'prefix': getPrefix(request),
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'templates': help_template_list,
        }
        response = render(request, 'help.html', data, content_type='text/html')
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
        return error_response(request, message='not supported', status=204)


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
        if 'istestmonitor' in request.session['requestParams'] and (
                request.session['requestParams']['istestmonitor'] == 'yes'):
            return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        data['request'] = request
        if data['eventservice']:
            response = render(request, 'jobListES.html', data, content_type='text/html')
        else:
            response = render(request, 'jobList.html', data, content_type='text/html')
        _logger.info(
            'Rendered template with data from cache: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'dump' in request.session['requestParams'] and request.session['requestParams']['dump'] == 'parameters':
        return jobParamList(request)

    is_job_meta_required = False
    if 'fields' in request.session['requestParams'] and request.session['requestParams']['fields'] and 'metastruct' in \
            request.session['requestParams']['fields']:
        is_job_meta_required = True

    eventservice = False
    if 'jobtype' in request.session['requestParams'] and request.session['requestParams']['jobtype'] == 'eventservice':
        eventservice = True
    if 'eventservice' in request.session['requestParams'] and (
            request.session['requestParams']['eventservice'] == 'eventservice' or
            request.session['requestParams']['eventservice'] == '1' or
            request.session['requestParams']['eventservice'] == '4' or
            request.session['requestParams']['eventservice'] == 'jumbo'):
        eventservice = True
    elif 'eventservice' in request.session['requestParams'] and (
            '1' in request.session['requestParams']['eventservice'] or
            '2' in request.session['requestParams']['eventservice'] or
            '4' in request.session['requestParams']['eventservice'] or
            '5' in request.session['requestParams']['eventservice']):
        eventservice = True

    if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
        try:
            jeditaskid = int(request.session['requestParams']['jeditaskid'])
        except:
            jeditaskid = None
        if jeditaskid:
            eventservice = is_event_service_task(jeditaskid)
    else:
        jeditaskid = None

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
                (select pandaid from {}.filestable4 
                    where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                union all
                (select pandaid from {}.filestable_arch 
                    where jeditaskid = {} and datasetid in ( {} ) and fileid = {} )
                ) """.format(settings.DB_SCHEMA_PANDA, jeditaskid, datasetid, fileid,
                             settings.DB_SCHEMA_PANDA_ARCH, jeditaskid, datasetid, fileid)

        if 'ecstate' in request.session['requestParams'] and tk and datasetid:
            extraquery_files += """
                pandaid in (
                    (select pandaid from {}.filestable4 where jeditaskid = {} and datasetid in ( {} ) 
                        and fileid in (select id from {}.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    union all 
                    (select pandaid from {}.filestable_arch where jeditaskid = {} and datasetid in ( {} ) 
                        and fileid in (select id from {}.TMP_IDS1DEBUG where TRANSACTIONKEY={}) )
                    ) """.format(settings.DB_SCHEMA_PANDA, jeditaskid, datasetid, settings.DB_SCHEMA, tk,
                                 settings.DB_SCHEMA_PANDA_ARCH, jeditaskid, settings.DB_SCHEMA, datasetid, tk)
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
                (select pandaid from {}.filestable4 where jeditaskid = {} and datasetid = {} )
                union all
                (select pandaid from {}.filestable_arch where jeditaskid = {} and datasetid = {})
                ) """.format(settings.DB_SCHEMA_PANDA, jeditaskid, datasetid,
                             settings.DB_SCHEMA_PANDA_ARCH, jeditaskid, datasetid)
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
            select jeditaskid from {}.jedi_tasks where taskname like '{}' and username like '{}'
            ) """.format(settings.DB_SCHEMA_PANDA, taskname, taskusername)

    _logger.debug('Specific params processing: {}'.format(time.time() - request.session['req_init_time']))

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)

    _logger.debug('Setup view: {}'.format(time.time() - request.session['req_init_time']))

    if len(extraquery_files) > 1:
        wildCardExtension += ' AND ' + extraquery_files

    if len(extraquery_tasks) > 1:
        wildCardExtension += ' AND ' + extraquery_tasks

    if query == 'reqtoken' and wildCardExtension is None and LAST_N_HOURS_MAX is None:
        return error_response(request, message='Request token is not found or data is outdated. Please reload the original page.', status=204)

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
            'statechangetime', 'nevents', 'jobmetrics', 'noutputdatafiles', 'outputfiletype', 'parentid',
            'actualcorecount', 'schedulerid', 'pilotid', 'commandtopilot', 'cmtconfig', 'maxpss']
        if not eventservice:
            values.extend(['avgvmem', 'maxvmem', 'maxrss'])

        if settings.DEPLOYMENT != "POSTGRES":
            values.append('nucleus')
            values.append('eventservice')
            values.append('gshare')
            values.append('resourcetype')
            values.append('container_name')

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
        jobs = getHarvesterJobs(
            request,
            instance=request.session['requestParams']['harvesterinstance'],
            workerid=request.session['requestParams']['workerid'],
            jobstatus=harvesterjobstatus,
            fields=values)
    elif 'harvesterid' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
        jobs = getHarvesterJobs(
            request,
            instance=request.session['requestParams']['harvesterid'],
            workerid=request.session['requestParams']['workerid'],
            jobstatus=harvesterjobstatus,
            fields=values)
    elif 'harvesterinstance' not in request.session['requestParams'] and (
            'harvesterid' not in request.session['requestParams']) and (
            'workerid' in request.session['requestParams']):
        jobs = getHarvesterJobs(
            request,
            workerid=request.session['requestParams']['workerid'],
            jobstatus=harvesterjobstatus,
            fields=values)
    elif 'harvesterce' in request.session['requestParams']:
        jobs = getCeHarvesterJobs(request, computingelement=request.session['requestParams']['harvesterce'])
    else:
        # apply order by to get recent jobs
        order_by = '-modificationtime'
        # exclude time from query for DB tables with active jobs
        etquery = copy.deepcopy(query)
        if ('modificationtime__castdate__range' in etquery and (
                len({'date_to', 'hours'}.intersection(request.session['requestParams'].keys())) == 0)) or (
                'jobstatus' in request.session['requestParams'] and (
                is_job_active(request.session['requestParams']['jobstatus']))):
            del etquery['modificationtime__castdate__range']
            warning['notimelimit'] = "no time window limiting was applied for active jobs in this selection"

        jobs.extend(Jobsdefined4.objects.filter(**etquery).extra(where=[wildCardExtension]).order_by(order_by)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsactive4.objects.filter(**etquery).extra(where=[wildCardExtension]).order_by(order_by)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobswaiting4.objects.filter(**etquery).extra(where=[wildCardExtension]).order_by(order_by)[:JOB_LIMIT].values(*values))
        jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension]).order_by(order_by)[:JOB_LIMIT].values(*values))
        _logger.info('Got jobs: {}'.format(time.time() - request.session['req_init_time']))
        listJobs = [Jobsarchived4, Jobsactive4, Jobswaiting4, Jobsdefined4]

        if not noarchjobs:
            queryFrozenStates = []
            if 'jobstatus' in request.session['requestParams']:
                queryFrozenStates = list(
                    set(request.session['requestParams']['jobstatus'].split('|')).intersection(job_final_states))
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
                        (datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0],
                                                            settings.DATETIME_FORMAT)).days > 2 or
                        (datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
                                                            settings.DATETIME_FORMAT)).days > 2):
                    # add jobsarchived model to calculation of total jobs count in a separate thread
                    listJobs.append(Jobsarchived)
                    # remove timewindow if all jobs for a task or full list is requested
                    if 'jeditaskid' in request.session['requestParams'] or (is_json_request(request) and (
                            'fulllist' in request.session['requestParams'] and request.session['requestParams']['fulllist'] == 'true')):
                        del query['modificationtime__castdate__range']
                    # jobsarchived table has index by statechangetime, use it instead of modificationtime
                    if 'modificationtime__castdate__range' in query:
                        query['statechangetime__castdate__range'] = query['modificationtime__castdate__range']
                        del query['modificationtime__castdate__range']
                    # order by  statechangetime to get recent jobs as it is an index
                    order_by = '-statechangetime'
                    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension]).order_by(order_by)[:JOB_LIMIT].values(*values))
                    _logger.info('Got archived jobs: {}'.format(time.time() - request.session['req_init_time']))
        if not is_json_request(request):
            thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension, dkey))
            thread.start()
        else:
            thread = None

    # If the list is for a particular JEDI task, filter out the jobs superseded by retries
    # if ES -> nodrop by default
    dropmode = False
    if jeditaskid or (
            'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'drop'):
        dropmode = True
    if eventservice or (
            'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'nodrop'):
        dropmode = False

    isReturnDroppedPMerge = False
    if 'processingtype' in request.session['requestParams'] and request.session['requestParams'][
        'processingtype'] == 'pmerge':
        isReturnDroppedPMerge = True

    droplist = []
    droppedPmerge = set()
    cntStatus = []
    if dropmode and jeditaskid:
        jobs, droplist, droppedPmerge = drop_job_retries(jobs, jeditaskid, is_return_dropped_jobs=isReturnDroppedPMerge)
        _logger.debug('Done droppping if was requested: {}'.format(time.time() - request.session['req_init_time']))

    # get attempts of file if fileid in request params
    files_attempts_dict = {}
    files_attempts = []
    if fileid:
        if fileid and jeditaskid and datasetid:
            fquery = {'pandaid__in': [job['pandaid'] for job in jobs if len(jobs) > 0], 'fileid': fileid}
            files_attempts.extend(Filestable4.objects.filter(**fquery).values('pandaid', 'attemptnr'))
            files_attempts.extend(FilestableArch.objects.filter(**fquery).values('pandaid', 'attemptnr'))
            if len(files_attempts) > 0:
                files_attempts_dict = dict(
                    zip([f['pandaid'] for f in files_attempts], [ff['attemptnr'] for ff in files_attempts]))

            jfquery = {'jeditaskid': jeditaskid, 'datasetid': datasetid, 'fileid': fileid}
            jedi_file = JediDatasetContents.objects.filter(**jfquery).values('attemptnr', 'maxattempt', 'failedattempt',
                                                                             'maxfailure')
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
        display_limit = 100
        url_nolimit = request.get_full_path()
    njobsmax = display_limit

    sortby = 'time-descending'
    sortby_reverse = True
    sortby_key = 'modificationtime'
    if fileid:
        sortby = "fileattemptnr-descending"
    if 'computingsite' in request.session['requestParams']:
        sortby = 'time-descending'
    if 'jeditaskid' in request.session['requestParams']:
        sortby = "attemptnr-descending"
    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']

    if sortby:
        if sortby.endswith('-descending'):
            sortby_reverse = True
        elif sortby.endswith('-ascending'):
            sortby_reverse = False

        if sortby.startswith('create'):
            sortby_key = 'creationtime'
        elif sortby.startswith('time'):
            sortby_key = 'modificationtime'
        elif sortby.startswith('statetime'):
            sortby_key = 'statechangetime'
        elif sortby.startswith('priority'):
            sortby_key = 'currentpriority'
        elif sortby.startswith('duration'):
            sortby_key = 'durationsec'
        elif sortby.startswith('attemptnr'):
            sortby_key = 'attemptnr'
        elif sortby.startswith('PandaID'):
            sortby_key = 'pandaid'

        if 'time' in sortby_key:
            # use default date for sorting if it is none
            jobs = sorted(jobs, key=lambda x: x[sortby_key] if not None else datetime(1900, 1, 1), reverse=sortby_reverse)
        else:
            jobs = sorted(jobs, key=lambda x: x[sortby_key], reverse=sortby_reverse)
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

    sumd, esjobdict = job_summary_dict(
        request,
        jobs,
        const.JOB_FIELDS_ATTR_SUMMARY + (
            'corecount', 'noutputdatafiles', 'actualcorecount', 'schedulerid', 'pilotversion', 'computingelement',
            'container_name', 'nevents', 'processor_type'
        ))
    # Sort in order to see the most important tasks
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
    if 'prodsourcelabel' in request.session['requestParams'] and (
            request.session['requestParams']['prodsourcelabel'].lower().find('test') >= 0):
        testjobs = True

    errsByCount, _, _, _, errdSumd, _ = errorSummaryDict(request, jobs, testjobs, output=['errsByCount', 'errdSumd'])
    _logger.debug('Built error summary: {}'.format(time.time() - request.session['req_init_time']))
    errsByMessage = get_error_message_summary(jobs)
    _logger.debug('Built error message summary: {}'.format(time.time() - request.session['req_init_time']))

    if not is_json_request(request):
        # Here we're getting extended data for list of jobs to be shown
        jobsToShow = jobs[:njobsmax]
        jobsToShow = add_files_info_to_jobs(jobsToShow)
        _logger.debug(
            'Got file info for list of jobs to be shown: {}'.format(time.time() - request.session['req_init_time']))

        # Getting PQ status for list of jobs to be shown
        pq_dict = get_panda_queues()
        for job in jobsToShow:
            if job['computingsite'] in pq_dict:
                if 'status' in pq_dict[job['computingsite']]:
                    job['computingsitestatus'] = pq_dict[job['computingsite']]['status']
                else:
                    job['computingsitestatus'] = 'UNKNOWN'
                if 'comment' in pq_dict[job['computingsite']]:
                    job['computingsitecomment'] = pq_dict[job['computingsite']]['comment']
                else:
                    job['computingsitecomment'] = 'UNKNOWN'

        _logger.debug('Got extra params for sites: {}'.format(time.time() - request.session['req_init_time']))

        # checking if log file replica available
        if settings.LOGS_PROVIDER == 'rucio' and (
                'extra' in request.session['requestParams'] and 'checklogs' in request.session['requestParams']['extra']):
            dids = [j['log_did'] for j in jobsToShow if 'log_did' in j]
            replicas = None
            try:
                from core.filebrowser.ruciowrapper import ruciowrapper
                rucio_client = ruciowrapper()
                replicas = rucio_client.list_file_replicas(dids)
            except:
                _logger.warning('Can not check log existence')
            if replicas is not None:
                for j in jobs:
                    if 'log_did' in j and 'name' in j['log_did']:
                        if j['log_did']['name'] in replicas:
                            j['is_log_available'] = 1
                        else:
                            j['is_log_available'] = -1
                    else:
                        j['is_log_available'] = 0
            _logger.debug('Checked logs existence via Rucio: {}'.format(time.time() - request.session['req_init_time']))

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
        # xurl = removeParam(nosorturl, 'mode', mode='extensible')
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
            'pandaids': [j['pandaid'] for j in jobsToShow if 'pandaid' in j],
            'warning': warning,
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobList", json.dumps(data, cls=DateEncoder), 60 * 20)

        _logger.debug('Cache was set: {}'.format(time.time() - request.session['req_init_time']))

        if eventservice:
            response = render(request, 'jobListES.html', data, content_type='text/html')
        else:
            response = render(request, 'jobList.html', data, content_type='text/html')

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
                        if f['type'] == 'input':
                            ninput += 1
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
            _logger.info(
                'Got dataset and file info if requested: {}'.format(time.time() - request.session['req_init_time']))

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

        # add outputs to data to return
        data = {}
        if 'outputs' in request.session['requestParams'] and len(jobs) > 0:
            outputs = request.session['requestParams']['outputs'].split(',')
        else:
            # return everything
            outputs = ['selectionsummary', 'jobs', 'errsByCount']

        data['selectionsummary'] = sumd if 'selectionsummary' in outputs else []
        data['jobs'] = jobs if 'jobs' in outputs else []
        data['errsByCount'] = errsByCount if 'errsByCount' in outputs else []

        # cache json response for particular usage (HC test monitor for RU)
        if 'istestmonitor' in request.session['requestParams'] and request.session['requestParams'][
            'istestmonitor'] == 'yes':
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

    errors = {job['pandaid']: job['errorinfo'] for job in jobs if job['jobstatus'] == 'failed'}

    response = render(request, 'jobDescentErrors.html', {'errors': errors}, content_type='text/html')
    request = complete_request(request)
    return response


@login_customrequired
@csrf_exempt
def jobInfo(request, pandaid=None, batchid=None):
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
            response = render(request, 'jobInfoES.html', data, content_type='text/html')
        else:
            response = render(request, 'jobInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    # Get the current AUTH type
    auth = get_auth_provider(request)

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
        response = render(request, 'jobInfo.html', data, content_type='text/html')
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
                    dn = dn[:-(len(CNs[-1]) + 4)]
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
                    if 'jobmetrics' in jobs[0] and jobs[0]['jobmetrics'] is not None and len(jobs[0]['jobmetrics']) > 0:
                        for s in jobs[0]['jobmetrics'].split(' '):
                            if 'logBucketID' in s:
                                logBucketID = int(s.split('=')[1])
                                if logBucketID in [45, 41, 105, 106, 42, 61, 103, 2, 82, 101, 117,
                                                   115]:  # Bucket Codes for S3 destination
                                    f['destination'] = 'S3'
                if f['type'] == 'pseudo_input': npseudo_input += 1
                f['fsizemb'] = round(convert_bytes(f['fsize'], output_unit='MB'), 2)

                if f['datasetid'] in datasets_dict:
                    f['datasetname'] = datasets_dict[f['datasetid']]
                    if f['scope'] and f['scope'] + ":" in f['datasetname']:
                        f['ruciodatasetname'] = f['datasetname'].split(":")[1]
                    else:
                        f['ruciodatasetname'] = f['datasetname']
                    if job['computingsite'] in panda_queues:
                        if job['computingsite'] in ('CERN-P1'):
                            f['ddmsite'] = panda_queues[job['computingsite']]['gocname']
                        else:
                            f['ddmsite'] = computeSvsAtlasS.get(job['computingsite'], "")
                if f['destinationdblocktoken'] and 'dst' in f['destinationdblocktoken']:
                    parced = f['destinationdblocktoken'].split("_")
                    f['ddmsite'] = parced[0][4:]
                    f['dsttoken'] = 'ATLAS' + parced[1]
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
                    if f['scope'] and f['scope'] + ":" in f['dataset']:
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
            file['attemptnr'] = dcfilesDict[file['fileid']]['attemptnr'] if file['fileid'] in dcfilesDict else file[
                'attemptnr']
            file['maxattempt'] = dcfilesDict[file['fileid']]['maxattempt'] if file['fileid'] in dcfilesDict else None
            inputfiles.append(
                {'jeditaskid': file['jeditaskid'], 'datasetid': file['datasetid'], 'fileid': file['fileid']})

    # get log provider
    request.session['viewParams']['log_provider'] = get_log_provider(pandaid)

    if 'pilotid' in job and job['pilotid'] and job['pilotid'].startswith('http') and '{' not in job['pilotid'] and not (
        request.session['viewParams']['log_provider'] == 's3' and 's3' in job['pilotid']
    ):
        stdout = job['pilotid'].split('|')[0]
        if stdout.endswith('pilotlog.txt'):
            stdlog = stdout.replace('pilotlog.txt', 'payload.stdout')
            stderr = stdout.replace('pilotlog.txt', 'payload.stderr')
            stdjdl = None
        else:
            stderr = stdout.replace('.out', '.err')
            stdlog = stdout.replace('.out', '.log')
            stdjdl = stdout.replace('.out', '.jdl')
    elif len(job['harvesterInfo']) > 0 and 'batchlog' in job['harvesterInfo'] and job['harvesterInfo']['batchlog']:
        stdlog = job['harvesterInfo']['batchlog']
        stderr = stdlog.replace('.log', '.err')
        stdout = stdlog.replace('.log', '.out')
        stdjdl = stdlog.replace('.log', '.jdl')
    else:
        stdout = stderr = stdlog = stdjdl = None

    prmon_logs = {}
    if settings.PRMON_LOGS_DIRECTIO_LOCATION and job.get('jobstatus') in ('finished', 'failed') and (
        request.session['viewParams']['log_provider'] == 'gs'
    ):
        prmon_logs['prmon_summary'] = settings.PRMON_LOGS_DIRECTIO_LOCATION.format(
            queue_name=job.get('computingsite'),
            panda_id=pandaid) + '/memory_monitor_summary.json'
        prmon_logs['prmon_details'] = settings.PRMON_LOGS_DIRECTIO_LOCATION.format(
            queue_name=job.get('computingsite'),
            panda_id=pandaid) + '/memory_monitor_output.txt'

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
    if ('jobset' in request.session['requestParams'] or is_event_service(job)) and (
            'jobsetid' in job and job['jobsetid'] and isinstance(job['jobsetid'], int) and job['jobsetid'] > 0):
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
                '.*PIPELINE_TASK=([a-zA-Z0-9]+).*PIPELINE_PROCESSINSTANCE=([0-9]+).*PIPELINE_STREAM=([0-9.]+)',
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
            'prmon_logs': prmon_logs,
            'authtype': auth,
        }
        data.update(getContextVariables(request))
        setCacheEntry(request, "jobInfo", json.dumps(data, cls=DateEncoder), 60 * 20)
        if is_event_service(job):
            response = render(request, 'jobInfoES.html', data, content_type='text/html')
        else:
            response = render(request, 'jobInfo.html', data, content_type='text/html')
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
        return error_response(request, message='not understood', status=400)


@never_cache
def get_job_relationships(request, pandaid=-1):
    """
    Getting job relationships in both directions: downstream (further retries); upstream (past retries).
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    direction = ''
    if 'direction' in request.session['requestParams'] and request.session['requestParams']['direction']:
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
                job_relationships.extend(
                    JediJobRetryHistory.objects.filter(**retryquery).order_by('newpandaid').reverse().values())
            else:
                job_relationships = getSequentialRetries_ESupstream(job['pandaid'], job['jobsetid'], job['jeditaskid'],
                                                                    countOfInvocations)
        elif direction == 'upstream':
            if not is_event_service(job):
                job_relationships = getSequentialRetries(job['pandaid'], job['jeditaskid'], countOfInvocations)
            else:
                job_relationships = getSequentialRetries_ES(job['pandaid'], job['jobsetid'], job['jeditaskid'],
                                                            countOfInvocations)
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
    response = render(request, 'jobRelationships.html', data, content_type='text/html')
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
        response = render(request, 'userList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

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
        uquery = setupView(request, hours=90 * 24, limit=-99, wildCardExt=False, querytype='user')
        if 'modificationtime__castdate__range' in uquery:
            if 'extra' in request.session['requestParams'] and 'notimelimit' in request.session['requestParams'][
                'extra']:
                del uquery['modificationtime__castdate__range']
            else:
                uquery['lastmod__castdate__range'] = uquery['modificationtime__castdate__range']
                del uquery['modificationtime__castdate__range']

        userdb.extend(Users.objects.filter(**uquery).values())
        anajobs = 0
        n1000 = 0
        n10k = 0
        nrecent3 = 0
        nrecent7 = 0
        nrecent30 = 0
        nrecent90 = 0
        # Move to a list of dicts and adjust CPU unit
        for u in userdb:
            u['latestjob'] = u['lastmod']
            udict = {}
            udict['name'] = u['name']
            udict['dn'] = u['dn']
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
        query = setupView(request, hours=nhours, limit=999999, querytype='job')
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
        response = render(request, 'userList.html', data, content_type='text/html')
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
    query_task = None
    query_job = None

    if user == '':
        if 'user' in request.session['requestParams']:
            user = request.session['requestParams']['user']
        if 'produsername' in request.session['requestParams']:
            user = request.session['requestParams']['produsername']
        if request.user.is_authenticated and user == '{} {}'.format(
                request.user.first_name.replace('\'', ''),
                request.user.last_name
        ):
            is_prepare_history_links = True

        # Here we serve only personal user pages. No user parameter specified
        if user == '':
            if request.user.is_authenticated:
                login = user = request.user.username
                # replace middle name by wildcard if exists
                first_name = str(request.user.first_name.replace('\'', ''))
                last_name = str(request.user.last_name)
                if ' ' not in first_name and ' ' not in last_name:
                    query_task = Q(username=login) | Q(username=f"{first_name} {last_name}")
                    query_job = Q(produsername=login) | Q(produsername=f"{first_name} {last_name}")
                else:
                    first_name = first_name.split(' ')[0] if ' ' in first_name else first_name
                    query_task = Q(username=login) | (Q(username__istartswith=first_name) & Q(username__iendswith=last_name))
                    query_job = Q(produsername=login) | (Q(produsername__istartswith=first_name) & Q(produsername__iendswith=last_name))
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
        requestParams[escape_input(param.strip())] = escape_input(
            request.session['requestParams'][param.strip()].strip())
    request.session['requestParams'] = requestParams

    # getting most relevant links based on visit statistics
    links = {}
    if 'ORACLE' in settings.DEPLOYMENT:
        if is_prepare_history_links:
            userids = BPUser.objects.filter(email=request.user.email).values('id')
            userid = userids[0]['id']
            fields = {
                'job': copy.deepcopy(standard_fields),
                'task': copy.deepcopy(standard_taskfields),
                'site': copy.deepcopy(const.SITE_FIELDS_STANDARD),
            }
            links = get_relevant_links(userid, fields)

    # Tasks owned by the user
    query, extra_query_str, _ = setupView(request, hours=days * 24, limit=999999, querytype='task', wildCardExt=True)

    # extend query if any idds-related params are present
    if any(['idds' in p for p in request.session['requestParams']]):
        try:
            from core.iDDS.utils import extend_view_idds
            query, extra_str = extend_view_idds(request, query, extra_query_str)
        except ImportError:
            _logger.exception('Failed to import iDDS utils')
        except Exception as e:
            _logger.exception(f'Failed to extend query with idds related parameters with:\n{e}')

    if query_task is None:
        query['username__icontains'] = user.strip()
        tasks = JediTasks.objects.filter(**query).extra(where=[extra_query_str]).values()
    else:
        tasks = JediTasks.objects.filter(**query).filter(query_task).extra(where=[extra_query_str]).values()
    _logger.info('Got {} tasks: {}'.format(len(tasks), time.time() - request.session['req_init_time']))

    tasks = cleanTaskList(tasks, sortby=sortby, add_datasets_info=True, add_idds_info=True)
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
        url_nolimit_tasks = removeParam(extensibleURL(request), 'display_limit_tasks',
                                        mode='extensible') + "display_limit_tasks=" + str(len(tasks))

        ntasks = len(tasks)
        tasksumd = task_summary_dict(request, tasks)
        _logger.info('Tasks summary generated: {}'.format(time.time() - request.session['req_init_time']))

        # Jobs
        limit = 5000
        query, extra_query_str, LAST_N_HOURS_MAX = setupView(request, hours=72, limit=limit, querytype='job',
                                                             wildCardExt=True)
        jobs = []
        values = 'eventservice', 'produsername', 'cloud', 'computingsite', 'cpuconsumptiontime', 'jobstatus', 'transformation', 'prodsourcelabel', 'specialhandling', 'vo', 'modificationtime', 'pandaid', 'atlasrelease', 'jobsetid', 'processingtype', 'workinggroup', 'jeditaskid', 'taskid', 'currentpriority', 'creationtime', 'starttime', 'endtime', 'brokerageerrorcode', 'brokerageerrordiag', 'ddmerrorcode', 'ddmerrordiag', 'exeerrorcode', 'exeerrordiag', 'jobdispatchererrorcode', 'jobdispatchererrordiag', 'piloterrorcode', 'piloterrordiag', 'superrorcode', 'superrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'transexitcode', 'homepackage', 'inputfileproject', 'inputfiletype', 'attemptnr', 'jobname', 'proddblock', 'destinationdblock', 'container_name', 'cmtconfig'

        if query_job is None:
            query['produsername__icontains'] = user.strip()
            jobs.extend(Jobsdefined4.objects.filter(**query).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsactive4.objects.filter(**query).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobswaiting4.objects.filter(**query).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
                jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_query_str])[
                            :request.session['JOB_LIMIT']].values(*values))
        else:
            jobs.extend(Jobsdefined4.objects.filter(**query).filter(query_job).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsactive4.objects.filter(**query).filter(query_job).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobswaiting4.objects.filter(**query).filter(query_job).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))
            jobs.extend(Jobsarchived4.objects.filter(**query).filter(query_job).extra(where=[extra_query_str])[
                        :request.session['JOB_LIMIT']].values(*values))

            # Here we go to an archive. Separation OR condition is done to enforce Oracle to perform indexed search.
            if len(jobs) == 0 or (len(jobs) < limit and LAST_N_HOURS_MAX > 72):
                query['produsername__startswith'] = user.strip()  # .filter(query_job)
                archjobs = []
                # This two filters again to force Oracle search
                archjobs.extend(Jobsarchived.objects.filter(**query).filter(Q(produsername=user.strip())).extra(
                    where=[extra_query_str])[:request.session['JOB_LIMIT']].values(*values))
                if len(archjobs) > 0:
                    jobs = jobs + archjobs
                elif len(fullname) > 0:
                    # del query['produsername']
                    query['produsername__startswith'] = fullname
                    jobs.extend(Jobsarchived.objects.filter(**query).extra(where=[extra_query_str])[
                                :request.session['JOB_LIMIT']].values(*values))

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
        url_nolimit_jobs = removeParam(extensibleURL(request), 'display_limit_jobs',
                                       mode='extensible') + 'display_limit_jobs=' + str(len(jobs))

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
            response = render(request, 'userInfo.html', data, content_type='text/html')
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
            response = render(request, 'userDash.html', data, content_type='text/html')
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

        jobs = get_job_list(jquery, error_info=True)
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
            'options': {'legend_position': 'bottom', 'size_mp': 0.2, 'color_scheme': 'job_states', }
        }, ]
        _logger.info('Got job status summary: {}'.format(time.time() - request.session['req_init_time']))

        for t in tasks:
            for metric in metrics:
                if t['jeditaskid'] in metrics[metric]['group_by']:
                    t['job_' + metric] = metrics[metric]['group_by'][t['jeditaskid']]
                else:
                    t['job_' + metric] = ''
            if 'dsinfo' in t and len(t['dsinfo']) > 0 and 'nfilesmissing' in t['dsinfo'] and \
                    t['dsinfo']['nfilesmissing'] and t['dsinfo']['nfilesmissing'] > 0:
                t['errordialog'] = '{} files is missing and is not included for processing. {}'.format(
                    t['dsinfo']['nfilesmissing'],
                    t['errordialog'] if t['errordialog'] is not None else ''
                )
            if t['jeditaskid'] in errs_by_task_dict and t['superstatus'] != 'done':
                link_jobs_base = '/jobs/?mode=nodrop&jobstatus=failed&jeditaskid={}&'.format(t['jeditaskid'])
                link_logs_base = '/filebrowser/?'
                t['top_errors_list'] = [[
                    '<a href="{}{}={}">{}</a>'.format(link_jobs_base, err['codename'], err['codeval'], err['count']),
                    ' [{}]'.format(err['error']),
                    ' "{}"'.format(err['diag']),
                    ' <a href="{}pandaid={}">[<i class="fi-link"></i>]</a>'.format(link_logs_base, err['example_pandaid'])
                    ] for err in errs_by_task_dict[t['jeditaskid']]['errorlist'] if len(err['diag']) > 0
                ][:2]
                t['top_errors'] = '<br>'.join(
                    ['<a href="{}{}={}">{}</a> [{}] "{}" <a href="{}pandaid={}">[<i class="fi-link"></i>]</a>'.format(
                        link_jobs_base, err['codename'], err['codeval'], err['count'], err['error'], err['diag'],
                        link_logs_base, err['example_pandaid'],
                    ) for err in errs_by_task_dict[t['jeditaskid']]['errorlist'] if len(err['diag']) > 0][:2])
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
            'jeditaskid', 'idds_request_id', 'attemptnr', 'tasktype', 'taskname', 'nfiles', 'nfilesfinished',
            'nfilesfailed', 'pctfinished', 'status', 'duration_days',
            'errordialog', 'job_failed', 'top_errors_list',
            'job_queuetime', 'job_walltime', 'job_maxpss_per_actualcorecount', 'job_efficiency', 'job_attemptnr',
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
        response = render(request, 'siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'sortby' in request.session['requestParams']:
        sortby = request.session['requestParams']['sortby']
    else:
        sortby = 'alpha'
    for param in request.session['requestParams']:
        request.session['requestParams'][param] = escape_input(request.session['requestParams'][param])

    # get full list of queues
    pqs = get_panda_queues()
    if 'copytool' in request.session['requestParams'] and request.session['requestParams']['copytool'] is not None:
        pqs = {k: v for k, v in pqs.items() if request.session['requestParams']['copytool'] in v['copytools']}

    pqs = filter_pq_json(request, pqs_dict=pqs)
    pqs = list(pqs.values())

    xurl = extensibleURL(request)
    nosorturl = removeParam(xurl, 'sortby', mode='extensible')
    if not is_json_request(request):
        # attribute summary
        sumd = site_summary_dict(pqs, vo_mode=VOMODE, sortby=sortby)
        # prepare data for table
        for pq in pqs:
            if 'maxrss' in pq and isinstance(pq['maxrss'], int):
                pq['maxrss_gb'] = round(pq['maxrss'] / 1000., 1)
            if 'minrss' in pq and isinstance(pq['minrss'], int):
                pq['minrss_gb'] = round(pq['minrss'] / 1000., 1)
            if 'maxtime' in pq and isinstance(pq['maxtime'], int) and pq['maxtime'] > 0:
                pq['maxtime_hours'] = round(pq['maxtime'] / 3600.)
            if 'maxinputsize' in pq and isinstance(pq['maxinputsize'], int) and pq['maxinputsize'] > 0:
                pq['maxinputsize_gb'] = round(pq['maxinputsize'] / 1000.)
            if 'copytools' in pq and pq['copytools'] and len(pq['copytools']) > 0:
                pq['copytool'] = ', '.join(list(pq['copytools'].keys()))
        pq_params_table = [
            'cloud', 'gocname', 'tier', 'nickname', 'status', 'type', 'workflow', 'system', 'copytool', 'harvester',
            'minrss_gb', 'maxrss_gb', 'maxtime_hours', 'maxinputsize_gb', 'comment'
        ]
        sites = []
        for pq in pqs:
            tmp_dict = {}
            for param in pq_params_table:
                tmp_dict[param] = pq[param] if param in pq and pq[param] is not None else '---'
            sites.append(tmp_dict)

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'sites': sites,
            'sumd': sumd,
            'xurl': xurl,
            'nosorturl': nosorturl,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        setCacheEntry(request, "siteList", json.dumps(data, cls=DateEncoder), 60 * 20)
        response = render(request, 'siteList.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(pqs, cls=DateEncoder), content_type='application/json')


@login_customrequired
def siteInfo(request, site=''):
    valid, response = initRequest(request)
    if not valid:
        return response

    if site == '' and 'site' in request.session['requestParams']:
        site = request.session['requestParams']['site']

    # get data from new schedconfig_json table
    HPC = False
    njobhours = 12
    panda_queue = []
    pq_dict = None
    pqquery = {'pandaqueue': site}
    panda_queues = SchedconfigJson.objects.filter(**pqquery).values()
    panda_queue_type = None

    if len(panda_queues) > 0:
        pq_dict = panda_queues[0]['data']
        if isinstance(pq_dict, str):
            pq_dict = json.loads(pq_dict)
    # get PQ params from CRIC if no info in DB
    if not pq_dict:
        pq_dict = get_panda_resource(site)

    if pq_dict:
        panda_queue_type = pq_dict['type']
        for par, val in pq_dict.items():
            val = ', '.join([str(subpar) + ' = ' + str(subval) for subpar, subval in val.items()]) if isinstance(val,
                                                                                                                 dict) else val
            panda_queue.append({'param': par, 'value': val})
        panda_queue = sorted(panda_queue, key=lambda x: x['param'])

        # if HPC increase hours for links
        if 'catchall' in pq_dict and pq_dict['catchall'] and pq_dict['catchall'].find('HPC') >= 0:
            HPC = True
            njobhours = 48

    if not is_json_request(request):
        # prepare relevant params for top table
        attrs = []
        if pq_dict:
            attrs.append({'name': 'GOC name', 'value': pq_dict['gocname'] if 'gocname' in pq_dict else ''})
            if HPC:
                attrs.append({'name': 'HPC', 'value': 'This is a High Performance Computing (HPC) supercomputer queue'})
            if 'catchall' in pq_dict and pq_dict['catchall'].find('log_to_objectstore') >= 0:
                attrs.append({'name': 'Object store logs', 'value': 'Logging to object store is enabled'})
            if 'objectstore' in pq_dict and pq_dict['objectstore'] and len(pq_dict['objectstore']) > 0:
                fields = pq_dict['objectstore'].split('|')
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

            if 'nickname' in pq_dict and pq_dict['nickname'] != site:
                attrs.append({'name': 'Queue (nickname)', 'value': pq_dict['nickname']})
            attrs.append({'name': 'Status', 'value': pq_dict['status'] if 'status' in pq_dict else '-'})
            if 'comment' in pq_dict and pq_dict['comment'] and len(pq_dict['comment']) > 0:
                attrs.append({'name': 'Comment', 'value': pq_dict['comment']})
            if 'type' in pq_dict and pq_dict['type']:
                attrs.append({'name': 'Type', 'value': pq_dict['type']})
            if 'cloud' in pq_dict and pq_dict['cloud']:
                attrs.append({'name': 'Cloud', 'value': pq_dict['cloud']})
            if 'tier' in pq_dict and pq_dict['tier']:
                attrs.append({'name': 'Tier', 'value': pq_dict['tier']})
            if 'corecount' in pq_dict and isinstance(pq_dict['corecount'], int):
                attrs.append({'name': 'Cores', 'value': pq_dict['corecount']})
            if 'maxrss' in pq_dict and isinstance(pq_dict['maxrss'], int):
                attrs.append({'name': 'Max RSS', 'value': "{} GB".format(round(pq_dict['maxrss'] / 1000., 1))})
            if 'maxtime' in pq_dict and isinstance(pq_dict['maxtime'], int) and pq_dict['maxtime'] > 0:
                attrs.append({'name': 'Max time', 'value': "{} hours".format(round(pq_dict['maxtime'] / 3600.))})
            if 'maxinputsize' in pq_dict and isinstance(pq_dict['maxinputsize'], int) and pq_dict['maxinputsize'] > 0:
                attrs.append(
                    {'name': 'Max input size', 'value': "{} GB".format(round(pq_dict['maxinputsize'] / 1000.))})

            # get calculated metrics
            if 'ATLAS' in settings.DEPLOYMENT:
                try:
                    metrics = get_pq_metrics(pq_dict['nickname'])
                except Exception as ex:
                    metrics = {}
                    _logger.exception('Failed to get metrics for {}\n {}'.format(pq_dict['nickname'], ex))
                if len(metrics) > 0:
                    for pq, m_dict in metrics.items():
                        for m in m_dict:
                            panda_queue.append({'label': m, 'param': m, 'value': m_dict[m]})

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'site': pq_dict,
            'colnames': panda_queue,
            'attrs': attrs,
            'name': site,
            'pq_type': panda_queue_type,
            'njobhours': njobhours,
            'hc_link_dates': [
                (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d")],
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        data.update(getContextVariables(request))
        response = render(request, 'siteInfo.html', data, content_type='text/html')
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
        hours = 24 * int(request.session['requestParams']['days'])
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
        response = render(request, 'wnInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "wnInfo")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'wnInfo.html', data, content_type='text/html')
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

    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        sortby = 1  # count
    else:
        sortby = 0  # alpha

    # Remove None wn from failed jobs plot if it is in system, add warning banner
    warning = {}
    if 'None' in plots_data['failed']:
        warning['message'] = '{} failed jobs are excluded from "Failed jobs per WN slot" plot because of unknown modificationhost.'.format(
            plots_data["failed"]["None"]
        )
        try:
            del plots_data['failed']['None']
        except:
            pass

    wnPlotFailedL = sorted(
        [[k, v] for k, v in plots_data['failed'].items()],
        key=lambda x: x[sortby],
        reverse=True
    )
    wnPlotFinishedL = sorted(
        [[k, v] for k, v in plots_data['finished'].items()],
        key=lambda x: x[sortby],
        reverse=True
    )

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
        response = render(request, 'wnInfo.html', data, content_type='text/html')
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


@login_customrequired
def dashboard(request, view='all'):
    """
    This is a legacy. We keep it to redirect to the region dashboard with filters depending on the view.
    :param request:
    :param view:
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # do redirect
    if 'cloudview' in request.session['requestParams'] and request.session['requestParams']['cloudview'] == 'world':
        return redirect('/dash/world/')
    else:
        if view == 'production':
            return redirect('/dash/region/?jobtype=prod&splitby=jobtype')
        elif view == 'analysis':
            return redirect('/dash/region/?jobtype=analy&splitby=jobtype')

    # task view is decommissioned -> return error message
    if 'mode' in request.session['requestParams'] and request.session['requestParams']['mode'] == 'task':
        return error_response(request, message='This view was decommissioned!')

    return redirect('/dash/region/')



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
        response = render(request, 'JobSummaryRegion.html', data, content_type='text/html')
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

    if 'site' in request.session['requestParams'] and request.session['requestParams']['site'] != 'all':
        request.session['requestParams']['queueatlas_site'] = request.session['requestParams']['site']

    # do queue related filtering according to request params
    pqs_dict = filter_pq_json(request)

    # get job summary data
    jsr_queues_dict, jsr_sites_dict, jsr_regions_dict = get_job_summary_region(
        jquery,
        extra=extra_str,
        region=region,
        jobtype=jobtype,
        resourcetype=resourcetype,
        split_by=split_by,
        pqs_dict=pqs_dict
    )

    if is_json_request(request):
        extra_info_params = ['links', ]
        extra_info = {ep: False for ep in extra_info_params}
        if 'extra' in request.session['requestParams'] and 'links' in request.session['requestParams']['extra']:
            extra_info['links'] = True
        jsr_queues_dict, jsr_sites_dict, jsr_regions_dict = prettify_json_output(jsr_queues_dict, jsr_sites_dict,
                                                                                 jsr_regions_dict, hours=hours,
                                                                                 extra=extra_info)
        data = {
            'regions': jsr_regions_dict,
            'sites': jsr_sites_dict,
            'queues': jsr_queues_dict,
        }
        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # transform dict to list and filter out rows depending on split by request param
        jsr_queues_list, jsr_sites_list, jsr_regions_list = prepare_job_summary_region(
            jsr_queues_dict,
            jsr_sites_dict,
            jsr_regions_dict,
            split_by=split_by)

        # prepare lists of unique values for drop down menus
        select_params_dict = {
            'resourcetype': sorted(
                [rt for rt in jsr_queues_dict[list(jsr_queues_dict.keys())[0]]['summary']['all'].keys() if rt != 'all']),
            'queuetype': sorted(list(set([pq[1] for pq in jsr_queues_list]))),
            'queuestatus': sorted(list(set([pq[3] for pq in jsr_queues_list]))),
        }

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
            'sites': jsr_sites_list,
            'queues': jsr_queues_list,
            'show': 'all',
        }

        response = render(request, 'JobSummaryRegion.html', data, content_type='text/html')
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
        response = render(request, 'JobSummaryNucleus.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    if 'hours' in request.session['requestParams'] and request.session['requestParams']['hours']:
        hours = int(request.session['requestParams']['hours'])
    else:
        hours = 12

    query, extra, nhours = setupView(request, hours=hours, limit=999999, wildCardExt=True)

    # get summary data
    is_add_hs06s = True if settings.DEPLOYMENT == 'ORACLE_ATLAS' else False
    jsn_nucleus_dict, jsn_satellite_dict = get_job_summary_nucleus(
        query,
        extra=extra,
        job_states_order=copy.deepcopy(statelist),
        hs06s=is_add_hs06s
    )

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
        response = render(request, 'JobSummaryNucleus.html', data, content_type='text/html')
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
        response = render(request, 'EventService.html', data, content_type='text/html')
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

    jquery, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='job',
                                                            wildCardExt=True)

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
        jsr_queues_dict, _, jsr_regions_dict = prettify_json_output(jsr_queues_dict, {}, jsr_regions_dict, hours=hours,
                                                                    extra=extra_info)
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
        select_params_dict['resourcetype'] = sorted(
                [rt for rt in jsr_queues_dict[list(jsr_queues_dict.keys())[0]]['summary']['all'].keys() if rt != 'all'])

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

        response = render(request, 'EventService.html', data, content_type='text/html')
        setCacheEntry(request, "EventService", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def dashAnalysis(request):
    return dashboard(request, view='analysis')


@login_customrequired
def dashProduction(request):
    return dashboard(request, view='production')


def taskESExtendedInfo(request):
    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return error_response(request, message="No jeditaskid provided", status=400)

    eventsdict = []
    equery = {'jeditaskid': jeditaskid}
    eventsdict.extend(
        JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
    for state in eventsdict:
        state['statusname'] = eventservicestatelist[state['status']]

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
        return render(request, "csrftoken.html", c)
    else:
        resp = {"detail": "User not authenticated. Please login to bigpanda"}
        dump = json.dumps(resp, cls=DateEncoder)
        response = HttpResponse(dump, content_type='application/json')
        return response


@login_customrequired
@csrf_exempt
def taskList(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "taskList")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if data['eventservice'] is True:
            response = render(request, 'taskListES.html', data, content_type='text/html')
        else:
            response = render(request, 'taskList.html', data, content_type='text/html')
        _logger.info('Rendered template with cached data: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    thread = None
    transaction_key = None
    dkey = digkey(request)

    if 'limit' in request.session['requestParams']:
        limit = int(request.session['requestParams']['limit'])
    else:
        limit = 5000
        if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'pctfailed':
            limit = 50000

    if 'tasktype' in request.session['requestParams'] and request.session['requestParams']['tasktype'].startswith('ana'):
        hours = 3 * 24
    else:
        hours = 7 * 24

    sortby = "jeditaskid-desc"
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby']:
        sortby = request.session['requestParams']['sortby']

    eventservice = False
    if 'eventservice' in request.session['requestParams'] and (
            request.session['requestParams']['eventservice'] == 'eventservice' or
            request.session['requestParams']['eventservice'] == '1'):
        eventservice = True
        hours = 7 * 24

    query, extra_str, _ = setupView(request, hours=hours, limit=limit, querytype='task', wildCardExt=True)

    if 'ATLAS' in settings.DEPLOYMENT:
        query, extra_str = extend_view_deft(request, query, extra_str)
    if any(['idds' in p for p in request.session['requestParams']]):
        try:
            from core.iDDS.utils import extend_view_idds
            query, extra_str = extend_view_idds(request, query, extra_str)
        except ImportError:
            _logger.exception('Failed to import iDDS utils')
        else:
            _logger.exception('Failed to extend query with idds related parameters')

    # remove time limit if jeditaskid in query
    if 'modificationtime__castdate__range' in query and (
            'jeditaskid__in' in query or 'jeditaskid in' in extra_str.lower()):
        del query['modificationtime__castdate__range']

    # if jeditaskid list in query is too long -> put it in tmp table
    if 'jeditaskid__in' in query and len(query['jeditaskid__in']) > settings.DB_N_MAX_IN_QUERY:
        tmp_table_name = get_tmp_table_name()
        transaction_key = insert_to_temp_table(query['jeditaskid__in'])
        extra_str += "and jeditaskid in (select tmp.id from {} tmp where transactionkey={})".format(
            tmp_table_name,
            transaction_key
        )
        del query['jeditaskid__in']

    listTasks = []
    if 'statenotupdated' in request.session['requestParams']:
        tasks = tasks_not_updated(request, query, extra_str)
    else:
        tasks = JediTasks.objects.filter(**query).extra(where=[extra_str]).order_by('-modificationtime')[:limit].values()
        # calculate total number of tasks suited for query without hard limit
        listTasks.append(JediTasks)
        if not is_json_request(request):
            thread = Thread(target=totalCount, args=(listTasks, query, extra_str, dkey))
            thread.start()
        else:
            thread = None
    _logger.info('Got {} tasks: {}'.format(len(tasks), time.time() - request.session['req_init_time']))

    # schedule task error diag analyser in a separate thread
    error_codes_analyser = TasksErrorCodesAnalyser()
    if not is_json_request(request):
        error_codes_analyser.schedule_preprocessing(tasks)

    datasetstage = []
    hashtags = []
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        # Getting hashtags for task selection
        tasks, hashtags = hashtags_for_tasklist(request, tasks, transaction_key)
        _logger.info('Got hashtags: {}'.format(time.time() - request.session['req_init_time']))
        if 'tape' in request.session['requestParams']:
            tasks, datasetstage = staging_info_for_tasklist(request, tasks, transaction_key)
            _logger.info('Got info of staging datasets: {}'.format(time.time() - request.session['req_init_time']))

    if eventservice:
        tasks = add_event_summary_to_tasklist(tasks)
        _logger.info('Got events summary: {}'.format(time.time() - request.session['req_init_time']))

    tasks = cleanTaskList(tasks, sortby=sortby, add_datasets_info=True)
    _logger.info('Cleaned task list: {}'.format(time.time() - request.session['req_init_time']))

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
                jvalues = ('pandaid', 'jeditaskid', 'jobstatus', 'creationtime')
                job_pids.extend(Jobsarchived4.objects.filter(**jobQuery).values(*jvalues))
                job_pids.extend(Jobsarchived.objects.filter(**jobQuery).values(*jvalues))
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
        return JsonResponse(tasks, encoder=DateEncoder, safe=False)
    else:
        xurl = extensibleURL(request)
        nohashtagurl = removeParam(xurl, 'hashtag', mode='extensible')
        noerrordialogurl = removeParam(xurl, 'errordialog', mode='extensible')

        ntasks = len(tasks)
        sumd = task_summary_dict(
            request,
            tasks,
            copy.deepcopy(standard_taskfields) + ['stagesource'] if 'tape' in request.session['requestParams'] else copy.deepcopy(standard_taskfields))
        _logger.info('Prepared attribute summary: {}'.format(time.time() - request.session['req_init_time']))

        # for tasks plots
        taskl = [task['jeditaskid'] for task in tasks]
        transaction_key = insert_to_temp_table(taskl)
        setCacheEntry(request, transaction_key, taskl, 60 * 20, isData=True)

        # set up google flow diagram
        flowstruct = buildGoogleFlowDiagram(request, tasks=tasks)

        # get results from error diag analyser
        error_summary_table = error_codes_analyser.get_errors_table()
        error_summary_table = json.dumps(error_summary_table, cls=DateEncoder)
        _logger.info('Prepared error summary: {}'.format(time.time() - request.session['req_init_time']))

        # join the thread with counting tasks without hard limit
        if thread:
            try:
                thread.join()
                tasksTotalCount = sum(tcount[dkey])
                _logger.info('Found {} tasks in total. dkey={}, tcount={}'.format(tasksTotalCount, dkey, tcount))
                del tcount[dkey]
            except:
                tasksTotalCount = -1
                _logger.exception('Failed to get total number of tasks: {}'.format(
                    time.time() - request.session['req_init_time']
                ))
        else:
            tasksTotalCount = -1
        if math.fabs(ntasks - tasksTotalCount) < 1000 or tasksTotalCount == -1:
            tasksTotalCount = None
        else:
            tasksTotalCount = int(math.ceil((tasksTotalCount + 10000) / 10000) * 10000)

        # cut task list
        n_tasks_to_show = 100
        if 'display_limit' in request.session['requestParams']:
            n_tasks_to_show = int(request.session['requestParams']['display_limit'])
        tasks = tasks[:n_tasks_to_show]
        # add idds info to tasks if not ATLAS deployment
        if 'ATLAS' not in settings.DEPLOYMENT and 'core.iDDS' in settings.INSTALLED_APPS:
            tasks = add_idds_info_to_tasks(tasks)
        tasks = stringify_datetime_fields(tasks, JediTasks)

        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'tasks': tasks,
            'datasetstage': json.dumps(datasetstage, cls=DateEncoder),
            'ntasks': ntasks,
            'sumd': sumd,
            'hashtags': hashtags,
            'xurl': xurl,
            'nohashtagurl': nohashtagurl,
            'noerrordialogurl': noerrordialogurl,
            'flowstruct': flowstruct,
            'eventservice': eventservice,
            'tasksTotalCount': tasksTotalCount,
            'built': datetime.now().strftime("%H:%M:%S"),
            'idtasks': transaction_key,
            'error_summary_table': error_summary_table
        }

        setCacheEntry(request, "taskList", json.dumps(data, cls=DateEncoder), 60 * 20)
        if eventservice:
            response = render(request, 'taskListES.html', data, content_type='text/html')
        else:
            response = render(request, 'taskList.html', data, content_type='text/html')
        _logger.info('Template rendered: {}'.format(time.time() - request.session['req_init_time']))
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
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Token ' + prodsysToken
    }
    conn = urllib3.HTTPSConnectionPool(prodsysHost, timeout=100)
    resp = None

    # if request.session['IS_TESTER']:
    resp = conn.urlopen('POST', prodsysUrl, body=json.dumps(postdata, cls=DateEncoder), headers=headers, retries=1,
                        assert_same_host=False)
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


@never_cache
def killtasks_token(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    from requests import get, post

    taskid = -1
    action = -1

    if 'task' in request.session['requestParams']:
        taskid = int(request.session['requestParams']['task'])
    if 'action' in request.session['requestParams']:
        action = int(request.session['requestParams']['action'])

    id_token = None
    token_type = None
    access_token = None

    username = None
    fullname = None
    organisation = 'atlas'

    auth_provider = None

    user = request.user

    if user.is_authenticated and user.social_auth is not None:

        auth_provider = (request.user.social_auth.get()).provider
        social = request.user.social_auth.get(provider=auth_provider)

        if (auth_provider == 'indigoiam'):
            if (social.extra_data['auth_time'] + social.extra_data['expires_in'] - 10) <= int(time.time()):
                resp = {"detail": "id token is expired"}
                dump = json.dumps(resp, cls=DateEncoder)
                response = HttpResponse(dump, content_type='application/json')
                return response
            else:
                token_type = social.extra_data['token_type']
                access_token = social.extra_data['access_token']
                id_token = social.extra_data['id_token']

                os.environ['PANDA_AUTH_ID_TOKEN'] = id_token
                os.environ['PANDA_AUTH'] = 'oidc'
                os.environ['PANDA_AUTH_VO'] = organisation
        else:
            return None

    resp = get('https://atlas-auth.web.cern.ch/api/tokens/access', data={"grant_type": "access_token"},
               headers={'Authorization': '%s %s' % (token_type, access_token)})

    header = {}
    header['Authorization'] = 'Bearer {0}'.format(id_token)
    header['Origin'] = 'atlas'
    resp1 = post('https://pandaserver.cern.ch/server/panda/getAttr', headers=header)

    if resp.ok:
        user_tokens = json.loads(resp.text)
        #
        # from pandaclient import Client
        # c=Client()

        _logger.info('completed')
        # c = Client()
        # c.show_tasks()
        # from core.panda_client.utils import kill_task, show_tasks
        # show_tasks()

    resp = None

    if resp and len(resp.data) > 0:
        try:
            pass
        except:
            pass
    else:
        pass

    dump = json.dumps(resp, cls=DateEncoder)
    response = HttpResponse(dump, content_type='application/json')
    return response


def getErrorSummaryForEvents(request):
    valid, response = initRequest(request)
    if not valid: return response
    data = {}
    eventsErrors = []
    _logger.debug('getting error summary for events')
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
    equery['jeditaskid'] = jeditaskid
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
                  from {}.jedi_events
                  where jeditaskid={} and ERROR_CODE is not null
                  group by error_code, pandaid ) e
                join
                  (select ID from {} where TRANSACTIONKEY={} ) j
                on e.pandaid = j.ID))
            group by error_code""".format(settings.DB_SCHEMA_PANDA, jeditaskid, tmpTableName, transactionKey)
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
                  from {}.jedi_events
                  where jeditaskid={} and ERROR_CODE is not null 
                    and pandaid not in ( select ID from {} where TRANSACTIONKEY={} )
                  group by error_code, pandaid ) e
                ))
            group by error_code""".format(settings.DB_SCHEMA_PANDA, jeditaskid, tmpTableName, transactionKeyDJ)
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
                          from {}.jedi_events
                          where jeditaskid={} and ERROR_CODE is not null
                          group by error_code, pandaid)
                  group by error_code
            """.format(settings.DB_SCHEMA_PANDA, jeditaskid)
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

    response = render(request, 'eventsErrorSummary.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


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
            request.session['viewParams']['selection'] = ', started at ' + task_profile_start['starttime'].strftime(
                settings.DATETIME_FORMAT)
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
    response = render(request, 'taskProgressMonitor.html', data, content_type='text/html')
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
        task_profile_dict = task_profile.get_raw_task_profile_full(
            taskid=jeditaskid,
            jobstatus=request_job_states,
            category=request_job_types
        )
    else:
        msg = 'Not valid jeditaskid provided: {}'.format(jeditaskid)
        _logger.exception(msg)
        response = HttpResponse(json.dumps(msg), status=400)

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
            order_dict[jtn + '_' + js] = order_mpx[js] * order_mpx[jtn]

    task_profile_data_dict = {}
    for jt in job_types:
        if len(task_profile_dict[jt]) > 0:
            for js in list(set(job_states) & set([r['jobstatus'] for r in task_profile_dict[jt]])):
                for jtmn in job_time_names:
                    task_profile_data_dict['_'.join((jtmn, js, jt))] = {
                        'name': '_'.join((jtmn, js, jt)),
                        'label': jtmn.capitalize() + ' time of a ' + js + ' ' + jt + ' job',
                        'pointRadius': round(1 + 3.0 * math.exp(-0.0004 * len(task_profile_dict[jt]))),
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
    response = render(request, 'userProfile.html', data, content_type='text/html')
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
        'creation': {'active': 'RGBA(0,169,255,0.75)', 'finished': 'RGBA(162,198,110,0.75)',
                     'failed': 'RGBA(255,176,176,0.75)',
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
            order_dict[jtn + '_' + js] = order_mpx[js] * order_mpx[jtn]

    user_Dataprofile_data_dict = {}
    for jt in job_types:
        if len(user_Dataprofile_dict[jt]) > 0:
            for js in list(set(job_states) & set([r['jobstatus'] for r in user_Dataprofile_dict[jt]])):
                for jtmn in job_time_names:
                    user_Dataprofile_data_dict['_'.join((jtmn, js, jt))] = {
                        'name': '_'.join((jtmn, js, jt)),
                        'label': jtmn.capitalize() + ' time of a ' + js + ' ' + jt + ' job',
                        'pointRadius': round(1 + 4.0 * math.exp(-0.0004 * len(user_Dataprofile_dict[jt]))),
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
        jeditaskid = re.findall("\\d+", jeditaskid)
        jdtstr = ""
        for jdt in jeditaskid:
            jdtstr = jdtstr + str(jdt)
        return redirect('/task/' + jdtstr)
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
    # Get the current AUTH type
    auth = get_auth_provider(request)

    # temporarily turn off caching
    # data = None
    if data is not None:
        data = json.loads(data)
        if data is not None:
            doRefresh = False

            if 'authtype' in data and data['authtype'] != auth:
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
                        if (task['status'] == data['task']['status'] and task['superstatus'] == data['task'][
                            'superstatus'] and
                                task['modificationtime'].strftime(settings.DATETIME_FORMAT) == data['task'][
                                    'modificationtime']):
                            doRefresh = False
                        else:
                            doRefresh = True
                    else:
                        doRefresh = True

            if not doRefresh:
                data['request'] = request
                if data['eventservice']:
                    if 'version' not in request.session['requestParams'] or (
                            'version' in request.session['requestParams'] and request.session['requestParams'][
                        'version'] != 'old'):
                        response = render(request, 'taskInfoESNew.html', data, content_type='text/html')
                    else:
                        response = render(request, 'taskInfoES.html', data, content_type='text/html')
                else:
                    response = render(request, 'taskInfo.html', data, content_type='text/html')
                patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
                return response

    if 'taskname' in request.session['requestParams'] and request.session['requestParams']['taskname'].find('*') >= 0:
        return taskList(request)

    setupView(request, hours=365 * 24, limit=999999999, querytype='task')
    tasks = []
    warning = {}
    info = {}

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
            return error_response(request, message="No jeditaskid or taskname provided", status=400)

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
        return render(request, 'taskInfo.html', data, content_type='text/html')

    eventservice = False
    if 'eventservice' in taskrec and (taskrec['eventservice'] == 1 or taskrec['eventservice'] == 'eventservice'):
        eventservice = True
        mode = 'nodrop'

    # nodrop only for tasks older than 2 years
    if get_task_timewindow(taskrec, format_out='datetime')[0] <= datetime.now() - timedelta(days=365 * 3):
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

    # analyse cliParams -> warnings
    if 'cliParams' in taskparams:
        warning['submission'] = analyse_task_submission_options(taskparams['cliParams'])

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
    if taskrec and dsinfo:
        taskrec['dsinfo'] = dsinfo
        taskrec['totev'] = dsinfo['neventsTot']
        taskrec['totevproc'] = dsinfo['neventsUsedTot']
        taskrec['pctfinished'] = (100 * taskrec['totevproc'] / taskrec['totev']) if (taskrec['totev'] > 0) else ''
        taskrec['totevhs06'] = round(
            taskrec['totev'] * convert_hs06(taskrec['cputime'], taskrec['cputimeunit'])) if (
                    taskrec['cputime'] and taskrec['cputimeunit'] and taskrec['totev'] > 0) else None
        taskrec['totevoutput'] = dsinfo['neventsOutput'] if 'neventsOutput' in dsinfo else 0
    # get input and output containers
    inctrs = []
    outctrs = []
    if 'dsForIN' in taskparams and taskparams['dsForIN'] and isinstance(taskparams['dsForIN'], str):
        inctrs = [{
            'containername': cin,
            'nfiles': 0,
            'nfilesfinished': 0,
            'nfilesfailed': 0,
            'nfilesmissing': 0,
            'pct': 0
        } for cin in taskparams['dsForIN'].split(',')]
        # fill the list of input containers with progress info
        for inc in inctrs:
            for ds in dsets:
                if ds['containername'] == inc['containername']:
                    inc['nfiles'] += ds['nfiles'] if 'nfiles' in ds and ds['nfiles'] else 0
                    inc['nfilesfinished'] += ds['nfilesfinished'] if 'nfilesfinished' in ds and ds['nfilesfinished'] else 0
                    inc['nfilesfailed'] += ds['nfilesfailed'] if 'nfilesfailed' in ds and ds['nfilesfailed'] else 0
                    inc['nfilesmissing'] += ds['nfilesmissing'] if 'nfilesmissing' in ds and ds['nfilesmissing'] else 0
                    inc['pct'] = math.floor(100.0 * inc['nfilesfinished'] / inc['nfiles']) if ds['nfiles'] and ds['nfiles'] > 0 else inc['pct']

    outctrs.extend(
        list(set([ds['containername'] for ds in dsets if ds['type'] in ('output', 'log') and ds['containername']])))
    # get dataset locality
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        dataset_locality = get_dataset_locality(jeditaskid)
    else:
        dataset_locality = {}
    for ds in dsets:
        if jeditaskid in dataset_locality and ds['datasetid'] in dataset_locality[jeditaskid]:
            ds['rse'] = ', '.join([item['rse'] for item in dataset_locality[jeditaskid][ds['datasetid']]])
    _logger.info("Loading datasets info: {}".format(time.time() - request.session['req_init_time']))

    # get sum of hs06sec grouped by status
    # creating a jquery with timewindow
    jquery = copy.deepcopy(query)
    jquery['modificationtime__castdate__range'] = get_task_timewindow(taskrec, format_out='str')
    if 'ATLAS' in settings.DEPLOYMENT:
        job_metrics_sum = get_job_metrics_summary_for_task(jquery)
    else:
        job_metrics_sum = {}
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
            taskrec['pcttotevproc_evst'] = round(100. * taskrec['totevproc_evst'] / taskrec['totev'], 2) if taskrec[
                                                                                                                'totev'] > 0 else ''
            taskrec['pctfinished'] = round(100. * taskrec['totevproc'] / taskrec['totev'], 2) if taskrec[
                                                                                                     'totev'] > 0 else ''
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
        _logger.info(
            "Loaded corecount and normalized corecount: {}".format(time.time() - request.session['req_init_time']))

    # update taskrec dict
    if taskrec:
        if 'tasktype' in taskrec and taskrec['tasktype'] and 'ORACLE' in settings.DEPLOYMENT:
            tmcj_list = get_top_memory_consumers(taskrec)
            if len(tmcj_list) > 0 and len([True for job in tmcj_list if job['maxrssratio'] >= 1]) > 0:
                warning['memoryleaksuspicion'] = {}
                warning['memoryleaksuspicion']['message'] = 'Some jobs in this task consumed a lot of memory. '
                warning['memoryleaksuspicion'][
                    'message'] += 'We suspect there might be memory leaks or some misconfiguration.'
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

        if job_metrics_sum:
            taskrec['totevprochs06'] = int(job_metrics_sum['hs06sec']['finished']) if 'hs06sec' in job_metrics_sum else None
            taskrec['failedevprochs06'] = int(job_metrics_sum['hs06sec']['failed']) if 'hs06sec' in job_metrics_sum else None
            taskrec['currenttotevhs06'] = int(job_metrics_sum['hs06sec']['total']) if 'hs06sec' in job_metrics_sum else None

            if 'gco2_global' in job_metrics_sum:
                taskrec['gco2_global_humanized'] = {}
                for k, v in job_metrics_sum['gco2_global'].items():
                    cv, unit = convert_grams(float(v), output_unit='auto')
                    taskrec['gco2_global_humanized'][k] = {'unit': unit, 'value': round_to_n_digits(cv, n=0, method='floor')}
                taskrec.update({
                    'gco2_global_' + k: int(v) for k, v in job_metrics_sum['gco2_global'].items()
                })

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
        # get split rule changes from ES-atlas (we do it only for rendered templates)
        if 'ATLAS' in settings.DEPLOYMENT:
            try:
                connection = create_os_connection()
                split_rule = get_split_rule_info(connection, jeditaskid)
                if len(split_rule) > 0:
                    info['split_rule'] = {}
                    info['split_rule']['messages'] = split_rule
            except Exception as e:
                _logger.exception('Failed to get split rule info for task from opensearch with:\n{}'.format(e))

            if job_metrics_sum:
                if 'hs06sec' in job_metrics_sum:
                    taskrec['hs23s_humanized'] = {
                        k: dict(zip(
                            ('value', 'unit'),
                            (round_to_n_digits(convert_to_si_prefix(v)[0], n=1), convert_to_si_prefix(v)[1])
                        )) for k, v in job_metrics_sum['hs06sec'].items()
                    }
                    if 'totevhs06' in taskrec:
                        taskrec['hs23s_humanized']['expected'] = dict(zip(('value', 'unit'), (round_to_n_digits(
                            convert_to_si_prefix(taskrec['totevhs06'])[0], n=1), convert_to_si_prefix(taskrec['totevhs06'])[1])))
                    else:
                        taskrec['hs23s_humanized']['expected'] = '-'
                if 'gco2_global' in job_metrics_sum:
                    taskrec['gco2_global_humanized'] = {}
                    for k, v in job_metrics_sum['gco2_global'].items():
                        cv, unit = convert_grams(float(v), output_unit='auto')
                        taskrec['gco2_global_humanized'][k] = {'unit': unit, 'value': round_to_n_digits(cv, n=0)}
                    taskrec.update({
                        'gco2_global_' + k: int(v) for k, v in job_metrics_sum['gco2_global'].items()
                    })

        # prepare data for template
        taskparams, jobparams = humanize_task_params(taskparams)

        furl = request.get_full_path()
        nomodeurl = extensibleURL(request, removeParam(furl, 'mode'))

        # decide on data caching time [seconds]
        cacheexpiration = 60 * 20  # second/minute * minutes
        if taskrec and 'status' in taskrec and taskrec['status'] in const.TASK_STATES_FINAL and (
                'dsinfo' in taskrec and 'nfiles' in taskrec['dsinfo'] and (
                    isinstance(taskrec['dsinfo']['nfiles'], int) and taskrec['dsinfo']['nfiles'] > 10000)):
            cacheexpiration = 3600 * 24 * 31  # we store such data a month

        user_expert = is_expert(request)

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
            'info': info,
            'authtype': auth,
            'userexpert': user_expert
        }
        data.update(getContextVariables(request))

        if eventservice:
            data['eventssummary'] = eventssummary
            if 'version' not in request.session['requestParams'] or (
                    'version' in request.session['requestParams'] and request.session['requestParams'][
                'version'] != 'old'):
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
                setCacheEntry(request, transactionKeyIEC, json.dumps(inputfiles_list, cls=DateTimeEncoder), 60 * 30,
                              isData=True)
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
                response = render(request, 'taskInfoESNew.html', data, content_type='text/html')
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
                response = render(request, 'taskInfoES.html', data, content_type='text/html')
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
            response = render(request, 'taskInfo.html', data, content_type='text/html')

        _logger.info('Rendered template: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


def getEventsDetails(request, mode='drop', jeditaskid=0):
    """
    A view for ES task Info page to get events details in different states
    """
    valid, response = initRequest(request)
    if not valid: return response

    tmpTableName = get_tmp_table_name()

    if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
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
          from {}.jedi_events e
            join
                (select computingsite, computingelement,pandaid from {}.JOBSARCHIVED4 where jeditaskid={} {}
                UNION
                select computingsite, computingelement,pandaid from {}.JOBSARCHIVED where jeditaskid={} {}
                ) j
            on (e.jeditaskid={} and e.pandaid=j.pandaid)
        group by j.computingsite, j.COMPUTINGELEMENT, e.objstore_id, e.status""".format(
        settings.DB_SCHEMA_PANDA, settings.DB_SCHEMA_PANDA, jeditaskid, extrastr,
        settings.DB_SCHEMA_PANDA_ARCH, jeditaskid, extrastr,
        jeditaskid)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    ossummary = cur.fetchall()
    cur.close()

    ossummarynames = ['computingsite', 'computingelement', 'objectstoreid', 'statusindex', 'nevents']
    objectStoreDict = [dict(zip(ossummarynames, row)) for row in ossummary]
    for row in objectStoreDict: row['statusname'] = eventservicestatelist[row['statusindex']]

    return HttpResponse(json.dumps(objectStoreDict, cls=DateEncoder), content_type='application/json')


def taskchain(request):
    """
    Task chain plot based on ATLAS_DEFT tables
    :param request:
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    jeditaskid = -1
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == -1:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    new_cur = connection.cursor()
    taskChainSQL = "SELECT * FROM table({}.GETTASKSCHAIN_TEST({}))".format(settings.DB_SCHEMA, jeditaskid)
    new_cur.execute(taskChainSQL)
    taskChain = new_cur.fetchall()
    results = ["".join(map(str, r)) for r in taskChain]
    ts = "".join(results)

    data = {
        'viewParams': request.session['viewParams'],
        'taskChain': ts,
        'jeditaskid': jeditaskid
    }
    response = render(request, 'taskchain.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


def ganttTaskChain(request):
    """"
     Task chain Gantt diagram based on ATLAS_DEFT tables
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    jeditaskid = -1
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = int(request.session['requestParams']['jeditaskid'])
    if jeditaskid == -1:
        data = {"error": "no jeditaskid supplied"}
        return HttpResponse(json.dumps(data, cls=DateTimeEncoder), content_type='application/json')

    task_chain_data = get_task_chain(jeditaskid)

    data = {
        'viewParams': request.session['viewParams'],
        'ganttTaskChain': task_chain_data,
        'jeditaskid': jeditaskid,
        'request': request,
    }
    response = render(request, 'ganttTaskChain.html', data, content_type='text/html')
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

    data = getCacheEntry(request, "jobSummaryForTask" + str(jeditaskid) + mode, isData=True)
    data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request

        if infotype == 'jobsummary':
            response = render(request, 'jobSummaryForTask.html', data, content_type='text/html')
        elif infotype == 'scouts':
            response = render(request, 'scoutsForTask.html', data, content_type='text/html')
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
    setCacheEntry(request, 'jobSummaryForTask' + str(jeditaskid) + mode, json.dumps(alldata, cls=DateEncoder), 60 * 10,
                  isData=True)

    if infotype == 'jobsummary':
        data = {
            'jeditaskid': jeditaskid,
            'mode': mode,
            'jobsummary': jobsummary,
        }
        response = render(request, 'jobSummaryForTask.html', data, content_type='text/html')
    elif infotype == 'scouts':
        data = {
            'jeditaskid': jeditaskid,
            'jobscoutids': jobScoutIDs,
        }
        response = render(request, 'scoutsForTask.html', data, content_type='text/html')
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
    _logger.debug('Thread started')
    lock.acquire()
    try:
        tcount.setdefault(dkey, [])
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
    _logger.debug('Thread finished')


def digkey(rq):
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
                    list[0] = datetime.strptime(list[0], "%Y-%m-%dT%H:%M:%S")
                except:
                    pass
        _logger.info('Processed cached data: {}'.format(time.time() - request.session['req_init_time']))
        response = render(request, 'errorSummary.html', data, content_type='text/html')
        _logger.info('Rendered template from cached data: {}'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

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
    # filter out previous errors for successfully processed files, i.e. keeping errors for unfinished files only
    if 'jeditaskid' in request.session['requestParams'] and 'extra' in request.session['requestParams'] and \
            request.session['requestParams']['extra'] == 'unfinishedfiles':
        jeditaskid = request.session['requestParams']['jeditaskid']
        # get unfinished input files
        files_input_unfinished = JediDatasetContents.objects.filter(jeditaskid=jeditaskid).exclude(status__in=('finished',)).extra(
            where=[f"""datasetid in (
                select datasetid from {settings.DB_SCHEMA_PANDA}.JEDI_DATASETS 
                where jeditaskid={jeditaskid} and type in ('input', 'pseudo_input') and masterid is null)"""]
        ).values('fileid', 'datasetid', 'status')
        if len(files_input_unfinished) > settings.DB_N_MAX_IN_QUERY:
            # put into tmp table
            transaction_key = insert_to_temp_table([f['fileid'] for f in files_input_unfinished])
            extra_str = f"""select id from {get_tmp_table_name()} where TRANSACTIONKEY = {transaction_key}"""
        else:
            extra_str = ','.join([str(f['fileid']) for f in files_input_unfinished])
        wildCardExtension += f""" and pandaid in (
            select pandaid from {settings.DB_SCHEMA_PANDA}.filestable4 where jeditaskid={jeditaskid} and fileid in ({extra_str})
            union all 
            select pandaid from {settings.DB_SCHEMA_PANDA_ARCH}.filestable_arch where jeditaskid={jeditaskid} and fileid in ({extra_str})
        )"""
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

    if (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0],
                                             "%Y-%m-%d %H:%M:%S")).days > 1) or \
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
    errsByCount, errsBySite, errsByUser, errsByTask, sumd, errHist = errorSummaryDict(request, jobs, testjobs,
                                                                                      errHist=True)

    _logger.info('Error summary built: {}'.format(time.time() - request.session['req_init_time']))

    # Build the state summary by computingsite to give perspective
    notime = False  # behave as it used to before introducing notime for dashboards. Pull only 12hrs.
    # remove jobstatus from query
    squery = copy.deepcopy(query)
    if 'jobstatus__in' in squery:
        del squery['jobstatus__in']
    jsr_queues_dict, _, _ = get_job_summary_region(squery, extra=wildCardExtension)
    sitestates = {}
    savestates = ['finished', 'failed', 'cancelled', 'holding', ]
    for pq, data in jsr_queues_dict.items():
        sitestates[pq] = {}
        for s in savestates:
            sitestates[pq][s] = 0
            if 'summary' in data and 'all' in data['summary'] and 'all' in data['summary']['all'] and s in data['summary']['all']['all']:
                sitestates[pq][s] += data['summary']['all']['all'][s]
        if sitestates[pq]['failed'] > 0:
            sitestates[pq]['pctfail'] = round(
                100.0*sitestates[pq]['failed']/(sitestates[pq]['finished'] + sitestates[pq]['failed'])
            )
        else:
            sitestates[pq]['pctfail'] = 0

    for site in errsBySite:
        sitename = site['name']
        if sitename in sitestates:
            for s in savestates:
                if s in sitestates[sitename]:
                    site[s] = sitestates[sitename][s]
            if 'pctfail' in sitestates[sitename]:
                site['pctfail'] = sitestates[sitename]['pctfail']
    _logger.info('Built errors by site summary: {}'.format(time.time() - request.session['req_init_time']))

    taskname = ''
    if not testjobs:
        # Build the task state summary and add task state info to task error summary
        taskstatesummary = task_summary(query, limit=limit, view=jobtype)

        _logger.info(
            'Prepared data for errors by task summary: {}'.format(time.time() - request.session['req_init_time']))

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
            taskname = get_task_name_by_taskid(request.session['requestParams']['jeditaskid'])
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
            _logger.debug(f"{dkey}: total jobs found {tcount[dkey]})")
            del tcount[dkey]
        except:
            jobsErrorsTotalCount = -1
    else:
        jobsErrorsTotalCount = -1

    _logger.info(
        'Finished thread counting total number of jobs: {}'.format(time.time() - request.session['req_init_time']))

    listPar = []
    for key, val in request.session['requestParams'].items():
        if (key != 'limit' and key != 'display_limit'):
            listPar.append(key + '=' + str(val))
    if len(listPar) > 0:
        urlParametrs = '&'.join(listPar) + '&'
    else:
        urlParametrs = None
    _logger.debug(listPar)
    del listPar
    if math.fabs(njobs - jobsErrorsTotalCount) < 1000:
        jobsErrorsTotalCount = None
    else:
        jobsErrorsTotalCount = int(math.ceil((jobsErrorsTotalCount + 10000) / 10000) * 10000)
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
        response = render(request, 'errorSummary.html', data, content_type='text/html')

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
            resp.append(
                {'pandaid': job['pandaid'], 'status': job['jobstatus'], 'prodsourcelabel': job['prodsourcelabel'],
                 'produserid': job['produserid']})
        return HttpResponse(json.dumps(resp), content_type='application/json')


def decommissioned(request, **kwargs):
    """
    Placeholder for decommissioned views
    :param request:
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    if not is_json_request(request):
        data = {
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'request': request,
        }
        response = render(request, '_decommissioned.html', data, content_type='text/html')
    else:
        response = JsonResponse({'message': 'decommissioned'}, status=410)
    return response



def esatlasPandaLoggerJson(request):
    valid, response = initRequest(request)
    if not valid or settings is None:
        return response

    if settings.DEPLOYMENT != 'ORACLE_ATLAS':
        return HttpResponse('It does not exist for non ATLAS BipPanDA monitoring system', content_type='text/html')

    os_conn = create_os_connection()

    jedi_logs_index = settings.OS_INDEX_JEDI_LOGS

    s = Search(using=os_conn, index=jedi_logs_index)

    s.aggs.bucket('jediTaskID', 'terms', field='jediTaskID', size=100) \
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    res = s.execute()
    _logger.debug('query completed')

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

    if settings.DEPLOYMENT != 'ORACLE_ATLAS':
        return error_response(request, message='It does not exist for non ATLAS BipPanDA monitoring system')

    connection = create_os_connection()

    today = time.strftime("%Y.%m.%d")

    pandaDesc = {
        "panda.log.RetrialModule": ["cat1", "Retry module to apply rules on failed jobs"],

        "panda.log.Serveraccess": ["cat2", "Apache request log"],
        "panda.log.Servererror": ["cat2", "Apache errors"],
        "panda.log.PilotRequests": ["cat2", "Pilot requests"],
        "panda.log.Entry": ["cat2", "Entry point to the PanDA server"],
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
        "panda.log.AtlasProdTaskBroker": ["cat1", "Production task brokerage"],
        "panda.log.TaskBroker": ["cat7", "Task brokerage factory"],
        "panda.log.AtlasProdJobBroker": ["cat1", "Production job brokerage"],
        "panda.log.AtlasAnalJobBroker": ["cat1", "Analysis job brokerage"],
        "panda.log.JobBroker": ["cat7", "Job brokerage factory"],

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
    jediCat = ['cat1', 'cat2', 'cat3', 'cat4', 'cat5', 'cat6', 'cat7']

    panda_index = settings.OS_INDEX_PANDA_LOGS[:-1]+'-'
    jedi_index = settings.OS_INDEX_JEDI_LOGS[:-1]+'-'

    indices = [panda_index, jedi_index]

    panda = {}
    jedi = {}

    for index in indices:
        s = Search(using=connection, index=index + str(today))

        s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
            .bucket('type', 'terms', field='fields.type.keyword', size=1000) \
            .bucket('logLevel', 'terms', field='logLevel.keyword')

        res = s.execute()

        if index == panda_index:
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
        elif index == jedi_index:
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
        'pandadesc': pandaDesc,
        'jedi': jedi,
        'jedidesc': jediDesc,
        'time': time.strftime("%Y-%m-%d"),
    }

    if not is_json_request(request):
        response = render(request, 'esatlasPandaLogger.html', data, content_type='text/html')
        return response
    else:
        return JsonResponse({'panda': panda, 'pandadesc': pandaDesc, 'jedi': jedi, 'jedidesc': jediDesc})


@login_customrequired
def datasetInfo(request):
    valid, response = initRequest(request)
    if not valid:
        return response
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
        response = render(request, 'datasetInfo.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(dsrec, cls=DateEncoder), content_type='application/json')


@login_customrequired
def datasetList(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    setupView(request, hours=7 * 24, limit=999999999)
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
        query['containername'] = request.session['requestParams']['containername']
    if 'jeditaskid' in request.session['requestParams']:
        query['jeditaskid'] = int(request.session['requestParams']['jeditaskid'])

    dsets = []
    message = None
    if 'containername' in query or 'jeditaskid' in query:
        status = 200
        dsets.extend(JediDatasets.objects.filter(**query).extra(where=[wild_card_str]).values())
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())
    else:
        message = 'Neither containername nor jeditaskid provided. At least one of them is required.'
        status = 400

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
            'message': message,
        }
        data.update(getContextVariables(request))
        response = render(request, 'datasetList.html', data, content_type='text/html', status=status)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(json.dumps(dsets, cls=DateEncoder), content_type='application/json', status=status)


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

    if file or ('pandaid' in query and query['pandaid'] is not None) or (
            'jeditaskid' in query and query['jeditaskid'] is not None):
        files = JediDatasetContents.objects.filter(**query).values()
        if len(files) == 0:
            fquery = {k: v for k, v in query.items() if k != 'creationdate__castdate__range'}
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
        response = render(request, 'fileInfo.html', data, RequestContext(request))
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
    if not valid:
        return response

    setupView(request, hours=365 * 24, limit=999999999)
    query = {}
    files = []
    defaultlimit = 1000
    datasetname = ''
    datasetid = 0

    # It's dangerous when dataset name is not unique over table
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
        return error_response(request, message='No datasetid or datasetname was provided', status=400)

    extraparams = ''
    if 'procstatus' in request.session['requestParams'] and request.session['requestParams']['procstatus']:
        query['procstatus'] = request.session['requestParams']['procstatus']
        extraparams += '&procstatus=' + request.session['requestParams']['procstatus']

    nfilestotal = 0
    nfilesunique = 0
    if int(datasetid) > 0:
        query['datasetid'] = datasetid
        nfilestotal = JediDatasetContents.objects.filter(**query).count()
        nfilesunique = JediDatasetContents.objects.filter(**query).values('lfn').distinct().count()

    del request.session['TFIRST']
    del request.session['TLAST']
    if not is_json_request(request):
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
        response = render(request, 'fileList.html', data, content_type='text/html')
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
    fvalues = ('fileid', 'dispatchdblock', 'scope', 'destinationdblock')
    files_ft.extend(
        Filestable4.objects.filter(**query).extra(where=[extra_str]).values(*fvalues))
    if len(files_ft) == 0:
        files_ft.extend(
            FilestableArch.objects.filter(**query).extra(where=[extra_str]).values(*fvalues))
    if len(files_ft) > 0:
        for f in files_ft:
            files_ft_dict[f['fileid']] = f


    for f in files:
        f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
        if settings.RUCIO_UI_URL is not None and isinstance(settings.RUCIO_UI_URL, str) and len(settings.RUCIO_UI_URL) > 0:
            ruciolink_base = settings.RUCIO_UI_URL + 'did?scope='
            f['ruciolink'] = ''
            if f['fileid'] in files_ft_dict:
                name_param = ''
                if 'dispatchdblock' in files_ft_dict[f['fileid']] and len(files_ft_dict[f['fileid']]['dispatchdblock']) > 0:
                    name_param = 'dispatchdblock'
                elif 'destinationdblock' in files_ft_dict[f['fileid']] and len(files_ft_dict[f['fileid']]['destinationdblock']) > 0:
                    name_param = 'destinationdblock'
                if len(name_param) > 0:
                    if files_ft_dict[f['fileid']][name_param].startswith(files_ft_dict[f['fileid']]['scope']):
                        ruciolink_base += files_ft_dict[f['fileid']]['scope']
                    else:
                        ruciolink_base += files_ft_dict[f['fileid']][name_param].split('.')[0]
                    f['ruciolink'] = ruciolink_base + '&name=' + files_ft_dict[f['fileid']][name_param]
        else:
            f['ruciolink'] = ''
        f['creationdatecut'] = f['creationdate'].strftime('%Y-%m-%d')
        f['creationdate'] = f['creationdate'].strftime(settings.DATETIME_FORMAT)
        if f['endevent'] is not None and f['startevent'] is not None:
            f['end_start_nevents'] = int(f['endevent']) + 1 - int(f['startevent'])
        else:
            f['end_start_nevents'] = int(f['nevents']) if f['nevents'] is not None else 0

    dump = json.dumps(files, cls=DateEncoder)
    return HttpResponse(dump, content_type='application/json')


@login_customrequired
def workQueues(request):
    valid, response = initRequest(request)
    data = getCacheEntry(request, "workQueues")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'workQueues.html', data, content_type='text/html')
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
        response = render(request, 'workQueues.html', data, content_type='text/html')
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
    if (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0],
                                             "%Y-%m-%d %H:%M:%S")).days > 1) or \
            ((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
                                                 "%Y-%m-%d %H:%M:%S")).days > 1)):
        jobs.extend(
            Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[:request.session['JOB_LIMIT']].values(
                *values))

    if 'amitag' in request.session['requestParams']:

        tmpTableName = get_tmp_table_name()
        transactionKey = insert_to_temp_table([job['pandaid'] for job in jobs]) # Backend dependable
        new_cur = connection.cursor()
        new_cur.execute("""
            SELECT JOBPARAMETERS, PANDAID 
            FROM {}.JOBPARAMSTABLE 
            WHERE PANDAID in (SELECT ID FROM {} WHERE TRANSACTIONKEY={})
            """.format(settings.DB_SCHEMA_PANDA, tmpTableName, transactionKey))
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
        if job['metastruct']['executor'][0]['logfileReport']['countSummary']['FATAL'] > 0:
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
    else:
        server = '-'

    if 'HTTP_X_FORWARDED_FOR' in request.META:
        remote = request.META['HTTP_X_FORWARDED_FOR']
    else:
        remote = request.META['REMOTE_ADDR']

    if 'wsgi.url_scheme' in request.META:
        urlProto = request.META['wsgi.url_scheme']
    else:
        urlProto = 'http'

    if 'HTTP_X_FORWARDED_PROTO' in request.META:
        urlProto = request.META['HTTP_X_FORWARDED_PROTO']
    urlProto = str(urlProto) + "://"

    try:
        urls = urlProto + request.META['SERVER_NAME'] + request.META['REQUEST_URI']
    except:
        if 'SERVER_PORT' in request.META:
            port = ':' + request.META['SERVER_PORT']
        else:
            port = ''
        if 'PATH_INFO' in request.META:
            path = request.META['PATH_INFO']
        else:
            path = ''
        if 'QUERY_STRING' in request.META and request.META['QUERY_STRING'] != "":
            qstring = '?' + request.META['QUERY_STRING']
        else:
            qstring = ''
        urls = urlProto + request.META['SERVER_NAME'] + port + path + qstring
    _logger.debug(urls)

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
    response = render(request, '500.html', {}, context_instance=RequestContext(request))
    response.status_code = 500
    return response


def getBadEventsForTask(request):
    if 'jeditaskid' in request.GET:
        jeditaskid = int(request.GET['jeditaskid'])
    else:
        return error_response(request, message="Not jeditaskid supplied", status=400)

    mode = 'drop'
    if 'mode' in request.GET and request.GET['mode'] == 'nodrop':
        mode = 'nodrop'

    data = []
    cursor = connection.cursor()

    plsql = """
    select datasetid, error_code, 
        rtrim(xmlagg(xmlelement(e,def_min_eventid,',').extract('//text()') order by def_min_eventid).getclobval(),',') as bb,
        rtrim(xmlagg(xmlelement(e,pandaid,',').extract('//text()') order by pandaid).getclobval(),',') as pandaids, 
        count(*) 
    from {}.jedi_events 
    where jeditaskid={} and attemptnr = 1 group by datasetid, error_code 
    """.format(settings.DB_SCHEMA_PANDA, jeditaskid)

    if mode == 'drop':
        plsql = """
        select datasetid, error_code, 
            rtrim(xmlagg(xmlelement(e,def_min_eventid,',').extract('//text()') order by def_min_eventid).getclobval(),',') as bb, 
            rtrim(xmlagg(xmlelement(e,pandaid,',').extract('//text()') order by pandaid).getclobval(),',') as pandaids,
            count(*) 
        from {}.jedi_events 
        where jeditaskid={} and attemptnr = 1 and pandaid in (
            select pandaid 
            from {}.jedi_dataset_contents 
            where jeditaskid={} and type in ('input', 'pseudo_input')
            )
        group by datasetid, error_code 
        """.format(settings.DB_SCHEMA_PANDA, jeditaskid, settings.DB_SCHEMA_PANDA, jeditaskid)

    cursor.execute(plsql)
    evtable = cursor.fetchall()
    errorCodes = get_job_error_desc()
    for row in evtable:
        dataitem = {}
        dataitem['DATASETID'] = row[0]
        dataitem['ERROR_CODE'] = "{} ({})".format(
            errorCodes['piloterrorcode'][row[1]] if row[1] in errorCodes['piloterrorcode'] else '',
            str(row[1])
        )
        dataitem['EVENTS'] = list(set(str(row[2].read()).split(','))) if not row[2] is None else None
        dataitem['PANDAIDS'] = list(set(str(row[3].read()).split(','))) if not row[3] is None else None
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

    sqlRequest = """
    select oldpandaid, newpandaid, max(lev) as lev, min(pth) as pth 
    from (
        select oldpandaid, newpandaid, level as lev, connect_by_isleaf as il, sys_connect_by_path(oldpandaid, ',') pth 
        from (
            select oldpandaid, newpandaid 
            from {}.jedi_job_retry_history 
            where jeditaskid={} and relationtype='jobset_retry'
            ) t1 
        connect by oldpandaid=prior newpandaid
    ) t2 
    group by oldpandaid, newpandaid
    """.format(settings.DB_SCHEMA_PANDA, str(jeditaskid))

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


def getTaskDataMovementData(request, jeditaskid=None):
    """
    Getting information of volume of input data has been moved initiated by PanDA
    :param request: request
    :param jeditaskid: int
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    data_movement_info = {}

    if not jeditaskid:
        return JsonResponse(data={}, status=400)

    query_str = """
    with a as (
    select pandaid,dispatchdblock from atlas_panda.jobsarchived4 where jeditaskid=:jeditaskid and dispatchdblock is not null 
    union 
    select pandaid,dispatchdblock from atlas_pandaarch.jobsarchived where jeditaskid=:jeditaskid and dispatchdblock is not null
    ) 
    select count(*) as n, count(distinct lfn) as nfiles, count(distinct pandaid) as njobs, sum(fsize) fsize_sum from (
        select f.pandaid, f.lfn, fsize from atlas_panda.filestable4 f, a 
            where jeditaskid=:jeditaskid  and f.pandaid=a.pandaid and f.dispatchdblock=a.dispatchdblock 
        union 
        select f.pandaid, f.lfn, fsize from atlas_pandaarch.filestable_arch f, a 
            where jeditaskid=:jeditaskid  and f.pandaid=a.pandaid and f.dispatchdblock=a.dispatchdblock
    )
    """
    cur = connection.cursor()
    cur.execute(query_str, {'jeditaskid': jeditaskid})
    result = cur.fetchall()
    cur.close()

    if len(result) > 0 and result[0][0] > 0:
        data_movement_info['n'] = result[0][0]
        data_movement_info['nfiles'] = result[0][1]
        data_movement_info['njobs'] = result[0][2]
        data_movement_info['fsize_sum'] = round(convert_bytes(result[0][3], 'GB'), 2) if result[0][3] is not None else 0

    return JsonResponse(data_movement_info)


@never_cache
def getJobStatusLog(request, pandaid=None):
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
            if c < len(statusLog) - 1:
                if statusLog[c + 1][mtimeparam] is not None and statusLog[c][mtimeparam] is not None:
                    duration = statusLog[c + 1][mtimeparam] - statusLog[c][mtimeparam]
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
        response = render(request, 'jobStatusLog.html', {'statusLog': statusLog}, content_type='text/html')
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
    if not valid:
        return response

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
            if c < len(statusLog) - 1:
                if statusLog[c + 1][mtimeparam] is not None and statusLog[c][mtimeparam] is not None:
                    duration = statusLog[c + 1][mtimeparam] - statusLog[c][mtimeparam]
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
        response = render(request, 'taskStatusLog.html', {'statusLog': statusLog}, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@never_cache
def getTaskLogs(request, jeditaskid=None):
    """
    A view to asynchronously load task logs from OpenSearch storage
    :param request:
    :param jeditaskid:
    :return: json
    """
    valid, response = initRequest(request)
    if not valid:
        return response

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
    if request.headers.get('x-requested-with') and 'XMLHttpRequest' in request.headers.get('x-requested-with'):
        try:
            q = request.GET.get('term', '')
            pq_dict = get_panda_queues()
            results = []
            for pq_name in pq_dict:
                if q.lower() in pq_name.lower():
                    results.append(pq_name)
            data = json.dumps(results)
        except:
            data = 'fail'
            HttpResponse(data, 'application/json', status=204)
    else:
        data = 'fail'
        HttpResponse(data, 'application/json', status=406)
    return HttpResponse(data, 'application/json')


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

    jvalues = ['pilottiming', ]
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
    if timerange[0] < datetime.now(tz=timezone.utc) - timedelta(days=4) and timerange[1] < datetime.now(tz=timezone.utc) - timedelta(days=4):
        is_archive_only = True
    if timerange[0] < datetime.now(tz=timezone.utc)- timedelta(days=3):
        is_archive = True

    if not is_archive_only:
        jobs.extend(
            Jobsdefined4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
        jobs.extend(Jobsactive4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
        jobs.extend(
            Jobswaiting4.objects.filter(**excluded_time_query).extra(where=[wildCardExtension]).values(*jvalues))
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
        test['inputfilesizemb'] = round(job['inputfilesize'] / 1000000., 2) if 'inputfilesize' in job and isinstance(
            job['inputfilesize'], int) else None

        wallclocktime = get_job_walltime(job)
        queuetime = get_job_queuetime(job)

        if wallclocktime is not None:
            test['wallclocktime'] = wallclocktime
            if wallclocktime > 0:
                test['cpuefficiency'] = round(float(job['cpuconsumptiontime']) / test['wallclocktime'], 3)
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
    A view to asynchronously load pilot logs from OpenSearch storage by pandaid or taskid
    :param request:
    :param id:
    :return: json
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    connection = create_os_connection()
    mode = 'pandaid'

    log_content = {}
    if request.POST and "pandaid" in request.POST:
        try:
            id = int(request.POST['pandaid'])
            start_var = int(request.POST['start'])
            length_var = int(request.POST['length'])
        except:
            HttpResponse(status=404, content_type='text/html')
    else:
        HttpResponse(status=404, content_type='text/html')

    if request.POST and "order[0][dir]" in request.POST:
        sort = request.POST['order[0][dir]']
    else:
        sort = request.POST['sort']
    if request.POST and "search[value]" in request.POST:
        search_string = request.POST['search[value]']
    else:
        search_string = request.POST['search']

    pilot_logs_index = settings.OS_INDEX_PILOT_LOGS

    payloadlog, job_running_flag, total = get_payloadlog(
        id,
        connection,
        pilot_logs_index,
        start=start_var,
        length=length_var,
        mode=mode,
        sort=sort,
        search_string=search_string
    )

    log_content['payloadlog'] = payloadlog
    log_content['flag'] = job_running_flag
    log_content['recordsTotal'] = total
    log_content['recordsFiltered'] = total
    if request.POST and "draw" in request.POST:
        log_content['draw'] = request.POST['draw']
    else:
        log_content['draw'] = 0
    response = HttpResponse(json.dumps(log_content, cls=DateEncoder), content_type='application/json')

    return response
