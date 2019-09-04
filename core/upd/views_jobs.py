"""
Created on 28.08.2019
:author Tatiana Korchuganova
A set of core views updated during interface refactoring.
The main change is portioned delivery of information.
"""

import json, logging
from datetime import datetime

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers

from core.upd.init_view import login_customrequired, init_request, setup_view
from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.self_monitor import end_self_monitor
# from core.libs.exlib import DateEncoder
from core.libs.exlib import produce_objects_sample, make_timestamp_hist, is_eventservice_request
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table
from core.libs.url import remove_param, extensible_url

from core.common.models import Jobparamstable

from core.settings.local import dbaccess, defaultDatetimeFormat

_logger_error = logging.getLogger('bigpandamon-error')
_logger_info = logging.getLogger('bigpandamon')


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)

@login_customrequired
def job_list(request, mode=None):
    """
    A view for list of jobs
    :param request:
    :param mode: drop or nodrop
    :return:
    """
    # check request query for validity
    valid, response = init_request(request)
    if not valid:
        return response

    # Here we try to get data from cache
    data = getCacheEntry(request, "jobList")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        if data['eventservice'] == True:
            response = render_to_response('jobListES.html', data, content_type='text/html')
        else:
            response = render_to_response('jobList.html', data, content_type='text/html')
        end_self_monitor(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    warning = {}
    eventservice = is_eventservice_request(request)

    if 'dump' in request.session['requestParams'] and request.session['requestParams']['dump'] == 'parameters':
        return job_param_list(request)

    noarchjobs = False
    if ('noarchjobs' in request.session['requestParams'] and request.session['requestParams']['noarchjobs'] == '1'):
        noarchjobs = True

    query, wild_card_extension, LAST_N_HOURS_MAX = setup_view(request, wildCardExt=True)

    # insert dropped jobs im tmp table  if dropping is needed
    dropmode = False
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'drop': dropmode = True
    if 'mode' in request.session['requestParams'] and request.session['requestParams'][
        'mode'] == 'nodrop': dropmode = False

    if dropmode:
        wild_card_extension, dtkey = insert_dropped_jobs_to_tmp_table(query, wild_card_extension)

    # insert suitable pandaids to temporary table for further use

    timestamps, jkey, wild_card_extension = produce_objects_sample('job', query, wild_card_extension)

    njobs = len(timestamps)

    timestamp_hist = make_timestamp_hist(timestamps)

    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
            'json' not in request.session['requestParams'])):
        xurl = extensible_url(request)
        time_locked_url = remove_param(remove_param(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
                          'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
                          '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
        nodurminurl = remove_param(xurl, 'durationmin', mode='extensible')
        xurl = remove_param(xurl, 'mode', mode='extensible')

        TFIRST = request.session['TFIRST'].strftime(defaultDatetimeFormat)
        TLAST = request.session['TLAST'].strftime(defaultDatetimeFormat)
        del request.session['TFIRST']
        del request.session['TLAST']

        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'timestamps': timestamps,
            'timerange': [TFIRST, TLAST],
            'eventservice': eventservice,
            'njobs': njobs,
            'timestamp_hist': timestamp_hist,
            'xurl': xurl,
            'time_locked_url': time_locked_url,
            'warning': warning,
            'built': datetime.now().strftime("%H:%M:%S"),
        }

        setCacheEntry(request, "job_list_init", json.dumps(data, cls=DateEncoder), 60 * 20)
        ##self monitor
        end_self_monitor(request)
        response = render_to_response('job_list_init.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        del request.session['TFIRST']
        del request.session['TLAST']

        # TODO loading the all necessary data that is being asynchroniously dilivered to the page
        jobs = []
        sumd = {}
        errsByCount = []
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
        ##self monitor
        end_self_monitor(request)
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response



#
# def get_arrtibute_summary(request):
#     data = {}
#
#
#
#
#     job_attr_values = (
#         'actualcorecount',
#         'atlasrelease',
#         'attemptnr',
#         'cloud',
#         'computingsite',
#         'corecount',
#         'eventservice',
#         'gshare',
#         'homepackage',
#         'inputfileproject',
#         'inputfiletype',
#         'jeditaskid',
#         'jobstatus',
#         'jobsubstatus',
#         'noutputdatafiles',
#         'nucleus',
#         'currentpriority',
#         'processingtype',
#         'prodsourcelabel',
#         'produsername',
#         'reqid',
#         'resourcetype',
#         'transformation',
#         'workinggroup',
#     )
#
#     job_error_attr_values = (
#         'brokerageerrorcode',
#         'brokerageerrordiag',
#         'ddmerrorcode',
#         'ddmerrordiag',
#         'exeerrorcode',
#         'exeerrordiag',
#         'jobdispatchererrorcode',
#         'jobdispatchererrordiag',
#         'piloterrorcode',
#         'piloterrordiag',
#         'superrorcode',
#         'superrordiag',
#         'taskbuffererrorcode',
#         'taskbuffererrordiag',
#         'transexitcode'
#     )
#
#     job_extra_attr_values = (
#         'durationmin',
#         'eventservicestatus',
#         'harvesterinstance',
#         'minramcount',
#         'outputfiletype',
#         'pilotversion',
#     )
#
#     try:
#         from concurrent.futures import ThreadPoolExecutor
#     except ImportError:
#         _logger_error.error('[job_list] failed to import library')
#
#     attributes_summary_raw = {}
#     inputs_list = []
#     for value in job_attr_values:
#         attributes_summary_raw[value] = QueryThread(
#             model_name=[Jobsarchived4, Jobsactive4, Jobswaiting4, Jobsdefined4],
#             query=query,
#             wild_card_extension=wild_card_extension,
#             tkey=jkey,
#             param_name=value)
#     for value in attributes_summary_raw.keys():
#         inputs_list.append({'QueryThread_instances_dict': attributes_summary_raw,
#                             'param_name': value,
#                             'aggregation_type': 'count_distinct'})
#
#
#     N_MAX_CONCURRENT_QUERIES = 10
#     with ThreadPoolExecutor(max_workers=N_MAX_CONCURRENT_QUERIES) as executor:
#         executor.map(run_query_in_thread, inputs_list)
#
#     attributes_summary = extract_results_list(attributes_summary_raw, jkey)
#
#
#     JOB_LIMITS = request.session['JOB_LIMIT']
#     totalJobs = 0
#     showTop = 0
#
#     if 'limit' in request.session['requestParams']:
#         request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
#
#     droppedList = []
#     if request.user.is_authenticated and request.user.is_tester:
#         taskids = {}
#         tk = 0
#         if 'eventservice' in request.session['requestParams']:
#             isEventTask = True
#             print('Event Service!')
#         else:
#             isEventTask = False
#         if 'jeditaskid' in request.session['requestParams']:
#             taskids[request.session['requestParams']['jeditaskid']] = 1
#
#         # isReturnDroppedPMerge = False
#         # if 'processingtype' in request.session['requestParams'] and \
#         #         request.session['requestParams']['processingtype'] == 'pmerge': isReturnDroppedPMerge = True
#         # isJumbo = False
#         # if dropmode and (len(taskids) == 1) and 'eventservice' in request.session['requestParams']:
#         #     if request.session['requestParams']['eventservice'] != '4' and request.session['requestParams'][
#         #         'eventservice'] != 'jumbo':
#         #         tk, droppedList, wildCardExtension = dropalgorithm.dropRetrielsJobs(list(taskids.keys())[0],
#         #                                                                             wildCardExtension, isEventTask)
#         #     else:
#         #         isJumbo = True
#     jobs = []
#     values = job_attr_values
#     harvesterjobstatus = ''
#
#     from core.harvester.views import getHarvesterJobs
#
#     if 'jobstatus' in request.session['requestParams']:
#         harvesterjobstatus = request.session['requestParams']['jobstatus']
#     if 'transferringnotupdated' in request.session['requestParams']:
#         jobs = stateNotUpdated(request, state='transferring', values=values, wildCardExtension=wildCardExtension)
#     elif 'statenotupdated' in request.session['requestParams']:
#         jobs = stateNotUpdated(request, values=values, wildCardExtension=wildCardExtension)
#     elif 'harvesterinstance' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
#         jobs = getHarvesterJobs(request, instance=request.session['requestParams']['harvesterinstance'],
#                                 workerid=request.session['requestParams']['workerid'], jobstatus=harvesterjobstatus)
#     elif 'harvesterid' in request.session['requestParams'] and 'workerid' in request.session['requestParams']:
#         jobs = getHarvesterJobs(request, instance=request.session['requestParams']['harvesterid'],
#                                 workerid=request.session['requestParams']['workerid'], jobstatus=harvesterjobstatus)
#     elif ('harvesterinstance' not in request.session['requestParams'] and 'harvesterid' not in request.session[
#         'requestParams']) and 'workerid' in request.session['requestParams']:
#         jobs = getHarvesterJobs(request, workerid=request.session['requestParams']['workerid'],
#                                 jobstatus=harvesterjobstatus)
#     else:
#         excludedTimeQuery = copy.deepcopy(query)
#         if ('modificationtime__castdate__range' in excludedTimeQuery and not 'date_to' in request.session[
#             'requestParams']):
#             del excludedTimeQuery['modificationtime__castdate__range']
#         jobs.extend(Jobsdefined4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
#                     :request.session['JOB_LIMIT']].values(*values))
#         jobs.extend(Jobsactive4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
#                     :request.session['JOB_LIMIT']].values(*values))
#         jobs.extend(Jobswaiting4.objects.filter(**excludedTimeQuery).extra(where=[wildCardExtension])[
#                     :request.session['JOB_LIMIT']].values(*values))
#         jobs.extend(Jobsarchived4.objects.filter(**query).extra(where=[wildCardExtension])[
#                     :request.session['JOB_LIMIT']].values(*values))
#         listJobs = [Jobsarchived4, Jobsactive4, Jobswaiting4, Jobsdefined4]
#         if not noarchjobs:
#             queryFrozenStates = []
#             if 'jobstatus' in request.session['requestParams']:
#                 if isEventTask:
#                     queryFrozenStates = list(
#                         filter(set(request.session['requestParams']['jobstatus'].split('|')).__contains__,
#                                ['finished', 'failed', 'cancelled', 'closed', 'merging']))
#                 else:
#                     queryFrozenStates = list(
#                         filter(set(request.session['requestParams']['jobstatus'].split('|')).__contains__,
#                                ['finished', 'failed', 'cancelled', 'closed']))
#             ##hard limit is set to 2K
#             if ('jobstatus' not in request.session['requestParams'] or len(queryFrozenStates) > 0):
#
#                 if ('limit' not in request.session['requestParams'] and 'jeditaskid' not in request.session[
#                     'requestParams']):
#                     request.session['JOB_LIMIT'] = 20000
#                     JOB_LIMITS = 20000
#                     showTop = 1
#                 elif ('limit' not in request.session['requestParams'] and 'jeditaskid' in request.session[
#                     'requestParams']):
#                     request.session['JOB_LIMIT'] = 200000
#                     JOB_LIMITS = 200000
#                 else:
#                     request.session['JOB_LIMIT'] = int(request.session['requestParams']['limit'])
#                     JOB_LIMITS = int(request.session['requestParams']['limit'])
#                 if (((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][0],
#                                                          "%Y-%m-%d %H:%M:%S")).days > 1) or \
#                         ((datetime.now() - datetime.strptime(query['modificationtime__castdate__range'][1],
#                                                              "%Y-%m-%d %H:%M:%S")).days > 1)):
#                     if 'jeditaskid' in request.session['requestParams'] and 'json' in request.session['requestParams'] \
#                             and ('fulllist' in request.session['requestParams'] and
#                                  request.session['requestParams']['fulllist'] == 'true'):
#                         del query['modificationtime__castdate__range']
#                     archJobs = Jobsarchived.objects.filter(**query).extra(where=[wildCardExtension])[
#                                :request.session['JOB_LIMIT']].values(*values)
#                     listJobs.append(Jobsarchived)
#                     totalJobs = len(archJobs)
#                     jobs.extend(archJobs)
#         if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
#                 'json' not in request.session['requestParams'])):
#             thread = Thread(target=totalCount, args=(listJobs, query, wildCardExtension, dkey))
#             thread.start()
#         else:
#             thread = None
#
#     ## If the list is for a particular JEDI task, filter out the jobs superseded by retries
#     taskids = {}
#
#     for job in jobs:
#         if 'jeditaskid' in job: taskids[job['jeditaskid']] = 1
#     dropmode = True
#     if 'mode' in request.session['requestParams'] and request.session['requestParams'][
#         'mode'] == 'drop': dropmode = True
#     if 'mode' in request.session['requestParams'] and request.session['requestParams'][
#         'mode'] == 'nodrop': dropmode = False
#     isReturnDroppedPMerge = False
#     if 'processingtype' in request.session['requestParams'] and \
#             request.session['requestParams']['processingtype'] == 'pmerge': isReturnDroppedPMerge = True
#     droplist = []
#     newdroplist = []
#     droppedPmerge = set()
#     newdroppedPmerge = set()
#     cntStatus = []
#     newjobs = copy.deepcopy(jobs)
#     if dropmode and (len(taskids) == 1):
#         start = time.time()
#         jobs, droplist, droppedPmerge = dropRetrielsJobs(jobs, list(taskids.keys())[0], isReturnDroppedPMerge)
#         end = time.time()
#         print(end - start)
#         if request.user.is_authenticated and request.user.is_tester:
#             if 'eventservice' in request.session['requestParams']:
#                 isEventTask = True
#                 print('Event Service!')
#             else:
#                 isEventTask = False
#             start = time.time()
#             if isJumbo == False:
#                 newjobs, newdroppedPmerge, newdroplist = dropalgorithm.clearDropRetrielsJobs(tk=tk,
#                                                                                              droplist=droppedList,
#                                                                                              jobs=newjobs,
#                                                                                              isEventTask=isEventTask,
#                                                                                              isReturnDroppedPMerge=isReturnDroppedPMerge)
#             end = time.time()
#             print(end - start)
#
#     # get attemps of file if fileid in request params
#     files_attempts_dict = {}
#     files_attempts = []
#     if fileid:
#         if fileid and jeditaskid and datasetid:
#             fquery = {}
#             fquery['pandaid__in'] = [job['pandaid'] for job in jobs if len(jobs) > 0]
#             fquery['fileid'] = fileid
#             files_attempts.extend(Filestable4.objects.filter(**fquery).values('pandaid', 'attemptnr'))
#             files_attempts.extend(FilestableArch.objects.filter(**fquery).values('pandaid', 'attemptnr'))
#             if len(files_attempts) > 0:
#                 files_attempts_dict = dict(
#                     zip([f['pandaid'] for f in files_attempts], [ff['attemptnr'] for ff in files_attempts]))
#
#             jfquery = {'jeditaskid': jeditaskid, 'datasetid': datasetid, 'fileid': fileid}
#             jedi_file = JediDatasetContents.objects.filter(**jfquery).values('attemptnr', 'maxattempt', 'failedattempt',
#                                                                              'maxfailure')
#             if jedi_file and len(jedi_file) > 0:
#                 jedi_file = jedi_file[0]
#             if len(files_attempts_dict) > 0:
#                 for job in jobs:
#                     if job['pandaid'] in files_attempts_dict:
#                         job['fileattemptnr'] = files_attempts_dict[job['pandaid']]
#                     else:
#                         job['fileattemptnr'] = None
#                     if jedi_file and 'maxattempt' in jedi_file:
#                         job['filemaxattempts'] = jedi_file['maxattempt']
#
#     jobs = cleanJobList(request, jobs)
#     jobs = reconstructJobsConsumers(jobs)
#
#     njobs = len(jobs)
#     jobtype = ''
#     if 'jobtype' in request.session['requestParams']:
#         jobtype = request.session['requestParams']['jobtype']
#     elif '/analysis' in request.path:
#         jobtype = 'analysis'
#     elif '/production' in request.path:
#         jobtype = 'production'
#
#     if u'display_limit' in request.session['requestParams']:
#         if int(request.session['requestParams']['display_limit']) > njobs:
#             display_limit = njobs
#         else:
#             display_limit = int(request.session['requestParams']['display_limit'])
#         url_nolimit = removeParam(request.get_full_path(), 'display_limit')
#     else:
#         display_limit = 1000
#         url_nolimit = request.get_full_path()
#     njobsmax = display_limit
#
#     if 'sortby' in request.session['requestParams']:
#         sortby = request.session['requestParams']['sortby']
#
#         if sortby == 'time-ascending':
#             jobs = sorted(jobs,
#                           key=lambda x: x['modificationtime'] if not x['modificationtime'] is None else datetime(1900,
#                                                                                                                  1, 1))
#         if sortby == 'time-descending':
#             jobs = sorted(jobs,
#                           key=lambda x: x['modificationtime'] if not x['modificationtime'] is None else datetime(1900,
#                                                                                                                  1, 1),
#                           reverse=True)
#         if sortby == 'statetime':
#             jobs = sorted(jobs,
#                           key=lambda x: x['statechangetime'] if not x['statechangetime'] is None else datetime(1900, 1,
#                                                                                                                1),
#                           reverse=True)
#         elif sortby == 'priority':
#             jobs = sorted(jobs, key=lambda x: x['currentpriority'] if not x['currentpriority'] is None else 0,
#                           reverse=True)
#         elif sortby == 'attemptnr':
#             jobs = sorted(jobs, key=lambda x: x['attemptnr'], reverse=True)
#         elif sortby == 'duration-ascending':
#             jobs = sorted(jobs, key=lambda x: x['durationsec'])
#         elif sortby == 'duration-descending':
#             jobs = sorted(jobs, key=lambda x: x['durationsec'], reverse=True)
#         elif sortby == 'duration':
#             jobs = sorted(jobs, key=lambda x: x['durationsec'])
#         elif sortby == 'PandaID':
#             jobs = sorted(jobs, key=lambda x: x['pandaid'], reverse=True)
#     elif fileid:
#         sortby = "fileattemptnr-descending"
#         jobs = sorted(jobs, key=lambda x: x['fileattemptnr'], reverse=True)
#     else:
#         sortby = "attemptnr-descending,pandaid-descending"
#         jobs = sorted(jobs, key=lambda x: [-x['attemptnr'], -x['pandaid']])
#
#     taskname = ''
#     if 'jeditaskid' in request.session['requestParams']:
#         taskname = getTaskName('jeditaskid', request.session['requestParams']['jeditaskid'])
#     if 'taskid' in request.session['requestParams']:
#         taskname = getTaskName('jeditaskid', request.session['requestParams']['taskid'])
#
#     if 'produsername' in request.session['requestParams']:
#         user = request.session['requestParams']['produsername']
#     elif 'user' in request.session['requestParams']:
#         user = request.session['requestParams']['user']
#     else:
#         user = None
#
#     ## set up google flow diagram
#     flowstruct = buildGoogleFlowDiagram(request, jobs=jobs)
#
#     if ('datasets' in request.session['requestParams']) and (
#             request.session['requestParams']['datasets'] == 'yes') and ((
#                                                                                 ('HTTP_ACCEPT' in request.META) and (
#                                                                                 request.META.get('HTTP_ACCEPT') in (
#                                                                         'text/json', 'application/json'))) or (
#                                                                                 'json' in request.session[
#                                                                             'requestParams'])):
#         for job in jobs:
#             files = []
#             pandaid = job['pandaid']
#             files.extend(JediDatasetContents.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
#             ninput = 0
#
#             dsquery = Q()
#             counter = 0
#             if len(files) > 0:
#                 for f in files:
#                     if f['type'] == 'input': ninput += 1
#                     f['fsizemb'] = "%0.2f" % (f['fsize'] / 1000000.)
#
#                     f['DSQuery'] = {'jeditaskid': job['jeditaskid'], 'datasetid': f['datasetid']}
#                     dsquery = dsquery | Q(Q(jeditaskid=job['jeditaskid']) & Q(datasetid=f['datasetid']))
#                     counter += 1
#                     if counter == 30:
#                         break
#
#                 dsets = JediDatasets.objects.filter(dsquery).extra(
#                     select={"dummy1": '/*+ INDEX_RS_ASC(ds JEDI_DATASETS_PK) */ 1 '}).values()
#                 if len(dsets) > 0:
#                     for ds in dsets:
#                         for file in files:
#                             if 'DSQuery' in file and file['DSQuery']['jeditaskid'] == ds['jeditaskid'] and \
#                                     file['DSQuery']['datasetid'] == ds['datasetid']:
#                                 file['dataset'] = ds['datasetname']
#                                 del file['DSQuery']
#
#                     # dsets = JediDatasets.objects.filter(jeditaskid=job['jeditaskid'], datasetid=f['datasetid']).extra(select={"dummy1" : '/*+ INDEX_RS_ASC(ds JEDI_DATASETS_PK) */ 1 '}).values()
#                     # if len(dsets) > 0:
#                     #    f['datasetname'] = dsets[0]['datasetname']
#
#             if True:
#                 # if ninput == 0:
#                 files.extend(Filestable4.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
#                 if len(files) == 0:
#                     files.extend(FilestableArch.objects.filter(jeditaskid=job['jeditaskid'], pandaid=pandaid).values())
#                 if len(files) > 0:
#                     for f in files:
#                         if 'creationdate' not in f: f['creationdate'] = f['modificationtime']
#                         if 'fileid' not in f: f['fileid'] = f['row_id']
#                         if 'datasetname' not in f and 'dataset' in f: f['datasetname'] = f['dataset']
#                         if 'modificationtime' in f: f['oldfiletable'] = 1
#                         if 'destinationdblock' in f and f['destinationdblock'] is not None:
#                             f['destinationdblock_vis'] = f['destinationdblock'].split('_')[-1]
#             files = sorted(files, key=lambda x: x['type'])
#             nfiles = len(files)
#             logfile = {}
#             for file in files:
#                 if file['type'] == 'log':
#                     logfile['lfn'] = file['lfn']
#                     logfile['guid'] = file['guid']
#                     if 'destinationse' in file:
#                         logfile['site'] = file['destinationse']
#                     else:
#                         logfilerec = Filestable4.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
#                         if len(logfilerec) == 0:
#                             logfilerec = FilestableArch.objects.filter(pandaid=pandaid, lfn=logfile['lfn']).values()
#                         if len(logfilerec) > 0:
#                             logfile['site'] = logfilerec[0]['destinationse']
#                             logfile['guid'] = logfilerec[0]['guid']
#                     logfile['scope'] = file['scope']
#                 file['fsize'] = int(file['fsize'] / 1000000)
#             job['datasets'] = files
#
#     # show warning or not
#     if njobs <= request.session['JOB_LIMIT']:
#         showwarn = 0
#     else:
#         showwarn = 1
#
#     # Sort in order to see the most important tasks
#     sumd, esjobdict = jobSummaryDict(request, jobs,
#                                      standard_fields + ['corecount', 'noutputdatafiles', 'actualcorecount',
#                                                         'schedulerid', 'pilotversion'])
#     if sumd:
#         for item in sumd:
#             if item['field'] == 'jeditaskid':
#                 item['list'] = sorted(item['list'], key=lambda k: k['kvalue'], reverse=True)
#
#     if 'jeditaskid' in request.session['requestParams']:
#         if len(jobs) > 0:
#             for job in jobs:
#                 if 'maxvmem' in job:
#                     if type(job['maxvmem']) is int and job['maxvmem'] > 0:
#                         job['maxvmemmb'] = "%0.2f" % (job['maxvmem'] / 1000.)
#                         job['avgvmemmb'] = "%0.2f" % (job['avgvmem'] / 1000.)
#                 if 'maxpss' in job:
#                     if type(job['maxpss']) is int and job['maxpss'] > 0:
#                         job['maxpss'] = "%0.2f" % (job['maxpss'] / 1024.)
#
#     testjobs = False
#     if 'prodsourcelabel' in request.session['requestParams'] and request.session['requestParams'][
#         'prodsourcelabel'].lower().find('test') >= 0:
#         testjobs = True
#     tasknamedict = taskNameDict(jobs)
#     errsByCount, errsBySite, errsByUser, errsByTask, errdSumd, errHist = errorSummaryDict(request, jobs, tasknamedict,
#                                                                                           testjobs)
#
#     # Here we getting extended data for site
#     jobsToShow = jobs[:njobsmax]
#     from core.libs import exlib
#     try:
#         jobsToShow = exlib.fileList(jobsToShow)
#     except Exception as e:
#         logger = logging.getLogger('bigpandamon-error')
#         logger.error(e)
#     ###RESERVE
#     distinctComputingSites = []
#     for job in jobsToShow:
#         distinctComputingSites.append(job['computingsite'])
#     distinctComputingSites = list(set(distinctComputingSites))
#     query = {}
#     query['siteid__in'] = distinctComputingSites
#     siteres = Schedconfig.objects.filter(**query).exclude(cloud='CMS').extra().values('siteid', 'status',
#                                                                                       'comment_field')
#     siteHash = {}
#     for site in siteres:
#         siteHash[site['siteid']] = (site['status'], site['comment_field'])
#     for job in jobsToShow:
#         if job['computingsite'] in siteHash.keys():
#             job['computingsitestatus'] = siteHash[job['computingsite']][0]
#             job['computingsitecomment'] = siteHash[job['computingsite']][1]
#     if thread != None:
#         try:
#             thread.join()
#             jobsTotalCount = sum(tcount[dkey])
#             print(dkey)
#             print(tcount[dkey])
#             del tcount[dkey]
#             print(tcount)
#             print(jobsTotalCount)
#         except:
#             jobsTotalCount = -1
#     else:
#         jobsTotalCount = -1
#
#     listPar = []
#     for key, val in request.session['requestParams'].items():
#         if (key != 'limit' and key != 'display_limit'):
#             listPar.append(key + '=' + str(val))
#     if len(listPar) > 0:
#         urlParametrs = '&'.join(listPar) + '&'
#     else:
#         urlParametrs = None
#     print(listPar)
#     del listPar
#     if (math.fabs(njobs - jobsTotalCount) < 1000 or jobsTotalCount == -1):
#         jobsTotalCount = None
#     else:
#         jobsTotalCount = int(math.ceil((jobsTotalCount + 10000) / 10000) * 10000)
#
#     for job in jobsToShow:
#         if job['creationtime']:
#             job['creationtime'] = job['creationtime'].strftime(defaultDatetimeFormat)
#         if job['modificationtime']:
#             job['modificationtime'] = job['modificationtime'].strftime(defaultDatetimeFormat)
#         if job['statechangetime']:
#             job['statechangetime'] = job['statechangetime'].strftime(defaultDatetimeFormat)
#
#     isincomparisonlist = False
#     clist = []
#     if request.user.is_authenticated and request.user.is_tester:
#
#         cquery = {}
#         cquery['object'] = 'job'
#         cquery['userid'] = request.user.id
#         try:
#             jobsComparisonList = ObjectsComparison.objects.get(**cquery)
#         except ObjectsComparison.DoesNotExist:
#             jobsComparisonList = None
#
#         if jobsComparisonList:
#             try:
#                 clist = json.loads(jobsComparisonList.comparisonlist)
#                 newlist = []
#                 for ce in clist:
#                     try:
#                         ceint = int(ce)
#                         newlist.append(ceint)
#                     except:
#                         pass
#                 clist = newlist
#             except:
#                 clist = []
#
#     if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
#             'json' not in request.session['requestParams'])):
#
#         xurl = extensibleURL(request)
#         time_locked_url = removeParam(removeParam(xurl, 'date_from', mode='extensible'), 'date_to', mode='extensible') + \
#                           'date_from=' + request.session['TFIRST'].strftime('%Y-%m-%dT%H:%M') + \
#                           '&date_to=' + request.session['TLAST'].strftime('%Y-%m-%dT%H:%M')
#         nodurminurl = removeParam(xurl, 'durationmin', mode='extensible')
#         print(xurl)
#         nosorturl = removeParam(xurl, 'sortby', mode='extensible')
#         nosorturl = removeParam(nosorturl, 'display_limit', mode='extensible')
#         # nosorturl = removeParam(nosorturl, 'harvesterinstance', mode='extensible')
#         xurl = removeParam(nosorturl, 'mode', mode='extensible')
#
#         TFIRST = request.session['TFIRST'].strftime(defaultDatetimeFormat)
#         TLAST = request.session['TLAST'].strftime(defaultDatetimeFormat)
#         del request.session['TFIRST']
#         del request.session['TLAST']
#         errsByCount = importToken(request, errsByCount=errsByCount)
#         nodropPartURL = cleanURLFromDropPart(xurl)
#         difDropList = dropalgorithm.compareDropAlgorithm(droplist, newdroplist)
#         data = {
#             'prefix': getPrefix(request),
#             'errsByCount': errsByCount,
#             'errdSumd': errdSumd,
#             'request': request,
#             'viewParams': request.session['viewParams'],
#             'requestParams': request.session['requestParams'],
#             'jobList': jobsToShow,
#             'jobtype': jobtype,
#             'njobs': njobs,
#             'user': user,
#             'sumd': sumd,
#             'xurl': xurl,
#             'xurlnopref': xurl[5:],
#             'droplist': droplist,
#             'ndrops': len(droplist) if len(droplist) > 0 else (- len(droppedPmerge)),
#             'tfirst': TFIRST,
#             'tlast': TLAST,
#             'plow': PLOW,
#             'phigh': PHIGH,
#             'showwarn': showwarn,
#             'joblimit': request.session['JOB_LIMIT'],
#             'limit': JOB_LIMITS,
#             'totalJobs': totalJobs,
#             'showTop': showTop,
#             'url_nolimit': url_nolimit,
#             'display_limit': display_limit,
#             'sortby': sortby,
#             'nosorturl': nosorturl,
#             'nodurminurl': nodurminurl,
#             'time_locked_url': time_locked_url,
#             'taskname': taskname,
#             'flowstruct': flowstruct,
#             'nodropPartURL': nodropPartURL,
#             'eventservice': eventservice,
#             'jobsTotalCount': jobsTotalCount,
#             'requestString': urlParametrs,
#             'built': datetime.now().strftime("%H:%M:%S"),
#             'newndrop_test': len(newdroplist) if len(newdroplist) > 0 else (- len(newdroppedPmerge)),
#             'cntStatus_test': cntStatus,
#             'ndropPmerge_test': len(newdroppedPmerge),
#             'droppedPmerge2_test': newdroppedPmerge,
#             'pandaIDList_test': newdroplist,
#             'difDropList_test': difDropList,
#             'clist': clist,
#             'warning': warning,
#         }
#         data.update(getContextVariables(request))
#         setCacheEntry(request, "jobList", json.dumps(data, cls=DateEncoder), 60 * 20)
#         ##self monitor
#         endSelfMonitor(request)
#         if eventservice:
#             response = render_to_response('jobListES.html', data, content_type='text/html')
#         else:
#             response = render_to_response('jobList.html', data, content_type='text/html')
#         patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
#         return response
#     else:
#         del request.session['TFIRST']
#         del request.session['TLAST']
#         if (('fields' in request.session['requestParams']) and (len(jobs) > 0)):
#             fields = request.session['requestParams']['fields'].split(',')
#             fields = (set(fields) & set(jobs[0].keys()))
#             if 'pandaid' not in fields:
#                 list(fields).append('pandaid')
#             for job in jobs:
#                 for field in list(job.keys()):
#                     if field in fields:
#                         pass
#                     else:
#                         del job[field]
#
#         data = {
#             "selectionsummary": sumd,
#             "jobs": jobs,
#             "errsByCount": errsByCount,
#         }
#         ##self monitor
#         endSelfMonitor(request)
#         response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
#         patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
#         return response


def job_param_list(request):
    """A view returns dump of jobsparams in JSON format"""
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