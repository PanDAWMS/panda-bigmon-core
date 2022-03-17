"""
    A set of views for DataCarousel app
"""

import json
import math
import logging
import time

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils.cache import patch_response_headers
from django.views.decorators.cache import never_cache
from django.utils import timezone
from django.db import connection

from core.libs.exlib import build_time_histogram, dictfetchall, convert_bytes
from core.oauth.utils import login_customrequired
from core.views import initRequest, setupView, DateEncoder
from core.datacarousel.utils import getBinnedData, getStagingData, getStagingDatasets, send_report_rse
from core.datacarousel.utils import retreiveStagingStatistics, getOutliers, substitudeRSEbreakdown, extractTasksIds

from core.settings.base import DATA_CAROUSEL_MAIL_DELAY_DAYS, DATA_CAROUSEL_MAIL_REPEAT
from core.settings import defaultDatetimeFormat

_logger = logging.getLogger('bigpandamon')


@never_cache
@login_customrequired
def dataCarouselleDashBoard(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=24, limit=9999999, querytype='task', wildCardExt=True)

    if query and 'modificationtime__castdate__range' in query:
        request.session['timerange'] = query['modificationtime__castdate__range']

    request.session['viewParams']['selection'] = ''
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
        'requestParams': request.session['requestParams'] if 'requestParams' in request.session else {},
        'timerange': request.session['timerange'],
    }

    response = render(request, 'DataTapeCarouselle.html', data, content_type='text/html')
    # patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response


@never_cache
@login_customrequired
def dataCarouselTailsDashBoard(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render(request, 'DataTapeCaruselTails.html', data, content_type='text/html')
    return response


@never_cache
def getStagingInfoForTask(request):
    valid, response = initRequest(request)
    data = getStagingData(request)
    response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response


def getStagingTailsData(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    timewindow = query['modificationtime__castdate__range']
    if 'source' in request.GET:
        source = request.GET['source']
    else:
        source = None
    datasets = getStagingDatasets(timewindow, source)
    tasks = extractTasksIds(datasets)
    setOfSEs = datasets.keys()
    outliers = None
    if len(setOfSEs) > 0:
        stageStat, tasks_to_rucio = retreiveStagingStatistics(setOfSEs, taskstoingnore=tasks)
        outliers = getOutliers(datasets, stageStat, tasks_to_rucio)
    return JsonResponse(outliers, safe=False)


@never_cache
def getDTCSubmissionHist(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    staginData = getStagingData(request)
    _logger.debug('Got data: {}'.format(time.time() - request.session['req_init_time']))

    timelistSubmitted = []
    timelistSubmittedFiles = []
    progressDistribution = []
    timelistIntervalfin = []
    timelistIntervalact = []
    timelistIntervalqueued = []

    dataset_list = []
    summary = {
        'processingtype': {},
        'source_rse': {},
        'campaign': {}
    }
    selection_options = {
        'campaign': [],
        'source_rse': [],
        'processingtype': [],
    }
    calc_temp = {
        "ds_active": 0, "ds_done": 0, "ds_queued": 0, "ds_90pdone": 0,
        'files_total': 0, "files_rem": 0, "files_queued": 0, "files_done": 0, 'files_active': 0,
        'bytes_total': 0, 'bytes_done': 0, "bytes_queued": 0, 'bytes_rem': 0, 'bytes_active': 0,
    }

    for task, dsdata in staginData.items():
        epltime = None
        timelistSubmitted.append(dsdata['start_time'])
        timelistSubmittedFiles.append([dsdata['start_time'], dsdata['total_files']])

        for key in summary:
            if dsdata[key] not in summary[key]:
                summary[key][dsdata[key]] = {
                    key: dsdata[key],
                }
                summary[key][dsdata[key]].update(calc_temp)
                if key == "source_rse":
                    summary[key][dsdata[key]]['source_rse_breakdown'] = substitudeRSEbreakdown(dsdata['source_rse'])

            if dsdata['occurence'] == 1:
                summary[key][dsdata[key]]['files_total'] += dsdata['total_files']
                summary[key][dsdata[key]]['files_done'] += dsdata['staged_files']
                summary[key][dsdata[key]]['files_rem'] += (dsdata['total_files'] - dsdata['staged_files'])
                summary[key][dsdata[key]]['bytes_total'] += convert_bytes(dsdata['dataset_bytes'], output_unit='GB')
                summary[key][dsdata[key]]['bytes_done'] += convert_bytes(dsdata['staged_bytes'], output_unit='GB')
                summary[key][dsdata[key]]['bytes_rem'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], output_unit='GB')

                # Build the summary by SEs and create lists for histograms
                if dsdata['end_time'] is not None:
                    summary[key][dsdata[key]]["ds_done"] += 1
                    epltime = dsdata['end_time'] - dsdata['start_time']
                    timelistIntervalfin.append(epltime)
                elif dsdata['status'] != 'queued':
                    epltime = timezone.now() - dsdata['start_time']
                    timelistIntervalact.append(epltime)
                    summary[key][dsdata[key]]["ds_active"] += 1
                    summary[key][dsdata[key]]['files_active'] += (dsdata['total_files'] - dsdata['staged_files'])
                    summary[key][dsdata[key]]['bytes_active'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], output_unit='GB')
                    if dsdata['staged_files'] >= dsdata['total_files'] * 0.9:
                        summary[key][dsdata[key]]["ds_90pdone"] += 1
                elif dsdata['status'] == 'queued':
                    epltime = timezone.now() - dsdata['start_time']
                    timelistIntervalqueued.append(epltime)
                    summary[key][dsdata[key]]["ds_queued"] += 1
                    summary[key][dsdata[key]]["files_queued"] += (dsdata['total_files'] - dsdata['staged_files'])
                    summary[key][dsdata[key]]["bytes_queued"] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], output_unit='GB')

        progressDistribution.append(dsdata['staged_files'] / dsdata['total_files'])
        dataset_list.append({
            'campaign': dsdata['campaign'], 'pr_id': dsdata['pr_id'], 'taskid': dsdata['taskid'],
            'status': dsdata['status'], 'total_files': dsdata['total_files'], 'staged_files': dsdata['staged_files'],
            'size': round(convert_bytes(dsdata['dataset_bytes'], output_unit='GB'), 2),
            'progress': int(math.floor(dsdata['staged_files'] * 100.0 / dsdata['total_files'])),
            'source_rse': dsdata['source_rse'],
            'elapsedtime': str(epltime).split('.')[0] if epltime is not None else '---',
            'start_time': dsdata['start_time'].strftime(defaultDatetimeFormat) if dsdata['start_time'] else '---',
            'rse': dsdata['rse'],
            'update_time': str(dsdata['update_time']).split('.')[0] if dsdata['update_time'] is not None else '---',
            'update_time_sort': dsdata['update_time_sort'],
            'processingtype': dsdata['processingtype']})

    # fill options for selection menus
    for key in selection_options:
        if key in summary:
            selection_options[key] = sorted(
                [{"name": value, "value": value, "selected": "0"} for value in summary[key]],
                key=lambda x: x['name'].lower()
            )

    # round bytes
    for param in summary:
        for value in summary[param]:
            for key in summary[param][value]:
                if key.startswith('bytes'):
                    summary[param][value][key] = round(summary[param][value][key], 2)

    # dict -> list for summary + sorting
    for param in summary:
        summary[param] = sorted(list(summary[param].values()), key=lambda x: x[param].lower())

    binned_subm_datasets = build_time_histogram(timelistSubmitted) if len(timelistSubmitted) > 0 else {}
    binned_subm_files = build_time_histogram(timelistSubmittedFiles) if len(timelistSubmittedFiles) > 0 else {}

    binnedActFinData = getBinnedData(
        timelistIntervalact,
        additionalList1=timelistIntervalfin, additionalList2=timelistIntervalqueued)
    eplTime = [['Time', 'Active staging', 'Finished staging', 'Queued staging']] + [[round(time_str, 1), data[0], data[1], data[2]] for (time_str, data) in binnedActFinData]

    _logger.debug('Prepared data: {}'.format(time.time() - request.session['req_init_time']))

    finalvalue = {}
    finalvalue["elapsedtime"] = eplTime
    finalvalue["submittime"] = [['Time', 'Count']] + [[time_str, data[0]] for time_str, data in binned_subm_datasets]
    finalvalue["submittimefiles"] = [['Time', 'Count']] + [[time_str, data[0]] for time_str, data in binned_subm_files]
    finalvalue["progress"] = [["Progress"]] + [[x * 100] for x in progressDistribution]
    finalvalue['summary'] = summary
    finalvalue['selection'] = selection_options
    finalvalue["detailstable"] = dataset_list
    response = HttpResponse(json.dumps(finalvalue, cls=DateEncoder), content_type='application/json')
    return response


@never_cache
def send_stalled_requests_report(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # get data
    try:
        query = """
        SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE as RSE, t1.TOTAL_FILES, 
            t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID, t3.campaign, t3.PR_ID, 
            ROW_NUMBER() OVER(PARTITION BY t1.DATASET_STAGING_ID ORDER BY t1.start_time DESC) AS occurence, 
            (CURRENT_TIMESTAMP-t1.UPDATE_TIME) as UPDATE_TIME, t4.processingtype 
        FROM ATLAS_DEFT.T_DATASET_STAGING t1
        INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
        INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID 
        INNER JOIN ATLAS_PANDA.JEDI_TASKS t4 on t2.TASKID=t4.JEDITASKID 
        where END_TIME is NULL and (t1.STATUS = 'staging') and t1.UPDATE_TIME <= TRUNC(SYSDATE) - {}
        """.format(DATA_CAROUSEL_MAIL_DELAY_DAYS)
        cursor = connection.cursor()
        cursor.execute(query)
        rows = dictfetchall(cursor)
    except Exception as e:
        _logger.error(e)
        rows = []

    rows = sorted(rows, key=lambda x: x['UPDATE_TIME'], reverse=True)
    ds_per_rse = {}
    for r in rows:
        if r['SOURCE_RSE'] not in ds_per_rse:
            ds_per_rse[r['SOURCE_RSE']] = []

        data = {
            "SE": r['SOURCE_RSE'],
            "RR": r['RSE'],
            "START_TIME": str(r['START_TIME']),
            "TASKID": r['TASKID'],
            "TOT_FILES": r['TOTAL_FILES'],
            "STAGED_FILES": r['STAGED_FILES'],
            "UPDATE_TIME": str(r['UPDATE_TIME'])
        }
        ds_per_rse[r['SOURCE_RSE']].append(data)

    for rse, data in ds_per_rse.items():
        _logger.debug("DataCarouselMails processes this RSE: {}".format(rse))
        send_report_rse(rse, data)

    return JsonResponse({'sent': len(rows)})

