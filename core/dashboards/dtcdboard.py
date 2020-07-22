"""
    Created on 06.06.2018
"""

import random, json, math
from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect
from django.db import connection
from django.utils.cache import patch_response_headers
from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.exlib import dictfetchall
from core.views import login_customrequired, initRequest, setupView, DateEncoder, setCacheData
from core.common.models import JediTasksOrdered
from core.schedresource.models import Schedconfig
from core.settings.local import dbaccess
import pandas as pd
import numpy as np
from django.views.decorators.cache import never_cache
from django.utils import timezone

@never_cache
@login_customrequired
def dataCarouselleDashBoard(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render_to_response('DataTapeCarouselle.html', data, content_type='text/html')
    #patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response


def getStagingInfoForTask(request):
    valid, response = initRequest(request)
    data = getStagingData(request)
    response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response

def getBinnedData(listData, additionalList1 = None, additionalList2 = None):
    isTimeNotDelta = True
    timesadd1 = None
    timesadd2 = None

    try:
        times = pd.to_datetime(listData)
        if additionalList1:
            timesadd1 = pd.to_datetime(additionalList1)
        if additionalList2:
            timesadd2 = pd.to_datetime(additionalList2)

    except:
        times = pd.to_timedelta(listData)
        isTimeNotDelta = False
        if additionalList1:
            timesadd1 = pd.to_timedelta(additionalList1)
        if additionalList2:
            timesadd2 = pd.to_timedelta(additionalList2)

    #if not timesadd is None:
    #    mergedIndex = times.union(timesadd)
    #else:
    #    mergedIndex = times


    df = pd.DataFrame({
        "Count1": [1 for _ in listData]
    }, index=times)

    if not timesadd1 is None:
        dfadd = pd.DataFrame({
            "Count2": [1 for _ in additionalList1]
        }, index=timesadd1)
        result = pd.concat([df, dfadd])
    else:
        result = df

    if not timesadd2 is None:
        dfadd = pd.DataFrame({
            "Count3": [1 for _ in additionalList2]
        }, index=timesadd2)
        result = pd.concat([result, dfadd])

    grp = result.groupby([pd.Grouper(freq="24h")]).count()
    values = grp.values.tolist()
    if isTimeNotDelta:
        index = grp.index.to_pydatetime().tolist()
    else:
        index = (grp.index / pd.Timedelta(hours=1)).tolist()

    if not additionalList1 is None and len(additionalList1) == 0:
        tmpval = []
        for item in values:
            if additionalList2:
                tmpval.append([item[0], 0, item[1]])
            else:
                tmpval.append([item[0], 0])
        values = tmpval

    if not additionalList2 is None and len(additionalList2) == 0:
        tmpval = []
        if len(values) > 1:  # temp fix, to be looked closer
            for item in values:
                tmpval.append([item[0], item[1], 0])
        values = tmpval


    data = []
    for time, count in zip(index, values):
        data.append([time, count])
    return data


@never_cache
def getDTCSubmissionHist(request):
    valid, response = initRequest(request)
    staginData = getStagingData(request)

    timelistSubmitted = []

    progressDistribution = []
    summarytableDict = {}
    selectCampaign = []
    selectSource = []
    detailsTable = []
    timelistIntervalfin = []
    timelistIntervalact = []
    timelistIntervalqueued = []


    for task, dsdata in staginData.items():
        epltime = None
        timelistSubmitted.append(dsdata['start_time'])

        dictSE = summarytableDict.get(dsdata['source_rse'], {"source": dsdata['source_rse'], "ds_active":0, "ds_done":0, "ds_queued":0, "ds_90pdone":0, "files_rem":0, "files_q":0, "files_done":0})

        dictSE["files_done"] += dsdata['staged_files']
        dictSE["files_rem"] += (dsdata['total_files'] - dsdata['staged_files'])

        # Build the summary by SEs and create lists for histograms
        if dsdata['end_time'] != None:
            dictSE["ds_done"]+=1
            epltime = dsdata['end_time'] - dsdata['start_time']
            timelistIntervalfin.append(epltime)

        elif dsdata['status'] != 'queued':
            epltime = timezone.now() - dsdata['start_time']
            timelistIntervalact.append(epltime)
            dictSE["ds_active"]+=1
            if dsdata['staged_files'] >= dsdata['total_files']*0.9:
                dictSE["ds_90pdone"] += 1
        elif dsdata['status'] == 'queued':
            dictSE["ds_queued"] += 1
            dictSE["files_q"] += (dsdata['total_files'] - dsdata['staged_files'])
            epltime = timezone.now() - dsdata['start_time']
            timelistIntervalqueued.append(epltime)

        progressDistribution.append(dsdata['staged_files'] / dsdata['total_files'])

        summarytableDict[dsdata['source_rse']] = dictSE
        selectCampaign.append({"name": dsdata['campaign'], "value": dsdata['campaign'], "selected": "0"})
        selectSource.append({"name": dsdata['source_rse'], "value": dsdata['source_rse'], "selected": "0"})
        detailsTable.append({'campaign': dsdata['campaign'], 'pr_id': dsdata['pr_id'], 'taskid': dsdata['taskid'], 'status': dsdata['status'], 'total_files': dsdata['total_files'],
                             'staged_files': dsdata['staged_files'], 'progress': int(round(dsdata['staged_files'] * 100.0 / dsdata['total_files'])),
                             'source_rse': dsdata['source_rse'], 'elapsedtime': epltime, 'start_time': dsdata['start_time'], 'rse': dsdata['rse']})

    #For uniquiness
    selectSource = list({v['name']: v for v in selectSource}.values())
    selectCampaign = list({v['name']: v for v in selectCampaign}.values())

    summarytableList = list(summarytableDict.values())

    # timedelta = pd.to_timedelta(timelistIntervalfin)
    # timedelta = (timedelta / pd.Timedelta(hours=1))
    # arr = [["EplTime"]]
    # arr.extend([[x] for x in timedelta.tolist()])
    #
    # timedelta = pd.to_timedelta(timelistIntervalact)
    # timedelta = (timedelta / pd.Timedelta(hours=1))
    # #arr1 = [["EplTime"]]
    # arr.extend([[x] for x in timedelta.tolist()])

    binnedActFinData = getBinnedData(timelistIntervalact, additionalList1 = timelistIntervalfin, additionalList2 = timelistIntervalqueued)
    eplTime = [['Time', 'Act. staging', 'Fin. staging', 'Q. staging']] + [[time, data[0], data[1], data[2]] for (time, data) in binnedActFinData]
    #, 'Queued staging'

    finalvalue = {"epltime": eplTime}

    arr = [["Progress"]]
    arr.extend([[x*100] for x in progressDistribution])
    finalvalue["progress"] = arr

    binnedSubmData = getBinnedData(timelistSubmitted)
    finalvalue["submittime"] = [['Time', 'Count']] + [[time, data[0]] for (time, data) in binnedSubmData]
    finalvalue["progresstable"] = summarytableList

    selectTime = [
        {"name": "Last 1 hour", "value": "hours1", "selected": "0"},
        {"name":"Last 12 hours", "value":"hours12", "selected":"0"},
        {"name":"Last day", "value":"hours24", "selected":"0"},
        {"name":"Last week","value":"hours168", "selected":"0"},
        {"name":"Last month","value":"hours720", "selected":"0"},
        {"name": "Last 3 months", "value": "hours2160", "selected": "0"},
        {"name": "Last 6 months", "value": "hours4320", "selected": "0"}
    ]

    hours = ""
    if 'hours' in request.session['requestParams']:
        hours = request.session['requestParams']['hours']

    for selectTimeItem in selectTime:
        if selectTimeItem["value"] == "hours"+str(hours):
            selectTimeItem["selected"] = "1"
            break

    finalvalue["selectsource"] = selectSource
    finalvalue["selecttime"] = selectTime
    finalvalue["selectcampaign"] = selectCampaign
    finalvalue["detailstable"] = detailsTable


    response = HttpResponse(json.dumps(finalvalue, cls=DateEncoder), content_type='application/json')
    return response


def getStagingData(request):

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    timewindow = query['modificationtime__castdate__range']

    if 'source' in request.GET:
        source = request.GET['source']
    else:
        source = None

    if 'destination' in request.GET:
        destination = request.GET['destination']
    else:
        destination = None

    if 'campaign' in request.GET:
        campaign = request.GET['campaign']
    else:
        campaign = None

    data = {}
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"

    new_cur = connection.cursor()

    selection = "where 1=1 "
    jeditaskid = None
    if 'jeditaskid' in request.session['requestParams']:
        jeditaskid = request.session['requestParams']['jeditaskid']
        taskl = [int(jeditaskid)] if '|' not in jeditaskid else [int(taskid) for taskid in jeditaskid.split('|')]
        new_cur = connection.cursor()
        transactionKey = random.randrange(1000000)
        executionData = []
        for id in taskl:
            executionData.append((id, transactionKey))

        query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
        new_cur.executemany(query, executionData)
        connection.commit()
        selection += "and t2.taskid in (SELECT tmp.id FROM %s tmp where TRANSACTIONKEY=%i)"  % (tmpTableName, transactionKey)
    else:
        selection += "and t2.TASKID in (select taskid from ATLAS_DEFT.T_ACTION_STAGING)"

    if source:
        sourcel = [source] if ',' not in source else [SE for SE in source.split(',')]
        selection += " AND t1.SOURCE_RSE in (" + ','.join('\''+str(x)+'\'' for x in sourcel) + ")"

    if campaign:
        campaignl = [campaign] if ',' not in campaign else [camp for camp in campaign.split(',')]
        selection += " AND t3.campaign in (" + ','.join('\''+str(x)+'\'' for x in campaignl) + ")"

    if not jeditaskid:
        selection += " AND (END_TIME BETWEEN TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') and TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') or (END_TIME is NULL and not (t1.STATUS = 'done')))" \
                     % (timewindow[0], timewindow[1])

    new_cur.execute(
        """
                SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE, t1.TOTAL_FILES, 
                t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID, t3.campaign, t3.PR_ID FROM ATLAS_DEFT.T_DATASET_STAGING t1
                INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
                INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID %s 
        """ % selection
    )
    datasets = dictfetchall(new_cur)
    for dataset in datasets:
        # Sort out requests by request on February 19, 2020
        if dataset['STATUS'] in ('staging', 'queued', 'done'):
            dataset = {k.lower(): v for k, v in dataset.items()}
            data[dataset['taskid']] = dataset
    return data
