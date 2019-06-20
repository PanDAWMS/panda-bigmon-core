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
from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, DateEncoder, setCacheData
from core.common.models import JediTasksOrdered
from core.schedresource.models import Schedconfig
from core.settings.local import dbaccess
import pandas as pd
import numpy as np
from django.views.decorators.cache import never_cache
from django.utils import timezone

@never_cache
@login_customrequired
def datatapeCarouselleDashBoard(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
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


@never_cache
def getDTCSubmissionHist(request):
    valid, response = initRequest(request)
    staginData = getStagingData(request)

    timelistQueued = []
    timelistInterval = []

    progressDistribution = []
    summarytableDict = {}
    selectCampaign = []
    selectSource = []
    detailsTable = []

    for task, dsdata in staginData.items():
        timelistQueued.append(dsdata['start_time'])
        if dsdata['end_time']:
            timelistInterval.append(dsdata['end_time'] - dsdata['start_time'])
        else:
            timelistInterval.append(timezone.now() - dsdata['start_time'])

        dictSE = summarytableDict.get(dsdata['source_rse'], {"source": dsdata['source_rse'], "ds_active":0, "ds_done":0, "ds_90pdone":0, "files_rem":0, "files_done":0})
        if dsdata['end_time'] != None:
            dictSE["ds_done"]+=1
        else:
            dictSE["ds_active"]+=1
            if dsdata['staged_files'] >= dsdata['total_files']*0.9:
                dictSE["ds_90pdone"] += 1
            progressDistribution.append(dsdata['staged_files'] / dsdata['total_files'])

        dictSE["files_done"] += dsdata['staged_files']
        dictSE["files_rem"] += (dsdata['total_files'] - dsdata['staged_files'])
        summarytableDict[dsdata['source_rse']] = dictSE
        selectCampaign.append({"name": dsdata['campaign'], "value": dsdata['campaign'], "selected": "0"})
        selectSource.append({"name": dsdata['source_rse'], "value": dsdata['source_rse'], "selected": "0"})
        detailsTable.append([dsdata['campaign'], dsdata['pr_id'], dsdata['taskid'], dsdata['status'], dsdata['total_files'],
                             dsdata['staged_files'], int(round(dsdata['staged_files'] / dsdata['total_files'])) * 100,
                             dsdata['source_rse'], timelistInterval[-1], dsdata['start_time'], dsdata['rse'] ])

    #For uniquiness
    selectSource = list({v['name']: v for v in selectSource}.values())
    selectCampaign = list({v['name']: v for v in selectCampaign}.values())

    summarytableList = list(summarytableDict.values())

    timedelta = pd.to_timedelta(timelistInterval)
    timedelta = (timedelta / pd.Timedelta(hours=1))
    arr = [["EplTime"]]
    arr.extend([[x] for x in timedelta.tolist()])
    finalvalue = {"epltime": arr}

    arr = [["Progress"]]
    arr.extend([[x*100] for x in progressDistribution])
    finalvalue["progress"] = arr

    times = pd.to_datetime(timelistQueued)
    df = pd.DataFrame({
        "Count": [1 for _ in timelistQueued]
    }, index=times)

    grp = df.groupby([pd.Grouper(freq="12h")]).count()
    values = grp.values.tolist()
    index = grp.index.to_pydatetime().tolist()  # an ndarray method, you probably shouldn't depend on this

    data = [['Time', 'Count']]
    for time, count in zip(index, values):
        data.append([time, count[0]])

    finalvalue["submittime"] = data
    finalvalue["progresstable"] = summarytableList

    selectTime = [
        {"name": "Last 1 hour", "value": "hours1", "selected": "0"},
        {"name":"Last 12 hours", "value":"hours12", "selected":"0"},
        {"name":"Last day", "value":"hours24", "selected":"0"},
        {"name":"Last week","value":"hours168", "selected":"0"},
        {"name":"Last month","value":"hours720", "selected":"0"}]

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
        selection += "and t2.TASKID in (select taskid from ATLAS_DEFT.T_ACTION_STAGING @ INTR.CERN.CH)"

    if source:
        sourcel = [source] if ',' not in source else [SE for SE in source.split(',')]
        selection += " AND t1.SOURCE_RSE in (" + ','.join('\''+str(x)+'\'' for x in sourcel) + ")"

    if campaign:
        campaignl = [campaign] if ',' not in campaign else [camp for camp in campaign.split(',')]
        selection += " AND t3.campaign in (" + ','.join('\''+str(x)+'\'' for x in campaignl) + ")"

    if not jeditaskid:
        selection += " AND (END_TIME BETWEEN TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') and TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') or END_TIME is NULL)" \
                     % (timewindow[0], timewindow[1])

    new_cur.execute(
        """
                SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE, t1.TOTAL_FILES, 
                t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID, t3.campaign, t3.PR_ID FROM ATLAS_DEFT.T_DATASET_STAGING@INTR.CERN.CH t1
                INNER join ATLAS_DEFT.T_ACTION_STAGING@INTR.CERN.CH t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
                INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID %s 
        """ % selection
    )
    datasets = dictfetchall(new_cur)
    for dataset in datasets:
        dataset = {k.lower(): v for k, v in dataset.items()}
        data[dataset['taskid']] = dataset
    return data
