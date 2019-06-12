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


@login_customrequired
def datatapeCarouselleDashBoard(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    data = {
        'request': request,
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
    for task, dsdata in staginData.items():
        timelistQueued.append(dsdata['start_time'])
        if dsdata['end_time']:
            timelistInterval.append(dsdata['end_time'] - dsdata['start_time'])

    times = pd.to_datetime(timelistQueued)
    df = pd.DataFrame({
        "Count": [1 for _ in timelistQueued]
    }, index=times)

    grp = df.groupby([pd.Grouper(freq="12h")]).count()
    #grp = df.groupby([pd.Grouper(20)]).count()

    values = grp.values.tolist()
    index = grp.index.to_pydatetime().tolist()  # an ndarray method, you probably shouldn't depend on this

    data = [['Time', 'Count']]

    for time, count in zip(index, values):
        data.append([time, count[0]])

    finalvalue = {"submittime": data}

    response = HttpResponse(json.dumps(finalvalue, cls=DateEncoder), content_type='application/json')
    return response


def getStagingData(request):
    data = {}
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1"
    else:
        tmpTableName = "TMP_IDS1"

    new_cur = connection.cursor()

    selection = ""
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
        selection = "and taskid in (SELECT tmp.id FROM %s tmp where TRANSACTIONKEY=%i)"  % (tmpTableName, transactionKey)
    else:
        selection = "where t2.TASKID in (select taskid from ATLAS_DEFT.T_ACTION_STAGING @ INTR.CERN.CH)"
    new_cur.execute(
        """
                SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE, t1.TOTAL_FILES, t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID FROM ATLAS_DEFT.T_DATASET_STAGING@INTR.CERN.CH t1 
                            join ATLAS_DEFT.t_production_task t2 ON t1.DATASET=t2.PRIMARY_INPUT %s 
        """ % selection
    )
    datasets = dictfetchall(new_cur)
    for dataset in datasets:
        dataset = {k.lower(): v for k, v in dataset.items()}
        data[dataset['taskid']] = dataset
    return data
