import pandas as pd
from matplotlib import pyplot as plt
import urllib.request as urllibr
from urllib.error import HTTPError
import json
import datetime
import numpy as np
import os
#from sklearn.preprocessing import scale
from core.views import initRequest, setupView, DateEncoder, setCacheData
from django.shortcuts import render_to_response
from django.views.decorators.cache import never_cache
from core.auth.utils import login_customrequired
from django.db import connection
from core.libs.exlib import dictfetchall
from core.settings.local import dbaccess
import random
import cx_Oracle
from django.http import JsonResponse
from django.core.cache import cache
from django.utils.six.moves import cPickle as pickle
import logging
_logger = logging.getLogger('bigpandamon')

BASE_STAGE_INFO_URL = 'https://bigpanda.cern.ch/staginprogress/?jeditaskid='

@never_cache
@login_customrequired
def dataCarouselTailsDashBoard(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''
    data = {
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render_to_response('DataTapeCaruselTails.html', data, content_type='text/html')
    #patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response

def getListOfTapeSE():
    listOfSE = []
    selection = """
    
    """

def getStagingDatasets(timewindow, source):
    selection = """
                select tbig.DATASET, tbig.STATUS, tbig.STAGED_FILES, tbig.START_TIME, tbig.END_TIME, tbig.RRULE, tbig.TOTAL_FILES, tbig.SOURCE_RSE, tbig.TASKID, NULL as PROGRESS_RETRIEVED, NULL as PROGRESS_DATA from (
                SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE as RRULE, t1.TOTAL_FILES,
                 t1.SOURCE_RSE, t2.TASKID, ROW_NUMBER() OVER(PARTITION BY t1.DATASET_STAGING_ID ORDER BY t1.start_time DESC) AS occurence FROM ATLAS_DEFT.T_DATASET_STAGING t1
                INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
                INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID
                order by t1.START_TIME desc
                )tbig 
                LEFT OUTER JOIN ATLAS_PANDABIGMON.DATACAR_ST_PROGRESS_ARCH arch on arch.RRULE=tbig.RRULE
                where occurence=1 and tbig.RRULE is not NULL
    """
    selection += " AND (tbig.END_TIME BETWEEN TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') and TO_DATE(\'%s\','YYYY-mm-dd HH24:MI:SS') or (tbig.END_TIME is NULL and not (tbig.STATUS in ('done', 'cancelled'))))" \
                 % (timewindow[0], timewindow[1])
    cursor = connection.cursor()
    cursor.execute(selection)
    datasets = dictfetchall(cursor)
    datasets_dict = {}
    for ds in datasets:
        datasets_dict.setdefault(ds["SOURCE_RSE"], []).append(ds)
    return datasets_dict


def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.LOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize = cursor.arraysize)
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize = cursor.arraysize)
    elif defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize = cursor.arraysize)


def transform_into_eq_intervals(in_series, name):
    df = pd.Series(in_series, name=name)
    df = df.resample('15Min').mean()
    df.interpolate(method='linear', limit_direction='forward', inplace=True)
    df.index = df.index - df.index[0]
    return df


def retreiveStagingStatistics(SEs, taskstoingnore):
    cursor=connection.cursor()
    SEsStr = ','.join('\''+SE+'\'' for SE in SEs)
    query = """select * from (
    select START_TIME, PROGRESS_DATA, TOTAL_FILES, RRULE, TASKID, SOURCE_RSE, row_number() over (PARTITION BY SOURCE_RSE order by START_TIME desc) as rn from atlas_pandabigmon.DATACAR_ST_PROGRESS_ARCH 
    where PROGRESS_RETRIEVED=1 and SOURCE_RSE in (%s)) where rn <= 15""" % (SEsStr)
    cursor.execute(query)

    data = {}
    tasks_to_rucio = {}
    for row in cursor:
        if row[4] not in taskstoingnore:
            intermediate_row = patch_start_time(row)
            intermediate_row = transform_into_eq_intervals(intermediate_row, str(row[4]))
            data.setdefault(row[5], []).append(intermediate_row)
            tasks_to_rucio[row[4]] = row[3]
    return data, tasks_to_rucio


def getStaginProgress(taskid):
    response = None
    try:
        req = urllibr.Request(BASE_STAGE_INFO_URL + taskid)
        response = urllibr.urlopen(req, timeout=180).read()
        response = json.loads(response)
    except Exception or HTTPError as e:
        _logger.error(e)
    return response


def patch_start_time(dbrow):
    dformat = "%Y-%m-%d %H:%M:%S"
    start = dbrow[0].strftime(dformat)
    if isinstance(dbrow[1], cx_Oracle.LOB):
        serie=json.loads(dbrow[1].read())
    else:
        serie=dbrow[1]
    serie[0] = [start, 0]
    serie_dict = {}
    for row in serie:
        row[0] = datetime.datetime.strptime(row[0],dformat)
        serie_dict[row[0]] = row[1]/100.0*dbrow[2]
    return serie_dict

def getCachedProgress(se, taskid):
    serialized_progress = cache.get('serialized_staging_progress' + se + "_" + str(taskid))
    if serialized_progress:
        return pickle.loads(serialized_progress)
    else:
        return None

def setCachedProgress(se, taskid, stagestatus, progress):
    progress = pickle.dumps(progress)
    timeout = 3600
    if stagestatus == 'done':
        timeout = 3600 * 24 * 30 * 6
    cache.set('serialized_staging_progress' + se + "_" + str(taskid), progress, timeout)


def getOutliers(datasets_dict, stageStat, tasks_to_rucio):
    output = {}
    output_table = {}
    for se, datasets in datasets_dict.items():
        basicstat = stageStat.get(se, [])
        for ds in datasets:
            progress_info = getCachedProgress(se, ds['TASKID'])
            if not progress_info:
                progress_info = getStaginProgress(str(ds['TASKID']))
                if progress_info:
                    setCachedProgress(se, ds['TASKID'], ds['STATUS'], progress_info)
            progress_info = patch_start_time((ds['START_TIME'], progress_info, ds['TOTAL_FILES']))
            progress_info = transform_into_eq_intervals(progress_info, str(ds['TASKID']))
            basicstat.append(progress_info)
            tasks_to_rucio[ds['TASKID']] = ds['RRULE']
        datamerged = pd.concat([s for s in basicstat], axis=1)
        zscore = datamerged.copy(deep=True)
        zscore = zscore.apply(lambda V: scale(V,axis=0,with_mean=True, with_std=True,copy=False),axis=1)
        zscore_df = pd.DataFrame.from_dict(dict(zip(zscore.index, zscore.values))).T
        outliers = ((zscore_df< -1.5).any().values)
        datamerged = datamerged.fillna("_")
        list_of_val = datamerged.values.tolist()
        timeticks = (datamerged.index / np.timedelta64(1, 'h')).tolist()
        for i in range(len(timeticks)):
            list_of_val[i] = [timeticks[i]] + list_of_val[i]
        tasksids = datamerged.columns.values.tolist()
        report = {}
        report['series'] = [["Time"]+tasksids] + list_of_val
        report['tasksids'] = tasksids
        report['outliers'] =  outliers.tolist()
        output[se] = report
        if len(list(filter(lambda x: x, report['outliers']))) > 0:
            outliers_tasks_rucio = [(tasksids[idx], tasks_to_rucio.get(int(tasksids[idx]), None)) for idx, state in enumerate(report['outliers']) if state]
            output_table.setdefault(se, []).extend(outliers_tasks_rucio)
    return {'plotsdata':output, 'tasks_rucio':output_table}


def extractTasksIds(datasets):
    tasksIDs = []
    for se,datasets  in datasets.items():
        for dataset in datasets:
            tasksIDs.append(dataset["TASKID"])
    return tasksIDs

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

