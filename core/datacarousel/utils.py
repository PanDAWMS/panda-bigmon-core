"""
    A set of functions needed for DataCarousel app
"""

import random
import logging
import json
import time
import datetime
import pickle
import numpy as np
import pandas as pd
import cx_Oracle
import urllib.request as urllibr
from sklearn.preprocessing import scale
from urllib.error import HTTPError
from elasticsearch_dsl import Search

from django.core.cache import cache

from django.db import connection

from core.reports.sendMail import send_mail_bp
from core.reports.models import ReportEmails
from core.views import setupView
from core.libs.exlib import dictfetchall, get_tmp_table_name
from core.libs.elasticsearch import create_es_connection
from core.schedresource.utils import getCRICSEs
from core.filebrowser.ruciowrapper import ruciowrapper

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

BASE_STAGE_INFO_URL = 'https://bigpanda.cern.ch/staginprogress/?jeditaskid='
#BASE_STAGE_INFO_URL = 'http://aipanda163.cern.ch:8000/staginprogress/?jeditaskid='


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


def substitudeRSEbreakdown(rse):
    rses = getCRICSEs().get(rse, [])
    final_string = ""
    for rse in rses:
        final_string += "&var-src_endpoint=" + rse
    return final_string


def getStagingData(request):
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, wildCardExt=True)
    timewindow = query['modificationtime__castdate__range']

    if 'source' in request.GET or 'source_rse' in request.GET:
        source = request.GET['source'] if 'source' in request.GET else request.GET['source_rse']
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

    if 'processingtype' in request.GET:
        processingtype = request.GET['processingtype']
    else:
        processingtype = None

    data = {}
    tmpTableName = get_tmp_table_name()

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

        query = """insert into """ + tmpTableName + """(id,transactionkey) values (%s, %s)"""
        new_cur.executemany(query, executionData)
        connection.commit()
        selection += "and t2.taskid in (SELECT tmp.id FROM %s tmp where transactionkey=%i)"  % (tmpTableName, transactionKey)
    else:
        selection += "and t2.TASKID in (select taskid from ATLAS_DEFT.T_ACTION_STAGING)"

    if source:
        sourcel = [source] if ',' not in source else [SE for SE in source.split(',')]
        selection += " AND t1.SOURCE_RSE in (" + ','.join('\''+str(x)+'\'' for x in sourcel) + ")"

    if campaign:
        campaignl = [campaign] if ',' not in campaign else [camp for camp in campaign.split(',')]
        selection += " AND t3.campaign in (" + ','.join('\''+str(x)+'\'' for x in campaignl) + ")"

    if processingtype:
        processingtypel = [processingtype] if ',' not in processingtype else [pt for pt in processingtype.split(',')]
        selection += " AND t4.processingtype in (" + ','.join('\''+str(x)+'\'' for x in processingtypel) + ")"

    if not jeditaskid:
        selection += """  
        and not (nvl(t4.endtime, current_timestamp) < t1.start_time) 
        and (
            end_time between to_date('{}', 'YYYY-mm-dd HH24:MI:SS') and to_date('{}', 'YYYY-mm-dd HH24:MI:SS') 
            or (end_time is null and not (t1.status = 'done'))
        )
        """.format(timewindow[0], timewindow[1])

    new_cur.execute(
        """
        select t1.dataset, t1.status, t1.staged_files, t1.start_time, t1.end_time, t1.rse as rse, t1.total_files, 
            t1.update_time, t1.source_rse, t2.taskid, t3.campaign, t3.pr_id, t1.dataset_bytes, t1.staged_bytes,
            row_number() over(partition by t1.dataset_staging_id order by t1.start_time desc) as occurence, 
            (current_timestamp-t1.update_time) as update_time, t4.processingtype, t2.step_action_id 
        from atlas_deft.t_dataset_staging t1
        inner join {0}.t_action_staging t2 on t1.dataset_staging_id=t2.dataset_staging_id
        inner join {0}.t_production_task t3 on t2.taskid=t3.taskid 
        inner join {1}.jedi_tasks t4 on t2.taskid=t4.jeditaskid {2} 
        """.format('atlas_deft', settings.DB_SCHEMA_PANDA, selection))
    datasets = dictfetchall(new_cur)
    for dataset in datasets:
        # Sort out requests by request on February 19, 2020
        if dataset['STATUS'] in ('staging', 'queued', 'done'):
            dataset = {k.lower(): v for k, v in dataset.items()}

            datasetname = dataset.get('dataset')
            if ':' in datasetname:
                dataset['scope'] = datasetname.split(':')[0]
            else:
                dataset['scope'] = datasetname.split('.')[0]

            data[dataset['taskid']] = dataset
    return data


def getStagingDatasets(timewindow, source):
    selection = """
    select tbig.dataset, tbig.status, tbig.staged_files, tbig.start_time, tbig.end_time, tbig.rrule, tbig.total_files, 
        tbig.source_rse, tbig.taskid, null as progress_retrieved, null as progress_data 
    from (
        select t1.dataset, t1.status, t1.staged_files, t1.start_time, t1.end_time, t1.rse as rrule, t1.total_files,
            t1.source_rse, t2.taskid, 
            row_number() over(partition by t1.dataset_staging_id order by t1.start_time desc) as occurence 
        from {0}.t_dataset_staging t1
        inner join {0}.t_action_staging t2 on t1.dataset_staging_id=t2.dataset_staging_id
        inner join {0}.t_production_task t3 on t2.taskid=t3.taskid
        order by t1.start_time desc
        ) tbig 
    left outer join {1}.datacar_st_progress_arch arch on arch.rrule=tbig.rrule
    where occurence=1 and tbig.rrule is not null
    """.format('atlas_deft', settings.DB_SCHEMA)
    selection += """ 
    AND (
        tbig.end_time between to_date('{}','YYYY-mm-dd HH24:MI:SS') 
        and to_date('{}','YYYY-mm-dd HH24:MI:SS') 
        or (
            tbig.end_time is null 
            and not tbig.status in ('done', 'cancelled')
        )
    )""".format(timewindow[0], timewindow[1])
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
    query = """
    select * 
    from (
        select start_time, progress_data, total_files, rrule, taskid, source_rse, 
            row_number() over (partition by source_rse order by start_time desc) as rn 
        from {}.datacar_st_progress_arch 
        where progress_retrieved=1 and source_rse in ({})
    ) where rn <= 15
    """.format(settings.DB_SCHEMA, SEsStr)
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
    if len(serie) > 0:
        serie[0] = [start, 0]
    serie_dict = {}
    for row in serie:
        row[0] = datetime.datetime.strptime(row[0],dformat)
        serie_dict[row[0]] = row[1]/100.0*dbrow[2]
    return serie_dict


def getCachedProgress(se, taskid):
    serialized_progress = cache.get('serialized_staging_progress' + se + "_" + str(taskid), None)
    if serialized_progress:
        return pickle.loads(serialized_progress)
    else:
        return None


def setCachedProgress(se, taskid, stagestatus, progress):
    progress = pickle.dumps(progress)
    timeout = 3600 * 2
    if stagestatus == 'done':
        timeout = 3600 * 24 * 30 * 6
    cache.set('serialized_staging_progress' + se + "_" + str(taskid), progress, timeout)


def getOutliers(datasets_dict, stageStat, tasks_to_rucio):
    output = {}
    output_table = {}
    basicstat = None
    _logger.debug('Getting staging progress and identifying outliers for {} RSEs:'.format(len(datasets_dict)))
    for se, datasets in datasets_dict.items():
        _logger.debug('RSE: {}, {} datasets to analyze'.format(se, len(datasets)))
        basicstat = stageStat.get(se, [])
        for ds in datasets:
            try:
                progress_info = getCachedProgress(se, ds['TASKID'])
            except:
                progress_info = None
            # protection against wrong epoch -> datatime transformation stored in cache
            if progress_info and len(progress_info) > 1 and '1970' in progress_info[1][0]:
                progress_info = None
            if not progress_info:
                progress_info = getStaginProgress(str(ds['TASKID']))
                if progress_info:
                    setCachedProgress(se, ds['TASKID'], ds['STATUS'], progress_info)
            if progress_info and len(progress_info) > 1 and '1970' not in progress_info[1][0]:
                progress_info = patch_start_time((ds['START_TIME'], progress_info, ds['TOTAL_FILES']))
                progress_info = transform_into_eq_intervals(progress_info, str(ds['TASKID']))
                basicstat.append(progress_info)
                tasks_to_rucio[ds['TASKID']] = ds['RRULE']
                _logger.debug('Length of progress data: {}, task {}, RSE {}'.format(len(progress_info), ds['TASKID'], se))
        if basicstat:
            datamerged = pd.concat([s for s in basicstat], axis=1)
            _logger.debug('Merged data shape: {}'.format(datamerged.shape))
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
            report['outliers'] = outliers.tolist()
            output[se] = report
            if len(list(filter(lambda x: x, report['outliers']))) > 0:
                outliers_tasks_rucio = [(tasksids[idx], tasks_to_rucio.get(int(tasksids[idx]), None)) for idx, state in enumerate(report['outliers']) if state]
                output_table.setdefault(se, []).extend(outliers_tasks_rucio)
    # dict -> list of output table
    output_table_list = []
    for se, outlier_data in output_table.items():
        for row in outlier_data:
            output_table_list.append({'rse': se, 'jeditaskid': row[0], 'rucio_rule': row[1]})
    return {'plotsdata': output, 'tasks_rucio': output_table_list}


def extractTasksIds(datasets):
    tasksIDs = []
    for se,datasets  in datasets.items():
        for dataset in datasets:
            tasksIDs.append(dataset["TASKID"])
    return tasksIDs


def send_report_rse(rse, data, experts_only=True):
    mail_template = "templated_email/dataCarouselStagingAlert.html"
    max_mail_attempts = 10
    subject = "{} Data Carousel Alert for {} {}".format(
        settings.EMAIL_SUBJECT_PREFIX,
        rse,
        '(likely *not* tape related problem)' if experts_only else '')

    rquery = {'report': 'dc_stalled', 'type__in': ['all', ]}
    if not experts_only:
        rquery['type__in'].append(rse)
    recipient_list = list(ReportEmails.objects.filter(**rquery).values('email', 'type'))
    recipient_list = list(set([r['email'] for r in recipient_list]))

    cache_key = "mail_sent_flag_{RSE}".format(RSE=rse)
    if not cache.get(cache_key, False):
        is_sent = False
        i = 0
        while not is_sent:
            i += 1
            if i > 1:
                # put 10 seconds delay to bypass the message rate limit of smtp server
                time.sleep(10)
            is_sent = send_mail_bp(mail_template, subject, data, recipient_list, send_html=True)
            _logger.debug("Email to {} attempted to send with result {}".format(','.join(recipient_list), is_sent))
            if i >= max_mail_attempts:
                break

        if is_sent:
            cache.set(cache_key, "1", settings.DATA_CAROUSEL_MAIL_REPEAT*24*3600)


def staging_rule_verification(rule_id: str, rse: str) -> (bool, list):
    """
    Check if a cause of a stalled rule is tape or disk
    Got logic from ProdSys2 https://github.com/PanDAWMS/panda-bigmon-atlas/blob/main-py3/atlas/prestage/views.py
    :param rule_id:
    :param rse:
    :return: bool: if any of files stuck due to tape problem
    :return: list: list of stuck files
    """
    stuck_days = 10
    rucio = ruciowrapper()
    # Get list of files which are not yet staged
    stuck_files = [file_lock['name'] for file_lock in rucio.client.list_replica_locks(rule_id) if file_lock['state'] != 'OK']
    # Check rucio claims it's Tape problem:
    rule_info = rucio.client.get_replication_rule(rule_id)
    if rule_info.get('error') and ('[TAPE SOURCE]' in rule_info.get('error')):
        return True, stuck_files
    # Check in ES that files have failed attempts from tape. Limit to 1000 files, should be enough
    es_conn = create_es_connection(instance='es-monit', timeout=10000)
    start_time = rule_info.get('created_at', None)
    days_since_start = (datetime.datetime.now() - start_time).days if start_time else settings.DATA_CAROUSEL_MAIL_REPEAT
    sources = list(getCRICSEs().get(rse, []))
    s = Search(using=es_conn, index='monit_prod_ddm_enr_*').\
        query("terms", data__name=stuck_files[:1000]).\
        query("range", **{
                "metadata.timestamp": {
                    "gte": f"now-{days_since_start}d/d",
                    "lt": "now/d"
                }}).\
        query("match", data__event_type='transfer-failed').\
        query('terms', data__src_endpoint=sources)
    if s.count() > 0:
        return True, stuck_files
    return False, stuck_files
