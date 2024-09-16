"""
    A set of functions needed for DataCarousel app
"""

import logging
import time
import datetime
import pandas as pd

from opensearchpy import Search

from django.core.cache import cache
from django.db import connection

from core.reports.sendMail import send_mail_bp
from core.reports.models import ReportEmails
from core.views import setupView, initRequest
from core.libs.exlib import dictfetchall, get_tmp_table_name, convert_epoch_to_datetime, insert_to_temp_table
from core.libs.elasticsearch import create_os_connection
from core.schedresource.utils import getCRICSEs
from core.filebrowser.ruciowrapper import ruciowrapper

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def getStagingData(request):
    valid, response = initRequest(request)
    if not valid:
        return response

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
        transactionKey =  insert_to_temp_table(taskl)
        selection += "and t2.taskid in (SELECT tmp.id FROM %s tmp where transactionkey=%i)"  % (tmpTableName, transactionKey)
    else:
        selection += "and t2.TASKID in (select taskid from ATLAS_DEFT.T_ACTION_STAGING)"

    if source:
        sourcel = [source] if ',' not in source else [rse for rse in source.split(',')]
        selection += " AND t1.SOURCE_RSE in (" + ','.join('\''+str(x)+'\'' for x in sourcel) + ")"

    if destination:
        destinationl = [destination] if ',' not in destination else [rse for rse in destination.split(',')]
        selection += " AND t1.DESTINATION_RSE in (" + ','.join('\'' + str(x) + '\'' for x in destinationl) + ")"

    if campaign:
        campaignl = [campaign] if ',' not in campaign else [camp for camp in campaign.split(',')]
        if 'Unknown' in campaignl:
            campaignl.remove('Unknown')
            if len(campaignl) > 0:
                selection += " AND (t3.campaign in (" + ','.join('\''+str(x)+'\'' for x in campaignl) + ") OR t3.campaign is null)"
            else:
                selection += " AND t3.campaign is null"
        else:
            selection += " AND t3.campaign in (" + ','.join('\''+str(x)+'\'' for x in campaignl) + ")"

    if processingtype:
        processingtypel = [processingtype] if ',' not in processingtype else [pt for pt in processingtype.split(',')]
        if 'analysis' in processingtypel:
            processingtypel.remove('analysis')
            if processingtypel and len(processingtypel) > 0:
                selection += " AND (t4.processingtype in (" + ','.join('\''+str(x)+'\'' for x in processingtypel) + ") OR t4.tasktype='anal')"
            else:
                selection += " AND t4.tasktype='anal'"
        else:
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
            t1.update_time, t1.source_rse, t1.destination_rse, t1.dataset_bytes, t1.staged_bytes,
            t2.taskid, t3.campaign, t3.pr_id,
            row_number() over(partition by t1.dataset_staging_id order by t1.start_time desc) as occurence, 
            (current_timestamp-t1.update_time) as update_time, t4.processingtype, t2.step_action_id 
        from atlas_deft.t_dataset_staging t1
        inner join {0}.t_action_staging t2 on t1.dataset_staging_id=t2.dataset_staging_id
        inner join {0}.t_production_task t3 on t2.taskid=t3.taskid 
        inner join {1}.jedi_tasks t4 on t2.taskid=t4.jeditaskid {2} 
        """.format('atlas_deft', settings.DB_SCHEMA_PANDA, selection))
    datasets = dictfetchall(new_cur, style='lowercase')
    for dataset in datasets:
        # Sort out requests by request on February 19, 2020
        if dataset['status'] in ('staging', 'queued', 'done'):
            dataset = {k.lower(): v for k, v in dataset.items()}
            datasetname = dataset.get('dataset')
            if ':' in datasetname:
                dataset['scope'] = datasetname.split(':')[0]
            else:
                dataset['scope'] = datasetname.split('.')[0]
            data[dataset['taskid']] = dataset
    return data


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
    os_conn = create_os_connection(instance='monit-opensearch', timeout=10000)
    start_time = rule_info.get('created_at', None)
    days_since_start = (datetime.datetime.now() - start_time).days if start_time else settings.DATA_CAROUSEL_MAIL_REPEAT
    sources = list(getCRICSEs().get(rse, []))
    s = Search(using=os_conn, index='monit_prod_ddm_enr_*').\
        query("terms", data__name=stuck_files[:1000]).\
        query("range", **{
                "metadata.timestamp": {
                    "gte": f"now-{days_since_start}d/d",
                    "lt": "now/d"
                }}).\
        query("match", data__event_type='transfer-failed').\
        query("prefix", data__reason='STAGING').\
        query('terms', data__src_endpoint=sources)
    res = list([h['data'] for h in s.scan()])
    if len(res) > 0:
        # filter files having failures with error message starting with "STAGING"
        file_names = set([r['name'] for r in res])
        stuck_files = [f"{rule_info['scope']}:{f}" for f in stuck_files if f in file_names]
        if len(stuck_files) > 0:
            return True, stuck_files
    return False, stuck_files


def get_stuck_files_data(rule_id, source_rse):
    """
    Get stuck files and info of failed transfers
    :param rule_id:
    :param source_rse:
    :return: stuck_files_info: dict
    """
    stuck_files_info = {}
    rucio = ruciowrapper()
    # Get list of files which are not yet staged
    stuck_files = [file_lock['name'] for file_lock in rucio.client.list_replica_locks(rule_id) if file_lock['state'] != 'OK']
    if len(stuck_files) > 0:
        stuck_files_info = {f: {'transfers': []} for f in stuck_files}
        # Check rucio claims it's Tape problem:
        rule_info = rucio.client.get_replication_rule(rule_id)
        # Check in ES that files have failed attempts from tape. Limit to 1000 files, should be enough
        os_conn = create_os_connection(instance='monit-opensearch', timeout=10000)
        start_time = rule_info.get('created_at', None)
        days_since_start = (datetime.datetime.now() - start_time).days if start_time else settings.DATA_CAROUSEL_MAIL_REPEAT
        sources = list(getCRICSEs().get(source_rse, []))
        s = Search(using=os_conn, index='monit_prod_ddm_enr_*').\
            query("terms", data__name=stuck_files[:1000]).\
            query("range", **{
                    "metadata.timestamp": {
                        "gte": f"now-{days_since_start}d/d",
                        "lt": "now/d"
                    }}).\
            query("match", data__event_type='transfer-failed'). \
            query("match", data__activity='Staging'). \
            query('terms', data__src_endpoint=sources)

        for hit in s:
            if hit['data']['name'] in stuck_files_info:
                stuck_files_info[hit['data']['name']]['transfers'].append({
                    'lfn': hit['data']['name'],
                    'src': hit['data']['src_rse'],
                    'dst': hit['data']['dst_rse'],
                    'transfer_link': hit['data']['transfer_link'],
                    'submitted_at': convert_epoch_to_datetime(
                        hit['data']['submitted_at']).strftime(settings.DATETIME_FORMAT),
                    'started_at': convert_epoch_to_datetime(
                        hit['data']['started_at']).strftime(settings.DATETIME_FORMAT),
                    'transferred_at': convert_epoch_to_datetime(
                        hit['data']['transferred_at']).strftime(settings.DATETIME_FORMAT),
                    'duration': hit['data']['duration'],
                    'reason': hit['data']['reason'],
                    'transfer_id': hit['data']['transfer_id'],
                })

    return stuck_files_info

def getBinnedData(timestamps_list, additional_timestamps_list_1 = None, additional_timestamps_list_2 = None):
    isTimeNotDelta = True
    timesadd1 = None
    timesadd2 = None

    try:
        times = pd.to_datetime(timestamps_list)
        if additional_timestamps_list_1:
            timesadd1 = pd.to_datetime(additional_timestamps_list_1)
        if additional_timestamps_list_2:
            timesadd2 = pd.to_datetime(additional_timestamps_list_2)
    except:
        times = pd.to_timedelta(timestamps_list)
        isTimeNotDelta = False
        if additional_timestamps_list_1:
            timesadd1 = pd.to_timedelta(additional_timestamps_list_1)
        if additional_timestamps_list_2:
            timesadd2 = pd.to_timedelta(additional_timestamps_list_2)

    df = pd.DataFrame({
        "Count1": [1 for _ in timestamps_list]
    }, index=times)

    if not timesadd1 is None:
        dfadd = pd.DataFrame({
            "Count2": [1 for _ in additional_timestamps_list_1]
        }, index=timesadd1)
        result = pd.concat([df, dfadd])
    else:
        result = df

    if not timesadd2 is None:
        dfadd = pd.DataFrame({
            "Count3": [1 for _ in additional_timestamps_list_2]
        }, index=timesadd2)
        result = pd.concat([result, dfadd])

    grp = result.groupby([pd.Grouper(freq="24h")]).count()
    values = grp.values.tolist()
    if isTimeNotDelta:
        index = grp.index.to_pydatetime().tolist()
    else:
        index = (grp.index / pd.Timedelta(hours=1)).tolist()

    if not additional_timestamps_list_1 is None and len(additional_timestamps_list_1) == 0:
        tmpval = []
        for item in values:
            if additional_timestamps_list_2:
                tmpval.append([item[0], 0, item[1]])
            else:
                tmpval.append([item[0], 0])
        values = tmpval

    if not additional_timestamps_list_2 is None and len(additional_timestamps_list_2) == 0:
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

