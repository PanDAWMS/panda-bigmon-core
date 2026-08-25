"""
    A set of functions needed for DataCarousel app
"""
import copy
import logging
import json
import time
import datetime
import math
import pandas as pd

try:
    from opensearch_dsl import Search
except ImportError:
    from opensearchpy import Search

from django.core.cache import cache
from django.db import connection
from django.utils import timezone
from core.reports.sendMail import send_mail_bp
from core.reports.models import ReportEmails
from core.views import setupView
from core.libs.checks import is_positive_int_field
from core.libs.exlib import dictfetchall, get_tmp_table_name, convert_epoch_to_datetime, insert_to_temp_table, convert_bytes, convert_sec
from core.libs.elasticsearch import create_os_connection
from core.schedresource.utils import getCRICSEs
from core.filebrowser.ruciowrapper import ruciowrapper
from core.iDDS.utils import get_idds_data_for_tasks

from django.conf import settings
import core.datacarousel.constants as const

_logger = logging.getLogger('bigpandamon')


def setup_view_dc(request):
    """
    Process Data Carousel related params from the request and add them to query
    :param request:
    :return: extra_query: str - query to be added to the where clause
    """
    extra_str = "where (1=1)"
    query, _, _ = setupView(request, wildCardExt=True)
    time_window = query['modificationtime__castdate__range']

    request_params = copy.deepcopy(request.session['requestParams'])

    campaign_column = "t3.campaign"
    processingtype_column = "t3.processingtype"
    tasktype_column = "t3.tasktype"
    taskid_column = "task_id"
    tasks_table = "ATLAS_PANDA.DATA_CAROUSEL_RELATIONS"
    source_rse = "TAPE"

    if 'jeditaskid' in request_params:
        taskl = request_params['jeditaskid'].split('|')
        if len(taskl) > settings.DB_N_MAX_IN_QUERY:
            transaction_key = insert_to_temp_table(taskl)
            extra_str += f" and t2.{taskid_column} in (select id from {get_tmp_table_name()} where transactionkey={transaction_key})"
        else:
            extra_str += f" and t2.{taskid_column} in ({','.join([str(x) for x in taskl])})"
    else:
        extra_str += f" and t2.{taskid_column} in (select {taskid_column} from {tasks_table})"

        extra_str += f""" 
        and (
            t1.end_time between to_date('{time_window[0]}', 'YYYY-mm-dd HH24:MI:SS') and to_date('{time_window[1]}', 'YYYY-mm-dd HH24:MI:SS') 
            or (t1.end_time is null and not (t1.status = 'done'))
        )
        """

    if 'source' in request_params or 'source_rse' in request_params:
        source = request_params['source'] if 'source' in request_params else request_params['source_rse']
        quoted_values = ",".join(f"'{str(x)}'" for x in source.split(","))
        extra_str += f" AND t1.SOURCE_{source_rse} in ({quoted_values})"

    if 'destination' in request_params:
        dest_values = request_params['destination'].split(',')
        quoted_dest = ",".join(f"'{str(x)}'" for x in dest_values)
        extra_str += f" AND t1.DESTINATION_RSE in ({quoted_dest})"

    if 'campaign' in request_params:
        campaignl = request_params['campaign'].split(',')
        if 'Unknown' in campaignl:
            campaignl.remove('Unknown')
            if len(campaignl) > 0:
                extra_str += " AND (" + campaign_column + " in (" + ','.join(
                    '\'' + str(x) + '\'' for x in campaignl) + ") OR " + campaign_column + "is null)"
            else:
                extra_str += " AND " + campaign_column + " is null"
        else:
            extra_str += " AND " + campaign_column + " in (" + ','.join('\'' + str(x) + '\'' for x in campaignl) + ")"

    if 'processingtype' in request_params:
        processingtypel = request_params['processingtype'].split(',')
        if 'analysis' in processingtypel:
            processingtypel.remove('analysis')
            if processingtypel and len(processingtypel) > 0:
                extra_str += " AND (" + processingtype_column + " in (" + ','.join(
                    '\'' + str(x) + '\'' for x in processingtypel) + ") OR " + processingtype_column + "='anal')"
            else:
                extra_str += " AND " + tasktype_column + "='anal'"
        else:
            extra_str += " AND " + processingtype_column + " in (" + ','.join('\'' + str(x) + '\'' for x in processingtypel) + ")"

    if 'username' in request_params:
        usernamel = request_params['username'].split(',')
        extra_str += " AND t3.username in (" + ','.join('\'' + str(x) + '\'' for x in usernamel) + ")"

    return extra_str


def prepare_dsdata(dsdata):
    for key in ['processingtype', 'source_rse', 'campaign', 'username', 'destination_rse']:
        if dsdata.get(key) is None:
            dsdata[key] = 'Unknown'
        if key == "source_rse" and 'source_rse_breakdown' not in dsdata:
            dsdata['source_rse_breakdown'] = substitudeRSEbreakdown(dsdata['source_rse'])
        if dsdata.get('processingtype', '').startswith('panda-client'):
            dsdata['processingtype'] = 'analysis'


def get_staging_data(extra_str, add_idds_data=False):
    """
    Get staging data from the database
    :param extra_str:
    :param add_idds_data:
    :return:
    """
    data = []


    sql_query = f"""
        SELECT
            t1.request_id,
            t1.dataset,
            t1.status,
            t1.staged_files,
            t1.start_time,
            t1.creation_time,
            t1.end_time,
            t1.ddm_rule_id AS rse,
            t1.total_files,
            t1.modification_time AS modification_time,
            t1.last_staged_time,
            t1.source_tape as source_rse,
            t1.source_rse as source_rse_old,
            t1.destination_rse,
            t1.dataset_size AS dataset_bytes,
            t1.staged_size AS staged_bytes,
            t2.task_id AS taskid,
            t3.reqid as pr_id,
            t3.processingtype,
            t3.username,
            t3.tasktype,
            t3.campaign,
            row_number() over(partition by t1.request_id order by t1.start_time desc) as occurrence,
            (current_timestamp - t1.modification_time) AS update_time,
            (current_timestamp - t1.last_staged_time) as since_last_staged_file
        FROM {settings.DB_SCHEMA_PANDA}.data_carousel_requests t1
        INNER JOIN {settings.DB_SCHEMA_PANDA}.data_carousel_relations t2 ON t1.request_id = t2.request_id
        INNER JOIN {settings.DB_SCHEMA_PANDA}.jedi_tasks t3 ON t2.task_id = t3.jeditaskid
        {extra_str}
    """

    new_cur = connection.cursor()
    new_cur.execute(sql_query)
    datasets = dictfetchall(new_cur, style='lowercase')
    new_cur.close()

    datasets_idds_info = None
    if add_idds_data:
        datasets_idds_info = get_idds_data_for_tasks([d['taskid'] for d in datasets])

    datasets_statuses = ('staging', 'queued', 'done', 'cancelled', 'retired')

    for dataset in datasets:
        if datasets_idds_info is not None and len(datasets_idds_info) > 0 and dataset['dataset'] in datasets_idds_info:
            dataset.update(datasets_idds_info[dataset['dataset']])
        # Sort out requests by request on February 19, 2020
        if dataset['status'] in datasets_statuses:
            dataset = {k.lower(): v for k, v in dataset.items()}
            datasetname = dataset.get('dataset')
            if ':' in datasetname:
                dataset['scope'] = datasetname.split(':')[0]
            else:
                dataset['scope'] = datasetname.split('.')[0]

            prepare_dsdata(dataset)
            data.append(dataset)

    return data


def send_report_rse(rse: str, data, experts_only:bool=True) -> int:
    """
    Send email alert about stalled Data Carousel rules
    :return: 0 if email sent successfully, 1 if email sending failed, 2 if no rules to send
    """
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

    # get rules from cache and filter if it is expired and need to send again or not
    time_epoch_now = int(datetime.datetime.now().timestamp())
    data_to_send = {'rse': rse, 'name': data['name'], 'rules': []}
    cache_key = f"dc_stalled_alert_{rse}_{str(experts_only)}"
    data_cached = cache.get(cache_key, None)
    rules_cached = json.loads(data_cached) if data_cached else {}
    # clean up cache from rules that are not stuck anymore
    rules_currently_stuck = {r['rr'] for r in data['rules']}
    rules_cached = {k: v for k, v in rules_cached.items() if k in rules_currently_stuck}
    for rule in data['rules']:
        cached_rule = rules_cached.get(rule['rr'], None)
        if cached_rule is None or cached_rule['mail_delay_till'] < time_epoch_now:
            # new or expired rule - set expiration and send alert
            rule['mail_delay_till'] = time_epoch_now + const.DATA_CAROUSEL_MAIL_REPEAT * 24 * 3600
            data_to_send['rules'].append(rule)
            rules_cached[rule['rr']] = rule

    # save updated cache if any
    if len(data_to_send['rules']) > 0:
        cache.set(cache_key, json.dumps(rules_cached), const.DATA_CAROUSEL_MAIL_REPEAT * 24 * 3600)

    # sort rules, newest first
    if len(data_to_send) > 0 and len(data_to_send['rules']) > 0:
        is_sent = False
        i = 0
        while not is_sent:
            i += 1
            if i > 1:
                # put 10 seconds delay to bypass the message rate limit of smtp server
                time.sleep(10)
            is_sent = send_mail_bp(mail_template, subject, data_to_send, recipient_list, send_html=True)
            _logger.debug("Email to {} attempted to send with result {}".format(','.join(recipient_list), is_sent))
            if i >= max_mail_attempts:
                break

        if is_sent:
            return 0
        else:
            _logger.error("Failed to send email to {} after {} attempts".format(','.join(recipient_list), max_mail_attempts))
            return 1

    _logger.info(f"The delay between emails {const.DATA_CAROUSEL_MAIL_REPEAT} days not reached, not sending email")
    return 2



def staging_rule_verification(rule_id: str, rse: str) -> tuple[bool, list]:
    """
    Check if a cause of a stalled rule is tape or disk
    Got logic from ProdSys2 https://github.com/PanDAWMS/panda-bigmon-atlas/blob/main-py3/atlas/prestage/views.py
    :param rule_id:
    :param rse:
    :return: bool: if any of files stuck due to tape problem
    :return: list: list of stuck files
    """
    is_tape_problem = False
    rucio = ruciowrapper()
    # Get list of files which are not yet staged
    stuck_files = [{
        'name':file_lock['name'],
        'scope': file_lock['scope'],
        'errors': []
    } for file_lock in rucio.client.list_replica_locks(rule_id) if file_lock['state'] != 'OK']
    # Check rucio claims it's Tape problem:
    rule_info = rucio.client.get_replication_rule(rule_id)
    if rule_info.get('error') and ('[TAPE SOURCE]' in rule_info.get('error')):
        is_tape_problem = True
    # Check in ES that files have failed attempts from tape. Limit to 1000 files, should be enough
    os_conn = create_os_connection(instance='monit-opensearch', timeout=10000)
    start_time = rule_info.get('created_at', None)
    days_since_start = (datetime.datetime.now() - start_time).days if start_time else const.DATA_CAROUSEL_MAIL_REPEAT
    sources = list(getCRICSEs().get(rse, []))
    s = Search(using=os_conn, index='monit_prod_ddm_enr_*').\
        query("terms", data__name=[f['name'] for f in stuck_files[:1000]]).\
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
        file_errors = {}
        for r in res:
            if r['name'] not in file_errors:
                file_errors[r['name']] = []
            file_errors[r['name']].append(r['reason'])
        stuck_files = [{
            'name': f"{f['scope']}:{f['name']}",
            'errors': file_errors[f['name']][:3] if len(file_errors[f['name']]) > 3 else file_errors[f['name']]
        } for f in stuck_files if f['name'] in file_errors]
        if len(stuck_files) > 0:
            is_tape_problem = True
    return is_tape_problem, stuck_files


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
        days_since_start = (datetime.datetime.now() - start_time).days if start_time else const.DATA_CAROUSEL_MAIL_REPEAT
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
                        hit['data']['submitted_at']).strftime(settings.DATETIME_FORMAT) if 'submitted_at' in hit['data'] else '-',
                    'started_at': convert_epoch_to_datetime(
                        hit['data']['started_at']).strftime(settings.DATETIME_FORMAT) if 'started_at' in hit['data'] else '-',
                    'transferred_at': convert_epoch_to_datetime(
                        hit['data']['transferred_at']).strftime(settings.DATETIME_FORMAT) if 'transferred_at' in hit['data'] else '-',
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



def build_summary_data(staginData):

    summary = {
        'processingtype': {},
        'source_rse': {},
        'campaign': {},
        'username': {},
        'destination_rse': {},
    }
    ts_submitted = []
    ts_submitted_files = []
    ts_interval_fin = []
    ts_interval_act = []
    ts_interval_queued = []
    progress_distribution = []
    seen_request_ids = set()

    calc_temp = {
        "ds_active": 0, "ds_done": 0, "ds_queued": 0, "ds_90pdone": 0,
        'files_total': 0, "files_rem": 0, "files_queued": 0, "files_done": 0, 'files_active': 0,
        'bytes_total': 0, 'bytes_done': 0, "bytes_queued": 0, 'bytes_rem': 0, 'bytes_active': 0,
    }

    for dsdata in staginData:
        if dsdata.get('request_id') in seen_request_ids:
            continue
        seen_request_ids.add(dsdata.get('request_id'))

        for key in summary:
            if key == 'destination_rse' and dsdata['status'] == 'queued':
                continue

            if dsdata[key] not in summary[key]:
                summary[key][dsdata[key]] = {key: dsdata[key]}
                summary[key][dsdata[key]].update(calc_temp)
                if key == "source_rse":
                    summary[key][dsdata[key]]['source_rse_breakdown'] = substitudeRSEbreakdown(dsdata['source_rse'])

            if is_positive_int_field(dsdata, 'total_files'):
                summary[key][dsdata[key]]['files_total'] += dsdata['total_files']
            if is_positive_int_field(dsdata, 'staged_files'):
                summary[key][dsdata[key]]['files_done'] += dsdata['staged_files']
            if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                summary[key][dsdata[key]]['files_rem'] += dsdata['total_files'] - dsdata['staged_files']
            if is_positive_int_field(dsdata, 'dataset_bytes'):
                summary[key][dsdata[key]]['bytes_total'] += convert_bytes(dsdata['dataset_bytes'], 'GB')
            if is_positive_int_field(dsdata, 'staged_bytes'):
                summary[key][dsdata[key]]['bytes_done'] += convert_bytes(dsdata['staged_bytes'], 'GB')
            if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                summary[key][dsdata[key]]['bytes_rem'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')

            if dsdata['end_time'] is not None and dsdata['start_time'] is not None:
                summary[key][dsdata[key]]['ds_done'] += 1
                ts_interval_fin.append(dsdata['end_time'] - dsdata['start_time'])
            elif dsdata['status'] != 'queued':
                summary[key][dsdata[key]]['ds_active'] += 1
                if dsdata['start_time'] is not None:
                    ts_interval_act.append(timezone.now() - dsdata['start_time'])
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                    summary[key][dsdata[key]]['files_active'] += dsdata['total_files'] - dsdata['staged_files']
                if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                    summary[key][dsdata[key]]['bytes_active'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files') and dsdata['staged_files'] >= dsdata['total_files'] * 0.9:
                    summary[key][dsdata[key]]['ds_90pdone'] += 1
            elif dsdata['status'] == 'queued':
                summary[key][dsdata[key]]['ds_queued'] += 1
                ts_interval_queued.append(timezone.now() - dsdata['creation_time'])
                if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
                    summary[key][dsdata[key]]['files_queued'] += dsdata['total_files'] - dsdata['staged_files']
                if is_positive_int_field(dsdata, 'dataset_bytes') and is_positive_int_field(dsdata, 'staged_bytes'):
                    summary[key][dsdata[key]]['bytes_queued'] += convert_bytes(dsdata['dataset_bytes'] - dsdata['staged_bytes'], 'GB')

        if dsdata['start_time'] is not None:
            ts_submitted.append(dsdata['start_time'])
            ts_submitted_files.append([dsdata['start_time'], dsdata['total_files']])
        if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files'):
            progress_distribution.append(dsdata['staged_files'] / dsdata['total_files'])

    return summary, ts_submitted, ts_submitted_files, ts_interval_act, ts_interval_fin, ts_interval_queued, progress_distribution


def build_dataset_list(staginData):
    dataset_list = []

    for dsdata in staginData:
        epltime = None
        if dsdata.get('end_time') and dsdata.get('start_time'):
            epltime = dsdata['end_time'] - dsdata['start_time']
        elif dsdata.get('start_time'):
            epltime = timezone.now() - dsdata['start_time']
        elif dsdata.get('creation_time'):
            epltime = timezone.now() - dsdata['creation_time']

        dataset_list.append({
            'campaign': dsdata['campaign'],
            'username': dsdata['username'],
            'pr_id': dsdata['pr_id'],
            'taskid': dsdata['taskid'],
            'dataset': dsdata['dataset'],
            'status': dsdata['status'],
            'total_files': dsdata['total_files'] if is_positive_int_field(dsdata, 'total_files') else 0,
            'staged_files': dsdata['staged_files'] if is_positive_int_field(dsdata, 'staged_files') else 0,
            'size': int(
                round(convert_bytes(dsdata['dataset_bytes'], 'GB'))
            ) if is_positive_int_field(dsdata, 'dataset_bytes') else "0",
            'progress': int(
                math.floor(dsdata['staged_files'] * 100.0 / dsdata['total_files'])
            ) if is_positive_int_field(dsdata, 'total_files') and is_positive_int_field(dsdata, 'staged_files') else 0,
            'source_rse': dsdata['source_rse'],
            'destination_rse': dsdata['destination_rse'] if dsdata.get('destination_rse') else '---',
            'elapsedtime': convert_sec(epltime.total_seconds(), 'str') if epltime is not None else '---',
            'start_time': dsdata['start_time'].strftime(settings.DATETIME_FORMAT) if dsdata['start_time'] else '---',
            'rrule': dsdata['rse'],
            'update_time': convert_sec(dsdata['update_time'].total_seconds(), 'str') if dsdata.get('update_time') else '---',
            'processingtype': dsdata['processingtype']
        })
    return dataset_list