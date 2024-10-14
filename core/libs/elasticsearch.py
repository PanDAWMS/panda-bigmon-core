import logging
import json
import requests
from requests.auth import HTTPBasicAuth
import hashlib
from datetime import datetime
from opensearchpy import OpenSearch, Search, Q
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search as ESearch, Q as EQ

from core.pandajob.models import Jobsactive4
from core.libs.DateTimeEncoder import DateTimeEncoder
from django.conf import settings
from urllib.parse import urlparse
from itertools import zip_longest

_logger = logging.getLogger('bigpandamon')

def get_os_credentials(instance):
    """
    Getting credentials from settings
    :param instance: str, os-atlas or monit-opensearch
    :return:
    """
    os_host = None
    os_user = None
    os_password = None
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        protocol = 'https'
        if instance == 'os-atlas' and hasattr(settings, 'OS'):
            os_host = settings.OS.get('osHost', None)
            os_port = settings.OS.get('osPort', None)
            os_host = protocol +'://' + os_host + ':' + os_port + '/os' if os_host else None
            os_user = settings.OS.get('osUser', None)
            os_password = settings.OS.get('osPassword', None)
        elif instance == 'monit-opensearch' and hasattr(settings, 'MONIT_OPENSEARCH'):
            os_host = settings.MONIT_OPENSEARCH.get('osHost', None)
            os_port = settings.MONIT_OPENSEARCH.get('osPort', None)
            os_host = protocol + '://' + os_host + ':' + os_port + '/os' if os_host else None
            os_user = settings.MONIT_OPENSEARCH.get('osUser', None)
            os_password = settings.MONIT_OPENSEARCH.get('osPassword', None)
        if any(i is None for i in (os_host, os_user, os_password)):
            raise Exception('OS cluster credentials was not found in settings')
    else:
        if hasattr(settings, 'ES_CLUSTER'):
            es_host = settings.ES_CLUSTER.get('esHost', '')
            es_port = settings.ES_CLUSTER.get('esPort', '9200')
            es_protocol = settings.ES_CLUSTER.get('esProtocol', 'http')
            es_path = settings.ES_CLUSTER.get('esPath', '')
            es_host = es_protocol + '://' + es_host + ':' + es_port + es_path
            es_user = settings.ES_CLUSTER.get('esUser', '')
            es_password = settings.ES_CLUSTER.get('esPassword', '')
            os_host = es_host
            os_user = es_user
            os_password = es_password

    return os_host, os_user, os_password

def create_os_connection(instance='os-atlas', timeout=2000, max_retries=10, retry_on_timeout=True):
    """
    Create a connection to OpenSearch cluster
    """
    os_host, os_user, os_password = get_os_credentials(instance)
    try:
        parsed_uri = urlparse(os_host)
        protocol = '{uri.scheme}'.format(uri=parsed_uri)

        if settings.DEPLOYMENT == 'ORACLE_ATLAS':
            if protocol == 'https':
                ca_certs = settings.OS_CA_CERT

                connection = OpenSearch(
                    [os_host],
                    http_auth=(os_user, os_password),
                    verify_certs=True,
                    timeout=timeout,
                    max_retries=max_retries,
                    retry_on_timeout=retry_on_timeout,
                    ca_certs=ca_certs
                )
            else:
                connection = OpenSearch(
                    [os_host],
                    http_auth=(os_user, os_password),
                    timeout=timeout,
                    max_retries=max_retries,
                    retry_on_timeout=retry_on_timeout)
        else:
            connection = Elasticsearch (
                    [os_host],
                    http_auth=(os_user, os_password),
                    timeout=timeout,
                    max_retries=max_retries,
                    retry_on_timeout=retry_on_timeout)

        return connection

    except Exception as ex:
        _logger.error(ex)
    return None

def get_date(item):
    return datetime.strptime(item['@timestamp'],  '%Y-%m-%dT%H:%M:%S.%fZ')

def get_payloadlog(id, os_conn, index, start=0, length=50, mode='pandaid', sort='asc', search_string=''):
    """
    Get pilot logs from ATLAS OpenSearch storage
    """
    logs_list = []
    query = {}
    jobs = []
    total = 0
    flag_running_job = True

    end = start + length
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        s = Search(using=os_conn, index=index)
        q = Q("multi_match", query=search_string, fields=['level', 'message'])
    else:
        s = ESearch(using=os_conn, index=index)
        q = EQ("multi_match", query=search_string, fields=['level', 'message'])

    s = s.source(["@timestamp", "@timestamp_nanoseconds", "level", "message", "PandaJobID", "TaskID",
                  "Harvester_WorkerID", "Harvester_ID"])

    if mode == 'pandaid':
        query['pandaid'] = int(id)
        jobs.extend(Jobsactive4.objects.filter(**query).values())
        if len(jobs) == 0:
            flag_running_job = False
        if sort == 'asc':
            s = s.query('match', PandaJobID='{0}'.format(id)).sort("@timestamp")
        else:
            s = s.query('match', PandaJobID='{0}'.format(id)).sort("-@timestamp")
        if search_string != '':
            s = s.query(q)
    elif mode == 'jeditaskid':
        s = s.query('match', TaskID='{0}'.format(id)).sort("@timestamp")
    try:
        _logger.debug('OpenSearch query: {0}'.format(str(s.to_dict())))
        response = s[start:end].execute()

        total = response.hits.total.value

        for hit in response:
            logs_list.append(hit.to_dict())
    except Exception as ex:
        _logger.error(ex)

    return logs_list, flag_running_job, total

def upload_data(os_conn, index_name_base, data, timestamp_param='creationdate', id_param='jeditaskid'):
    """
    Push data to OpenSearch cluster
    :param os_conn: connection to use
    :param index_name_base: name of ES index where data should be written
    :param data: list of dicts
    :param timestamp_param: name of parameter in data dict to be used for ES index creation
    :param id_param: name of parameter in data which should be unique
    :return:
    """
    result = {'status': '', 'message': '', 'link': ''}
    index_name_postfixes = []
    date_current = datetime.now()
    for item in data:
        if timestamp_param in item and isinstance(item[timestamp_param], datetime):
            index_name_postfixes.append(item[timestamp_param].strftime('%Y.%m'))
        else:
            index_name_postfixes.append(date_current.strftime('%Y.%m'))
    index_name_postfixes = list(set(index_name_postfixes))
    index_names = [f"{index_name_base}-{postfix}" for postfix in index_name_postfixes]

    # crete index if it does not exist yet
    for index_name in index_names:
        while True:
            try:
                if not os_conn.indices.exists(index=index_name):
                    _logger.info(f"Creating index: {index_name}")
                    os_conn.indices.create(index=index_name)
                    _logger.info(f"Index created")
                    break
                else:
                    break
            except Exception as ex:
                _logger.exception(ex)

    jsons = []
    for doc in data:
        if timestamp_param in doc and isinstance(doc[timestamp_param], datetime):
            full_index_name = f"{index_name_base}-{doc[timestamp_param].strftime('%Y.%m')}"
            doc['@timestamp'] = str(doc[timestamp_param]).replace(' ', 'T') + 'Z'
        else:
            full_index_name = f"{index_name_base}-{date_current.strftime('%Y.%m')}"
            doc['@timestamp'] = str(date_current).replace(' ', 'T') + 'Z'

        hash_string = " ".join([
            str(doc[id_param])
        ])
        _id = hashlib.sha1(hash_string.encode()).hexdigest()
        jsons.append('{"index": {"_index": "%s", "_id": "%s"}}\n%s\n' % (
            full_index_name, _id, json.dumps(doc, cls=DateTimeEncoder)))

    # join records to send via POST requests
    data = ''.join(jsons)

    # send data via POST request
    os_host, os_user, os_password = get_os_credentials(instance='os-atlas')
    if '/' in os_host:
        os_host = os_host.split('/')[0]
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(
        f"https://{os_host}/os/_bulk",
        data=data,
        headers=headers,
        auth=HTTPBasicAuth(os_user, os_password),
        verify='/etc/pki/tls/certs/ca-bundle.trust.crt',
        timeout=120
    )
    if response.status_code != 200:
        result['status'] = 'error'
        result['message'] = f"{response}: {response.text}"
        _logger.error(result['message'])
        raise ConnectionError(result['message'])
    else:
        result['status'] = 'success'
        result['message'] = "Successfully pushed data to the ES cluster"
        result['link'] = "https://os-atlas.cern.ch/kibana/goto/c987a191e5fa02605e20e5e6eaa9bc1f?security_tenant=global"
        _logger.info(result['message'])


    return result

def get_split_rule_info(os_host, jeditaskid):
    """
    Get split rule entries from ATLAS Elastic
    :param os_host: connection to the ATLAS Elastic
    :param jeditaskid: unique task ID
    :return: split rule messages
    """
    split_rules = []
    jedi_logs_index = settings.OS_INDEX_JEDI_LOGS

    s = Search(using=os_host, index=jedi_logs_index)
    s = s.source(['@timestamp', 'message'])
    s = s.filter('term', jediTaskID='{0}'.format(jeditaskid))
    q = Q("match", message='change_split_rule')
    s = s.query(q)
    response = s.scan()
    for hit in response:
        values = hit.to_dict().values()
        str_values = [str(x) for x in values]
        split_rules.append('\t'.join(str_values))

    return split_rules

def get_gco2_sum_for_tasklist(task_list=None):
    """
    Getting sum of gCO2 for list of tasks from OS-ATLAS
    :param: tasks_list: list of jeditaskid
    :return: gco2_sum
    """
    gco2_sum = {'total': 0, 'finished': 0, 'failed': 0}
    if task_list is None:
        return gco2_sum

    os_jobs_index = 'atlas_jobs_archived*'
    os_conn = create_os_connection(instance='os-atlas')

    chunk_size = 50000
    task_list_chunks = list(chunks(task_list, chunk_size))

    for chunk in task_list_chunks:
        s = Search(using=os_conn, index=os_jobs_index)

        s.filter('range', **{
            '@timestamp': {'gte': 'now-2y', 'lte': 'now'}
        })
        s = s.query("terms", jeditaskid=chunk)
        s.aggs.bucket('jobstatus', 'terms', field='jobstatus.keyword') \
            .metric('sum_gco2global', 'sum', field='gco2global') \
            .metric('sum_gco2regional', 'sum', field='gco2regional')

        response = s.execute()

        for js in response.aggregations['jobstatus']:
            if js['key'] in gco2_sum:
                gco2_sum[js['key']] += js['sum_gco2global']['value']

    gco2_sum['total'] = gco2_sum['finished'] + gco2_sum['failed']

    return gco2_sum

def get_os_task_status_log(db_source, jeditaskid, os_instance='os-atlas'):

    task_message_ids_list = []
    task_message_dict = {}

    jobs_info_status_dict = {}
    jobs_info_errors_dict = {}
    task_info_status_dict = {}

    full_index_name = db_source + '_tasks_status_log*'

    os_conn = create_os_connection(instance=os_instance)

    s = Search(using=os_conn, index=full_index_name)

    fields_list = ['@timestamp','db_source', 'inputs', 'job_hs06sec', 'job_inputfilebytes', 'job_nevents', 'job_ninputdatafiles',
                  'job_ninputfiles', 'job_noutputdatafiles', 'job_outputfilebytes', 'jobid', 'message_id', 'msg_type', 'status',
                   'computingsite',
                  'taskid','timestamp']
    errors_diag_fields_list = ['brokerageerrordiag', 'ddmerrordiag', 'exeerrordiag', 'jobdispatchererrordiag',
                   'piloterrordiag', 'superrordiag', 'taskbuffererrordiag']
    errors_code_fields_list = ['brokerageerrorcode', 'ddmerrorcode', 'exeerrorcode', 'jobdispatchererrorcode',
                   'piloterrorcode', 'superrorcode', 'taskbuffererrorcode']

    fields_list += errors_diag_fields_list
    fields_list += errors_code_fields_list

    s = s.source(fields_list)
    s = s.filter('term', taskid='{0}'.format(jeditaskid))
    q = Q("match", msg_type='job_status')
    s = s.query(q)

    response = s.scan()

    for hit in response:
        hit_dict = hit.to_dict()
        if not hit_dict['jobid'] in jobs_info_status_dict:
            jobs_info_status_dict[hit_dict['jobid']] = {}

        jobs_info_status_dict[hit_dict['jobid']][hit_dict['status']] = {
                                                                            'timestamp': hit_dict['timestamp'],
                                                                            'message_id': hit_dict['message_id'],
                                                                            'status': hit_dict['status'],
                                                                            'time': hit_dict['@timestamp']
                                                                        }

        task_message_ids_list.append(hit_dict['message_id'])
        task_message_dict[hit_dict['message_id']] = hit_dict

        if hit_dict['status'] in ('finished', 'failed', 'closed', 'cancelled'):
            if hit_dict['status'] in ('failed', 'closed', 'cancelled'):
                if hit_dict['jobid'] not in jobs_info_errors_dict:
                    jobs_info_errors_dict[hit_dict['jobid']] = {}
                    jobs_info_errors_dict[hit_dict['jobid']]['errors'] = []
                for field in errors_diag_fields_list:
                    if field in hit_dict and hit_dict[field] != 'NULL':
                        error_field = field.replace("diag", "")
                        error_code_field = field.replace("diag", "code")

                        jobs_info_errors_dict[hit_dict['jobid']]['errors'].append({
                            'timestamp': hit_dict['@timestamp'],
                            'error_type': error_field,
                            'code': hit_dict.get(error_code_field, "None"),
                            'text': hit_dict.get(field, "None")
                            })

            if 'job_inputfilebytes' in hit_dict and hit_dict['job_inputfilebytes'] != 'NULL':
                job_inputfilebytes = hit['job_inputfilebytes']
            else:
                job_inputfilebytes = 0

            if 'job_hs06sec' in hit_dict and hit_dict['job_hs06sec'] != 'NULL':
                job_hs06sec = hit['job_hs06sec']
            else:
                job_hs06sec = 0

            if 'job_nevents' in hit_dict and hit_dict['job_nevents'] != 'NULL':
                job_nevents = hit['job_nevents']
            else:
                job_nevents = 0

            jobs_info_status_dict[hit_dict['jobid']][hit_dict['status']] = {
                'message_id': hit_dict['message_id'], 'job_inputfilebytes': job_inputfilebytes,
                'job_hs06sec': job_hs06sec, 'status': hit_dict['status'], 'job_nevents': job_nevents,
                'time': hit_dict['@timestamp'],'timestamp': hit_dict['timestamp']
            }

        fields_list = list(hit_dict.keys())
        for field in fields_list:
            if hit_dict[field] is None:
                hit_dict[field] = "None"

    task_info_conn = Search(using=os_conn, index=full_index_name)
    task_info_conn = task_info_conn.filter('term', taskid='{0}'.format(jeditaskid))
    task_info_conn.aggs.bucket('status', 'terms', field='status.keyword', size=1000) \
        .metric('timestamp', 'max', field='timestamp') \
        .metric('time', 'max', field='@timestamp')

    q = Q("match", msg_type='task_status')
    task_info_conn = task_info_conn.query(q)

    task_info_conn = task_info_conn.execute()

    for hit in task_info_conn.aggregations.status:
        task_info_status_dict[hit['key']] = int(hit['time']['value'])

    return task_message_ids_list, task_info_status_dict, jobs_info_status_dict, jobs_info_errors_dict

def chunks(iterable, chunk_size):
    """Split an iterable into chunks of the specified size."""
    args = [iter(iterable)] * chunk_size
    return ([
        tuple([x for x in y if x])
        for y in list(zip_longest(*args, fillvalue=None))
    ])