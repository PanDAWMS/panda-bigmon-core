import logging
import json
import requests
from requests.auth import HTTPBasicAuth
import hashlib
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from core.pandajob.models import Jobsactive4
from core.libs.DateTimeEncoder import DateTimeEncoder
from django.conf import settings
from urllib.parse import urlparse

_logger = logging.getLogger('bigpandamon')

def get_es_credentials(instance):
    """
    Getting credentials from settings
    :param instance: str, es-atlas or es-monit
    :return:
    """
    es_host = None
    es_user = None
    es_password = None
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        protocol = 'https'
        if instance == 'es-atlas' and hasattr(settings, 'ES'):
            es_host = settings.ES.get('esHost', None)
            es_port = settings.ES.get('esPort', None)
            es_host = protocol +'://'+ es_host + ':' + es_port + '/es' if es_host else None
            es_user = settings.ES.get('esUser', None)
            es_password = settings.ES.get('esPassword', None)
        elif instance == 'es-monit' and hasattr(settings, 'ES_MONIT'):
            es_host = settings.ES_MONIT.get('esHost', None)
            es_port = settings.ES_MONIT.get('esPort', None)
            es_host = protocol + '://' + es_host + ':' + es_port + '/es' if es_host else None
            es_user = settings.ES_MONIT.get('esUser', None)
            es_password = settings.ES_MONIT.get('esPassword', None)
        if any(i is None for i in (es_host, es_user, es_password)):
            raise Exception('ES cluster credentials was not found in settings')
    else:
        if hasattr(settings, 'ES_CLUSTER'):
            es_host = settings.ES_CLUSTER.get('esHost', '')
            es_port = settings.ES_CLUSTER.get('esPort', '9200')
            es_protocol = settings.ES_CLUSTER.get('esProtocol', 'http')
            es_path = settings.ES_CLUSTER.get('esPath', '')
            es_host = es_protocol + '://' + es_host + ':' + es_port + es_path
            es_user = settings.ES_CLUSTER.get('esUser', '')
            es_password = settings.ES_CLUSTER.get('esPassword', '')

    return es_host, es_user, es_password

def create_es_connection(instance='es-atlas', timeout=2000, max_retries=10, retry_on_timeout=True):
    """
    Create a connection to ElasticSearch cluster
    """
    es_host, es_user, es_password = get_es_credentials(instance)
    try:
        parsed_uri = urlparse(es_host)
        protocol = '{uri.scheme}'.format(uri=parsed_uri)

        if protocol == 'https':
            ca_certs = settings.ES_CA_CERT

            connection = Elasticsearch(
                [es_host],
                http_auth=(es_user, es_password),
                verify_certs=True,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout,
                ca_certs = ca_certs
            )
        else:
            connection = Elasticsearch(
                [es_host],
                http_auth=(es_user, es_password),
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout)

        return connection

    except Exception as ex:
        _logger.error(ex)
    return None

def get_date(item):
    return datetime.strptime(item['@timestamp'],  '%Y-%m-%dT%H:%M:%S.%fZ')

def get_payloadlog(id, es_conn, index, start=0, length=50, mode='pandaid', sort='asc', search_string=''):
    """
    Get pilot logs from ATLAS ElasticSearch storage
    """
    logs_list = []
    query = {}
    jobs = []
    total = 0
    flag_running_job = True

    end = start + length

    s = Search(using=es_conn, index=index)

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
            q = Q("multi_match", query=search_string, fields=['level', 'message'])
            s = s.query(q)
    elif mode == 'jeditaskid':
        s = s.query('match', TaskID='{0}'.format(id)).sort("@timestamp")
    try:
        _logger.debug('ElasticSearch query: {0}'.format(str(s.to_dict())))
        response = s[start:end].execute()

        total = response.hits.total.value

        for hit in response:
            logs_list.append(hit.to_dict())
    except Exception as ex:
        _logger.error(ex)

    return logs_list, flag_running_job, total

def upload_data(es_conn, index_name_base, data, timestamp_param='creationdate', id_param='jeditaskid'):
    """
    Push data to ElasticSearch cluster
    :param es_conn: connection to use
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
                if not es_conn.indices.exists(index=index_name):
                    _logger.info(f"Creating index: {index_name}")
                    es_conn.indices.create(index=index_name)
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
    es_host, es_user, es_password = get_es_credentials(instance='es-atlas')
    if '/' in es_host:
        es_host = es_host.split('/')[0]
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(
        f"https://{es_host}/es/_bulk",
        data=data,
        headers=headers,
        auth=HTTPBasicAuth(es_user, es_password),
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
        result['link'] = "https://es-atlas.cern.ch/kibana/goto/c987a191e5fa02605e20e5e6eaa9bc1f?security_tenant=global"
        _logger.info(result['message'])


    return result

def get_split_rule_info(es_conn, jeditaskid):
    """
    Get split rule entries from ATLAS Elastic
    :param es_conn: connection to the ATLAS Elastic
    :param jeditaskid: unique task ID
    :return: split rule messages
    """
    split_rules = []
    jedi_logs_index = settings.ES_INDEX_JEDI_LOGS

    s = Search(using=es_conn, index=jedi_logs_index)
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

def get_gco2_sum_for_tasklist(task_list):
    """
    Getting sum of gCO2 for list of tasks from ES-ATLAS
    :param: tasks_list: list of jeditaskid
    :return: gco2_sum
    """
    gco2_sum = {'total': 0, 'finished': 0, 'failed': 0}
    es_jobs_index = 'atlas_jobs_archived*'
    es_conn = create_es_connection(instance='es-atlas')

    s = Search(using=es_conn, index=es_jobs_index).query("terms", jeditaskid=[str(t) for t in task_list])

    s.filter('range', **{
        '@timestamp': {'gte': 'now-1y', 'lte': 'now'}
    }).query("terms", jeditaskid=task_list)

    s.aggs.bucket('jobstatus', 'terms', field='jobstatus.keyword') \
        .metric('sum_gco2global', 'sum', field='gco2global') \
        .metric('sum_gco2regional', 'sum', field='gco2regional')

    response = s.execute()

    for js in response.aggregations['jobstatus']:
        if js['key'] in gco2_sum:
            gco2_sum[js['key']] += js['sum_gco2global']['value']

    gco2_sum['total'] = gco2_sum['finished'] + gco2_sum['failed']

    return gco2_sum

def get_es_task_status_log(db_source, jeditaskid, es_instance='es-atlas'):

    task_message_list = []
    task_message_ids_list = []
    task_message_dict = {}

    jobs_info_status_dict = {}

    full_index_name = db_source + '_tasks_status_log*'

    es_conn = create_es_connection(instance=es_instance)

    s = Search(using=es_conn, index=full_index_name)

    fields_list = ['@timestamp','db_source', 'inputs', 'job_hs06sec', 'job_inputfilebytes', 'job_nevents', 'job_ninputdatafiles',
                  'job_ninputfiles', 'job_noutputdatafiles', 'job_outputfilebytes', 'jobid', 'message_id', 'msg_type', 'status',
                  'taskid','timestamp']

    s = s.source(fields_list)
    s = s.filter('term', taskid='{0}'.format(jeditaskid))
    q = Q("match", msg_type='job_status')
    s = s.query(q)

    response = s.scan()
    for hit in response:
        hit_dict = hit.to_dict()
        # if hit_dict['status'] not in jobs_info_status_dict:
        #     jobs_info_status_dict[hit_dict['status']] = set()
        #
        #     jobs_info_status_dict[hit_dict['status']].add(hit_dict['jobid'])
        # else:
        #     jobs_info_status_dict[hit_dict['status']].add(hit_dict['jobid'])
        if not hit_dict['jobid'] in jobs_info_status_dict:
            jobs_info_status_dict[hit_dict['jobid']] = {}

        jobs_info_status_dict[hit_dict['jobid']][hit_dict['status']] = {'timestamp': hit_dict['timestamp'],
                                                                        'message_id': hit_dict['message_id'],
                                                                        'status': hit_dict['status']}

        task_message_list.append(hit_dict)
        task_message_ids_list.append(hit_dict['message_id'])
        task_message_dict[hit_dict['message_id']] = hit_dict

        if hit_dict['status'] in ('finished', 'failed', 'closed', 'cancelled'):
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

            jobs_info_status_dict[hit_dict['jobid']][hit_dict['status']] = {'message_id': hit_dict['message_id'], 'job_inputfilebytes': job_inputfilebytes, 'job_hs06sec': job_hs06sec, 'status': hit_dict['status'], 'job_nevents': job_nevents, 'timestamp':hit_dict['timestamp']}

        fields_list = list(hit_dict.keys())
        for field in fields_list:
            if hit_dict[field] is None:
                hit_dict[field] = "None"

    task_message_list = sorted(task_message_list, key=get_date)

    return task_message_list, task_message_ids_list, jobs_info_status_dict