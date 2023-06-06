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
        if instance == 'es-atlas' and hasattr(settings, 'ES'):
            es_host = settings.ES.get('esHost', None)
            es_port = settings.ES.get('esPort', None)
            es_host = es_host + ':' + es_port + '/es' if es_host else None
            es_user = settings.ES.get('esUser', None)
            es_password = settings.ES.get('esPassword', None)
        elif instance == 'es-monit' and hasattr(settings, 'ES_MONIT'):
            es_host = settings.ES_MONIT.get('esHost', None)
            es_port = settings.ES_MONIT.get('esPort', None)
            es_host = es_host + ':' + es_port + '/es' if es_host else None
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

def create_es_connection(instance='es-atlas', protocol='https', timeout=2000, max_retries=10,
                         retry_on_timeout=True):
    """
    Create a connection to ElasticSearch cluster
    """
    es_host, es_user, es_password = get_es_credentials(instance)

    try:
        if protocol == 'https':
            ca_certs = settings.ES_CA_CERT

            connection = Elasticsearch(
                ['{0}://{1}'.format(protocol, es_host)],
                http_auth=(es_user, es_password),
                verify_certs=True,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout,
                ca_certs = ca_certs
            )
        else:
            connection = Elasticsearch(
                ['{0}://{1}'.format(protocol, es_host)],
                http_auth=(es_user, es_password),
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout)
        return connection

    except Exception as ex:
        _logger.error(ex)
    return None


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
    es_host, es_user, es_password = get_es_credentials()
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
    :return: split rule messagees
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