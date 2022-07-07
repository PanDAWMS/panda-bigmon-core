import logging

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from core.pandajob.models import Jobsactive4

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def create_esatlas_connection(verify_certs=True, timeout=2000, max_retries=10, retry_on_timeout=True):
    """
    Create a connection to ElasticSearch cluster
    """

    esHost = None
    esUser = None
    esPassword = None

    if 'esHost' in settings.ES:
        esHost = settings.ES['esHost'][0:8] + '1' + settings.ES['esHost'][8:]
    if 'esUser' in settings.ES:
        esUser = settings.ES['esUser']
    if 'esPassword' in settings.ES:
        esPassword = settings.ES['esPassword']
    try:
        connection = Elasticsearch(
            ['https://{0}/es'.format(esHost)],
            http_auth=(esUser, esPassword),
            verify_certs=verify_certs,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
            ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt'
        )
        return connection
    except Exception as ex:
        _logger.error(ex)
    return None


def get_payloadlog(id, connection, start = 0, length = 50, mode = 'pandaid', sort = 'asc', search_string =''):
    """
    Get pilot logs from ATLAS ElasticSearch storage
    """
    logs_list = []
    query = {}
    jobs = []
    total = 0
    flag_running_job = True
    end = start + length
    s = Search(using=connection, index='atlas_pilotlogs*')

    s = s.source(["@timestamp", "@timestamp_nanoseconds", "level", "message", "PandaJobID", "TaskID",
                  "Harvester_WorkerID", "Harvester_ID"])

    if mode == 'pandaid':
        query['pandaid'] = int(id)
        jobs.extend(Jobsactive4.objects.filter(**query).values())
        if len(jobs) == 0:
            flag_running_job = False
        if sort == 'asc':
            s = s.filter('term', PandaJobID__keyword='{0}'.format(id)).sort("@timestamp")
        else:
            s = s.filter('term', PandaJobID__keyword='{0}'.format(id)).sort("-@timestamp")
        if search_string != '':
            q = Q("multi_match", query=search_string, fields=['level', 'message'])
            s = s.query(q)
    elif mode == 'jeditaskid':
        s = s.filter('term', TaskID__keyword='{0}'.format(id)).sort("@timestamp")
    try:
        response = s[start:end].execute()

        total = response.hits.total.value

        for hit in response:
            logs_list.append(hit.to_dict())
    except Exception as ex:
        _logger.error(ex)

    return logs_list, flag_running_job, total