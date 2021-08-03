from core.settings.local import ES
from elasticsearch import Elasticsearch


def create_esatlas_connection(verify_certs=True, timeout=2000, max_retries=10,
                      retry_on_timeout=True):
    """
    Create a connection to ElasticSearch cluster
    """

    esHost = None
    esUser = None
    esPassword = None

    if 'esHost' in ES:
        #esHost = ES['esHost']
        esHost = ES['esHost'][0:8] + '1' + ES['esHost'][8:]
    if 'esUser' in ES:
        esUser = ES['esUser']
    if 'esPassword' in ES:
        esPassword = ES['esPassword']
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
        print(ex)
    return None