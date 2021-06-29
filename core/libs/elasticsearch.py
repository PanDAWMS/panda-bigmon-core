from core.settings.local import ES
from elasticsearch import Elasticsearch


def create_esatlas_connection(use_ssl=True, verify_certs=False, timeout=2000, max_retries=10,
                      retry_on_timeout=True):
    """
    Create a connection to ElasticSearch cluster
    """

    esHost = None
    esPort = None
    esUser = None
    esPassword = None

    if 'esHost' in ES:
        esHost = ES['esHost']
    if 'esPort' in ES:
        esPort = ES['esPort']
    if 'esUser' in ES:
        esUser = ES['esUser']
    if 'esPassword' in ES:
        esPassword = ES['esPassword']
    try:
        connection = Elasticsearch(
            [{'host': esHost, 'port': int(esPort)}],
            http_auth=(esUser, esPassword),
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
        )
        return connection
    except Exception as ex:
        print(ex)
    return None