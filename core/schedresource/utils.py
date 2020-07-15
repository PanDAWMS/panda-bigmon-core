"""
Utils to get schedresources info from dedicated information system (CRIC)
"""
import urllib3
import json
import logging

from django.core.cache import cache

from core.schedresource.models import SchedconfigJson

_logger = logging.getLogger('bigpandamon')


def get_panda_queues():
    """
    Get PanDA queues info from available sources, priority: CRIC -> SchedconfigJson table -> Schedconfig
    :return: dict of PQs
    """
    # try get info from CRIC
    try:
        panda_queues_dict = get_CRIC_panda_queues()
    except:
        panda_queues_dict = None
        _logger.error("[JSR] cannot get json from CRIC")

    if not panda_queues_dict:
        # get data from new SCHEDCONFIGJSON table
        panda_queues_list = []
        panda_queues_dict = {}
        panda_queues_list.extend(SchedconfigJson.objects.values())
        if len(panda_queues_list) > 0:
            for pq in panda_queues_list:
                try:
                    panda_queues_dict[pq['pandaqueue']] = json.loads(pq['data'])
                except:
                    panda_queues_dict[pq['pandaqueue']] = None
                    _logger.error("cannot load json from SCHEDCONFIGJSON table for {} PanDA queue".format(pq['pandaqueue']))

    return panda_queues_dict


def get_CRIC_panda_queues():
    """Get PanDA queues config from CRIC"""
    panda_queues_dict = cache.get('pandaQueues')
    if not panda_queues_dict:
        panda_queues_dict = {}
        url = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        http = urllib3.PoolManager()
        data = {}
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for pq, params in data.items():
                if 'vo_name' in params and params['vo_name'] == 'atlas':
                    panda_queues_dict[pq] = params
        except Exception as exc:
            print (exc)

        cache.set('pandaQueues', panda_queues_dict, 3600)

    return panda_queues_dict