"""
Utils to get schedresources info from dedicated information system (CRIC)
"""
import urllib3
import json

from django.core.cache import cache


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