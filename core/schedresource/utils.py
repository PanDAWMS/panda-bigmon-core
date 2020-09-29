"""
Utils to get schedresources info from dedicated information system (CRIC)
"""
import urllib3
import json
import logging

from django.core.cache import cache

from core.schedresource.models import SchedconfigJson

_logger = logging.getLogger('bigpandamon')


def get_CRIC_panda_queues():
    """Get PanDA queues config from CRIC and put to cache"""
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


def get_pq_atlas_sites():
    """
    Get dict of PQ and corresponding ATLAS sites
    :return: atlas_sites_dict: dict
    """
    atlas_sites_dict = {}
    pq_dict = get_panda_queues()

    if pq_dict:
        for pq, pqdata in pq_dict.items():
            if 'atlas_site' in pqdata:
                atlas_sites_dict[pq] = pqdata['atlas_site']

    return atlas_sites_dict


def get_pq_resource_types():
    """
    Extract resource types for PQs from CRIC PQ JSON
    :return:
    """
    resource_types_dict = {}
    pq_dict = get_panda_queues()

    if pq_dict:
        for pq, pqdata in pq_dict.items():
            if 'siteid' in pqdata and 'resource_type' in pqdata:
                resource_types_dict[pqdata['siteid']] = pqdata['resource_type']

    return resource_types_dict


def get_pq_fairshare_policy():
    """
    Extract fairshare policy for PQs from CRIC PQ JSON
    :return:
    """
    fairshare_policy_dict = {}
    pq_dict = get_panda_queues()

    if pq_dict:
        for pq, pqdata in pq_dict.items():
            if 'siteid' in pqdata and 'fairsharepolicy' in pqdata:
                fairshare_policy_dict[pqdata['siteid']] = pqdata['fairsharepolicy']

    return fairshare_policy_dict


def get_basic_info_for_pqs(pq_list):
    """
    Return list of dicts with basic info for list of PQs, including ATLAS site, region (cloud), tier, corepower, status
    If input pq_list empty, return all
    :param pq_list: list
    :return: site_list: list
    """
    pq_info_list = []
    pq_info = get_panda_queues()
    if len(pq_list) > 0:
        for pq in pq_list:
            if pq in pq_info and pq_info[pq]:
                pq_info_list.append({
                    'pq_name': pq,
                    'site': pq_info[pq]['gocname'],
                    'region': pq_info[pq]['cloud'],
                    'tier': pq_info[pq]['tier'],
                    'corepower': pq_info[pq]['corepower'],
                    'status': pq_info[pq]['status'],
                })
    else:
        for pq, pqdata in pq_info.items():
            pq_info_list.append({
                'pq_name': pq,
                'site': pq_info[pq]['gocname'],
                'region': pq_info[pq]['cloud'],
                'tier': pq_info[pq]['tier'],
                'corepower': pq_info[pq]['corepower'],
                'status': pq_info[pq]['status'],
            })

    return pq_info_list


def get_object_stores():
    object_stores_dict = cache.get('objectStores')
    if not object_stores_dict:
        object_stores_dict = {}
        url = "https://atlas-cric.cern.ch/api/atlas/ddmendpoint/query/?json&type=OS_"
        http = urllib3.PoolManager()
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))

        except Exception as exc:
            _logger.exception(exc)
            data = {}

        for OSname, OSdescr in data.items():
            if "resource" in OSdescr and "bucket_id" in OSdescr["resource"]:
                object_stores_dict[OSdescr["resource"]["bucket_id"]] = {
                    'name': OSname,
                    'site': OSdescr["site"],
                    'region': OSdescr['cloud'],
                }
                object_stores_dict[OSdescr["resource"]["id"]] = {
                    'name': OSname,
                    'site': OSdescr["site"],
                    'region': OSdescr['cloud'],
                }
                object_stores_dict[OSdescr["id"]] = {
                    'name': OSname,
                    'site': OSdescr["site"],
                    'region': OSdescr['cloud'],
                }

        cache.set('objectStores', object_stores_dict, 3600)

    return object_stores_dict


def getCRICSites():
    sitesUcore = cache.get('sitesUcore')
    sitesHarvester = cache.get('sitesHarvester')
    sitesType = cache.get('sitesType')
    computevsAtlasCE = cache.get('computevsAtlasCE')

    if not (sitesUcore and sitesHarvester and computevsAtlasCE and sitesType):
        sitesUcore, sitesHarvester = [], []
        computevsAtlasCE, sitesType = {}, {}
        url = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        http = urllib3.PoolManager()
        data = {}
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))

            for cs in data.keys():
                if 'unifiedPandaQueue' in data[cs]['catchall'] or 'ucore' in data[cs]['capability']:
                    sitesUcore.append(data[cs]['siteid'])
                if 'harvester' in data[cs] and len(data[cs]['harvester']) != 0:
                    sitesHarvester.append(data[cs]['siteid'])
                if 'panda_site' in data[cs]:
                    computevsAtlasCE[cs] = data[cs]['atlas_site']
                if 'type' in data[cs]:
                    sitesType[cs] = data[cs]['type']
        except Exception as exc:
            print(exc)

        cache.set('sitesUcore', sitesUcore, 3600)
        cache.set('sitesHarvester', sitesHarvester, 3600)
        cache.set('sitesType', sitesType, 3600)
        cache.set('computevsAtlasCE', computevsAtlasCE, 3600)

    return sitesUcore, sitesHarvester, sitesType, computevsAtlasCE
