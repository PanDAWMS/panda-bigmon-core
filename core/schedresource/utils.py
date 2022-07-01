"""
Utils to get schedresources info from dedicated information system (CRIC)
"""
import urllib3
import json
import logging

from django.core.cache import cache

from core.schedresource.models import SchedconfigJson
from core.settings.config import CRIC_API_URL, DEPLOYMENT

_logger = logging.getLogger('bigpandamon')


def get_CRIC_panda_queues():
    """Get PanDA queues config from CRIC and put to cache"""
    panda_queues_dict = cache.get(f'pandaQueues{DEPLOYMENT}')
    if not panda_queues_dict:
        panda_queues_dict = {}
        url = CRIC_API_URL
        http = urllib3.PoolManager()
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for pq, params in data.items():
                if DEPLOYMENT == 'ORACLE_ATLAS':
                    if 'vo_name' in params and params['vo_name'] == 'atlas':
                        panda_queues_dict[pq] = params
                if DEPLOYMENT == 'ORACLE_DOMA':
                    if 'vo_name' in params and params['vo_name'] in ['osg', 'atlas']:
                        panda_queues_dict[pq] = params
        except Exception as exc:
            print (exc)
        cache.set(f'pandaQueues{DEPLOYMENT}', panda_queues_dict, 60*20)
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


def get_panda_resource(pq_name):
    """Rerurns dict for a particular PanDA queue"""
    pq_dict = get_panda_queues()

    if pq_dict and pq_name in pq_dict:
        return pq_dict[pq_name]

    return None

    url = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
    http = urllib3.PoolManager()
    data = {}
    try:
        r = http.request('GET', url)
        data = json.loads(r.data.decode('utf-8'))
        for cs in data.keys():
            if (data[cs] and siterec.siteid == data[cs]['siteid']):
                return data[cs]['panda_resource']
    except Exception as exc:
        print(exc)


def get_pq_clouds():
    """
    Return dict of PQ:cloud
    :return: pq_clouds: dict
    """
    pq_clouds = {}
    pq_dict = get_panda_queues()
    if len(pq_dict) > 0:
        for pq, pq_info in pq_dict.items():
            if 'siteid' in pq_info and pq_info['siteid'] is not None:
                pq_clouds[pq_info['siteid']] = pq_info['cloud']

    return pq_clouds


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


def get_object_stores(**kwargs):

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


def get_pq_object_store_path():
    """

    :return:
    """
    pq_object_store_paths = {}
    pq_dict = get_panda_queues()
    for pq, pq_info in pq_dict.items():
        if pq_info['catchall'] is not None and 'objectstore' in pq_info and (
                pq_info['catchall'].find('log_to_objectstore') >= 0 or pq_info['objectstore'] != ''):
            try:
                fpath = getFilePathForObjectStore(pq_info['objectstore'], filetype="logs")
                # dirty hack
                fpath = fpath.replace('root://atlas-objectstore.cern.ch/atlas/logs',
                                      'https://atlas-objectstore.cern.ch:1094/atlas/logs')
                if fpath != "" and fpath.startswith('http'):
                    pq_object_store_paths[pq_info['siteid']] = fpath
            except:
                pass

    return pq_object_store_paths


def getCRICSEs():
    SEs = cache.get('CRIC_SEs')
    if not SEs:
        url = "https://atlas-cric.cern.ch/api/atlas/ddmendpoint/query/?json"
        http = urllib3.PoolManager()
        SEs = {}
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for se in data.keys():
                su = data[se].get("su", None)
                if su:
                    SEs.setdefault(su, set()).add(se)
        except Exception:
            _logger.exception('Got exception on getCRICSEs')
        cache.set('CRIC_SEs', SEs, 7200)
    return SEs


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


def getFilePathForObjectStore(objectstore, filetype="logs"):
    """ Return a proper file path in the object store """

    # For single object stores
    # root://atlas-objectstore.cern.ch/|eventservice^/atlas/eventservice|logs^/atlas/logs
    # For multiple object stores
    # eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://atlas-objectstore.bnl.gov//atlas/logs

    basepath = ""

    # Which form of the schedconfig.objectstore field do we currently have?
    if objectstore != "":
        _objectstore = objectstore.split("|")
        if "^" in _objectstore[0]:
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
        else:
            _objectstore = objectstore.split("|")
            url = _objectstore[0]
            for obj in _objectstore:
                if obj[:len(filetype)] == filetype:
                    basepath = obj.split("^")[1]
                    break
            if basepath != "":
                if url.endswith('/') and basepath.startswith('/'):
                    basepath = url + basepath[1:]
                else:
                    basepath = url + basepath

        if basepath == "":
            _logger.warning("Object store path could not be extracted using file type \'%s\' from objectstore=\'%s\'" % (
            filetype, objectstore))

    else:
        _logger.info("Object store not defined in queuedata")

    return basepath