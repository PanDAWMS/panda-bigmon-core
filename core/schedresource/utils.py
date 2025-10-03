"""
Utils to get schedresources info from dedicated information system (CRIC)
"""
import os
import re
import urllib3
import logging
import json
from urllib.parse import urlparse

from django.conf import settings
from django.core.cache import cache

from core.schedresource.models import SchedconfigJson
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def cric_conn(url):
    """ Create connection to CRIC with proxy if needed"""
    # check http proxy
    netloc = urlparse(url)
    proxy = None
    if 'no_proxy' in os.environ and netloc.hostname in os.environ['no_proxy'].split(','):
        # no_proxy
        pass
    elif netloc.scheme == 'https' and 'https_proxy' in os.environ:
        # https proxy
        proxy = os.environ['https']
    elif netloc.scheme == 'http' and 'http_proxy' in os.environ:
        # http proxy
        proxy = os.environ['http']
    if proxy:
        http = urllib3.ProxyManager(proxy)
    else:
        http = urllib3.PoolManager(ca_certs=settings.OS_CA_CERT)
    return http


def get_CRIC_panda_queues():
    """Get PanDA queues config from CRIC and put to cache"""
    panda_queues_dict = cache.get(f'pandaQueues{settings.DEPLOYMENT}', None)
    if not panda_queues_dict:
        panda_queues_dict = {}
        url = settings.CRIC_API_URL
        http = cric_conn(url)
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for pq, params in data.items():
                if settings.DEPLOYMENT == 'ORACLE_ATLAS':
                    if 'vo_name' in params and params['vo_name'] == 'atlas':
                        panda_queues_dict[pq] = params
                elif settings.DEPLOYMENT == 'ORACLE_DOMA':
                    if 'vo_name' in params and params['vo_name'] in ['osg', 'atlas']:
                        panda_queues_dict[pq] = params
                else:
                    # add all queues to dict despite VO
                    panda_queues_dict[pq] = params
        except Exception as exc:
            print(exc)
        cache.set(f'pandaQueues{settings.DEPLOYMENT}', panda_queues_dict, 60*20)
    return panda_queues_dict



def get_ddm_downtimes():
    """ Get DDM downtimes from CRIC """
    ddm_downtimes = cache.get(f'ddm_downtimes_{settings.DEPLOYMENT}', None)
    if not ddm_downtimes:
        ddm_downtimes = {}
        url = settings.CRIC_API_URL.split('pandaqueue')[0] + 'ddmendpointstatus/query/?json'
        http = cric_conn(url)
        headers = {"Content-Type": "application/json"}
        try:
            r = http.request('GET', url, headers=headers)
            if r.status == 200:
                data = json.loads(r.data.decode('utf-8'))
                _logger.debug(data)
            else:
                data = {}
                _logger.info(f'CRIC ddmendpointstatus returned status {r.status}')
        except Exception as exc:
            _logger.exception(exc)

        # cache.set(f'cric_ddm_downtimes{settings.DEPLOYMENT}', ddm_downtimes, 60*20)  # cache for 20 min

    return ddm_downtimes


def get_panda_queues():
    """
    Get PanDA queues info from available sources, priority: CRIC -> SchedconfigJson table
    :return: dict of PQs
    """
    # try to get info from CRIC
    try:
        panda_queues_dict = get_CRIC_panda_queues()
    except:
        panda_queues_dict = None
        _logger.exception("cannot get json from CRIC, trying get them from schedconfig_json table")

    if not panda_queues_dict:
        # get data from new SCHEDCONFIGJSON table
        panda_queues_list = []
        panda_queues_dict = {}
        panda_queues_list.extend(SchedconfigJson.objects.values())
        if len(panda_queues_list) > 0:
            for pq in panda_queues_list:
                if isinstance(pq['data'], dict):
                    panda_queues_dict[pq['pandaqueue']] = pq['data']
                else:
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
                    'site': pq_info[pq]['atlas_site'] if 'atlas_site' in pq_info[pq] else '-',
                    'region': pq_info[pq]['cloud'] if 'cloud' in pq_info[pq] else '-',
                    'tier': pq_info[pq]['tier'] if 'tier' in pq_info[pq] else '-',
                    'corepower': pq_info[pq]['corepower'] if 'corepower' in pq_info[pq] else 0,
                    'status': pq_info[pq]['status'] if 'status' in pq_info[pq] else '-',
                })
    else:
        for pq, pqdata in pq_info.items():
            pq_info_list.append({
                'pq_name': pq,
                'site': pq_info[pq]['atlas_site'] if 'atlas_site' in pq_info[pq] else '-',
                'region': pq_info[pq]['cloud'] if 'cloud' in pq_info[pq] else '-',
                'tier': pq_info[pq]['tier'] if 'tier' in pq_info[pq] else '-',
                'corepower': pq_info[pq]['corepower'] if 'corepower' in pq_info[pq] else 0,
                'status': pq_info[pq]['status'] if 'status' in pq_info[pq] else '-',
            })

    return pq_info_list


def get_object_stores(**kwargs):

    object_stores_dict = cache.get('objectStores', None)
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
        if 'catchall' in pq_info and pq_info['catchall'] is not None and 'objectstore' in pq_info and (
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
    SEs = cache.get('CRIC_SEs', None)
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
        except Exception as e:
            _logger.exception('Got exception on getCRICSEs:\n{}'.format(e))
        cache.set('CRIC_SEs', SEs, 7200)
    return SEs


def getCRICSites():
    sitesUcore = cache.get('sitesUcore', None)
    sitesHarvester = cache.get('sitesHarvester', None)
    sitesType = cache.get('sitesType', None)
    computevsAtlasCE = cache.get('computevsAtlasCE', None)

    if not (sitesUcore and sitesHarvester and computevsAtlasCE and sitesType):
        sitesUcore, sitesHarvester = [], []
        computevsAtlasCE, sitesType = {}, {}

        data = get_panda_queues()
        for cs in data:
            if 'unifiedPandaQueue' in data[cs]['catchall'] or 'ucore' in data[cs]['capability']:
                sitesUcore.append(data[cs]['siteid'])
            if 'harvester' in data[cs] and len(data[cs]['harvester']) != 0:
                sitesHarvester.append(data[cs]['siteid'])
            if 'panda_site' in data[cs]:
                computevsAtlasCE[cs] = data[cs]['atlas_site']
            if 'type' in data[cs]:
                sitesType[cs] = data[cs]['type']

        cache.set('sitesUcore', sitesUcore, 3600)
        cache.set('sitesHarvester', sitesHarvester, 3600)
        cache.set('sitesType', sitesType, 3600)
        cache.set('computevsAtlasCE', computevsAtlasCE, 3600)

    return sitesUcore, sitesHarvester, sitesType, computevsAtlasCE


def get_cric_rse_downtimes():
    """ Get RSE downtimes from CRIC """
    rse_downtimes = cache.get('cric_rse_downtimes', None)
    if not rse_downtimes:
        rse_downtimes = {}
        url = "https://atlas-cric.cern.ch/api/atlas/downtime/query/?json&active=True"
        http = urllib3.PoolManager()
        try:
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for dt_id, dt_info in data.items():
                if 'rse' in dt_info and dt_info['rse'] is not None:
                    rse_downtimes[dt_info['rse']] = {
                        'id': dt_id,
                        'starttime': dt_info['starttime'],
                        'endtime': dt_info['endtime'],
                        'comment': dt_info['comment'] if 'comment' in dt_info else '',
                    }
        except Exception as exc:
            _logger.exception(exc)

        cache.set('cric_rse_downtimes', rse_downtimes, 300)

    return rse_downtimes


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
            _logger.warning("Object store path could not be extracted using file type={} from objectstore={}".format(
                filetype, objectstore))

    else:
        _logger.info("Object store not defined in queuedata")

    return basepath


def filter_pq_json(request, **kwargs):
    """
    Filter CRIC PQs JSON by request params. Only support filtering by int or str types of values.
    'site' param is not supported.
    :param request:
    :return: pqs_dict: filtered PQs dict
    """
    if 'pqs_dict' in kwargs:
        pqs_dict = kwargs['pqs_dict']
    else:
        # get full PanDA queues dict
        pqs_dict = get_panda_queues()

    # get list of params we can filter on
    filter_params = {}
    if pqs_dict:
        pqs_list = list(pqs_dict.values())
        for key, value in pqs_list[0].items():
            if isinstance(value, int) or isinstance(value, float):
                filter_params[key] = 'str'
            elif isinstance(value, str):
                filter_params[key] = 'str'
        # the 'site' meaning in BigPanDA language does not match to 'site' in CRIC due to historical reasons ->
        # excluding it from available filter params
        exclude_list = ('site', 'region')
        for e in exclude_list:
            if e in filter_params:
                del filter_params[e]

    # filter the PQs dict
    filtered_pq_names_final = list(pqs_dict.keys())
    for param in request.session['requestParams']:
        req_param_value = request.session['requestParams'][param]
        if param.startswith('queue'):
            param = param.replace('queue', '')
        if param in filter_params:
            filtered_pq_names = []
            excluded_pq_names = []
            if filter_params[param] == 'str':
                # handling OR clause
                if '|' in req_param_value:
                    req_param_values = req_param_value.split('|')
                else:
                    req_param_values = [req_param_value, ]

                for req_param_value in req_param_values:
                    is_not = False
                    if req_param_value.startswith('!'):
                        is_not = True
                        req_param_value = req_param_value[1:]
                    if '*' not in req_param_value and not is_not:
                        filtered_pq_names.extend(
                            [k for k, v in pqs_dict.items() if v[param] is not None and v[param] == req_param_value])
                    elif '*' not in req_param_value and is_not:
                        excluded_pq_names.extend(
                            [k for k, v in pqs_dict.items() if v[param] is not None and v[param] == req_param_value])
                    elif '*' in req_param_value:
                        try:
                            pattern = re.compile(r'^{}$'.format(req_param_value.replace('*', '.*')))
                            if not is_not:
                                filtered_pq_names.extend(
                                    [k for k, v in pqs_dict.items() if
                                     v[param] is not None and pattern.match(v[param]) is not None])
                            else:
                                excluded_pq_names.extend(
                                    [k for k, v in pqs_dict.items() if
                                     v[param] is not None and pattern.match(v[param]) is not None])
                        except Exception as e:
                            _logger.exception('Failed to compile regex pattern and filter PQs list: \n{}'.format(e))

            elif filter_params[param] == 'int':
                filtered_pq_names.extend([k for k, v in pqs_dict.items() if v[param] == req_param_value])

            # unite
            if len(filtered_pq_names) > 0:
                filtered_pq_names_final = list(set(filtered_pq_names_final) & set(filtered_pq_names))
            filtered_pq_names_final = list(set(filtered_pq_names_final) - set(excluded_pq_names))

    pqs_dict = {k: v for k, v in pqs_dict.items() if k in filtered_pq_names_final}

    return pqs_dict


def site_summary_dict(sites, vo='ATLAS', sortby='alpha'):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {'copytool': {}}
    for site in sites:
        for f in const.SITE_FIELDS_STANDARD:
            if f in site and site[f] is not None:
                if f not in sumd:
                    sumd[f] = {}
                if site[f] not in sumd[f]:
                    sumd[f][site[f]] = 0
                sumd[f][site[f]] += 1
        if 'copytool' in const.SITE_FIELDS_STANDARD:
            if 'copytools' in site and site['copytools'] and len(site['copytools']) > 0:
                copytools = list(site['copytools'].keys())
                for cp in copytools:
                    if cp not in sumd['copytool']:
                        sumd['copytool'][cp] = 0
                    sumd['copytool'][cp] += 1

    if vo != 'ATLAS':
        try:
            del sumd['cloud']
        except:
            _logger.exception('Failed to remove cloud key from dict')

    # convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        # sorting
        if sortby == 'count':
            iteml = sorted(iteml, key=lambda x: -x['kvalue'])
        else:
            iteml = sorted(iteml, key=lambda x: x['kname'])
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def is_osg_pool_pq(pqdata) -> bool:
    """ Return True if the given PQ is an OSG pool """
    if 'system' in pqdata and pqdata['system'] == 'osg':
        return True
    if 'queues' in pqdata and pqdata['queues'] is not None and len(pqdata['queues']) > 0:
        for ce in pqdata['queues']:
            if 'ce_jobmanager' in ce and (ce['ce_jobmanager'] == 'osg' or 'osg' in ce['ce_jobmanager']):
                return True
    return False


def is_any_osg_pool(pqs_dict) -> bool:
    """ Return True if any of the given PQs is an OSG pool """
    for pq, pqdata in pqs_dict.items():
        if is_osg_pool_pq(pqdata):
            return True
    return False


def get_osg_pool_pqs() -> dict:
    """ Return dict of OSG pool PQs """
    osg_pool_pqs = {}
    pqs_dict = get_panda_queues()
    for pq, pqdata in pqs_dict.items():
        if is_osg_pool_pq(pqdata):
            osg_pool_pqs[pq] = pqdata
    return osg_pool_pqs