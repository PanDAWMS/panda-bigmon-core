import json

import hashlib

from django.utils import encoding
from django.conf import settings as djangosettings
from django.core.cache import cache
from core.utils import is_json_request

notcachedRemoteAddress = ['188.184.185.129', '188.184.116.46']


def deleteCacheTestData(request,data):
### Filtering data
    if request.user.is_authenticated() and request.user.is_tester:
        return data
    else:
        if data is not None:
            for key in data.keys():
                if '_test' in key:
                    del data[key]
    return data


import socket
import uuid
import logging


def cacheIsAvailable(request):
    hostname = "bigpanda-redis.cern.ch"
    port = "6379"
    try:
        host = socket.gethostbyname(hostname)
        s = socket.create_connection((host, port), 2)
        if(s):
            cache_key = uuid.uuid4()
            from core.views import DateEncoder
            data = json.dumps({"message":"ping-pong"}, cls=DateEncoder)
            timeout = 0.5
            cache.set(cache_key, data, timeout)
            data = cache.get(cache_key, None)
            return True
    except Exception as e:
        logger = logging.getLogger('bigpandamon-error')
        message = "Internal Servicer Error: %s | Error in Reddis: %s" %(str(request),e)
        #e = 'Internal Server Error: Reddis! '+ e
        logger.error(message)
        pass
    return False


def getCacheEntry(request, viewType, skipCentralRefresh = False, isData = False):
    # isCache = cacheIsAvailable(request)
    isCache = True
    if isCache:
        is_json = is_json_request(request)

        # We do this check to always rebuild cache for the page when it called from the crawler
        if (('HTTP_X_FORWARDED_FOR' in request.META) and (request.META['HTTP_X_FORWARDED_FOR'] in notcachedRemoteAddress) and
                skipCentralRefresh == False):

            return None

        request._cache_update_cache = False
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        if isData == False:
            try:
                if request.method == "POST":
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path() + '?' + request.body)))
                else:
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            except:
                path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            cache_key = '%s.%s' % (key_prefix, path.hexdigest())
            return cache.get(cache_key, None)
        else:
            if 'harvester' in request.META['PATH_INFO']:
                is_json = False
            key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
            cache_key = '%s' % (key_prefix)
            return cache.get(cache_key, None)
    else:
        return None


def setCacheEntry(request, viewType, data, timeout, isData = False):
    # isCache = cacheIsAvailable(request)
    isCache = True
    # do not cache data for 'refreshed' pages
    if 'requestParams' in request.session and 'timestamp' in request.session['requestParams']:
        isCache = False
    if isCache:
        is_json = is_json_request(request)
        request._cache_update_cache = False
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        if isData == False:
            try:
                if request.method == "POST":
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path() + '?' + request.body)))
                else:
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            except: path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            cache_key = '%s.%s' % (key_prefix, path.hexdigest())
        else:
            cache_key = '%s' % (key_prefix)
        cache.set(cache_key, data, timeout)


def preparePlotData(data):
    oldPlotData = data
    if isinstance(oldPlotData, dict):
        newPlotData = {}
        for key, value in oldPlotData.items():
            newPlotData[str(key)] = float(value)
    else:
        newPlotData = oldPlotData
    return newPlotData


# Managing static cache
def get_last_static_file_update_date(filename):
    """
    Get the last update time of static files
    :param absolute_path: path to static files
    :return:
    """
    try:
        import os
        from datetime import datetime
    except ImportError:
        raise
    try:
        from core.settings import BASE_DIR, STATIC_URL
    except ImportError:
        BASE_DIR = None
        STATIC_URL = None
    absolute_path = BASE_DIR + STATIC_URL if BASE_DIR and STATIC_URL else None
    if absolute_path and filename:
        timestamp = os.path.getmtime(absolute_path + str(filename))
        # timestamp = max(map(lambda x: os.path.getmtime(x[0]), os.walk(os.path.join(absolute_path, 'static'))))
        try:
            timestamp = datetime.utcfromtimestamp(int(timestamp))
        except ValueError:
            return ""
        lastupdatetime = timestamp.strftime('%Y%m%d%H%M%S')
    else:
        lastupdatetime = datetime.now().strftime('%Y%m%d%H%M%S')
    return lastupdatetime


def get_version(filename):
    """Form version of static file by last update date"""
    lastupdate = get_last_static_file_update_date(filename)
    return '_v_={lastupdate}'.format(lastupdate=lastupdate)


def set_cache_timeout(request):
    """ Set cache timeout for a browser depending on request"""

    default_timeout_min = 10
    request_path = request.get_full_path()
    pattern_to_timeout = {
        '/errors/': 5,
        '/dashboard/': 5,
        'timestamp': 0,
    }
    request.session['max_age_minutes'] = default_timeout_min
    for p, t in pattern_to_timeout.items():
        if p in request_path:
            request.session['max_age_minutes'] = t

