import json

import hashlib

from django.utils import encoding
from django.conf import settings as djangosettings
from django.core.cache import cache

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
        is_json = False

        #logger = logging.getLogger('bigpandamon-error')
        #logger.error(request.META.get('HTTP_X_FORWARDED_FOR'))

        # We do this check to always rebuild cache for the page when it called from the crawler
        if (('HTTP_X_FORWARDED_FOR' in request.META) and (request.META['HTTP_X_FORWARDED_FOR'] in notcachedRemoteAddress) and
                skipCentralRefresh == False):

            return None

        request._cache_update_cache = False
        if ((('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) or (
                'json' in request.GET)):
            is_json = True
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        if isData==False:
            try:
                if request.method == "POST":
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path() + '?' + request.body)))
                else:
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            except: path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
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
    if isCache:
        is_json = False
        request._cache_update_cache = False
        if ((('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) or (
                    'json' in request.GET)):
            is_json = True
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        if isData==False:
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
    else:
        None

def preparePlotData(data):
    oldPlotData = data
    if isinstance(oldPlotData, dict):
        newPlotData = {}
        for key, value in oldPlotData.items():
            newPlotData[str(key)] = float(value)
    else:
        newPlotData = oldPlotData
    return newPlotData