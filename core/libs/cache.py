import json
import hashlib
import socket
import uuid
import logging
from collections import defaultdict

from django.utils import encoding
from django.conf import settings
from django.core.cache import cache

from core.utils import is_json_request
from core.libs.DateEncoder import DateEncoder


def getCacheEntry(request, viewType, skipCentralRefresh = False, isData = False):
    """
    Getting cache entry
    :param request:
    :param viewType:
    :param skipCentralRefresh:
    :param isData:
    :return:
    """
    isCache = True
    if isCache:
        is_json = is_json_request(request)

        # We do this check to always rebuild cache for the page when it called from the crawler
        if (('HTTP_X_FORWARDED_FOR' in request.META) and (request.META['HTTP_X_FORWARDED_FOR'] in settings.CACHING_CRAWLER_HOSTS) and
                skipCentralRefresh == False):
            return None

        request._cache_update_cache = False
        if isData is False:
            try:
                if request.method == "POST":
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path() + '?' + request.body)))
                else:
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            except:
                path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            cache_key = '{}_{}_{}_.{}'.format(is_json, settings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType, path.hexdigest())
            return cache.get(cache_key, None)
        else:
            cache_key = '{}_{}'.format(settings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
            return cache.get(cache_key, None)
    else:
        return None


def setCacheEntry(request, viewType, data, timeout, isData = False):
    """
    Putting data to cache
    :param request:
    :param viewType:
    :param data:
    :param timeout:
    :param isData:
    :return:
    """
    isCache = True
    # do not cache data for 'refreshed' pages
    if 'requestParams' in request.session and 'timestamp' in request.session['requestParams'] and not isData:
        isCache = False
    if isCache:
        is_json = is_json_request(request)
        request._cache_update_cache = False
        if isData == False:
            try:
                if request.method == "POST":
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path() + '?' + request.body)))
                else:
                    path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            except: path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
            cache_key = '{}_{}_{}_.{}'.format(is_json, settings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType, path.hexdigest())
        else:
            cache_key = '{}_{}'.format(settings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        cache.set(cache_key, data, timeout)


def setCacheData(request,lifetime=60*120,**parametrlist):
    transactionKey = uuid.uuid4().hex
    dictinoary = {}
    dictinoary[transactionKey] = {}
    keys = parametrlist.keys()
    for key in keys:
        dictinoary[transactionKey][key] = str(parametrlist[key])
    data = json.dumps(dictinoary, cls=DateEncoder)
    setCacheEntry(request, str(transactionKey), data, lifetime,isData=True)

    return transactionKey


def getCacheData(request,requestid):
    data = getCacheEntry(request, str(requestid), isData=True)
    if data is not None:
        data = json.loads(data)
        if 'childtk'in data[requestid]:
            tklist = defaultdict(list)
            data = str(data[requestid]['childtk']).split(',')
            if data is not None:
                for child in data:
                    ch = getCacheEntry(request, str(child), isData=True)
                    if ch is not None:
                        ch = json.loads(ch)
                        # merge data
                        for k, v in ch[child].items():
                            tklist[k].append(v)
                data = {}
                for k,v in tklist.items():
                    data[k] = ','.join(v)
        else:
            data = data[requestid]
        return data
    else:
        return None


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

    absolute_path = settings.BASE_DIR + settings.STATIC_URL if settings.BASE_DIR and settings.STATIC_URL else None
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

