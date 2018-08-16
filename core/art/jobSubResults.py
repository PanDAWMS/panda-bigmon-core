

import urllib, urllib3, json
from datetime import datetime
from core.art.modelsART import ARTSubResult
from django.db import connection, transaction, DatabaseError
from core.settings import defaultDatetimeFormat
import requests, multiprocessing

def getJobReport(guid, lfn, scope):
    filebrowserURL = "http://bigpanda.cern.ch/filebrowser/"  # This is deployment specific because memory monitoring is intended to work in ATLAS
    jobSubResult = []
    http = urllib3.PoolManager()
    resp = http.request('GET', filebrowserURL, fields={'guid': guid, 'lfn': lfn, 'scope': scope, 'json': 1})
    if resp and len(resp.data) > 0:
        try:
            data = json.loads(resp.data)
            HOSTNAME = data['HOSTNAME']
            tardir = data['tardir']
            MEDIA_URL = data['MEDIA_URL']
            dirprefix = data['dirprefix']
            files = data['files']
            files = [f for f in files if 'jobReport.json' in f['name']]
        except:
            return -2
    else:
        return -2

    urlBase = "http://" + HOSTNAME + "/" + MEDIA_URL + dirprefix + "/" + tardir

    for f in files:
        url = urlBase + "/" + f['name']
        response = http.request('GET', url)
        data = json.loads(response.data)

    return data

def getARTjobSubResults(data):
    jobSubResult = {}

    if isinstance(data, dict) and 'art' in data:
        jobSubResult = data['art']

    # protection of json format change from list to list of dicts
    if 'result' in jobSubResult and isinstance(jobSubResult['result'], list):
        resultlist = []
        for r in jobSubResult['result']:
            if not isinstance(r, dict):
                resultlist.append({'name': '', 'result': r})
            else:
                resultlist.append({'name': r['name'] if 'name' in r else '', 'result': r['result'] if 'result' in r else r})
        jobSubResult['result'] = resultlist
    return jobSubResult


def subresults_getter(url_params_str):
    """
    A function for getting ART jobs sub results in multithreading mode
    :return: dictionary with sub-results
    """
    base_url = "http://bigpanda.cern.ch/filebrowser/?json=1"
    subresults_dict = {}

    pandaidstr = url_params_str.split('=')[-1]
    try:
        pandaid = int(pandaidstr)
    except:
        print "PandaID can not be transformed to int type"
        pass

    http = urllib3.PoolManager()
    resp = http.request('GET', base_url + url_params_str)
    if resp and len(resp.data) > 0:
        try:
            data = json.loads(resp.data)
            HOSTNAME = data['HOSTNAME']
            tardir = data['tardir']
            MEDIA_URL = data['MEDIA_URL']
            dirprefix = data['dirprefix']
            files = data['files']
            files = [f for f in files if 'jobReport.json' in f['name']]
        except:
            return -2
    else:
        return -2

    urlBase = "http://" + HOSTNAME + "/" + MEDIA_URL + dirprefix + "/" + tardir

    for f in files:
        url = urlBase + "/" + f['name']
        response = http.request('GET', url)
        data = json.loads(response.data)

    if isinstance(data, dict) and 'art' in data:
        subresults_dict = data['art']

    # protection of json format change from list to list of dicts
    if 'result' in subresults_dict and isinstance(subresults_dict['result'], list):
        resultlist = []
        for r in subresults_dict['result']:
            if not isinstance(r, dict):
                resultlist.append({'name': '', 'result': r})
            else:
                resultlist.append({'name': r['name'] if 'name' in r else '', 'result': r['result'] if 'result' in r else r})
            subresults_dict['result'] = resultlist

    return {pandaid: subresults_dict}


def save_subresults(subResultsDict):
    """
    A function to save subresults of ART jobs to the special table - ART_SUBRESULT
    :param subResultsDict:
    :return: True or False
    """
    try:
        with transaction.atomic():
            for pandaid, data in subResultsDict.iteritems():
                row = ARTSubResult(pandaid=pandaid,
                                   subresult=data)
                row.save()
    except DatabaseError as e:
        print e.message
        return False

    return True


def lock_nqueuedjobs(cur, nrows):
    """
    Function to lock first N rows for futher processing
    :param nrows:
    :return: lock_time
    """

    lock_time = datetime.now().strftime(defaultDatetimeFormat)
    lquery = """UPDATE atlas_pandabigmon.art_results_queue
                SET IS_LOCKED = 1,
                    LOCK_TIME = to_date('%s', 'YYYY-MM-DD HH24:MI:SS')
                WHERE rownum <= %i AND IS_LOCKED = 0""" % (lock_time, nrows)
    try:
        cur.execute(lquery)
    except DatabaseError as e:
        print e.message
        raise

    return lock_time


def delete_queuedjobs(cur, lock_time):
    """
    A function to delete processed jobs from ART_RESULTS_QUEUE
    :param lock_time:
    :return:
    """

    dquery = """DELETE FROM atlas_pandabigmon.art_results_queue
                  WHERE IS_LOCKED = 1
                    AND LOCK_TIME = to_date('%s', 'YYYY-MM-DD HH24:MI:SS')""" % (lock_time)
    try:
        cur.execute(dquery)
    except DatabaseError as e:
        print e.message
        raise



    return True