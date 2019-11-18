

import urllib3, json, math
import numpy as np
from datetime import datetime
from core.art.modelsART import ARTSubResult
from django.db import transaction, DatabaseError
from core.settings import defaultDatetimeFormat
import logging

_logger = logging.getLogger('bigpandamon-error')

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
        _logger.exception('Exception was caught while transforming pandaid from str to int.')
        raise

    print('Loading {}'.format(base_url+url_params_str))

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
            _logger.exception('Exception was caught while seeking jobReport.json in logs for PanDA job: {}'.format(str(pandaid)))
            return {}
    else:
        _logger.exception('Exception was caught while downloading logs using Rucio for PanDA job: {}'.format(str(pandaid)))
        return {}

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

    print('ART Results for {} is {}'.format(str(pandaid), str(subresults_dict)))

    return {pandaid: subresults_dict}


def save_subresults(subResultsDict):
    """
    A function to save subresults of ART jobs to the special table - ART_SUBRESULT
    :param subResultsDict:
    :return: True or False
    """
    try:
        with transaction.atomic():
            for pandaid, data in subResultsDict.items():
                row = ARTSubResult(pandaid=pandaid,
                                   subresult=data)
                row.save()
    except DatabaseError as e:
        print (e)
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
        print (e)
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
        print (e)
        raise

    return True


def clear_queue(cur):
    """
    A function to delete processed jobs from ART_RESULTS_QUEUE
    :param lock_time:
    :return:
    """

    cquery = """DELETE FROM atlas_pandabigmon.art_results_queue
                  WHERE IS_LOCKED = 1"""
    try:
        cur.execute(cquery)
    except DatabaseError as e:
        print (e)
        raise

    return True

def get_final_result(job):
    """ A function to determine the real ART test result depending on sub-step results, exitcode and PanDA job state
    0 - done - green
    1 - finished - yellow
    2 - running - blue
    3 - failed - red
    """
    finalResultDict = {0: 'done', 1: 'finished', 2: 'active', 3: 'failed'}
    extraParamsDict = {
        'testexitcode': None,
        'subresults': None,
        'testdirectory': None,
        'reportjira': None,
        'reportmail': None,
        'description': None
        }
    finalresult = ''
    if job['jobstatus'] == 'finished':
        finalresult = 0
    elif job['jobstatus'] in ('failed', 'cancelled'):
        finalresult = 3
    else:
        finalresult = 2
    try:
        job['result'] = json.loads(job['result'])
    except:
        job['result'] = None
    try:
        extraParamsDict['testexitcode'] = job['result']['exit_code'] if 'exit_code' in job['result'] else None
    except:
        pass
    try:
        extraParamsDict['subresults'] = job['result']['result'] if 'result' in job['result'] else []
    except:
        pass
    try:
        extraParamsDict['testdirectory'] = job['result']['test_directory'] if 'test_directory' in job['result'] else []
    except:
        pass
    try:
        extraParamsDict['reportjira'] = job['result']['report-to']['jira'] if 'report-to' in job['result'] and 'jira' in job['result']['report-to'] else None
    except:
        pass
    try:
        extraParamsDict['reportmail'] = job['result']['report-to']['mail'] if 'report-to' in job['result'] and 'mail' in job['result']['report-to'] else None
    except:
        pass
    try:
        extraParamsDict['description'] = job['result']['description'] if 'description' in job['result'] else None
    except:
        pass

    if job['result'] is not None:
        if 'result' in job['result'] and len(job['result']['result']) > 0:
            finalresult = analize_test_subresults(job['result']['result'])
            # for r in job['result']['result']:
            #     if int(r['result']) > 0:
            #         finalresult = 'failed'
        elif 'exit_code' in job['result'] and job['result']['exit_code'] > 0:
            finalresult = 3

    finalresult = finalResultDict[finalresult]

    return finalresult, extraParamsDict


def analize_test_subresults(subresults):
    """A function for analysing a sub-step results to decide if test failed in its first steps which are Athena or Reco
    or in additional checks and comparison steps
    """
    finalstate = -1

    if not any([r['result'] for r in subresults]) > 0:
        finalstate = 0
    else:
        weights = [math.exp(-i) for i in range(0, len(subresults))]
        weights_normalized = [w/sum(weights) for w in weights]
        bin_subresults = [0 if int(r['result']) > 0 else 1 for r in subresults]
        weighted_subresults = np.multiply(weights_normalized, bin_subresults)
        if sum(weighted_subresults) > 0.5:
            finalstate = 1
        else:
            finalstate = 3

    return finalstate