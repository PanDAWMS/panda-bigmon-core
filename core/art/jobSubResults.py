
import os
import shutil
import json, math
import numpy as np
import logging
from datetime import datetime

from django.db import transaction, DatabaseError
from core.art.modelsART import ARTSubResult, ARTTests
import core.art.constants as const_art
from core.filebrowser.utils import get_job_log_file_path

from django.conf import settings

_logger = logging.getLogger('bigpandamon-art')


def subresults_getter(pandaid):
    """
    Getting ART report json avoiding HTTP calls to filebrowser
    :param pandaid: int
    :return: dictionary with sub-results
    """
    filename = 'artReport.json'
    subresults_dict = {}

    # get path of log file
    path_json = get_job_log_file_path(pandaid=pandaid, filename=filename)
    if path_json is not None and os.path.exists(path_json):
        with open(path_json) as json_file:
            try:
                data_json = json.load(json_file)
            except Exception as e:
                _logger.exception('Failed to read {} file\n{}'.format(filename, e))
                return {pandaid: subresults_dict}
    else:
        return {pandaid: subresults_dict}

    if isinstance(data_json, dict) and 'art' in data_json:
        subresults_dict = data_json['art']

    # protection of json format change from list to list of dicts
    if 'result' in subresults_dict and isinstance(subresults_dict['result'], list):
        resultlist = []
        for r in subresults_dict['result']:
            if not isinstance(r, dict):
                resultlist.append({'name': '', 'result': r})
            else:
                resultlist.append({'name': r['name'] if 'name' in r else '', 'result': r['result'] if 'result' in r else r})
            subresults_dict['result'] = resultlist
    _logger.info('ART Results for {} is {}'.format(str(pandaid), str(subresults_dict)))

    return {pandaid: subresults_dict}


def save_subresults(subResultsDict):
    """
    A function to save subresults of ART jobs to the special table - ART_SUBRESULT
    :param subResultsDict:
    :return: True or False
    """

    with transaction.atomic():
        for pandaid, data in subResultsDict.items():
            try:
                row = ARTSubResult(
                    pandaid=ARTTests.objects.get(pandaid=pandaid),
                    subresult=data
                )
                row.save()
            except DatabaseError as e:
                _logger.error(e)
                return False

    return True

def update_test_status(test_subresults):
    """
    A function to update test status in ART_TESTS table depending on sub-step results
    :param test_subresults: dict in format {'pandaid': pandaid, 'result': subresults}
    :return: True or False
    """
    pandaid = test_subresults['pandaid']
    # get final status to update test record
    status_index, _ = get_final_result(test_subresults, output='index')

    with transaction.atomic():
        _logger.info(f"Updating status of ART test {pandaid} to {status_index}")
        try:
            ARTTests.objects.filter(pandaid=pandaid).update(status=status_index)
        except DatabaseError as e:
            _logger.error(e)
            return False

    return True

def lock_nqueuedjobs(cur, nrows):
    """
    Function to lock first N rows for further processing
    :param nrows:
    :return: lock_time
    """

    lock_time = datetime.now().strftime(settings.DATETIME_FORMAT)
    lquery = """UPDATE {}.art_results_queue
                SET IS_LOCKED = 1,
                    LOCK_TIME = to_date('{}', 'YYYY-MM-DD HH24:MI:SS')
                WHERE rownum <= {} AND IS_LOCKED = 0""".format(settings.DB_SCHEMA, lock_time, nrows)
    try:
        cur.execute(lquery)
    except DatabaseError as e:
        _logger.error(e)
        raise

    return lock_time


def delete_queuedjobs(cur, lock_time):
    """
    A function to delete processed jobs from ART_RESULTS_QUEUE
    :param lock_time:
    :return:
    """

    dquery = """DELETE FROM {}.art_results_queue
                  WHERE IS_LOCKED = 1
                    AND LOCK_TIME = to_date('{}', 'YYYY-MM-DD HH24:MI:SS')""".format(settings.DB_SCHEMA, lock_time)
    try:
        cur.execute(dquery)
    except DatabaseError as e:
        _logger.error(e)
        raise

    return True


def clear_queue(cur):
    """
    A function to delete processed jobs from ART_RESULTS_QUEUE
    :param lock_time:
    :return:
    """

    cquery = """DELETE FROM {}.art_results_queue
                  WHERE IS_LOCKED = 1""".format(settings.DB_SCHEMA)
    try:
        cur.execute(cquery)
    except DatabaseError as e:
        _logger.error(e)
        raise

    return True


def get_final_result(job, output='str'):
    """ A function to determine the real ART test result depending on sub-step results, exitcode and PanDA job state
    0 - succeeded - green
    1 - finished - yellow
    2 - active - blue
    3 - failed - red
    :param job: dict - should contain 'jobstatus' and 'result' keys
    :param output: str - 'str' or 'index'
    :return: final_result: str or int
    :return: extra_params_dict: dict -
    """
    extra_params_dict = {
        'testexitcode': None,
        'subresults': None,
        'testdirectory': None,
        'reportjira': None,
        'reportmail': None,
        'description': None,
        'linktoplots': None,
    }

    # set final result preliminary based on jobstatus
    if 'jobstatus' in job and job['jobstatus'] == 'finished':
        final_result = 0
    elif 'jobstatus' in job and job['jobstatus'] in ('failed', 'cancelled'):
        final_result = 3
    elif 'jobstatus' in job:
        final_result = 2
    else:
        final_result = 3

    # load result subdict if it is str
    if 'result' in job:
        if isinstance(job['result'], str):
            try:
                job['result'] = json.loads(job['result'])
            except:
                job['result'] = None
    else:
        job['result'] = None

    # extract extra params from result subdict
    try:
        extra_params_dict['testexitcode'] = job['result']['exit_code'] if 'exit_code' in job['result'] else None
    except:
        pass
    try:
        extra_params_dict['subresults'] = job['result']['result'] if 'result' in job['result'] else None
    except:
        pass
    try:
        extra_params_dict['testdirectory'] = job['result']['test_directory'] if 'test_directory' in job['result'] else None
    except:
        pass
    try:
        extra_params_dict['reportjira'] = job['result']['report-to']['jira'] if 'report-to' in job['result'] and 'jira' in job['result']['report-to'] else None
    except:
        pass
    try:
        extra_params_dict['reportmail'] = job['result']['report-to']['mail'] if 'report-to' in job['result'] and 'mail' in job['result']['report-to'] else None
    except:
        pass
    try:
        extra_params_dict['description'] = job['result']['description'] if 'description' in job['result'] else None
    except:
        pass
    try:
        extra_params_dict['linktoplots'] = job['result']['url'] if 'url' in job['result'] else None
    except:
        pass

    # analyze result subdict to get final result
    if 'result' in job['result'] and job['result'] is not None:
        if 'result' in job['result'] and len(job['result']['result']) > 0:
            final_result = analize_test_subresults(job['result']['result'])
        elif 'exit_code' in job['result']:
            if job['result']['exit_code'] == 0:
                final_result = 0
            else:
                final_result = 3

    # format output
    if output == 'str':
        final_result_dict = {v: k for k, v in const_art.TEST_STATUS_INDEX.items()}
        final_result = final_result_dict[final_result]

    return final_result, extra_params_dict


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


def copy_payload_log_for_analysis(src_postfix, dst_postfix):
    """
    Copy payload logs for further analysis
    :return:
    """
    if dst_postfix is None:
        return False

    src = '/cephfs/atlpan/' + src_postfix + '/'
    dst = '/cephfs/atlpan/pandajoblogs/{}/'.format(dst_postfix)

    if not os.path.exists(dst):
        try:
            os.makedirs(dst)
        except Exception as e:
            _logger.error('Failed to create dst path dirs {}:\n{}'.format(dst, e))

    logs_to_collect = [f for f in os.listdir(src) if f in ('payload.stdout', 'pilotlog.txt', 'artReport.json') or (
                f.startswith("log.") and '.tgz' not in f)]
    for file in logs_to_collect:
        try:
            shutil.copy(src + file, dst + file)
            _logger.debug('Copying log file {}'.format(src + file))
        except Exception as e:
            _logger.error('Failed to copy log file {}:\n{}'.format(src + file, e))

    return True
