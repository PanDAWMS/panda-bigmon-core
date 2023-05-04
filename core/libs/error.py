"""
Set of functions related to job errors parsing and extracting descriptions

Created by Tatiana Korchuganova on 05.03.2020
"""
import json
import logging
import numpy as np
from html import escape

from django.core.cache import cache
from django.conf import settings

from core.pandajob.models import Jobsarchived4, Jobsarchived

from core.libs.exlib import get_tmp_table_name, insert_to_temp_table

from core.libs.ErrorCodes import ErrorCodes, ErrorCodesAtlas
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def get_job_error_desc():
    """
    Get ErrorCodes and put into cache
    :return:
    """
    try:
        error_desc_dict = cache.get('errorCodes', None)
    except Exception as e:
        _logger.warning('Can not get error codes from cache: \n{} \nLoading directly instead...'.format(e))
        error_desc_dict = None
    error_desc_dict = None
    if not error_desc_dict:
        if 'ATLAS' in settings.DEPLOYMENT:
            codes = ErrorCodesAtlas()
        else:
            codes = ErrorCodes()
        error_desc_dict = codes.getErrorCodes()
        cache.set('errorCodes', error_desc_dict, 60*60*24)
    return error_desc_dict


def getErrorDescription(job, mode='html', provideProcessedCodes = False, **kwargs):
    txt = ''
    codesDescribed = []

    if 'errorCodes' in kwargs:
        errorCodes = kwargs['errorCodes']
    else:
        errorCodes = get_job_error_desc()

    if 'metastruct' in job:
        if type(job['metastruct']) is np.unicode:
            try:
                meta = json.loads(job['metastruct'])
            except:
                print ('Meta type: '+str(type(job['metastruct'])))
                meta = job['metastruct']
            if 'exitCode' in meta and meta['exitCode'] != 0:
                txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
                if provideProcessedCodes:
                    return txt, codesDescribed
                else:
                    return txt
            else:
                if provideProcessedCodes:
                    return '-', codesDescribed
                else:
                    return '-'
        else:
            meta = job['metastruct']
            if 'exitCode' in meta and meta['exitCode'] != 0:
                txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
                if provideProcessedCodes:
                    return txt, codesDescribed
                else:
                    return txt

    for errcode in errorCodes:
        errval = 0
        if errcode in job:
            errval = job[errcode]
            if errval != 0 and errval != '0' and errval != None and errval != '':
                try:
                    errval = int(errval)
                except:
                    pass  # errval = -1
                codesDescribed.append(errval)
                errdiag = errcode.replace('errorcode', 'errordiag')
                if errcode.find('errorcode') > 0:
                    if job[errdiag] is not None:
                        diagtxt = str(job[errdiag])
                    else:
                        diagtxt = ''
                else:
                    diagtxt = ''
                if len(diagtxt) > 0:
                    desc = diagtxt
                elif errval in errorCodes[errcode]:
                    desc = errorCodes[errcode][errval]
                else:
                    desc = "Unknown %s error code %s" % (errcode, errval)
                errname = errcode.replace('errorcode', '')
                errname = errname.replace('exitcode', '')
                if mode == 'html':
                    txt += " <b>%s, %d:</b> %s" % (errname, errval, desc)
                else:
                    txt = "%s, %d: %s" % (errname, errval, desc)
    if provideProcessedCodes:
        return txt, codesDescribed
    else:
        return txt


def errorInfo(job, nchars=300, mode='html', **kwargs):
    errtxt = ''
    err1 = ''
    if 'errorCodes' in kwargs:
        errorCodes = kwargs['errorCodes']
    else:
        errorCodes = get_job_error_desc()

    desc, codesDescribed = getErrorDescription(job, provideProcessedCodes=True, errorCodes=errorCodes)

    for error_cat in const.JOB_ERROR_CATEGORIES:
        if error_cat['error'] in job and job[error_cat['error']] != '' and not job[error_cat['error']] is None and int(job[error_cat['error']]) != 0 and int(job[error_cat['error']]) not in codesDescribed:
            if error_cat['diag'] is not None:
                errtxt += '{} {}: {} <br>'.format(
                    error_cat['title'],
                    job[error_cat['error']],
                    escape(job[error_cat['diag']], quote=True)
                )
                if err1 == '':
                    err1 = "{}: {}".format(error_cat['name'], escape(job[error_cat['diag']], quote=True))
            else:
                errtxt += '{} {} <br>'.format(error_cat['title'], job[error_cat['error']])
                if err1 == '':
                    err1 = "{}: {}".format(error_cat['name'], job[error_cat['error']])

    if len(desc) > 0:
        errtxt += '%s<br>' % desc
        if err1 == '':
            err1 = getErrorDescription(job, mode='string', errorCodes=errorCodes)

    if err1.find('lost heartbeat') >= 0:
        err1 = 'lost heartbeat'
    if err1.lower().find('unknown transexitcode') >= 0:
        err1 = 'unknown transexit'
    if err1.find(' at ') >= 0:
        err1 = err1[:err1.find(' at ') - 1]
    if errtxt.find('lost heartbeat') >= 0:
        err1 = 'lost heartbeat'
    err1 = err1.replace('\n', ' ')

    if mode == 'html':
        return errtxt
    else:
        return err1[:nchars]


def get_job_errors(pandaids):
    """
    Get error info for a list of PanDA jobs
    :param pandaids: list of pandaids
    :return: errors_dict: dict
    """
    MAX_ENTRIES__IN = 100

    errors_dict = {}

    jobs = []
    jquery = {}
    extra_str = ' (1=1) '
    values = (
        'pandaid',
        'transexitcode',
        'brokerageerrorcode', 'brokerageerrordiag',
        'ddmerrorcode', 'ddmerrordiag',
        'exeerrorcode', 'exeerrordiag',
        'jobdispatchererrorcode', 'jobdispatchererrordiag',
        'piloterrorcode', 'piloterrordiag',
        # 'superrorcode', 'superrordiag',
        'taskbuffererrorcode', 'taskbuffererrordiag'
        )

    if len(pandaids) > 0 and len(pandaids) <= MAX_ENTRIES__IN:
        jquery['pandaid__in'] = pandaids
    elif len(pandaids) > 0 and len(pandaids) > MAX_ENTRIES__IN:
        # insert pandaids to temp DB table
        tmp_table_name = get_tmp_table_name()
        tk_pandaids = insert_to_temp_table(pandaids)
        extra_str += " AND pandaid in (select ID from {} where TRANSACTIONKEY={})".format(tmp_table_name, tk_pandaids)
    else:
        return errors_dict

    jobs.extend(Jobsarchived4.objects.filter(**jquery).extra(where=[extra_str]).values(*values))
    jobs.extend(Jobsarchived.objects.filter(**jquery).extra(where=[extra_str]).values(*values))

    for job in jobs:
        errors_dict[job['pandaid']] = errorInfo(job)

    return errors_dict