"""
Created by Tatiana Korchuganova on 04.10.2019
"""
import copy
import logging
import time
import json
from datetime import datetime, timedelta

from django.db.models.functions import Substr

from core.libs.sqlcustom import preprocess_wild_card_string
from core.art.modelsART import ARTTests
from core.art.jobSubResults import analize_test_subresults

from core.libs.job import get_job_list

import core.art.constants as art_const
artdateformat = '%Y-%m-%d'
_logger = logging.getLogger('bigpandamon-art')


def setupView(request):
    """
    Transform request params into ORM query object
    :param request:
    :param days: int - limit of N last nightlies to be used if no other ntags related params specified
    :return: query: dict
    :return: query_str: str - for where clause which is not supported by Django ORM
    """
    if not 'view' in request.session['requestParams']:
        request.session['requestParams']['view'] = 'packages'

    query = {}
    startdate = None
    enddate = None
    datelist = []

    art_view = str(request.path).split('/')[2]
    days_max = art_const.N_DAYS_MAX[art_view] if art_view in art_const.N_DAYS_MAX else art_const.N_DAYS_MAX['other']
    days_default = art_const.N_DAYS_DEFAULT[art_view] if art_view in art_const.N_DAYS_DEFAULT else art_const.N_DAYS_DEFAULT['other']

    # Process time range related params
    if 'ntag_from' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag_from']
        try:
            startdate = datetime.strptime(startdatestr, '%Y-%m-%d')
        except:
            del request.session['requestParams']['ntag_from']

    if 'ntag_to' in request.session['requestParams']:
        enddatestr = request.session['requestParams']['ntag_to']
        try:
            enddate = datetime.strptime(enddatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag_to']

    if 'ntag' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag']
        try:
            startdate = datetime.strptime(startdatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag']
    if 'ntags' in request.session['requestParams']:
        dateliststr = request.session['requestParams']['ntags']
        for datestr in dateliststr.split(','):
            try:
                datei = datetime.strptime(datestr, artdateformat)
                datelist.append(datei)
            except:
                pass
    if 'ntag_full' in request.session['requestParams']:
        startdatestr = request.session['requestParams']['ntag_full'][:10]
        try:
            startdate = datetime.strptime(startdatestr, artdateformat)
        except:
            del request.session['requestParams']['ntag_full']

    if 'ntag_from' in request.session['requestParams'] and not 'ntag_to' in request.session['requestParams']:
        enddate = startdate + timedelta(days=days_default)
    elif not 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams']:
        startdate = enddate - timedelta(days=days_default)
    elif 'ntag_from' in request.session['requestParams'] and 'ntag_to' in request.session['requestParams'] and (enddate-startdate).days > days_max:
        enddate = startdate + timedelta(days=days_max)
    if 'days' in request.session['requestParams']:
        try:
            days = int(request.session['requestParams']['days'])
        except:
            days = days_default
        enddate = datetime.now()
        if days < days_max:
            days = days_max
        startdate = enddate - timedelta(days=days)
    if 'nlastnightlies' in request.session['requestParams'] or (startdate is None and enddate is None and len(datelist) == 0):
        nlastnightlies = int(request.session['requestParams']['nlastnightlies']) if 'nlastnightlies' in request.session['requestParams'] else days_default
        datelist = find_last_n_nightlies(request, nlastnightlies)
        _logger.debug('Got n last nightly tags: {}'.format(time.time() - request.session['req_init_time']))

    # view params will be shown at page top
    if 'ntag' in request.session['requestParams']:
        request.session['viewParams']['ntag'] = startdate.strftime(art_const.DATETIME_FORMAT['humanized'])
    elif 'ntag_full' in request.session['requestParams']:
        request.session['viewParams']['ntag_full'] = request.session['requestParams']['ntag_full']
    elif len(datelist) > 0:
        request.session['viewParams']['ntags'] = ', '.join([date_i.strftime(art_const.DATETIME_FORMAT['humanized']) for date_i in datelist])
    else:
        request.session['viewParams']['ntag_from'] = startdate.strftime(art_const.DATETIME_FORMAT['humanized'])
        request.session['viewParams']['ntag_to'] = enddate.strftime(art_const.DATETIME_FORMAT['humanized'])

    # Process and prepare a query as a string
    query_str = '(1=1) '
    if 'ntag_full' in request.session['requestParams']:
        query['nightly_tag'] = request.session['requestParams']['ntag_full']
    elif 'ntag' in request.session['requestParams']:
        query['nightly_tag__startswith'] = request.session['requestParams']['ntag']
    elif startdate is not None and enddate is not None:
        query['nightly_tag_date__range'] = [startdate, enddate + timedelta(days=1) - timedelta(seconds=1)]
    elif len(datelist) == 1:
        query['nightly_tag_date__range'] = [datelist[0], datelist[0] + timedelta(days=1) - timedelta(seconds=1)]
    elif len(datelist) > 1:
        query_str += ' and ('
        query_str += ' or '.join([f"nightly_tag like '{date_i.strftime(artdateformat)}%%'" for date_i in datelist])
        query_str += ') '

    art_tests_str_fields = copy.deepcopy(
        [f.name for f in ARTTests._meta.get_fields() if not f.is_relation and 'String' in f.description])
    for p, v in request.session['requestParams'].items():
        if p == 'branch' and ',' in v:
            query['branch__in'] = v.split(',')
            continue
        elif p == 'package' and ',' in v:
            query['package__in'] = v.split(',')
            continue
        elif p == 'package' and '*' in v:
            query_str += 'and ' + preprocess_wild_card_string(v, p)
            continue
        for f in art_tests_str_fields:
            if p == f:
                query[f] = v

    return query, query_str


def find_last_n_nightlies(request, limit=7):
    """
    Find N last nightlies dates in registered tests
    :param request:
    :param limit: int - how many nightlies to return
    :return: list of ntags
    """
    nquery = {}
    querystr = '(1=1)'
    if 'package' in request.session['requestParams'] and not ',' in request.session['requestParams']['package'] and not '*' in request.session['requestParams']['package']:
        nquery['package'] = request.session['requestParams']['package']
    elif 'package' in request.session['requestParams'] and ',' in request.session['requestParams']['package']:
        nquery['package__in'] = [p for p in request.session['requestParams']['package'].split(',')]
    elif 'package' in request.session['requestParams'] and '*' in request.session['requestParams']['package']:
        querystr += ' AND ' + preprocess_wild_card_string(request.session['requestParams']['package'], 'package')
    if 'branch' in request.session['requestParams']:
        branches = request.session['requestParams']['branch'].split(',')
        querystr += ' AND (NIGHTLY_RELEASE_SHORT || \'/\' || PROJECT || \'/\' || PLATFORM)  IN ( '
        for b in branches:
            querystr += '(\'' + b + '\'), '
        if querystr.endswith(', '):
            querystr = querystr[:len(querystr) - 2]
        querystr += ')'
    if 'testname' in request.session['requestParams']:
        nquery['testname'] = request.session['requestParams']['testname']
    ndates = ARTTests.objects.filter(**nquery).extra(where=[querystr]).annotate(ndate=Substr('nightly_tag_display', 1, 10)).values('ndate').order_by('-ndate').distinct()[:limit]

    datelist = []
    for datestr in ndates:
        try:
            datei = datetime.strptime(datestr['ndate'], artdateformat)
            datelist.append(datei)
        except:
            pass
    datelist = sorted(datelist)

    return datelist


def find_last_successful_test(testname, branch):
    """
    :param testname:
    :param branch:
    :return:
    """
    last_successful_test = {}
    query = {
        'testname': testname,
        'nightly_release_short': (branch.split('/'))[0],
        'project': (branch.split('/'))[1],
        'platform': (branch.split('/'))[2]
    }
    tests = []
    tests.extend(ARTTests.objects.filter(**query).values('pandaid', 'nightly_tag', 'artsubresult__subresult').order_by('-pandaid'))

    for t in tests:
        if t['artsubresult__subresult'] is not None and len(t['artsubresult__subresult']) > 0:
            subresults_dict_tmp = json.loads(t['artsubresult__subresult'])
            if 'result' in subresults_dict_tmp and len(subresults_dict_tmp['result']) > 0:
                if analize_test_subresults(subresults_dict_tmp['result']) < 1:
                    last_successful_test = t
            elif 'exit_code' in subresults_dict_tmp and subresults_dict_tmp['exit_code'] == 0:
                last_successful_test = t
            else:
                # get job status
                jobs = get_job_list({'pandaid': t['pandaid']})
                if len(jobs) > 0 and 'jobstatus' in jobs[0] and jobs[0]['jobstatus'] == 'finished':
                    last_successful_test = t

        if len(last_successful_test) > 0:
            break

    return last_successful_test


def getjflag(job):
    """Returns flag if job in finished state"""
    return 1 if job['jobstatus'] in ('finished', 'failed', 'cancelled', 'closed') else 0


def build_gitlab_link(package, testname):
    if package and len(package) > 0 and testname and len(testname) > 0 and package in art_const.GITLAB_PATH_PER_PACKAGE:
        gitlab_path = art_const.GITLAB_PATH_PER_PACKAGE[package]
        return f'https://gitlab.cern.ch/atlas/athena/blob/main/{gitlab_path}/{testname}'
    else:
        return None

def get_result_for_multijob_test(states):
    """Return worst final result for a test that has several PanDA jobs"""
    result = None
    state_dict = {
        'active': 0,
        'failed': 1,
        'finished': 2,
        'succeeded': 3,
    }

    result_index = min([state_dict[s] for s in list(set(states)) if s in state_dict])
    result = list(state_dict.keys())[result_index] if result_index < len(state_dict) else None

    return result


def concat_branch(job):
    """
    Concat branch from 3 params from ART_TESTS table
    :param job:
    :return:
    """
    branch = None
    if 'nightly_release_short' in job and 'project' in job and 'platform' in job:
        branch = '{}/{}/{}'.format(job['nightly_release_short'], job['project'], job['platform'])
    return branch


def get_test_diff(test_a, test_b):
    """
    Finding difference in 2 test results
    :param test_1: dict, {finalstatus, subresults} - previous.
    :param test_2: dict, {finalstatus, subresults} - current.
    :return: index

    """
    state_index = {'active': 3, 'succeeded': 2, 'finished': 1, 'failed': 0}
    result_translation = {
        -1: 'active',
        0: 'ok',
        1: 'warning_b',
        2: 'warning',
        3: 'warning_w',
        4: 'alert',
    }
    diff_matrix = [
        [4,   2,  0, -1],
        [4,   9,  0, -1],
        [4,   2,  0, -1],
        [-1, -1, -1, -1]
    ]
    result = diff_matrix[state_index[test_a['finalresult']]][state_index[test_b['finalresult']]]

    if result == 9:
        # compare substep results
        is_diff = None
        for step in range(0, len(test_b['subresults'])):
            try:
                if len(test_a['subresults']) - 1 > step and test_a['subresults'][step]['name'] == test_b['subresults'][step]['name']:
                    if test_a['subresults'][step]['result'] != test_b['subresults'][step]['result']:
                        is_diff = test_b['subresults'][step]['result'] - test_a['subresults'][step]['result']
                        break
            except:
                print('ddd')
        if is_diff is not None:
            result = 1 if is_diff < 0 else 3
        else:
            result = 2

    return result_translation[result]


def clean_tests_list(tests, add_link_previous_attempt=False):
    """
    Dropping previous attempts of tests, leaving only last attempt
    :param tests: list of dict
    :param add_link_previous_attempt: bool - add link to previous attempt or not
    :return: tests_filtered: list of dict
    """

    tmp_dict = {}
    for t in tests:
        m = f"{t['testname']}{t['nightly_release_short']}{t['project']}{t['platform']}{t['package']}{t['nightly_tag']}{t['jeditaskid']}{t['inputfileid']}"
        if m not in tmp_dict:
            tmp_dict[m] = []
        tmp_dict[m].append(t['pandaid'])

    tests_filtered = []
    for t in tests:
        m = f"{t['testname']}{t['nightly_release_short']}{t['project']}{t['platform']}{t['package']}{t['nightly_tag']}{t['jeditaskid']}{t['inputfileid']}"
        if t['pandaid'] == max(tmp_dict[m]):
            t['ntag'] = t['nightly_tag_date'].replace(hour=0, minute=0, second=0, microsecond=0)  # only date is needed
            if add_link_previous_attempt and len(tmp_dict[m]) > 1:
                t['linktopreviousattemptlogs'] = f"?pandaid={min(tmp_dict[m])}"
            tests_filtered.append(t)

    return tests_filtered