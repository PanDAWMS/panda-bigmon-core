"""
Created by Tatiana Korchuganova on 04.10.2019
"""
import copy
import logging
import time
import json
import re
from collections import Counter
from datetime import datetime, timedelta
from queue import Empty

from django.core.cache import cache
from django.db import close_old_connections
from django.db.models.functions import Substr

from core.libs.job import get_job_list
from core.libs.sqlcustom import preprocess_wild_card_string
from core.art.models import ARTTests
from core.art.jobSubResults import analize_test_subresults, get_final_result
from core.pandajob.models import JediTasks

from django.conf import settings
import core.art.constants as art_const
import core.constants as const

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
    if 'view' not in request.session['requestParams']:
        request.session['requestParams']['view'] = 'packages'

    query = {}
    startdate = None
    enddate = None
    datelist = []

    art_view = str(request.path).split('/')[2] if len(request.path.split('/')) > 2 else 'main'
    days_max = art_const.N_DAYS_MAX[art_view] if art_view in art_const.N_DAYS_MAX else art_const.N_DAYS_MAX['other']
    days_default = art_const.N_DAYS_DEFAULT[art_view] if art_view in art_const.N_DAYS_DEFAULT else art_const.N_DAYS_DEFAULT['other']

    # Process time range-related params
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

    if 'ntag_from' in request.session['requestParams'] and 'ntag_to' not in request.session['requestParams']:
        enddate = startdate + timedelta(days=days_default)
    elif 'ntag_from' not in request.session['requestParams'] and 'ntag_to' in request.session['requestParams']:
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
    elif startdate is not None and enddate is not None:
        request.session['viewParams']['ntag_from'] = startdate.strftime(art_const.DATETIME_FORMAT['humanized'])
        request.session['viewParams']['ntag_to'] = enddate.strftime(art_const.DATETIME_FORMAT['humanized'])

    # Process and prepare a query as a string
    query_str = '(1=1) '
    if 'pandaid' in request.session['requestParams'] and request.session['requestParams']['pandaid'].isdigit():
        query['pandaid'] = int(request.session['requestParams']['pandaid'])
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


def build_gitlab_link(package, nightly_release_short,  testname):
    """
    Compose a link to test code in GitLab
    :param package: str
    :param nightly_release_short: str
    :param testname: str
    :return: https_link: str
    """
    if (package and len(package) > 0 and testname and len(testname) > 0 and nightly_release_short and len(nightly_release_short) > 0
            and package in art_const.GITLAB_PATH_PER_PACKAGE):
        gitlab_path = art_const.GITLAB_PATH_PER_PACKAGE[package]
        return f'https://gitlab.cern.ch/atlas/athena/blob/{nightly_release_short}/{gitlab_path}/{testname}'
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
    Concat branch from 3 params from the ART_TESTS table
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
                _logger.warning(f"Can not compare subresults for test {test_a['testname']} and {test_b['testname']}")
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


def get_client_ip(request):
    # Get the 'X-Forwarded-For' header (which contains the real client IP if behind a proxy)
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        # If there are multiple IP addresses in the header (because of multiple proxies),
        # the first one is usually the original client IP
        ip = x_forwarded_for.split(',')[0]
    else:
        # Otherwise, use the direct connection's IP address (REMOTE_ADDR)
        ip = request.META.get('REMOTE_ADDR')

    return ip


def get_test_results(n_days, test_type='grid', agg_by='branch'):
    """
    Get test results from ARTTests model
    :param n_days: int - number of days to look back for tests
    :param test_type: str - type of test, default is 'grid'
    :param agg_by: str - aggregation field, default is 'branch'
    :return: list of dicts with test results
    """
    if agg_by not in ['branch', 'package']:
        raise ValueError("agg_by must be one of ['branch', 'package']")

    results_aggregated = {}
    query = {}
    if test_type == 'all':
        query['test_type__in'] = ['grid', 'local']
    elif test_type in ['grid', 'local']:
        query['test_type__in'] = [test_type,]
    else:
        raise ValueError("test_type must be one of ['grid', 'local', 'all']")
    if n_days is None or not isinstance(n_days, int) or n_days <= 0:
        raise ValueError("n_days must be an integer and greater than 0")
    elif n_days > art_const.N_DAYS_MAX['main']:
        raise ValueError("n_days must be less than or equal to {}".format(art_const.N_DAYS_MAX))
    else:
        query['nightly_tag_date__gte'] = datetime.now() - timedelta(days=n_days)

    tests = ARTTests.objects.filter(**query).values(
        'package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag', 'attemptnr', 'status',
        'pandaid', 'jeditaskid', 'testname', 'inputfileid', 'nightly_tag_date', 'test_type'
    )
    tests = clean_tests_list(tests)
    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    for test in tests:
        if 'status' in test and isinstance(test['status'], int):
            result = final_result_dict[test['status']]
        else:
            result = 'active'  # default to active if status is not set or not an int
        if test[agg_by] not in results_aggregated:
            results_aggregated[test[agg_by]] = {}
        if test['nightly_tag'] not in results_aggregated[test[agg_by]]:
            results_aggregated[test[agg_by]][test['nightly_tag']] = {t: {x: 0 for x in art_const.TEST_STATUS} for t in query['test_type__in']}
        results_aggregated[test[agg_by]][test['nightly_tag']][test['test_type']][result] += 1

    return results_aggregated


def get_unregistered_tests(query_test=None, query_test_str=None):
    """Return a list of ART tasks which stuck or does not have started jobs yet"""
    tests_unregistered = []
    tasks = []
    cache_key = "art:unregistered_tests"
    cached_tasks = cache.get(cache_key)
    if cached_tasks is not None:
        tasks = cached_tasks
        _logger.debug(f"{len(tasks)} tasks found in cache")
    else:
        n_days = 7
        query_task = {'username': 'artprod', 'creationdate__gte': datetime.now() - timedelta(days=n_days)}
        where_str = f'jeditaskid not in (select jeditaskid from {settings.DB_SCHEMA}.art_tests where nightly_tag_date > sysdate - {n_days})'
        tasks.extend(JediTasks.objects.filter(**query_task).extra(where=[where_str]).values('jeditaskid', 'creationdate', 'taskname', 'status'))

        # put tasks in cache
        cache.set(cache_key, tasks, 60 * 10)
        _logger.debug(f'Found {len(tasks)} tasks in PanDA DB and cached')

    # translate the query for art_tests into regex for taskname
    taskname_start_sub = r"^group\.art\.atlas"
    branch_sub = r".*?"
    ntag_sub = r"\d{4}-\d{2}-\d{2}T\d{4}"
    package_sub = r"[^.]+"
    if query_test is not None:
        # handle branch or branch__in
        if 'branch' in query_test:
            branch_val = str(query_test['branch']).replace('/', '.')
            branch_sub = re.escape(branch_val)
        elif 'branch__in' in query_test:
            branches = [re.escape(str(b).replace('/', '.')) for b in query_test['branch__in']]
            branch_sub = f"({'|'.join(branches)})"

        # handle nightly tag or prefix
        if 'nightly_tag' in query_test:
            ntag_sub = re.escape(str(query_test['nightly_tag']))
        elif 'nightly_tag__startswith' in query_test:
            ntag_sub = re.escape(str(query_test['nightly_tag__startswith'])) + r"T\d{4}"

        # handle package or package__in
        if 'package' in query_test:
            package_sub = re.escape(str(query_test['package']))
        elif 'package__in' in query_test:
            packages = [re.escape(str(p)) for p in query_test['package__in']]
            package_sub = f"({'|'.join(packages)})"

    # handle wildcards from query_test_str
    if query_test_str is not None:
        match_p = re.search(r"package\s+like\s+'([^']+)'", query_test_str, re.IGNORECASE)
        if match_p:
            wildcard_val = match.group(1).replace('%', '.*')
            package_sub = wildcard_val
        if 'nightly_tag like' in query_test_str:
            ntag_values = re.findall(r"nightly_tag like '([^']+)'", query_test_str)
            if len(ntag_values) > 0:
                ntag_values = [re.escape(v[:-2]) + '.*' if v.endswith('%%') else re.escape(v) for v in ntag_values]
                ntag_sub = f"(?:{'|'.join(ntag_values)})"

    # translate back the taskname to branch, ntag, package
    task_parser_regex = re.compile(
        rf'{taskname_start_sub}\.'
        rf'(?P<branch>{branch_sub})\.'
        rf'(?P<ntag>{ntag_sub})\.'
        r'\d+\.'
        rf'(?P<package>{package_sub})\.'
        r'\d+/$'
    )

    for t in tasks:
        match = task_parser_regex.match(t['taskname'])
        if match:
            gd = match.groupdict()
            branch_parts = gd['branch'].split('.')
            if len(branch_parts) == 3:
                branch = '/'.join(branch_parts)
            elif len(branch_parts) == 4:
                branch = f"{branch_parts[0]}.{branch_parts[1]}/{branch_parts[2]}/{branch_parts[3]}"
            else:
                branch = gd['branch'].replace('.', '/')
            tests_unregistered.append({
                'jeditaskid': t['jeditaskid'],
                'creationdate': t['creationdate'],
                'package': gd['package'],
                'branch': branch,
                'ntag': gd['ntag'][:10],
                'nigthly_tag_date': gd['ntag'],
                'status': t['status']
            })

    return tests_unregistered


def fetch_unregistered_tests(query, extra_str, result_queue):
    """
    Fetch unregistered tests from PanDA and put them into the result queue.
    Args:
        query: query dict
        extra_str: extra string to add to the query
        result_queue: queue to put the results in
    """
    _logger.debug(f'Getting unregistered tests started')
    close_old_connections()
    try:
        tests = get_unregistered_tests(query, extra_str)
        result_queue.put(tests)
    except Exception as e:
        _logger.debug(f'Getting unregistered tests failed with exception {e}')
    finally:
        close_old_connections()
    _logger.debug(f'Getting unregistered tests finished')


def resolve_unregistered_tests(thread, result_queue, timeout: int = 5) -> list:
    """
    Waits for the getting unregistered tests background thread to complete within a timeout,
    extracts them from the queue.

    Args:
        thread: thread object
        result_queue: queue object
        timeout: timeout in seconds

    Returns:
        unregistered_tests: list of unregistered tests or empty list if the thread timed out
    """
    if thread is None:
        return []

    # join threads
    thread.join(timeout=timeout)
    if thread.is_alive():
        _logger.warning(f"Total job count query timed out after {timeout}s. Continuing without count.")
        return []

    try:
        unregistered_tests = result_queue.get_nowait()
        if unregistered_tests is not None and isinstance(unregistered_tests, list):
            _logger.info(f"Found {len(unregistered_tests)} tasks containing unregistered tests in PanDA DB")
            return unregistered_tests
        else:
            _logger.warning("Thread completed but returned None due to internal query failure.")
    except Empty:
        _logger.error("Thread completed without putting results into queue.")
    except Exception as e:
        _logger.error(f"Error reading result from thread queue: {e}", exc_info=True)

    return []


def add_unregistered_tests(test_summary, unregistered_tests, level='overview', ao=None, ntag_list=None):
    """
    Adds unregistered tests to the test summary, including injecting
    packages/branches that have 0 regular jobs and only unregistered tasks.
    :param test_summary: dict
    :param unregistered_tests: list of dicts
    :param level: str - summary level, default is 'overview', can be 'task' or 'overview'
    :param ao: list of str - order of summary fields, default is ['package', 'branch']
    :return: test_summary: dict - updated test summary
    """
    if ao is None:
        ao = ['package', 'branch']

    ntag_label = 'ntag' if level == 'overview' else 'nigthly_tag_date'

    # group unregistered tasks into a nested dictionary structure
    unregistered_tests_dict = {}
    for t in unregistered_tests:
        key_a = t.get(ao[0])
        key_b = t.get(ao[1])
        ntag_val = t.get(ntag_label) or t.get('ntag')

        if not key_a or not key_b or not ntag_val:
            continue

        if key_a not in unregistered_tests_dict:
            unregistered_tests_dict[key_a] = {}
        if key_b not in unregistered_tests_dict[key_a]:
            unregistered_tests_dict[key_a][key_b] = {}
        if ntag_val not in unregistered_tests_dict[key_a][key_b]:
            unregistered_tests_dict[key_a][key_b][ntag_val] = {
                'n_tasks': 0, 'id_list': [], 'status_list': [], 'ntag': t['ntag']
            }

        unregistered_tests_dict[key_a][key_b][ntag_val]['n_tasks'] += 1
        unregistered_tests_dict[key_a][key_b][ntag_val]['id_list'].append(t['jeditaskid'])
        unregistered_tests_dict[key_a][key_b][ntag_val]['status_list'].append(t['status'])

    # update existing entries in test_summary
    if level == 'overview':
        for a, a_dict in test_summary.items():
            for ntag, ntag_dict in a_dict.items():
                for test_type, counts in ntag_dict.items():
                    if test_type == 'grid' and a in unregistered_tests_dict:
                        tasks_summary = {'n_tasks': 0, 'id_list': [], 'status_counts': {}}
                        status_list = []
                        for c, c_dict in unregistered_tests_dict[a].items():
                            if ntag in c_dict:
                                tasks_summary['n_tasks'] += c_dict[ntag]['n_tasks']
                                tasks_summary['id_list'].extend(c_dict[ntag]['id_list'])
                                status_list.extend(c_dict[ntag]['status_list'])
                        if tasks_summary['n_tasks'] > 0:
                            tasks_summary['status_counts'] = dict(Counter(status_list))
                            test_summary[a][ntag][test_type]['tasks'] = tasks_summary
                            _logger.debug(f"Added {tasks_summary['n_tasks']} tasks containing unregistered tests to {a}/{ntag}")

        # inject entirely missing package or branch that only have unregistered tasks
        for a, a_dict in unregistered_tests_dict.items():
            if a not in test_summary:
                ntag_map = {}
                if ntag_list is None:
                    ntag_list = list(test_summary[test_summary.keys()[0]].keys())
                for ntag in ntag_list:
                    ntag_map[ntag] = {'grid': {
                        'tasks': {'n_tasks': 0, 'id_list': [], 'status_counts': {}, 'status_list': []},
                        'active': 0,
                        'finished': 0,
                        'failed': 0,
                        'succeeded': 0,
                        'ntag': ntag,
                        'test_type': 'grid'
                    }}

                for b, ntag_data in a_dict.items():
                    for ntag, info in ntag_data.items():
                        if ntag in ntag_map:
                            ntag_map[ntag]['grid']['tasks']['n_tasks'] += info['n_tasks']
                            ntag_map[ntag]['grid']['tasks']['id_list'].extend(info['id_list'])
                            ntag_map[ntag]['grid']['tasks']['status_list'].extend(info['status_list'])
                for ntag, ntag_data in ntag_map.items():
                    if ntag_data['grid']['tasks']['n_tasks'] > 0:
                        ntag_data['grid']['tasks']['status_counts'] = dict(Counter(ntag_data['grid']['tasks']['status_list']))
                        del ntag_data['grid']['tasks']['status_list']
                test_summary[a] = ntag_map
                _logger.debug(f"Injected completely missing summary entry for package/branch: {a}")

    elif level == 'task':
        for a, a_dict in test_summary.items():
            for b, b_dict in a_dict.items():
                for ntag, ntag_dict in b_dict.items():
                    for test_type, counts in ntag_dict.items():
                        for ntag_full, ntag_full_dict in counts.items():
                            if (ntag_full != 'ntag_hf' and test_type == 'grid' and a in unregistered_tests_dict
                                    and b in unregistered_tests_dict[a] and ntag_full in unregistered_tests_dict[a][b]):
                                test_summary[a][b][ntag][test_type][ntag_full]['tasks'] = {
                                    'n_tasks': unregistered_tests_dict[a][b][ntag_full]['n_tasks'],
                                    'id_list': unregistered_tests_dict[a][b][ntag_full]['id_list'],
                                    'status_counts': dict(Counter(unregistered_tests_dict[a][b][ntag_full]['status_list']))
                                }
                                _logger.debug(f"Added {unregistered_tests_dict[a][b][ntag_full]['n_tasks']} tasks to {a}/{b}/{ntag_full}")

        # inject missing combinations for 'task' level
        for a, a_dict in unregistered_tests_dict.items():
            if a not in test_summary:
                test_summary[a] = {}
            for b, ntag_data in a_dict.items():
                if b not in test_summary[a]:
                    test_summary[a][b] = {}
                    if ntag_list is None:
                        ntag_list = list(test_summary[test_summary.keys()[0]][test_summary[test_summary.keys()[0]].keys()[0]].keys())
                    for ntag in ntag_list:
                        test_summary[a][b][ntag] = {'grid': {
                            'ntag_hf': datetime.strftime(
                                datetime.strptime(ntag, art_const.DATETIME_FORMAT['default']), art_const.DATETIME_FORMAT['humanized']
                            )
                        }}

                    for ntag_full, info in ntag_data.items():
                        if info['ntag'] in test_summary[a][b]:
                            test_summary[a][b][info['ntag']]['grid'][ntag_full] = {
                                'active': 0,
                                'failed': 0,
                                'finished': 0,
                                'succeeded': 0,
                                'tasks': {
                                    'n_tasks': info['n_tasks'],
                                    'id_list': info['id_list'],
                                    'status_counts': dict(Counter(info['status_list']))
                                }
                            }
                            _logger.debug(f"Injected completely missing task-level entry for {a}/{b}/{ntag_full}")

    return test_summary


def build_detected_tasks_summary(unregistered_tests):
    """
    Build summary of detected tasks for art jobs view
    :param unregistered_tests: a list of dicts
    :return: tasks_detected_summary - dict
    """
    tasks_detected_summary = {'n_tasks': 0, 'id_list': [], 'status_summary': {}}
    n_tasks = len(unregistered_tests)
    for t in unregistered_tests:
        tasks_detected_summary['n_tasks'] += 1
        if n_tasks < 100:
            tasks_detected_summary['id_list'].append(t['jeditaskid'])
        elif t['status'] not in ('done', 'finished') and len(tasks_detected_summary['id_list']) < 100:
            tasks_detected_summary['id_list'].append(t['jeditaskid'])
        if t['status'] not in tasks_detected_summary['status_summary']:
            tasks_detected_summary['status_summary'][t['status']] = 0
        tasks_detected_summary['status_summary'][t['status']] = tasks_detected_summary['status_summary'].get(t['status'], 0) + 1
    return tasks_detected_summary
