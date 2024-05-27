"""
    art.views
"""
import logging
import json
import re
import time
import multiprocessing
from datetime import datetime, timedelta

from django.utils import timezone
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils.cache import patch_response_headers
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.cache import never_cache
from django.template.defaulttags import register
from django.db.models.functions import Concat, Substr
from django.db.models import Value as V, F, Max

from core.oauth.utils import login_customrequired
from core.utils import is_json_request, complete_request, removeParam
from core.views import initRequest, extensibleURL
from core.reports.sendMail import send_mail_bp
from core.art.modelsART import ARTTests, ARTResultsQueue, ARTSubResult
from core.art.jobSubResults import subresults_getter, save_subresults, lock_nqueuedjobs, delete_queuedjobs, clear_queue, \
    get_final_result, update_test_status
from core.reports.models import ReportEmails
from core.libs.DateEncoder import DateEncoder
from core.libs.datetimestrings import parse_datetime
from core.libs.job import get_job_list, get_job_walltime
from core.libs.error import get_job_errors
from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import convert_sec, convert_bytes, round_to_n_digits
from core.pandajob.models import CombinedWaitActDefArch4, Jobsarchived
from core.common.models import Filestable4

from core.art.utils import setupView, get_test_diff, get_result_for_multijob_test, concat_branch, \
    find_last_successful_test, build_gitlab_link, clean_tests_list

from django.conf import settings
import core.art.constants as art_const

_logger = logging.getLogger('bigpandamon-art')

@register.filter(takes_context=True)
def remove_dot(value):
    return value.replace(".", "").replace('/','')

@register.filter(takes_context=True)
def get_time(value):
    return value[-5:]


@login_customrequired
def art(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # Here we try to get cached data
    data = getCacheEntry(request, "artMain")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artMainPage.html', data, content_type='text/html')
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    tquery = {'platform__endswith': 'opt'}

    # limit results by N days
    if 'days' in request.session['requestParams']:
        n_days_limit = int(request.session['requestParams']['days'])
    else:
        n_days_limit = 90
    tquery['created__castdate__range'] = [timezone.now() - timedelta(days=n_days_limit), timezone.now()]

    packages = ARTTests.objects.filter(**tquery).values('package').distinct().order_by('package')
    branches = ARTTests.objects.filter(**tquery).values('nightly_release_short', 'platform','project').annotate(branch=Concat('nightly_release_short', V('/'), 'project', V('/'), 'platform')).values('branch').distinct().order_by('-branch')
    ntags = ARTTests.objects.values('nightly_tag_display').annotate(nightly_tag_date=Substr('nightly_tag_display', 1, 10)).values('nightly_tag_date').distinct().order_by('-nightly_tag_date')[:5]

    if not is_json_request(request):
        # a workaround for the DF split into a lot of separate packages
        df_name = 'DerivationFramework'
        package_list = list(set([p['package'] if not p['package'].startswith(df_name) else df_name + '*' for p in packages]))

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'packages': sorted(package_list, key=str.lower),
            'branches': [b['branch'] for b in branches],
            'ntags': [t['nightly_tag_date'] for t in ntags]
        }
        response = render(request, 'artMainPage.html', data, content_type='text/html')
        setCacheEntry(request, "artMain", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    else:
        data = {
            'packages': [p['package'] for p in packages],
            'branches': [b['branch'] for b in branches],
        }
        response = JsonResponse(data)
    return response


@login_customrequired
def artOverview(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)
    
    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        ao = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        ao = ['branch', 'package']
    else:
        return HttpResponse(status=401)

    # Here we try to get cached data
    data = getCacheEntry(request, "artOverview")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artOverview.html', data, content_type='text/html')
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    tests = []
    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    
    # process URL params to query params
    query, extra_str = setupView(request)
    # getting tests
    values = ('package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag', 'attemptnr', 'status',
              'pandaid', 'jeditaskid', 'testname', 'inputfileid', 'nightly_tag_date')
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values))
    # filter out previous attempts, concat branch etc
    jobs = clean_tests_list(tests)

    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))
    _logger.info("Got ART tests: {}".format(time.time() - request.session['req_init_time']))

    # temporary art_jobs_dict for agg by both package and branch.
    # This is needed for multi job test cases when we need to count them as one, choosing the worst result across them.
    art_jobs_dict = {}
    # dict for final overview result
    art_summary_dict = {}
    for j in jobs:
        if j[ao[0]] not in art_summary_dict:
            art_jobs_dict[j[ao[0]]] = {}
            art_summary_dict[j[ao[0]]] = {}
            for n in ntagslist:
                art_summary_dict[j[ao[0]]][n.strftime(art_const.DATETIME_FORMAT['default'])] = {}
                art_summary_dict[j[ao[0]]][n.strftime(art_const.DATETIME_FORMAT['default'])]['ntag'] = n.strftime(
                    art_const.DATETIME_FORMAT['default']
                )
                for state in art_const.TEST_STATUS:
                    art_summary_dict[j[ao[0]]][n.strftime(art_const.DATETIME_FORMAT['default'])][state] = 0

        if j[ao[1]] not in art_jobs_dict[j[ao[0]]]:
            art_jobs_dict[j[ao[0]]][j[ao[1]]] = {}
        if j['ntag'] not in art_jobs_dict[j[ao[0]]][j[ao[1]]]:
            art_jobs_dict[j[ao[0]]][j[ao[1]]][j['ntag']] = {}
        if j['testname'] not in art_jobs_dict[j[ao[0]]][j[ao[1]]][j['ntag']]:
            art_jobs_dict[j[ao[0]]][j[ao[1]]][j['ntag']][j['testname']] = []

        if 'status' in j and isinstance(j['status'], int):
            finalresult = final_result_dict[j['status']]
        else:
            finalresult, _ = get_final_result(j)
        art_jobs_dict[j[ao[0]]][j[ao[1]]][j['ntag']][j['testname']].append(finalresult)

    # for multi job test, choose the worst result across them
    for ao0, ao0_dict in art_jobs_dict.items():
        for ao1, ao1_dict in ao0_dict.items():
            for ntag, tests in ao1_dict.items():
                for test, job_states in tests.items():
                    if len(job_states) > 0:
                        art_summary_dict[ao0][ntag.strftime(art_const.DATETIME_FORMAT['default'])][get_result_for_multijob_test(job_states)] += 1
    _logger.info("Prepared summary data dict: {}".format(time.time() - request.session['req_init_time']))

    if is_json_request(request):
        data = {
            'artpackages': art_summary_dict,
        }
        # per nightly tag summary for buildmonitor globalview
        if 'extra' in request.session['requestParams'] and 'per_nightly_tag' in request.session['requestParams']['extra']:
            art_overview_per_nightly_tag = {}
            for j in jobs:
                if j['nightly_tag'] not in art_overview_per_nightly_tag:
                    art_overview_per_nightly_tag[j['nightly_tag']] = {}
                if j[ao[0]] not in art_overview_per_nightly_tag[j['nightly_tag']]:
                    art_overview_per_nightly_tag[j['nightly_tag']][j[ao[0]]] = {}
                    for state in art_const.TEST_STATUS:
                        art_overview_per_nightly_tag[j['nightly_tag']][j[ao[0]]][state] = 0
                if 'status' in j and isinstance(j['status'], int):
                    finalresult = final_result_dict[j['status']]
                else:
                    finalresult, _ = get_final_result(j)
                art_overview_per_nightly_tag[j['nightly_tag']][j[ao[0]]][finalresult] += 1
            data['art_overview_per_nightly_tag'] = art_overview_per_nightly_tag

        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # dict -> list for datatable
        art_overview = [[ao[0]] + [n.strftime(art_const.DATETIME_FORMAT['humanized']) for n in ntagslist] ]
        art_overview.extend([[k,]+[j for i, j in v.items()] for k, v in art_summary_dict.items()])
        xurl = extensibleURL(request)
        noviewurl = removeParam(xurl, 'view', mode='extensible')

        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'artpackages': art_summary_dict,
            'noviewurl': noviewurl,
            'ntaglist': [ntag.strftime(art_const.DATETIME_FORMAT['default']) for ntag in ntagslist],
            'artoverview': art_overview,
        }
        setCacheEntry(request, "artOverview", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artOverview.html', data, content_type='text/html')
        _logger.info("Template rendered: {}".format(time.time() - request.session['req_init_time']))
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def artTasks(request):
    valid, response = initRequest(request)
    if not valid:
        return HttpResponse(status=401)

    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        ao = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        ao = ['branch', 'package']
    else:
        return HttpResponse(status=401)

    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}

    # Here we try to get cached data
    data = getCacheEntry(request, "artTasks")
    # data = None
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artTasks.html', data, content_type='text/html')
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    tests = []
    # process URL params to query params
    query, extra_str = setupView(request)
    # getting tests
    values = ('package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag', 'attemptnr', 'status',
              'pandaid', 'jeditaskid', 'testname', 'inputfileid', 'nightly_tag_date')
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values))
    # filter out previous attempts, add branch etc
    jobs = clean_tests_list(tests)

    ntagslist = list(sorted(set([x['ntag'] for x in jobs])))
    _logger.info("Got ART tests: {}".format(time.time() - request.session['req_init_time']))

    art_jobs_dict = {}
    arttasksdict = {}
    for job in jobs:
        if job[ao[0]] not in art_jobs_dict:
            art_jobs_dict[job[ao[0]]] = {}

            arttasksdict[job[ao[0]]] = {}
        if job[ao[1]] not in art_jobs_dict[job[ao[0]]]:
            art_jobs_dict[job[ao[0]]][job[ao[1]]] = {}

            arttasksdict[job[ao[0]]][job[ao[1]]] = {}
            for n in ntagslist:
                arttasksdict[job[ao[0]]][job[ao[1]]][n.strftime(art_const.DATETIME_FORMAT['default'])] = {}
                arttasksdict[job[ao[0]]][job[ao[1]]][n.strftime(art_const.DATETIME_FORMAT['default'])]['ntag_hf'] = n.strftime(art_const.DATETIME_FORMAT['humanized'])
        if job['nightly_tag'] not in art_jobs_dict[job[ao[0]]][job[ao[1]]]:
            art_jobs_dict[job[ao[0]]][job[ao[1]]][job['nightly_tag']] = {}

            arttasksdict[job[ao[0]]][job[ao[1]]][job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])][job['nightly_tag']] = {}
            for state in art_const.TEST_STATUS:
                arttasksdict[job[ao[0]]][job[ao[1]]][job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])][job['nightly_tag']][state] = 0
        if job['testname'] not in art_jobs_dict[job[ao[0]]][job[ao[1]]][job['nightly_tag']]:
            art_jobs_dict[job[ao[0]]][job[ao[1]]][job['nightly_tag']][job['testname']] = []

        if 'status' in job and isinstance(job['status'], int):
            finalresult = final_result_dict[job['status']]
        else:
            finalresult, _ = get_final_result(job)

        art_jobs_dict[job[ao[0]]][job[ao[1]]][job['nightly_tag']][job['testname']].append(finalresult)

    for ao0, ao0_dict in art_jobs_dict.items():
        for ao1, ao1_dict in ao0_dict.items():
            for ntag, tests in ao1_dict.items():
                for test, job_states in tests.items():
                    if len(job_states) > 0:
                        arttasksdict[ao0][ao1][ntag[:-5]][ntag][get_result_for_multijob_test(job_states)] += 1
    _logger.info("Prepared summary: {}".format(time.time() - request.session['req_init_time']))

    if is_json_request(request):
        data = {
            'arttasks': arttasksdict,
        }
        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        xurl = extensibleURL(request)
        noviewurl = removeParam(xurl, 'view', mode='extensible')
        # convert dict to list for datatable
        art_tasks = [[ao[0], ao[1]] + [n.strftime(art_const.DATETIME_FORMAT['humanized']) for n in ntagslist] ]
        for ao0, ao0_dict in arttasksdict.items():
            for ao1, ao1_dict in ao0_dict.items():
                tmp_nightlies = []
                for n, v in ao1_dict.items():
                    tmp = []
                    for s, results in v.items():
                        if s != 'ntag_hf':
                            res_dict = results
                            res_dict['ntag_time'] = s[-5:]
                            res_dict['ntag_full'] = s
                            tmp.append(res_dict)
                    tmp_nightlies.append(tmp)
                art_tasks.append([ao0, ao1] + tmp_nightlies)

        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'arttasks': art_tasks,
            'noviewurl': noviewurl,
            'ntaglist': [ntag.strftime(art_const.DATETIME_FORMAT['default']) for ntag in ntagslist],
        }
        setCacheEntry(request, "artTasks", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artTasks.html', data, content_type='text/html')
        _logger.info("Rendered template: {}".format(time.time() - request.session['req_init_time']))
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def artJobs(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    # getting aggregation order and view
    art_view = 'package'
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        ao = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        ao = ['branch', 'package']
        art_view = 'branch'
    else:
        return HttpResponse(status=401)

    # show subresults or not
    if 'ntag' in request.session['requestParams'] or 'ntag_full' in request.session['requestParams'] or (
            'extra' in request.session['requestParams'] and request.session['requestParams']['extra'] == 'subresults'):
        request.session['viewParams']['subresults'] = 1
    else:
        request.session['viewParams']['subresults'] = 0

    # add PanDA jobs metrics or not
    if 'ntag' in request.session['requestParams'] or 'ntag_full' in request.session['requestParams']:
        request.session['viewParams']['metrics'] = 1
    else:
        request.session['viewParams']['metrics'] = 0

    # Here we try to get cached data
    data = getCacheEntry(request, "artJobs")
    # data = None
    if data is not None:
        _logger.info('Got data from cache: {}s'.format(time.time() - request.session['req_init_time']))
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artJobs.html', data, content_type='text/html')
        _logger.info('Rendered template with data from cache: {}s'.format(time.time()-request.session['req_init_time']))
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    # process URL params to query params
    query, extra_str = setupView(request)
    _logger.info('Set up view: {}s'.format(time.time() - request.session['req_init_time']))

    # getting tests
    tests = []
    values = [f.name for f in ARTTests._meta.get_fields() if not f.is_relation]
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values, result=F('artsubresult__subresult')))

    # filter out previous attempts, add branch etc
    art_jobs = clean_tests_list(tests, add_link_previous_attempt=True)
    ntagslist = list(sorted(set([x['ntag'] for x in art_jobs])))
    jeditaskids = set([x['jeditaskid'] for x in art_jobs])
    _logger.info('Got {} tests: {}s'.format(len(tests), time.time() - request.session['req_init_time']))

    # get PanDA job metrics
    panda_jobs_dict = {}
    if request.session['viewParams']['metrics'] == 1:
        panda_jobs = get_job_list(
            {'jeditaskid__in': jeditaskids},
            values=('pandaid', 'jobstatus', 'maxrss', 'endtime', 'starttime', 'cpuconsumptiontime', 'cpuconsumptionunit')
        )
        panda_jobs_dict = {j['pandaid']: j for j in panda_jobs}
        _logger.info('Got PanDA jobs: {}s'.format(time.time() - request.session['req_init_time']))

    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    reportTo = {'mail': [], 'jira': {}}
    gitlabids = list(sorted(set([x['gitlabid'] for x in art_jobs if 'gitlabid' in x and x['gitlabid'] is not None])))
    linktoplots = []
    eos_art_link = 'https://atlas-art-data.web.cern.ch/atlas-art-data/'
    link_prefix = 'https://atlas-art-data.web.cern.ch/atlas-art-data/grid-output/'
    artjobsdict={}

    for job in art_jobs:
        if job[ao[0]] not in artjobsdict:
            artjobsdict[job[ao[0]]] = {}
        if job[ao[1]] not in artjobsdict[job[ao[0]]]:
            artjobsdict[job[ao[0]]][job[ao[1]]] = {}

        if job['testname'] not in artjobsdict[job[ao[0]]][job[ao[1]]]:
            artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']] = {}
            for n in ntagslist:
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])] = {}
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])]['ntag_hf'] = n.strftime(art_const.DATETIME_FORMAT['humanized'])
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])]['jobs'] = []

        if job['ntag'].strftime(art_const.DATETIME_FORMAT['default']) in artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']]:
            jobdict = {
                'origpandaid': job['pandaid'],
                'ntag': job['nightly_tag'],
                'ntagtime': job['nightly_tag'][-5:],
                'computingsite': job['computingsite'],
                'jeditaskid': job['jeditaskid'],
                'attemptnr': job['attemptnr'],
                'maxattempt': job['maxattempt'],
                'inputfileid': job['inputfileid'],
                'tarindex': job['tarindex'],
                'finalresult': final_result_dict[job['status']] if job['status'] in final_result_dict else '---',
                'gitlab': build_gitlab_link(job['package'], job['testname']),
                'linktopreviousattemptlogs': job['linktopreviousattemptlogs'] if 'linktopreviousattemptlogs' in job else ''
            }
            # enrich with PanDA job metrics
            if job['pandaid'] in panda_jobs_dict:
                if 'maxrss' in panda_jobs_dict[job['pandaid']] and panda_jobs_dict[job['pandaid']]['maxrss'] is not None:
                    jobdict['maxrss'] = round(panda_jobs_dict[job['pandaid']]['maxrss'] * 1.0 / 1000, 1)
                else:
                    jobdict['maxrss'] = '---'
                if 'cpuconsumptiontime' in panda_jobs_dict[job['pandaid']] and panda_jobs_dict[job['pandaid']]['cpuconsumptiontime'] is not None:
                    jobdict['cpuconsumptiontime'] = panda_jobs_dict[job['pandaid']]['cpuconsumptiontime']
                else:
                    jobdict['cpuconsumptiontime'] = '---'
                if 'cpuconsumptionunit' in panda_jobs_dict[job['pandaid']] and panda_jobs_dict[job['pandaid']]['cpuconsumptionunit'] is not None:
                    jobdict['cpuconsumptionunit'] = panda_jobs_dict[job['pandaid']]['cpuconsumptionunit']
                else:
                    jobdict['cpuconsumptionunit'] = '---'

                jobdict['duration'] = convert_sec(get_job_walltime(panda_jobs_dict[job['pandaid']]), out_unit='str')
            else:
                jobdict['maxrss'] = '---'
                jobdict['cpuconsumptiontime'] = '---'
                jobdict['cpuconsumptionunit'] = '---'
                jobdict['duration'] = '---'

            # ATLINFR-3305
            if 'extrainfo' in job:
                try:
                    job['extrainfo'] = json.loads(job['extrainfo'])
                except:
                    job['extrainfo'] = {}

            jobdict['linktext'] = '{}/{}/{}/{}/'.format(job[ao[1]], job['nightly_tag'], job['package'], job['testname'][:-3])
            jobdict['eoslink'] = link_prefix + jobdict['linktext']
            if 'html' in job['extrainfo'] and job['extrainfo']['html']:
                if job['extrainfo']['html'].startswith('http'):
                    jobdict['htmllink'] = job['extrainfo']['html'] + jobdict['linktext']
                    # replace eoslink
                    # TODO: temporary dirty fix until ART begins to send a proper eos path
                    if not job['extrainfo']['html'].startswith(eos_art_link):
                        jobdict['eoslink'] = '/'.join(job['extrainfo']['html'].split('/', 4)[:4]) + '/art/' + jobdict['linktext']
                else:
                    jobdict['htmllink'] = link_prefix + jobdict['linktext'] + job['extrainfo']['html'] + '/'

            finalresult, extraparams = get_final_result(job)
            if 'status' in job and isinstance(job['status'], int):
                finalresult = final_result_dict[job['status']]
            jobdict['finalresult'] = finalresult
            jobdict.update(extraparams)

            artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])]['jobs'].append(jobdict)

            if jobdict['reportjira'] is not None:
                for jira, link in jobdict['reportjira'].items():
                    if jira not in reportTo['jira'].keys():
                        reportTo['jira'][jira] = link
            if jobdict['reportmail'] is not None and jobdict['reportmail'] not in reportTo['mail']:
                reportTo['mail'].append(jobdict['reportmail'])

            if 'linktoplots' in extraparams and extraparams['linktoplots'] is not None:
                linktoplots.append(extraparams['linktoplots'])


    _logger.info('Prepared data: {}s'.format(time.time() - request.session['req_init_time']))

    if is_json_request(request):

        data = {
            'artjobs': artjobsdict,
        }

        dump = json.dumps(data, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')
    else:
        # transform to list for datatable
        extra_metrics_columns = {
            'computingsite': 'Site',
            'duration': 'Duration, d:h:m:s',
            'cpuconsumptiontime': 'CPU time, s',
            'maxrss': 'Max RSS, MB',
            'cpuconsumptionunit': 'CPU type',
        }
        art_jobs = [[ao[0], ao[1], 'test name'] + [n.strftime(art_const.DATETIME_FORMAT['humanized']) for n in ntagslist]]
        if len(ntagslist) == 1:
            art_jobs[0] += [m_title for  m, m_title in extra_metrics_columns.items()]
        for ao0, ao0_dict in artjobsdict.items():
            for ao1, ao1_dict in ao0_dict.items():
                for t, t_dict in ao1_dict.items():
                    tmp_list = [ao0, ao1, t,]
                    for ntag, jobs in t_dict.items():
                        tmp_list.append([
                            {
                                k: v for k, v in j.items() if v is not None
                            } for j in sorted(jobs['jobs'], key=lambda x: (x['ntagtime'], x['origpandaid']), reverse=True)
                        ])
                        if len(t_dict.keys()) == 1:
                            for m, m_title in extra_metrics_columns.items():
                                tmp_list.append([
                                    j[m] for j in sorted(jobs['jobs'], key=lambda x: (x['ntagtime'], x['origpandaid']), reverse=True)
                                ])
                    art_jobs.append(tmp_list)

        _logger.info('Converted data from dict to list: {}s'.format(time.time() - request.session['req_init_time']))

        linktoplots = set(linktoplots)
        xurl = extensibleURL(request)
        noviewurl = removeParam(xurl, 'view', mode='extensible')

        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'artview': art_view,
            'noviewurl': noviewurl,
            'ntaglist': [ntag.strftime(art_const.DATETIME_FORMAT['default']) for ntag in ntagslist],
            'gitlabids': gitlabids,
            'reportto': reportTo,
            'linktoplots': linktoplots,
            'art_jobs': art_jobs,
        }
        setCacheEntry(request, "artJobs", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artJobs.html', data, content_type='text/html')
        _logger.info('Rendered template: {}s'.format(time.time() - request.session['req_init_time']))
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def artTest(request, package=None, testname=None):
    """
    Single test page
    :param request: request
    :param package: str, name of ART package
    :param testname: str, ART test name
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    error = {'message': ''}
    if not package:
        if 'package' in request.session['requestParams']:
            package = request.session['requestParams']['package']
        else:
            error['message'] += 'No package provided. '
    else:
        request.session['requestParams']['package'] = package

    if not testname:
        if 'testname' in request.session['requestParams']:
            testname = request.session['requestParams']['testname']
        else:
            error['message'] += 'No test name provided. '
    else:
        request.session['requestParams']['testname'] = testname

    art_test = {}
    art_branches = {}
    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    if len(error['message']) == 0:
        # process URL params to query params
        query, extra_str = setupView(request)
        _logger.info('Set up view: {}s'.format(time.time() - request.session['req_init_time']))

        # get test
        art_jobs = []
        values = [f.name for f in ARTTests._meta.get_fields() if not f.is_relation]
        art_jobs.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values, result=F('artsubresult__subresult')))

        if len(art_jobs) > 0:
            # get info for PanDA jobs
            jquery = {
                'pandaid__in': [j['pandaid'] for j in art_jobs],
            }
            panda_jobs = get_job_list(jquery, values=['cpuconsumptionunit',], error_info=True)
            panda_jobs = {j['pandaid']: j for j in panda_jobs}
            # combine info from ART and PanDA
            for job in art_jobs:
                pid = job['pandaid']
                job['eos'] = '{}{}/{}/{}/{}/'.format(
                    art_const.EOS_PREFIX, concat_branch(job), job['nightly_tag'], job['package'], job['testname'][:-3])
                if len(job['extrainfo']) > 0:
                    try:
                        extrainfo_json = json.loads(job['extrainfo'])
                        job.update(extrainfo_json)
                    except:
                        pass
                if 'html' in job and job['html']:
                    if job['html'].startswith('http'):
                        job['htmllink'] = '{}{}/{}/{}/{}'.format(
                            job['html'], concat_branch(job), job['nightly_tag'], job['package'], job['testname'][:-3])
                    else:
                        job['htmllink'] = '{}/{}/'.format('eos',job['html'])

                if pid in panda_jobs:
                    job['jobstatus'] = panda_jobs[pid]['jobstatus']
                    job['attemptnr'] = panda_jobs[pid]['attemptnr']
                    job['computingsite'] = panda_jobs[pid]['computingsite']
                    job['cpuconsumptiontime'] = panda_jobs[pid]['cpuconsumptiontime']
                    job['cputype'] = panda_jobs[pid]['cpuconsumptionunit']
                    if 'actualcorecount' in panda_jobs[pid] and panda_jobs[pid]['actualcorecount'] is not None \
                            and panda_jobs[pid]['actualcorecount'] > 0 and isinstance(panda_jobs[pid]['maxpss'], int):
                        job['maxpss_per_core_gb'] = round_to_n_digits(1.0*convert_bytes(
                                1000*panda_jobs[pid]['maxpss'], output_unit='GB')/panda_jobs[pid]['actualcorecount'],
                            n=2, method='ceil'
                        )
                    job['duration_str'] = convert_sec(get_job_walltime(panda_jobs[pid]), out_unit='str')
                    job['errorinfo'] = panda_jobs[pid]['errorinfo']

                    finalresult, extraparams = get_final_result(job)
                    if 'status' in job and isinstance(job['status'], int):
                        finalresult = final_result_dict[job['status']]
                    job['finalresult'] = finalresult
                    job.update(extraparams)

            # prepare data for template
            art_test['testname'] =  testname
            art_test['package'] = package
            descriptions = list(set([j['description'] for j in art_jobs if 'description' in j and j['description']]))
            art_test['description'] = descriptions[0] if len(descriptions) > 0 else ''

            for job in art_jobs:
                branch = concat_branch(job)
                if branch not in art_branches:
                    art_branches[branch] = {'jobs': []}
                if 'gitlab' not in art_test:
                    art_test['gitlab'] = build_gitlab_link(job['package'], testname)
                art_branches[branch]['jobs'].append(job)

            # if failed test, find last successful one
            if 'ntag' not in request.session['requestParams'] and 'ntag_full' not in request.session['requestParams']:
                for b, data in art_branches.items():
                    if 'succeeded' not in set([j['finalresult'] for j in data['jobs']]):
                        art_branches[b]['lst'] = find_last_successful_test(testname, b)

    if is_json_request(request):
        data = {
            'art_test': art_test,
            'error': error,
        }
        return JsonResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        art_branches = sorted(
            [[
                {'branch': b, 'lst': data['lst'] if 'lst' in data else None},
                sorted(data['jobs'], key=lambda x:x['pandaid'])
            ] for b, data in art_branches.items()],
            key=lambda x: x[0]['branch'],
            reverse=True)
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'error': error,
            'test': art_test,
            'branches': art_branches,
        }
        setCacheEntry(request, "artTest", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artTest.html', data, content_type='text/html')
        _logger.info('Rendered template: {}s'.format(time.time() - request.session['req_init_time']))
        request = complete_request(request)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def artStability(request):
    """
    A view to summarize changes of a test results over last week
    :param request: HTTP request
    :return:
    """
    starttime = datetime.now()
    valid, response = initRequest(request)
    if not valid:
        return response

    # getting aggregation order
    if not 'view' in request.session['requestParams'] or (
            'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'packages'):
        ao = ['package', 'branch']
    elif 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'branches':
        ao = ['branch', 'package']
    else:
        return HttpResponse(status=401)

    # Here we try to get cached data
    data = getCacheEntry(request, "artStability")
    # data = None
    if data is not None:
        _logger.info('Got data from cache: {}s'.format(time.time() - request.session['req_init_time']))
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artStability.html', data, content_type='text/html')
        _logger.info('Rendered template with data from cache: {}s'.format(time.time()-request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    # process URL params to query params
    query, extra_str = setupView(request)
    _logger.info('Set up view: {}s'.format(time.time() - request.session['req_init_time']))

    # getting tests
    tests = []
    values = (
        'package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag_date', 'nightly_tag', 'testname',
        'pandaid', 'jeditaskid', 'inputfileid', 'status'
    )
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values, result=F('artsubresult__subresult')))

    # filter out previous attempts, add branch etc
    art_jobs = clean_tests_list(tests, add_link_previous_attempt=True)
    ntagslist = list(sorted(set([x['ntag'] for x in art_jobs])))
    _logger.info('Got {} tests: {}s'.format(len(tests), time.time() - request.session['req_init_time']))

    artjobsdict = {}
    for job in art_jobs:
        if job[ao[0]] not in artjobsdict.keys():
            artjobsdict[job[ao[0]]] = {}
        if job[ao[1]] not in artjobsdict[job[ao[0]]].keys():
            artjobsdict[job[ao[0]]][job[ao[1]]] = {}

        if job['testname'] not in artjobsdict[job[ao[0]]][job[ao[1]]].keys():
            artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']] = {}
            for n in ntagslist:
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])] = {}
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])]['ntag_hf'] = n.strftime(art_const.DATETIME_FORMAT['humanized'])
                artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][n.strftime(art_const.DATETIME_FORMAT['default'])]['jobs'] = []
        if job['ntag'].strftime(art_const.DATETIME_FORMAT['default']) in artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']]:
            jobdict = {}
            finalresult, extraparams = get_final_result(job)
            if 'status' in job and isinstance(job['status'], int):
                finalresult = final_result_dict[job['status']]
            jobdict['finalresult'] = finalresult
            jobdict.update(extraparams)
            artjobsdict[job[ao[0]]][job[ao[1]]][job['testname']][job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])]['jobs'].append(jobdict)

    art_jobs_diff_header = [ao[1], 'testname', ]
    art_jobs_diff_header.extend([n.strftime(art_const.DATETIME_FORMAT['humanized_short']) for n in ntagslist][1:])

    # finding a diff
    art_jobs_diff = {}
    # filter out stable tests
    ntags = [n.strftime(art_const.DATETIME_FORMAT['default']) for n in ntagslist]
    for i, i_dict in artjobsdict.items():
        if i not in art_jobs_diff:
            art_jobs_diff[i] = []
            art_jobs_diff[i].append(art_jobs_diff_header)
        for j, j_dict in i_dict.items():
            for t, t_dict in j_dict.items():
                tmp_row = [
                    j,
                    t,
                ]
                for ntag_i in range(0, len(ntags)-1):
                    if len(t_dict[ntags[ntag_i+1]]['jobs']) > 0:
                        # find existing previous one
                        ntag_prev = -1
                        for ntag_j in range(0, ntag_i+1):
                            if len(t_dict[ntags[ntag_i-ntag_j]]['jobs']) > 0:
                                ntag_prev = ntag_i-ntag_j
                                break
                        if ntag_prev >= 0:
                            # compare one test
                            tmp_row.append(get_test_diff(t_dict[ntags[ntag_prev]]['jobs'][0], t_dict[ntags[ntag_i+1]]['jobs'][0]))
                        else:
                            tmp_row.append('na')
                    else:
                        tmp_row.append('-')

                # for ntag, t_jobs in t_dict.items():
                #     tmp_row[ntag] = '_'.join(set([r['finalresult'] for r in t_jobs['jobs']]))
                # if len(set(['_'.join(r['finalresult'] for r in t_jobs['jobs']) for ntag, t_jobs in t_dict.items() if len(t_jobs['jobs'])])) > 1:
                #     tmp_row['diff'] = '+'
                # else:
                #     tmp_row['diff'] = '-'
                art_jobs_diff[i].append(tmp_row)
    _logger.info('Prepared tests diff: {}s'.format(len(tests), time.time() - request.session['req_init_time']))

    # response
    if is_json_request(request):
        data = {
            'art': art_jobs_diff,
        }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        xurl = extensibleURL(request)
        noviewurl = removeParam(xurl, 'view', mode='extensible')
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'ntags': ntags[1:],
            'noviewurl': noviewurl,
            'artaggrorder': ao,
            'artjobsdiff': art_jobs_diff,
        }
        setCacheEntry(request, "artStability", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artStability.html', data, content_type='text/html')
        _logger.info('Rendered template: {}s'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


@login_customrequired
def artErrors(request):
    """
    A view to summarize changes of a test results over last week
    :param request: HTTP request
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    # Here we try to get cached data
    data = getCacheEntry(request, "artErrors")
    # data = None
    if data is not None:
        _logger.info('Got data from cache: {}s'.format(time.time() - request.session['req_init_time']))
        data = json.loads(data)
        data['request'] = request
        response = render(request, 'artErrors.html', data, content_type='text/html')
        _logger.info('Rendered template with data from cache: {}s'.format(time.time()-request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    # process URL params to query params
    query, extra_str = setupView(request)
    _logger.info('Set up view: {}s'.format(time.time() - request.session['req_init_time']))

    # getting tests
    tests = []
    values = (
        'package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag_date', 'nightly_tag', 'testname',
        'pandaid', 'jeditaskid', 'inputfileid', 'status'
    )
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values, result=F('artsubresult__subresult')))

    # filter out previous attempts, add branch etc
    art_jobs = clean_tests_list(tests, add_link_previous_attempt=True)
    ntagslist = list(sorted(set([x['ntag'] for x in art_jobs])))
    _logger.info('Got {} tests: {}s'.format(len(tests), time.time() - request.session['req_init_time']))


    artjobsdict = {}
    artjoberrors = []
    artjoberrors_header = [
        {'title': 'Package', 'param': 'package'},
        {'title': 'Branch', 'param': 'branch'},
        {'title': 'Test name', 'param': 'testname'},
        {'title': 'Ntag', 'param': 'ntag_str'},
        {'title': 'Test result', 'param': 'finalresult'},
        {'title': 'Sub-step results', 'param': 'subresults_str'},
        {'title': 'PanDA job errors', 'param': 'pandaerror_str'},
    ]

    # get PanDA job error info
    pandaids = [j['pandaid'] for j in art_jobs]
    error_desc_dict = get_job_errors(pandaids)
    _logger.info('Got PanDA job errors: {}s'.format(time.time() - request.session['req_init_time']))

    for job in art_jobs:
        job['ntag_str'] = job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])
        if 'status' in job and isinstance(job['status'], int):
            finalresult = final_result_dict[job['status']]
            _, extraparams = get_final_result(job)
        else:
            finalresult, extraparams = get_final_result(job)
        job['finalresult'] = finalresult
        job.update(extraparams)

        if job['subresults'] is not None and len(job['subresults']) > 0:
            job['subresults_str'] = ', '.join(['{}:<b>{}</b>'.format(s['name'], s['result']) for s in job['subresults']])
        else:
            job['subresults_str'] = '-'

        if job['pandaid'] in error_desc_dict:
            job['pandaerror_str'] = error_desc_dict[job['pandaid']]
        else:
            job['pandaerror_str'] = '-'

        if job['finalresult'] not in ('succeeded', 'active'):
            row = [job[col['param']] for col in artjoberrors_header]
            artjoberrors.append(row)

    if len(artjoberrors) > 0:
        artjoberrors.insert(0, [col['title'] for col in artjoberrors_header])

    # response
    if is_json_request(request):
        data = {
            'art': artjoberrors,
        }
        return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    else:
        xurl = extensibleURL(request)
        noviewurl = removeParam(xurl, 'view', mode='extensible')
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'built': datetime.now().strftime("%H:%M:%S"),
            'noviewurl': noviewurl,
            'artjoberrors': artjoberrors,
        }
        setCacheEntry(request, "artErrors", json.dumps(data, cls=DateEncoder), art_const.CACHE_TIMEOUT_MINUTES)
        response = render(request, 'artErrors.html', data, content_type='text/html')
        _logger.info('Rendered template: {}s'.format(time.time() - request.session['req_init_time']))
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response


def updateARTJobList(request):
    """
    Loading sub-step results for tests from PanDA job log files managed by Rucio
    :param request: HTTP request
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'days' in request.session['requestParams'] and request.session['requestParams']['days']:
        n_days = request.session['requestParams']['days']
    else:
        n_days = 7

    starttime = datetime.now()

    # Adding to ART_RESULTS_QUEUE jobs with not loaded result json yet
    cur = connection.cursor()
    cur.execute(f"""
    insert into {settings.DB_SCHEMA}.art_results_queue
    (pandaid, is_locked, lock_time)
    select at.pandaid, 0, null                     
    from {settings.DB_SCHEMA}.art_tests at, {settings.DB_SCHEMA}.combined_wait_act_def_arch4 j 
    where at.pandaid = j.pandaid
        and at.nightly_tag_date > sysdate - {n_days} 
        and at.pandaid not in (select pandaid from {settings.DB_SCHEMA}.art_subresult ar)
        and j.jobstatus in ('finished', 'failed', 'cancelled', 'closed')
        and at.pandaid not in (select pandaid from {settings.DB_SCHEMA}.art_results_queue)
    """)
    cur.close()

    data = {
        'strt': starttime,
        'endt': datetime.now()
    }
    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


def loadSubResults(request):
    """
    Loading sub-step results for tests from PanDA job log files managed by Rucio
    Get N items from queue -> call filebrowser view -> extract art json -> save to ART_SUBRESULT table
    :param request: HTTP request
    :return:
    """
    starttime = datetime.now()
    # limit to N rows to avoid timeouts
    N_CYCLES = 100
    # number of concurrent download requests to Rucio
    N_ROWS = 1

    cur = connection.cursor()
    cur.autocommit = True
    is_queue_empty = False
    n_rows_so_far = 0
    while not is_queue_empty and n_rows_so_far < N_CYCLES * N_ROWS:

        # Locking first N rows
        lock_time = lock_nqueuedjobs(cur, N_ROWS)

        # Getting locked jobs from ART_RESULTS_QUEUE
        equery = {}
        equery['lock_time'] = lock_time
        equery['is_locked'] = 1
        ids = ARTResultsQueue.objects.filter(**equery).values()

        # Loading subresults from logs
        if len(ids) > 0:

            # Loading subresults in parallel and collecting to list of dictionaries
            pool = multiprocessing.Pool(processes=N_ROWS)
            try:
                sub_results = pool.map(subresults_getter, [id['pandaid'] for id in ids])
            except:
                _logger.exception(
                    'Exception was caught while mapping pool requests responses for next pandaid(s): {}'.format(ids))
                sub_results = []
            pool.close()
            pool.join()

            # list -> dict
            subResultsDict = {}
            for sr in sub_results:
                try:
                    pandaid = int(list(sr.keys())[0])
                except:
                    _logger.exception('Exception was caught while transforming pandaid from str to int.' + str(sr))
                    pandaid = -1
                if pandaid > 0 and sr[pandaid] is not None:
                    subResultsDict[pandaid] = json.dumps(sr[pandaid])

            # insert subresults to special table
            save_subresults(subResultsDict)

            # updating test status
            for pandaid, subresults in subResultsDict.items():
                update_test_status({'pandaid':pandaid, 'result':subresults})

            # deleting processed jobs from queue
            delete_queuedjobs(cur, lock_time)
        else:
            is_queue_empty = True

        n_rows_so_far += N_ROWS

    # clear queue in case there are locked jobs of previously crashed requests
    clear_queue(cur)
    cur.close()

    count = ARTResultsQueue.objects.count()
    if not isinstance(count, int):
        count = 0
    data = {
        'strt': starttime,
        'endt': datetime.now(),
        'queue_len': count,
    }
    _logger.info('ART sub-step results queue: {} rows done, rest: {}'.format(n_rows_so_far, count))

    return HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')


@csrf_exempt
def registerARTTest(request):
    """
    API to register ART tests
    Example of curl command:
    curl -X POST -d "pandaid=XXX" -d "testname=test_XXXXX.sh" http://bigpanda.cern.ch/art/registerarttest/?json
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    pandaid = -1
    jeditaskid = -1
    testname = ''
    nightly_release_short = None
    platform = None
    project = None
    package = None
    nightly_tag = None
    nightly_tag_display = None
    extra_info = {}
    computingsite = None
    attemptnr = None
    tarindex = None
    inputfileid = None
    gitlabid = None

    # log all the req params for debug
    _logger.debug('[ART] registerARTtest requestParams: ' + str(request.session['requestParams']))

    ### Checking whether params were provided
    if 'requestParams' in request.session and 'pandaid' in request.session['requestParams'] and 'testname' in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
        testname = request.session['requestParams']['testname']
    else:
        data = {'exit_code': -1, 'message': "There were not recieved any pandaid and testname"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    if 'nightly_release_short' in request.session['requestParams']:
        nightly_release_short = request.session['requestParams']['nightly_release_short']
    else:
        data = {'exit_code': -1, 'message': "No nightly_release_short provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    if 'platform' in request.session['requestParams']:
        platform = request.session['requestParams']['platform']
    else:
        data = {'exit_code': -1, 'message': "No platform provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    if 'project' in request.session['requestParams']:
        project = request.session['requestParams']['project']
    else:
        data = {'exit_code': -1, 'message': "No project provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    if 'package' in request.session['requestParams']:
        package = request.session['requestParams']['package']
    else:
        data = {'exit_code': -1, 'message': "No package provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    if 'nightly_tag' in request.session['requestParams']:
        nightly_tag = request.session['requestParams']['nightly_tag']
    else:
        data = {'exit_code': -1, 'message': "No nightly_tag provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=400, content_type='application/json')

    ### Processing extra params
    if 'nightly_tag_display' in request.session['requestParams']:
        nightly_tag_display = request.session['requestParams']['nightly_tag_display']
    else:
        nightly_tag_display = request.session['requestParams']['nightly_tag']

    if 'html' in request.session['requestParams']:
        extra_info['html'] = request.session['requestParams']['html']

    ### Checking whether params is valid
    try:
        pandaid = int(pandaid)
    except:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=422, content_type='application/json')

    if pandaid < 0:
        data = {'exit_code': -1, 'message': "Illegal pandaid was recieved"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=422, content_type='application/json')

    if not str(testname).startswith('test_'):
        data = {'exit_code': -1, 'message': "Illegal test name was recieved"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=422, content_type='application/json')

    # Checking if provided pandaid exists in panda db
    query = {'pandaid': pandaid}
    values = ('pandaid', 'jeditaskid', 'username', 'computingsite', 'jobname')
    jobs = []
    jobs.extend(CombinedWaitActDefArch4.objects.filter(**query).values(*values))
    try:
       job = jobs[0]
    except:
        data = {'exit_code': -1, 'message': "Provided pandaid does not exists"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=422, content_type='application/json')

    # Checking whether provided pandaid is art job
    if 'username' in job and job['username'] != 'artprod':
        data = {'exit_code': -1, 'message': "Provided pandaid is not art job"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return HttpResponse(json.dumps(data), status=422, content_type='application/json')

    # Preparing params to register art job
    branch = concat_branch({'nightly_release_short':nightly_release_short, 'project': project, 'platform': platform})
    if 'computingsite' in job:
        computingsite = job['computingsite']
    if 'jeditaskid' in job:
        jeditaskid = job['jeditaskid']

        # get files -> extract log tarball name, attempts
        files = []
        fquery = {'jeditaskid': jeditaskid, 'pandaid': pandaid, 'type__in': ('pseudo_input', 'input', 'log')}
        files.extend(Filestable4.objects.filter(**fquery).values('jeditaskid', 'pandaid', 'fileid', 'lfn', 'type', 'attemptnr'))
        # count of attempts starts from 0, for readability change it to start from 1
        if len(files) > 0:
            input_files = [f for f in files if f['type'] in ('pseudo_input', 'input')]
            if len(input_files) > 0:
                attemptnr = 1 + max([f['attemptnr'] for f in input_files])
                inputfileid = max([f['fileid'] for f in input_files])
            log_lfn = [f['lfn'] for f in files if f['type'] == 'log']
            if len(log_lfn) > 0:
                try:
                    tarindex = int(re.search('.([0-9]{6}).log.', log_lfn[0]).group(1))
                except:
                    _logger.info('Failed to extract tarindex from log lfn')
                    tarindex = None
    if 'jobname' in job:
        try:
            gitlabid = int(re.search('.([0-9]{6,8}).', job['jobname']).group(1))
        except:
            _logger.info('Failed to extract tarindex from log lfn')
            gitlabid = None
    _logger.info(f"""Got job-related metadata for test {pandaid}: 
        computingsite={computingsite}, tarindex={tarindex}, inputfileid={inputfileid}, attemptnr={attemptnr}""")

    # extract datetime from str nightly time
    nightly_tag_date = None
    try:
        nightly_tag_date = parse_datetime(nightly_tag)
    except:
        _logger.exception('Failed to parse date from nightly_tag')

    # Check whether the pandaid has been registered already
    if ARTTests.objects.filter(pandaid=pandaid).count() == 0:
        # INSERT ROW
        try:
            insertRow = ARTTests.objects.create(
                pandaid=pandaid,
                jeditaskid=jeditaskid,
                testname=testname,
                nightly_release_short=nightly_release_short,
                nightly_tag=nightly_tag,
                nightly_tag_display=nightly_tag_display,
                project=project,
                platform=platform,
                branch=branch,
                package=package,
                extrainfo=json.dumps(extra_info),
                created=timezone.now(),
                nightly_tag_date=nightly_tag_date,
                attemptnr=attemptnr,
                maxattempt=2,
                inputfileid=inputfileid,
                tarindex=tarindex,
                gitlabid=gitlabid,
                computingsite=computingsite,
                status=art_const.TEST_STATUS_INDEX['active'],
            )
            insertRow.save()
            data = {'exit_code': 0, 'message': "Provided pandaid has been successfully registered"}
            _logger.info(data['message'] + str(request.session['requestParams']))
        except Exception as e:
            data = {'exit_code': 0, 'message': "Failed to register test, can not save the row to DB"}
            _logger.error('{}\n{}\n{}'.format(data['message'], str(e), str(request.session['requestParams'])))
    else:
        data = {'exit_code': 0, 'message': "Provided pandaid is already registered"}
        _logger.warning(data['message'] + str(request.session['requestParams']))

    return HttpResponse(json.dumps(data), status=200, content_type='application/json')

@csrf_exempt
def upload_test_result(request):
    """
    A view to receive and save test result i.e. content of artReport.json.
    It expects as a POST param:
        pandaid - int
        artreport - dict, content of artReport.json
    E.g.  curl -X POST -d 'pandaid=XXX' -d 'artreport={"art": {}}' http://bigpanda.cern.ch/art/uploadtestresult/?json
    :param request:
    :return: HTTP response
    """
    _logger.debug('[ART] uploadtestresults: GET=' + str(request.GET) + ' POST=' + str(request.GET) + ' body:' + str(request.body))

    valid, response = initRequest(request)
    if not valid:
        return JsonResponse({'error': "Bad request"}, status=400)

    # log all the req params
    _logger.info('[ART] uploadtestresults requestParams: ' + str(request.session['requestParams']))

    if 'requestParams' in request.session and 'pandaid' in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
    else:
        data = {'message': "No valid pandaid provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return JsonResponse(data, status=400)

    if 'requestParams' in request.session and 'artreport' in request.session['requestParams']:
        art_report = request.session['requestParams']['artreport']
        if isinstance(art_report, str):
            try:
                art_report = json.loads(art_report)
            except json.JSONDecodeError as e:
                _logger.exception(f"Invalid JSON syntax: {e}")
                return JsonResponse({"message": "Invalid JSON syntax of data"}, status=400)
        if 'art' in art_report:
            art_report = art_report['art']
        else:
            data = {'message': "Wrong format of provided data"}
            _logger.warning(data['message'] + str(request.session['requestParams']))
            return JsonResponse(data, status=400)
    else:
        data = {'message': "No valid pandaid provided"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return JsonResponse(data, status=400)

    # check provided pandaid exist, and it is ART test
    # get test
    art_jobs = []
    values = [f.name for f in ARTTests._meta.get_fields() if not f.is_relation]
    art_jobs.extend(ARTTests.objects.filter(pandaid=pandaid).values(*values, result=F('artsubresult__subresult')))
    if len(art_jobs) == 0:
        data = {'message': "Provided pandaid does not exists or it is not ART test "}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return JsonResponse(data, status=400)
    art_job = art_jobs[0]

    # check if ART results already exist
    if 'result' in art_job and len(art_job['result']) > 0:
        data = {'message': "Results for this test is already exist"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        # return JsonResponse(data, status=400)

    # check provided data is valid
    _logger.debug(f"Got the following result for pandaid={pandaid}:\n{art_report}")
    if art_report and 'panda_id' in art_report and str(art_report['panda_id']) != str(pandaid):
        data = {'message': "pandaids does not match"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return JsonResponse(data, status=400)
    if art_report and 'name' in art_report and 'testname' in art_job and art_report['name'] != art_job['testname']:
        data = {'message': "test names do not match"}
        _logger.warning(data['message'] + str(request.session['requestParams']))
        return JsonResponse(data, status=400)

    # insert subresults to special table
    is_saved = save_subresults({pandaid: json.dumps(art_report)})

    # update status of test accordingly
    is_updated = update_test_status({'pandaid': pandaid, 'result': json.dumps(art_report)})
    if not is_updated:
        _logger.warning(f"Failed to update test {pandaid} status, will need to do it loading tarball afterwords")

    if is_saved:
        data = {'message': 'Test result has been successfully saved'}
        _logger.info(f"pandaid: {pandaid}: {data['message']}")
        response = JsonResponse(data, status=200)
    else:
        data = {'message': 'Failed to save test result'}
        _logger.info(f"pandaid: {pandaid}: {data['message']}")
        response = JsonResponse(data, status=500)
    return response


def sendArtReport(request):
    """
    A view to send ART jobs status report by email
    :param request:
    :return: json
    """
    valid, response = initRequest(request)
    template = 'templated_email/artReportPackage.html'
    try:
        EMAIL_SUBJECT_PREFIX = settings.EMAIL_SUBJECT_PREFIX
    except:
        _logger.warning('No EMAIL_SUBJECT_PREFIX set in settings, keeping blank')
        EMAIL_SUBJECT_PREFIX = ''
    subject = '{}[ART] GRID ART jobs status report'.format(EMAIL_SUBJECT_PREFIX)
    errorMessage = ''
    if 'ntag_from' not in request.session['requestParams']:
        valid = False
        errorMessage = 'No ntag provided!'
    elif 'ntag_to' not in request.session['requestParams']:
        valid = False
        errorMessage = 'No ntag provided!'
    elif request.session['requestParams']['ntag_from'] != (datetime.now() - timedelta(days=1)).strftime(art_const.DATETIME_FORMAT['default']) \
            or request.session['requestParams']['ntag_to'] != datetime.now().strftime(art_const.DATETIME_FORMAT['default']):
        valid = False
        errorMessage = 'Provided ntag is not valid'
    if not valid:
        return HttpResponse(json.dumps({'errorMessage': errorMessage}), content_type='application/json')


    final_result_dict = {v: k for k, v in art_const.TEST_STATUS_INDEX.items()}
    # process URL params to query params
    query, extra_str = setupView(request)
    _logger.info('Set up view: {}s'.format(time.time() - request.session['req_init_time']))

    # getting tests
    tests = []
    values = (
        'package', 'branch', 'nightly_release_short', 'platform', 'project', 'nightly_tag_date', 'nightly_tag', 'testname',
        'pandaid', 'jeditaskid', 'inputfileid', 'status'
    )
    tests.extend(ARTTests.objects.filter(**query).extra(where=[extra_str]).values(*values, result=F('artsubresult__subresult')))

    # filter out previous attempts, add branch etc
    art_jobs = clean_tests_list(tests, add_link_previous_attempt=True)
    _logger.info('Got {} tests: {}s'.format(len(tests), time.time() - request.session['req_init_time']))

    # prepare data for report
    artjobsdictpackage = {}
    for job in art_jobs:
        # we want in report only overnight builds, usually starting from 8pm
        if job['nightly_tag_date'] > query['nightly_tag_date__range'][0] + timedelta(hours=20):
            if job['package'] not in artjobsdictpackage.keys():
                artjobsdictpackage[job['package']] = {}
                artjobsdictpackage[job['package']]['branch'] = job['branch']
                artjobsdictpackage[job['package']]['ntag_full'] = job['nightly_tag']
                artjobsdictpackage[job['package']]['ntag'] = job['ntag'].strftime(art_const.DATETIME_FORMAT['default'])
                artjobsdictpackage[job['package']]['link'] = 'https://bigpanda.cern.ch/art/tasks/?package={}&ntag={}'.format(
                    job['package'], job['ntag'].strftime(art_const.DATETIME_FORMAT['default']))
                artjobsdictpackage[job['package']]['branches'] = {}
            if job['branch'] not in artjobsdictpackage[job['package']]['branches'].keys():
                artjobsdictpackage[job['package']]['branches'][job['branch']] = {}
                artjobsdictpackage[job['package']]['branches'][job['branch']]['name'] = job['branch']
                for state in art_const.TEST_STATUS:
                    artjobsdictpackage[job['package']]['branches'][job['branch']]['n' + state] = 0
                artjobsdictpackage[job['package']]['branches'][job['branch']]['linktoeos'] = 'https://atlas-art-data.web.cern.ch/atlas-art-data/grid-output/{}/{}/{}/'.format(
                    job['branch'], job['nightly_tag'], job['package'])

            if 'status' in job and isinstance(job['status'], int):
                finalresult = final_result_dict[job['status']]
            else:
                finalresult, _ = get_final_result(job)
            artjobsdictpackage[job['package']]['branches'][job['branch']]['n' + finalresult] += 1

    # dict -> list & ordering
    for packagename, sumdict in artjobsdictpackage.items():
        sumdict['packages'] = sorted(artjobsdictpackage[packagename]['branches'].values(), key=lambda k: k['name'])

    # get recipient emails and prepare test results summary per email
    rquery = {}
    rquery['report'] = 'art'
    recipientslist = ReportEmails.objects.filter(**rquery).values()

    summaryPerRecipient = {}
    for row in recipientslist:
        if row['email'] is not None and len(row['email']) > 0 and row['email'] not in summaryPerRecipient:
            summaryPerRecipient[row['email']] = {}
        if row['type'] in artjobsdictpackage.keys():
            summaryPerRecipient[row['email']][row['type']] = artjobsdictpackage[row['type']]

    maxTries = 1
    for recipient, summary in summaryPerRecipient.items():
        isSent = False
        i = 0
        while not isSent:
            i += 1
            if i > 1:
                time.sleep(10)
            isSent = send_mail_bp(template, subject, summary, recipient)
            # put 10 seconds delay to bypass the message rate limit of smtp server
            time.sleep(10)
            if i >= maxTries:
                break

    return HttpResponse(json.dumps({'isSent': isSent, 'nTries': i}), content_type='application/json')


def sendDevArtReport(request):
    """
    A view to send special report to ART developer
    :param request:
    :return: json
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    isSent = False
    template = 'templated_email/artDevReport.html'
    try:
        EMAIL_SUBJECT_PREFIX = settings.EMAIL_SUBJECT_PREFIX
    except:
        _logger.warning('No EMAIL_SUBJECT_PREFIX set in settings, keeping blank')
        EMAIL_SUBJECT_PREFIX = ''
    subject = '{}[ART] Run on specific day tests'.format(EMAIL_SUBJECT_PREFIX)

    query = {'created__castdate__range': [timezone.now() - timedelta(hours=1), timezone.now()]}
    exquery = {'nightly_tag__exact': F('nightly_tag_display')}

    tests = list(ARTTests.objects.filter(**query).exclude(**exquery).values())

    if len(tests) > 0:
        for test in tests:
            test['artlink'] = 'https://bigpanda.cern.ch/art/jobs/?package={}&branch={}&ntag={}'.format(
                test['package'],
                '/'.join((test['nightly_release_short'], test['project'], test['platform'])),
                test['nightly_tag_display'][:10],
            )
            test['joblink'] = 'https://bigpanda.cern.ch/job/{}/'.format(test['pandaid'])

        rquery = {}
        rquery['report'] = 'artdev'
        recipientslist = list(ReportEmails.objects.filter(**rquery).values('email'))

        maxTries = 1
        for recipient in recipientslist:
            isSent = False
            i = 0
            while not isSent:
                i += 1
                if i > 1:
                    time.sleep(10)
                isSent = send_mail_bp(template, subject, tests, recipient)
                # put 10 seconds delay to bypass the message rate limit of smtp server
                time.sleep(10)
                if i >= maxTries:
                    break

    return HttpResponse(json.dumps({'isSent': isSent}), content_type='application/json')


@never_cache
def remove_old_tests(request):
    """
    Remove old records in art_tests
    :param request:
    :return:
    """
    start = datetime.now()
    message = ''
    # get max pandaid for tests older than retention policy
    pandaid_max = None
    pandaid = ARTTests.objects.filter(created__lte=(datetime.now() - timedelta(days=art_const.RETENTION_PERIOD_DAYS))).aggregate(Max('pandaid'))
    if len(pandaid) > 0 and pandaid['pandaid__max'] is not None:
        pandaid_max = int(pandaid['pandaid__max'])
    else:
        return JsonResponse({'message': f"No test for deletion found, it took {(datetime.now() - start).total_seconds()}s"}, status=200)

    # delete results and tests older than retention policy
    try:
        res = ARTSubResult.objects.filter(pandaid__lte=pandaid_max).delete()
        _logger.info(f"Deleted {res[0]} test results for tests,  older than {pandaid_max}")
    except Exception as ex:
        message += "Failed to delete test results"
        _logger.exception(f"{message} with:\n{ex}")
    try:
        tests = ARTTests.objects.filter(pandaid__lte=pandaid_max).delete()
        _logger.info(f"Deleted {tests[0]} tests, older than {pandaid_max}")
    except Exception as ex:
        message += "Failed to delete tests"
        _logger.exception(f"{message} with:\n{ex}")

    status = 200
    if len(message) > 0:
        status = 500
    else:
        message += f"Successfully deleted old records"

    return JsonResponse({'message': f"{message}, it took {(datetime.now()-start).total_seconds()}s"}, status=status)


@never_cache
def fill_table(request):
    """
    Fill new columns in art_tests
    :param request:
    :return:
    """
    start = datetime.now()
    # get last ntag with empty new fields
    ntag = None
    ntags = ARTTests.objects.filter(gitlabid__isnull=True,created__lt=(datetime.now() - timedelta(days=2))).aggregate(Max('nightly_tag'))
    if len(ntags) > 0:
        ntag = ntags['nightly_tag__max']

    tests_to_update = []
    if ntag is not None:
        tests_to_update.extend(ARTTests.objects.filter(nightly_tag=ntag).values('pandaid'))

    print(f"Got {len(tests_to_update)} tests to update for ntag={ntag}")
    i = 0
    for t in tests_to_update:
        # Preparing params to fill art_tests
        i = i+1
        print(f"/n{i}/{len(tests_to_update)}")
        gitlabid = None
        pandaid = t['pandaid']
        query = {'pandaid': pandaid}
        values = ('pandaid', 'jobname')
        jobs = []
        jobs.extend(CombinedWaitActDefArch4.objects.filter(**query).values(*values))
        if len(jobs) == 0:
            jobs.extend(Jobsarchived.objects.filter(**query).values(*values))
        job = jobs[0]

        try:
            gitlabid = int(re.search('.([0-9]{6,8}).', job['jobname']).group(1))
        except:
            _logger.info('Failed to extract tarindex from log lfn')
            gitlabid = None


        print(f"""Got job-related metadata for test {pandaid}: gitlabid={gitlabid}""")

        try:
            ARTTests.objects.filter(pandaid=pandaid).update(
                gitlabid=gitlabid
            )
        except Exception as ex:
            print(f"""Failed to update test {pandaid} with : 
                gitlabid={gitlabid}\n{str(ex)}""")
            return JsonResponse({'message': f"Failed to update info for test {pandaid}"}, status=500)

    return JsonResponse({'message': f"Updated {len(tests_to_update)} tests for ntag={ntag}, it took {(datetime.now()-start).total_seconds()}s"}, status=200)