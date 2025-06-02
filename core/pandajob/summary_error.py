
import copy
import logging
import pandas as pd
import time

from core.libs.task import taskNameDict
from core.libs.job import get_job_walltime
from core.libs.exlib import calc_freq_time_series

from django.conf import settings
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def get_error_message_summary(jobs):
    """
    Aggregation of error messages for each error code
    :param jobs: list of job dicts including error codes, error messages, timestamps of job start and end, corecount
    :return: list of rows for datatable
    """
    error_message_summary_list = []
    error_message_summary = {}
    N_SAMPLE_JOBS = 3

    error_components = copy.deepcopy(const.JOB_ERROR_COMPONENTS)

    for job in jobs:
        for comp in error_components:
            if comp['error'] in job and job[comp['error']] is not None and job[comp['error']] != '' and int(job[comp['error']]) > 0:
                comp_code_str = f"{comp['name']}:{str(job[comp['error']])}"
                if not comp_code_str in error_message_summary:
                    error_message_summary[comp_code_str] = {'count': 0, 'walltimeloss': 0, 'messages': {}}
                error_message_summary[comp_code_str]['count'] += 1
                try:
                    corecount = int(job['actualcorecount'])
                except:
                    corecount = 1
                try:
                    walltime = int(get_job_walltime(job))
                except:
                    walltime = 0
                error_message_summary[comp_code_str]['walltimeloss'] += walltime * corecount
                # transexitcode has no related diag field, but we already added it from ErrorDescriptions
                if comp['name'] != 'transform':
                    diag = job[comp['diag']] if len(job[comp['diag']]) > 0 else '---'
                else:
                    diag = job['transformerrordiag'] if 'transformerrordiag' in job and len(job['transformerrordiag']) > 0 else '---'
                if not diag in error_message_summary[comp_code_str]['messages']:
                    error_message_summary[comp_code_str]['messages'][diag] = {'count': 0, 'pandaids': []}
                error_message_summary[comp_code_str]['messages'][diag]['count'] += 1
                if len(error_message_summary[comp_code_str]['messages'][diag]['pandaids']) < N_SAMPLE_JOBS:
                    error_message_summary[comp_code_str]['messages'][diag]['pandaids'].append(job['pandaid'])

    # form a dict for mapping error code name and field in panda db in order to prepare links to job selection
    errname2dbfield = {}
    for comp in error_components:
        errname2dbfield[comp['name']] = comp['error']

    # dict -> list
    for errcode, errinfo in error_message_summary.items():
        errcodename = errname2dbfield[errcode.split(':')[0]]
        errcodeval = errcode.split(':')[1]
        for errmessage, errmessageinfo in errinfo['messages'].items():
            error_message_summary_list.append({
                'errcode': errcode,
                'errcodename': errcodename,
                'errcodeval': errcodeval,
                'errcodecount': errinfo['count'],
                'errcodewalltimeloss': round(errinfo['walltimeloss']/60.0/60.0/24.0/360.0, 2),
                'errmessage': errmessage,
                'errmessagecount': errmessageinfo['count'],
                'pandaids': list(errmessageinfo['pandaids'])
            })

    return error_message_summary_list


def get_job_error_categories(job):
    """
    Get shortened error category string by error field and error code
    :param job: dict, name of error field
    :return: error_category_list: list of str, shortened error category string
    """
    error_category_list = []
    for k in list(const.JOB_ERROR_COMPONENTS):
        if k['error'] in job and job[k['error']] is not None and job[k['error']] != '' and int(job[k['error']]) > 0:
            error_category_list.append(f"{k['name']}:{job[k['error']]}")

    return error_category_list


def prepare_binned_and_total_data(df, column, freq='10T'):
    """
    Prepare binned and total time-series data for plots
    :param df: data frame
    :param column: column in data frame which use to split values for stacking
    :param freq: frequency for resampling
    :return:
    """
    # resample in 10-minute bins and count occurrences for each unique value in the specified column
    resampled = df.groupby([pd.Grouper(freq=freq), column]).size().unstack(fill_value=0)

    # calculate total counts across all bins for pie chart
    total_counts = resampled.sum().to_dict()

    # convert binned data to Chart.js format
    header = ["timestamp"] + list(resampled.columns)
    binned_data = [header] + [
        [timestamp.strftime(settings.DATETIME_FORMAT)] + list(row) for timestamp, row in resampled.iterrows()
    ]

    return {
        'binned': binned_data,
        'total': total_counts
    }


def categorize_low_impact_by_percentage(df, column, threshold_percent):
    """
    Replace low impact values as "Other" category
    :param df: data frame
    :param column: column name
    :param threshold_percent: int
    :return:
    """
    # count occurrences of each unique value across the entire dataset
    counts = df[column].value_counts()
    total_count = counts.sum()

    # calculate threshold in terms of counts
    threshold_count = total_count * (threshold_percent / 100.0)

    # identify low-impact values below this threshold
    low_impact_values = counts[counts < threshold_count].index

    # replace low-impact values with "Other"
    df[column] = df[column].apply(lambda x: "Other" if x in low_impact_values else x)
    return df


def build_error_histograms(jobs, is_wn_instead_of_site=False):
    """
    Prepare histograms data by different categories
    :param jobs:
    :return: error_histograms: dict of data for histograms by different categories
    """
    threshold_percent = 2  # % threshold for low-impact values

    timestamp_list = []
    data = []
    for job in jobs:
        data.append({
            'modificationtime': job['modificationtime'],
            'site': job['computingsite'] if not is_wn_instead_of_site else job['wn'],
            'code': ','.join(sorted(get_job_error_categories(job))),
            'task': str(job['jeditaskid']),
            'user': job['produsername'],
            'request': str(job['reqid']) if 'reqid' in job else 'None',
        })
        timestamp_list.append(job['modificationtime'])

    freq = calc_freq_time_series(timestamp_list, n_bins_max=60)

    if len(data) > 0:
        df = pd.DataFrame(data)
        df['modificationtime'] = pd.to_datetime(df['modificationtime'])
        df.set_index('modificationtime', inplace=True)

        # Apply the function to each column where you want low-impact values grouped
        for column in ['site', 'code', 'task', 'user', 'request']:
            df = categorize_low_impact_by_percentage(df, column, threshold_percent)

        # Generate JSON-ready data for each column
        output_data = {}
        for column in ['site', 'code', 'task', 'user', 'request']:
            output_data[column] = prepare_binned_and_total_data(df, column, freq=freq)

        total_jobs_per_bin = df.resample(freq).size().reset_index(name='total')
        total_jobs_per_bin['modificationtime'] = total_jobs_per_bin['modificationtime'].dt.strftime(
            settings.DATETIME_FORMAT)

        output_data['total'] = {
            'binned': [['timestamp', 'total']] + total_jobs_per_bin.values.tolist(),
            'total': {}
        }
    else:
        output_data = {}

    return output_data


def errorSummaryDict(jobs, is_test_jobs=False, sortby='count', is_user_req=False, is_site_req=False, **kwargs):
    """
    Takes a job list and produce error summaries from it
    :param jobs: list of dicts
    :param is_test_jobs:  bool: for test jobs we do not limit to "failed" jobs only
    :param sortby: str: count or alpha
    :param is_user_req: bool: we do jeditaskid in attribute summary only if a user is specified
    :param is_site_req: bool: we do summary per worker node if True
    :param kwargs: flist and outputs
    :return: errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, error_histograms
    """

    start_time = time.time()
    errsByCount = {}
    errsBySite = {}
    errsByUser = {}
    errsByTask = {}
    error_histograms = {}
    sumd = {}

    if 'flist' in kwargs:
        flist = kwargs['flist']
    else:
        flist = copy.deepcopy(const.JOB_FIELDS_ERROR_VIEW)
    if is_user_req is not None and 'jeditaskid' in flist:
        flist = list(flist)
        flist.remove('jeditaskid')

    if 'output' in kwargs:
        outputs = kwargs['output']
    else:
        outputs = ['errsByCount', 'errsBySite', 'errsByUser', 'errsByTask', 'errsHist']

    # get task names needed for error summary by task
    tasknamedict = {}
    if 'errsByTask' in outputs:
        tasknamedict = taskNameDict(jobs)
        _logger.debug('Got tasknames for summary by task: {}'.format(time.time() - start_time))

    error_components = copy.deepcopy(const.JOB_ERROR_COMPONENTS)

    for job in jobs:
        if not is_test_jobs and job['jobstatus'] not in ['failed', 'holding']:
            continue
        # if specific site, we do summary per worker node
        if is_site_req:
            site = job['wn']
        else:
            site = job['computingsite']
        user = job['produsername']
        taskname = ''
        if job['jeditaskid'] is not None and job['jeditaskid'] > 0:
            taskid = job['jeditaskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'jeditaskid'
        else:
            taskid = job['taskid'] if not job['taskid'] is None else 0
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'taskid'

        ## Overall summary
        for f in flist:
            if f in job and job[f]:
                if not f in sumd:
                    sumd[f] = {}
                if not job[f] in sumd[f]:
                    sumd[f][job[f]] = 0
                sumd[f][job[f]] += 1
        if 'specialhandling' in job and job['specialhandling']:
            if not 'specialhandling' in sumd:
                sumd['specialhandling'] = {}
            shl = job['specialhandling'].split()
            for v in shl:
                if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                sumd['specialhandling'][v] += 1

        errsByList = {}
        for err in error_components:
            # error code of zero is not an error
            if job[err['error']] != 0 and job[err['error']] != '' and job[err['error']] != '0' and job[err['error']] is not None:
                errdiag = ''
                try:
                    errnum = int(job[err['error']])
                except ValueError:
                    continue
                if err['diag']:
                    errdiag = job[err['diag']]
                elif err['name'] == 'transform':
                    errdiag = job['transformerrordiag'] if 'transformerrordiag' in job and len(job['transformerrordiag']) > 0 else ''

                errsByList[job['pandaid']] = errdiag
                errcode = f"{err['name']}:{str(errnum)}"
                if errcode not in errsByCount:
                    errsByCount[errcode] = {}
                    errsByCount[errcode]['error'] = errcode
                    errsByCount[errcode]['codename'] = err['error']
                    errsByCount[errcode]['codeval'] = errnum
                    errsByCount[errcode]['diag'] = errdiag
                    errsByCount[errcode]['example_pandaid'] = job['pandaid']
                    errsByCount[errcode]['count'] = 0
                    errsByCount[errcode]['pandalist'] = {}
                errsByCount[errcode]['count'] += 1
                errsByCount[errcode]['pandalist'].update(errsByList)
                if user not in errsByUser:
                    errsByUser[user] = {}
                    errsByUser[user]['name'] = user
                    errsByUser[user]['errors'] = {}
                    errsByUser[user]['toterrors'] = 0
                if errcode not in errsByUser[user]['errors']:
                    errsByUser[user]['errors'][errcode] = {}
                    errsByUser[user]['errors'][errcode]['error'] = errcode
                    errsByUser[user]['errors'][errcode]['codename'] = err['error']
                    errsByUser[user]['errors'][errcode]['codeval'] = errnum
                    errsByUser[user]['errors'][errcode]['diag'] = errdiag
                    errsByUser[user]['errors'][errcode]['example_pandaid'] = job['pandaid']
                    errsByUser[user]['errors'][errcode]['count'] = 0
                errsByUser[user]['errors'][errcode]['count'] += 1
                errsByUser[user]['toterrors'] += 1

                if site not in errsBySite:
                    errsBySite[site] = {}
                    errsBySite[site]['name'] = site
                    errsBySite[site]['errors'] = {}
                    errsBySite[site]['toterrors'] = 0
                    errsBySite[site]['toterrjobs'] = 0
                if errcode not in errsBySite[site]['errors']:
                    errsBySite[site]['errors'][errcode] = {}
                    errsBySite[site]['errors'][errcode]['error'] = errcode
                    errsBySite[site]['errors'][errcode]['codename'] = err['error']
                    errsBySite[site]['errors'][errcode]['codeval'] = errnum
                    errsBySite[site]['errors'][errcode]['diag'] = errdiag
                    errsBySite[site]['errors'][errcode]['example_pandaid'] = job['pandaid']
                    errsBySite[site]['errors'][errcode]['count'] = 0
                errsBySite[site]['errors'][errcode]['count'] += 1
                errsBySite[site]['toterrors'] += 1

                if tasktype == 'jeditaskid' or (taskid is not None and taskid > 1000000):
                    if taskid not in errsByTask:
                        errsByTask[taskid] = {}
                        errsByTask[taskid]['name'] = taskid
                        errsByTask[taskid]['longname'] = taskname
                        errsByTask[taskid]['errors'] = {}
                        errsByTask[taskid]['toterrors'] = 0
                        errsByTask[taskid]['toterrjobs'] = 0
                        errsByTask[taskid]['tasktype'] = tasktype
                    if errcode not in errsByTask[taskid]['errors']:
                        errsByTask[taskid]['errors'][errcode] = {}
                        errsByTask[taskid]['errors'][errcode]['error'] = errcode
                        errsByTask[taskid]['errors'][errcode]['codename'] = err['error']
                        errsByTask[taskid]['errors'][errcode]['codeval'] = errnum
                        errsByTask[taskid]['errors'][errcode]['diag'] = errdiag
                        errsByTask[taskid]['errors'][errcode]['example_pandaid'] = job['pandaid']
                        errsByTask[taskid]['errors'][errcode]['count'] = 0
                    errsByTask[taskid]['errors'][errcode]['count'] += 1
                    errsByTask[taskid]['toterrors'] += 1

        if site in errsBySite: errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask: errsByTask[taskid]['toterrjobs'] += 1
    _logger.debug('Built summary dicts: {}'.format(time.time() - start_time))

    # reorganize as sorted lists
    errsByCountL = []
    errsBySiteL = []
    errsByUserL = []
    errsByTaskL = []
    esjobs = []
    kys = errsByCount.keys()
    kys = sorted(kys)
    for err in kys:
        for key, value in sorted(errsByCount[err]['pandalist'].items()):
            if value == '':
                value = 'None'
            esjobs.append(key)
        errsByCountL.append(errsByCount[err])

    kys = list(errsByUser.keys())
    kys = sorted(kys)
    for user in kys:
        errsByUser[user]['errorlist'] = []
        errkeys = errsByUser[user]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsByUser[user]['errorlist'].append(errsByUser[user]['errors'][err])
        if sortby == 'count':
            errsByUser[user]['errorlist'] = sorted(errsByUser[user]['errorlist'], key=lambda x: -x['count'])
        errsByUserL.append(errsByUser[user])

    kys = list(errsBySite.keys())
    kys = sorted(kys)
    for site in kys:
        errsBySite[site]['errorlist'] = []
        errkeys = errsBySite[site]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsBySite[site]['errorlist'].append(errsBySite[site]['errors'][err])
        if sortby == 'count':
            errsBySite[site]['errorlist'] = sorted(errsBySite[site]['errorlist'], key=lambda x: -x['count'])
        errsBySiteL.append(errsBySite[site])

    kys = list(errsByTask.keys())
    kys = sorted(kys)
    for taskid in kys:
        errsByTask[taskid]['errorlist'] = []
        errkeys = errsByTask[taskid]['errors'].keys()
        errkeys = sorted(errkeys)
        for err in errkeys:
            errsByTask[taskid]['errorlist'].append(errsByTask[taskid]['errors'][err])
        if sortby == 'count':
            errsByTask[taskid]['errorlist'] = sorted(errsByTask[taskid]['errorlist'], key=lambda x: -x['count'])
        errsByTaskL.append(errsByTask[taskid])


    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sorted(sumd[f].keys())

        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])

    if sortby == 'count':
        for item in suml:
            item['list'] = sorted(item['list'], key=lambda x: -x['kvalue'])

        errsByCountL = sorted(errsByCountL, key=lambda x: -x['count'])
        errsByTaskL = sorted(errsByTaskL, key=lambda x: -x['toterrors'])
        errsBySiteL = sorted(errsBySiteL, key=lambda x: -x['toterrors'])
        errsByUserL = sorted(errsByUserL, key=lambda x: -x['toterrors'])

    _logger.debug('Dict -> list & sorting are done: {}'.format(time.time() - start_time))

    if 'errsHist' in outputs:
        error_histograms = build_error_histograms(jobs, is_wn_instead_of_site=is_site_req)
    _logger.debug('Built errHist: {}'.format(time.time() - start_time))

    return errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, error_histograms

