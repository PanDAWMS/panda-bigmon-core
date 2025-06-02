import copy
import logging
import pandas as pd
import time
from collections import defaultdict

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


def create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid):
    """Create a structured error entry for the error summary."""
    return {
        'error': errcode,
        'codename': err['error'],
        'codeval': errnum,
        'diag': errdiag,
        'desc': errdesc,
        'example_pandaid': pandaid,
        'count': 0
    }


def update_error_summary(error_summary_dict, key, errcode, error_entry):
    """Update the error summary dictionary with a new error entry."""
    error_summary_dict[key]['name'] = key
    if errcode not in error_summary_dict[key]['errors']:
        error_summary_dict[key]['errors'][errcode] = error_entry.copy()
    error_summary_dict[key]['errors'][errcode]['count'] += 1
    error_summary_dict[key]['toterrors'] += 1


def to_sorted_list(d, errsort=False, totalkey=None):
    """Convert a dictionary to a sorted list of dictionaries."""
    out = []
    for key in sorted(d):
        item = d[key]
        item['errorlist'] = sorted(item['errors'].values(), key=lambda x: -x['count']) if errsort else list(item['errors'].values())
        out.append(item)
    if totalkey:
        out = sorted(out, key=lambda x: -x[totalkey])
    return out


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
    error_histograms = {}

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

    errsByCount = {}
    errsByUser = defaultdict(lambda: {'errors': {}, 'toterrors': 0})
    errsBySite = defaultdict(lambda: {'errors': {}, 'toterrors': 0, 'toterrjobs': 0})
    errsByTask = defaultdict(lambda: {'errors': {}, 'toterrors': 0, 'toterrjobs': 0})
    sumd = defaultdict(lambda: defaultdict(int))

    for job in jobs:
        if not is_test_jobs and job['jobstatus'] not in ['failed', 'holding']:
            continue

        site = job['wn'] if is_site_req else job['computingsite']
        user = job['produsername']
        taskid = job.get('jeditaskid') or job.get('taskid') or 0
        tasktype = 'jeditaskid' if job.get('jeditaskid') else 'taskid'
        taskname = tasknamedict.get(taskid, '')

        # Summary aggregation
        for f in flist:
            if job.get(f):
                sumd[f][job[f]] += 1
        for sh in job.get('specialhandling', '').split():
            if sh:
                sumd['specialhandling'][sh] += 1

        for err in error_components:
            errval = job.get(err['error'])
            if not errval or str(errval) == '0':
                continue
            try:
                errnum = int(errval)
            except ValueError:
                continue
            if err['name'] == 'transform':
                # for transformation errors, we do not have a related diag field, we added it from ErrorDescriptions already
                errdiag = job.get('transformerrordiag', '')
            else:
                errdiag = job.get(err['diag']) or ''
            errcode = f"{err['name']}:{errnum}"
            errdesc = job.get(f"{err['name']}_error_desc", '')
            pandaid = job['pandaid']

            if errcode not in errsByCount:
                errsByCount[errcode] = create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid)
                errsByCount[errcode]['pandalist'] = {}
            errsByCount[errcode]['count'] += 1
            errsByCount[errcode]['pandalist'][pandaid] = errdiag

            update_error_summary(errsByUser, user, errcode, errsByCount[errcode])
            update_error_summary(errsBySite, site, errcode, errsByCount[errcode])
            if taskid > 1000000 or tasktype == 'jeditaskid':
                if 'name' not in errsByTask[taskid]:
                    errsByTask[taskid]['name'] = taskid
                    errsByTask[taskid]['longname'] = taskname
                    errsByTask[taskid]['tasktype'] = tasktype
                update_error_summary(errsByTask, taskid, errcode, errsByCount[errcode])

        errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask:
            errsByTask[taskid]['toterrjobs'] += 1

    # Convert summaries to sorted lists
    errsByUserL = to_sorted_list(errsByUser, errsort=True, totalkey='toterrors')
    errsBySiteL = to_sorted_list(errsBySite, errsort=True, totalkey='toterrors')
    errsByTaskL = to_sorted_list(errsByTask, errsort=True, totalkey='toterrors')
    errsByCountL = sorted(errsByCount.values(), key=lambda x: -x['count'])

    suml = [
        {
            'field': f,
            'list': sorted(
                [{'kname': k, 'kvalue': v} for k, v in sumd[f].items()], key=lambda x: -x['kvalue'] if sortby == 'count' else x['kname']
            )
        }
        for f in sorted(sumd)
    ]
    _logger.debug('Dict -> list & sorting are done: {}'.format(time.time() - start_time))

    if 'errsHist' in outputs:
        error_histograms = build_error_histograms(jobs, is_wn_instead_of_site=is_site_req)
    _logger.debug('Built errHist: {}'.format(time.time() - start_time))

    return errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, error_histograms

