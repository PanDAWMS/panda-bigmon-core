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
from core.pandajob.utils import get_job_error_descriptions

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


def get_job_error_component_code_list(job):
    """
    Get shortened error component:code string by error field and error code
    :param job: dict, name of error field
    :return: error_list: list of str, shortened error category string
    """
    error_list = []
    for k in list(const.JOB_ERROR_COMPONENTS):
        if k['error'] in job and job[k['error']] is not None and job[k['error']] != '' and int(job[k['error']]) > 0:
            error_list.append(f"{k['name']}:{job[k['error']]}")

    return error_list


def get_job_error_category(job, error_descriptions=None):
    """
    Get error category for a job
    :param job: dict, job data
    :param error_descriptions: dict, error descriptions mapping
    :return: str, error category name
    """
    error_cat_desc = {
        '0': '0. Uncategorized',
        '1': '1. File and Storage Issues',
        '2': '2. Execution and Payload Failures',
        '3': '3. Network and Communication Errors',
        '4': '4. Job Termination and Kill Signals',
        '5': '5. Software and Environment Issues',
        '6': '6. Internal and Unknown Errors',
        '7': '7. Brokerage Errors',
        '8': '8. DDM Errors',
        '9': '9. Task Buffer Errors',
        '10': '10. PanDA Job Dispatcher Errors',
        '11': '11. PanDA Supervisor Errors',
        '12': '12. Transformation Errors',
    }

    if not error_descriptions:
        error_descriptions = get_job_error_descriptions()

    error_list = get_job_error_component_code_list(job)
    if len(error_list) == 0:
        return None

    # use the first error code as the category
    error_categories = list(set([error_descriptions.get(err, {}).get('category', 0) for err in error_list]))

    if len(error_categories) > 1 and 0 in error_categories:
        # if 'Uncategorized' is present but there are other categories, we remove 'Uncategorized' from the list
        error_categories.remove(0)

    if len(error_categories) == 1:
        return error_cat_desc[str(error_categories[0])] if str(error_categories[0]) in error_cat_desc else 'Uncategorized'
    else:
        _logger.debug(f"Multiple error categories found for job {job['pandaid']}: {error_categories}. Defaulting to 'Uncategorized'.")
        return f"Uncategorized ({','.join([str(c) for c in error_categories if c != 0])})"


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
    :param is_wn_instead_of_site : bool, if True, use worker node instead of site
    :return: error_histograms: dict of data for histograms by different categories
    """
    threshold_percent = 2  # % threshold for low-impact values
    plots = ['site', 'code', 'task', 'user', 'request', 'category']
    error_descriptions = get_job_error_descriptions()
    timestamp_list = []
    data = []
    for job in jobs:
        if job['jobstatus'] not in ['failed', 'holding']:
            continue
        data.append({
            'modificationtime': job['modificationtime'],
            'site': job['computingsite'] if not is_wn_instead_of_site else job['wn'],
            'code': ','.join(sorted(get_job_error_component_code_list(job))),
            'task': str(job['jeditaskid']),
            'user': job['produsername'],
            'request': str(job['reqid']) if 'reqid' in job else 'None',
            'category': get_job_error_category(job, error_descriptions),
        })
        timestamp_list.append(job['modificationtime'])

    freq = calc_freq_time_series(timestamp_list, n_bins_max=60)

    if len(data) > 0:
        df = pd.DataFrame(data)
        df['modificationtime'] = pd.to_datetime(df['modificationtime'])
        df.set_index('modificationtime', inplace=True)

        # Apply the function to each column where you want low-impact values grouped
        for column in plots:
            df = categorize_low_impact_by_percentage(df, column, threshold_percent)

        # Generate JSON-ready data for each column
        output_data = {}
        for column in plots:
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

    fields = ['user', 'site', 'task']
    error_count_dict = {}
    summary_attribute = {}
    summary_field = {}
    for f in fields:
        summary_field[f] = {}

    for job in jobs:
        # attribute summary aggregation
        for f in flist:
            if f not in summary_attribute:
                summary_attribute[f] = defaultdict(int)
            if f == 'specialhandling':
                shl = job['specialhandling'].split(',') if 'specialhandling' in job and job['specialhandling'] is not None else []
                for v in shl:
                    # ignore tq = taskQueuedTime timestamp
                    if v.startswith('tq'):
                        continue
                    if v not in summary_attribute['specialhandling']:
                        summary_attribute['specialhandling'][v] = 0
                    summary_attribute['specialhandling'][v] += 1
            else:
                summary_attribute[f][job[f]] += 1 if job.get(f) else 0


        if not is_test_jobs and job['jobstatus'] not in ['failed', 'holding']:
            continue

        fields_values = {
            'user': job['produsername'],
            'site': job['wn'] if is_site_req else job['computingsite'],
            'task': job.get('jeditaskid') or job.get('taskid') or None
        }
        taskname = tasknamedict.get(fields_values['task'], '')

        for err in error_components:
            errval = job.get(err['error'])
            if not errval or str(errval) == '0' or errval == 0:
                continue
            try:
                errnum = int(errval)
            except ValueError:
                continue
            errcode = f"{err['name']}:{errnum}"
            # for transform errors, we do not have a related diag field, but we added one from ErrorDescriptions already
            errdiag = job.get(err['diag'], '') if err['name'] != 'transform' else job.get('transformerrordiag', '')
            errdesc = job.get(f"{err['name']}_error_desc", '')
            pandaid = job['pandaid']

            if errcode not in error_count_dict:
                error_count_dict[errcode] = create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid)
                error_count_dict[errcode]['pandalist'] = {}
            error_count_dict[errcode]['count'] += 1

            for f in fields_values:
                if f == 'task' and fields_values[f] is None:
                    continue
                if f not in summary_field:
                    summary_field[f] = {}
                if fields_values[f] not in summary_field[f]:
                    summary_field[f][fields_values[f]] = {'name': fields_values[f], 'errors': {}, 'toterrors': 0, 'toterrjobs': 0}
                    if f == 'task':
                        summary_field[f][fields_values[f]]['longname'] = taskname
                if errcode not in summary_field[f][fields_values[f]]['errors']:
                    summary_field[f][fields_values[f]]['errors'][errcode] = create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid)
                summary_field[f][fields_values[f]]['errors'][errcode]['count'] += 1
                summary_field[f][fields_values[f]]['toterrors'] += 1

        # count job with error just once
        if job['jobstatus'] == 'failed':
            for f in fields_values:
                if f in summary_field and fields_values[f] and fields_values[f] in summary_field[f]:
                    summary_field[f][fields_values[f]]['toterrjobs'] += 1

    # Convert summaries to sorted lists
    for f, data in summary_field.items():
        summary_field[f] = to_sorted_list(data, errsort=True, totalkey='toterrors')
    error_count = sorted(error_count_dict.values(), key=lambda x: -x['count'])
    suml = [{
        'field': f,
        'list': sorted(
            [{'kname': k, 'kvalue': v} for k, v in summary_attribute[f].items()],
            key=lambda x: -x['kvalue'] if sortby == 'count' else x['kname']
        )} for f in sorted(summary_attribute)
    ]
    _logger.debug('Dict -> list & sorting are done: {}'.format(time.time() - start_time))

    if 'errsHist' in outputs:
        error_histograms = build_error_histograms(jobs, is_wn_instead_of_site=is_site_req)
    _logger.debug('Built errHist: {}'.format(time.time() - start_time))

    return error_count, summary_field['site'], summary_field['user'], summary_field['task'], suml, error_histograms

