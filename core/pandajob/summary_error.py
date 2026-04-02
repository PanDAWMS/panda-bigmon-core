import copy
import logging
import pandas as pd
import time
from collections import defaultdict

from core.libs.task import taskNameDict
from core.libs.job import get_job_walltime
from core.libs.error import get_job_error_category, get_job_error_component_code_list
from core.libs.exlib import calc_freq_time_series

from django.conf import settings
import core.constants as const
from core.pandajob.utils import get_job_error_descriptions, get_errors_per_category

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
            if column == 'category':
                continue
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


def create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid, errcat):
    """Create a structured error entry for the error summary."""
    return {
        'error': errcode,
        'codename': err['error'],
        'codeval': errnum,
        'diag': errdiag,
        'desc': errdesc,
        'cat': {"name": const.ERROR_CATEGORIES.get(str(errcat), 'Uncategorized'), "id": errcat},
        'example_pandaid': pandaid,
        'count': 0
    }


def to_sorted_list(d: dict, errsort=False, totalkey=None) -> list:
    """Convert a dictionary to a sorted list of dictionaries."""
    out = []
    for key in sorted(d):
        item = d[key]
        item['errorlist'] = sorted(item['errors'].values(), key=lambda x: -x['count']) if errsort else list(item['errors'].values())
        out.append(item)
    if totalkey:
        out = sorted(out, key=lambda x: -x[totalkey])
    return out


def errorSummaryDict(jobs, is_test_jobs=False, sortby='count', is_user_req=False, is_site_req=False, category=-1, **kwargs):
    """
    Takes a job list and produce error summaries from it
    :param jobs: list of dicts
    :param is_test_jobs: bool: for test jobs we do not limit to "failed" jobs only
    :param sortby: str: count or alpha
    :param is_user_req: bool: we do jeditaskid in attribute summary only if a user is specified
    :param is_site_req: bool: we do summary per worker node if True
    :param category: int: if specified, only consider errors of this category (use -1 for all categories)
    :param kwargs: flist and outputs
    :return: errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, error_histograms, error_msg_summary
    """
    start_time = time.time()
    flist = kwargs.get('flist', const.JOB_FIELDS_ERROR_VIEW)
    if is_user_req is not None and 'jeditaskid' in flist:
        flist = [f for f in flist if f != 'jeditaskid']
    outputs = kwargs.get('output', ['errsByCount', 'errsBySite', 'errsByUser', 'errsByTask', 'errsHist', 'errsByMessage'])

    N_SAMPLE_JOBS = 3
    fields = ['user', 'site', 'task']
    error_count_dict = {}
    summary_attribute = {f: defaultdict(int) for f in flist}
    summary_field = {f: {} for f in fields}
    error_histograms = {}
    error_message_summary = {}

    error_components = const.JOB_ERROR_COMPONENTS
    error_descriptions = get_job_error_descriptions()
    error_codes_per_category = get_errors_per_category(category, error_descriptions=error_descriptions)
    errname2dbfield = {comp['name']: comp['error'] for comp in error_components}
    _logger.debug('Got error desc and categories: {}'.format(time.time() - start_time))

    for job in jobs:
        # attribute summary aggregation
        for f in flist:
            val = job.get(f)
            if val is None:
                continue
            if f == 'specialhandling':
                for v in val.split(','):
                    if not v.startswith('tq'):
                        summary_attribute[f][v] += 1
            else:
                summary_attribute[f][val] += 1

        status = job.get('jobstatus')
        if not is_test_jobs and status not in ('failed', 'holding'):
            continue

        fields_values = {
            'user': job.get('produsername', ''),
            'site': job.get('wn') if is_site_req else job.get('computingsite', ''),
            'task': job.get('jeditaskid') or job.get('taskid')
        }
        pandaid = job.get('pandaid')

        for err in error_components:
            err_field = err['error']
            errval = job.get(err_field)

            # skip if error code is 0, None, or empty string
            if not errval or str(errval) == '0':
                continue
            errnum = int(errval)
            if errnum <= 0:
                continue

            err_name = err['name']
            errcode = f"{err_name}:{errnum}"
            errdiag = job.get(err['diag'], '') if err['name'] != 'transform' else job.get('transformerrordiag', '')
            errdesc = job.get(f"{err['name']}_error_desc", '')
            err_desc_obj = error_descriptions.get(errcode, {})
            errcat = err_desc_obj.get('category', 0)

            if category >= 0:
                if category != errcat:
                    continue
                if err_name in error_codes_per_category and errnum not in error_codes_per_category[err_name]:
                    continue

            if errcode not in error_count_dict:
                error_count_dict[errcode] = create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid, errcat)
            error_count_dict[errcode]['count'] += 1

            for f, f_val in fields_values.items():
                if f_val is None:
                    continue
                f_dict = summary_field[f]
                if f_val not in f_dict:
                    f_dict[f_val] = {'name': f_val, 'errors': {}, 'toterrors': 0, 'toterrjobs': 0}

                target = f_dict[f_val]
                if errcode not in target['errors']:
                    target['errors'][errcode] = create_error_entry(errcode, err, errnum, errdiag, errdesc, pandaid, errcat)
                target['errors'][errcode]['count'] += 1
                target['toterrors'] += 1

            if 'errsByMessage' in outputs:
                if errcode not in error_message_summary:
                    error_message_summary[errcode] = {'count': 0, 'messages': {}, 'cat': errcat}

                cur_err_msg = error_message_summary[errcode]
                cur_err_msg['count'] += 1

                if errdiag not in cur_err_msg['messages']:
                    cur_err_msg['messages'][errdiag] = {'count': 0, 'pandaids': []}

                msg_info = cur_err_msg['messages'][errdiag]
                msg_info['count'] += 1
                if len(msg_info['pandaids']) < N_SAMPLE_JOBS:
                    msg_info['pandaids'].append(pandaid)

        if status == 'failed':
            for f, f_val in fields_values.items():
                if f_val and f_val in summary_field[f]:
                    summary_field[f][f_val]['toterrjobs'] += 1
    _logger.debug('Error summaries are done: {}'.format(time.time() - start_time))

    if 'errsHist' in outputs:
        error_histograms = build_error_histograms(jobs, is_wn_instead_of_site=is_site_req)
        _logger.debug('Built errHist: {}'.format(time.time() - start_time))

    # dict -> list for datatables
    error_msg_summary = []
    for comp_code, err_info in error_message_summary.items():
        comp_name, code = comp_code.split(':')

        for mgs, msg_info in err_info['messages'].items():
            error_msg_summary.append({
                'errcode': comp_code,
                'errcodename': errname2dbfield.get(comp_name, ''),
                'errcodeval': code,
                'errcodecount': err_info['count'],
                'errcat': err_info['cat'],
                'errcatname': const.ERROR_CATEGORIES.get(str(err_info['cat']), 'Uncategorized'),
                'errmessage': mgs,
                'errmessagecount': msg_info['count'],
                'pandaids': msg_info['pandaids']
            })

    for f in summary_field:
        summary_field[f] = to_sorted_list(summary_field[f], errsort=True, totalkey='toterrors')

    error_count = sorted(error_count_dict.values(), key=lambda x: -x['count'])
    suml = [{
        'field': f,
        'list': sorted([
            {'kname': k, 'kvalue': v} for k, v in summary_attribute[f].items()
        ], key=lambda x: -x['kvalue'] if sortby == 'count' else x['kname'])
    } for f in sorted(summary_attribute)]
    _logger.debug('Dict -> list & sorting are done: {}'.format(time.time() - start_time))
    return error_count, summary_field['site'], summary_field['user'], summary_field['task'], suml, error_histograms, error_msg_summary


