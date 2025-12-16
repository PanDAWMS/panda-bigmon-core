"""
Set of functions related to job errors parsing and extracting descriptions

Created by Tatiana Korchuganova on 05.03.2020
"""
import logging
from html import escape

from django.core.cache import cache
from django.conf import settings

from core.pandajob.models import Jobsarchived4, Jobsarchived
from core.pandajob.utils import get_job_error_descriptions
from core.libs.exlib import get_tmp_table_name, insert_to_temp_table
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def format_error(component, code, diag=None, output_format='html'):
    """
    Format error code and diagnostic message
    :param component: str, name of the component (e.g., 'brokerage', 'exe', etc.)
    :param code: int, error code
    :param diag: str, diagnostic message (optional)
    :param output_format: str, 'html' or 'str'
    :return: formatted error string
    """
    if diag is None and component == 'transformation':
        diag = f"Unknown transformation exit code {code}"
    elif diag is None:
        diag = ''
    # remove timestamp from diag for lost heartbeat errors
    if 'lost heartbeat' in diag:
        diag = 'lost heartbeat'
    if ' at ' in diag:
        diag = diag[:diag.find(' at ') - 1]
    diag = diag.replace('\n', ' ')

    #  format the error string based on the output format
    if output_format == 'html':
        error_str = f"<b>{component}:{code}</b> {escape(diag, quote=True)} "
    else:
        error_str = f"{component}:{code} {diag} "
    return error_str



def add_error_info_to_job(job, n_chars=300, mode='html', do_add_desc=False, error_desc=None):
    """
    Concat all error codes and diagnostics into a single string and add to job dict
    :param job: dict
    :param n_chars: int, max length of the error string to return
    :param mode: str, 'html' or 'string'
    :param do_add_desc: bool, if True, add long description of errors
    :param error_desc: dict, if provided, use this instead of fetching from cache
    :return: job: dict with 'error_info' key added
    """
    error_info_str = ''
    error_desc_str = ''
    if error_desc is None:
        error_desc = get_job_error_descriptions()

    for comp in const.JOB_ERROR_COMPONENTS:
        if (comp['error'] in job and job[comp['error']] != '' and job[comp['error']] is not None
                and int(job[comp['error']]) != 0):
            # there is no error diag for transformations, get it from error_desc
            comp_code_str = f"{comp['name']}:{job[comp['error']]}"
            if comp['name'] == 'transform':
                if comp_code_str in error_desc:
                    diag = error_desc[comp_code_str]['diagnostics']
                else:
                    diag = f"Unknown transformation exit code {job[comp['error']]}"
                job['transformerrordiag'] = diag
            else:
                diag = job[comp['diag']]

            # taskbuffer and supervisor error diagnostics almost the same, so skip taskbuffer
            if comp['name'] == 'taskbuffer' and 'superrorcode' in job and job['superrorcode'] and int(job['superrorcode']) != 0:
                continue
            error_info_str += format_error(
                comp['name'],
                job[comp['error']],
                diag,
                output_format=mode
            )
            # longer LLM generated error description
            if do_add_desc:
                job[f"{comp['name']}_error_desc"] = error_desc[comp_code_str]['description'] if comp_code_str in error_desc else ''
                if len(job[f"{comp['name']}_error_desc"]) > 0:
                    error_key = f"{comp['name']}_error_desc"
                    error_desc_str += f"{comp_code_str} - {job[error_key]} <br>"

    if mode == 'str' and len(error_info_str) > n_chars:
        error_info_str = error_info_str[:n_chars] + '...'

    job['errorinfo'] = error_info_str  # keep it as it is used by API clients
    job['error_desc'] = error_desc_str
    return job


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
        'superrorcode', 'superrordiag',
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

    error_desc = get_job_error_descriptions()
    for job in jobs:
        job_with_error_info = add_error_info_to_job(job, n_chars=1000, mode='str', do_add_desc=False, error_desc=error_desc)
        errors_dict[job['pandaid']] = job_with_error_info['errorinfo']

    return errors_dict


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
        return const.ERROR_CATEGORIES[str(error_categories[0])] if str(error_categories[0]) in const.ERROR_CATEGORIES else 'Uncategorized'
    else:
        _logger.debug(f"Multiple error categories found for job {job['pandaid']}: {error_categories}. Defaulting to 'Uncategorized'.")
        return f"Uncategorized ({','.join([str(c) for c in error_categories if c != 0])})"


def top_errors_summary(jobs, n_top=3) -> dict:
    """
    Get a summary of the top errors with descriptions and categories
    :param jobs: list of dicts
    :param n_top: int, number of top errors to return
    :return:
    """
    if not jobs or len(jobs) == 0:
        return []

    error_desc = get_job_error_descriptions()

    # errors by category
    n_failed_jobs_total = 0
    err_cat_sum = {}
    for job in jobs:
        if job['jobstatus'] != 'failed':
            continue
        n_failed_jobs_total += 1
        err_cat = get_job_error_category(job, error_descriptions=error_desc)
        if err_cat not in err_cat_sum:
            err_cat_sum[err_cat] = {'count': 0, 'sites': set(), 'codes': {}}
        err_cat_sum[err_cat]['count'] += 1
        err_cat_sum[err_cat]['sites'].add(job['computingsite'])

        err_comp_code_list = get_job_error_component_code_list(job)
        err_comp_code = ','.join(err_comp_code_list)
        if err_comp_code not in err_cat_sum[err_cat]['codes']:
            err_cat_sum[err_cat]['codes'][err_comp_code] = {'count': 0, 'diag': '', 'desc': ''}
            # special case for payload error, we do not show other component codes to avoid confusion
            if 'pilot:1305' in err_comp_code_list:
                err_cat_sum[err_cat]['codes'][err_comp_code]['diag'] = error_desc['pilot:1305']['diagnostics']
                err_cat_sum[err_cat]['codes'][err_comp_code]['desc'] = error_desc['pilot:1305']['description']
            else:
                for comp_code in err_comp_code_list:
                    if comp_code in error_desc:
                        err_cat_sum[err_cat]['codes'][err_comp_code]['diag'] += f" {comp_code} {error_desc[comp_code]['diagnostics']}"
                        err_cat_sum[err_cat]['codes'][err_comp_code]['desc'] += f" {comp_code} {error_desc[comp_code]['description']}"

        err_cat_sum[err_cat]['codes'][err_comp_code]['count'] += 1

    for cat in err_cat_sum:
        err_cat_sum[cat]['sites'] = sorted(list(err_cat_sum[cat]['sites']))
        # sort error codes by count and keep only top n_top codes
        err_cat_sum[cat]['codes'] = sorted(err_cat_sum[cat]['codes'].items(), key=lambda x: -x[1]['count'])
        if len(err_cat_sum[cat]['codes']) > n_top:
            err_cat_sum[cat]['codes'] = err_cat_sum[cat]['codes'][:n_top]

    return err_cat_sum





