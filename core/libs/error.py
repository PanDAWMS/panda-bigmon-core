"""
Set of functions related to job errors parsing and extracting descriptions

Created by Tatiana Korchuganova on 05.03.2020
"""
import logging
from html import escape

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


def get_error_filed_from_component(component):
    """
    Get error code field name and diagnostic field name from component name
    :param component: str, name of the component (e.g., 'brokerage', 'exe', etc.)
    :return: tuple of (error_code_field, diag_field)
    """
    for comp in const.JOB_ERROR_COMPONENTS:
        if comp['name'] == component:
            return comp['error'], comp['diag']
    return None, None


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
    job['error_category'] = get_job_error_category(job, error_descriptions=error_desc, output_format='str')
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

    # remove less priority errors like taskbuffer:300 and dataservice:200
    for err in ['taskbuffer:300', 'dataservice:200']:
        if err in error_list and len(error_list) > 1:
            error_list.remove(err)
    if 'supervisor:8970' in error_list and len(error_list) > 1:
        error_list.remove('supervisor:8970')
    # special case for payload error, we do not show other component codes to avoid confusion
    for pilot_err in ('pilot:1305', 'pilot:1235'):
        if pilot_err in error_list:
            error_list = [pilot_err]

    return error_list


def get_job_error_category(job, error_descriptions=None, output_format='str'):
    """
    Get error category for a job
    :param job: dict, job data
    :param error_descriptions: dict, error descriptions mapping
    :param output_format: str, 'str' or 'int', determines the format of the returned category name
    :return: str or int
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
    if len(error_categories) > 1 and 6 in error_categories:
        # if unknown error category is present but there are other categories, we remove unknown category from the list
        error_categories.remove(6)

    if len(error_categories) == 1:
        category = error_categories[0]
    else:
        _logger.debug(f"Multiple error categories found for job {job['pandaid']}: {error_categories}. Defaulting to 'Uncategorized'.")
        category = 0

    if output_format == 'str':
        category = const.ERROR_CATEGORIES[str(category)] if str(category) in const.ERROR_CATEGORIES else 'Uncategorized'
    return category


def top_errors_summary(jobs, n_top=3, error_descriptions=None) -> dict:
    """
    Get a summary of the top errors with descriptions and categories
    :param jobs: list of dicts
    :param n_top: int, number of top errors to return
    :param error_descriptions: dict, error descriptions
    :return:
    """
    if not jobs or len(jobs) == 0:
        return []
    if not error_descriptions:
        error_descriptions = get_job_error_descriptions()

    # errors by category
    n_failed_jobs_total = 0
    err_cat_sum = {}
    for job in jobs:
        if job['jobstatus'] != 'failed':
            continue
        n_failed_jobs_total += 1
        err_cat_i = get_job_error_category(job, error_descriptions=error_descriptions, output_format='int')
        err_cat = const.ERROR_CATEGORIES[str(err_cat_i)] if str(err_cat_i) in const.ERROR_CATEGORIES else 'Uncategorized'
        if err_cat not in err_cat_sum:
            err_cat_sum[err_cat] = {'count': 0, 'id': err_cat_i, 'sites': set(), 'codes': {}}
        err_cat_sum[err_cat]['count'] += 1
        err_cat_sum[err_cat]['sites'].add(job['computingsite'])

        err_comp_code_list = get_job_error_component_code_list(job)
        err_comp_code = ','.join(err_comp_code_list)
        if err_comp_code not in err_cat_sum[err_cat]['codes']:
            _, err_field_diag = get_error_filed_from_component(err_comp_code.split(':')[0])
            err_cat_sum[err_cat]['codes'][err_comp_code] = {'count': 0, 'diag': '', 'desc': '', 'examples': []}
            for comp_code in err_comp_code_list:
                if comp_code in error_descriptions:
                    err_cat_sum[err_cat]['codes'][err_comp_code]['diag'] += f" {comp_code} {error_descriptions[comp_code]['diagnostics']}"
                    err_cat_sum[err_cat]['codes'][err_comp_code]['desc'] += f" {comp_code} {error_descriptions[comp_code]['description']}"
                elif err_field_diag in job and job[err_field_diag] is not None and job[err_field_diag] != '' and len(job[err_field_diag]) > 0:
                    err_cat_sum[err_cat]['codes'][err_comp_code]['diag'] += f" {comp_code} {job[err_field_diag]}"
        err_cat_sum[err_cat]['codes'][err_comp_code]['count'] += 1
        if len(err_cat_sum[err_cat]['codes'][err_comp_code]['examples']) < 3:
            err_cat_sum[err_cat]['codes'][err_comp_code]['examples'].append(job['pandaid'])

    # dict -> list of dicts & sort
    err_cat_sum_list = []
    for cat in err_cat_sum:
        err_cat_sum[cat]['sites'] = sorted(list(err_cat_sum[cat]['sites']))
        # keep only top n_top codes
        err_cat_sum[cat]['codes'] = sorted(err_cat_sum[cat]['codes'].items(), key=lambda x: -x[1]['count'])
        if len(err_cat_sum[cat]['codes']) > n_top:
            err_cat_sum[cat]['codes'] = err_cat_sum[cat]['codes'][:n_top]
        cat_codes = []
        for code_info_tuple in err_cat_sum[cat]['codes']:
            code_info_tuple[1]['comp_code'] = code_info_tuple[0]
            cat_codes.append(code_info_tuple[1])
        err_cat_sum[cat]['codes'] = sorted(cat_codes, key=lambda x: -x['count'])
        err_cat_sum_list.append({
            'name': cat,
            'id': err_cat_sum[cat]['id'],
            'desc': const.ERROR_CATEGORY_DESCRIPTIONS.get(str(err_cat_sum[cat]['id']), ''),
            'count': err_cat_sum[cat]['count'],
            'sites': err_cat_sum[cat]['sites'],
            'codes': err_cat_sum[cat]['codes']
        })
    err_cat_sum_list = sorted(err_cat_sum_list, key=lambda x: -x['count'])
    return err_cat_sum_list


def error_category_summary_by_task(jobs, error_descriptions=None) -> dict:
    """
    Get a summary of errors by task
    :param jobs: list of dicts
    :param error_descriptions: dict, error descriptions
    :return:
    """
    if not jobs or len(jobs) == 0:
        return {}
    if not error_descriptions:
        error_descriptions = get_job_error_descriptions()


    jobs_failed_per_task = {}
    for job in jobs:
        if job['jobstatus'] != 'failed':
            continue
        if 'jeditaskid' in job and job['jeditaskid']:
            tid = job['jeditaskid']
        else:
            continue
        if tid not in jobs_failed_per_task:
            jobs_failed_per_task[tid] = []
        jobs_failed_per_task[tid].append(job)

    err_task_sum = {}
    for tid in jobs_failed_per_task:
        err_task_sum[tid] = top_errors_summary(jobs_failed_per_task[tid], n_top=3, error_descriptions=error_descriptions)

    return err_task_sum




