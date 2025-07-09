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
