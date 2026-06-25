import logging
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getWorkFlowProgressItemized
from core.libs.exlib import lower_dicts_in_list, round_to_n_digits
from core.libs.task import get_datasets_for_tasklist
import pandas as pd

_logger = logging.getLogger('bigpandamon')

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()

def percentile(x, ndigits=0, rmethod='normal'):
    result = round_to_n_digits(x*100, n=ndigits, method=rmethod)
    if (x > 0 and x < 0.01) or (x > 0.99 and x < 1):
        while int(list(str(result))[-1]) == 0:
            ndigits += 1
            result = round_to_n_digits(x*100, n=ndigits, method=rmethod)
    return result


def prepare_requests_summary(workflows):
    summary = {'status': {}, 'username': {}}
    """
    completion
    age
    """
    for workflow in workflows:
        summary['status'][workflow['r_status']] = summary['status'].get(workflow['r_status'], 0) + 1
        if workflow['username'] == '':
            workflow['username'] = "Not set"
        summary['username'][workflow['username']] = summary['username'].get(workflow['username'], 0) + 1
    return summary


def get_workflow_progress_data(request_params, **kwargs):
    workflows_items = getWorkFlowProgressItemized(request_params, **kwargs)
    workflows_items = pd.DataFrame(workflows_items)
    # get datasets for all tasks in workflows to calculate percentage of finished files from JEDI
    workflows_datasets_all_list = get_datasets_for_tasklist(
        [{'jeditaskid': int(task)} for task in workflows_items.WORKLOAD_ID.dropna().unique()]
    )
    workflows_datasets_all_dict = {task['jeditaskid']: task['datasets'] for task in workflows_datasets_all_list}
    workflows_semi_grouped = []
    if not workflows_items.empty:
        # clean up fields before grouping
        fillna_fields = {
            'USERNAME': '', 'R_NAME': '', 'CLOUD': '', 'SITE': '', 'TRANSFORM_TAG': '', 'CAMPAIGN': '',
            'P_STATUS': 0, 'TRANSFORM_TYPE': 99}
        workflows_items = workflows_items.fillna(fillna_fields).astype({
            'R_STATUS': int, 'TRANSFORM_TAG': str, 'P_STATUS': int, 'TRANSFORM_TYPE': int, 'WORKLOAD_ID': 'Int64', 'CAMPAIGN': str,
        })
        workflows_items['R_CREATED_AT'] = pd.to_datetime(workflows_items['R_CREATED_AT']).dt.strftime('%Y-%m-%d %H:%M:%S')
        workflows_pd = workflows_items.groupby(
            ['REQUEST_ID', 'R_STATUS', 'P_STATUS', 'R_NAME', 'CLOUD', 'SITE', 'USERNAME', 'TRANSFORM_TYPE', 'TRANSFORM_TAG', 'CAMPAIGN'],
            dropna=False
        ).agg(
            PROCESSING_FILES_SUM=('PROCESSING_FILES', 'sum'),
            PROCESSED_FILES_SUM=('PROCESSED_FILES', 'sum'),
            TOTAL_FILES=('TOTAL_FILES', 'sum'),
            P_STATUS_COUNT=('P_STATUS', 'count'),
            R_CREATED_AT=('R_CREATED_AT', 'first'),
            workload_ids=('WORKLOAD_ID', lambda x: list(x.dropna())),
        ).reset_index()
        # clean new fields after grouping
        field_defaults = {'TOTAL_FILES': 0, 'PROCESSING_FILES_SUM': 0, 'PROCESSED_FILES_SUM': 0}
        workflows_pd = workflows_pd.fillna(field_defaults).astype({
            "PROCESSING_FILES_SUM": int,"PROCESSED_FILES_SUM": int, "TOTAL_FILES": int, "P_STATUS_COUNT": int
        })
        workflows_semi_grouped = workflows_pd.to_dict('records')

    # get constants for statuses and types
    r_status_to_str = subtitleValue.substitleValue("requests", "status")
    t_type_to_str = subtitleValue.substitleValue("transforms", "type")
    p_status_to_str = subtitleValue.substitleValue("processings", "status")
    workflows = {}
    for row in workflows_semi_grouped:
        req_id = row['REQUEST_ID']
        workflow = workflows.setdefault(req_id, {
            "REQUEST_ID": req_id,
            "R_STATUS": r_status_to_str.get(row['R_STATUS'], 'N/A'),
            "CREATED_AT": row['R_CREATED_AT'],
            "TOTAL_TASKS": 0,
            "TASKS_STATUSES": {},
            "FINISHED_FILES": 0,
            "FAILED_FILES": 0,
            "UNRELEASED_FILES": 0,
            "RELEASED_FILES": 0,
            "PROCESSING_FILES": 0,
            "TOTAL_FILES": 0,
            "TOTAL_JEDI_FILES": 0,
            "TRANSFORM_TYPE": 99,
            "TRANSFORM_TAG": '',
            "CAMPAIGN": '',
        })
        workflow['TRANSFORM_TYPE'] = t_type_to_str.get(row['TRANSFORM_TYPE'], 'N/A')
        workflow['TRANSFORM_TAG'] = row['TRANSFORM_TAG']
        workflow['TOTAL_TASKS'] += row['P_STATUS_COUNT']
        workflow['R_NAME'] = row['R_NAME']
        workflow['CLOUD'] = row['CLOUD']
        workflow['SITE'] = row['SITE']
        workflow['CAMPAIGN'] = row['CAMPAIGN'] if row['TRANSFORM_TYPE'] != 2 else '-'
        workflow['USERNAME'] = row['USERNAME']
        workflow['CREATED_AT'] = row['R_CREATED_AT']
        workflow["TASKS_STATUSES"][p_status_to_str.get(row['P_STATUS'], 'N/A')] = row['P_STATUS_COUNT']
        workflow['RELEASED_FILES'] += row['PROCESSED_FILES_SUM']
        workflow['PROCESSING_FILES'] += row['PROCESSING_FILES_SUM']
        workflow['TOTAL_FILES'] += row['TOTAL_FILES']
        workflow['UNRELEASED_FILES'] = workflow['TOTAL_FILES'] - workflow['RELEASED_FILES']

        # add info from JEDI datasets
        workload_ids = row['workload_ids']
        if workload_ids:
            for tid in workload_ids:
                if tid in workflows_datasets_all_dict:
                    for ds in workflows_datasets_all_dict[tid]:
                        if "input" in ds['type'] and ds['masterid'] is None and ds.get('status', '') != 'removed':
                            workflow['FINISHED_FILES'] += ds["nfilesfinished"]
                            workflow['FAILED_FILES'] += ds["nfilesfailed"]
                            workflow['TOTAL_JEDI_FILES'] += ds["nfiles"]

    # calculate percentages
    for run, workflow in workflows.items():
        total_jedi = workflow['TOTAL_JEDI_FILES']
        workflow['FINISHED_FILES'] = percentile(workflow['FINISHED_FILES'] / total_jedi) if total_jedi else 0
        workflow['FAILED_FILES'] = percentile(workflow['FAILED_FILES'] / total_jedi) if total_jedi else 0

    workflows = lower_dicts_in_list(list(workflows.values()))
    return workflows




