import logging
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getWorkFlowProgressItemized
from core.libs.exlib import lower_dicts_in_list
from core.libs.task import get_datasets_for_tasklist
import pandas as pd

_logger = logging.getLogger('bigpandamon')

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()


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
    workflows_semi_grouped = []
    if not workflows_items.empty:
        workflows_items.USERNAME.fillna(value='', inplace=True)
        workflows_items.WORKLOAD_ID = workflows_items.WORKLOAD_ID.astype('Int64')
        # workflows_items.PROCESSING_FILES.fillna(value=0, inplace=True)
        workflows_pd = workflows_items.astype({"R_CREATED_AT":str}).groupby(
            ['REQUEST_ID', 'R_STATUS', 'P_STATUS', 'R_NAME', 'USERNAME', 'TRANSFORM_TYPE', 'TRANSFORM_TAG'],
            dropna=False
        ).agg(
            PROCESSING_FILES_SUM=pd.NamedAgg(column="PROCESSING_FILES", aggfunc="sum"),
            PROCESSED_FILES_SUM=pd.NamedAgg(column="PROCESSED_FILES", aggfunc="sum"),
            TOTAL_FILES=pd.NamedAgg(column="TOTAL_FILES", aggfunc="sum"),
            P_STATUS_COUNT=pd.NamedAgg(column="P_STATUS", aggfunc="count"),
            R_CREATED_AT=pd.NamedAgg(column="R_CREATED_AT", aggfunc="first"),
            workload_ids=('WORKLOAD_ID', lambda x: list(x.dropna())),
        ).reset_index()
        # fill NAN with 0 for N files
        workflows_pd.TOTAL_FILES.fillna(value=0, inplace=True)
        workflows_pd.PROCESSING_FILES_SUM.fillna(value=0, inplace=True)
        workflows_pd.PROCESSED_FILES_SUM.fillna(value=0, inplace=True)
        workflows_pd.P_STATUS.fillna(value=0, inplace=True)
        workflows_pd.TRANSFORM_TYPE.fillna(value=99, inplace=True)
        workflows_pd.TRANSFORM_TAG.fillna(value='', inplace=True)
        workflows_pd = workflows_pd.astype({
            "R_STATUS":int,
            "P_STATUS":int,
            "PROCESSING_FILES_SUM": int,
            "PROCESSED_FILES_SUM": int,
            "TOTAL_FILES": int,
            "P_STATUS_COUNT": int,
            "TRANSFORM_TYPE": int,
            "TRANSFORM_TAG": str
        })
        workflows_semi_grouped = workflows_pd.values.tolist()

    workflows = {}
    for workflow_group in workflows_semi_grouped:
        workflow = workflows.setdefault(workflow_group[0], {
            "REQUEST_ID":workflow_group[0],
            "R_STATUS": subtitleValue.substitleValue("requests", "status")[workflow_group[1]] if \
                workflow_group[1] in subtitleValue.substitleValue("requests", "status") else \
                'N/A',
            "CREATED_AT":workflow_group[8],
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
            "TRANSFOMR_TAG": '',
            })
        workflow['TRANSFORM_TYPE'] = subtitleValue.substitleValue("transforms", "type")[workflow_group[5]] if \
            workflow_group[5] in subtitleValue.substitleValue("transforms", "type") else \
            'N/A'
        workflow['TRANSFORM_TAG'] = workflow_group[6]
        workflow['TOTAL_TASKS'] += workflow_group[10]
        workflow['R_NAME'] = workflow_group[3]
        workflow['USERNAME'] = workflow_group[4]
        workflow['CREATED_AT'] = workflow_group[11]
        processing_status_name = subtitleValue.substitleValue("processings", "status")[workflow_group[2]] if \
            workflow_group[2] in subtitleValue.substitleValue("processings", "status") else \
            'N/A'
        workflow["TASKS_STATUSES"][processing_status_name] = workflow_group[10]
        workflow['RELEASED_FILES'] += workflow_group[8]
        workflow['PROCESSING_FILES'] += workflow_group[7]
        workflow['TOTAL_FILES'] += workflow_group[9]
        workflow['UNRELEASED_FILES'] = workflow['TOTAL_FILES'] - workflow['RELEASED_FILES']

        # get data from JEDI of N processed files
        tasks = workflow_group[12]
        tasks = [{'jeditaskid': task} for task in tasks]
        tasks = get_datasets_for_tasklist(tasks)
        for task in tasks:
            for ds in task['datasets']:
                if "input" in ds['type'] and ds['masterid'] is None:
                    workflow['FINISHED_FILES'] += ds["nfilesfinished"]
                    workflow['FAILED_FILES'] += ds["nfilesfailed"]
                    workflow['TOTAL_JEDI_FILES'] += ds["nfiles"]

    # convert PROCESSED_FILES to percentage
    for run, workflow in workflows.items():
        workflow['FINISHED_FILES'] = round(100 * workflow['FINISHED_FILES'] / workflow['TOTAL_JEDI_FILES'], 1) if workflow['TOTAL_JEDI_FILES'] else 0
        workflow['FAILED_FILES'] = round(100 * workflow['FAILED_FILES'] / workflow['TOTAL_JEDI_FILES'], 1) if workflow['TOTAL_JEDI_FILES'] else 0
        workflows[run] = workflow

    workflows = lower_dicts_in_list(list(workflows.values()))
    return workflows




