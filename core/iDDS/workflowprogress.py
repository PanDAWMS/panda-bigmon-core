from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.http import JsonResponse

from core.views import initRequest, login_customrequired, DateEncoder
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getRequests, getTransforms, getWorkFlowProgressItemized
from core.iDDS.algorithms import generate_requests_summary, parse_request
from core.libs.exlib import lower_dicts_in_list
import pandas as pd


CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()

def get_workflow_progress_data(request_params):
    workflows_items = getWorkFlowProgressItemized(request_params)
    workflows_items = pd.DataFrame(workflows_items)
    workflows_semi_grouped = []
    if not workflows_items.empty:
        workflows_items.USERNAME.fillna(value='', inplace=True)
        workflows_pd = workflows_items.astype({"WORKLOAD_ID":str}).astype({"R_CREATED_AT":str}).groupby(['REQUEST_ID', 'R_STATUS', 'P_STATUS', 'R_NAME', 'USERNAME']).agg(
            PROCESSING_FILES_SUM=pd.NamedAgg(column="PROCESSING_FILES", aggfunc="sum"),
            PROCESSED_FILES_SUM=pd.NamedAgg(column="PROCESSED_FILES", aggfunc="sum"),
            TOTAL_FILES=pd.NamedAgg(column="TOTAL_FILES", aggfunc="sum"),
            P_STATUS_COUNT=pd.NamedAgg(column="P_STATUS", aggfunc="count"),
            R_CREATED_AT=pd.NamedAgg(column="R_CREATED_AT", aggfunc="first"),
            workload_ids=('WORKLOAD_ID', lambda x: '|'.join(x)),
        ).reset_index()
        workflows_pd = workflows_pd.astype({"R_STATUS":int, 'P_STATUS':int, "PROCESSING_FILES_SUM": int,
                                            "PROCESSED_FILES_SUM": int, "TOTAL_FILES": int, "P_STATUS_COUNT": int})
        workflows_semi_grouped = workflows_pd.values.tolist()
    workflows = {}
    for workflow_group in workflows_semi_grouped:
        workflow = workflows.setdefault(workflow_group[0], {
            "REQUEST_ID":workflow_group[0], "R_STATUS": subtitleValue.substitleValue("requests", "status")[workflow_group[1]], "CREATED_AT":workflow_group[8],"TOTAL_TASKS":0,
            "TASKS_STATUSES":{}, "TASKS_LINKS":{}, "REMAINING_FILES":0,"PROCESSED_FILES":0,"PROCESSING_FILES":0,
            "TOTAL_FILES":0})
        workflow['TOTAL_TASKS'] += workflow_group[8]
        workflow['R_NAME'] = workflow_group[3]
        workflow['USER'] = workflow_group[4]
        workflow['CREATED_AT'] = workflow_group[9]
        processing_status_name = subtitleValue.substitleValue("processings", "status")[workflow_group[2]]
        workflow["TASKS_STATUSES"][processing_status_name] = workflow_group[8]
        workflow["TASKS_LINKS"][processing_status_name] = workflow_group[10].replace('.0','')
        workflow['PROCESSED_FILES'] += workflow_group[6]
        workflow['PROCESSING_FILES'] += workflow_group[5]
        workflow['TOTAL_FILES'] += workflow_group[7]
        workflow['REMAINING_FILES'] = workflow['TOTAL_FILES'] - workflow['PROCESSED_FILES']
    workflows = lower_dicts_in_list(list(workflows.values()))
    #return JsonResponse(workflows, encoder=DateEncoder, safe=False)
    return workflows


@login_customrequired
def wfprogress(request):
    initRequest(request)
    iDDSrequests = get_workflow_progress_data(request.session['requestParams'])
    if (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json'))) or (
        'json' in request.session['requestParams']):
        return JsonResponse(iDDSrequests, encoder=DateEncoder, safe=False)

    data = {
        'iDDSrequests':iDDSrequests,
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }
    response = render_to_response('workflows.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


