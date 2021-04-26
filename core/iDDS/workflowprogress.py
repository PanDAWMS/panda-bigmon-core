import json

from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.http import JsonResponse
from django.template.defaulttags import register
from django.db.models import Q, F

from core.views import initRequest, login_customrequired, DateEncoder
from core.iDDS.models import Transforms, Collections, Requests, Req2transforms, Processings, Contents
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.rawsqlquery import getRequests, getTransforms, getWorkFlowProgressItemized
from core.iDDS.algorithms import generate_requests_summary, parse_request
from core.libs.exlib import lower_dicts_in_list
from django.core.cache import cache

CACHE_TIMEOUT = 20
OI_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

subtitleValue = SubstitleValue()

@login_customrequired
def get_workflow_progress(request):
    initRequest(request)
    workflows = getWorkFlowProgressItemized()

    return JsonResponse(workflows, encoder=DateEncoder, safe=False)

