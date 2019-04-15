import json, collections, random

from collections import OrderedDict, Counter

from datetime import datetime, timedelta

from django.db import connection
from django.db.models import Count

from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect

from django.utils.cache import patch_cache_control, patch_response_headers
from django.utils import timezone

from core.libs.cache import setCacheEntry, getCacheEntry
from core.libs.exlib import is_timestamp

from core.views import login_customrequired, initRequest, setupView, endSelfMonitor, escapeInput, DateEncoder, extensibleURL, DateTimeEncoder

from core.settings.local import dbaccess, defaultDatetimeFormat

from core.grafana.Grafana import Grafana
from core.grafana.Query import Query
from core.grafana.Headers import Headers

def grafana_api(request):
    valid, response = initRequest(request)

    result = []

    q = Query()
    q = q.request_to_query(request)
    try:
        result = Grafana().get_data(q)
    except Exception as ex:
        result.append(ex)

    return HttpResponse(json.dumps(result, cls=DateTimeEncoder), content_type='text/html')


