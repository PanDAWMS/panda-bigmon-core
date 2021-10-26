from core.views import initRequest
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
import urllib.request
from urllib.error import HTTPError, URLError
import json

BASE_URL = 'https://aipanda160.cern.ch:443'
SELECTION_CRITERIA = '/idds/monitor_request_relation'


def query_idds_srver(request_id):
    url = f"{BASE_URL}{SELECTION_CRITERIA}/{request_id}/null"
    try:
        response = urllib.request.urlopen(url).read()
    except (HTTPError, URLError) as e:
        print('Error: {}'.format(e.reason))
    stats = json.loads(response)
    return stats

def daggraph(request):
    initRequest(request)
    requestid = int(request.session['requestParams']['requestid'])
    stats = query_idds_srver(requestid)
    response = render_to_response('DAGgraph.html', {}, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
