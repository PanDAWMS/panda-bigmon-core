from core.views import initRequest
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
import urllib.request
from urllib.error import HTTPError, URLError
from core.settings.base import IDDS_SERVER_URL
import json

SELECTION_CRITERIA = '/idds/monitor_request_relation'


def query_idds_srver(request_id):
    url = f"{IDDS_SERVER_URL}{SELECTION_CRITERIA}/{request_id}/null"
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
    nodes = []
    edges = []
    if len(stats) > 0:
        relation_map = stats[0]['relation_map']
        if len(relation_map) > 0:
            relation_map = relation_map[0]
            nodes.append({ 'group': 'nodes',
                           'data': { 'id': str(relation_map['work']['workload_id']),
                                     'resolved': 'false'
                                  }
                        })
            edges.append({
              'group': 'edges',
              'data': {
                'id': str(relation_map['work']['workload_id']) + '_to_' + str(relation_map['work']['workload_id']),
                'target': 'collect data',
                'source': 'noop'
                }
            })

    DAG = [
        { 'group': 'nodes', 'data': { 'id': 'noop', 'resolved': 'false' } },
        {
          'group': 'nodes',
          'data': { 'id': 'collect data', 'resolved': 'false' }
        },
        {
          'group': 'nodes',
          'data': { 'id': 'send to waylay', 'resolved': 'false' }
        },
        { 'group': 'nodes', 'data': { 'id': 'send to BB', 'resolved': 'false' } },
        { 'group': 'nodes', 'data': { 'id': 'notify me', 'resolved': 'false' } },
        {
          'group': 'edges',
          'data': {
            'id': 'noop-collect data',
            'target': 'collect data',
            'source': 'noop'
          }
        },
        {
          'group': 'edges',
          'data': {
            'id': 'collect data-send to waylay',
            'target': 'send to waylay',
            'source': 'collect data'
          }
        },
        {
          'group': 'edges',
          'data': {
            'id': 'collect data-send to BB',
            'target': 'send to BB',
            'source': 'collect data'
          }
        },
        {
          'group': 'edges',
          'data': {
            'id': 'send to waylay-notify me',
            'target': 'notify me',
            'source': 'send to waylay'
          }
        },
        {
          'group': 'edges',
          'data': {
            'id': 'send to BB-notify me',
            'target': 'notify me',
            'source': 'send to BB'
          }
        }
    ]

    data = {
        'DAG': DAG
    }
    response = render_to_response('DAGgraph.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response
