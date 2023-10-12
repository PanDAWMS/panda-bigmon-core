import json

from django.conf import settings
from django.shortcuts import render
from core.oauth.utils import login_customrequired
from core.views import initRequest
from django.http import HttpResponse, JsonResponse
from elasticsearch_dsl import Search
from core.libs.elasticsearch import create_es_connection, get_es_task_status_log
from core.libs.DateEncoder import DateEncoder

@login_customrequired
def testTerminal(request,):
    valid, response = initRequest(request)
    return render(request, 'testTerminal.html', context={'text':'Test terminal'})
@login_customrequired
def taskLivePage(request, jeditaskid):
    valid, response = initRequest(request)
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        db_source = 'atlas'
    elif settings.DEPLOYMENT == 'ORACLE_DOMA':
        db_source = 'doma'
    db_source = 'doma' # TODO removed it

    data = {
        'db_source': db_source,
        'jeditaskid': jeditaskid
    }

    # archived_messages, task_message_ids_list, jobs_info_status_dict = get_es_task_status_log(db_source=db_source, jeditaskid=jeditaskid)
    # for key, value in jobs_info_status_dict.items():
    #     max_timestamp_object = max(value.values(), key=lambda item: item['timestamp'])
    #     print(max_timestamp_object)
    #
    # if len(archived_messages) > 0:
    #     data['archived_messages'] = archived_messages
    # else:
    #     data['archived_messages'] = 0

    data['archived_messages'] = 0
    return render(request, 'taskLivePage.html', data, content_type='text/html')