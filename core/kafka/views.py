import json

from django.conf import settings
from django.shortcuts import render
from core.oauth.utils import login_customrequired
from core.views import initRequest
from django.http import HttpResponse, JsonResponse
from opensearchpy import Search
from core.libs.elasticsearch import create_os_connection, get_os_task_status_log
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
        request.session['full_hostname'] = 'panda-doma.cern.ch'

    data = {
        'db_source': db_source,
        'jeditaskid': jeditaskid
    }

    return render(request, 'taskLivePage.html', data, content_type='text/html')