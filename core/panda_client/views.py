import os, sys
from requests import get, post
from django.http import HttpResponse
from django.views.decorators.cache import never_cache

from core.oauth.utils import login_customrequired
from core.panda_client.utils import get_auth_indigoiam
from core.views import initRequest

baseURL = 'https://pandaserver.cern.ch/server/panda'
@login_customrequired
@never_cache
def get_pandaserver_atter(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    auth = get_auth_indigoiam(request)

    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        data = {}
        url = baseURL + '/getAtter'
        resp = post(url, headers=auth, data=data)
        resp = resp.text
    else:
        resp = auth['detail']

    return HttpResponse(resp, content_type='application/json')

# kill task
@login_customrequired
@never_cache
def killTask(request):
    valid, response = initRequest(request)
    if not valid:
        return response

    """Kill a task

        request parameters:
           jediTaskID: jediTaskID of the task to be killed
    """
    auth = get_auth_indigoiam(request)

    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if 'jeditaskid' in request:
            data = {'jediTaskID': request['jeditaskid']}
            data['properErrorCode'] = True

            url = baseURL + '/killTask'

            try:
                resp = post(url, headers=auth, data=data)
            except Exception as ex:
                resp = "ERROR killTasl: %s %s" % (ex, resp.status_code)
        else:
            resp = 'jeditaskid is not defined'
    else:
        resp = auth['detail']

    return HttpResponse(resp, content_type='application/json')

@login_customrequired
@never_cache
def finishTask(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    """Finish a task

       request parameters:
           jediTaskID: jediTaskID of the task to be finished
           soft: If True, new jobs are not generated and the task is
                 finihsed once all remaining jobs are done.
                 If False, all remaining jobs are killed and then the
                 task is finished
    """
    auth = get_auth_indigoiam(request)

    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if 'jeditaskid' in request:
            data = {'jediTaskID': request['jeditaskid']}
            data['properErrorCode'] = True
            if 'soft' in request:
                data['soft'] = True
            else:
                data['soft'] = False
            url = baseURL + '/finishTask'

            try:
                resp = post(url, headers=auth, data=data)
            except Exception as ex:
                resp = "ERROR finishTask: %s %s" % (ex, resp.status_code)
        else:
            resp = 'jeditaskid is not defined'
    else:
        resp = auth['detail']

    return HttpResponse(resp, content_type='application/json')