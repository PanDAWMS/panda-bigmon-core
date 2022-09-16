import json
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
def get_pandaserver_attr(request):
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
    """Kill a task

        request parameters:
           jediTaskID: jediTaskID of the task to be killed
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    auth = get_auth_indigoiam(request)

    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid'] is not None:
            data = {}

            data['jediTaskID'] = request.session['requestParams']['jeditaskid']
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
    """Finish a task

       request parameters:
           jediTaskID: jediTaskID of the task to be finished
           soft: If True, new jobs are not generated and the task is
                 finihsed once all remaining jobs are done.
                 If False, all remaining jobs are killed and then the
                 task is finished
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    auth = get_auth_indigoiam(request)

    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid'] is not None:

            data = {}
            data['jediTaskID'] = request.session['requestParams']['jeditaskid']

            data['properErrorCode'] = True

            if 'soft' in request.session['requestParams'] and bool(request.session['requestParams']['soft']) == True:
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

@login_customrequired
@never_cache
def setNumSlots(request):
    """Finish a task

       request parameters:
        pandaQueueName: string
        numSlots: int
        gshare: string
        resourceType: string
        validPeriod: int (number of days)
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    auth = get_auth_indigoiam(request)
    details = {}
    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        data = {}
        if 'pandaqueuename' in request.session['requestParams'] and request.session['requestParams']['pandaqueuename'] is not None:
            data['pandaQueueName'] = request.session['requestParams']['pandaqueuename']
        if 'gshare' in request.session['requestParams'] and request.session['requestParams']['gshare'] is not None:
            data['gshare'] = request.session['requestParams']['gshare']
        if 'resourcetype' in request.session['requestParams'] and request.session['requestParams']['resourcetype'] is not None:
            data['resourceType'] = request.session['requestParams']['resourcetype']
        if 'numslots' in request.session['requestParams'] and request.session['requestParams']['numslots'] is not None:
            data['numSlots'] = request.session['requestParams']['numslots']
        if 'validperiod' in request.session['requestParams'] and request.session['requestParams']['validperiod'] is not None:
            data['validPeriod'] = request.session['requestParams']['validperiod']
        url = baseURL + '/setNumSlotsForWP'
        try:
            resp = post(url, headers=auth, data=data)
            details['code'] = resp.status_code
            details['text'] = resp.text
            details['auth_details'] = auth
            details['data'] = data
        except Exception as ex:
            resp = "ERROR setNumSlots: %s %s" % (ex, resp.status_code)
            details['message'] = resp
    else:
        resp = auth['detail']
        details['message'] = resp

    return HttpResponse(json.dumps(details), content_type='application/json')