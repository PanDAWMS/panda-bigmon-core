import json
import logging
from django.apps import apps
from django.http import HttpResponse, JsonResponse

from core.libs.job import get_job_list
from core.oauth.decorators import login_required
from core.panda_client.utils import get_auth_indigoiam, kill_task, finish_task, set_debug_mode, to_bool, get_user_groups
from core.panda_client.ask_panda import AskPanda
from core.utils import error_response
from core.views import initRequest

_logger = logging.getLogger('panda.client')


@login_required
def client(request, task_id=None):
    valid, response = initRequest(request)
    if not valid:
        return response

    info = {'redirect': 'false'}

    data = request.session.get('requestParams') or {}

    if not data:
        info['text'] = 'Request body is empty'
        return HttpResponse(json.dumps(info), content_type='text/html')

    jeditaskid = data.get('taskID') or data.get('taskid')

    action = data.get('action')

    try:
        if action == 'finishtask' and jeditaskid:
            info['text'] = finish_task(request=request, jeditaskid=jeditaskid)

        elif action == 'killtask' and jeditaskid:
            info['text'] = kill_task(request=request, jeditaskid=jeditaskid)

        elif action == 'setdebugmode' and data.get('pandaid') is not None:
            auth = get_auth_indigoiam(request) or {}
            bearer = auth.get('Authorization')

            modeOn = False
            if data.get('params'):
                params = json.loads(data['params'])
                if params.get('modeOn') is not None:
                    modeOn = to_bool(params['modeOn'])

            groups = get_user_groups(bearer) if bearer else []

            info['text'] = set_debug_mode(
                request=request,
                job_id=data['pandaid'],
                mode=modeOn,
                user_id=getattr(request.user, 'id', None),
                groups=groups,
            )
            if 'Succeeded' in (info['text'] or '') and modeOn:
                info['redirect'] = 'true'
            else:
                info['redirect'] = 'false'

        else:
            if not jeditaskid and action in ('finishtask', 'killtask'):
                info['text'] = 'Error! JeditaskID is none'
            else:
                info['text'] = 'Operation error'

    except Exception as e:
        _logger.exception("Error in client view")
        info['text'] = f'Operation failed: {e}'

    return HttpResponse(json.dumps(info), content_type='text/html')



@login_required
def job_error_analysis(request):
    """Handles job error analysis done by AskPanda"""
    valid, response = initRequest(request)
    if not valid:
        return response

    authz = apps.get_app_config("oauth").authz
    if not authz.enforce(list(request.user.groups.values_list('name', flat=True)), 'error_analysis', 'read', {}, {}):
        return error_response(request, "You are not authorized to access this resource", 403)

    pandaid = None
    if 'pandaid' in request.session['requestParams']:
        pandaid = request.session['requestParams']['pandaid']
    if not pandaid:
        return error_response(request, "No pandaid provided", 400)

    job_list = get_job_list(query={'pandaid': pandaid})
    if job_list and len(job_list) > 0:
        job =job_list[0]
    else:
        return error_response(request, "provided pandaid does not exist", 404)

    ask_panda = AskPanda()
    res = ask_panda.job_error_analysis(job)
    status_code = 200 if res.get("success") else 502
    data = {
        'pandaid': pandaid,
        'result': res
    }
    return JsonResponse(data, status=status_code)

