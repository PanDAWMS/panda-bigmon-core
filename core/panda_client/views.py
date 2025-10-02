import json
import logging
from django.http import HttpResponse

from django.views.decorators.csrf import csrf_exempt

from core.panda_client.utils import get_auth_indigoiam, kill_task, finish_task, set_debug_mode, to_bool, get_user_groups
from core.views import initRequest

from core.oauth.utils import is_expert

_logger = logging.getLogger('panda.client')
@csrf_exempt
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