import json
import logging
from django.http import HttpResponse

from django.views.decorators.csrf import csrf_exempt

from core.panda_client.utils import get_auth_indigoiam, kill_task, finish_task, set_debug_mode, to_bool, get_user_groups
from core.views import initRequest

from core.oauth.utils import is_expert

_logger = logging.getLogger('panda.client')
@csrf_exempt
def client(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    auth = get_auth_indigoiam(request)
    info = {}

    info['redirect'] = 'false'
    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if len(request.session['requestParams']) > 0:
            data = request.session['requestParams']
            ###Finish Task
            if data['action'] == 'finishtask' and ('taskID' in data and data['taskID'] is not None):
                info['text'] = finish_task(auth=auth, jeditaskid=data['taskID'])
            ### Kill Task
            elif data['action'] == 'killtask' and ('taskID' in data and data['taskID'] is not None):
                info['text'] = kill_task(auth=auth, jeditaskid=data['taskID'])
            ### Set debug mode
            elif data['action'] == 'setdebugmode' and ('pandaid' in data and data['pandaid'] is not None):
                if ('params' in data and data['params'] is not None):
                    params = json.loads(data['params'])
                    if ('modeOn' in params and params['modeOn'] is not None):
                        modeOn = to_bool(params['modeOn'])
                    else:
                        modeOn = False

                info['text'] = set_debug_mode(auth, pandaid=data['pandaid'], modeOn=modeOn,
                                            user_id=request.user.id, groups=get_user_groups(auth['Authorization']))
                if (info['text'].find('Succeeded') != -1 and modeOn):
                    info['redirect'] = 'true'
                else:
                    info['redirect'] = 'false'
            else:
                info['text'] = 'Operation error'
        else:
            info['text'] = 'Request body is empty'
    else:
        info['text'] = auth['detail']

    return HttpResponse(json.dumps(info), content_type='text/html')
