
from django.http import HttpResponse

from django.views.decorators.csrf import csrf_exempt

from core.panda_client.utils import get_auth_indigoiam, kill_task, finish_task
from core.views import initRequest


@csrf_exempt
def client(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    auth = get_auth_indigoiam(request)
    info = {}
    if auth is not None and ('Authorization' in auth and 'Origin' in auth):
        if len(request.session['requestParams']) > 0:
            data = request.session['requestParams']
            if data['action'] == 'finishtask' and ('task' in data and data['task'] is not None):
                info = finish_task(auth=auth, jeditaskid=data['task'])
            elif data['action'] == 'killtask' and ('task' in data and data['task'] is not None):
                info = kill_task(auth=auth, jeditaskid=data['task'])
            else:
                info = 'Operation error'
        else:
            info = 'Request body is empty'
    else:
        info = auth['detail']

    return HttpResponse(info, content_type='text/html')
