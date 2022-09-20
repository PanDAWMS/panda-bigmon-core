import time
from requests import post
from core.oauth.utils import get_auth_provider
baseURL = 'https://pandaserver.cern.ch/server/panda'
def get_auth_indigoiam(request):
    header = {}
    organisation = 'atlas'

    auth_provider = get_auth_provider(request)

    if auth_provider:
        social = request.user.social_auth.get(provider=auth_provider)
        if (auth_provider == 'indigoiam'):
            if (social.extra_data['auth_time'] + social.extra_data['expires_in'] - 10) <= int(time.time()):
                header = {"detail": "id token is expired"}
                return header
            else:
                id_token = social.extra_data['id_token']
        else:
            header = {"detail": "[P]lease log in via indigoiam"}
            return header

        header['Authorization'] = 'Bearer {0}'.format(id_token)
        header['Origin'] = organisation

    return header

def kill_task(auth, jeditaskid):
    """Kill a task

        request parameters:
           jediTaskID: jediTaskID of the task to be killed

"""
    if jeditaskid is not None:
        data = {}

        data['jediTaskID'] = jeditaskid
        data['properErrorCode'] = True

        url = baseURL + '/killTask'

        try:
            resp = post(url, headers=auth, data=data)
            resp = resp.text
        except Exception as ex:
            resp = "ERROR killTask: %s %s" % (ex, resp.status_code)
    else:
        resp = 'Jeditaskid is not defined'

    return resp


def finish_task(auth, jeditaskid, soft=True):
    """Finish a task

       request parameters:
           jediTaskID: jediTaskID of the task to be finished
           soft: If True, new jobs are not generated and the task is
                 finihsed once all remaining jobs are done.
                 If False, all remaining jobs are killed and then the
                 task is finished
    """
    if jeditaskid is not None:

        data = {}

        data['jediTaskID'] = jeditaskid
        data['properErrorCode'] = True
        data['soft'] = soft


        url = baseURL + '/finishTask'

        try:
            resp = post(url, headers=auth, data=data)
            resp = resp.text
        except Exception as ex:
            resp = "ERROR finishTask: %s %s" % (ex, resp.status_code)
    else:
        resp = 'Jeditaskid is not defined'

    return resp

### TODO change it later
# def pandaclient_initialization(request):
#     user = request.user
#     from pandaclient import Client
#     if user.is_authenticated and user.social_auth is not None:
#         auth_provider = (request.user.social_auth.get()).provider
#         social = request.user.social_auth.get(provider=auth_provider)
#
#         os.environ['PANDA_AUTH_ID_TOKEN'] = social.extra_data['id_token']
#         os.environ['PANDA_AUTH'] = 'oidc'
#         os.environ['PANDA_AUTH_VO'] = 'atlas'
#
#         try:
#             c = Client()
#             print('Successful')
#         except Exception as ex:
#             print(ex)