import time, os

from pandaclient import Client

def get_auth_indigoiam(request):
    header = {}
    organisation = 'atlas'
    user = request.user

    if user.is_authenticated and user.social_auth is not None:
        auth_provider = (request.user.social_auth.get()).provider
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

### TODO change it later
def pandaclient_initialization(request):
    user = request.user

    if user.is_authenticated and user.social_auth is not None:
        auth_provider = (request.user.social_auth.get()).provider
        social = request.user.social_auth.get(provider=auth_provider)

        os.environ['PANDA_AUTH_ID_TOKEN'] = social.extra_data['id_token']
        os.environ['PANDA_AUTH'] = 'oidc'
        os.environ['PANDA_AUTH_VO'] = 'atlas'

        try:
            c = Client()
            print('Successful')
        except Exception as ex:
            print(ex)
