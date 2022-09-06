import json
import time

from requests import get, post
from django.http import HttpResponse, JsonResponse
from django.views.decorators.cache import never_cache

from core.oauth.utils import login_customrequired
from core.libs.DateEncoder import DateEncoder

@login_customrequired
@never_cache
def get_pandaserver_attr(request):

    id_token = None
    token_type = None
    access_token = None

    username = None
    fullname = None
    organisation = 'atlas'

    auth_provider = None

    user = request.user
    resp = {}

    if user.is_authenticated and user.social_auth is not None:

        auth_provider = (request.user.social_auth.get()).provider
        social = request.user.social_auth.get(provider=auth_provider)

        if (auth_provider == 'indigoiam'):
            if (social.extra_data['auth_time'] + social.extra_data['expires_in'] - 10) <= int(time.time()):
                resp = {"detail": "id token is expired"}
                dump = json.dumps(resp, cls=DateEncoder)
                response = HttpResponse(dump, content_type='application/json')
                return response
            else:
                token_type = social.extra_data['token_type']
                access_token = social.extra_data['access_token']
                id_token = social.extra_data['id_token']
        else:
            resp = {"detail": "[P]lease log in via indigoiam"}
            dump = json.dumps(resp, cls=DateEncoder)
            response = HttpResponse(dump, content_type='application/json')
            return response

    header = {}
    header['Authorization'] = 'Bearer {0}'.format(access_token)
    header['Origin'] = organisation

    resp = post('https://pandaserver.cern.ch/server/panda/getAttr', headers=header, data={})

    return HttpResponse(resp.text, content_type='application/json')