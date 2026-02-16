from urllib.parse import urljoin

from social_core.backends.oauth import BaseOAuth2
from django.conf import settings
from social_core.exceptions import AuthUnknownError

import logging
_logger = logging.getLogger('social')


class CernAuthOIDC(BaseOAuth2):
    """CERN OAuth2 authentication backend via OpenID Connect"""
    name = 'cernoidc'
    DEFAULT_SCOPE = ['openid',]
    SCOPE_SEPARATOR = ','
    REDIRECT_STATE = False
    ACCESS_TOKEN_METHOD = 'POST'
    ID_KEY = 'email'

    def authorization_url(self):
        return urljoin(settings.SOCIAL_AUTH_CERNOIDC_BASEPATH, 'auth')

    def access_token_url(self):
        return urljoin(settings.SOCIAL_AUTH_CERNOIDC_BASEPATH, 'token')

    def get_userinfo_url(self):
        return urljoin(settings.SOCIAL_AUTH_CERNOIDC_BASEPATH, 'userinfo')

    def get_header(self, access_token):
        return {'Authorization': f'Bearer {access_token}'}

    def user_data(self, access_token, *args, **kwargs):
        """Load user data from the service, it will be propagated down the pipeline"""
        user_data = self.get_json(self.get_userinfo_url(), headers=self.get_header(access_token))
        return user_data

    def get_user_details(self, response):
        """Return user details to be saved to auth_user django table"""
        _logger.debug([f"{k}: {v}" for k, v in response.items()])
        user_details = {
            'username': response.get('cern_upn'),
            'email': response.get('email'),
            'first_name': response.get('given_name'),
            'last_name': response.get('family_name'),
        }
        if user_details['email'] is None or user_details['email'] == '':
            raise AuthUnknownError("The mandatory field: email was not return by the auth provider.")
        return user_details

    def extra_data(self, user, uid, response, details=None, *args, **kwargs):
        """
        This data will be stored to social_auth_user.extra_data field.
        """
        data = super().extra_data(user, uid, response, details=details, *args, **kwargs)
        data.update({
            'expires_access': data['auth_time'] + response.get('expires_in', ''),
            'expires_refresh': data['auth_time'] + response.get('refresh_expires_in', ''),
            'refresh_token': response.get('refresh_token', ''),
            'id_token': response.get('id_token', ''),
            'home_institute': response.get('home_institute', ''),
            'cern_person_id': response.get('cern_person_id', ''),
        })
        return data



