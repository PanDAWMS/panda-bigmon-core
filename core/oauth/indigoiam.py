import logging
from social_core.backends.oauth import BaseOAuth2
from urllib.parse import urljoin

_logger = logging.getLogger('social')


class IndigoIamOIDC(BaseOAuth2):
    name = 'indigoiam'
    ID_KEY = 'email'
    ACCESS_TOKEN_METHOD = 'POST'
    REFRESH_TOKEN_METHOD = 'POST'
    DEFAULT_SCOPE = ['openid', 'profile', 'email', 'address', 'phone']
    EXTRA_DATA = [
        ('id_token', 'id_token', True),
        ('expires_in', 'expires_in'),
        ('token_type', 'token_type', True),
        ('scope', 'scope'),
    ]
    def authorization_url(self):
        return urljoin(self.setting('BASEPATH'), 'authorize')
    def access_token_url(self):
        return urljoin(self.setting('BASEPATH'), 'token')
    def user_data(self, access_token, *args, **kwargs):
        """Load user data from the service, it will be propagated down the pipeline"""
        return self.get_json(urljoin(self.setting('BASEPATH'), 'userinfo'), headers={'Authorization': f'Bearer {access_token}'})

    def get_user_details(self, response):
        """Return user details to be saved to auth_user django table"""
        for k, v in response.items():
            _logger.info('{}: {}'.format(k, v))
        return {
            'username': response.get('preferred_username'),
            'email': response.get('email', ''),
            'first_name': response.get('given_name', '').title(),
            'last_name': response.get('family_name', '').title(),
        }
