from social_core.backends.oauth import BaseOAuth2

from urllib.parse import urljoin
import logging

class IndigoIamOIDC(BaseOAuth2):

    name = 'indigoiam'
    ID_KEY = 'email'
    ACCESS_TOKEN_METHOD = 'POST'
    
    EXTRA_DATA = [
        ('refresh_token', 'refresh_token', True),
        ('expires_in', 'expires_in'),
        ('token_type', 'token_type', True),
        ('scope', 'scope'),
    ]
    def authorization_url(self):
        return urljoin(self.setting('BASEPATH'), 'authorize')
    def access_token_url(self):
        return urljoin(self.setting('BASEPATH'), 'token')
    def get_user_details(self, response):
        logger = logging.getLogger('social')
        for k, v in response.items():
            logger.info('{}: {}'.format(k, v))
        return {
            'username': response.get('preferred_username'),
            'email': response.get('email', ''),
            'first_name': response.get('given_name', ''),
            'last_name': response.get('family_name', ''),
            'name': response.get('name', ''),
        }
    def user_data(self, access_token, *args, **kwargs):
        return self.get_json(urljoin(self.setting('BASEPATH'), 'userinfo'),
                             headers={'Authorization': f'Bearer {access_token}'})

