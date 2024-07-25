from requests import ConnectionError, request
from social_core.backends.oauth import BaseOAuth2
from social_core.exceptions import AuthMissingParameter, AuthFailed
from social_core.utils import SSLHttpAdapter

import logging
_logger = logging.getLogger('social')


class CernAuthOIDC(BaseOAuth2):
    """CERN OAuth2 authentication backend via OpenID Connect"""

    name = 'cernoidc'
    AUTHORIZATION_URL = 'https://auth.cern.ch/auth/realms/cern/protocol/openid-connect/auth'
    ACCESS_TOKEN_URL = 'https://auth.cern.ch/auth/realms/cern/protocol/openid-connect/token'
    USER_DATA = 'https://auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo'
    DEFAULT_SCOPE = ['openid', ]
    SCOPE_SEPARATOR = ','
    REDIRECT_STATE = False
    EXTRA_DATA = [
        ('id', 'id'),
        ('expires', 'expires')
    ]
    ACCESS_TOKEN_METHOD = 'POST'
    ID_KEY = 'email'
    message = ''
    errordesc = ''

    def get_user_details(self, response):
        """Return user details from CERN account"""
        _logger.debug([f"{k}: {v}" for k, v in response.items()])
        return {
            'username': response.get('preferred_username'),
            'email': response.get('email') or '',
            'first_name': response.get('given_name'),
            'last_name': response.get('family_name'),
            'name': response.get('name'),
            'roles': response.get('cern_roles') or [],
        }

    def user_data(self, access_token, *args, **kwargs):
        """Load user data from the service"""
        return self.get_json(
            self.USER_DATA,
            # headers=self.get_auth_header(access_token)
            data={"access_token": access_token}
        )

    def get_auth_header(self, access_token):
        return {'Authorization': 'Bearer {0}'.format(access_token)}

    def extra_data(self, user, uid, response, details=None, *args, **kwargs):
        """Return users extra data"""
        return {
            'username': response.get('preferred_username'),
            'email': response.get('email') or '',
            'first_name': response.get('given_name'),
            'last_name': response.get('family_name'),
            'name': response.get('name'),
            'roles': response.get('cern_roles') or [],
        }

    def uses_redirect(self):
        """Return True if this provider uses redirect url method,
        otherwise return false."""
        return True

    def user_agent(self):
        """Builds a simple User-Agent string to send in requests"""
        return 'social-auth-1.6.0'

    def request(self, url, method='POST', *args, **kwargs):
        """Request helper"""
        kwargs.setdefault('headers', {})
        if self.setting('VERIFY_SSL') is not None:
            kwargs.setdefault('verify', self.setting('VERIFY_SSL'))
        kwargs.setdefault('timeout', self.setting('REQUESTS_TIMEOUT') or self.setting('URLOPEN_TIMEOUT'))
        if self.SEND_USER_AGENT and 'User-Agent' not in kwargs['headers']:
            kwargs['headers']['User-Agent'] = self.setting('USER_AGENT') or self.user_agent()
        try:
            if self.SSL_PROTOCOL:
                session = SSLHttpAdapter.ssl_adapter_session(self.SSL_PROTOCOL)
                response = session.request(method, url, *args, **kwargs)
            else:
                if method == 'POST':
                    if 'params' in kwargs:
                        del kwargs['params']
                response = request(method, url, *args, **kwargs)
        except ConnectionError as err:
            raise AuthFailed(self, str(err))
        response.raise_for_status()
        try:
            if self.message != '':
                self.general_to_message(kwargs, response)
                self.message_write()
            else:
                _logger.error('Message is Empty!')
        except Exception as ex:
            _logger.error(ex)
            pass
        return response

    def validate_state(self):
        """Validate state value. Raises exception on error, returns state
        value if valid."""
        if not self.STATE_PARAMETER and not self.REDIRECT_STATE:
            return None
        state = self.get_session_state()
        request_state = self.get_request_state()
        if not request_state:
            self.errordesc = AuthMissingParameter(self, 'state').__str__()
            self.social_error_logger(self.errordesc)
        elif not state:
            self.errordesc = 'Session value state missing.'
            self.social_error_logger(self.errordesc)
        elif not request_state == state:
            self.errordesc = 'Wrong state parameter given'
            self.social_error_logger(self.errordesc)
        else:
            return state

    def message_write(self):
        _logger.error(self.message)
        self.message = ''

    def self_to_message(self):
        dictattr = {}
        self.message += 'SELF OBJECT:'+'\n'
        for attr in dir(self):
            if attr.isupper():
                dictattr[attr] = getattr(self, attr)
                self.message += attr + ':' + str(getattr(self, attr)) + '\n'

    def general_to_message(self, *attrs):
        for attr in attrs:
            self.message += '================ADDITIONAL INFORMATION==================='+'\n'
            try:
                newattr = vars(attr)
            except:
                newattr = attr
            for subattr in newattr:
                self.message += subattr + ':' + str(newattr[subattr]) + '\n'

    def social_error_logger(self, errmess):
        try:
            if 'HTTP_REFERER' in self.strategy.request.META:
                self.message += f"ERROR during authentication via {self.name}: {self.strategy.request.META['HTTP_REFERER']} \n"
            else:
                self.message += "ERROR during authentication\n"
        except:
            self.message += "ERROR during authentication\n"
        self.message += 'EXCEPTION:' + errmess + '\n'
        self.self_to_message()
        self.message += 'SESSION INFO:'+'\n'
        if hasattr(self, 'data'):
            if 'code' in self.data:
                self.message += 'Code in data: '+ self.data['code'] + '\n'
            else:
                self.message += 'Code in data: None \n'
            if 'state' in self.data:
                self.message += 'State in data: ' + self.data['state'] + '\n'
            else:
                self.message += 'State in data: None \n'
        else:
            self.message += 'Data not exists \n'
        if hasattr(self.strategy, 'session'):
            self.message += 'Session exists' + '\n'
            if hasattr(self.strategy.session, 'cache_key'):
                self.message += 'Cache key: ' + self.strategy.session.cache_key + '\n'
            else:
                self.message += 'Cache key:  None \n'
            if hasattr(self.strategy.session, 'session_key'):
                self.message += 'Session key: ' + self.strategy.session.session_key + '\n'
            else:
                self.message += 'Session key: None \n'
            if hasattr(self.strategy.session, '_SessionBase__session_key'):
                self.message += '_SessionBase__session_key: ' + self.strategy.session._SessionBase__session_key + '\n'
            else:
                self.message += '_SessionBase__session_key:  None \n'
            if hasattr(self.strategy.session, '_session'):
                self.message += '_session in the session object exists' + '\n'
                if self.strategy.session._session is not None:
                    self.message += '_session size: ' + str(len(self.strategy.session._session)) + '\n'
                else:
                    self.message += '_session size: None'+'\n'
                for v in dict(self.strategy.session._session):
                    self.message += v + ':' + str(self.strategy.session._session[v]) + '\n'
            else:
                self.message += '_session in the session object not exists' + '\n'
        else:
            self.message += 'Session NOT exists' + '\n'

        _logger.error(self.message)