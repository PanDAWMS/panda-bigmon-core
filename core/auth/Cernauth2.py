from requests import ConnectionError, request
from social_core.backends.oauth import BaseOAuth2
import urllib

from social_core.exceptions import AuthMissingParameter, AuthStateMissing, AuthStateForbidden, AuthFailed

import logging

from social_core.utils import SSLHttpAdapter


class Cernauth2(BaseOAuth2):


    """CERN OAuth2 authentication backend"""
    name = 'cernauth2'
    AUTHORIZATION_URL = 'https://oauth.web.cern.ch/OAuth/Authorize'
    ACCESS_TOKEN_URL = 'https://oauth.web.cern.ch/OAuth/Token'
    SCOPE_SEPARATOR = ','
    REDIRECT_STATE = False
    EXTRA_DATA = [
        ('id', 'id'),
        ('expires', 'expires')
    ]

    def get_user_details(self, response):
         """Return user details from CERN account"""
         return {'username': response.get('username'),
                 'email': response.get('email') or '',
                 'first_name': response.get('first_name'),
                 'last_name' : response.get('last_name'),
                 'federation' : response.get('federation'),
                 'name' : response.get('name'),
                 }



    def user_data(self, access_token, *args, **kwargs):
        """Load user data from the service"""
        return self.get_json(
            'https://oauthresource.web.cern.ch/api/User',
            headers=self.get_auth_header(access_token)
        )

    def get_auth_header(self, access_token):
        return {'Authorization': 'Bearer {0}'.format(access_token)}

    def extra_data(self, user, uid, response, details=None, *args, **kwargs):
        """Return users extra data"""
        return {'username': response.get('username'),
                 'email': response.get('email') or '',
                 'first_name': response.get('first_name'),
                 'last_name' : response.get('last_name'),
                 'federation' : response.get('federation'),
                 'name' : response.get('name'),}
    def uses_redirect(self):
        """Return True if this provider uses redirect url method,
        otherwise return false."""
        return True

    def user_agent(self):
        """Builds a simple User-Agent string to send in requests"""
        return 'social-auth-1.6.0'

    def request(self, url, method='GET', *args, **kwargs):
        kwargs.setdefault('headers', {})
        if self.setting('VERIFY_SSL') is not None:
            kwargs.setdefault('verify', self.setting('VERIFY_SSL'))
        kwargs.setdefault('timeout', self.setting('REQUESTS_TIMEOUT') or
                                     self.setting('URLOPEN_TIMEOUT'))
        if self.SEND_USER_AGENT and 'User-Agent' not in kwargs['headers']:
            kwargs['headers']['User-Agent'] = self.setting('USER_AGENT') or \
                                              self.user_agent()

        try:
            if self.SSL_PROTOCOL:
                session = SSLHttpAdapter.ssl_adapter_session(self.SSL_PROTOCOL)
                response = session.request(method, url, *args, **kwargs)
            else:
                response = request(method, url, *args, **kwargs)
        except ConnectionError as err:
            raise AuthFailed(self, str(err))
        response.raise_for_status()
        try:
            if 'message' in globals():
                self.general_to_message(kwargs,response)
                self.message_write()
        except:
            pass
        return response

    def validate_state(self):
        """Validate state value. Raises exception on error, returns state
        value if valid."""
        if not self.STATE_PARAMETER and not self.REDIRECT_STATE:
            return None
        state = self.get_session_state()
        request_state = self.get_request_state()
        self.social_error_logger('Missing needed parameter')
        if not request_state:
            #self.social_logger()
            self.social_error_logger(AuthMissingParameter(self, 'state').__str__())
            raise AuthMissingParameter(self, 'state')
        elif not state:
            self.social_error_logger('Session value state missing.')
            raise AuthStateMissing(self, 'state')
            # name = self.name + '_state'
            # state = self.data['state']
            # self.strategy.session_set(name, state)
            # state = self.get_session_state()
            # if not state:
            #     raise AuthStateMissing(self, 'state')
            # else: return state
        elif not request_state == state:
            self.self.social_error_logger('Wrong state parameter given')
            raise AuthStateForbidden(self)
        else:
            return state

    def message_write(self):
        global message
        logger = logging.getLogger('social')
        logger.error(message)
        del globals()['message']

    messsage = ''

    def self_to_message(self):
        dictattr = {}
        global message
        message += 'SELF OBJECT:'+'\n'
        for attr in dir(self):
            if attr.isupper():
            #if 'method-wrapper' not in str(type(getattr(self,attr))) and 'instancemethod' not in str(type(getattr(self,attr))) and attr=='__dict__':
                dictattr[attr] = getattr(self,attr)
                message += attr +':'+ str(getattr(self,attr)) +'\n'
        dictattr.update(vars(self))

    def general_to_message(self,*attrs):
        global message
        #dictattr = {}
        for attr in attrs:
            message+= '================ADDITIONAL INFORMATION==================='+'\n'
            try:
                newattr = vars(attr)
            except:
                newattr = attr
            for subattr in newattr:
                message += subattr+ ':'+ str(newattr[subattr]) + '\n'

    def social_error_logger(self,errmess):
        global message
        if 'HTTP_REFERER' in self.strategy.request.META:
            message += 'Internal Server Error: ' + self.strategy.request.META['HTTP_REFERER']+ '\n'
        else: message += 'Internal Server Error: -' + '\n'
        message += 'EXCEPTION:' + errmess + '\n\n'
        self.self_to_message()
        message+= 'SESSION INFO:'+'\n'
        if hasattr(self,'data'):
            if 'code' in self.data:
                message += 'Code in data: '+ self.data['code'] + '\n'
            else: message += 'Code in data: None \n'
            if 'state' in self.data:
                message += 'State in data: ' + self.data['state'] + '\n'
            else: message += 'State in data: None \n'
        else: message = 'Data not exists \n'
        if hasattr(self.strategy,'session'):
            message += 'Session exists' + '\n'
            if hasattr(self.strategy.session,'cache_key'):
                message += 'Cache key: ' + self.strategy.session.cache_key + '\n'
            else:
                message += 'Cache key:  None \n'
            if hasattr(self.strategy.session,'session_key'):
                message += 'Session key: ' + self.strategy.session.session_key + '\n'
            else:
                message += 'Session key: None \n'
            if hasattr(self.strategy.session,'_SessionBase__session_key'):
                message += '_SessionBase__session_key: ' + self.strategy.session._SessionBase__session_key + '\n'
            else:
                message += '_SessionBase__session_key:  None \n'
            if hasattr(self.strategy.session,'_session'):
                message += '_session in the session object exists' + '\n'
                for v in dict(self.strategy.session._session):
                    message+= v+':'+ str(self.strategy.session._session[v]) + '\n'
            else:
                message += '_session in the session object not exists' + '\n'
        else:
            message += 'Session NOT exists' + '\n'
        #logger = logging.getLogger('social')
        #logger.debug(message)