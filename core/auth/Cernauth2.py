from social_core.backends.oauth import BaseOAuth2
import urllib

from social_core.exceptions import AuthMissingParameter, AuthStateMissing, AuthStateForbidden

import logging
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
    #
    # def auth_url(self):
    #     """Return redirect url"""
    #     state = self.get_or_create_state()
    #     params = self.auth_params(state)
    #     params.update(self.get_scope_argument())
    #     params.update(self.auth_extra_arguments())
    #     params = urllib.urlencode(params)
    #     if not self.REDIRECT_STATE:
    #         # redirect_uri matching is strictly enforced, so match the
    #         # providers value exactly.
    #         params = urllib.unquote(params)
    #     return '{0}?{1}'.format(self.authorization_url(), params)
    #
    # def state_token(self):
    #     """Generate csrf token to include as state parameter."""
    #     return self.strategy.random_string(64)
    #
    # def get_or_create_state(self):
    #     if self.STATE_PARAMETER or self.REDIRECT_STATE:
    #         # Store state in session for further request validation. The state
    #         # value is passed as state parameter (as specified in OAuth2 spec),
    #         # but also added to redirect, that way we can still verify the
    #         # request if the provider doesn't implement the state parameter.
    #         # Reuse token if any.
    #         name = self.name + '_state'
    #         state = self.strategy.session_get(name)
    #         if state is None:
    #             state = self.state_token()
    #             self.strategy.session_set(name, state)
    #     else:
    #         state = None
    #     return state
    #
    # def get_session_state(self):
    #     return self.strategy.session_get(self.name + '_state')
    #
    # def get_request_state(self):
    #     request_state = self.data.get('state') or \
    #                     self.data.get('redirect_state')
    #     if request_state and isinstance(request_state, list):
    #         request_state = request_state[0]
    #     return request_state

    def validate_state(self):
        """Validate state value. Raises exception on error, returns state
        value if valid."""
        if not self.STATE_PARAMETER and not self.REDIRECT_STATE:
            return None
        state = self.get_session_state()
        request_state = self.get_request_state()
        if not request_state:
            self.social_logger()
            raise AuthMissingParameter(self, 'state')
        elif not state:
            self.social_logger()
            raise AuthStateMissing(self, 'state')
        elif not request_state == state:
            self.social_logger()
            raise AuthStateForbidden(self)
        else:
            return state

    def social_logger(self):
        message = 'Session report'+'\n'
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
        logger = logging.getLogger('social')
        logger.debug(message)