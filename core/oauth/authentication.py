from rest_framework.authentication import TokenAuthentication
from core.oauth.models import BPToken

class BPTokenAuthentication(TokenAuthentication):
    """
    Simple DRF token based authentication with a custom token model (BPToken).

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "Token ".  For example:

        Authorization: Token 401f7ac837da42b97f613d789819ff93537bee6a
    """
    def get_model(self):
        return BPToken
