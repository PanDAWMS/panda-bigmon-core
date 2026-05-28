import requests
import jwt
from jwt import PyJWKClient
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed
from django.conf import settings
from core.oauth.models import BPToken, BPUser
from core.oauth.utils import get_token_expiry_info


class BPTokenAuthentication(TokenAuthentication):
    """
    Simple DRF token based authentication with a custom token model (BPToken).

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "Token ".  For example:

        Authorization: Token 401f7ac837da42b97f613d789819ff93537bee6a
    """
    def get_model(self):
        return BPToken

    def authenticate(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return None

        parts = auth_header.split()
        if len(parts) != 2:
            return None

        prefix = parts[0].lower()
        token_value = parts[1]

        # check with IAM service for token validity and expiry
        if prefix == 'bearer':
            return self.authenticate_iam_token(token_value)

        # if DRF token, check in our database
        if prefix == 'token':
            return super().authenticate(request)

        return None


    def authenticate_iam_token(self, token):
        """Validates external IAM tokens via the IAM service and returns the associated user."""

        base_path = getattr(settings, 'SOCIAL_AUTH_INDIGOIAM_BASEPATH', 'https://atlas-auth.cern.ch/')
        if not base_path.endswith('/'):
            base_path += '/'
        jwk_uri = base_path + "jwk"

        try:
            jwk_client = PyJWKClient(jwk_uri)
            signing_key = jwk_client.get_signing_key_from_jwt(token)
            token_info = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                options={"verify_aud": False},
                issuer=base_path
            )
        except jwt.ExpiredSignatureError:
            raise AuthenticationFailed(_('ATLAS IAM token has expired.'))
        except jwt.InvalidTokenError as e:
            raise AuthenticationFailed(_(f'Invalid ATLAS IAM token: {str(e)}'))
        except Exception as e:
            raise AuthenticationFailed(_(f'Token validation error: {str(e)}'))

        # Align this mapping exactly with your get_user_details() method in IndigoIamOIDC
        username = token_info.get('preferred_username', None)
        if not username:
            userinfo_url = f"{base_path}userinfo"
            try:
                ui_response = requests.get(userinfo_url, headers={'Authorization': f'Bearer {token}'}, timeout=5)
                if ui_response.status_code == 200:
                    ui_data = ui_response.json()
                    username = ui_data.get('preferred_username')
                elif ui_response.status_code in [401, 403]:
                    raise AuthenticationFailed(
                        _("IAM rejected this token while validating. It may be revoked, expired, or missing openid scope.")
                    )
                else:
                    raise AuthenticationFailed(
                        _(f"IAM userinfo endpoint returned an unexpected error status: {ui_response.status_code}")
                    )
                if not username:
                    raise AuthenticationFailed(_("Username not found in userinfo response, may be missing scopes in the token."))
            except requests.RequestException as e:
                raise AuthenticationFailed(_(f"Network error connecting to IAM userinfo service: {str(e)}"))

        # Get user for the authenticated token
        try:
            user = BPUser.objects.get(username=username)
        except BPUser.DoesNotExist:
            raise AuthenticationFailed(_(f"Unknown user '{username}', need to log in at least once to web interface."))
        if not user.is_active:
            raise AuthenticationFailed(_('User associated with this IAM token is deactivated.'))

        return (user, token_info)


    def authenticate_credentials(self, key):
        model = self.get_model()
        try:
            token = model.objects.select_related('user').get(key=key)
        except model.DoesNotExist:
            raise AuthenticationFailed(_('Invalid token.'))

        if not token.user.is_active:
            raise AuthenticationFailed(_('User inactive or deleted.'))

        # Check if the token has expired
        token_expiry_info = get_token_expiry_info(token)
        if token_expiry_info['is_expired']:
            raise AuthenticationFailed(_('Token has expired.'))

        return (token.user, token)