"""Apps.py for the OAuth application."""
import logging

from django.apps import AppConfig
from django.conf import settings
from panda_authz.service import AuthorizationService

_logger = logging.getLogger("social")


class OAuthConfig(AppConfig):
    """Django AppConfig for the OAuth application."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "core.oauth"
    authz = None  # Placeholder for the authorization service instance

    def ready(self):
        # Initialize authorization service
        try:
            self.authz = AuthorizationService(settings.AUTHORIZATION_POLICY_PATH)
            _logger.debug(f"Total policies loaded: {len(self.authz.enforcer.get_policy())} from {settings.AUTHORIZATION_POLICY_PATH}")
        except Exception as e:
            raise f"Critical: AuthorizationService failed: {e}"
