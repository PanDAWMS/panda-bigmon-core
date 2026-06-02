"""Apps.py for the OAuth application."""
import logging
import os
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
        """
        Initialize the authorization service when the app is ready.
        Use policy file specified in the settings via environment variable 'AUTHORIZATION_POLICY_PATH',
            or experiment policy from policies directory,
            or default policy.
        """
        policy_path = getattr(settings, 'AUTHORIZATION_POLICY_PATH', None)
        if not policy_path:
            base_policy_dir = os.path.join(settings.BASE_DIR, 'oauth', 'policies')
            # try to find vo-specific policy file, if not found, use default policy
            vo = getattr(settings, 'MON_VO', '').lower().strip()
            vo_policy_file = os.path.join(base_policy_dir, f'policy_{vo}.csv')
            default_policy_file = os.path.join(base_policy_dir, 'policy__default.csv')
            if vo and os.path.exists(vo_policy_file):
                policy_path = vo_policy_file
                _logger.info(f"Targeting VO policy file for: '{vo}'")
            else:
                policy_path = default_policy_file
                _logger.info(f"VO policy file '{default_policy_file}' not found. Falling back to default.")

        # initialize the authorization service
        try:
            if os.path.exists(policy_path):
                self.authz = AuthorizationService(policy_path)
                _logger.debug(f"Total policies loaded: {len(self.authz.enforcer.get_policy())} from {policy_path}")
            else:
                _logger.error(f"Critical: Chosen policy path does not exist: {policy_path}")
        except Exception as e:
            raise RuntimeError(f"Critical: AuthorizationService failed to initialize: {e}")
