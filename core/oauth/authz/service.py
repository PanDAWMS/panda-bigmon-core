import os
import casbin
from django.conf import settings
from .functions import match_object, match_constrains, match_role


class AuthorizationService:
    def __init__(self):
        base_dir = settings.BASE_DIR

        model_path = os.path.join(base_dir, "oauth", "authz", "model.conf")
        policy_path = os.path.join(base_dir, "oauth", "authz", "policy_test.csv")

        self.enforcer = casbin.Enforcer(model_path, policy_path)

        self.enforcer.add_function("match_role", match_role)
        self.enforcer.add_function("match_object", match_object)
        self.enforcer.add_function("match_constrains", match_constrains)

    def enforce(self, sub, obj, act, num=None):
        num = num or {}
        return self.enforcer.enforce(sub, obj, act, num)


authz = AuthorizationService()