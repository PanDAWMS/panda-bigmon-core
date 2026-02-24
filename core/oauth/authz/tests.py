
import random
import time
import unittest
from django.test import Client
from core.oauth.models import BPUser
from core.oauth.authz.service import authz

class BPAuthzTests(unittest.TestCase):
    def setUp(self):
        # Every test needs a client
        self.client = Client()
        # log in client as test user
        self.client.force_login(BPUser.objects.get_or_create(username='testuser')[0])
        self.user_roles = list(BPUser.objects.get(username='testuser').groups.values_list("name", flat=True))
        # create random timestamp to avoid getting cached data
        self.timestamp_str = 'timestamp={}'.format(random.randrange(999999999))
        # headers template
        self.headers = {}
        # per test time
        self.start_time = time.time()

    def tearDown(self):
        print('{}: {}s'.format(self.id(), (time.time() - self.start_time)))

    # test the task update actions
    def test_task_update_matrix(self):
        cases = [
            ("prod", 700, True),
            ("prod", 100, False),
            ("analy", "Express Analysis", True),
            ("analy", "Invalid", False),
        ]

        for tasktype, value, expected in cases:
            if tasktype == "prod":
                obj = {"type": "task", "tasktype": tasktype}
                new = {"priority": value}
            else:
                obj = {"type": "task", "tasktype": tasktype}
                new = {"globalshare": value}

            result = authz.enforce(
                self.user_roles,
                obj,
                "update",
                new
            )
            self.assertEqual(result, expected)

    # test user contact read access
    def test_user_contact_read_access(self):
        # test with no roles
        result = authz.enforce([], {"type": "user_contact"}, "read", {})
        self.assertFalse(result)

        # test with some irrelevant role
        result = authz.enforce(["some_role"], {"type": "user_contact"}, "read", {})
        self.assertFalse(result)

        # test with a role that should have access
        result = authz.enforce(self.user_roles, {"type": "user_contact"}, "read", {})
        self.assertTrue(result)