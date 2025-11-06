import unittest
import json
import random
import time
from django.test import Client
from core.oauth.models import BPUser

class BPSchedResourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects like /site/xyz/ etc
        cls.test_data = {
            'computingsite': None,
        }
        # get a job
        client = Client()
        response = client.get('/jobs/?days=1&limit=1&json')
        data = json.loads(response.content)
        if data is not None and 'jobs' in data and len(data['jobs']) > 0:
            cls.test_data['computingsite'] = data['jobs'][0]['computingsite']


    def setUp(self):
        # Every test needs a client
        self.client = Client()
        # log in client as test user
        self.client.force_login(BPUser.objects.get_or_create(username='testuser')[0])
        # create random timestamp to avoid getting cached data
        self.timestamp_str = 'timestamp={}'.format(random.randrange(999999999))
        # headers template
        self.headers = {}
        # per test time
        self.start_time = time.time()

    def tearDown(self):
        print('{}: {}s'.format(self.id(), (time.time() - self.start_time)))

    def test_site_list(self):
        response = self.client.get('/sites/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_site_info(self):
        response = self.client.get(f'/site/?{self.test_data['computingsite']}' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)