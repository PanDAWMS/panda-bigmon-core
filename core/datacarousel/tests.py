import unittest
import json
import random
import time
from django.test import Client
from core.oauth.models import BPUser

class BPDataCarouselTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects like /job/<pandaid>/, /task/<taskid>/ etc
        cls.test_data = {
            'taskid': None,
            'rule_id': None,
            'source_rse': None
        }
        # get last finished job
        client = Client()
        response = client.get('/api/dc/dash/?days=1&json')
        data = json.loads(response.content)
        if data is not None and 'detailstable' in data and len(data['detailstable']) > 0:
            cls.test_data['taskid'] = data['detailstable'][0]['taskid']
            cls.test_data['rule_id'] = data['detailstable'][0]['rse']
            cls.test_data['source_rse'] = data['detailstable'][0]['source_rse']


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

    def test_dc_dash(self):
        response = self.client.get('/dc/dash/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_dc_task(self):
        response = self.client.get(f'/api/dc/staginginfofortask/?jeditaskid={self.test_data["taskid"]}&{self.timestamp_str}')
        self.assertEqual(response.status_code, 200)

    def test_dc_stuck_files(self):
        response = self.client.get(f'/api/dc/stuckfiles/?rule_id={self.test_data["rule_id"]}&source_rse={self.test_data["source_rse"]}&{self.timestamp_str}')
        self.assertEqual(response.status_code, 200)