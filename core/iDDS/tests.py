import unittest
import json
import random
import time
from django.test import Client
from core.oauth.models import BPUser


class BPIddsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects
        cls.test_data = {
            'request_id': None,
            'transform_id': None,
            'coll_id': None,
        }
        # get last finished iDDS workflow
        client = Client()
        response = client.get('/idds/?json=1')
        data = json.loads(response.content)
        if data is not None and len(data) > 0:
            cls.test_data['request_id'] = data[len(data) - 1]['request_id']
            cls.test_data['transform_id'] = data[len(data) - 1]['transform_id']
            if cls.test_data['transform_id'] is not None:
                response = client.get('/idds/collections/?transform_id={}'.format(cls.test_data['transform_id']))
                data = json.loads(response.content)
                if data is not None and 'data' in data and len(data['data']) > 0:
                    cls.test_data['coll_id'] = data['data'][0]['coll_id'] if 'coll_id' in data['data'][0] else None

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

    # main iDDS page & cascade apis for it
    def test_idds_main(self):
        response = self.client.get('/idds/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_idds_transforms(self):
        self.assertIsInstance(self.test_data['request_id'], int)
        response = self.client.get('/idds/transforms/?requestid={}&{}'.format(
            self.test_data['request_id'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_idds_collections(self):
        self.assertIsInstance(self.test_data['transform_id'], int)
        response = self.client.get('/idds/collections/?transform_id={}&{}'.format(
            self.test_data['transform_id'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_idds_processings(self):
        self.assertIsInstance(self.test_data['transform_id'], int)
        response = self.client.get('/idds/processings/?transform_id={}&{}'.format(
            self.test_data['transform_id'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_idds_contents(self):
        self.assertIsInstance(self.test_data['coll_id'], int)
        response = self.client.get('/idds/contents/?coll_id={}&{}'.format(
            self.test_data['coll_id'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    # iDDS workflows
    def test_idds_workflows(self):
        response = self.client.get('/idds/wfprogress/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # DAG graph
    def test_idds_daggraph(self):
        self.assertIsInstance(self.test_data['request_id'], int)
        response = self.client.get('/idds/daggraph/?requestid={}&{}'.format(
            self.test_data['request_id'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)


