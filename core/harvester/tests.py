import unittest
import json
import random
import time
from django.test import Client
from core.oauth.models import BPUser


class BPHarvesterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects
        cls.test_data = {
            'instance': None,
            'workerid': None,
            'computingsite': None,
            'pandaid': None,
        }
        # get harvester worker
        client = Client()
        response = client.get('/harvester/workers/?status=finished&json=1')
        data = json.loads(response.content)
        if data is not None and 'workers' in data and len(data['workers']) > 0:
            cls.test_data['instance'] = data['workers'][0]['harvesterid']
            cls.test_data['workerid'] = data['workers'][0]['workerid']
            cls.test_data['computingsite'] = data['workers'][0]['computingsite']

            if cls.test_data['workerid'] is not None and cls.test_data['instance'] is not None:
                response = client.get('/harvester/getjobs/?limit=1&instance={}&workerid={}'.format(
                    cls.test_data['instance'],
                    cls.test_data['workerid']
                ))
                data = json.loads(response.content)
                if data is not None and len(data) > 0:
                    cls.test_data['pandaid'] = data[0]['pandaid'] if 'pandaid' in data[0] else None

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
    def test_harvester_instances(self):
        response = self.client.get('/harvester/instances/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_harvester_slots(self):
        response = self.client.get('/harvester/slots/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_harvester_workers(self):
        self.assertIsInstance(self.test_data['instance'], str)
        response = self.client.get('/harvester/workers/?instance={}&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_harvester_worker(self):
        self.assertIsInstance(self.test_data['workerid'], int)
        response = self.client.get('/harvester/worker/{}/?instance={}&{}'.format(
            self.test_data['workerid'],
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_harvester_get_workers(self):
        self.assertIsInstance(self.test_data['instance'], str)
        response = self.client.get('/harvester/getworkers/?instance={}&dt&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_harvester_get_worker_stats(self):
        self.assertIsInstance(self.test_data['instance'], str)
        response = self.client.get('/harvester/getworkerstats/?instance={}&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_harvester_get_jobs(self):
        self.assertIsInstance(self.test_data['instance'], str)
        response = self.client.get('/harvester/getjobs/?instance={}&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_api_harvester_get_getdiagnostics(self):
        self.assertIsInstance(self.test_data['instance'], str)
        response = self.client.get('/harvester/getdiagnostics/?instance={}&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

