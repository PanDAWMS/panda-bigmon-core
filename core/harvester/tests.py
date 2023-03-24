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
        self.assertIsInstance(self.test_data['workerid'], int)
        response = self.client.get('/harvester/getworkers/?instance={}&{}'.format(
            self.test_data['instance'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)



    # def test_api_idds_collections(self):
    #     self.assertIsInstance(self.test_data['transform_id'], int)
    #     response = self.client.get('/idds/collections/?transform_id={}&{}'.format(
    #         self.test_data['transform_id'],
    #         self.timestamp_str
    #     ))
    #     self.assertEqual(response.status_code, 200)
    #
    # def test_api_idds_processings(self):
    #     self.assertIsInstance(self.test_data['transform_id'], int)
    #     response = self.client.get('/idds/processings/?transform_id={}&{}'.format(
    #         self.test_data['transform_id'],
    #         self.timestamp_str
    #     ))
    #     self.assertEqual(response.status_code, 200)
    #
    # def test_api_idds_contents(self):
    #     self.assertIsInstance(self.test_data['coll_id'], int)
    #     response = self.client.get('/idds/contents/?coll_id={}&{}'.format(
    #         self.test_data['coll_id'],
    #         self.timestamp_str
    #     ))
    #     self.assertEqual(response.status_code, 200)
    #
    # # iDDS workflows
    # def test_idds_workflows(self):
    #     response = self.client.get('/idds/wfprogress/?' + self.timestamp_str)
    #     self.assertEqual(response.status_code, 200)
    #
    # # DAG graph
    # def test_idds_daggraph(self):
    #     self.assertIsInstance(self.test_data['request_id'], int)
    #     response = self.client.get('/idds/daggraph/?requestid={}&{}'.format(
    #         self.test_data['request_id'],
    #         self.timestamp_str
    #     ))
    #     self.assertEqual(response.status_code, 200)
    #

