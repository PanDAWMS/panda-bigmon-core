import unittest
import json
import random
import time
import requests
from jsondiff import diff
from django.test import Client
from core.oauth.models import BPUser

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

class BPArtTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects like /job/<pandaid>/, /task/<taskid>/ etc
        cls.test_data = {
            'package': None,
            'branch': None,
            'testname': None
        }
        # get last finished job
        client = Client()
        response = client.get('/art/?json')
        data = json.loads(response.content)
        if data is not None and 'packages' in data and len(data['packages']) > 0:
            cls.test_data['package'] = data['packages'][0]
        if data is not None and 'branches' in data and len(data['branches']) > 0:
            cls.test_data['branch'] = data['branches'][0]

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

    # main page
    def test_main(self):
        response = self.client.get('/art/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # art overview page
    def test_art_overview(self):
        response = self.client.get('/art/overview/?nlastnightlies=7&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # art tasks page
    def test_art_tasks(self):
        response = self.client.get('/art/tasks/?nlastnightlies=7&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # art jobs page
    def test_art_jobs(self):
        response = self.client.get('/art/jobs/?nlastnightlies=7&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        response = self.client.get(f"/art/jobs/?nlastnightlies=7&package={self.test_data['package']}&{self.timestamp_str}")
        self.assertEqual(response.status_code, 200)


    # comparison
    @unittest.skip("Skip art overview comparison")
    def test_art_overview_old_vs_new(self):
        response_old = requests.get(f'https://bigpanda.cern.ch/art/overview/?nlastnightlies=3&json&{self.timestamp_str}')

        response_new = self.client.get('/art/overview/?nlastnightlies=3&json&tm=' + self.timestamp_str)
        self.assertEqual(response_new.status_code, 200)

        data_old = response_old.json()
        data_new = json.loads(response_new.content)

        if ordered(data_old) == ordered(data_new):
            print("Identical")
        else:
            print("---***---NOT identical---***---")
            d = diff(data_old, data_new)
            print(f"~{round(len(str(d))*100.0/len(str(data_old)),2)}% of difference")
            print(d)

    @unittest.skip("Skip art tasks comparison")
    def test_art_tasks_old_vs_new(self):
        response_old = requests.get(
            f'https://bigpanda.cern.ch/art/tasks/?nlastnightlies=3&json&tm={self.timestamp_str}')

        response_new = self.client.get('/art/tasks/?nlastnightlies=3&json&' + self.timestamp_str)
        self.assertEqual(response_new.status_code, 200)

        data_old = response_old.json()
        data_new = json.loads(response_new.content)

        if ordered(data_old) == ordered(data_new):
            print("Identical")
        else:
            print("---***---NOT identical---***---")
            d = diff(data_old, data_new)
            print(f"~{round(len(str(d))*100.0/len(str(data_old)),2)}% of difference")
            print(d)

    @unittest.skip("Skip art jobs comparison")
    def test_art_jobs_old_vs_new(self):

        request_params_str = f"/art/jobs/?package={self.test_data['package']}&nlastnightlies=3&json&tm={self.timestamp_str}"
        response_old = requests.get(f'https://bigpanda.cern.ch{request_params_str}')

        response_new = self.client.get(f'{request_params_str}')
        self.assertEqual(response_new.status_code, 200)

        data_old = response_old.json()
        data_new = json.loads(response_new.content)

        if ordered(data_old) == ordered(data_new):
            print("Identical")
        else:
            print("---***---NOT identical---***---")
            d = diff(data_old, data_new)
            print(f"~{round(len(str(d))*100.0/len(str(data_old)),2)}% of difference")
            print(d)


    @unittest.skip("Skip art stability comparison")
    def test_art_stability_old_vs_new(self):

        request_params_str = f"/art/stability/?package={self.test_data['package']}&nlastnightlies=7&json&tm={self.timestamp_str}"
        response_old = requests.get(f'https://bigpanda.cern.ch{request_params_str}')

        response_new = self.client.get(f'{request_params_str}')
        self.assertEqual(response_new.status_code, 200)

        data_old = response_old.json()
        data_new = json.loads(response_new.content)

        if ordered(data_old) == ordered(data_new):
            print("Identical")
        else:
            print("---***---NOT identical---***---")
            d = diff(data_old, data_new)
            print(f"~{round(len(str(d))*100.0/len(str(data_old)),2)}% of difference")
            print(d)