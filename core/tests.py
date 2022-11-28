import unittest
import json
import random
import time
from django.test import Client
from core.oauth.models import BPUser


class BPCoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # query the BP to get a ids of objects to test views of single objects like /job/<pandaid>/, /task/<taskid>/ etc
        cls.test_data = {
            'pandaid': None,
            'jeditaskid': None,
            'lfn': None,
            'datasetid': None,
            'datasetname': None,
            'produsername': None,
            'computingsite': None,
            'wn': None,
        }
        # get last finished job
        client = Client()
        response = client.get('/jobs/?json&datasets=true&limit=1&jobstatus=finished&days=7&sortby=time-descending')
        data = json.loads(response.content)
        if data is not None and 'jobs' in data and len(data['jobs']) > 0:
            cls.test_data['pandaid'] = data['jobs'][0]['pandaid']
            cls.test_data['jeditaskid'] = data['jobs'][0]['jeditaskid']
            cls.test_data['produsername'] = data['jobs'][0]['produsername']
            cls.test_data['computingsite'] = data['jobs'][0]['computingsite']
            if data['jobs'][0]['modificationhost'] is not None and '@' in data['jobs'][0]['modificationhost']:
                cls.test_data['wn'] = data['jobs'][0]['modificationhost'].split('@')[1]
            if 'datasets' in data['jobs'][0] and len(data['jobs'][0]['datasets']) > 0:
                cls.test_data['lfn'] = data['jobs'][0]['datasets'][0]['lfn']
                cls.test_data['datasetid'] = data['jobs'][0]['datasets'][0]['datasetid']
                cls.test_data['datasetname'] = data['jobs'][0]['datasets'][0]['datasetname']

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
        response = self.client.get('/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # help page
    def test_help(self):
        response = self.client.get('/help/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # jobs and everything related/available for on demand loading
    def test_jobs(self):
        response = self.client.get('/jobs/?limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_jobs(self):
        response = self.client.get('/jobs/?json=1&limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('selectionsummary', 'jobs', 'errsByCount')
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_job(self):
        self.assertIsInstance(self.test_data['pandaid'], int)
        response = self.client.get('/job/' + str(self.test_data['pandaid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_job(self):
        self.assertIsInstance(self.test_data['pandaid'], int)
        response = self.client.get('/job/' + str(self.test_data['pandaid']) + '/?json=1&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('files', 'job', 'dsfiles')
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_job_descendentjoberrsinfo(self):
        self.assertIsInstance(self.test_data['pandaid'], int)
        response = self.client.get('/descendentjoberrsinfo/?pandaid={}&jeditaskid={}&{}'.format(
            str(self.test_data['pandaid']),
            str(self.test_data['jeditaskid']),
            self.timestamp_str))
        self.assertEqual(response.status_code, 200)

    def test_job_jobrelationships(self):
        self.assertIsInstance(self.test_data['pandaid'], int)
        response = self.client.get('/jobrelationships/' + str(self.test_data['pandaid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_job_jobstatuslog(self):
        self.assertIsInstance(self.test_data['pandaid'], int)
        response = self.client.get('/jobstatuslog/' + str(self.test_data['pandaid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # tasks and everything related/available for on demand loading
    def test_tasks(self):
        response = self.client.get('/tasks/?limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_tasks(self):
        response = self.client.get('/tasks/?json=1&limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting list of tasks
        self.assertIsInstance(data, list)

    def test_task(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/task/' + str(self.test_data['jeditaskid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_task(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/task/' + str(self.test_data['jeditaskid']) + '/?json=1&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('task', 'taskparams', 'datasets')
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_task_getjobsummaryfortask(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/getjobsummaryfortask/{}/?infotype=jobsummary&{}'.format(
            str(self.test_data['jeditaskid']),
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    def test_task_taskstatuslog(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/taskstatuslog/' + str(self.test_data['jeditaskid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_task_taskprofile(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/taskprofile/' + str(self.test_data['jeditaskid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_taskprofiledata(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/task/' + str(self.test_data['jeditaskid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_task_taskflow(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/taskflow/' + str(self.test_data['jeditaskid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # sites
    def test_sites(self):
        response = self.client.get('/sites/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        
    def test_site(self):
        response = self.client.get('/site/' + str(self.test_data['computingsite']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # this is for prefill selection of pqs in search by site in top bar
    def test_api_get_sites(self):
        self.headers.update({'x-requested-with': 'XMLHttpRequest'})
        response = self.client.post('/api/get_sites/?' + self.timestamp_str, headers=self.headers)
        self.assertEqual(response.status_code, 200)

    # worker nodes
    def test_wns(self):
        response = self.client.get('/wns/' + str(self.test_data['computingsite']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_wn(self):
        if self.test_data['wn'] is None or not isinstance(self.test_data['wn'], str) or len(self.test_data['wn']) < 1:
            self.skipTest('skipping as there is no modificationhost for the selected job')
        response = self.client.get('/wn/{}/{}/?{}'.format(
            str(self.test_data['computingsite']),
            self.test_data['wn'],
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    # users
    def test_users(self):
        response = self.client.get('/users/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_user(self):
        response = self.client.get('/user/' + str(self.test_data['produsername']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_user(self):
        response = self.client.get('/api/user_dash/' + str(self.test_data['produsername']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('msg', 'data',)
        for k in expected_keys:
            self.assertIn(k, data.keys())

    # user jobs profile
    def test_user_profile(self):
        response = self.client.get('/userProfile/' + str(self.test_data['produsername']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_user_profile(self):
        response = self.client.get('/userProfileData/?username={}&{}'.format(
            str(self.test_data['produsername']),
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    # errors
    def test_errors(self):
        response = self.client.get('/errors/?limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_errors_fields_specified(self):
        # expecting particular structure of the response data
        expected_keys = ('jobSummary', 'errsByCount', 'errsBySite', 'errsByUser', 'errsByTask')
        response = self.client.get('/errors/?json=1&limit=10&fields={}&{}'.format(
            ','.join(expected_keys),
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_api_errors_no_fields_specified(self):
        # expecting list of jobs
        response = self.client.get('/errors/?json=1&limit=10&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIsInstance(data, list)

    # files
    def test_files(self):
        self.assertIsInstance(self.test_data['datasetid'], int)
        response = self.client.get('/files/?datasetid=' + str(self.test_data['datasetid']) + '&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_files(self):
        self.assertIsInstance(self.test_data['datasetid'], int)
        response = self.client.get('/loadFileList/' + str(self.test_data['datasetid']) + '/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        # expecting list of files
        data = json.loads(response.content)
        self.assertIsInstance(data, list)

    def test_file(self):
        self.assertIsInstance(self.test_data['lfn'], str)
        response = self.client.get('/file/?pandaid={}&lfn={}&{}'.format(
            str(self.test_data['pandaid']),
            str(self.test_data['lfn']),
            self.timestamp_str
        ))
        self.assertEqual(response.status_code, 200)

    # datasets
    def test_datasets(self):
        self.assertIsInstance(self.test_data['jeditaskid'], int)
        response = self.client.get('/datasets/?jeditaskid={}&{}'.format(
            str(self.test_data['jeditaskid']),
            self.timestamp_str
        ))
        # it's OK if the status code is 302 as if a task has only one dataset the BP redirects it to single dataset page
        self.assertIn(response.status_code, (200, 302))

    def test_dataset(self):
        self.assertIsInstance(self.test_data['datasetid'], int)
        response = self.client.get('/dataset/?datasetid=' + str(self.test_data['datasetid']) + '&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    # region/nucleus dash
    def test_dash_region(self):
        response = self.client.get('/dash/region/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_dash_region(self):
        response = self.client.get('/dash/region/?json=1' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('regions', 'sites', 'queues')
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_dash_nucleus(self):
        response = self.client.get('/dash/world/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_dash_nucleus(self):
        response = self.client.get('/dash/world/?json=1&' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('nucleuses', 'nucleussummary', 'statelist', 'built')
        for k in expected_keys:
            self.assertIn(k, data.keys())

    def test_status_summary(self):
        response = self.client.get('/status_summary/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)

    def test_api_status_summary(self):
        response = self.client.get('/status_summary/api/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        # expecting particular structure of the response data
        expected_keys = ('data',)
        for k in expected_keys:
            self.assertIn(k, data.keys())

    # jedi work queues
    def test_work_queues(self):
        response = self.client.get('/workQueues/?' + self.timestamp_str)
        self.assertEqual(response.status_code, 200)
