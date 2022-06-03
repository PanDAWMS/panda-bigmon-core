import unittest
from django.test import Client


class BPCoreTest(unittest.TestCase):
    def setUp(self):
        # Every test needs a client.
        self.client = Client()

    def test_main(self):
        response = self.client.get('/')
        print(response)
        self.assertEqual(response.status_code, 200)

    def test_main_json(self):
        response = self.client.get('/?json')
        print(response)
        self.assertEqual(response.status_code, 200)

    def test_help(self):
        response = self.client.get('/help/')
        self.assertEqual(response.status_code, 200)
