"""
    pbm.tests
    
"""
#import commands
import datetime
import os

#from django.utils import timezone
from django.conf import settings
import unittest2
from django.test.client import Client


class SimplePandaBrokerageMonitorTest(unittest2.TestCase):
    def setUp(self):
        # Every test needs a client.
        self.client = Client()

    @unittest2.skip('skipping on purpose')
    def test_1(self):
        """
            test_1
            
            Test ...
            
        """
        pass

