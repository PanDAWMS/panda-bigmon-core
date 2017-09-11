import time

import django.core.exceptions
import commands
import random

# We postpone JSON requests is server is overloaded
# Done for protection from bunch of requests from JSON


class DDOSMiddleware(object):

    sleepInterval = 5 #sec
    maxAllowedHttpProcesses = 300

    def __init__(self):
        pass

    def process_request(self, request):
        if not request.GET.get('json') is None:
            while (sum([float(pf) for pf in commands.getstatusoutput("ps aux | grep httpd | grep -v grep | awk {'print $3'}")[1].split('\n')]) > self.maxAllowedHttpProcesses):
                time.sleep(self.sleepInterval+random.randint(0,10))

