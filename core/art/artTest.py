"""
Created on 05.03.2018
:author Tatiana Korchuganova
A artTest class to register the ART jobs in pandabigmon database via special view
"""

import requests


class ArtTest:
    """
    Class ArtTest creates a model of test described by 7 parameters.
    Method __init__ accepts 7 parameters:
    pandaid, testname, nightly_release_short, platform, project, package, nightly_tag
    """
    url = "http://bigpanda.cern.ch/art/registerarttest/?json"
    nattempts = 5
    timeout = 10
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, pandaid, testname, nightly_release_short,platform, project, package, nightly_tag):
        self.pid = pandaid
        self.tn = testname
        self.nrs = nightly_release_short
        self.platform = platform
        self.project = project
        self.package = package
        self.nt = nightly_tag

    def registerArtTest(self):
        """
        registerArtTest()
        Registers a test by sending a POST request to special bigpanda.cern.ch view
        :return: True or False
        """
        payload = {}
        payload['pandaid'] = self.pid
        payload['testname'] = self.tn
        payload['nightly_release_short'] = self.nrs
        payload['platform'] = self.platform
        payload['project'] = self.project
        payload['package'] = self.package
        payload['nightly_tag'] = self.nt

        for i in range(0, self.nattempts):
            try:
                r = requests.post(self.url, data=payload, timeout=self.timeout, headers=self.headers, verify=False)
            except:
                continue
            if r.status_code == 200:
                try:
                    r = r.json()
                except:
                    print 'The response was corrupted'
                    raise
                if 'exit_code' in r and r['exit_code'] == 0:
                    return True
        return False

