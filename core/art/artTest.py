### A artTest class to register the ART jobs in pandabigmon database via special view
### Possible usage example
# if ArtTest(XXX, 'test_example.sh').registerArtTest():
#     print 'success'
# else:
#     print 'fail'

import requests


class ArtTest:
    def __init__(self, pandaid, testname):
        self.pid = pandaid
        self.tn = testname

    def registerArtTest(self):
        url = "http://bigpanda.cern.ch/art/registerarttest/?json"
        headers = {}
        payload = {'pandaid': self.pid, 'testname': self.tn}
        nattempts = 2
        timeout = 10
        for i in range(0, nattempts):
            print i
            r = requests.post(url, data=payload, timeout=timeout, verify=False)
            print r.status_code
            if r.status_code == 200:
                r = r.json()
                if 'exit_code' in r and r['exit_code'] == 0:
                    return True
        return False


# if ArtTest(3860537803, 'test_example.sh').registerArtTest():
#     print 'success'
# else:
#     print 'fail'