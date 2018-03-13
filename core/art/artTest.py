### A artTest class to register the ART jobs in pandabigmon database via special view
### Possible usage example
# if ArtTest(XXX, 'test_example.sh').registerArtTest():
#     print 'success'
# else:
#     print 'fail'

import requests


class ArtTest:
    url = "http://bigpanda.cern.ch/art/registerarttest/?json"
    nattempts = 2
    timeout = 10

    def __init__(self, pandaid, testname):
        self.pid = pandaid
        self.tn = testname

    def registerArtTest(self):
        payload = {'pandaid': self.pid, 'testname': self.tn}
        for i in range(0, self.nattempts):
            print ('%i attempt to register test' % (i+1))
            r = requests.post(self.url, data=payload, timeout=self.timeout, verify=False)
            if r.status_code == 200:
                r = r.json()
                if 'exit_code' in r and r['exit_code'] == 0:
                    print (r['message'])
                    return True
        print ('%i attempts to register test failed' % (i+1))
        return False


# if ArtTest(3860537803, 'test_example.sh').registerArtTest():
#     print 'success'
# else:
#     print 'fail'