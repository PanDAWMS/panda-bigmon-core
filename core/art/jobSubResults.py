

import urllib, urllib3, json
def getJobReport(guid, lfn, scope):
    filebrowserURL = "http://bigpanda.cern.ch/filebrowser/"  # This is deployment specific because memory monitoring is intended to work in ATLAS
    jobSubResult = []
    http = urllib3.PoolManager()
    resp = http.request('GET', filebrowserURL, fields={'guid': guid, 'lfn': lfn, 'scope': scope, 'json': 1})
    if resp and len(resp.data) > 0:
        try:
            data = json.loads(resp.data)
            HOSTNAME = data['HOSTNAME']
            tardir = data['tardir']
            MEDIA_URL = data['MEDIA_URL']
            dirprefix = data['dirprefix']
            files = data['files']
            files = [f for f in files if 'jobReport.json' in f['name']]
        except:
            return -2
    else:
        return -2

    urlBase = "http://" + HOSTNAME + "/" + MEDIA_URL + dirprefix + "/" + tardir

    for f in files:
        url = urlBase + "/" + f['name']
        response = http.request('GET', url)
        data = json.loads(response.data)
    if 'art' in data:
        jobSubResult = data['art']
    return jobSubResult