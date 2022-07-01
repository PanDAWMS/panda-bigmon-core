#!/usr/bin/env python
from multiprocessing import Pool
from datetime import datetime
from random import randint
import urllib2, socket, time, os, commands
from urllib2 import HTTPError, URLError

numOfProcesses = 10  # be careful with this value on production systems
timeOutForQuery = 300  # in seconds

monHost = 'aipanda163.cern.ch'
monPort = '8007'

# monHost = 'localhost'
# monPort = '10001'

# monHost = 'bigpanda.cern.ch'
# monHost = 'aipanda022.cern.ch'
# monPort = '80'

if "PANDAMON_HOST" in os.environ:
    monHost = os.environ['PANDAMON_HOST']

if "PANDAMON_PORT" in os.environ:
    monPort = os.environ['PANDAMON_PORT']

monURL = 'http://%s:%s' % (monHost, monPort)

tests = [
    {'url': '/?', 'isJSON': False},
    {'url': '/job?pandaid=2424153059', 'isJSON': False},
    {'url': '/workingGroups/?', 'isJSON': False},
    {'url': '/jobs/?display_limit=100&limit=100', 'isJSON': False},
    {'url': '/jobs/?jobtype=analysis&display_limit=100&limit=100', 'isJSON': False},
    {'url': '/tasks/?eventservice=1&display_limit=100&limit=100', 'isJSON': False},
    {'url': '/task/10593330/?', 'isJSON': False},
    {'url': '/task/10569749/?', 'isJSON': False},
    {'url': '/errors/?sortby=count&limit=100', 'isJSON': False},
    {'url': '/errors/?produsername=gangarbt&jobstatus=failed|holding&piloterrorcode=1099&hours=12&limit=100', 'isJSON': False},
    {'url': '/user/gangarbt/?display_limit=100&limit=100', 'isJSON': False},
    {'url': '/sites/?cloud=CA', 'isJSON': False},
    {'url': '/workQueues/?', 'isJSON': False},
    {'url': '/incidents/?', 'isJSON': False},
    {'url': '/logger/?', 'isJSON': False},
    {'url': '/?mode=quicksearch', 'isJSON': False},
    {'url': '/wns/ANALY_SFU/?', 'isJSON': False},
    {'url': '/jobs/?cloud=All&jobtype=analysis&hours=1&jobtype=analysis&display_limit=100&limit=100',
     'isJSON': False},
    {'url': '/jobs/?display_limit=100&jobstatus=failed&transexitcode=6&display_limit=100&specialhandling=ddm:rucio',
        'isJSON': False},
    {'url': '/job?pandaid=3205354738', 'isJSON': False},
    {'url': '/tasks/?parent_tid=10568751&display_limit=100', 'isJSON': False},
    {'url': '/jobs/?jobname=3cc1b8b7-094c-4de8-aca6-007344563ded_63147&taskid=None&sortby=attemptnr',
     'isJSON': False},
    {'url': '/user/gangarbt/?display_limit=100&limit=100', 'isJSON': False},
    {'url': '/jobs/?jeditaskid=10514057&eventservice=not2&display_limit=100', 'isJSON': False},
    {'url': '/jobs/?jeditaskid=10514057&eventservice=not2&display_limit=100&jobstatus=failed&taskbuffererrorcode=114&display_limit=100',
        'isJSON': False},
    {'url': '/incidents/?', 'isJSON': False},
    {'url': '/incidents/?type=queuecontrol', 'isJSON': False},
    {'url': '/incidents/?site=ITEP_MCORE', 'isJSON': False},
    {'url': '/jobs/?jobtype=test&display_limit=100&jobstatus=failed&exeerrorcode=65&display_limit=100',
     'isJSON': False},
    {'url': '/incidents/?site=ITEP_MCORE', 'isJSON': False},
    {'url': '/taskprofileplot/?jeditaskid=10557536', 'isJSON': False},
    {'url': '/datasetList/?containername=data16_13TeV.00309375.physics_Main.merge.DAOD_HIGG2D1.f749_m1684_p2950',
        'isJSON': False},
    {'url': '/fileInfo/?lfn=group.det-muon.10505609.EXT0._000009.HITS.pool.root&scope=group.det-muon',
     'isJSON': False},
    {'url': '/jobs/?jobtype=groupproduction&display_limit=100&computingsite=AGLT2_LMEM', 'isJSON': False}
]

extendedtests = [
    {'url': '/jobs/?jobparam=*14_13TeV*|14_15TeV', 'isJSON': False},
    {'url': '/jobs/?jobparam=*14_13TeV*|14_15TeV', 'isJSON': True},
    {'url': '/jobs/?jeditaskid=4862100', 'isJSON': True},
    {'url': '/tasks/?taskname=mc12_8TeV.227851.MadGraphPythia_AUET2B_CTEQ6L1_pMSSM_QCD_381934528_METFilter.merge.e3470_a220_a263_a264_r4540_p1328',
        'isJSON': True},
    {'url': '/task/?jeditaskid=1592496', 'isJSON': True},
    {'url': '/status_summary/?', 'isJSON': False},
    {'url': '/dash/region/?', 'isJSON': False},
    {'url': '/runningprodtasks/?precet=MC', 'isJSON': False},
    {'url': '/runningprodtasks/?preset=DPD', 'isJSON': False},
    {'url': '/runningprodtasks/?preset=DATA', 'isJSON': False},
    {'url': '/jobs/?cloud=&jobtype=analysis&hours=3&jobtype=analysis&mismatchedcloudsite=true&display_limit=100&atlasrelease=Atlas-17.7.3',
        'isJSON': False},
    {'url': '/jobs/?cloud=&jobtype=analysis&hours=3&jobtype=analysis&mismatchedcloudsite=true&display_limit=100&atlasrelease=Atlas-17.7.3&atlasrelease=Atlas-17.7.3&atlasrelease=Atlas-17.7.3',
        'isJSON': False},
    {'url': '/user/atlas-dpd-production/?display_limit=100&limit=100', 'isJSON': False}
]


tests = tests + extendedtests

def runTest(x):
    urlToTest = monURL + tests[x]['url']
    if urlToTest.endswith('?'):
        urlToTest += 'timestamp={}'.format(randint(1, 999999))
    else:
        urlToTest += '&timestamp={}'.format(randint(1, 999999))
    print("Testing %s...  %s" % (datetime.today().strftime("%Y-%m-%d %H:%M:%S %Z"), urlToTest))
    if tests[x]['isJSON'] is False:
        headers = {'Accept': 'text/html'}
    else:
        headers = {'Accept': 'application/json'}
    req = urllib2.Request(urlToTest, headers=headers)
    starttime = time.time()
    testResult = {}
    testResult['url'] = urlToTest
    try:
        result = urllib2.urlopen(req, timeout=timeOutForQuery)
    except socket.timeout as e:
        testResult['status'] = -1
    except HTTPError as e:
        testResult['status'] = e.code
    except URLError as e:
        testResult['status'] = e.reason
    else:
        testResult['status'] = result.getcode()
    endtime = time.time()
    testResult['timeToPerform'] = endtime - starttime
    return testResult


if __name__ == '__main__':
    pool = Pool(processes=numOfProcesses)
    #    result = pool.apply_async(runTest, range(len(tests)))
    outputList = pool.map(runTest, range(len(tests)))
    print("\nTest result:")
    countErrors = 0
    for outout in outputList:
        if (outout['status'] != 200):
            print(outout['url'] + " failed with status " + str(outout['status']))
            countErrors += 1
    print('All tests passed' if countErrors == 0 else ' -1 status means exceding time limit')
    print("\nTop 10 of the heaviest queries:")
    outputList = sorted(outputList, key=lambda x: -x['timeToPerform'], reverse=False)
    for outout in outputList[0:9]:
        print(outout['url'] + ' : ' + str(int(outout['timeToPerform'])) + ' sec')

