#!/usr/bin/env python
from multiprocessing import Pool
from datetime import datetime, timedelta
import urllib2, socket, time, os, commands
from urllib2 import HTTPError


numOfProcesses = 10 # be careful with this value on production systems
timeOutForQuery = 301 # in seconds
#monHost = 'aipanda022.cern.ch'
#monPort = '80'

monHost = 'localhost'
monPort = '8080'


if "PANDAMON_HOST" in os.environ:
        monHost = os.environ['PANDAMON_HOST']

if "PANDAMON_PORT" in os.environ:        
        monPort = os.environ['PANDAMON_PORT']

monURL = 'http://%s:%s' % ( monHost, monPort )

tests = [
        {'url' :'/', 'isJSON': False},
        {'url' :'/job?pandaid=2424153059', 'isJSON': False},  
        {'url' :'/dash/', 'isJSON': False}, 
        {'url' :'/workingGroups/', 'isJSON': False},  
        {'url' :'/jobs/?display_limit=100&limit=100', 'isJSON': False},  
        {'url' :'/jobs/?jobtype=analysis&display_limit=100&limit=100', 'isJSON': False}, 
        {'url' :'/tasks/?eventservice=1&display_limit=100&limit=100', 'isJSON': False},   
        {'url' :'/task/4213085/', 'isJSON': False},  
        {'url' :'/errors/?sortby=count&limit=100', 'isJSON': False}, 
        {'url' :'/errors/?computingsite=UKI-SCOTGRID-DURHAM_SL6&produsername=gangarbt&jobstatus=failed|holding&piloterrorcode=1099&hours=12&display_limit=100&limit=100', 'isJSON': False}, 
        {'url' :'/user/simon%20head/?display_limit=100&limit=100', 'isJSON': False},  
        {'url' :'/sites/?cloud=CA', 'isJSON': False}, 
        {'url' :'/workQueues/', 'isJSON': False}, 
        {'url' :'/incidents/', 'isJSON': False},  
        {'url' :'/logger/', 'isJSON': False}, 
        {'url' :'/?mode=quicksearch', 'isJSON': False}, 
        {'url' :'/wns/ANALY_SFU/', 'isJSON': False},   
        {'url' :'/dp/', 'isJSON': False}   
]

extendedtests = [
         { 'url' : '/jobs/?jobparam=*14_13TeV*|14_15TeV', 'isJSON': False},
         { 'url' : '/jobs/?jobparam=*14_13TeV*|14_15TeV', 'isJSON': True},
         { 'url' : '/jobs/?jeditaskid=4862100', 'isJSON': True},
         { 'url' : '/tasks/?taskname=mc12_8TeV.227851.MadGraphPythia_AUET2B_CTEQ6L1_pMSSM_QCD_381934528_METFilter.merge.e3470_a220_a263_a264_r4540_p1328', 'isJSON': True},
         { 'url' : '/task/?jeditaskid=1592496', 'isJSON': True}         
         ]

#tests = tests + extendedtests

def runTest(x):
    urlToTest = monURL+tests[x]['url']
    passed = False
    print "Testing %s...  %s" % ( datetime.today().strftime("%Y-%m-%d %H:%M:%S %Z"), urlToTest )
    if (tests[x]['isJSON'] == False):
        headers = {'Accept' : 'text/html'}
    else:
        headers = {'Accept' : 'application/json'}
    req = urllib2.Request(urlToTest, headers=headers)            
    starttime = time.time()
    testResult = {}
    testResult['url'] = urlToTest
    try:    
        result = urllib2.urlopen(req, timeout = timeOutForQuery)
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
    result = pool.apply_async(runTest, range(len(tests)))   
    outputList=pool.map(runTest, range(len(tests)))
    print "\nTest result:"
    countErrors = 0;
    for outout in outputList:
        if (outout['status'] != 200):
                print outout['url'] + " failed with status " + str(outout['status'])
                countErrors += 1        
    print 'All tests passed' if countErrors == 0 else ' -1 status means exceding time limit'         
    print "\nTop 10 of the heaviest queries:"
    outputList = sorted(outputList, key=lambda x:-x['timeToPerform'], reverse=False)
    for outout in outputList[0:9]:
        print outout['url'] + ' : ' + str(int(outout['timeToPerform'])) + ' sec'
        
