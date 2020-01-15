import time

from datetime import timedelta, datetime

import json

import numpy as np

from django.http import HttpResponse

from core.libs.CustomJSONSerializer import NpEncoder

from core.libs.cache import getCacheEntry

from core.pandajob.models import Jobsdefined4, Jobsarchived, Jobswaiting4, Jobsactive4, Jobsarchived4


pandaSites = {}

def getJobsData(request):
    if request.is_ajax():
        try:
            idList = request.GET.get('idtasks', '')
            tasksList = getCacheEntry(request, idList, isData=True)
            results = getJobsInfo(tasksList)
            data = json.dumps(results, cls=NpEncoder)
        except Exception as ex:
            print(ex)
            data = 'fail'
    else:
        data = 'fail'
    return HttpResponse(data, content_type='application/json')

def getJobsInfo(tasksList, exclude={}, extra = "(1=1)"):
    query = {}
    query["jeditaskid__in"] = tasksList
    newquery = query

    jobs = []

    values = 'actualcorecount', 'eventservice', 'specialhandling', 'modificationtime', 'jobsubstatus', 'pandaid', \
             'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', \
             'jobsetid', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'parentid','attemptnr'
    # newquery['jobstatus'] = 'finished'

    # Here we apply sort for implem rule about two jobs in Jobsarchived and Jobsarchived4 with 'finished' and closed statuses

    start = time.time()
    jobs.extend(Jobsdefined4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))

    jobs.extend(Jobsarchived.objects.filter(**newquery).extra(where=[extra]).exclude(**exclude).values(*values))
    end = time.time()
    print(end - start)

    ## drop duplicate jobs
    job1 = {}
    newjobs = []
    for job in jobs:
        pandaid = job['pandaid']
        dropJob = 0
        if pandaid in job1:
            ## This is a duplicate. Drop it.
            dropJob = 1
        else:
            job1[pandaid] = 1
        if (dropJob == 0):
            newjobs.append(job)
    jobs = newjobs


    jobsSet = {}
    newjobs = []

    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}
    cpuTimeCurrent = []
    for job in jobs:

        if not job['pandaid'] in jobsSet:
            jobsSet[job['pandaid']] = job['jobstatus']
            newjobs.append(job)
        elif jobsSet[job['pandaid']] == 'closed' and job['jobstatus'] == 'finished':
            jobsSet[job['pandaid']] = job['jobstatus']
            newjobs.append(job)
        if 'actualcorecount' in job and job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if job['jobstatus'] in ['finished', 'failed'] and 'endtime' in job and 'starttime' in job and job[
            'starttime'] and job['endtime']:
            duration = max(job['endtime'] - job['starttime'], timedelta(seconds=0))
            job['duration'] = duration.days * 24 * 3600 + duration.seconds
            if job['hs06sec'] is None:
                if job['computingsite'] in pandaSites:
                    job['hs06sec'] = (job['duration']) * float(pandaSites[job['computingsite']]['corepower']) * job[
                    'actualcorecount']
                else:
                    job['hs06sec'] = 0
            if job['nevents'] and job['nevents'] > 0:
                cpuTimeCurrent.append(job['hs06sec'] / job['nevents'])
                job['walltimeperevent'] = round(job['duration'] * job['actualcorecount'] / (job['nevents']*1.0), 2)
            hs06sSum['finished'] += job['hs06sec'] if job['jobstatus'] == 'finished' else 0
            hs06sSum['failed'] += job['hs06sec'] if job['jobstatus'] == 'failed' else 0
            hs06sSum['total'] += job['hs06sec']

    jobs = newjobs

    plotsNames = ['maxpss', 'maxpsspercore', 'nevents', 'walltime', 'walltimeperevent', 'hs06s', 'cputime', 'cputimeperevent', 'maxpssf', 'maxpsspercoref', 'walltimef', 'hs06sf', 'cputimef', 'cputimepereventf']
    plotsDict = {}

    for pname in plotsNames:
        plotsDict[pname] = {'sites': {}, 'ranges': {}}

    for job in jobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if job['maxpss'] is not None and job['maxpss'] != -1:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['maxpsspercore']['sites']:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['nevents']['sites']:
                    plotsDict['nevents']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpss']['sites']:
                    plotsDict['maxpss']['sites'][job['computingsite']] = []

                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']].append(job['maxpss'] / 1024 / job['actualcorecount'])
                plotsDict['maxpss']['sites'][job['computingsite']].append(job['maxpss'] / 1024)
                plotsDict['nevents']['sites'][job['computingsite']].append(job['nevents'])
            elif job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['maxpsspercoref']['sites']:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpssf']['sites']:
                    plotsDict['maxpssf']['sites'][job['computingsite']] = []
                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']].append(job['maxpss'] / 1024 / job['actualcorecount'])
                plotsDict['maxpssf']['sites'][job['computingsite']].append(job['maxpss'] / 1024)
        if 'duration' in job and job['duration']:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['walltime']['sites']:
                    plotsDict['walltime']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['hs06s']['sites']:
                    plotsDict['hs06s']['sites'][job['computingsite']] = []
                plotsDict['walltime']['sites'][job['computingsite']].append(job['duration'])
                plotsDict['hs06s']['sites'][job['computingsite']].append(job['hs06sec'])
                if 'walltimeperevent' in job:
                    if job['computingsite'] not in plotsDict['walltimeperevent']['sites']:
                        plotsDict['walltimeperevent']['sites'][job['computingsite']] = []
                    plotsDict['walltimeperevent']['sites'][job['computingsite']].append(job['walltimeperevent'])
            if job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['walltimef']['sites']:
                    plotsDict['walltimef']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['hs06sf']['sites']:
                    plotsDict['hs06sf']['sites'][job['computingsite']] = []
                plotsDict['walltimef']['sites'][job['computingsite']].append(job['duration'])
                plotsDict['hs06sf']['sites'][job['computingsite']].append(job['hs06sec'])
        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['cputime']['sites']:
                    plotsDict['cputime']['sites'][job['computingsite']] = []
                plotsDict['cputime']['sites'][job['computingsite']].append(job['cpuconsumptiontime'])
                if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                    if job['computingsite'] not in plotsDict['cputimeperevent']['sites']:
                        plotsDict['cputimeperevent']['sites'][job['computingsite']] = []
                    plotsDict['cputimeperevent']['sites'][job['computingsite']].append(round(job['cpuconsumptiontime']/(job['nevents']*1.0), 2))
            if job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['cputimef']['sites']:
                    plotsDict['cputimef']['sites'][job['computingsite']] = []
                plotsDict['cputimef']['sites'][job['computingsite']].append(job['cpuconsumptiontime'])
                if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                    if job['computingsite'] not in plotsDict['cputimepereventf']['sites']:
                        plotsDict['cputimepereventf']['sites'][job['computingsite']] = []
                    plotsDict['cputimepereventf']['sites'][job['computingsite']].append(round(job['cpuconsumptiontime']/(job['nevents']*1.0), 2))

    nbinsmax = 100
    for pname in plotsNames:
        rawdata = []
        for k,d in plotsDict[pname]['sites'].items():
            rawdata.extend(d)
        if len(rawdata) > 0:
            plotsDict[pname]['stats'] = []
            plotsDict[pname]['stats'].append(np.average(rawdata))
            plotsDict[pname]['stats'].append(np.std(rawdata))
            bins, ranges = np.histogram(rawdata, bins='auto')
            if len(ranges) > nbinsmax + 1:
                bins, ranges = np.histogram(rawdata, bins=nbinsmax)
            if pname not in ('walltimeperevent', 'cputimeperevent'):
                plotsDict[pname]['ranges'] = list(np.ceil(ranges))
            else:
                plotsDict[pname]['ranges'] = list(ranges)
            for site in plotsDict[pname]['sites'].keys():
                sitedata = [x for x in plotsDict[pname]['sites'][site]]
                plotsDict[pname]['sites'][site] = list(np.histogram(sitedata, ranges)[0])
        else:
            try:
                del(plotsDict[pname])
            except:
                pass


    # transactionKey = -1
    # if isEventServiceFlag and not isESMerge:
    #     print ('getting events states summary')
    #     if mode == 'drop' and len(jobs) < 400000:
    #         esjobs = []
    #         for job in jobs:
    #             esjobs.append(job['pandaid'])
    #
    #         random.seed()

            # if dbaccess['default']['ENGINE'].find('oracle') >= 0:
            #     tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
            # else:
            #     tmpTableName = "TMP_IDS1DEBUG"

            # transactionKey = random.randrange(1000000)
#            connection.enter_transaction_management()
#             new_cur = connection.cursor()
            # executionData = []
            # for id in esjobs:
            #     executionData.append((id, transactionKey, timezone.now().strftime(defaultDatetimeFormat) ))
            # query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY,INS_TIME) VALUES (%s, %s, %s)"""
            # new_cur.executemany(query, executionData)

    return plotsDict