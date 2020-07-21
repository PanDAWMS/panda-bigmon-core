
import time
import copy
import random
import numpy as np
from datetime import datetime, timedelta
from django.db import connection
from django.db.models import Count
from core.common.models import JediEvents, JediDatasetContents
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4
from core.libs.exlib import dictfetchall, insert_to_temp_table
from core.settings.local import defaultDatetimeFormat


def job_summary_for_task(request, query, pandaSites, statelist, extra="(1=1)", isEventServiceFlag=False):
    """An attempt to rewrite it moving dropping to db request level"""

    jobScoutTypes = ['cpuTime', 'walltime', 'ramCount', 'ioIntensity', 'outDiskCount']
    jobScoutIDs = {}
    for jst in jobScoutTypes:
        jobScoutIDs[jst] = []

    # hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}
    cpuTimeCurrent = []

    plotsNames = ['maxpss', 'maxpsspercore', 'nevents', 'walltime', 'walltimeperevent', 'hs06s', 'cputime',
                  'cputimeperevent', 'maxpssf', 'maxpsspercoref', 'walltimef', 'hs06sf', 'cputimef', 'cputimepereventf']
    plotsDict = {}

    newquery = copy.deepcopy(query)

    values = 'actualcorecount', 'eventservice', 'modificationtime', 'jobsubstatus', 'pandaid', 'jobstatus', 'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', 'jobmetrics', 'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime'


    # Here we apply sort for implem rule about two jobs in Jobsarchived and Jobsarchived4 with 'finished' and closed statuses
    jobs = []
    start = time.time()
    jobs.extend(Jobsarchived.objects.filter(**newquery).extra(where=[extra]).values(*values))

    jobs.extend(Jobsdefined4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    end = time.time()
    print("Jobs selection: {} sec".format(end - start))

    # drop duplicate jobs
    job1 = {}
    newjobs = []
    for job in jobs:
        pandaid = job['pandaid']
        dropJob = 0
        if pandaid in job1:
            # This is a duplicate. Drop it.
            dropJob = 1
        else:
            job1[pandaid] = 1
        if (dropJob == 0):
            newjobs.append(job)
    jobs = newjobs

    # divide jobs into ordinary and merge (pmerge for non ES tasks and es_merge for ES tasks)
    ojobs = []
    mjobs = []

    if not isEventServiceFlag:
        for job in jobs:
            if job['processingtype'] == 'pmerge':
                mjobs.append(job)
            else:
                ojobs.append(job)
    else:
        for job in jobs:
            if job['eventservice'] == 2 or job['eventservice'] == 'esmerge':
                mjobs.append(job)
            else:
                ojobs.append(job)



    # no plots, hs06 and scouts searching for merge jobs needed

    for job in ojobs:
        for jst in jobScoutTypes:
            if 'scout='+jst in job['jobmetrics'] or ('scout=' in job['jobmetrics'] and jst in job['jobmetrics'][job['jobmetrics'].index('scout='):]):
                jobScoutIDs[jst].append(job['pandaid'])

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
                job['walltimeperevent'] = round(job['duration'] * job['actualcorecount'] / (job['nevents'] * 1.0), 2)
            # hs06sSum['finished'] += job['hs06sec'] if job['jobstatus'] == 'finished' else 0
            # hs06sSum['failed'] += job['hs06sec'] if job['jobstatus'] == 'failed' else 0
            # hs06sSum['total'] += job['hs06sec']



    for pname in plotsNames:
        plotsDict[pname] = {'sites': {}, 'ranges': {}}

    for job in ojobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if job['jobstatus'] == 'finished':
            if job['computingsite'] not in plotsDict['nevents']['sites']:
                plotsDict['nevents']['sites'][job['computingsite']] = []
            plotsDict['nevents']['sites'][job['computingsite']].append(job['nevents'])
        if job['maxpss'] is not None and job['maxpss'] != -1:
            if job['jobstatus'] == 'finished':
                if job['computingsite'] not in plotsDict['maxpsspercore']['sites']:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpss']['sites']:
                    plotsDict['maxpss']['sites'][job['computingsite']] = []
                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercore']['sites'][job['computingsite']].append(
                        job['maxpss'] / 1024 / job['actualcorecount'])
                plotsDict['maxpss']['sites'][job['computingsite']].append(job['maxpss'] / 1024)
            elif job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['maxpsspercoref']['sites']:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']] = []
                if job['computingsite'] not in plotsDict['maxpssf']['sites']:
                    plotsDict['maxpssf']['sites'][job['computingsite']] = []
                if job['actualcorecount'] and job['actualcorecount'] > 0:
                    plotsDict['maxpsspercoref']['sites'][job['computingsite']].append(
                        job['maxpss'] / 1024 / job['actualcorecount'])
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
                    plotsDict['cputimeperevent']['sites'][job['computingsite']].append(
                        round(job['cpuconsumptiontime'] / (job['nevents'] * 1.0), 2))
            if job['jobstatus'] == 'failed':
                if job['computingsite'] not in plotsDict['cputimef']['sites']:
                    plotsDict['cputimef']['sites'][job['computingsite']] = []
                plotsDict['cputimef']['sites'][job['computingsite']].append(job['cpuconsumptiontime'])
                if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                    if job['computingsite'] not in plotsDict['cputimepereventf']['sites']:
                        plotsDict['cputimepereventf']['sites'][job['computingsite']] = []
                    plotsDict['cputimepereventf']['sites'][job['computingsite']].append(
                        round(job['cpuconsumptiontime'] / (job['nevents'] * 1.0), 2))

    # creating nevents piechart
    if 'nevents' in plotsDict and 'sites' in plotsDict['nevents'] and len(plotsDict['nevents']['sites']) > 0:
        plotsDict['neventsbysite'] = {}
        for site, neventslist in plotsDict['nevents']['sites'].items():
            plotsDict['neventsbysite'][str(site)] = sum(neventslist)


    # creation of bins for histograms
    nbinsmax = 100
    for pname in plotsNames:
        rawdata = []
        for k, d in plotsDict[pname]['sites'].items():
            rawdata.extend(d)
        if len(rawdata) > 0:
            plotsDict[pname]['stats'] = []
            plotsDict[pname]['stats'].append(np.average(rawdata))
            plotsDict[pname]['stats'].append(np.std(rawdata))
            try:
                bins, ranges = np.histogram(rawdata, bins='auto')
            except MemoryError:
                bins, ranges = np.histogram(rawdata, bins=nbinsmax)
            if len(ranges) > nbinsmax + 1:
                bins, ranges = np.histogram(rawdata, bins=nbinsmax)
            plotsDict[pname]['ranges'] = list(np.ceil(ranges))
            for site in plotsDict[pname]['sites'].keys():
                sitedata = [x for x in plotsDict[pname]['sites'][site]]
                plotsDict[pname]['sites'][site] = list(np.histogram(sitedata, ranges)[0])
        else:
            try:
                del (plotsDict[pname])
            except:
                pass


    start = time.time()

    ojobstates = []
    mjobstates = []

    for state in statelist:
        statecount = {}
        statecount['name'] = state
        statecount['count'] = 0
        if len(ojobs) > 0:
            for job in ojobs:
                if job['jobstatus'] == state:
                    statecount['count'] += 1
                    continue
        ojobstates.append(statecount)
        statecount = {}
        statecount['name'] = state
        statecount['count'] = 0
        if len(mjobs) > 0:
            for job in mjobs:
                if job['jobstatus'] == state:
                    statecount['count'] += 1
                    continue
        mjobstates.append(statecount)

    end = time.time()
    print("Jobs states aggregation: {} sec".format(end - start))

    #support of old keys for scouts
    jobScoutIDsNew = {}
    jobScoutIDsNew['cputimescoutjob'] = jobScoutIDs['cpuTime']
    jobScoutIDsNew['walltimescoutjob'] = jobScoutIDs['walltime']
    jobScoutIDsNew['ramcountscoutjob'] = jobScoutIDs['ramCount']
    jobScoutIDsNew['iointensityscoutjob'] = jobScoutIDs['ioIntensity']
    jobScoutIDsNew['outdiskcountscoutjob'] = jobScoutIDs['outDiskCount']

    return plotsDict, ojobstates, mjobstates, jobScoutIDsNew


def event_summary_for_task(mode, query, transactionKeyDroppedJobs):
    """

    :param transactionKeyDroppedJobs:
    :return: number of events in different states
    """
    eventservicestatelist = ['ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed',
                             'fatal', 'merged', 'corrupted']
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    print ('getting events states summary')
    if mode == 'drop':
        jeditaskid = query['jeditaskid']
        equerystr = """
        SELECT 
        /*+ cardinality(tmp 10) INDEX_RS_ASC(ev JEDI_EVENTS_PK) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  
            SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS EVCOUNT, 
            ev.STATUS 
        FROM ATLAS_PANDA.JEDI_EVENTS ev, 
            (select ja4.pandaid from ATLAS_PANDA.JOBSARCHIVED4 ja4 
                    where ja4.jeditaskid = {} and ja4.eventservice is not NULL and ja4.eventservice != 2 
                        and ja4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union 
            select ja.pandaid from ATLAS_PANDAARCH.JOBSARCHIVED ja 
                where ja.jeditaskid = {} and ja.eventservice is not NULL and ja.eventservice != 2 
                    and ja.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jav4.pandaid from ATLAS_PANDA.jobsactive4 jav4 
                where jav4.jeditaskid = {} and jav4.eventservice is not NULL and jav4.eventservice != 2 
                    and jav4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jw4.pandaid from ATLAS_PANDA.jobswaiting4 jw4 
                where jw4.jeditaskid = {} and jw4.eventservice is not NULL and jw4.eventservice != 2 
                    and jw4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jd4.pandaid from ATLAS_PANDA.jobsdefined4 jd4 
                where jd4.jeditaskid = {} and jd4.eventservice is not NULL and jd4.eventservice != 2 
                    and jd4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            )  j
        WHERE ev.PANDAID = j.pandaid AND ev.jeditaskid = {} 
        GROUP BY ev.STATUS
        """.format(jeditaskid, transactionKeyDroppedJobs, jeditaskid, transactionKeyDroppedJobs,
                   jeditaskid, transactionKeyDroppedJobs, jeditaskid, transactionKeyDroppedJobs,
                   jeditaskid, transactionKeyDroppedJobs, jeditaskid)
        new_cur = connection.cursor()
        new_cur.execute(equerystr)

        evtable = dictfetchall(new_cur)

        for ev in evtable:
            essummary[eventservicestatelist[ev['STATUS']]] += ev['EVCOUNT']
    if mode == 'nodrop':
        event_counts = []
        equery = {'jeditaskid': query['jeditaskid']}
        event_counts.extend(
            JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
        for state in event_counts:
            essummary[eventservicestatelist[state['status']]] = state['count']

    # creating ordered list of eventssummary
    for state in eventservicestatelist:
        eventstatus = {}
        eventstatus['statusname'] = state
        eventstatus['count'] = essummary[state]
        eventslist.append(eventstatus)

    return eventslist


def input_summary_for_task(taskrec, dsets):
    """
    The function returns:
    Input event chunks list for separate table
    Input event chunks summary by states
    A dictionary with tk as key and list of input files IDs that is needed for jobList view filter
    """
    jeditaskid = taskrec['jeditaskid']
    # Getting statuses of inputfiles
    if taskrec['creationdate'] < datetime.strptime('2018-10-22 10:00:00', defaultDatetimeFormat):
        ifsquery = """
            select  
            ifs.jeditaskid,
            ifs.datasetid,
            ifs.fileid,
            ifs.lfn, 
            ifs.startevent, 
            ifs.endevent, 
            ifs.attemptnr, 
            ifs.maxattempt, 
            ifs.failedattempt, 
            ifs.maxfailure,
            case when cstatus not in ('running') then cstatus 
                 when cstatus in ('running') and esmergestatus is null then cstatus
                 when cstatus in ('running') and esmergestatus = 'esmerge_transferring' then 'transferring' 
                 when cstatus in ('running') and esmergestatus = 'esmerge_merging' then 'merging' 
            end as status
            from (
                select jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus, count(f.esmergestatus) as n
                  from
                (select jd.jeditaskid, jd.datasetid, jdc.fileid, 
                    jdc.lfn, jdc.startevent, jdc.endevent, 
                    jdc.attemptnr, jdc.maxattempt, jdc.failedattempt, jdc.maxfailure,
                    case when (jdc.maxattempt <= jdc.attemptnr or jdc.failedattempt >= jdc.maxfailure) and jdc.status = 'ready' then 'failed' else jdc.status end as cstatus
                 from atlas_panda.jedi_dataset_contents jdc, 
                     atlas_panda.jedi_datasets jd
                 where jd.datasetid = jdc.datasetid 
                    and jd.jeditaskid = {}
                    and jd.masterid is NULL
                    and jdc.type in ( 'input', 'pseudo_input')
                ) jdcf 
                LEFT JOIN
                (select f4.jeditaskid, f4.fileid, f4.datasetid, f4.pandaid, 
                    case when ja4.jobstatus = 'transferring' and ja4.eventservice = 2 then 'esmerge_transferring' when ja4.eventservice = 2 then 'esmerge_merging' else null end as esmergestatus
                 from atlas_panda.filestable4 f4, ATLAS_PANDA.jobsactive4 ja4 
                 where f4.pandaid = ja4.pandaid and f4.type in ( 'input', 'pseudo_input') 
                            and f4.jeditaskid = {}
                ) f
                on jdcf.datasetid = f.datasetid and jdcf.fileid = f.fileid
                group by jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus
            ) ifs """.format(jeditaskid, jeditaskid)

        cur = connection.cursor()
        cur.execute(ifsquery)
        inputfiles = cur.fetchall()
        cur.close()

        inputfiles_names = ['jeditaskid', 'datasetid', 'fileid', 'lfn', 'startevent', 'endevent', 'attemptnr',
                            'maxattempt', 'failedattempt', 'maxfailure', 'procstatus']
        inputfiles_list = [dict(zip(inputfiles_names, row)) for row in inputfiles]

    else:
        ifsquery = {}
        ifsquery['jeditaskid'] = jeditaskid
        indsids = [ds['datasetid'] for ds in dsets if ds['type'] == 'input' and ds['masterid'] is None]
        ifsquery['datasetid__in'] = indsids if len(indsids) > 0 else [-1,]
        inputfiles_list = []
        inputfiles_list.extend(JediDatasetContents.objects.filter(**ifsquery).values())

    # counting of files in different states and building list of fileids for jobList
    inputfiles_counts = {}
    inputfilesids_states = {}
    dsids = []
    for inputfile in inputfiles_list:
        if inputfile['procstatus'] not in inputfiles_counts:
            inputfiles_counts[inputfile['procstatus']] = 0
        if inputfile['procstatus'] not in inputfilesids_states:
            inputfilesids_states[inputfile['procstatus']] = []
        if inputfile['datasetid'] not in dsids:
            dsids.append(inputfile['datasetid'])
        inputfiles_counts[inputfile['procstatus']] += 1
        inputfilesids_states[inputfile['procstatus']].append(inputfile['fileid'])

    inputfiles_tk = {}
    ifs_states = ['ready', 'queued', 'running', 'merging', 'transferring', 'finished', 'failed']
    ifs_summary = []
    for ifstate in ifs_states:
        ifstatecount = 0
        tk = random.randrange(100000000)
        if ifstate in inputfiles_counts.keys():
            ifstatecount = inputfiles_counts[ifstate]
            inputfiles_tk[tk] = inputfilesids_states[ifstate]
        ifs_summary.append({'name': ifstate, 'count': ifstatecount, 'tk': tk, 'ds': dsids})

    return inputfiles_list, ifs_summary, inputfiles_tk


def job_summary_for_task_light(taskrec):
    """
    Light version of jobSummary for ES tasks specifically. Nodrop mode by default. See ATLASPANDA-466 for details.
    :param taskrec:
    :return:
    """
    jeditaskidstr = str(taskrec['jeditaskid'])
    statelistlight = ['defined', 'assigned', 'activated', 'starting', 'running', 'holding', 'transferring', 'finished',
                      'failed', 'cancelled']
    estypes = ['es', 'esmerge', 'jumbo']
    jobSummaryLight = {}
    jobSummaryLightSplitted = {}
    for state in statelistlight:
        jobSummaryLight[str(state)] = 0

    for estype in estypes:
        jobSummaryLightSplitted[estype] = {}
        for state in statelistlight:
            jobSummaryLightSplitted[estype][str(state)] = 0

    jsquery = """
        select jobstatus, case eventservice when 1 then 'es' when 5 then 'es' when 2 then 'esmerge' when 4 then 'jumbo' else 'unknown' end, count(pandaid) as njobs from (
        (
        select pandaid, es as eventservice, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
        )
        union all
        (
        select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid
        minus
        select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid and pandaid in (
            select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
            )
        )
        )
        group by jobstatus, eventservice
    """
    cur = connection.cursor()
    cur.execute(jsquery, {'jtid': jeditaskidstr})
    js_count = cur.fetchall()
    cur.close()

    js_count_names = ['state', 'es', 'count']
    js_count_list = [dict(zip(js_count_names, row)) for row in js_count]

    for row in js_count_list:
        if row['state'] in statelistlight:
            if not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLight[row['state']] += row['count']
            if row['es'] in estypes and not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLightSplitted[row['es']][row['state']] += row['count']

    # dict -> list for template
    jobsummarylight = [dict(name=state, count=jobSummaryLight[state]) for state in statelistlight]
    jobsummarylightsplitted = {}
    for estype, count_dict in jobSummaryLightSplitted.items():
        jobsummarylightsplitted[estype] = [dict(name=state, count=count_dict[state]) for state in statelistlight]

    return jobsummarylight, jobsummarylightsplitted


def get_top_memory_consumers(taskrec):

    jeditaskidstr = str(taskrec['jeditaskid'])
    topmemoryconsumedjobs = []
    tmcquerystr = """
    select jeditaskid, pandaid, computingsite, jobmaxpss, jobmaxpss_percore, sitemaxrss, sitemaxrss_percore, maxpssratio 
    from (
        select j.jeditaskid, j.pandaid, j.computingsite, j.jobmaxpss, j.jobmaxpss_percore, s.maxrss as sitemaxrss, 
            s.maxrss/s.corecount as sitemaxrss_percore, j.jobmaxpss_percore/(s.maxrss/s.corecount) as maxpssratio, 
            row_number() over (partition by jeditaskid order by j.jobmaxpss_percore/(s.maxrss/s.corecount) desc) as jobrank
        from atlas_pandameta.schedconfig s,
        (select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDA.jobsarchived4 
            where jeditaskid = :jdtsid and maxrss is not null
        union
        select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDAARCH.jobsarchived 
            where jeditaskid = :jdtsid  and maxrss is not null
        ) j
        where j.computingsite = s.nickname
    ) 
    where jobrank <= 3
    """
    try:
        cur = connection.cursor()
        cur.execute(tmcquerystr, {'jdtsid': jeditaskidstr})
        tmc_list = cur.fetchall()
        cur.close()
    except:
        tmc_list = []
    tmc_names = ['jeditaskid', 'pandaid', 'computingsite', 'jobmaxrss', 'jobmaxpss_percore',
                 'sitemaxrss', 'sitemaxrss_percore', 'maxrssratio']
    topmemoryconsumedjobs = [dict(zip(tmc_names, row)) for row in tmc_list]
    return topmemoryconsumedjobs


def get_harverster_workers_for_task(jeditaskid):

    jsquery = """
        select t4.*, t5.BATCHID, 
        CASE WHEN not t5.ENDTIME is null THEN t5.ENDTIME-t5.STARTTIME
             WHEN not t5.STARTTIME is null THEN (CAST(SYS_EXTRACT_UTC(systimestamp)AS DATE)-t5.STARTTIME)
             ELSE 0
        END AS WALLTIME,
        
        t5.NCORE, t5.NJOBS FROM (
        SELECT HARVESTERID, WORKERID, SUM(nevents) as sumevents from (
        
        select HARVESTERID, WORKERID, t1.PANDAID, t2.nevents from ATLAS_PANDA.harvester_rel_jobs_workers t1 JOIN 
        
                (    select pandaid, nevents from (
                        (
                        select pandaid, nevents from atlas_pandabigmon.combined_wait_act_def_arch4 where eventservice in (1,3,4,5) and jeditaskid = :jtid
                        )
                        union all
                        (
                            select pandaid, nevents from atlas_pandaarch.jobsarchived where eventservice in (1,3,4,5) and jeditaskid = :jtid
                            minus
                            select pandaid, nevents from atlas_pandaarch.jobsarchived where eventservice in (1,3,4,5) and jeditaskid = :jtid and pandaid in (
                                select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 where eventservice in (1,3,4,5) and jeditaskid = :jtid
                                )
                        )
                    )
                )t2 on t1.pandaid=t2.pandaid
        )t3 group by HARVESTERID, WORKERID) t4
        JOIN ATLAS_PANDA.harvester_workers t5 on t5.HARVESTERID=t4.HARVESTERID and t5.WORKERID = t4.WORKERID
    """

    cur = connection.cursor()
    cur.execute(jsquery, {'jtid': jeditaskid})
    harv_workers = cur.fetchall()
    cur.close()

    harv_workers_names = ['harvesterid', 'workerid', 'sumevents', 'batchid', 'walltime', 'ncore', 'njobs']
    harv_workers_list = [dict(zip(harv_workers_names, row)) for row in harv_workers]
    return harv_workers_list


def get_job_state_summary_for_tasklist(tasks):
    """
    Getting job state summary for list of tasks. Nodrop mode only
    :return: taskJobStateSummary : dictionary
    """

    taskids = [int(task['jeditaskid']) for task in tasks]
    trans_key = insert_to_temp_table(taskids)

    jsquery = """
        select  jeditaskid, jobstatus, count(pandaid) as njobs from (
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
        )
        union all
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
        minus
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk ) 
                and pandaid in (
                    select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 
                        where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
            )
        )
        )
        group by jeditaskid, jobstatus
        """
    cur = connection.cursor()
    cur.execute(jsquery, {'tk': trans_key})
    js_count_bytask = cur.fetchall()
    cur.close()

    js_count_bytask_names = ['jeditaskid', 'jobstatus', 'count']
    js_count_bytask_list = [dict(zip(js_count_bytask_names, row)) for row in js_count_bytask]

    # list -> dict
    js_count_bytask_dict = {}
    for row in js_count_bytask_list:
        if row['jeditaskid'] not in js_count_bytask_dict:
            js_count_bytask_dict[row['jeditaskid']] = {}
        if row['jobstatus'] not in js_count_bytask_dict[row['jeditaskid']]:
            js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] = 0
        js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] += int(row['count'])

    return js_count_bytask_dict

