from django.db import connection
import time
from django.shortcuts import render, redirect
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from django.http import HttpResponse
from reportlab.pdfgen import canvas
from django.template import RequestContext, loader
from io import StringIO
import humanize
from django.utils.cache import patch_cache_control, patch_response_headers
import json
import hashlib
from django.conf import settings as djangosettings
from django.core.cache import cache
from django.utils import encoding
from datetime import datetime
from collections import OrderedDict

notcachedRemoteAddress = ['188.184.185.129']

class MC16aCPReport:
    def __init__(self):
        pass

    jobstatelist = ['pending','defined', 'assigned','waiting',    \
                 'activated', 'sent', 'starting', 'running', 'holding', \
                 'transferring', 'merging', 'finished', 'failed', 'cancelled',  'throttled', 'closed']

    taskstatelist = ['registered', 'topreprocess', 'preprocessing', 'defined', 'assigning', 'ready', 'pending', 'scouting', 'scouted', 'throttled', 'running',
                     'prepared', 'failed', 'finished',
                     'done',  'aborting', 'aborted', 'finishing',
                     'tobroken', 'broken', 'toretry', 'toincexec', 'rerefine']

    taskstatelistDEFT = ['waiting', 'registered', 'assigning', 'submitting', 'submitted',  'ready', 'running', 'exhausted',  'done+finished',  'failed', 'toretry', 'broken', 'pending',   'paused', 'aborted','obsolete']

    taskstatelistRecent = [ 'registered*',  'submitting**', 'submitting***', 'waiting**', 'waiting***',  'running**', 'running***', 'done*',  'aborted*', 'failed*', 'finished*', 'broken*',  'exhausted*',  ]


#    steps = ['evgen', 'simul', 'mergeHits', 'recon', 'merge']
#    stepsLabels = {'evgen':'Event Generation', 'simul':'Simulation', 'recon':'Reconstruction', 'merge':'Merge', 'mergeHits':'Hits Merge'}

    steps = ['Evgen', 'Evgen Merge', 'Simul', 'Merge', 'Reco', 'Rec Merge', 'Deriv', 'Deriv Merge']
    stepsLabels = {'Simul':'Simulation', 'Reco':'Reconstruction', 'Rec Merge':'AOD Merge', 'Merge':'HITS Merge', 'Deriv':'Derivation', 'Deriv Merge':'Derivation Merge', 'Evgen Merge':'Evgen Merge', 'Evgen':'Evgen'}


    def jobsQuery(self, condition, campaign='MC16a'):
        """
        TODO: Update HS06SEC summary to use not dropped jobs to calculate HEPSPEC
        """

        sqlRequest = '''
            SELECT STEP, SUM(FINISHEDJEV) as FINISHEDJEV, SUM(PENDINGJEV+DEFINEDJEV+ASSIGNEDJEV+WAITINGJEV+ACTIVATEDJEV+SENTJEV+STARTINGJEV+RUNNINGJEV+HOLDINGJEV+TRANSFERRINGJEV+MERGINGJEV) as RESTJEV, 
            SUM(PENDINGJEV), SUM(DEFINEDJEV), SUM(ASSIGNEDJEV), SUM (WAITINGJEV), SUM(ACTIVATEDJEV), SUM(SENTJEV), SUM(STARTINGJEV), SUM(RUNNINGJEV), SUM(HOLDINGJEV), SUM(TRANSFERRINGJEV), SUM(MERGINGJEV), SUM(FAILEDJEV), SUM(CANCELLEDJEV), SUM(THROTTLEDJEV), SUM(CLOSEDJEV),
            SUM(PENDINGNJ), SUM(DEFINEDNJ), SUM(ASSIGNEDNJ), SUM(WAITINGNJ), SUM(ACTIVATEDNJ), SUM(SENTNJ), SUM(STARTINGNJ), SUM(RUNNINGNJ), SUM(HOLDINGNJ), SUM(TRANSFERRINGNJ),  SUM(MERGINGNJ), SUM(FINISHEDNJ), SUM(FAILEDNJ), SUM(CANCELLEDNJ), SUM(THROTTLEDNJ), SUM(CLOSEDNJ), 
            SUM(INPUTEVENTS), SUM(HS06SECFAILED), SUM(HS06SECCANCELLED), SUM(HS06SECFINISHED), SUM(HS06SECCLOSED), 
            SUM(
                CASE 
                    WHEN STATUS NOT IN ('failed','aborted','broken') THEN NREQUESTEDEV
                    ELSE 0
                END 
            )
            FROM ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE WHERE DEFTSTATUS NOT IN ('failed','aborted','broken') {0} GROUP BY STEP
        '''
        sqlrequestwithcond = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlrequestwithcond)
        rawsummary = cur.fetchall()

        orderedjobssummary = OrderedDict()
        orderedjobsevsummary = OrderedDict()
        inputevents = 0

        for step in self.steps:
            jobstate = OrderedDict()
            for state in self.jobstatelist:
                jobstate[state] = 0
            orderedjobssummary[step] = {state:0}
            orderedjobsevsummary[step] = {state:0}

        for row in rawsummary:
            #if row[0] not in  ['Deriv Merge', 'Deriv']:
                orderedjobssummary[row[0]]['pending'] = row[18]
                orderedjobsevsummary[row[0]]['pending'] = row[3]
                orderedjobssummary[row[0]]['defined'] = row[19]
                orderedjobsevsummary[row[0]]['defined'] = row[4]
                orderedjobssummary[row[0]]['assigned'] = row[20]
                orderedjobsevsummary[row[0]]['assigned'] = row[5]
                orderedjobssummary[row[0]]['waiting'] = row[21]
                orderedjobsevsummary[row[0]]['waiting'] = row[6]
                orderedjobssummary[row[0]]['activated'] = row[22]
                orderedjobsevsummary[row[0]]['activated'] = row[7]
                orderedjobssummary[row[0]]['sent'] = row[23]
                orderedjobsevsummary[row[0]]['sent'] = row[8]
                orderedjobssummary[row[0]]['starting'] = row[24]
                orderedjobsevsummary[row[0]]['starting'] = row[9]
                orderedjobssummary[row[0]]['running'] = row[25]
                orderedjobsevsummary[row[0]]['running'] = row[10]
                orderedjobssummary[row[0]]['holding'] = row[26]
                orderedjobsevsummary[row[0]]['holding'] = row[11]
                orderedjobssummary[row[0]]['transferring'] = row[27]
                orderedjobsevsummary[row[0]]['transferring'] = row[12]
                orderedjobssummary[row[0]]['merging'] = row[28]
                orderedjobsevsummary[row[0]]['merging'] = row[13]
                orderedjobssummary[row[0]]['finished'] = row[29]
                orderedjobsevsummary[row[0]]['finished'] = row[1]
                orderedjobssummary[row[0]]['failed'] = row[30]
                orderedjobsevsummary[row[0]]['failed'] = row[14]
                orderedjobssummary[row[0]]['cancelled'] = row[31]
                orderedjobsevsummary[row[0]]['cancelled'] = row[15]
                orderedjobssummary[row[0]]['throttled'] = row[32]
                orderedjobsevsummary[row[0]]['throttled'] = row[16]
                orderedjobssummary[row[0]]['closed'] = row[33]
                orderedjobsevsummary[row[0]]['closed'] = row[17]
                inputevents = row[34]

        orderedjobssummary['title'] = 'Jobs processing summary'
        orderedjobsevsummary['title'] = 'Events processing summary'

        if campaign == 'MC16c':
            totalEvQuery = 'SELECT SUM(t1.TOTAL_REQ_EVENTS), t3.HASHTAG, t5.STEP_NAME as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_HT_TO_TASK t2, ATLAS_DEFT.T_HASHTAG t3, ' \
                           'ATLAS_DEFT.T_PRODUCTION_STEP t4, ATLAS_DEFT.T_STEP_TEMPLATE t5 ' \
                           'WHERE t3.HT_ID=t2.HT_ID and t1.TASKID=t2.TASKID AND t3.HASHTAG=\'MC16c_CP\' and t1.STEP_ID=t4.STEP_ID and t4.STEP_T_ID=t5.STEP_T_ID and t1.STATUS not in (\'broken\',\'aborted\',\'failed\') ' \
                           'GROUP BY t3.HASHTAG, t5.STEP_NAME'
            cur = connection.cursor()
            cur.execute(totalEvQuery)
            requestSummaryRes = cur.fetchall()
            for requestSummary in requestSummaryRes:
                if requestSummary[1] == 'MC16c_CP' and requestSummary[2] == 'Evgen':
                    inputevents = requestSummary[0]

        #Hepspecs estimation
        hepspecSummary = {'failed': {}, 'cancelled': {}, 'finished': {}, 'closed': {}}
        sqlRequest = '''
            SELECT STEP, SUM(HS06SECFAILED), SUM(HS06SECCANCELLED), SUM(HS06SECFINISHED), SUM(HS06SECCLOSED)
            FROM ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE WHERE (1=1) {0} GROUP BY STEP
        '''
        sqlrequestwithcond = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlrequestwithcond)
        rawhepssummary = cur.fetchall()
        for row in rawhepssummary:
            hepspecSummary['failed'][row[0]] = row[1]
            hepspecSummary['cancelled'][row[0]] = row[2]
            hepspecSummary['finished'][row[0]] = row[3]
            hepspecSummary['closed'][row[0]] = row[4]
        hepspecSummary['title'] = 'HS06SEC summary'

        #Progress calculation
        JediEventsR = {}
        JediEventsR['title'] = 'Overall events processing summary'

        if 'Evgen' in orderedjobsevsummary and 'finished' in orderedjobsevsummary['Evgen']:
            JediEventsR['evgened'] = orderedjobsevsummary['Evgen']['finished']
            JediEventsR['evgenedprogr'] = round(JediEventsR['evgened'] / float(inputevents) * 100, 2)

        if 'Evgen Merge' in orderedjobsevsummary and 'finished' in orderedjobsevsummary['Evgen Merge']:
            JediEventsR['evgenmerged'] = orderedjobsevsummary['Evgen Merge']['finished']
            JediEventsR['evgenmergededprogr'] = round(JediEventsR['evgenmerged'] / float(inputevents) * 100, 2)


        JediEventsR['simulated'] = orderedjobsevsummary['Simul']['finished']
        JediEventsR['simulatedprogr'] = round( JediEventsR['simulated']/float(inputevents)*100, 2)
        if 'finished' in orderedjobsevsummary['Reco']:
            JediEventsR['reconstructed'] = orderedjobsevsummary['Reco']['finished']
        else:
            JediEventsR['reconstructed'] = 0
        JediEventsR['reconstructedprog'] = round(JediEventsR['reconstructed']/float(inputevents)*100, 2)
        JediEventsR['mergeHits'] = orderedjobsevsummary['Merge']['finished']
        JediEventsR['mergeHitsprog'] = round(JediEventsR['mergeHits']/float(inputevents)*100, 2)
        if 'finished' in orderedjobsevsummary['Rec Merge']:
            JediEventsR['merge'] = orderedjobsevsummary['Rec Merge']['finished']
        else:
            JediEventsR['merge'] = 0
        JediEventsR['mergeprog'] = round(JediEventsR['merge']/float(inputevents)*100, 2)
        if 'finished' in orderedjobsevsummary['Deriv']:
            JediEventsR['deriv'] = orderedjobsevsummary['Deriv']['finished']
            JediEventsR['derivprog'] = round(orderedjobsevsummary['Deriv']['finished'] / float(inputevents) * 100, 2)
        else:
            JediEventsR['deriv'] = 0
            JediEventsR['derivprog'] = 0

        if 'finished' in orderedjobsevsummary['Deriv Merge']:
            JediEventsR['derivmerge'] = orderedjobsevsummary['Deriv Merge']['finished']
            JediEventsR['derivmergeprog'] = round(orderedjobsevsummary['Deriv Merge']['finished']/float(inputevents)*100, 2)
        else:
            JediEventsR['derivmerge'] = 0
            JediEventsR['derivmergeprog'] = 0

        JediEventsR['input'] = inputevents

        return (orderedjobssummary, orderedjobsevsummary, hepspecSummary, JediEventsR)


    def getJEDIEventsSummaryRequested(self, condition):
        sqlRequest = '''
          SELECT * FROM (
            SELECT SUM(NEVENTS), SUM(NEVENTSUSED), STEP FROM (
            SELECT t2.nevents, t2.neventsused,
            
                      CASE WHEN t1.TASKNAME LIKE '%.merge.%' AND substr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),instr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'merge'
                      WHEN t1.TASKNAME LIKE '%.merge.%' AND not substr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),instr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'mergeHits'
                      WHEN t1.TASKNAME LIKE '%.recon.%' THEN 'recon'
                      WHEN t1.TASKNAME LIKE '%.simul.%' THEN 'simul'
                      WHEN t1.TASKNAME LIKE '%.evgen.%' THEN 'evgen'
                      END AS STEP
            
            FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2 WHERE  t1.status not in ('failed','aborted','broken') and  t1.JEDITASKID=t2.JEDITASKID and t2.MASTERID IS NULL and t2.TYPE IN ('input', 'pseudo_input') {0} 
            )group by STEP
        )t1 where STEP IS NOT NULL
        '''
        # INPUT_EVENTS, TOTAL_EVENTS, STEP

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        tasks = cur.fetchall()
        summaryInput = {} #Step, INPUT_EVENTS
        summaryProcessed = {} #Step, TOTAL_EVENTS

        for task in tasks:
            step = task[2]
            if step not in summaryInput:
                summaryInput[step] = 0
            if step not in summaryProcessed:
                summaryProcessed[step] = 0

            inputEvent = task[0]
            outputEvent = task[1]
            summaryInput[step] += inputEvent
            summaryProcessed[step] += outputEvent

        fullSummary = {}
        fullSummary['input/processed'] = {}
        fullSummary['input/processed']['evgen'] = '%s/%s' % (  humanize.intcomma(summaryInput['evgen']) if 'evgen' in summaryInput else '-', humanize.intcomma(summaryProcessed['evgen']) if 'evgen' in summaryInput else '-')
        fullSummary['input/processed']['simul'] = '%s/%s' % (  humanize.intcomma(summaryInput['simul']) if 'simul' in summaryInput else '-', humanize.intcomma(summaryProcessed['simul']) if 'simul' in summaryInput else '-')
        fullSummary['input/processed']['recon'] = '%s/%s' % (  humanize.intcomma(summaryInput['recon']) if 'recon' in summaryInput else '-', humanize.intcomma(summaryProcessed['recon']) if 'recon' in summaryInput else '-')
        fullSummary['input/processed']['merge'] = '%s/%s' % (  humanize.intcomma(summaryInput['merge']) if 'merge' in summaryInput else '-', humanize.intcomma(summaryProcessed['merge']) if 'merge' in summaryInput else '-')
        return fullSummary


    def getDEFTEventsSummaryChain(self, condition):
        sqlRequest = '''
          SELECT * FROM (
          SELECT TASKID, PARENT_TID, 
          CASE WHEN TASKNAME LIKE '%.merge.%' AND substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'merge'
          WHEN t1.TASKNAME LIKE '%.merge.%' AND not substr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),instr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'mergeHits'
          WHEN TASKNAME LIKE '%.recon.%' THEN 'recon'
          WHEN TASKNAME LIKE '%.simul.%' THEN 'simul'
          WHEN TASKNAME LIKE '%.evgen.%' THEN 'evgen'
          END AS STEP,
          /*s.INPUT_EVENTS,*/ t.TOTAL_REQ_EVENTS, t.TOTAL_EVENTS, t.STATUS
          FROM ATLAS_DEFT.T_PRODUCTION_TASK t, ATLAS_DEFT.T_PRODUCTION_STEP s WHERE s.STEP_ID=t.STEP_ID and  not TASKNAME LIKE '%valid%' and TASKNAME LIKE 'mc16_%' {0}
        )t1 where STEP IS NOT NULL
        '''

        #TASKID, PARENT_TID, STEP, INPUT_EVENTS, TOTAL_EVENTS, STATUS

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        tasks = cur.fetchall()
        parentIDHash = {}
        processedHash = {}
        summaryInput = {} #Step, INPUT_EVENTS
        summaryProcessed = {} #Step, TOTAL_EVENTS

        for task in tasks:
            if not task[5] in ['failed','aborted','broken']:
                if task[0] != task[1]:
                    parentIDHash[task[0]] = task[1]
                processedHash[task[0]] = task[4]

        for task in tasks:
            if not task[5] in ['failed','aborted','broken']:
                step = task[2]
                if step not in summaryInput:
                    summaryInput[step] = 0
                if step not in summaryProcessed:
                    summaryProcessed[step] = 0

                taskid = task[0]
                inputEvent = task[3]
                outputEvent = task[4]
                #if taskid in parentIDHash and parentIDHash[taskid] in processedHash:
                #    inputEvent = processedHash[parentIDHash[taskid]]

                summaryInput[step] += inputEvent
                summaryProcessed[step] += outputEvent

        fullSummary = {}
        fullSummary['total'] = {}
        fullSummary['total']['evgen'] = '%s/%s' % (  humanize.intcomma(summaryInput['evgen']) if 'evgen' in summaryInput else '-', humanize.intcomma(summaryProcessed['evgen']) if 'evgen' in summaryInput else '-')
        fullSummary['total']['recon'] = '%s/%s' % (  humanize.intcomma(summaryInput['simul']) if 'simul' in summaryInput else '-', humanize.intcomma(summaryProcessed['simul']) if 'simul' in summaryInput else '-')
        fullSummary['total']['simul'] = '%s/%s' % (  humanize.intcomma(summaryInput['recon']) if 'recon' in summaryInput else '-', humanize.intcomma(summaryProcessed['recon']) if 'recon' in summaryInput else '-')
        fullSummary['total']['merge'] = '%s/%s' % (  humanize.intcomma(summaryInput['merge']) if 'merge' in summaryInput else '-', humanize.intcomma(summaryProcessed['merge']) if 'merge' in summaryInput else '-')
        return fullSummary


    def getTasksDEFTSummary(self, condition):
        sqlRequest = '''

            SELECT count(t1.STATUS), t1.STATUS, t3.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID {0} 
            group by t1.STATUS, t3.STEP_NAME
        '''
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()
        orderedsummary = OrderedDict()
        for step in self.steps:
            taskstate = OrderedDict()
            for state in self.taskstatelistDEFT:
                taskstate[state] = 0
            orderedsummary[step] = taskstate
        for summaryRow in campaignsummary:
#            if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                if summaryRow[1] == 'done' or summaryRow[1] == 'finished':
                    orderedsummary[summaryRow[2]]['done+finished'] += summaryRow[0]
                else:
                    if summaryRow[1] in self.taskstatelistDEFT:
                        orderedsummary[summaryRow[2]][summaryRow[1]] += summaryRow[0]
        return orderedsummary


    def topSitesFailureRate(self, topN, condition):
        sqlRequest ='''
            SELECT COUNT(*), PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, STEP, SUM(HS06SEC), COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, HS06SEC, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
              SELECT t1.STATUS, t1.taskid, t3.STEP_NAME as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID {0} 
            )
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS='failed'
            )tt) tb group by COMPUTINGSITE, STEP, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode order by SUM(HS06SEC) desc
        '''
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        errorsSummary = cur.fetchall()
        errorsSummaryList = []
        counter = 0
        for row in errorsSummary:
            errorsDict = {}
            errorsDict['COMPUTINGSITE'] = row[9]
            errorsDict['FAILEDHS06SEC'] = row[8]
            errorsDict['STEP'] = row[7]
            errorsDict['COUNT'] = row[0]
            errorStr = ""
            #PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode
            if row[1] > 0:
                errorStr += " PILOTERRORCODE:"+str(row[1])
            if row[2] > 0:
                errorStr += " TASKBUFFERERRORCODE:"+str(row[2])

            if not row[3] is None and int(row[3]) > 0:
                errorStr += " TRANSEXITCODE:"+str(row[3])
            if row[4] > 0:
                errorStr += " EXEERRORCODE:"+str(row[4])
            if row[5] > 0:
                errorStr += " JOBDISPATCHERERRORCODE:"+str(row[5])
            if row[6] > 0:
                errorStr += " DDMERRORCODE:"+str(row[6])
            errorsDict['ERRORS'] = errorStr
            errorsSummaryList.append(errorsDict)
            counter += 1
            if counter == topN:
                break
        return errorsSummaryList


    def topSitesActivatedRunning(self, topN, condition):
        sqlRequest ='''
            SELECT SA/SR, STEP,COMPUTINGSITE, SA, SR FROM (
            SELECT SUM(ISACTIVATED) as SA, SUM(ISRUNNING) as SR, STEP,COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, ISACTIVATED, ISRUNNING, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
              SELECT t1.STATUS, t1.taskid, t3.STEP_NAME as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID {0} 
            )
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            )tt) tb group by COMPUTINGSITE, STEP)ts WHERE SR > 0 order by SA/SR desc
         '''

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        errorsSummary = cur.fetchall()
        errorsSummaryList = []
        counter = 0
        for row in errorsSummary:
            rowDict = {"COMPUTINGSITE":row[2], "STEP":row[1], "acttorun":row[0], "SA":row[3], "SR":row[4]}
            errorsSummaryList.append(rowDict)
            counter += 1
            if counter == topN:
                break
        return errorsSummaryList


    def topSitesAssignedRunning(self, topN, condition):
        sqlRequest ='''
            SELECT SA/SR, STEP,COMPUTINGSITE, SA, SR FROM (
            SELECT SUM(ISASSIGNED) as SA, SUM(ISRUNNING) as SR, STEP,COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, ISASSIGNED, ISRUNNING, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
              SELECT t1.STATUS, t1.taskid, t3.STEP_NAME as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID {0} 
            )
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.taskid=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            )tt) tb group by COMPUTINGSITE, STEP)ts WHERE SR > 0 order by SA/SR desc
         '''

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        errorsSummary = cur.fetchall()
        errorsSummaryList = []
        counter = 0
        for row in errorsSummary:
            rowDict = {"COMPUTINGSITE":row[2], "STEP":row[1], "acttorun":row[0], "SA":row[3], "SR":row[4] }
            errorsSummaryList.append(rowDict)
            counter += 1
            if counter == topN:
                break
        return errorsSummaryList


    def topTasksWithFailedSuccessRat(self, condition):
        sqlRequest ='''
            SELECT COUNT(*), PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, STEP, SUM(HS06SEC), COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, HS06SEC, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE  TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE  TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE  TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE  TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE  TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            )
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS='failed'
            UNION ALL
            SELECT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, CASE WHEN HS06SEC is null THEN 0 ELSE HS06SEC END as HS06SEC, JOBSTATUS, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS='failed'
            )tt) tb group by COMPUTINGSITE, STEP, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode order by SUM(HS06SEC) desc
        '''
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        #SUM(HS06SEC)


    def prepareReportJEDI(self, request):

        requestList = [11035, 11034, 11048, 11049, 11050, 11051, 11052, 11198, 11197, 11222, 11359]
        requestList = '(' + ','.join(map(str, requestList)) + ')'

        data = self.getCacheEntry(request, "prepareReportMC16", skipCentralRefresh=True)
        #data = None
        if data is not None:
            data = json.loads(data)
            data['request'] = request
            response = render(request, 'reportCampaign.html', data, RequestContext(request))
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response

        (totalJobs, totalEvents, hepspecJobs, JediEventsR) = self.jobsQuery('and REQUESTID IN %s' % requestList)

        recentTasks = self.recentProgressReportDEFT('and t1.PR_ID IN %s' % requestList)
        recentTasks['title'] = 'Tasks updated during last 24 hours'
        totalTasks = self.getTasksDEFTSummary('and t1.PR_ID IN %s' % requestList)
        totalTasks['title'] = 'Tasks processing summary'

        worstSite = self.topSitesFailureRate(10, 'and t1.PR_ID IN %s' % requestList)
        siteActRun = self.topSitesActivatedRunning(10, 'and t1.PR_ID IN %s' % requestList)
        siteAssignRun = self.topSitesAssignedRunning(10, 'and t1.PR_ID IN %s' % requestList)

        data = {
                'title':'MC16a',
                "requestList":requestList,
                'viewParams': request.session['viewParams'],
                "JediEventsR":JediEventsR,
                "totalEvents": totalEvents,
                "totalTasks":totalTasks,
                "totalJobs":totalJobs,
                #"JediEventsHashsTable":jediEventsHashsTable,
                #"hashTable":hashTable,
                "recentTasks":recentTasks,
                "hepspecJobs":hepspecJobs,
                "worstSite":worstSite,
                "siteActRun":siteActRun,
                "siteAssignRun":siteAssignRun,
                "taskstatelist":self.taskstatelist,
                "taskstatelistDEFT": self.taskstatelistDEFT,
                "jobstatelist": self.jobstatelist,
                "steps":self.steps,
                "stepsLabels":self.stepsLabels,
                "taskstatelistRecent":self.taskstatelistRecent,
                "built": datetime.now().strftime("%d %b %Y %H:%M:%S")}
        self.setCacheEntry(request, "prepareReportMC16", json.dumps(data, cls=self.DateEncoder), 180*60)
        return render(request, 'reportCampaign.html', data, RequestContext(request))


    def prepareReportJEDIMC16c(self, request):
        selectCond = "(TASKID IN (SELECT t2.TASKID FROM ATLAS_DEFT.T_HT_TO_TASK t2, ATLAS_DEFT.T_HASHTAG t3 WHERE t2.HT_ID=t3.HT_ID AND t3.HASHTAG='MC16c_CP' AND t3.HASHTAG IS NOT NULL))"
        data = self.getCacheEntry(request, "prepareReportMC16c")
        if data is not None:
            data = json.loads(data)
            data['request'] = request
            response = render(request, 'reportCampaignHash.html', data, RequestContext(request))
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response

        (totalJobs, totalEvents, hepspecJobs, JediEventsR) = self.jobsQuery('and %s' % selectCond, campaign='MC16c')

        recentTasks = self.recentProgressReportDEFT('and %s' % selectCond)
        recentTasks['title'] = 'Tasks updated during last 24 hours'
        totalTasks = self.getTasksDEFTSummary('and %s' % selectCond)
        totalTasks['title'] = 'Tasks processing summary'

        worstSite = self.topSitesFailureRate(10, 'and %s' % selectCond)
        siteActRun = self.topSitesActivatedRunning(10, 'and %s' % selectCond)
        siteAssignRun = self.topSitesAssignedRunning(10, 'and %s' % selectCond)

        data = {
                'title': 'MC16c',
                'primaryHash':'MC16c_CP',
                "selectCond":selectCond,
                'viewParams': request.session['viewParams'],
                "JediEventsR":JediEventsR,
                "totalEvents": totalEvents,
                "totalTasks":totalTasks,
                "totalJobs":totalJobs,
                #"JediEventsHashsTable":jediEventsHashsTable,
                #"hashTable":hashTable,
                "recentTasks":recentTasks,
                "hepspecJobs":hepspecJobs,
                "worstSite":worstSite,
                "siteActRun":siteActRun,
                "siteAssignRun":siteAssignRun,
                "taskstatelist":self.taskstatelist,
                "taskstatelistDEFT": self.taskstatelistDEFT,
                "jobstatelist": self.jobstatelist,
                "steps":self.steps,
                "stepsLabels":self.stepsLabels,
                "taskstatelistRecent":self.taskstatelistRecent,
                "built": datetime.now().strftime("%d %b %Y %H:%M:%S")}
        self.setCacheEntry(request, "prepareReportMC16c", json.dumps(data, cls=self.DateEncoder), 180*60)
        return render(request, 'reportCampaignHash.html', data, RequestContext(request))



    def recentProgressReportDEFT(self, condition):
        """
        1. Request tasks with statuses 
                failed, finished, aborted, done, registered, exhausted, broken,  submitting, obsolete, assigning, ready
                + timestamp = current_time - 24h
                
        2. Request tasks with statuses
                running, waiting
                
        3. Request tasks with statuses
                running, waiting + SUBMIT_TIME = current_time - 24h

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()

        """


        orderedsummary = OrderedDict()
        for step in self.steps:
            taskstate = OrderedDict()
            for state in self.taskstatelistRecent:
                taskstate[state] = 0
            orderedsummary[step] = taskstate


        sqlRequest = """
        SELECT count(t1.STATUS), t1.STATUS, t3.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID and timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 {0} 
        group by t1.STATUS, t3.STEP_NAME
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()

        for summaryRow in campaignsummary:
            if summaryRow[1] in ('failed', 'finished', 'aborted', 'done', 'registered', 'exhausted', 'broken'):
#                if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                    orderedsummary[summaryRow[2]][summaryRow[1]+'*'] += summaryRow[0]


        fullSummary = {}
        for summaryRow in campaignsummary:
            if summaryRow[1] in ('failed', 'finished', 'aborted', 'done', 'registered', 'exhausted', 'broken'):
#                if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                    if summaryRow[1]+'*' not in fullSummary:
                        fullSummary[summaryRow[1]+'*'] = {}
                    if summaryRow[2] not in fullSummary[summaryRow[1]+'*']:
                        fullSummary[summaryRow[1]+'*'][summaryRow[2]] = 0
                    fullSummary[summaryRow[1]+'*'][summaryRow[2]] += summaryRow[0]



        sqlRequest = """
        SELECT count(t1.STATUS), t1.STATUS, t3.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID and t1.STATUS in ('running','waiting', 'submitting') {0} 
        group by t1.STATUS, t3.STEP_NAME
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()


        for summaryRow in campaignsummary:
#            if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                orderedsummary[summaryRow[2]][summaryRow[1]+'**'] += summaryRow[0]



        for summaryRow in campaignsummary:
#            if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                if summaryRow[1]+'**' not in fullSummary:
                    fullSummary[summaryRow[1]+'**'] = {}
                if summaryRow[2] not in fullSummary[summaryRow[1]+'**']:
                    fullSummary[summaryRow[1]+'**'][summaryRow[2]] = 0
                fullSummary[summaryRow[1]+'**'][summaryRow[2]] += summaryRow[0]

        sqlRequest = """
        SELECT count(t1.STATUS), t1.STATUS, t3.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_PRODUCTION_STEP t2, ATLAS_DEFT.T_STEP_TEMPLATE t3 WHERE t1.STEP_ID=t2.STEP_ID and t2.STEP_T_ID=t3.STEP_T_ID and t1.STATUS in ('running', 'waiting', 'submitting') and t1.SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 {0} 
        group by t1.STATUS, t3.STEP_NAME
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()

        for summaryRow in campaignsummary:
#            if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                orderedsummary[summaryRow[2]][summaryRow[1]+'***'] += summaryRow[0]

        for summaryRow in campaignsummary:
#            if summaryRow[2] not in ['Deriv Merge', 'Deriv']:
                if summaryRow[1]+'***' not in fullSummary:
                    fullSummary[summaryRow[1]+'***'] = {}
                if summaryRow[2] not in fullSummary[summaryRow[1]+'***']:
                    fullSummary[summaryRow[1]+'***'][summaryRow[2]] = 0
                fullSummary[summaryRow[1]+'***'][summaryRow[2]] += summaryRow[0]
        return orderedsummary


    def prepareReport(self):

        requestList = [11034,11048,11049,11050,11051,11052,11198,11197,11222,11359]
        requestList = '(' + ','.join(map(str, requestList)) + ')'

        tasksCondition = "tasktype = 'prod' and WORKINGGROUP NOT IN('AP_REPR', 'AP_VALI', 'GP_PHYS', 'GP_THLT') and " \
                         "processingtype in ('evgen', 'pile', 'simul', 'recon') and REQID in %s" % requestList

        sqlRequest = '''
        select sum(enev), STATUS from (
        select SUM(NEVENTS) as enev, STATUS FROM (
        select NEVENTS, STATUS, t1.JEDITASKID from (
        select sum(decode(c.startevent,NULL,c.nevents,endevent-startevent+1)) nevents,c.status, d.jeditaskid  from atlas_panda.jedi_datasets d,atlas_panda.jedi_dataset_contents c where d.jeditaskid=c.jeditaskid and d.datasetid=c.datasetid and d.type in ('input','pseudo_input') and d.masterid is null group by c.status, d.jeditaskid) t1
        join
        (SELECT JEDITASKID FROM JEDI_TASKS where %s)t2 ON t1.JEDITASKID = t2.JEDITASKID
        ) group by JEDITASKID, STATUS)t3 group by STATUS
        ''' % tasksCondition

        cur = connection.cursor()
        cur.execute(sqlRequest)
        campaignsummary = cur.fetchall()

        finished = 0
        running = 0
        ready = 0

        for row in campaignsummary:
            if row[1] == 'finished':
                finished = row[0]
            if row[1] == 'running':
                running = row[0]
            if row[1] == 'ready':
                ready = row[0]

        data = {
            'finished':finished,
            'running':running,
            'ready':ready,
        }
        return self.renderPDF(data)

    def renderPDF(self, data):
        buff = StringIO.StringIO()
        doc = SimpleDocTemplate(buff, pagesize=letter,
                                rightMargin=72, leftMargin=72,
                                topMargin=72, bottomMargin=18)
        finished = data['finished']
        running = data['running']
        ready = data['ready']

        Report = []
        styles = getSampleStyleSheet()
        style = getSampleStyleSheet()['Normal']
        style.leading = 24

        Report.append(Paragraph('Report on Campaign: ' + "MC16a", styles["Heading1"]))
        Report.append(Paragraph('Build on ' + time.ctime() + " by BigPanDA", styles["Bullet"]))
        Report.append(Paragraph('Progress and loads', styles["Heading2"]))
        Report.append(Paragraph('Done events: ' + humanize.intcomma(int(finished/1000000)) +' M', styles["Normal"]))
        Report.append(Paragraph('Running events: ' + humanize.intcomma(int(running)/1000000) +' M', styles["Normal"]))
        Report.append(Paragraph('Ready for processing events: ' + humanize.intcomma(int(ready)/1000000)  +' M', styles["Normal"]))

        doc.build(Report)
        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = 'attachment; filename="report.pdf"'
        response.write(buff.getvalue())
        buff.close()
        return response

    def getName(self):
        return 'Simple campaign report'

    def getParameters(self):
        return {'Request List': ['11034,11048,11049,11050,11051,11052,11198,11197,11222,11359']}

    def getCacheEntry(self,request, viewType, skipCentralRefresh = False):
        is_json = False

        skipCentralRefreshTick = cache.get("2hoursRefreshTick", False)
        if skipCentralRefreshTick:
            skipCentralRefresh = True

        # We do this check to always rebuild cache for the page when it called from the crawler
        if (('REMOTE_ADDR' in request.META) and (request.META['REMOTE_ADDR'] in notcachedRemoteAddress) and
                    skipCentralRefresh == False):
            return None

        request._cache_update_cache = False
        if ((('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) or (
                    'json' in request.GET)):
            is_json = True
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
        cache_key = '%s.%s' % (key_prefix, path.hexdigest())
        return cache.get(cache_key, None)

    def setCacheEntry(self,request, viewType, data, timeout):
        is_json = False
        request._cache_update_cache = False
        if ((('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) or (
                    'json' in request.GET)):
            is_json = True
        key_prefix = "%s_%s_%s_" % (is_json, djangosettings.CACHE_MIDDLEWARE_KEY_PREFIX, viewType)
        path = hashlib.md5(encoding.force_bytes(encoding.iri_to_uri(request.get_full_path())))
        cache_key = '%s.%s' % (key_prefix, path.hexdigest())
        cache.set(cache_key, data, timeout)
        cache.set("2hoursRefreshTick", True, 120*60)




    def getDKBEventsSummaryRequestedBreakDownHashTag(self, request):
        sqlRequestHashTags = '''SELECT SUM(t1.FINISHEDJEV) AS FINISHEDEV, 
            SUM(PENDINGJEV+DEFINEDJEV+ASSIGNEDJEV+WAITINGJEV+ACTIVATEDJEV+SENTJEV+STARTINGJEV+RUNNINGJEV+HOLDINGJEV+TRANSFERRINGJEV+MERGINGJEV) as RESTEV,
            t3.HASHTAG, STEP FROM ATLAS_PANDABIGMON.TASKS_PROD_AGGREGATE t1, ATLAS_DEFT.T_HT_TO_TASK t2, ATLAS_DEFT.T_HASHTAG t3 
            WHERE t3.HT_ID=t2.HT_ID and t1.TASKID=t2.TASKID AND t1.REQUESTID IN (11034, 11035, 11048,11049,11050,11051,11052,11198,11197,11222,11359)
            GROUP BY t3.HASHTAG, STEP
            '''
        sqlRequestFull = sqlRequestHashTags
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        dbResult = cur.fetchall()
        listHashProgress = []

        for row in dbResult:
            rowDict = {"finishedev":row[0], "restev":row[1], "hashtag":row[2], "step":row[3]}
            listHashProgress.append(rowDict)
        return listHashProgress




    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                return str(obj)
            return json.JSONEncoder.default(self, obj)

