from django.db import connection
import time
from django.shortcuts import render_to_response, render, redirect
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from django.http import HttpResponse
from reportlab.pdfgen import canvas
from django.template import RequestContext, loader
import StringIO
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

    steps = ['simul', 'mergeHits', 'recon', 'merge']
    stepsLabels = {'simul':'Simulation', 'recon':'Reconstruction', 'merge':'AOD Merge', 'mergeHits':'HITS Merge'}


    def refreshPreprocessed:
        """
        TaskID, Status, Campaign, RequestID, DEFTStatus, STEP, TimeStamp, SubmitTime, InputEvents, pendingJEv, definedJEv,	assignedJEv,	waitingJEv,	activatedJEv,	sentJEv,	startingJEv,	runningJEv,	holdingJEv,	transferringJEv,	mergingJEv,	finishedJEv,	failedJEv,	cancelledJEv,	throttledJEv,	closedJEv, pendingNJ,	definedNJ,	assignedNJ,	waitingNJ,	activatedNJ,	sentNJ,	startingNJ,	runningNJ,	holdingNJ,	transferringNJ,	mergingNJ,	finishedNJ,	failedNJ,	cancelledNJ,	throttledNJ,	closedNJ
        """
        pass


    def getJEDIEventsSummaryRequestedOutput(self, condition):
        sqlRequest = '''
          SELECT * FROM (
            SELECT SUM(NINPUTEVENTS), SUM(NEVENTSUSED), SUM(NOUTEVENTS), STEP FROM (
            SELECT 
            
                      CASE WHEN t1.TASKNAME LIKE '%.merge.%' AND substr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),instr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'merge'
                      WHEN t1.TASKNAME LIKE '%.merge.%' AND not substr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),instr(substr(t1.TASKNAME,instr(t1.TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' THEN 'mergeHits'
                      WHEN t1.TASKNAME LIKE '%.recon.%' THEN 'recon'
                      WHEN t1.TASKNAME LIKE '%.simul.%' THEN 'simul'
                      WHEN t1.TASKNAME LIKE '%.evgen.%' THEN 'evgen'
                      END AS STEP,
                      t1.JEDITASKID,
                      CASE WHEN t2.TYPE='output' THEN t2.nevents ELSE 0 END as NOUTEVENTS,
                      CASE WHEN t2.TYPE IN ('input', 'pseudo_input') THEN t2.nevents ELSE 0 END as NINPUTEVENTS,
                      CASE WHEN t2.TYPE IN ('input', 'pseudo_input') THEN t2.neventsused ELSE 0 END as NEVENTSUSED
            
            FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2 WHERE campaign like 'MC16%' and 
            t1.status not in ('failed','aborted','broken') and  t1.JEDITASKID=t2.JEDITASKID and t2.MASTERID IS NULL AND t1.REQID IN (11034, 11035, 11048,11049,11050,11051,11052,11198,11197,11222,11359) 
            ) GROUP BY STEP 
        )t1 

        '''

        # INPUT_EVENTS, TOTAL_EVENTS, OUTEV, STEP

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        stats = cur.fetchall()
        input = 0
        tosimulate = 0
        simulatedprogr = 0
        tomergeHits = 0
        mergeHitsprog = 0
        toreconstruct = 0
        reconstructedprog = 0
        tomerge = 0
        mergeprog = 0

        for row in stats:
            if row[3] == 'simul':
                input = row[0]
                tosimulate = row[0]
            if row[3] == 'mergeHits':
                tomergeHits = row[0]
            if row[3] == 'recon':
                toreconstruct = row[0]
            if row[3] == 'merge':
                tomerge = row[0]

        fullSummary = {"input":input,
                       "tosimulate":tosimulate,
                       "tomergeHits":tomergeHits,
                       "toreconstruct":toreconstruct,
                       "tomerge":tomerge
                      }
        return fullSummary


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
            
            FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2 WHERE campaign like 'MC16%'
            and t1.status not in ('failed','aborted','broken') and  t1.JEDITASKID=t2.JEDITASKID and t2.MASTERID IS NULL and t2.TYPE IN ('input', 'pseudo_input') {0} 
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

    #SELECT t1.TASKID, t3.HASHTAG FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_HT_TO_TASK t2, ATLAS_DEFT.T_HASHTAG t3 WHERE t3.HT_ID=t2.HT_ID and t1.TASKID=t2.TASKID AND PR_ID IN (11034,11048,11049,11050,11051,11052,11198,11197,11222,11359)

    def getJEDIEventsSummaryRequestedBreakDownHashTag(self, condition):
        sqlRequestHashTags = '''SELECT t1.TASKID, t3.HASHTAG FROM ATLAS_DEFT.T_PRODUCTION_TASK t1, ATLAS_DEFT.T_HT_TO_TASK t2, ATLAS_DEFT.T_HASHTAG t3 WHERE t3.HT_ID=t2.HT_ID and t1.TASKID=t2.TASKID AND t1.PR_ID IN {0}'''
        sqlRequestFull = sqlRequestHashTags.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        hashTags = cur.fetchall()
        hashTagsDict = {}

        for hashTag in hashTags:
            if not hashTag[1] in hashTagsDict:
                hashTagsDict[hashTag[1]] = []
            else:
                hashTagsDict[hashTag[1]].append(hashTag[0])

        JediEventsTableHashs = []
        hashTagsList = []
        for hashTagKey in hashTagsDict:
            tasksList = hashTagsDict[hashTagKey]
            if len(tasksList) > 0:
                requestListString = '(' + ','.join(map(str, tasksList)) + ')'
                JediEventsTableHash = self.getJEDIEventsSummaryRequested('and t1.REQID IN %s and t1.JEDITASKID IN %s' % (condition, requestListString))
                JediEventsTableHash['hashtag'] = hashTagKey
                JediEventsTableHashs.append(JediEventsTableHash)
                hashTagsList.append(hashTagKey)
        return (JediEventsTableHashs, hashTagsList)





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
          FROM ATLAS_DEFT.T_PRODUCTION_TASK t, ATLAS_DEFT.T_PRODUCTION_STEP s WHERE s.STEP_ID=t.STEP_ID and CAMPAIGN LIKE 'MC16%' and not TASKNAME LIKE '%valid%' and TASKNAME LIKE 'mc16_%' {0}
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


    def getTasksJEDISummary(self, condition):
        sqlRequest = '''

            SELECT count(t1.STATUS), t1.STATUS, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t1.STATUS
            UNION ALL
            SELECT count(t1.STATUS), t1.STATUS, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t1.STATUS
            UNION ALL
            SELECT count(t1.STATUS), t1.STATUS, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0} group by t1.STATUS
            UNION ALL
            SELECT count(t1.STATUS), t1.STATUS, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}  group by t1.STATUS
            UNION ALL
            SELECT count(t1.STATUS), t1.STATUS, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0} group by t1.STATUS

        '''
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()
        orderedsummary = OrderedDict()
        for step in self.steps:
            taskstate = OrderedDict()
            for state in self.taskstatelist:
                taskstate[state] = 0
            orderedsummary[step] = taskstate
        for summaryRow in campaignsummary:
            orderedsummary[summaryRow[2]][summaryRow[1]] += summaryRow[0]
        return orderedsummary


    def getTasksDEFTSummary(self, condition):
        sqlRequest = '''

            SELECT COUNT(STATUS), STATUS, STEP  FROM (
            SELECT STATUS, 'recon' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%'  {0} 
            UNION ALL
            SELECT STATUS, 'simul' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%'  {0} 
            UNION ALL
            SELECT STATUS, 'evgen' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%'  {0} 
            UNION ALL
            SELECT STATUS, 'merge' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
            UNION ALL
            SELECT STATUS, 'mergeHits' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
            )t1 group by STATUS, STEP

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
            if summaryRow[1] == 'done' or summaryRow[1] == 'finished':
                orderedsummary[summaryRow[2]]['done+finished'] += summaryRow[0]
            else:
                orderedsummary[summaryRow[2]][summaryRow[1]] += summaryRow[0]
        return orderedsummary



    def getJobsJEDISummary(self, condition):

        sqlRequest = '''
        SELECT COUNT(JOBSTATUS), JOBSTATUS, STEP, SUM(HS06SEC) FROM
            (
            SELECT DISTINCT PANDAID, JOBSTATUS, STEP, HS06SEC FROM (
            WITH selectedTasks AS (
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            )
            SELECT PANDAID, JOBSTATUS, selectedTasks.STEP, HS06SEC FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID
            UNION ALL
            SELECT PANDAID, JOBSTATUS, selectedTasks.STEP as STEP, HS06SEC FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID
            UNION ALL
            SELECT PANDAID, JOBSTATUS, selectedTasks.STEP as STEP, HS06SEC FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID
            UNION ALL
            SELECT PANDAID, JOBSTATUS, selectedTasks.STEP as STEP, HS06SEC FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID
            UNION ALL
            SELECT PANDAID, JOBSTATUS, selectedTasks.STEP as STEP, HS06SEC FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID
            )tt) tb group by JOBSTATUS, STEP
        '''

        orderedsummary = OrderedDict()
        for step in self.steps:
            jobstate = OrderedDict()
            for state in self.jobstatelist:
                jobstate[state] = 0
            orderedsummary[step] = jobstate


        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()


        fullSummary = {}
        hepspecSummary = {}
        for summaryRow in campaignsummary:
            if not summaryRow[3] is None and summaryRow[1] not in hepspecSummary:
                hepspecSummary[summaryRow[1]] = {}

            orderedsummary[summaryRow[2]][summaryRow[1]] += summaryRow[0]

            if summaryRow[1] not in fullSummary:
                fullSummary[summaryRow[1]] = {}
            if summaryRow[2] not in fullSummary[summaryRow[1]]:
                fullSummary[summaryRow[1]][summaryRow[2]] = 0

                if not summaryRow[3] is None:
                    hepspecSummary[summaryRow[1]][summaryRow[2]] = 0
            fullSummary[summaryRow[1]][summaryRow[2]] += summaryRow[0]
            if not summaryRow[3] is None:
                hepspecSummary[summaryRow[1]][summaryRow[2]] += summaryRow[3]

        fullSummaryTotal = {}
        for status, stepdict in fullSummary.items():
            for step, val in stepdict.items():
                if step not in fullSummaryTotal:
                    fullSummaryTotal[step] = 0
                fullSummaryTotal[step] += val
        fullSummary['total'] = fullSummaryTotal

        return (orderedsummary, hepspecSummary)


    def getEventsJEDISummaryJobStatus(self, condition):

        sqlRequest = '''
            with selectedEvents as (
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)) AS NEVENTS, t3.PANDAID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.merge.%' and t3.TYPE IN ('input', 'pseudo_input') and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t3.PANDAID
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)) AS NEVENTS, t3.PANDAID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.merge.%' and t3.TYPE IN ('input', 'pseudo_input') and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t3.PANDAID
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)) AS NEVENTS, t3.PANDAID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.recon.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.PANDAID
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)) AS NEVENTS, t3.PANDAID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.simul.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.PANDAID
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)) AS NEVENTS, t3.PANDAID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.evgen.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.PANDAID
          ),
          selectedJobs as (
            SELECT JOBSTATUS, STEP, NEVENTS, row_number() OVER(PARTITION BY pandaid order by JOBSTATUS) as rn FROM (
            SELECT t2.JOBSTATUS, STEP, selectedevents.NEVENTS, t2.pandaid FROM selectedevents, ATLAS_PANDAARCH.JOBSARCHIVED t2 where t2.pandaid = selectedevents.pandaid 
            UNION ALL
            SELECT t2.JOBSTATUS, STEP, selectedevents.NEVENTS, t2.pandaid FROM selectedevents, ATLAS_PANDA.JOBSACTIVE4 t2 where t2.pandaid = selectedevents.pandaid
            UNION ALL
            SELECT t2.JOBSTATUS, STEP, selectedevents.NEVENTS, t2.pandaid FROM selectedevents, ATLAS_PANDA.JOBSARCHIVED4 t2 where t2.pandaid = selectedevents.pandaid
            UNION ALL
            SELECT t2.JOBSTATUS, STEP, selectedevents.NEVENTS, t2.pandaid FROM selectedevents, ATLAS_PANDA.JOBSWAITING4 t2 where t2.pandaid = selectedevents.pandaid
            UNION ALL
            SELECT t2.JOBSTATUS, STEP, selectedevents.NEVENTS, t2.pandaid FROM selectedevents, ATLAS_PANDA.JOBSDEFINED4 t2 where t2.pandaid = selectedevents.pandaid
            ) 
          ) SELECT SUM(NEVENTS), JOBSTATUS, STEP FROM selectedJobs where rn = 1 GROUP BY STEP, JOBSTATUS
          
        '''
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()

        orderedsummary = OrderedDict()
        input = 0
        simulated = 0
        simulatedprogr = 0
        mergeHits = 0
        mergeHitsprog = 0
        reconstructed = 0
        reconstructedprog = 0
        merge = 0
        mergeprog = 0


        for step in self.steps:
            jobstate = OrderedDict()
            for state in self.jobstatelist:
                jobstate[state] = 0
            orderedsummary[step] = jobstate

        for summaryRow in campaignsummary:
            orderedsummary[summaryRow[2]][summaryRow[1]] += summaryRow[0]

        for stepName in orderedsummary:
            stepInfo = orderedsummary.get(stepName)
            for jobsStatus in stepInfo:
                countOfEv = stepInfo[jobsStatus]

                if jobsStatus == 'finished':
                    if stepName == 'simul':
                        simulated = countOfEv
                    if stepName == 'merge':
                        merge = countOfEv
                    if stepName == 'recon':
                        reconstructed = countOfEv
                    if stepName == 'mergeHits':
                        mergeHits = countOfEv
                elif jobsStatus in ['pending',	'defined',	'assigned',	'waiting',	'activated',	'sent',	'starting',	'running',	'holding',	'transferring',	'merging']:
                    if stepName == 'simul':
                        simulatedprogr += countOfEv
                    if stepName == 'merge':
                        mergeprog += countOfEv
                    if stepName == 'recon':
                        reconstructedprog += countOfEv
                    if stepName == 'mergeHits':
                        mergeHitsprog += countOfEv

        input = simulatedprogr
        simulatedprogr = int( (1-simulatedprogr/ float(simulated)) * 100)
        mergeprog = int((1-mergeprog/ float(merge)) * 100)
        reconstructedprog = int( (1 - reconstructedprog / float(reconstructed)) * 100)
        mergeHitsprog = int( (1- mergeHitsprog / float(mergeHits)) * 100)

        fullSummary = {"input":input,
                       "simulated":simulated,
                       "mergeHits":mergeHits,
                       "reconstructed":reconstructed,
                       "merge":merge,
                       "simulatedprogr":simulatedprogr,
                       "mergeHitsprog":mergeHitsprog,
                       "reconstructedprog":reconstructedprog,
                       "mergeprog":mergeprog,
                      }

        return (orderedsummary, fullSummary)


    def getEventsJEDISummary(self, condition):
        sqlRequest = '''
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)), t3.STATUS, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.merge.%' and t3.TYPE IN ('input', 'pseudo_input') and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t3.STATUS
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)), t3.STATUS, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.merge.%' and t3.TYPE IN ('input', 'pseudo_input') and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0} group by t3.STATUS
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)), t3.STATUS, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.recon.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.STATUS
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)), t3.STATUS, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.simul.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.STATUS
            UNION ALL
            SELECT sum(decode(t3.startevent,NULL,t3.nevents,t3.endevent-t3.startevent+1)), t3.STATUS, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1, ATLAS_PANDA.JEDI_DATASETS t2, ATLAS_PANDA.JEDI_DATASET_CONTENTS t3 WHERE campaign like 'MC16%' AND
            t1.status not in ('failed','aborted','broken') and t1.JEDITASKID=t2.JEDITASKID AND t3.DATASETID=t2.DATASETID AND t2.MASTERID IS NULL AND t3.JEDITASKID=t1.JEDITASKID and TASKNAME LIKE '%.evgen.%' and t3.TYPE IN ('input', 'pseudo_input') {0} group by t3.STATUS
        '''

        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()
        fullSummary = {}
        for summaryRow in campaignsummary:
            if summaryRow[1] not in fullSummary:
                fullSummary[summaryRow[1]] = {}
            if summaryRow[2] not in fullSummary[summaryRow[1]]:
                fullSummary[summaryRow[1]][summaryRow[2]] = 0
            fullSummary[summaryRow[1]][summaryRow[2]] += summaryRow[0]

        fullSummaryTotal = {}
        for status, stepdict in fullSummary.items():
            for step, val in stepdict.items():
                if step not in fullSummaryTotal:
                    fullSummaryTotal[step] = 0
                fullSummaryTotal[step] += val
        fullSummary['total'] = fullSummaryTotal
        return fullSummary


    def topSitesFailureRate(self, topN, condition):
        sqlRequest ='''
            SELECT COUNT(*), PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, STEP, SUM(HS06SEC), COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, HS06SEC, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
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
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            )
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'activated' THEN 1 ELSE 0 END as ISACTIVATED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('activated', 'running')
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
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            )
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSACTIVE4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSARCHIVED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDAARCH.JOBSARCHIVED t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSDEFINED4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
            UNION ALL
            SELECT PANDAID, CASE JOBSTATUS WHEN 'assigned' THEN 1 ELSE 0 END as ISASSIGNED, CASE JOBSTATUS WHEN 'running' THEN 1 ELSE 0 END as ISRUNNING, selectedTasks.STEP as STEP, COMPUTINGSITE FROM ATLAS_PANDA.JOBSWAITING4 t2, selectedTasks WHERE selectedTasks.JEDITASKID=t2.JEDITASKID and JOBSTATUS in ('assigned', 'running')
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


    def topTasksWithFailedSuccessRat(self, request):
        sqlRequest ='''
            SELECT COUNT(*), PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, STEP, SUM(HS06SEC), COMPUTINGSITE FROM
            (
            SELECT DISTINCT PANDAID, PILOTERRORCODE, TASKBUFFERERRORCODE, transexitcode, exeerrorcode, jobdispatchererrorcode, ddmerrorcode, HS06SEC, STEP, COMPUTINGSITE FROM (
            WITH selectedTasks AS (
            SELECT JEDITASKID, 'recon' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.recon.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'simul' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.simul.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'evgen' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%' {0}
            UNION ALL
            SELECT JEDITASKID, 'merge' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
            UNION ALL
            SELECT JEDITASKID, 'mergeHits' as STEP FROM ATLAS_PANDA.JEDI_TASKS t1 WHERE campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%' {0}
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

        #               11035, 11034, 11048, 11049, 11050, 11051, 11052, 11198, 11197, 11222, 11359
        requestList = [11035, 11034, 11048, 11049, 11050, 11051, 11052, 11198, 11197, 11222, 11359]
        requestList = '(' + ','.join(map(str, requestList)) + ')'

        data = self.getCacheEntry(request, "prepareReportMC16")
        if data is not None:
            data = json.loads(data)
            data['request'] = request
            response = render_to_response('reportCampaign.html', data, RequestContext(request))
            patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
            return response

        recentTasks = self.recentProgressReportDEFT('and PR_ID IN %s' % requestList)
        recentTasks['title'] = 'Tasks updated during last 24 hours'

        JediEventsR1 = self.getJEDIEventsSummaryRequestedOutput('and t1.REQID IN %s' % requestList)
        #JediEventsR['title'] = 'Overall events processing summary'

        #(jediEventsHashsTable, hashTable) = self.getJEDIEventsSummaryRequestedBreakDownHashTag(requestList)

        (totalEvents,JediEventsR) = self.getEventsJEDISummaryJobStatus('and REQID IN %s' % requestList)
        totalEvents['title'] = 'Events processing summary'
        JediEventsR['title'] = 'Overall events processing summary'
        JediEventsR['simulatedprogr'] =  int (JediEventsR['simulated']/float(JediEventsR1['tosimulate'])*100)
        JediEventsR['reconstructedprog'] =  int (JediEventsR['reconstructed']/float(JediEventsR1['tosimulate'])*100)
        JediEventsR['mergeHitsprog'] =  int (JediEventsR['mergeHits']/float(JediEventsR1['tosimulate'])*100)
        JediEventsR['mergeprog'] = int (JediEventsR['merge']/float(JediEventsR1['tosimulate'])*100)
        JediEventsR['input'] =  JediEventsR1['input']



        totalTasks = self.getTasksDEFTSummary('and PR_ID IN %s' % requestList)
        totalTasks['title'] = 'Tasks processing summary'

        (totalJobs, hepspecJobs) = self.getJobsJEDISummary('and REQID IN %s' % requestList)
        totalJobs['title'] = 'Jobs processing summary'
        hepspecJobs['title'] = 'HS06SEC summary'

        worstSite = self.topSitesFailureRate(10, 'and REQID IN %s' % requestList)
        #worstSite['title'] = 'List of top sites with highest failure rate'

        siteActRun = self.topSitesActivatedRunning(10, 'and REQID IN %s' % requestList)
        siteAssignRun = self.topSitesAssignedRunning(10, 'and REQID IN %s' % requestList)

        data = {"requestList":requestList,
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

        return render_to_response('reportCampaign.html', data, RequestContext(request))


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
        SELECT COUNT(STATUS), STATUS, STEP FROM (
        SELECT STATUS, 'recon' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.recon.%'  {0} 
        UNION ALL
        SELECT STATUS, 'simul' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.simul.%'  {0} 
        UNION ALL
        SELECT STATUS, 'evgen' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%'  {0} 
        UNION ALL
        SELECT STATUS, 'merge' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        UNION ALL
        SELECT STATUS, 'mergeHits' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE timestamp >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        )t1 group by STATUS, STEP
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()

        for summaryRow in campaignsummary:
            if summaryRow[1] in ('failed', 'finished', 'aborted', 'done', 'registered', 'exhausted', 'broken'):
                orderedsummary[summaryRow[2]][summaryRow[1]+'*'] += summaryRow[0]


        fullSummary = {}
        for summaryRow in campaignsummary:
            if summaryRow[1] in ('failed', 'finished', 'aborted', 'done', 'registered', 'exhausted', 'broken'):
                if summaryRow[1]+'*' not in fullSummary:
                    fullSummary[summaryRow[1]+'*'] = {}
                if summaryRow[2] not in fullSummary[summaryRow[1]+'*']:
                    fullSummary[summaryRow[1]+'*'][summaryRow[2]] = 0
                fullSummary[summaryRow[1]+'*'][summaryRow[2]] += summaryRow[0]



        sqlRequest = """
        SELECT COUNT(STATUS), STATUS, STEP FROM (
        SELECT STATUS, 'recon' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running','waiting', 'submitting') and campaign like 'MC16%' and TASKNAME LIKE '%.recon.%'  {0} 
        UNION ALL
        SELECT STATUS, 'simul' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running','waiting', 'submitting') and campaign like 'MC16%' and TASKNAME LIKE '%.simul.%'  {0} 
        UNION ALL
        SELECT STATUS, 'evgen' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running','waiting', 'submitting') and campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%'  {0} 
        UNION ALL
        SELECT STATUS, 'merge' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running','waiting', 'submitting') and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        UNION ALL
        SELECT STATUS, 'mergeHits' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running','waiting', 'submitting') and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        )t1 group by STATUS, STEP
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()


        for summaryRow in campaignsummary:
            orderedsummary[summaryRow[2]][summaryRow[1]+'**'] += summaryRow[0]



        for summaryRow in campaignsummary:
            if summaryRow[1]+'**' not in fullSummary:
                fullSummary[summaryRow[1]+'**'] = {}
            if summaryRow[2] not in fullSummary[summaryRow[1]+'**']:
                fullSummary[summaryRow[1]+'**'][summaryRow[2]] = 0
            fullSummary[summaryRow[1]+'**'][summaryRow[2]] += summaryRow[0]



        sqlRequest = """
        SELECT COUNT(STATUS), STATUS, STEP FROM (
        SELECT STATUS, 'recon' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running', 'waiting', 'submitting') and SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.recon.%'  {0} 
        UNION ALL
        SELECT STATUS, 'simul' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running', 'waiting', 'submitting') and SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.simul.%'  {0} 
        UNION ALL
        SELECT STATUS, 'evgen' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running', 'waiting', 'submitting') and SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.evgen.%'  {0} 
        UNION ALL
        SELECT STATUS, 'merge' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running', 'waiting', 'submitting') and SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        UNION ALL
        SELECT STATUS, 'mergeHits' as STEP FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 WHERE STATUS in ('running', 'waiting', 'submitting') and SUBMIT_TIME >= SYS_EXTRACT_UTC(systimestamp) - 1 and campaign like 'MC16%' and TASKNAME LIKE '%.merge.%' and not substr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),instr(substr(TASKNAME,instr(TASKNAME,'.',-1) + 1),'_',-1) + 1) like 'r%'  {0} 
        
        )t1 group by STATUS, STEP
        """
        sqlRequestFull = sqlRequest.format(condition)
        cur = connection.cursor()
        cur.execute(sqlRequestFull)
        campaignsummary = cur.fetchall()


        for summaryRow in campaignsummary:
            orderedsummary[summaryRow[2]][summaryRow[1]+'***'] += summaryRow[0]



        for summaryRow in campaignsummary:
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

    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                return str(obj)
            return json.JSONEncoder.default(self, obj)
