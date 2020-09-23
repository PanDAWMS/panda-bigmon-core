import random
import copy
import logging
import time
from django.db import connection
from django.utils import timezone
from core.libs.job import is_event_service
from core.common.models import JediJobRetryHistory
from core.settings.local import dbaccess

_logger = logging.getLogger('bigpandamon')


def drop_job_retries(jobs, jeditaskid, **kwards):
    """
    Dropping algorithm for jobs belong to a single task
    Mandatory job's attributes:
        PANDAID
        JOBSTATUS
        PROCESSINGTYPE
        JOBSETID
        SPECIALHANDLING
    :param jobs: list
    :param jeditaskid: int
    :return:
    """
    start_time = time.time()

    is_return_dropped_jobs = False
    if 'is_return_dropped_jobs' in kwards:
        is_return_dropped_jobs = True

    drop_list = []
    droppedIDs = set()
    drop_merge_list = set()

    # get job retry history for a task
    retryquery = {
        'jeditaskid': jeditaskid
    }
    extra = """
        OLDPANDAID != NEWPANDAID 
        AND RELATIONTYPE IN ('', 'retry', 'pmerge', 'merge', 'jobset_retry', 'es_merge', 'originpandaid')
    """
    retries = JediJobRetryHistory.objects.filter(**retryquery).extra(where=[extra]).order_by('newpandaid').values()
    _logger.info('Got {} retries whereas total number of jobs is {}: {} sec'.format(len(retries), len(jobs),
                                                                                    (time.time() - start_time)))

    hashRetries = {}
    for retry in retries:
        hashRetries[retry['oldpandaid']] = retry

    newjobs = []
    for job in jobs:
        dropJob = 0
        pandaid = job['pandaid']
        if not is_event_service(job):
            if pandaid in hashRetries:
                retry = hashRetries[pandaid]
                if retry['relationtype'] in ('', 'retry') or (job['processingtype'] == 'pmerge' and job['jobstatus'] in ('failed', 'cancelled') and retry['relationtype'] == 'merge'):
                    dropJob = retry['newpandaid']
            else:
                if job['jobsetid'] in hashRetries and hashRetries[job['jobsetid']]['relationtype'] == 'jobset_retry':
                    dropJob = 1
        else:

            if job['pandaid'] in hashRetries and job['jobstatus'] not in ('finished', 'merging'):
                if hashRetries[job['pandaid']]['relationtype'] == 'retry':
                    dropJob = 1

            # if hashRetries[job['pandaid']]['relationtype'] == 'es_merge' and job['jobsubstatus'] == 'es_merge':
            #     dropJob = 1

            if dropJob == 0:
                if job['jobsetid'] in hashRetries and hashRetries[job['jobsetid']]['relationtype'] == 'jobset_retry':
                    dropJob = 1

                if job['jobstatus'] == 'closed' and job['jobsubstatus'] in ('es_unused', 'es_inaction',):
                    dropJob = 1

        if dropJob == 0 and not is_return_dropped_jobs:
            #     and not (
            #     'processingtype' in request.session['requestParams'] and request.session['requestParams'][
            # 'processingtype'] == 'pmerge')

            if job['processingtype'] != 'pmerge':
                newjobs.append(job)
            else:
                drop_merge_list.add(pandaid)
        elif dropJob == 0:
            newjobs.append(job)
        else:
            if pandaid not in droppedIDs:
                droppedIDs.add(pandaid)
                drop_list.append({'pandaid': pandaid, 'newpandaid': dropJob})

    _logger.info('{} jobs dropped: {} sec'.format(len(jobs) - len(newjobs), time.time() - start_time))
    drop_list = sorted(drop_list, key=lambda x: -x['pandaid'])
    jobs = newjobs

    return jobs, drop_list, drop_merge_list


def dropRetrielsJobs(jeditaskid,extra=None,isEventTask=False):
    droppedIDList = []
    tk = 0
    wildCardExtension =''
    if isEventTask is False:
        sqlRequest = '''
        select distinct(pandaid) from (select fileid, pandaid, status, attemptnr, max(attemptnr) over (partition by fileid) as lastattempt  
        from atlas_panda.filestable4 
        where  jeditaskid = %s and DESTINATIONDBLOCKTOKEN is NULL  and Dataset not in ('RNDMSEED')) WHERE lastattempt!= attemptnr
        ''' % (jeditaskid)
        cur = connection.cursor()
        cur.execute(sqlRequest)
        droppedIDs = cur.fetchall()
        random.seed()
        tk = random.randrange(10000000)

        new_cur = connection.cursor()
        executionData = []
        for pandaid in droppedIDs:
            executionData.append((pandaid[0], tk))
            droppedIDList.append(pandaid[0])
        insertquery = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
        new_cur.executemany(insertquery, executionData)
        if extra is not None:
             wildCardExtension = extra + ' and PANDAID NOT IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%s)' % (tk)
        else: wildCardExtension = 'PANDAID NOT IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%s)'% (tk)

    else:
        sqlRequest = '''
         select distinct jobsetid from ATLAS_PANDA.JEDI_DATASET_CONTENTS where  jeditaskid = %s  and type like 'input' and jobsetid is not null
          ''' % (jeditaskid)
        cur = connection.cursor()
        cur.execute(sqlRequest)
        nonDroppedIDs = cur.fetchall()
        random.seed()
        tk = random.randrange(1000000)

        new_cur = connection.cursor()
        executionData = []
        for pandaid in nonDroppedIDs:
            executionData.append((pandaid[0], tk))
        insertquery = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
        new_cur.executemany(insertquery, executionData)
        if extra is not None:
            wildCardExtension = extra + ' and jobsetid IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%s)' % (
            tk)
        else:
            wildCardExtension = 'jobsetid IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%s)' % (
            tk)
    return tk, droppedIDList,wildCardExtension


def clearDropRetrielsJobs(tk,jobs,droplist=0,isEventTask=False,isReturnDroppedPMerge = False):
    newjobs=[]
    droppedPmerge =[]
    notDroppedPmerge = set()
    if isEventTask is False:
        for job in jobs:
            #if not isReturnDroppedPMerge:
            if not (job['processingtype'] == 'pmerge'):
                newjobs.append(job)
            else:
                droppedPmerge.append(job)
                    #droplist.append(job['pandaid'])
            #else: droplist.append(job['pandaid'])
        retryquery = {}
        if len(droppedPmerge)>0:
            if len(jobs) > 0:
                retryquery['jeditaskid'] = jobs[0]['jeditaskid']
                droppedPmerge,notDroppedPmerge = clearDropPmergeRetrielsJobs(droppedPmerge, retryquery)
                if not isReturnDroppedPMerge:
                    droplist = droplist + list(droppedPmerge)
                    droppedPmerge = []
                else:
                    newjobs = newjobs + list(notDroppedPmerge)
                    droppedPmerge = []
        return newjobs, droppedPmerge,droplist
    else:
        pandaDropIDList=set()
        checkDropJobs = []
        for job in jobs:
            pandaid = job['pandaid']
            if job['jobstatus'] == 'closed' and job['jobsubstatus'] in ('es_unused', 'es_inaction','es_retry'):
                pandaDropIDList.add(pandaid)
            elif job['jobstatus'] == 'failed' or (job['jobstatus'] == 'closed' and job['jobsubstatus'] in ('toreassign','es_noevent')):
                checkDropJobs.append(job)
            else:
                if not isReturnDroppedPMerge:
                    if not (job['processingtype'] == 'pmerge'):
                        newjobs.append(job)
                    else:
                        droppedPmerge.add(job['pandaid'])
                else:
                    newjobs.append(job)
        retryquery = {}
        if len(jobs)>0:
            retryquery['jeditaskid'] = jobs[0]['jeditaskid']
        if len(checkDropJobs)>0:
            random.seed()
            transactionKey = random.randrange(1000000)

            new_cur = connection.cursor()
            executionData = []
            for pandaid in jobs:
                executionData.append((pandaid["pandaid"], transactionKey))
            query = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
            new_cur.executemany(query, executionData)
            retries = JediJobRetryHistory.objects.filter(**retryquery).extra(
                where=["OLDPANDAID IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i) and RElATIONTYPE like 'retry'" % (
                transactionKey)]).values("oldpandaid","relationtype")

            retry_list = {}
            for retry in retries:
                retry_list[retry["oldpandaid"]]=retry["relationtype"]
            retry_keys = retry_list.keys()
            #retry_list = [d["oldpandaid"] for d in list(retries)]
            for checkJob in checkDropJobs:
                if checkJob["pandaid"] in retry_keys:
                    #if retry_list[checkJob["pandaid"]]=="retry":
                   pandaDropIDList.add(checkJob["pandaid"])
                   # elif retry_list[failedjob["pandaid"]]=="es_merge" and failedjob["jobstatus"] =="failed"  and failedjob["jobsubstatus"]=="es_merge_failed":
                    #    pandaDropIDList.add(failedjob["pandaid"])
                else: newjobs.append(checkJob)
                #else:
                #    newjobs.append(checkJob)
            new_cur.close()
            pandaDropIDList = list(pandaDropIDList)

        return newjobs, droppedPmerge,pandaDropIDList


def clearDropPmergeRetrielsJobs(dPmerge,retryquery):
    dropPmerge = set()
    notDropPmerge = []
    random.seed()
    transactionKey = random.randrange(1000000)

    new_cur = connection.cursor()
    executionData = []
    for pandaid in dPmerge:
        executionData.append((pandaid['pandaid'], transactionKey))
    query = """INSERT INTO ATLAS_PANDABIGMON.TMP_IDS1Debug(ID,TRANSACTIONKEY) VALUES (%s,%s)"""
    new_cur.executemany(query, executionData)

    retries = JediJobRetryHistory.objects.filter(**retryquery).extra(
        where=["OLDPANDAID IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i)" % (
            transactionKey)]).values("oldpandaid", "relationtype")

    retry_list = {}
    for retry in retries:
        retry_list[retry["oldpandaid"]] = retry["relationtype"]
    retry_keys = retry_list.keys()
    for checkJob in dPmerge:
        if checkJob['pandaid'] in retry_keys:
            if retry_list[checkJob['pandaid']] == "retry":
                dropPmerge.add(checkJob['pandaid'])
            else:
                notDropPmerge.append(checkJob)
        else:
            notDropPmerge.append(checkJob)
    return dropPmerge, notDropPmerge

def compareDropAlgorithm(oldDropDict,newDropList):
    difDropList = []
    oldDropList = []
    for drp in oldDropDict:
        oldDropList.append(drp['pandaid'])
    if len(oldDropList)>len(newDropList) and (len(oldDropDict) != 0 and len(newDropList) != 0):
        difDropList = set(oldDropList)- set(newDropList)
    elif len(oldDropList)<len(newDropList) and (len(oldDropDict)!= 0 and len(newDropList) != 0):
        difDropList = set(newDropList) - set(oldDropList)
    else:
        return difDropList
    difDropList  = list(difDropList)
    return difDropList


def insert_dropped_jobs_to_tmp_table(query, extra):
    """

    :return: extra sql query
    """

    newquery = copy.deepcopy(query)

    # insert retried pandaids to tmp table
    if dbaccess['default']['ENGINE'].find('oracle') >= 0:
        tmpTableName = "ATLAS_PANDABIGMON.TMP_IDS1DEBUG"
    else:
        tmpTableName = "TMP_IDS1DEBUG"

    transactionKey = random.randrange(1000000)
    new_cur = connection.cursor()

    jeditaskid = newquery['jeditaskid']

    ins_query = """
    INSERT INTO {0} 
    (ID,TRANSACTIONKEY,INS_TIME) 
    select pandaid, {1}, TO_DATE('{2}', 'YYYY-MM-DD') from (
        select unique pandaid from (
            select j.pandaid, j.jeditaskid, j.eventservice, j.specialhandling, j.jobstatus, j.jobsetid, j.jobsubstatus, j.processingtype,
                    h.oldpandaid, h.relationtype, h.newpandaid
            from (
                select ja4.pandaid, ja4.jeditaskid, ja4.eventservice, ja4.specialhandling, ja4.jobstatus, ja4.jobsetid, ja4.jobsubstatus, ja4.processingtype 
                    from ATLAS_PANDA.JOBSARCHIVED4 ja4 where ja4.jeditaskid = {3}
                union
                select ja.pandaid, ja.jeditaskid, ja.eventservice, ja.specialhandling, ja.jobstatus, ja.jobsetid, ja.jobsubstatus, ja.processingtype 
                    from ATLAS_PANDAARCH.JOBSARCHIVED ja where ja.jeditaskid = {4}
            ) j
            LEFT JOIN
            ATLAS_PANDA.jedi_job_retry_history h
            ON (h.jeditaskid = j.jeditaskid AND h.oldpandaid = j.pandaid) 
                OR (h.oldpandaid=j.jobsetid and h.jeditaskid = j.jeditaskid)
            )
            where 
              (oldpandaid is not null 
               AND oldpandaid != newpandaid 
               AND relationtype in ('', 'retry', 'pmerge', 'merge', 'jobset_retry', 'es_merge', 'originpandaid')
               AND
                (( 
                  (oldpandaid = pandaid and NOT (eventservice is not NULL and not specialhandling like '%sc:%')  
                        AND (relationtype='' OR relationtype='retry' 
                            or  (processingtype='pmerge' 
                                and jobstatus in ('failed','cancelled') 
                                and relationtype='merge')
                            )
                  )
                  OR
                  (
                    (oldpandaid = pandaid and eventservice in (1,2,4,5) and specialhandling not like '%sc:%')  
                    AND 
                    (
                        (jobstatus not IN ('finished', 'merging') AND relationtype='retry') 
                        OR 
                        (jobstatus='closed'  and (jobsubstatus in ('es_unused', 'es_inaction')))
                    )
                  )
                )   
                OR (oldpandaid=jobsetid and relationtype = 'jobset_retry')
                )
              ) 
              OR  (jobstatus='closed' and (jobsubstatus in ('es_unused', 'es_inaction')))
    )                   
    """.format(tmpTableName, transactionKey, timezone.now().strftime("%Y-%m-%d"), jeditaskid, jeditaskid)

    new_cur.execute(ins_query)
    # form an extra query condition to exclude retried pandaids from selection
    extra += " AND pandaid not in ( select id from {0} where TRANSACTIONKEY = {1})".format(tmpTableName, transactionKey)

    return extra, transactionKey