import itertools, random, copy
from django.db import connection
from django.utils import timezone
from core.common.models import JediJobRetryHistory
from core.settings.local import dbaccess

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
        new_cur = connection.cursor()
        new_cur.execute("DELETE FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i" % (tk))
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
            new_cur.execute("DELETE FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i" % (transactionKey))
            new_cur.execute("DELETE FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i" % (tk))
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
    new_cur.execute("DELETE FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i" % (transactionKey))
    return dropPmerge,notDropPmerge

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
                    h.oldpandaid, h.relationtype
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
              (oldpandaid is not null and
                (( 
                  (NOT (eventservice is not NULL and not specialhandling like '%sc:%')  
                        AND (relationtype='' OR relationtype='retry' 
                            or  (processingtype='pmerge' 
                                and jobstatus in ('failed','cancelled') 
                                and relationtype='merge')
                            )
                  )
                  OR
                  (
                    (eventservice in (1,2,4,5) and specialhandling not like '%sc:%')  
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