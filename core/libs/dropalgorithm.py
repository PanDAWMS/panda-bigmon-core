import itertools, random
from django.db import connection
from core.common.models import JediJobRetryHistory

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
    notDroppetPmerge = set()
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
                droppedPmerge,notDroppetPmerge = clearDropPmergeRetrielsJobs(droppedPmerge, retryquery)
                if not isReturnDroppedPMerge:
                    droplist = droplist + list(droppedPmerge)
                    droppedPmerge = []
                else:
                    newjobs = newjobs + list(notDroppetPmerge)
                    droppedPmerge = []
        new_cur = connection.cursor()
        new_cur.execute("DELETE FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i" % (tk))
        return newjobs, droppedPmerge,droplist
    else:
        pandaDropIDList=set()
        checkDropJobs = []
        for job in jobs:
            pandaid = job['pandaid']
            if job['jobstatus'] == 'closed' and job['jobsubstatus'] in ('es_unused', 'es_inaction'):
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
                where=["OLDPANDAID IN (SELECT ID FROM ATLAS_PANDABIGMON.TMP_IDS1Debug WHERE TRANSACTIONKEY=%i)" % (
                transactionKey)]).values("oldpandaid","relationtype")

            retry_list = {}
            for retry in retries:
                retry_list[retry["oldpandaid"]]=retry["relationtype"]
            retry_keys = retry_list.keys()
            #retry_list = [d["oldpandaid"] for d in list(retries)]
            for checkJob in checkDropJobs:
                if checkJob["pandaid"] in retry_keys:
                    if retry_list[checkJob["pandaid"]]=="retry":
                        pandaDropIDList.add(checkJob["pandaid"])
                   # elif retry_list[failedjob["pandaid"]]=="es_merge" and failedjob["jobstatus"] =="failed"  and failedjob["jobsubstatus"]=="es_merge_failed":
                    #    pandaDropIDList.add(failedjob["pandaid"])
                    else: newjobs.append(checkJob)
                else:
                    newjobs.append(checkJob)
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