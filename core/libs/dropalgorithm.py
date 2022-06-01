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
    if 'is_return_dropped_jobs' in kwards and kwards['is_return_dropped_jobs'] is True:
        is_return_dropped_jobs = True

    drop_list = []
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
    _logger.debug('Got {} retries whereas total number of jobs is {}: {} sec'.format(len(retries), len(jobs),
                                                                                    (time.time() - start_time)))

    hashRetries = {}
    # old run job -> new run job (relaton_type=retry) and run job -> merge job (relation_type=merge)
    hashMergeRelation = {}
    for retry in retries:
        hashRetries[retry['oldpandaid']] = retry
        if retry['relationtype'] == 'merge':
            # adding to dict all the run jobs for which merge job was created
            if retry['newpandaid'] not in hashMergeRelation:
                hashMergeRelation[retry['newpandaid']] = []
            hashMergeRelation[retry['newpandaid']].append(retry['oldpandaid'])

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
            if job['processingtype'] == 'pmerge' and pandaid in hashMergeRelation:
                for oldrunpandaid in hashMergeRelation[pandaid]:
                    if oldrunpandaid in hashRetries and hashRetries[oldrunpandaid]['relationtype'] == 'retry':
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

        if dropJob == 0:
            newjobs.append(job)
        else:
            if is_return_dropped_jobs:
                if job['processingtype'] != 'pmerge':
                    drop_list.append({'pandaid': pandaid, 'newpandaid': dropJob})
                elif job['processingtype'] == 'pmerge':
                    drop_merge_list.add(pandaid)

    _logger.debug('{} jobs dropped: {} sec'.format(len(jobs) - len(newjobs), time.time() - start_time))
    drop_list = sorted(drop_list, key=lambda x: -x['pandaid'])
    jobs = newjobs

    return jobs, drop_list, drop_merge_list


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