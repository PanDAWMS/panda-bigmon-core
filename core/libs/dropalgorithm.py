import random
import copy
import logging
import time
from collections import deque
from django.db import connection
from django.utils import timezone
from core.libs.eventservice import is_event_service
from core.libs.exlib import get_tmp_table_name_debug, create_temporary_table
from core.common.models import JediJobRetryHistory

from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def drop_job_retries(jobs, jeditaskid, is_return_dropped_jobs=False, task_status=None):
    """
    Dropping algorithm for jobs belong to a single task, it drops previous retries leaving only the very last attempt.
    Jobs relations:
        retry - simple retry (old run job -> new run job)
        jobset_id - job cloning (old jobset -> new jobs)
        jobset_retry - retry of a whole jobset
        merge - both retries of a merge job and run-merge relation, (run job -> merge job  and old merge job -> new merge job)

    Args:
        jobs: list[dict] - mandatory job attributes: pandaid, jobstatus, processingtype, jobsetid, specialhandling
        jeditaskid: int
        is_return_dropped_jobs: bool
        task_status: str - status of the task needed to improve the algorithm

    Returns:
        jobs: list[dict]
        drop_list: list[dict]
        drop_merge_list: list - list of pandaids
    """
    start_time = time.time()

    drop_list = []
    drop_merge_list = set()
    FAIL_STATES = ('failed', 'cancelled', 'closed')

    # get job retry history for a task
    extra = """oldpandaid != newpandaid"""
    retries = JediJobRetryHistory.objects.filter(jeditaskid=jeditaskid).extra(where=[extra]).order_by('newpandaid').values()
    _logger.debug(f'Got {len(retries)} retries whereas total number of jobs is {len(jobs)}: {(time.time() - start_time)} sec')

    # create a hash of retried job to compare against the full list of jobs
    hash_retries = {}
    hash_merge_retries = {}
    merge_children = {}
    retried_roots = {}
    for retry in retries:
        hash_retries[retry['oldpandaid']] = retry
        if retry['relationtype'] == 'merge':
            merge_children.setdefault(retry['oldpandaid'], []).append(retry['newpandaid'])
            # adding to dict all the run jobs for which merge job was created
            if retry['newpandaid'] not in hash_merge_retries:
                hash_merge_retries[retry['newpandaid']] = []
            hash_merge_retries[retry['newpandaid']].append(retry['oldpandaid'])
        if retry['relationtype'] == 'retry':
            retried_roots[retry['oldpandaid']] = 0

    # list of merge retry tails which did not finish and the root was retried by itself - whole branch should be dropped
    to_drop_due_to_retry_chain = {}
    for root in retried_roots:
        stack = deque([root])
        while stack:
            node = stack.pop()
            for child in merge_children.get(node, []):
                if child not in to_drop_due_to_retry_chain:
                    to_drop_due_to_retry_chain[child] = 1
                    stack.append(child)

    newjobs = []
    for job in jobs:
        is_drop_job = 0
        pandaid = job['pandaid']
        if not is_event_service(job):
            # simple retry of run job
            if pandaid in hash_retries:
                retry = hash_retries[pandaid]
                if retry['relationtype'] == 'retry':
                    is_drop_job = 1
            # jobset retry and job cloning
            if 'jobsetid' in job and job['jobsetid'] in hash_retries:
                if hash_retries[job['jobsetid']]['relationtype'] == 'jobset_retry':
                    is_drop_job = 1
                # job cloning
                elif hash_retries[job['jobsetid']]['relationtype'] == 'jobset_id' and (
                        pandaid != hash_retries[job['jobsetid']]['newpandaid'] and job['jobstatus'] in FAIL_STATES):
                    is_drop_job = 1
            if job['processingtype'] == 'pmerge':
                # merge retries
                if pandaid in hash_merge_retries and any([
                    orpid in hash_retries and hash_retries[orpid]['relationtype'] == 'retry' for orpid in hash_merge_retries[pandaid]
                ]):
                    is_drop_job = 1
                elif pandaid in hash_retries and hash_retries[pandaid]['relationtype'] == 'merge' and job['jobstatus'] in FAIL_STATES:
                    is_drop_job = 1
                # covers case when the whole run->merge1->merge2->mergeN was retried at a different site
                elif pandaid in to_drop_due_to_retry_chain:
                    is_drop_job = 1
        else:
            if job['pandaid'] in hash_retries and job['jobstatus'] not in ('finished', 'merging'):
                if hash_retries[job['pandaid']]['relationtype'] == 'retry':
                    is_drop_job = 1
            if is_drop_job == 0:
                if job['jobsetid'] in hash_retries and hash_retries[job['jobsetid']]['relationtype'] == 'jobset_retry':
                    is_drop_job = 1
                if job['jobstatus'] == 'closed' and job['jobsubstatus'] in ('es_unused', 'es_inaction',):
                    is_drop_job = 1

        # drop unsuccessful build jobs for done tasks
        if task_status and task_status == 'done' and job['jobstatus'] in FAIL_STATES and (
                ('category' in job and job['category'] == 'build') or ('transformation' in job and 'buildGen' in job['transformation'])):
            is_drop_job = 1

        if is_drop_job == 0:
            newjobs.append(job)
        else:
            if is_return_dropped_jobs:
                if job['processingtype'] != 'pmerge':
                    drop_list.append({'pandaid': pandaid, 'newpandaid': is_drop_job})
                elif job['processingtype'] == 'pmerge':
                    drop_merge_list.add(pandaid)

    _logger.debug('{} jobs dropped: {} sec'.format(len(jobs) - len(newjobs), time.time() - start_time))
    drop_list = sorted(drop_list, key=lambda x: -x['pandaid'])
    jobs = newjobs

    return jobs, drop_list, drop_merge_list


def insert_dropped_jobs_to_tmp_table(query, extra):
    """
    Args:
        query: dict - with jeditaskid inside
        extra: str - extra where query where tail of jeditaskid in tmp table will be added

    Returns:
        extra: str
        transactionKey: int
    """

    newquery = copy.deepcopy(query)

    # insert retried pandaids to tmp table
    tmpTableName = get_tmp_table_name_debug()

    transactionKey = random.randrange(1000000)
    new_cur = connection.cursor()
    unique_key = 'unique'
    if settings.DEPLOYMENT == 'POSTGRES':
        unique_key = 'distinct'
        exist_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            ); 
        """     
        schema, tablename = tmpTableName.split(".")
        new_cur.execute(exist_query,[schema, tablename])
        exists = new_cur.fetchone()[0]
        if not exists:
            create_temporary_table(new_cur, tmpTableName)
    jeditaskid = newquery['jeditaskid']

    ins_query = """
    insert into {0} 
    (id,transactionkey,ins_time) 
    select pandaid, {1}, TO_DATE('{2}', 'YYYY-MM-DD') from (
        select {6} pandaid from (
            select j.pandaid, j.jeditaskid, j.eventservice, j.specialhandling, j.jobstatus, j.jobsetid, j.jobsubstatus, j.processingtype,
                    h.oldpandaid, h.relationtype, h.newpandaid
            from (
                select ja4.pandaid, ja4.jeditaskid, ja4.eventservice, ja4.specialhandling, ja4.jobstatus, ja4.jobsetid, ja4.jobsubstatus, ja4.processingtype 
                    from {4}.jobsarchived4 ja4 where ja4.jeditaskid = {3}
                union
                select ja.pandaid, ja.jeditaskid, ja.eventservice, ja.specialhandling, ja.jobstatus, ja.jobsetid, ja.jobsubstatus, ja.processingtype 
                    from {5}.jobsarchived ja where ja.jeditaskid = {3}
            ) j
            LEFT JOIN
            {4}.jedi_job_retry_history h
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
    """.format(
        tmpTableName,
        transactionKey,
        timezone.now().strftime("%Y-%m-%d"),
        jeditaskid,
        settings.DB_SCHEMA_PANDA,
        settings.DB_SCHEMA_PANDA_ARCH,
        unique_key
    )

    new_cur.execute(ins_query)
    # form an extra query condition to exclude retried pandaids from selection
    extra += " AND pandaid not in ( select id from {0} where transactionkey = {1})".format(tmpTableName, transactionKey)

    return extra, transactionKey