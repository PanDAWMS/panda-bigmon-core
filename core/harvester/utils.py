
from datetime import datetime, timedelta

from django.db import connection
from django.db.models import Q, Count, Sum

from core.harvester.models import HarvesterWorkerStats, HarvesterWorkers
from core.pandajob.utils import identify_jobtype

from core.settings.local import defaultDatetimeFormat


def isHarvesterJob(pandaid):

    jobHarvesterInfo = []

    sqlQuery = """
    SELECT workerid, HARVESTERID, BATCHLOG, COMPUTINGELEMENT, ERRORCODE, DIAGMESSAGE  FROM (SELECT 
      a.PANDAID,
      a.workerid,
      a.HARVESTERID,
      b.BATCHLOG,
      b.COMPUTINGELEMENT,
      b.ERRORCODE,
      b.DIAGMESSAGE
      FROM ATLAS_PANDA.HARVESTER_REL_JOBS_WORKERS a,
      ATLAS_PANDA.HARVESTER_WORKERS b
      WHERE a.harvesterid = b.harvesterid and a.workerid = b.WORKERID) where pandaid = {0}
  """
    sqlQuery = sqlQuery.format(str(pandaid))

    cur = connection.cursor()
    cur.execute(sqlQuery)

    job = cur.fetchall()

    if len(job) == 0:
        return False

    columns = [str(column[0]).lower() for column in cur.description]

    for pid in job:
        jobHarvesterInfo.append(dict(zip(columns, pid)))

    return jobHarvesterInfo


def get_harverster_workers_for_task(jeditaskid):
    """
    :param jeditaskid: int
    :return: harv_workers_list: list
    """
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


def get_workers_summary_split(query, **kwargs):
    """Get statistics of submitted and running Harvester workers"""
    N_HOURS = 100
    wquery = {}
    if 'computingsite__in' in query:
        wquery['computingsite__in'] = query['computingsite__in']
    if 'resourcetype' in query:
        wquery['resourcetype'] = query['resourcetype']

    if 'source' in kwargs and kwargs['source'] == 'HarvesterWorkers':
        wquery['submittime__castdate__range'] = [
            (datetime.utcnow()-timedelta(hours=N_HOURS)).strftime(defaultDatetimeFormat),
            datetime.utcnow().strftime(defaultDatetimeFormat)
        ]
        wquery['status__in'] = ['running', 'submitted']
        # wquery['jobtype__in'] = ['managed', 'user', 'panda']
        w_running = Count('jobtype', filter=Q(status__exact='running'))
        w_submitted = Count('jobtype', filter=Q(status__exact='submitted'))
        w_values = ['computingsite', 'resourcetype', 'jobtype']
        worker_summary = HarvesterWorkers.objects.filter(**wquery).values(*w_values).annotate(nwrunning=w_running).annotate(nwsubmitted=w_submitted)
    else:
        wquery['jobtype__in'] = ['managed', 'user', 'panda']
        w_running = Sum('nworkers', filter=Q(status__exact='running'))
        w_submitted = Sum('nworkers', filter=Q(status__exact='submitted'))
        w_values = ['computingsite', 'resourcetype', 'jobtype']
        worker_summary = HarvesterWorkerStats.objects.filter(**wquery).values(*w_values).annotate(nwrunning=w_running).annotate(nwsubmitted=w_submitted)

    # Translate prodsourcelabel values to descriptive analy|prod job types
    worker_summary = identify_jobtype(worker_summary, field_name='jobtype')

    return list(worker_summary)