from django.db import connection

class ReportsDataSource:
    def __init__(self):
        pass

    def getCompaingTasksJobsStats(self, campaigname, depthhours, mcordpd = True):

        if mcordpd:
            tasksCondition = "tasktype = 'prod' and WORKINGGROUP NOT IN('AP_REPR', 'AP_VALI', 'GP_PHYS', 'GP_THLT') and " \
                             "processingtype in ('evgen', 'pile', 'simul', 'recon') and campaign LIKE '%s%%'" % campaigname
        else:
            tasksCondition = "tasktype='prod' and workinggroup='GP_PHYS'"

        sqlRequest = '''
        SELECT * FROM (
        SELECT tj AS JEDITASKID, SITE_JOB, MAX(SUPERSTATUS) as SUPERSTATUS,

        SUM(CASE WHEN JOBSTATUS = 'finished'
               THEN 1 ELSE 0 END) AS Njobssucceded,

        SUM(CASE WHEN JOBSTATUS = 'failed'
               THEN 1 ELSE 0 END) AS Njobsfailed,

        SUM(CASE WHEN JOBSTATUS = 'running'
               THEN 1 ELSE 0 END) AS Njobsrunning,

        SUM(CASE WHEN JOBSTATUS = 'finished'
               THEN WT ELSE 0 END) AS WalltimeSucceded,

        SUM(CASE WHEN JOBSTATUS = 'failed'
               THEN WT ELSE 0 END) AS WallTimeFailed,

        SUM(CASE WHEN JOBSTATUS = 'finished' AND ENDTIME >= (sysdate-%i/24-1/24)
            THEN NEVENTS ELSE 0 END) AS DeliveredEventsH,
        SUM(CASE WHEN JOBSTATUS = 'finished' THEN NEVENTS ELSE 0 END) AS DeliveredEvents,

        SUM(CASE WHEN JOBSTATUS = 'failed' AND ENDTIME >= (sysdate-%i/24-1/24)
               THEN WT ELSE 0 END) AS WallTimeFailedH,

        SUM(CASE WHEN JOBSTATUS = 'finished' AND ENDTIME >= (sysdate-%i/24-1/24)
               THEN 1 ELSE 0 END) AS NjobssuccededH,

        SUM(CASE WHEN JOBSTATUS = 'failed' AND ENDTIME >= (sysdate-%i/24-1/24)
               THEN 1 ELSE 0 END) AS NjobsfailedH,

        SUM(CASE WHEN JOBSTATUS = 'activated'
               THEN 1 ELSE 0 END) AS Njobsactivated,

        SUM(CASE WHEN JOBSTATUS = 'assigned'
               THEN 1 ELSE 0 END) AS Njobsassigned,

        SUM(CASE WHEN JOBSTATUS = 'finished' AND ENDTIME >= (sysdate-%i/24-1/24)
            THEN WT ELSE 0 END) AS WalltimeSuccededH


        FROM (
        WITH filtered_tasks AS (
          SELECT JEDITASKID tt, SUPERSTATUS as SUPERSTATUS FROM JEDI_TASKS WHERE %s
          )
          SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
            (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSARCHIVED4 JOIN filtered_tasks ON filtered_tasks.tt = JOBSARCHIVED4.JEDITASKID AND PROCESSINGTYPE!='pmerge' UNION
          (SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
            (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSARCHIVED JOIN filtered_tasks ON filtered_tasks.tt = JOBSARCHIVED.JEDITASKID AND PROCESSINGTYPE!='pmerge' MINUS
          SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
            (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSARCHIVED4 JOIN filtered_tasks ON filtered_tasks.tt = JOBSARCHIVED4.JEDITASKID AND PROCESSINGTYPE!='pmerge')  UNION
          SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
                            (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSACTIVE4 JOIN filtered_tasks ON filtered_tasks.tt = JOBSACTIVE4.JEDITASKID AND PROCESSINGTYPE!='pmerge' UNION
          SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
                        (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSDEFINED4 JOIN filtered_tasks ON filtered_tasks.tt = JOBSDEFINED4.JEDITASKID AND PROCESSINGTYPE!='pmerge' UNION
          SELECT PANDAID, JEDITASKID as tj, COMPUTINGSITE as SITE_JOB, SUPERSTATUS, JOBSTATUS, NEVENTS, ENDTIME,
          (ENDTIME-STARTTIME)*CORECOUNT AS WT FROM JOBSWAITING4 JOIN filtered_tasks ON filtered_tasks.tt = JOBSWAITING4.JEDITASKID AND PROCESSINGTYPE!='pmerge'
        ) table_jobs GROUP BY tj, SITE_JOB) tasks_grouped
        ''' % (depthhours,depthhours,depthhours,depthhours,depthhours, tasksCondition)
        cur = connection.cursor()
        cur.execute(sqlRequest)
        campaignsummary = cur.fetchall()
        cur.close()
        return campaignsummary

    def getCompaingTasksEventsStats(self, campaigname, mcordpd = True):
        if mcordpd:
            tasksCondition = "tasktype = 'prod' and WORKINGGROUP NOT IN('AP_REPR', 'AP_VALI', 'GP_PHYS', 'GP_THLT') and " \
                             "processingtype in ('evgen', 'pile', 'simul', 'recon') and campaign LIKE '%s%%'" % campaigname
        else:
            tasksCondition = "tasktype='prod' and workinggroup='GP_PHYS'"

        sqlRequest = '''SELECT JEDITASKID as tt, STATUS as TASKSTATUS, table1.TOTEVREM, table1.TOTEV FROM JEDI_TASKS
JOIN (select JEDITASKID as JEDITASKID1, SUM(NEVENTS-NEVENTSUSED) as TOTEVREM, SUM(NEVENTS) as TOTEV from ATLAS_PANDA.JEDI_DATASETS WHERE (TYPE IN ('input','pseudo_input')) AND MASTERID IS NULL GROUP BY JEDITASKID)table1
ON table1.JEDITASKID1 = JEDI_TASKS.JEDITASKID and %s''' % (tasksCondition)
        #print sqlRequest
        cur = connection.cursor()
        cur.execute(sqlRequest)
        campaignsummary = cur.fetchall()
        cur.close()
        return campaignsummary
