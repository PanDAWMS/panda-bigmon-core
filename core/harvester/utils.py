from django.db import connection

def isHarvesterJob(pandaid):

    jobHarvesterInfo = []

    sqlQuery = """
    SELECT workerid,HARVESTERID, BATCHLOG, COMPUTINGELEMENT FROM (SELECT 
      a.PANDAID,
      a.workerid,
      a.HARVESTERID,
      b.BATCHLOG,
      b.COMPUTINGELEMENT
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