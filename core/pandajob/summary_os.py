"""
A set of functions to get jobs from JOBS* and group them by object store
"""

from django.db import connection
from core.libs.cache import setCacheData
from core.libs.sqlcustom import fix_lob

from core.schedresource.utils import get_object_stores

import core.constants as const


def objectstore_summary_data(hours):

    sqlRequest = """
    SELECT JOBSTATUS, COUNT(JOBSTATUS) as COUNTJOBSINSTATE, COMPUTINGSITE, OBJSE, RTRIM(XMLAGG(XMLELEMENT(E,PANDAID,',').EXTRACT('//text()') ORDER BY PANDAID).GetClobVal(),',') AS PANDALIST 
    FROM 
      (SELECT DISTINCT t1.PANDAID, NUCLEUS, COMPUTINGSITE, JOBSTATUS, TASKTYPE, ES, CASE WHEN t2.OBJSTORE_ID > 0 THEN TO_CHAR(t2.OBJSTORE_ID) ELSE t3.destinationse END AS OBJSE 
      FROM ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4 t1 
      LEFT JOIN ATLAS_PANDA.JEDI_EVENTS t2 ON t1.PANDAID=t2.PANDAID and t1.JEDITASKID =  t2.JEDITASKID and (t2.ziprow_id>0 or t2.OBJSTORE_ID > 0) 
      LEFT JOIN ATLAS_PANDA.filestable4 t3 ON (t3.pandaid = t2.pandaid and  t3.JEDITASKID = t2.JEDITASKID and t3.row_id=t2.ziprow_id) WHERE t1.ES in (1) and t1.CLOUD='WORLD' and t1.MODIFICATIONTIME > (sysdate - interval '{hours}' hour) 
      AND t3.MODIFICATIONTIME >  (sysdate - interval '{hours}' hour)
      ) 
    WHERE NOT OBJSE IS NULL 
    GROUP BY JOBSTATUS, JOBSTATUS, COMPUTINGSITE, OBJSE 
    order by OBJSE
    """.format(hours=hours)

    cur = connection.cursor()
    cur.execute(sqlRequest)
    rawsummary = fix_lob(cur)

    return rawsummary


def objectstore_summary(request, hours=12):
    object_stores = get_object_stores()
    rawsummary = objectstore_summary_data(hours)

    mObjectStores = {}
    mObjectStoresTk = {}
    if len(rawsummary) > 0:
        for row in rawsummary:
            id = -1
            try:
                id = int(row[3])
            except ValueError:
                pass

            if not row[3] is None and id in object_stores:
                osName = object_stores[id]['name']
            else:
                osName = "Not defined"
            compsite = row[2]
            status = row[0]
            count = row[1]

            tk = setCacheData(request, pandaid=row[4], compsite=row[2])
            if osName in mObjectStores:
                if not compsite in mObjectStores[osName]:
                    mObjectStores[osName][compsite] = {}
                    for state in const.JOB_STATES_SITE + ["closed"]:
                        mObjectStores[osName][compsite][state] = {'count': 0, 'tk': 0}
                mObjectStores[osName][compsite][status] = {'count': count, 'tk': tk}
                if not status in mObjectStoresTk[osName]:
                    mObjectStoresTk[osName][status] = []
                mObjectStoresTk[osName][status].append(tk)
            else:
                mObjectStores[osName] = {}
                mObjectStores[osName][compsite] = {}
                mObjectStoresTk[osName] = {}
                mObjectStoresTk[osName][status] = []
                for state in const.JOB_STATES_SITE + ["closed"]:
                    mObjectStores[osName][compsite][state] = {'count': 0, 'tk': 0}
                mObjectStores[osName][compsite][status] = {'count': count, 'tk': tk}
                mObjectStoresTk[osName][status].append(tk)

    # Getting tk's for parents
    for osName in mObjectStoresTk:
        for state in mObjectStoresTk[osName]:
            mObjectStoresTk[osName][state] = setCacheData(request, childtk=','.join(mObjectStoresTk[osName][state]))

    mObjectStoresSummary = {}
    for osName in mObjectStores:
        mObjectStoresSummary[osName] = {}
        for site in mObjectStores[osName]:
            for state in mObjectStores[osName][site]:
                if state in mObjectStoresSummary[osName]:
                    mObjectStoresSummary[osName][state]['count'] += mObjectStores[osName][site][state]['count']
                    mObjectStoresSummary[osName][state]['tk'] = 0

                else:
                    mObjectStoresSummary[osName][state] = {}
                    mObjectStoresSummary[osName][state]['count'] = mObjectStores[osName][site][state]['count']
                    mObjectStoresSummary[osName][state]['tk'] = 0
    for osName in mObjectStoresSummary:
        for state in mObjectStoresSummary[osName]:
            if mObjectStoresSummary[osName][state]['count'] > 0:
                mObjectStoresSummary[osName][state]['tk'] = mObjectStoresTk[osName][state]

    return mObjectStores, mObjectStoresSummary
