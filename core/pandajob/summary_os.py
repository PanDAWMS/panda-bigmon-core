"""
A set of functions to get jobs from JOBS* and group them by object store
"""

from django.db import connection
from django.conf import settings
from core.libs.cache import setCacheData
from core.libs.sqlcustom import fix_lob

from core.schedresource.utils import get_object_stores

import core.constants as const


def objectstore_summary_data(hours):

    sqlRequest = """
    select jobstatus, count(jobstatus) as countjobsinstate, computingsite, objse, rtrim(xmlagg(xmlelement(e,pandaid,',').extract('//text()') order by pandaid).getclobval(),',') as pandalist 
    from 
      (select distinct t1.pandaid, nucleus, computingsite, jobstatus, tasktype, es, case when t2.objstore_id > 0 then to_char(t2.objstore_id) else t3.destinationse end as objse 
      from {DB_SCHEMA_BIGMON}.combined_wait_act_def_arch4 t1 
      left join {DB_SCHEMA_PANDA}.jedi_events t2 on t1.pandaid=t2.pandaid and t1.jeditaskid =  t2.jeditaskid and (t2.ziprow_id>0 or t2.objstore_id > 0) 
      left join {DB_SCHEMA_PANDA}.filestable4 t3 on (t3.pandaid = t2.pandaid and  t3.jeditaskid = t2.jeditaskid and t3.row_id=t2.ziprow_id) where t1.es in (1) and t1.cloud='world' and t1.modificationtime > (sysdate - interval '{hours}' hour) 
      and t3.modificationtime >  (sysdate - interval '{hours}' hour)
      ) 
    where not objse is null 
    group by jobstatus, jobstatus, computingsite, objse 
    order by objse
    """.format(hours=hours, DB_SCHEMA_PANDA=settings.DB_SCHEMA_PANDA, DB_SCHEMA_BIGMON=settings.DB_SCHEMA)

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
