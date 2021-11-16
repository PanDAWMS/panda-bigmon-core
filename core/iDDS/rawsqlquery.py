from django.db import connection
from core.libs.exlib import dictfetchall
from core.iDDS.useconstants import SubstitleValue
from core.settings.config import DB_SCHEMA_IDDS
subtitleValue = SubstitleValue()

def getTransforms(requestid):
    sqlpar = {"requestid": requestid}
    sql = """
    select r.request_id, wt.transform_id
    from atlas_idds.requests r
     full outer join (
        select request_id, workprogress_id from atlas_idds.workprogresses
     ) wp on (r.request_id=wp.request_id)
     full outer join atlas_idds.wp2transforms wt on (wp.workprogress_id=wt.workprogress_id)
    where r.request_id=:requestid
    """
    cur = connection.cursor()
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur)
    cur.close()
    return rows


def getRequests(query_params):
    condition = '(1=1)'
    sqlpar = {}

    if query_params and len(query_params) > 0:
        query_params = subtitleValue.replaceInverseKeys('requests', query_params)

        if 'reqstatus' in query_params:
            sqlpar['rstatus'] = query_params['reqstatus']
            condition = 'r.status = :rstatus'


    sql = f"""
    select r.request_id, r.scope, r.name, r.status, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files
from {DB_SCHEMA_IDDS}.requests r
 full outer join (
    select t.request_id, t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files,
    in_coll.processed_files in_processed_files, out_coll.status out_status, out_coll.total_files out_total_files,
    out_coll.processed_files out_processed_files
    from {DB_SCHEMA_IDDS}.transforms t
    full outer join (select coll_id , transform_id, status, total_files, processed_files from {DB_SCHEMA_IDDS}.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
    full outer join (select coll_id , transform_id, status, total_files, processed_files from {DB_SCHEMA_IDDS}.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
 ) tr on (r.request_id=tr.request_id)
    where {condition}
    """

    cur = connection.cursor()
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur)
    cur.close()
    return rows


def getWorkFlowProgressItemized(query_params=None):
    condition = '(1=1)'
    sqlpar = {}
    if query_params and len(query_params) > 0:
        query_params = subtitleValue.replaceInverseKeys('requests', query_params)

        if 'requestid' in query_params:
            sqlpar['requestid'] = query_params['requestid']
            condition = 'r.REQUEST_ID = :requestid'

    sql = f"""
    SELECT r.REQUEST_ID, r.NAME as r_NAME, r.STATUS as r_STATUS, r.CREATED_AT as r_CREATED_AT, r.CREATED_AT as r_CREATED_AT, c.total_files, 
    c.processed_files, c.processing_files, c.transform_id, t.workload_id, p.status as p_status FROM {DB_SCHEMA_IDDS}.requests r LEFT JOIN {DB_SCHEMA_IDDS}.collections c ON r.REQUEST_ID=c.REQUEST_ID
    LEFT JOIN {DB_SCHEMA_IDDS}.transforms t ON t.transform_id = c.transform_id 
    LEFT JOIN {DB_SCHEMA_IDDS}.processings p on p.transform_id=t.transform_id
    where c.relation_type=0 and {condition} order by r.request_id desc
    """
    cur = connection.cursor()
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur)
    cur.close()
    return rows

