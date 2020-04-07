from django.db import connection
from core.libs.exlib import dictfetchall
from core.iDDS.useconstants import SubstitleValue

subtitleValue = SubstitleValue()


def getRequests(query_params):
    condition = '(1=1)'
    sqlpar = {}

    if query_params and len(query_params) > 0:
        query_params = subtitleValue.replaceInverseKeys('requests', query_params)

        if 'reqstatus' in query_params:
            sqlpar['rstatus'] = query_params['reqstatus']
            condition = 'r.status = :rstatus'

    sql = """
        select r.request_id, r.scope, r.name, r.status, r.workload_id, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files
        from ATLAS_IDDS.requests r
         full outer join ATLAS_IDDS.req2transforms rt on (r.request_id=rt.request_id)
         full outer join (
            select t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files, in_coll.processed_files in_processed_files,
            out_coll.status out_status, out_coll.total_files out_total_files, out_coll.processed_files out_processed_files
            from ATLAS_IDDS.transforms t
            full outer join (select coll_id , transform_id, status, total_files, processed_files from ATLAS_IDDS.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
            full outer join (select coll_id , transform_id, status, total_files, processed_files from ATLAS_IDDS.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
            ) tr on (rt.transform_id=tr.transform_id)
        where %s
    """ % condition

    cur = connection.cursor()
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur)
    cur.close()
    return rows