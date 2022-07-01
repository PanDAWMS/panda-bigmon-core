from datetime import datetime, timedelta
from django.db import connection, connections
from core.libs.exlib import dictfetchall
from core.libs.sqlsyntax import bind_var
from core.iDDS.useconstants import SubstitleValue
from core.settings.config import DB_SCHEMA_IDDS
from core.settings.local import defaultDatetimeFormat
subtitleValue = SubstitleValue()


def getTransforms(requestid):
    sqlpar = {"requestid": requestid}
    sql = """
    select r.request_id, wt.transform_id
    from {0}.requests r
     full outer join (
        select request_id, workprogress_id from {0}.workprogresses
     ) wp on (r.request_id=wp.request_id)
     full outer join {0}.wp2transforms wt on (wp.workprogress_id=wt.workprogress_id)
    where r.request_id=:requestid
    """.format(DB_SCHEMA_IDDS)
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
    select r.request_id, r.scope, r.name, r.status, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, 
        tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files
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


def prepareSQLQueryParameters(request_params, **kwargs):
    db = 'oracle'
    if 'db' in kwargs:
        db = kwargs['db']

    sqlpar, condition = {}, " (1=1)  "
    request_params = {key: value for key, value in request_params.items() if key in ['requestid', 'username', 'status']}
    query_fields_for_subst = ['status']
    dict_for_subst = {key:request_params.get(key) for key in query_fields_for_subst if key in request_params}
    query_params_substituted = subtitleValue.replaceInverseKeys('requests', dict_for_subst)

    sqlpar['starttime'] = (datetime.utcnow()-timedelta(hours=24*90)).strftime(defaultDatetimeFormat)
    condition += 'and r.created_at > {} '.format(bind_var('starttime', db))

    for key in query_params_substituted.keys():
        request_params[key] = query_params_substituted[key]
    if request_params and len(request_params) > 0:
        if 'requestid' in request_params:
            sqlpar['requestid'] = request_params['requestid']
            condition += 'and r.request_id = {} '.format(bind_var('requestid', db))
        if 'username' in request_params:
            if request_params['username'] == 'Not set':
                condition += 'and r.username is null '
            else:
                sqlpar['username'] = request_params['username'].lower()
                condition += 'and lower(r.username) = {} '.format(bind_var('username', db))
        if 'status' in request_params:
            sqlpar['status'] = query_params_substituted.get('status')
            condition += 'and r.status = {} '.format(bind_var('status', db))
    return sqlpar, condition


def getWorkFlowProgressItemized(request_params, **kwargs):
    """
    Getting workflow progress in iDDS requests
    :param request_params:
    :param kwargs: idds_instance - for special a clone of iDDS app that uses separate DB instance
    :return:
    """

    connection_name = 'default'
    if 'idds_instance' in kwargs and kwargs['idds_instance'] == 'gcp':
        connection_name = 'doma_idds_gcp'
    db = connections[connection_name].vendor
    style = 'default'
    if db == 'postgresql':
        style = 'uppercase'
    sqlpar, condition = prepareSQLQueryParameters(request_params, db=db)
    sql = f"""
    select r.request_id, r.name as r_name, r.status as r_STATUS, r.created_at as r_created_at, c.total_files, 
    c.processed_files, c.processing_files, c.transform_id, t.workload_id, p.status as p_status, r.username from {DB_SCHEMA_IDDS}.requests r left join {DB_SCHEMA_IDDS}.collections c on r.request_id=c.request_id
    left join {DB_SCHEMA_IDDS}.transforms t on t.transform_id = c.transform_id 
    left join {DB_SCHEMA_IDDS}.processings p on p.transform_id=t.transform_id
    where c.relation_type=0 and {condition} order by r.request_id desc
    """
    cur = connections[connection_name].cursor()
    # cur = connection.cursor()
    # cur.execute('select count(request_id) from doma_idds.requests')
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur, style=style)
    cur.close()
    return rows

