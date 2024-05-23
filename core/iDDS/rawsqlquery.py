import logging
from datetime import datetime, timedelta, timezone
from django.db import connections
from core.libs.exlib import dictfetchall
from core.libs.sqlsyntax import bind_var
from core.iDDS.useconstants import SubstitleValue
from core.iDDS.algorithms import get_connection_name

from django.conf import settings

subtitleValue = SubstitleValue()
_logger = logging.getLogger('bigpandamon')


def getRequests(query_params, condition='(1=1)'):

    sqlpar = {}
    if query_params and len(query_params) > 0:
        query_params = subtitleValue.replaceInverseKeys('requests', query_params)

        if 'reqstatus' in query_params:
            sqlpar['rstatus'] = query_params['reqstatus']
            condition += 'AND r.status = :rstatus'

    sql = f"""
    select r.request_id, r.scope, r.name, r.status, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, 
        tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files
    from {settings.DB_SCHEMA_IDDS}.requests r
    full outer join (
        select t.request_id, t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files,
        in_coll.processed_files in_processed_files, out_coll.status out_status, out_coll.total_files out_total_files,
        out_coll.processed_files out_processed_files
        from {settings.DB_SCHEMA_IDDS}.transforms t
        full outer join (select coll_id , transform_id, status, total_files, processed_files from {settings.DB_SCHEMA_IDDS}.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
        full outer join (select coll_id , transform_id, status, total_files, processed_files from {settings.DB_SCHEMA_IDDS}.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
    ) tr on (r.request_id=tr.request_id)
    where {condition}
    """
    connection_name = get_connection_name()
    cur = connections[connection_name].cursor()
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur)
    cur.close()
    return rows


def prepareSQLQueryParameters(request_params, **kwargs):
    db = 'oracle'
    if 'db' in kwargs:
        db = kwargs['db']

    if 'days' in request_params:
        days = int(request_params['days'])
    else:
        days = 7

    sqlpar, condition = {}, " (1=1)  "
    request_params = {key: value for key, value in request_params.items() if key in ['requestid', 'username', 'status']}
    # statuses are numbers in DB, need to translate using constants classes from iDDS
    query_fields_for_subst = ['status']
    dict_for_subst = {key: request_params.get(key) for key in query_fields_for_subst if key in request_params}
    query_params_substituted = subtitleValue.replaceInverseKeys('requests', dict_for_subst)

    sqlpar['starttime'] = (datetime.now(tz=timezone.utc) - timedelta(hours=24*days)).strftime(settings.DATETIME_FORMAT)
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
    :param kwargs: dict
    :return:
    """

    connection_name = get_connection_name()
    db = connections[connection_name].vendor
    style = 'default'
    if db == 'postgresql':
        style = 'uppercase'
    sqlpar, condition = prepareSQLQueryParameters(request_params, db=db)
    sql = f"""
    select r.request_id, r.name as r_name, r.status as r_STATUS, r.created_at as r_created_at, c.total_files, 
    c.processed_files, c.processing_files, c.transform_id, t.workload_id, t.transform_type, t.transform_tag, p.status as p_status, r.username 
    from {settings.DB_SCHEMA_IDDS}.requests r 
    left join {settings.DB_SCHEMA_IDDS}.collections c on r.request_id=c.request_id
    left join {settings.DB_SCHEMA_IDDS}.transforms t on t.transform_id=c.transform_id 
    left join {settings.DB_SCHEMA_IDDS}.processings p on p.transform_id=t.transform_id
    where (c.relation_type=0 or c.relation_type is null) and {condition} order by r.request_id desc
    """
    cur = connections[connection_name].cursor()
    _logger.debug('!!! Using connection named: {}, vendor: {}, host: {}, port: {}, user: {} \n Query: {}'.format(
        connection_name, db, connections[connection_name].settings_dict['HOST'],
        connections[connection_name].settings_dict['PORT'], connections[connection_name].settings_dict['USER'], sql
    ))
    cur.execute(sql, sqlpar)
    rows = dictfetchall(cur, style=style)
    cur.close()
    return rows
