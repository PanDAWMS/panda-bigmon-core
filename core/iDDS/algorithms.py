from django.db import connection

from core.iDDS.useconstants import SubstitleValue
from core.libs.exlib import dictfetchall
from django.conf import settings


def generate_requests_summary(requests):
    fields_to_aggregate = ['status']
    agg_dict = {}
    for request in requests:
        for field in fields_to_aggregate:
            agg_dict[request[field]] = agg_dict.get(request[field], 0) + 1
    return agg_dict


def parse_request(request):
    retdict = {}
    status = request.session['requestParams'].get('reqstatus', None)
    if status:
        status = status.strip()
        retdict['reqstatus'] = status
    return retdict


def getiDDSInfoForTask(jeditaskid):
    subtitleValue = SubstitleValue()

    new_cur = connection.cursor()
    query = """
        select r.request_id, r.scope, r.name, r.request_type, r.transform_tag, r.workload_id, r.status, 
        r.created_at request_created_at, r.updated_at request_updated_at, tr.transform_id, tr.transform_status, 
        tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files, 
        tr.out_created_at, tr.out_updated_at
    from {0}.requests r
    join (
      select t.request_id, t.transform_id, t.workload_id, t.status transform_status, in_coll.status in_status, 
        in_coll.total_files in_total_files, in_coll.processed_files in_processed_files,
        out_coll.status out_status, out_coll.total_files out_total_files, out_coll.processed_files out_processed_files, 
        out_coll.created_at out_created_at, out_coll.updated_at out_updated_at
      from {0}.transforms t
      left join (select coll_id , transform_id, status, total_files, processed_files, created_at, updated_at 
                 from {0}.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
      left join (select coll_id , transform_id, status, total_files, processed_files, created_at, updated_at 
                 from {0}.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
    ) tr on (r.request_id=tr.request_id and tr.workload_id={1})
    """.format(settings.DB_SCHEMA_IDDS, int(jeditaskid))

    new_cur.execute(query)

    results = dictfetchall(new_cur)

    transformed_results = []
    map = subtitleValue.substitleMap

    for row in results:
        transformed_row = {k.lower(): v for k, v in row.items()}
        transformed_row['status'] = map['requests']['status'][transformed_row['status']]
        transformed_row['request_type'] = map['requests']['request_type'][transformed_row['request_type']]
        transformed_row['transform_status'] = map['requests']['transform_status'][transformed_row['transform_status']]

        if transformed_row['in_status'] is not None:
            transformed_row['in_status'] = map['requests']['in_status'][transformed_row['in_status']]
        if transformed_row['out_status'] is not None:
            transformed_row['out_status'] = map['requests']['out_status'][transformed_row['out_status']]

        if transformed_row['out_total_files'] and transformed_row['out_total_files'] != 0:
            transformed_row['pctprocessed'] = int(
                100. * transformed_row['out_processed_files'] / transformed_row['out_total_files'])
        else:
            transformed_row['pctprocessed'] = 0

        transformed_results.append(transformed_row)

    return transformed_results


def checkIfIddsTask(taskinfo):
    if taskinfo['splitrule']:
        split_rule = str(taskinfo['splitrule']).split(',')
        if 'HO=1' in split_rule:
            return 'hpo'
    if taskinfo['tasktype']:
        if taskinfo['tasktype'] == "prod" or taskinfo['tasktype'] == 'anal':
            return 'idds'
    return None


def get_connection_name():
    """
    Deciding which connection to use for raw SQL queries
    :return: connection_name: str
    """
    connection_name = 'default'
    if 'idds' in settings.DATABASES:
        connection_name = 'idds'
    return connection_name
