from django.db import connection

from core.iDDS.useconstants import SubstitleValue
from core.libs.exlib import dictfetchall

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

    transformationWithNested = None

    new_cur = connection.cursor()
    new_cur.execute(
        """ select r.request_id, r.scope, r.name, r.request_type, r.transform_tag, r.workload_id, r.status, r.created_at request_created_at, r.updated_at request_updated_at, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files, tr.out_created_at, tr.out_updated_at
            from atlas_idds.requests r
             join atlas_idds.req2transforms rt on (r.request_id=rt.request_id and r.workload_id={0})
             join (
                select t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files, in_coll.processed_files in_processed_files,
                out_coll.status out_status, out_coll.total_files out_total_files, out_coll.processed_files out_processed_files, out_coll.out_created_at, out_coll.out_updated_at
                from atlas_idds.transforms t
                join (select coll_id , transform_id, status, total_files, processed_files from atlas_idds.collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
                join (select coll_id , transform_id, status, total_files, processed_files, created_at out_created_at, updated_at out_updated_at from atlas_idds.collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
                ) tr on (rt.transform_id=tr.transform_id)
            """.format(int(jeditaskid)))

    transformationWithNested = dictfetchall(new_cur)

    if len(transformationWithNested) > 0:
        transformationWithNested = {k.lower(): v for k, v in transformationWithNested[0].items()}
        map = subtitleValue.substitleMap
        transformationWithNested['status'] = map['requests']['status'][transformationWithNested['status']]
        transformationWithNested['request_type'] = map['requests']['request_type'][
            transformationWithNested['request_type']]
        transformationWithNested['transform_status'] = map['requests']['transform_status'][
            transformationWithNested['transform_status']]
        transformationWithNested['in_status'] = map['requests']['in_status'][transformationWithNested['in_status']]
        transformationWithNested['out_status'] = map['requests']['out_status'][transformationWithNested['out_status']]
        try:
            transformationWithNested['pctprocessed'] = int(100. * transformationWithNested['out_processed_files'] / transformationWithNested['out_total_files'])
        except:
            transformationWithNested['pctprocessed'] = 0
    return transformationWithNested

def checkIfIddsTask(taskinfo):
    if taskinfo['splitrule']:
        split_rule = str(taskinfo['splitrule']).split(',')
        if 'HO=1' in split_rule:
            return 'hpo'
    if taskinfo['tasktype']:
        if taskinfo['tasktype'] == "prod":
            return 'idds'
    return None