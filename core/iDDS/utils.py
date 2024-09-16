"""
:author Tatiana Korchuganova
"""
import logging

from core.libs.exlib import insert_to_temp_table, get_tmp_table_name
from django.conf import settings
from core.iDDS.models import Transforms
from core.iDDS.useconstants import SubstitleValue

subtitleValue = SubstitleValue()
_logger = logging.getLogger('bigpandamon')


def extend_view_idds(request, query, extra_str):
    """
    Adding iDDS related parameters to query clause
    :param request: request object
    :param query: dict
    :param extra_str: extra_str
    :return: query: updated dict
    :return: extra_str: updated extra_str
    """
    idds_query = {}
    jeditaskid_list = []
    if 'idds_request_id' in request.session['requestParams'] and request.session['requestParams']['idds_request_id']:
        if ',' in request.session['requestParams']['idds_request_id']:
            idds_query['request_id__in'] = request.session['requestParams']['idds_request_id'].split(',')
        else:
            try:
                iddsreqid = int(request.session['requestParams']['idds_request_id'])
            except ValueError:
                _logger.exception("Invalid iddsreqid value! It must be integer!")
                raise
            idds_query['request_id'] = iddsreqid
    if 'idds_transform_status' in request.session['requestParams'] and request.session['requestParams']['idds_transform_status']:
        # translate human friendly status names into indexes
        idds_transform_status_desc = subtitleValue.substitleValue('transforms', 'status')
        requested_status = [
            s.lower() for s in request.session['requestParams']['idds_transform_status'].split(',') if len(s) > 0]
        translated_status = [k for k, v in idds_transform_status_desc.items() if v.lower() in requested_status]
        if len(translated_status) > 0:
            idds_query['status__in'] =  translated_status

    # getting workload_id (it is jeditaskid in PanDA)
    if len(idds_query) > 0:
        jeditaskid_list.extend(Transforms.objects.filter(**idds_query).values('workload_id'))
        jeditaskid_list = [row['workload_id'] for row in jeditaskid_list]
        # we add the list of jeditaskid to query directly or put into tmp table
        if len(jeditaskid_list) > settings.DB_N_MAX_IN_QUERY:
            tmp_table_name = get_tmp_table_name()
            transaction_key = insert_to_temp_table(jeditaskid_list)
            extra_str += ' and jeditaskid in (select id from {} where transactionkey = {})'.format(
                tmp_table_name,
                transaction_key
            )
        else:
            query['jeditaskid__in'] = jeditaskid_list


    return query, extra_str


def add_idds_info_to_tasks(tasks):
    """
    Add iDDS related information to tasks
    :param tasks: list of tasks
    :return: list of tasks with iDDS information
    """
    task_ids = [task['jeditaskid'] for task in tasks]

    query = {}
    extra_str = '(1=1)'
    if len(task_ids) > settings.DB_N_MAX_IN_QUERY:
        transaction_key = insert_to_temp_table(task_ids)
        extra_str += f' and workload_id in (select id from {get_tmp_table_name()} where transactionkey = {transaction_key})'
    else:
        query['workload_id__in'] = task_ids

    idds_info = Transforms.objects.filter(**query).extra(where=[extra_str]).values('workload_id', 'request_id')
    if len(idds_info) > 0:
        idds_info_dict = {row['workload_id']: row['request_id'] for row in idds_info}
        for task in tasks:
            if task['jeditaskid'] in idds_info_dict:
                task['idds_request_id'] = idds_info_dict.get(task['jeditaskid'])
            else:
                task['idds_request_id'] = None

    return tasks