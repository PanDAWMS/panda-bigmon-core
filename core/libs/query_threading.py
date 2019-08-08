"""
A set of functions for support of queries threading.
For example getting attributes distribution for objects.
"""

import logging
from threading import Lock
from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from django.db.models import Count

_logger_info = logging.getLogger('bigpandamon')
_logger_error = logging.getLogger('bigpandamon-error')
result = {}
lock = Lock()


class QueryThread:
    """A class for query to run in threads """
    def __init__(self, model_name, query, wild_card_extension, tkey, param_name):
        self.model_name = model_name
        self.query = query
        self.tkey = tkey
        self.wild_card_extension = wild_card_extension
        self.param_name = param_name
        self.thread_name = str(tkey) + '_' + param_name
        self.result = {}
        self.result.setdefault(self.thread_name, [])

    def run_query(self, aggregation_type):
        # lock.acquire()
        try:
            if aggregation_type == 'count_distinct':
                query_result = self.count_distinct()
                self.result[self.thread_name].extend(query_result)
        finally:
            pass
            # lock.release()

    def count_distinct(self):
        _logger_info.info('Thread for param {} started'.format(self.param_name))
        print('Thread for param {} started'.format(self.param_name))
        query_result = []
        if not isinstance(self.model_name, list):
            self.model_name = [self.model_name]

        for mn in self.model_name:
            try:
                query_result.extend(list(mn.objects\
                    .filter(**self.query)\
                    .extra(where=[self.wild_card_extension])\
                    .values(self.param_name)\
                    .annotate(total=Count(self.param_name))\
                    .order_by('total')))
            except:
                _logger_error.error('[QueryThread][count_distinct] Failed query to {} for param {}'.format(mn, self.param_name))
                pass
        _logger_info.info('Thread for param {} finished'.format(self.param_name))
        print('Thread for param {} finished'.format(self.param_name))
        return query_result


def run_query_in_thread(input):
    input['QueryThread_instances_dict'][input['param_name']].run_query(aggregation_type=input['aggregation_type'])
    return 0


def extract_results_list(raw_joined_result, tkey):
    """
    Extracting results from QueryThread class instances
    :param raw_joined_result: list of QueryThread class instances containing queries results
    :param tkey: hex key
    :return: list of results in format needed for template
    """
    results_list = []
    for k, v in raw_joined_result.items():
        # summing of attributes counts delivered from different tables (jobs case)
        attr_count_dict = {}
        for attr in v.result[tkey + '_' + k]:
            if attr[k] not in attr_count_dict:
                attr_count_dict[attr[k]] = 0
            attr_count_dict[attr[k]] += attr['total']
        # transform dict to list
        attr_count_list = [(attr_n, attr_c) for attr_n,attr_c in attr_count_dict.items()]
        results_list.append([k, attr_count_list])
    return results_list
