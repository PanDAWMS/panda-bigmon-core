"""
A set of functions for support of queries threading.
For example getting attributes distribution for objects.
"""

from threading import Lock
from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from django.db.models import Count

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

    def run_query_in_thread(self, aggregation_type):
        print('Thread {} started'.format(self.thread_name))
        lock.acquire()
        try:
            if aggregation_type == 'count_distinct':
                query_result = self.count_distinct()
                self.result[self.thread_name].extend(query_result)
        finally:
            lock.release()

    def count_distinct(self):
        query_result = []
        if not isinstance(self.model_name, list):
            self.model_name = [self.model_name]
            for mn in self.model_name:
                try:
                    query_result = mn.objects\
                        .filter(**self.query)\
                        .extra(where=[self.wild_card_extension])\
                        .values(self.param_name)\
                        .annotate(total=Count(self.param_name))\
                        .order_by('total')
                except:
                    pass
            else:
        print('Thread {} finished'.format(self.thread_name))
        return query_result


