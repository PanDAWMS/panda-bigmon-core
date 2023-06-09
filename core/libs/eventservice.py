"""
A set of functions related to handling EventService jobs and tasks
"""
import logging
from django.db import connection
from django.db.models import Count
from django.conf import settings
from core.libs.exlib import dictfetchall, get_tmp_table_name, insert_to_temp_table
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table, get_tmp_table_name_debug

from core.common.models import JediEvents, GetEventsForTask

_logger = logging.getLogger('bigpandamon')

def job_suppression(request):

    extra = '(1=1)'

    if not 'notsuppress' in request.session['requestParams']:
        suppressruntime = 10
        if 'suppressruntime' in request.session['requestParams']:
            try:
                suppressruntime = int(request.session['requestParams']['suppressruntime'])
            except:
                pass
        extra = '( not ((jobdispatchererrorcode=100 or piloterrorcode in (1200,1201,1202,1203,1204,1206,1207))'
        extra += 'and ((endtime-starttime)*24*60 < {} )))'.format(str(suppressruntime))

    return extra


def event_summary_for_task(mode, query, **kwargs):
    """
    Event summary for a task.
    If drop mode, we need a transaction key (tk_dj) to except job retries. If it is not provided we do it here.
    :param mode: str (drop or nodrop)
    :param query: dict
    :return: eventslist: list of dict (number of events in different states)
    """
    tk_dj = -1
    if tk_dj in kwargs:
        tk_dj = kwargs['tk_dj']

    if mode == 'drop' and tk_dj == -1:
        # inserting dropped jobs to tmp table
        extra = '(1=1)'
        extra, tk_dj = insert_dropped_jobs_to_tmp_table(query, extra)
        tmp_table = get_tmp_table_name_debug()

    eventservicestatelist = [
        'ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed', 'fatal', 'merged',
        'corrupted'
    ]
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    _logger.debug('getting events states summary')
    if mode == 'drop':
        jeditaskid = query['jeditaskid']
        # explicit time window for better searching over partitioned JOBSARCHIVED
        time_field = 'modificationtime'
        time_format = "YYYY-MM-DD HH24:MI:SS"
        if 'creationdate__range' in query:
            extra_str = " AND ( {} > TO_DATE('{}', '{}') AND {} < TO_DATE('{}', '{}') )".format(
                time_field, query['creationdate__range'][0], time_format,
                time_field, query['creationdate__range'][1], time_format)
        else:  # if no time range -> look in last 3 months
            extra_str = 'and {} > sysdate - 90'.format(time_field)
        equerystr = """
            select 
            /*+ cardinality(tmp 10) index_rs_asc(ev jedi_events_pk) no_index_ffs(ev jedi_events_pk) no_index_ss(ev jedi_events_pk) */  
                sum(def_max_eventid-def_min_eventid+1) as evcount, 
                ev.status 
            from {1}.jedi_events ev, 
                (select ja4.pandaid from {1}.jobsarchived4 ja4 
                        where ja4.jeditaskid = :tid and ja4.eventservice is not null and ja4.eventservice != 2 
                            and ja4.pandaid not in (select id from {3}.{4} where transactionkey = :tkdj)
                union 
                select ja.pandaid from {2}.jobsarchived ja 
                    where ja.jeditaskid = :tid and ja.eventservice is not null and ja.eventservice != 2 {0} 
                        and ja.pandaid not in (select id from {3}.{4} where transactionkey = :tkdj)
                union
                select jav4.pandaid from {1}.jobsactive4 jav4 
                    where jav4.jeditaskid = :tid and jav4.eventservice is not null and jav4.eventservice != 2 
                        and jav4.pandaid not in (select id from {3}.{4} where transactionkey = :tkdj)
                union
                select jw4.pandaid from {1}.jobswaiting4 jw4 
                    where jw4.jeditaskid = :tid and jw4.eventservice is not null and jw4.eventservice != 2 
                        and jw4.pandaid not in (select id from {3}.{4} where transactionkey = :tkdj)
                union
                select jd4.pandaid from {1}.jobsdefined4 jd4 
                    where jd4.jeditaskid = :tid and jd4.eventservice is not null and jd4.eventservice != 2 
                        and jd4.pandaid not in (select id from {3}.{4} where transactionkey = :tkdj)
                )  j
            where ev.pandaid = j.pandaid and ev.jeditaskid = :tid 
            group by ev.status
        """.format(extra_str, settings.DB_SCHEMA_PANDA, settings.DB_SCHEMA_PANDA_ARCH, settings.DB_SCHEMA, tmp_table)
        new_cur = connection.cursor()
        new_cur.execute(equerystr, {'tid': jeditaskid, 'tkdj': tk_dj})

        evtable = dictfetchall(new_cur)

        for ev in evtable:
            essummary[eventservicestatelist[ev['STATUS']]] += ev['EVCOUNT']
    if mode == 'nodrop':
        event_counts = []
        equery = {'jeditaskid': query['jeditaskid']}
        event_counts.extend(
            JediEvents.objects.filter(**equery).values('status').annotate(count=Count('status')).order_by('status'))
        for state in event_counts:
            essummary[eventservicestatelist[state['status']]] = state['count']

    # creating ordered list of eventssummary
    for state in eventservicestatelist:
        eventstatus = {}
        eventstatus['statusname'] = state
        eventstatus['count'] = essummary[state]
        eventslist.append(eventstatus)

    return eventslist


def add_event_summary_to_tasklist(tasks, transaction_key=None):
    """
    Adding 'eventsData' to tasks
    :param tasks:
    :param transaction_key:
    :return:
    """

    tmp_table_name = get_tmp_table_name()
    if transaction_key is None:
        # insert taskids into tmp table
        taskl = [t['jeditaskid'] for t in tasks]
        transaction_key = insert_to_temp_table(taskl)

    # we get here events data
    event_info_dict = {}
    extra_str = "jeditaskid in (select id from {} where transactionkey={})".format(tmp_table_name, transaction_key)
    task_event_info = GetEventsForTask.objects.extra(where=[extra_str]).values('jeditaskid', 'totevrem', 'totev')

    # We do it because we intermix raw and queryset queries. With next new_cur.execute tasksEventInfo cleares
    for t in task_event_info:
        event_info_dict[t["jeditaskid"]] = {
            "jeditaskid" : t["jeditaskid"],
            "totevrem" : t["totevrem"],
            "totev" : t["totev"]
        }

    # adding event data to tasks
    for task in tasks:
        if task['jeditaskid'] in event_info_dict.keys():
            task['eventsData'] = event_info_dict[task['jeditaskid']]

    return tasks