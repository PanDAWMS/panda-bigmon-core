"""
A set of functions related to handling EventService jobs and tasks
"""
import logging
import re
from django.db import connection
from django.db.models import Count
from django.conf import settings
from core.libs.exlib import dictfetchall, get_tmp_table_name, insert_to_temp_table, get_tmp_table_name_debug
from core.common.models import JediEvents, GetEventsForTask
from core import constants as const

_logger = logging.getLogger('bigpandamon')


def is_event_service(job):
    if 'eventservice' in job and job['eventservice'] is not None:
        if 'specialhandling' in job and job['specialhandling'] and (
                    job['specialhandling'].find('eventservice') >= 0 or job['specialhandling'].find('esmerge') >= 0 or (
                job['eventservice'] != 'ordinary' and job['eventservice'])) and job['specialhandling'].find('sc:') == -1:
                return True
        else:
            return False
    else:
        return False

def add_event_service_info_to_job(job):
    """
    Adding EventService specific info to job
    :param job: dict
    :return: job: dict
    """

    if job['eventservice'] in const.EVENT_SERVICE_JOB_TYPES:
        job['eventservice'] = const.EVENT_SERVICE_JOB_TYPES[job['eventservice']]

    # extract job substatus
    if 'jobmetrics' in job and job['jobmetrics']:
        pat = re.compile('.*mode\\=([^\\s]+).*HPCStatus\\=([A-Za-z0-9]+)')
        mat = pat.match(job['jobmetrics'])
        if mat:
            job['jobmode'] = mat.group(1)
            job['substate'] = mat.group(2)
        pat = re.compile('.*coreCount\\=([0-9]+)')
        mat = pat.match(job['jobmetrics'])
        if mat:
            job['corecount'] = mat.group(1)
    if 'jobsubstatus' in job and job['jobstatus'] == 'closed' and job['jobsubstatus'] == 'toreassign':
        job['jobstatus'] += ':' + job['jobsubstatus']
    return job


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

    eventservicestatelist = list(const.EVENT_STATES.values())
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    _logger.debug('getting events states summary')
    if mode == 'drop' and tk_dj != -1:
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
        """.format(extra_str, settings.DB_SCHEMA_PANDA, settings.DB_SCHEMA_PANDA_ARCH, settings.DB_SCHEMA, get_tmp_table_name_debug())
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


def get_event_status_summary(pandaids):
    """
    Getting event statuses summary for list of pandaids of ES jobs
    :param pandaids: list
    :return: dict of status: nevents
    """
    summary = {}
    tmpTableName = get_tmp_table_name()
    transactionKey = insert_to_temp_table(pandaids)
    new_cur = connection.cursor()
    new_cur.execute(
        """
        select status, count(status) as countstat 
        from (
            select /*+ dynamic_sampling(tmp_ids1 0) cardinality(tmp_ids1 10) index_rs_asc(ev jedi_events_pandaid_status_idx) no_index_ffs(ev jedi_events_pk) no_index_ss(ev jedi_events_pk) */ pandaid, status 
            from {2}.jedi_events ev, {0} 
            where transactionkey = {1} and  pandaid = id
        ) t1 
        group by status""".format(tmpTableName, transactionKey, settings.DB_SCHEMA_PANDA))

    evtable = dictfetchall(new_cur, style='lowercase')

    # translate numerical status to string, if not in const.EVENT_STATES, then it is 'unknown'
    for ev in evtable:
        if ev['status'] in const.EVENT_STATES:
            summary[const.EVENT_STATES[ev['status']]] = ev['countstat']
        else:
            if 'unknown' not in summary:
                summary['unknown'] = 0
            summary['unknown'] += ev['countstat']
    return summary