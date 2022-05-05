"""
A set of functions related to handling EventService jobs and tasks
"""
from django.db import connection
from django.db.models import Count

from core.libs.exlib import dictfetchall
from core.libs.dropalgorithm import insert_dropped_jobs_to_tmp_table

from core.common.models import JediEvents


def job_suppression(request):

    extra = '(1=1)'

    if not 'notsuppress' in request.session['requestParams']:
        suppressruntime = 10
        if 'suppressruntime' in request.session['requestParams']:
            try:
                suppressruntime = int(request.session['requestParams']['suppressruntime'])
            except:
                pass
        extra = '( not ((JOBDISPATCHERERRORCODE=100 OR PILOTERRORCODE in (1200,1201,1202,1203,1204,1206,1207))'
        extra += 'and ((ENDTIME-STARTTIME)*24*60 < {} )))'.format(str(suppressruntime))

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

    eventservicestatelist = [
        'ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed', 'fatal', 'merged',
        'corrupted'
    ]
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    print ('getting events states summary')
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
            extra_str = 'AND {} > SYSDATE - 90'.format(time_field)
        equerystr = """
            SELECT 
            /*+ cardinality(tmp 10) INDEX_RS_ASC(ev JEDI_EVENTS_PK) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  
                SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS EVCOUNT, 
                ev.STATUS 
            FROM ATLAS_PANDA.JEDI_EVENTS ev, 
                (select ja4.pandaid from ATLAS_PANDA.JOBSARCHIVED4 ja4 
                        where ja4.jeditaskid = :tid and ja4.eventservice is not NULL and ja4.eventservice != 2 
                            and ja4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union 
                select ja.pandaid from ATLAS_PANDAARCH.JOBSARCHIVED ja 
                    where ja.jeditaskid = :tid and ja.eventservice is not NULL and ja.eventservice != 2 {} 
                        and ja.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jav4.pandaid from ATLAS_PANDA.jobsactive4 jav4 
                    where jav4.jeditaskid = :tid and jav4.eventservice is not NULL and jav4.eventservice != 2 
                        and jav4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jw4.pandaid from ATLAS_PANDA.jobswaiting4 jw4 
                    where jw4.jeditaskid = :tid and jw4.eventservice is not NULL and jw4.eventservice != 2 
                        and jw4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                union
                select jd4.pandaid from ATLAS_PANDA.jobsdefined4 jd4 
                    where jd4.jeditaskid = :tid and jd4.eventservice is not NULL and jd4.eventservice != 2 
                        and jd4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY = :tkdj)
                )  j
            WHERE ev.PANDAID = j.pandaid AND ev.jeditaskid = :tid 
            GROUP BY ev.STATUS
        """.format(extra_str)
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