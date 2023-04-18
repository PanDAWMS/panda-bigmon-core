
from datetime import datetime, timedelta

from django.db import connection
from django.db.models import Q, Count, Sum

from core.harvester.models import HarvesterWorkerStats, HarvesterWorkers, HarvesterRelJobsWorkers, HarvesterDialogs
from core.pandajob.utils import identify_jobtype

from django.conf import settings


def setup_harvester_view(request, otype='worker'):
    """
    A function to process URL parameters into Django ORM compatible query dict
    :param request: request
    :param otype: object type
    :return:
    """
    query = {}
    extra = '(1=1)'

    internal_extra = ''

    DEFAULT_HOURS = 12
    startdate = None
    enddate = datetime.utcnow()

    if 'hours' in request.session['requestParams']:
        startdate = enddate - timedelta(hours=int(request.session['requestParams']['hours']))
    elif 'days' in request.session['requestParams']:
        startdate = enddate - timedelta(days=int(request.session['requestParams']['days']))

    if not startdate:
        startdate = enddate - timedelta(hours=DEFAULT_HOURS)

    # add timelimit to query
    if otype == 'worker':
        query['submittime__range'] = [
            startdate.strftime(settings.DATETIME_FORMAT),
            enddate.strftime(settings.DATETIME_FORMAT)]
    elif otype == 'jobs':
        if len(internal_extra) > 0:
            internal_extra += ' and '
        internal_extra += "{0} > to_date('{1}','{3}') and {0} < to_date('{2}','{3}')".format(
            'submittime',
            startdate.strftime(settings.DATETIME_FORMAT),
            enddate.strftime(settings.DATETIME_FORMAT),
            'yyyy-mm-dd hh24:mi:ss'
        )

    if 'instance' in request.session['requestParams'] or 'harvesterid' in request.session['requestParams']:
        if 'instance' in request.session['requestParams']:
            harvesterid = request.session['requestParams']['instance']
        else:
            harvesterid = request.session['requestParams']['harvesterid']
        query['harvesterid'] = harvesterid
        if otype == 'jobs':
            # to use index on workers table
            if len(internal_extra) > 0:
                internal_extra += ' and '
            internal_extra += "harvesterid = '{}' ".format(harvesterid)

    for rparam, rvalue in list(request.session['requestParams'].items()):
        if otype == 'worker':
            for field in HarvesterWorkers._meta.get_fields():
                param = field.name
                if rparam == param:
                    query[param] = rvalue
            if rparam == 'workerids':
                query['workerid__in'] = rvalue.split(',')
            if rparam == 'pandaid':
                extra += """ and workerid in (
                    select workerid from {}.harvester_rel_jobs_workers where pandaid in ({})
                )""".format(settings.DB_SCHEMA_PANDA, str(rvalue))

        elif otype == 'dialog':
            for field in HarvesterDialogs._meta.get_fields():
                param = field.name
                if rparam == param:
                    query[param] = rvalue
                elif param == 'creationtime':
                    query['creationtime__range'] = [
                        startdate.strftime(settings.DATETIME_FORMAT),
                        enddate.strftime(settings.DATETIME_FORMAT)]
        elif otype == 'workerstat':
            if rparam in ('status',):
                # status of worker != aggregated status for statistics -> exclude from query
                continue
            for field in HarvesterWorkerStats._meta.get_fields():
                param = field.name
                if rparam == param:
                    query[param] = rvalue
                elif param == 'lastupdate':
                    query['lastupdate__range'] = [
                        startdate.strftime(settings.DATETIME_FORMAT),
                        enddate.strftime(settings.DATETIME_FORMAT)]

        elif otype == 'jobs':
            if rparam in ('status', 'computingsite', 'computingelement', 'resourcetype'):
                if len(internal_extra) > 0:
                    internal_extra += ' and '
                internal_extra += rparam + '= \'' + rvalue + '\''
                continue
            if rparam == 'pandaid':
                pandaids = request.session['requestParams']['pandaid'].split(',')
                if len(pandaids) > 0:
                    query['pandaid__in'] = pandaids
                continue
            for field in HarvesterRelJobsWorkers._meta.get_fields():
                param = field.name
                if rparam == param:
                    query[param] = rvalue
                elif param == 'lastupdate':
                    query['lastupdate__range'] = [
                        startdate.strftime(settings.DATETIME_FORMAT),
                        enddate.strftime(settings.DATETIME_FORMAT)]

    if len(internal_extra) > 0:
        if otype == 'jobs':
            extra += ' and ( workerid in ( select workerid from {}.harvester_workers where {} ) )'.format(
                settings.DB_SCHEMA_PANDA,
                internal_extra
            )

    # if 'pandaid' in request.session['requestParams']:
    #     pandaid = request.session['requestParams']['pandaid']
    #     try:
    #         jobsworkersquery, pandaids = getWorkersByJobID(pandaid, request.session['requestParams']['instance'])
    #     except:
    #         message = """PandaID for this instance is not found """
    #         return HttpResponse(json.dumps({'message': message}),
    #                             content_type='text/html')
    #     extra += """ AND workerid in (%s)""" % (jobsworkersquery)
    #     URL += '&pandaid=' + str(request.session['requestParams']['pandaid'])

    return query, extra


def isHarvesterJob(pandaid):

    jobHarvesterInfo = []

    sqlQuery = f"""
    SELECT workerid, HARVESTERID, BATCHLOG, COMPUTINGELEMENT, ERRORCODE, DIAGMESSAGE  FROM (SELECT 
      a.PANDAID,
      a.workerid,
      a.HARVESTERID,
      b.BATCHLOG,
      b.COMPUTINGELEMENT,
      b.ERRORCODE,
      b.DIAGMESSAGE
      FROM {settings.DB_SCHEMA_PANDA}.HARVESTER_REL_JOBS_WORKERS a,
      {settings.DB_SCHEMA_PANDA}.HARVESTER_WORKERS b
      WHERE a.harvesterid = b.harvesterid and a.workerid = b.WORKERID) tmp_sub where pandaid = {pandaid}
  """
    cur = connection.cursor()
    cur.execute(sqlQuery)
    job = cur.fetchall()

    if len(job) == 0:
        return False

    columns = [str(column[0]).lower() for column in cur.description]

    for pid in job:
        jobHarvesterInfo.append(dict(zip(columns, pid)))

    return jobHarvesterInfo


def get_harverster_workers_for_task(jeditaskid):
    """
    :param jeditaskid: int
    :return: harv_workers_list: list
    """
    jsquery = """
    select t4.*, t5.batchid, 
        case when not t5.endtime is null then t5.endtime-t5.starttime
             when not t5.starttime is null then (cast(sys_extract_utc(systimestamp)as date)-t5.starttime)
             else 0
        end as walltime,
        t5.ncore, t5.njobs 
        from (
            select harvesterid, workerid, sum(nevents) as sumevents 
            from (
                select harvesterid, workerid, t1.pandaid, t2.nevents 
                from {0}.harvester_rel_jobs_workers t1 
                join (
                    select pandaid, nevents 
                    from (
                        (
                            select pandaid, nevents 
                            from {0}.combined_wait_act_def_arch4 
                            where eventservice in (1,3,4,5) and jeditaskid = :jtid
                        )
                        union all
                        (
                            select pandaid, nevents 
                            from {2}.jobsarchived 
                            where eventservice in (1,3,4,5) and jeditaskid = :jtid
                            minus
                            select pandaid, nevents 
                            from {2}.jobsarchived 
                            where eventservice in (1,3,4,5) and jeditaskid = :jtid and pandaid in (
                                select pandaid 
                                from {1}.combined_wait_act_def_arch4 
                                where eventservice in (1,3,4,5) and jeditaskid = :jtid
                            )
                        )
                )
            ) t2 on t1.pandaid=t2.pandaid
        ) t3 
    group by harvesterid, workerid
    ) t4
    join {0}.harvester_workers t5 on t5.harvesterid=t4.harvesterid and t5.workerid = t4.workerid
    """.format(settings.DB_SCHEMA_PANDA, settings.DB_SCHEMA, settings.DB_SCHEMA_PANDA_ARCH)

    cur = connection.cursor()
    cur.execute(jsquery, {'jtid': jeditaskid})
    harv_workers = cur.fetchall()
    cur.close()

    harv_workers_names = ['harvesterid', 'workerid', 'sumevents', 'batchid', 'walltime', 'ncore', 'njobs']
    harv_workers_list = [dict(zip(harv_workers_names, row)) for row in harv_workers]
    return harv_workers_list


def get_workers_summary_split(query, **kwargs):
    """Get statistics of submitted and running Harvester workers"""
    N_HOURS = 100
    wquery = {}
    if 'computingsite__in' in query:
        wquery['computingsite__in'] = query['computingsite__in']
    if 'resourcetype' in query:
        wquery['resourcetype'] = query['resourcetype']

    if 'source' in kwargs and kwargs['source'] == 'HarvesterWorkers':
        wquery['submittime__castdate__range'] = [
            (datetime.utcnow()-timedelta(hours=N_HOURS)).strftime(settings.DATETIME_FORMAT),
            datetime.utcnow().strftime(settings.DATETIME_FORMAT)
        ]
        wquery['status__in'] = ['running', 'submitted']
        # wquery['jobtype__in'] = ['managed', 'user', 'panda']
        w_running = Count('jobtype', filter=Q(status__exact='running'))
        w_submitted = Count('jobtype', filter=Q(status__exact='submitted'))
        w_values = ['computingsite', 'resourcetype', 'jobtype']
        worker_summary = HarvesterWorkers.objects.filter(**wquery).values(*w_values).annotate(nwrunning=w_running).annotate(nwsubmitted=w_submitted)
    else:
        wquery['jobtype__in'] = ['managed', 'user', 'panda']
        if settings.DEPLOYMENT == 'ORACLE_DOMA':
            wquery['jobtype__in'].append('ANY')
        w_running = Sum('nworkers', filter=Q(status__exact='running'))
        w_submitted = Sum('nworkers', filter=Q(status__exact='submitted'))
        w_values = ['computingsite', 'resourcetype', 'jobtype']
        worker_summary = HarvesterWorkerStats.objects.filter(**wquery).values(*w_values).annotate(nwrunning=w_running).annotate(nwsubmitted=w_submitted)

    # Translate prodsourcelabel values to descriptive analy|prod job types
    worker_summary = identify_jobtype(worker_summary, field_name='jobtype')
    return list(worker_summary)