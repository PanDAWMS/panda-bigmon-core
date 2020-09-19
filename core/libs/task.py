
import logging
import time
import copy
import random
import json
import numpy as np
from datetime import datetime, timedelta
from django.db import connection
from django.db.models import Count, Sum
from core.common.models import JediEvents, JediDatasetContents, JediDatasets, JediTaskparams
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4
from core.libs.exlib import dictfetchall, insert_to_temp_table, drop_duplicates, add_job_category, get_job_walltime, job_states_count_by_param

import core.constants as const
from core.settings.local import defaultDatetimeFormat

_logger = logging.getLogger('bigpandamon')


def job_summary_for_task(query, extra="(1=1)", **kwargs):
    """An attempt to rewrite it moving dropping to db request level"""

    mode = 'nodrop'
    if 'mode' in kwargs:
        mode = kwargs['mode']

    jobs = []

    # getting jobs from DB
    newquery = copy.deepcopy(query)
    values = ('actualcorecount', 'eventservice', 'modificationtime', 'jobsubstatus', 'pandaid', 'jobstatus',
              'jeditaskid', 'processingtype', 'maxpss', 'starttime', 'endtime', 'computingsite', 'jobmetrics',
              'nevents', 'hs06', 'hs06sec', 'cpuconsumptiontime', 'transformation')

    start = time.time()
    jobs.extend(Jobsarchived.objects.filter(**newquery).extra(where=[extra]).values(*values))

    jobs.extend(Jobsdefined4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobswaiting4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobsactive4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    jobs.extend(Jobsarchived4.objects.filter(**newquery).extra(where=[extra]).values(*values))
    end = time.time()
    _logger.info("Jobs selection: {} sec".format(end - start))

    start = time.time()
    # drop duplicate jobs
    jobs = drop_duplicates(jobs, id='pandaid')

    # determine jobs category (build, run or merge)
    jobs = add_job_category(jobs)

    # prepare data for job consumption plots
    plots_list = job_consumption_plots(jobs)

    # jobs states aggregation by category

    job_states = job_states_count_by_param(jobs, param='category')

    # find scouts
    scouts = get_task_scouts(jobs)

    end = time.time()
    _logger.info("Preparing job states aggregation and plots: {} sec".format(end - start))

    return plots_list, job_states, scouts


def get_task_scouts(jobs):
    """
    Get PanDAIDs of selected scouting metrics for a task
    :param jobs: list of dicts
    :return: dict:
    """
    scouts_dict = {}
    scout_types = ['cpuTime', 'walltime', 'ramCount', 'ioIntensity', 'outDiskCount']
    for jst in scout_types:
        scouts_dict[jst] = []

    for job in jobs:
        for jst in scout_types:
            if 'jobmetrics' in job and 'scout=' in job['jobmetrics'] and jst in job['jobmetrics'][job['jobmetrics'].index('scout='):]:
                scouts_dict[jst].append(job['pandaid'])

    return scouts_dict


def job_consumption_plots(jobs):

    plots_dict = {}
    plot_details = {
        'nevents_sum_finished': {'type': 'pie', 'title': 'Number of events', 'xlabel': 'N events'},
        'nevents_finished': {'type': 'stack_bar', 'title': 'Number of events', 'xlabel': 'N events'},
        'maxpss_finished': {'type': 'stack_bar', 'title': 'Max PSS (finished jobs)', 'xlabel': 'MaxPSS, KB'},
        'maxpsspercore_finished': {'type': 'stack_bar', 'title': 'Max PSS/core (finished jobs)', 'xlabel': 'MaxPSS per core, KB'},
        'walltime_finished': {'type': 'stack_bar', 'title': 'Walltime (finished jobs)', 'xlabel': 'Walltime, s'},
        'walltimeperevent_finished': {'type': 'stack_bar', 'title': 'Walltime/event (finished jobs)', 'xlabel': 'Walltime per event, s'},
        'hs06s_finished': {'type': 'stack_bar', 'title': 'HS06s (finished jobs)', 'xlabel': 'HS06s'},
        'cputime_finished': {'type': 'stack_bar', 'title': 'CPU time (finished jobs)', 'xlabel': 'CPU time, s'},
        'cputimeperevent_finished': {'type': 'stack_bar', 'title': 'CPU time/event (finished jobs)', 'xlabel': 'CPU time, s'},
        'maxpss_failed': {'type': 'stack_bar', 'title': 'Maximum PSS (failed jobs)', 'xlabel': 'MaxPSS, kB'},
        'maxpsspercore_failed': {'type': 'stack_bar', 'title': 'Max PSS/core (failed jobs)', 'xlabel': 'MaxPSS per core, KB'},
        'walltime_failed': {'type': 'stack_bar', 'title': 'Walltime (failed jobs)', 'xlabel': 'walltime, s'},
        'walltimeperevent_failed': {'type': 'stack_bar', 'title': 'Walltime/event (failed jobs)', 'xlabel': 'Walltime per event, s'},
        'hs06s_failed': {'type': 'stack_bar', 'title': 'HS06s (failed jobs)', 'xlabel': 'HS06s'},
        'cputime_failed': {'type': 'stack_bar', 'title': 'CPU time (failed jobs)', 'xlabel': 'CPU time, s'},
        'cputimeperevent_failed': {'type': 'stack_bar', 'title': 'CPU time/event (failed jobs)', 'xlabel': 'CPU time, s'},
    }

    plots_data = {}
    for pname, pd in plot_details.items():
        if pd['type'] not in plots_data:
            plots_data[pd['type']] = {}
        plots_data[pd['type']][pname] = {
            'build': {},
            'run': {},
            'merge': {}
        }

    MULTIPLIERS = {
        "SEC": 1.0,
        "MIN": 60.0,
        "HOUR": 60.0 * 60.0,
        "MB": 1024.0,
        "GB": 1024.0 * 1024.0,
    }

    # prepare data for plots
    for job in jobs:
        if job['actualcorecount'] is None:
            job['actualcorecount'] = 1
        if 'duration' not in job:
            job['duration'] = get_job_walltime(job)

        if job['jobstatus'] in ('finished', 'failed'):
            for pname, pd in plot_details.items():
                if job['computingsite'] not in plots_data[pd['type']][pname][job['category']]:
                    plots_data[pd['type']][pname][job['category']][job['computingsite']] = []
        else:
            continue

        if 'nevents' in job and job['nevents'] >= 0 and job['jobstatus'] == 'finished':
            plots_data['stack_bar']['nevents' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['nevents'])

            plots_data['pie']['nevents_sum_finished'][job['category']][job['computingsite']].append(job['nevents'])

        if 'maxpss' in job and job['maxpss'] is not None and job['maxpss'] >= 0:
            plots_data['stack_bar']['maxpss' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['maxpss'] / MULTIPLIERS['MB']
            )
            if job['actualcorecount'] and job['actualcorecount'] > 0:
                plots_data['stack_bar']['maxpsspercore' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['maxpss'] / MULTIPLIERS['MB'] / job['actualcorecount']
                )

        if 'hs06sec' in job and job['hs06sec']:
            plots_data['stack_bar']['hs06s' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['hs06sec'])

        if 'duration' in job and job['duration']:
            plots_data['stack_bar']['walltime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(job['duration'])
            if 'walltimeperevent' in job:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['walltimeperevent']
                )
            elif 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['walltimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['duration'] / (job['nevents'] * 1.0)
                )

        if 'cpuconsumptiontime' in job and job['cpuconsumptiontime'] is not None and job['cpuconsumptiontime'] > 0:
            plots_data['stack_bar']['cputime' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                job['cpuconsumptiontime']
            )
            if 'nevents' in job and job['nevents'] is not None and job['nevents'] > 0:
                plots_data['stack_bar']['cputimeperevent' + '_' + job['jobstatus']][job['category']][job['computingsite']].append(
                    job['cpuconsumptiontime'] / (job['nevents'] * 1.0)
                )

    # remove empty categories
    cat_to_remove = {'build': True, 'run': True, 'merge': True}
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, cd in pd.items():
                if len(cd) > 0:
                    cat_to_remove[cat] = False
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            for cat, is_remove in cat_to_remove.items():
                if is_remove:
                    del pd[cat]

    # add 'all' category to histograms
    for pt, td in plots_data.items():
        for pm, pd in td.items():
            all_cat = {}
            for cat, cd in pd.items():
                for site, sd in cd.items():
                    if site not in all_cat:
                        all_cat[site] = []
                    all_cat[site].extend(sd)
            pd['all'] = all_cat

    # remove empty plots
    plots_to_remove = []
    for pm, pd in plots_data['stack_bar'].items():
        if sum([len(site_data) for site, site_data in pd['all'].items()]) == 0:
            plots_to_remove.append(pm)
    for pm in plots_to_remove:
        del plots_data['stack_bar'][pm]
        del plot_details[pm]

    # prepare stack histogram data
    for pname, pd in plot_details.items():
        if pd['type'] == 'stack_bar':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }

            for cat, cd in plots_data[pd['type']][pname].items():
                stats, columns = build_stack_histogram(cd)
                plots_dict[pname]['data'][cat] = {
                    'columns': columns,
                    'stats': stats,
                }
        elif pd['type'] == 'pie':
            plots_dict[pname] = {
                'details': plot_details[pname],
                'data': {},
            }
            for cat, cd in plots_data[pd['type']][pname].items():

                columns = []
                for site in cd:
                    columns.append([site, sum(cd[site])])

                plots_dict[pname]['data'][cat] = {
                    'columns': columns,
                }

    # transform dict to list
    plots_list = []
    for pname, pdata in plots_dict.items():
        plots_list.append({'name': pname, 'data': pdata})

    return plots_list


def build_stack_histogram(data_raw, **kwargs):
    """
    Prepare stack histogram data and calculate mean and std metrics
    :param data_raw: dict of lists
    :param kwargs:
    :return:
    """

    n_decimals = 0
    if 'n_decimals' in kwargs:
        n_decimals = kwargs['n_decimals']

    N_BINS_MAX = 50
    stats = []
    columns = []

    data_all = []
    for site, sd in data_raw.items():
        data_all.extend(sd)

    stats.append(np.average(data_all) if not np.isnan(np.average(data_all)) else 0)
    stats.append(np.std(data_all) if not np.isnan(np.std(data_all)) else 0)

    bins_all, ranges_all = np.histogram(data_all, bins='auto')
    if len(ranges_all) > N_BINS_MAX + 1:
        bins_all, ranges_all = np.histogram(data_all, bins=N_BINS_MAX)
    ranges_all = list(np.round(ranges_all, n_decimals))

    x_axis_ticks = ['x']
    x_axis_ticks.extend(ranges_all[:-1])
    columns.append(x_axis_ticks)

    for stack_param, data in data_raw.items():
        column = [stack_param]
        column.extend(list(np.histogram(data, ranges_all)[0]))

        columns.append(column)

    return stats, columns


def event_summary_for_task(mode, query, transactionKeyDroppedJobs):
    """

    :param transactionKeyDroppedJobs:
    :return: number of events in different states
    """
    eventservicestatelist = ['ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed',
                             'fatal', 'merged', 'corrupted']
    eventslist = []
    essummary = dict((key, 0) for key in eventservicestatelist)

    print ('getting events states summary')
    if mode == 'drop':
        jeditaskid = query['jeditaskid']
        equerystr = """
        SELECT 
        /*+ cardinality(tmp 10) INDEX_RS_ASC(ev JEDI_EVENTS_PK) NO_INDEX_FFS(ev JEDI_EVENTS_PK) NO_INDEX_SS(ev JEDI_EVENTS_PK) */  
            SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS EVCOUNT, 
            ev.STATUS 
        FROM ATLAS_PANDA.JEDI_EVENTS ev, 
            (select ja4.pandaid from ATLAS_PANDA.JOBSARCHIVED4 ja4 
                    where ja4.jeditaskid = {} and ja4.eventservice is not NULL and ja4.eventservice != 2 
                        and ja4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union 
            select ja.pandaid from ATLAS_PANDAARCH.JOBSARCHIVED ja 
                where ja.jeditaskid = {} and ja.eventservice is not NULL and ja.eventservice != 2 
                    and ja.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jav4.pandaid from ATLAS_PANDA.jobsactive4 jav4 
                where jav4.jeditaskid = {} and jav4.eventservice is not NULL and jav4.eventservice != 2 
                    and jav4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jw4.pandaid from ATLAS_PANDA.jobswaiting4 jw4 
                where jw4.jeditaskid = {} and jw4.eventservice is not NULL and jw4.eventservice != 2 
                    and jw4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            union
            select jd4.pandaid from ATLAS_PANDA.jobsdefined4 jd4 
                where jd4.jeditaskid = {} and jd4.eventservice is not NULL and jd4.eventservice != 2 
                    and jd4.pandaid not in (select id from ATLAS_PANDABIGMON.TMP_IDS1DEBUG where TRANSACTIONKEY={})
            )  j
        WHERE ev.PANDAID = j.pandaid AND ev.jeditaskid = {} 
        GROUP BY ev.STATUS
        """.format(jeditaskid, transactionKeyDroppedJobs, jeditaskid, transactionKeyDroppedJobs,
                   jeditaskid, transactionKeyDroppedJobs, jeditaskid, transactionKeyDroppedJobs,
                   jeditaskid, transactionKeyDroppedJobs, jeditaskid)
        new_cur = connection.cursor()
        new_cur.execute(equerystr)

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


def datasets_for_task(jeditaskid):
    """
    Getting list of datasets corresponding to a task and file state summary
    :param jeditaskid: int
    :return: dsets: list of dicts
    :return: dsinfo: dict
    """
    dsets = []
    dsinfo = {}

    dsquery = {
        'jeditaskid': jeditaskid,
    }
    values = (
        'jeditaskid', 'datasetid', 'datasetname', 'containername', 'type', 'masterid', 'streamname', 'status',
        'storagetoken', 'nevents', 'neventsused', 'neventstobeused', 'nfiles', 'nfilesfinished', 'nfilesfailed'
    )
    dsets.extend(JediDatasets.objects.filter(**dsquery).values(*values))

    nfiles = 0
    nfinished = 0
    nfailed = 0
    neventsTot = 0
    neventsUsedTot = 0
    scope = ''
    newdslist = []

    if len(dsets) > 0:
        for ds in dsets:
            if len(ds['datasetname']) > 0:
                if not str(ds['datasetname']).startswith('user'):
                    scope = str(ds['datasetname']).split('.')[0]
                else:
                    scope = '.'.join(str(ds['datasetname']).split('.')[:2])
                if ':' in scope:
                    scope = str(scope).split(':')[0]
                ds['scope'] = scope
            newdslist.append(ds)
            if ds['type'] not in ['input', 'pseudo_input']:
                continue
            if ds['masterid']:
                continue
            if not ds['nevents'] is None and int(ds['nevents']) > 0:
                neventsTot += int(ds['nevents'])
            if not ds['neventsused'] is None and int(ds['neventsused']) > 0:
                neventsUsedTot += int(ds['neventsused'])

            if int(ds['nfiles']) > 0:
                ds['percentfinished'] = int(100. * int(ds['nfilesfinished']) / int(ds['nfiles']))
                nfiles += int(ds['nfiles'])
                nfinished += int(ds['nfilesfinished'])
                nfailed += int(ds['nfilesfailed'])

        dsets = newdslist
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())

    dsinfo['nfiles'] = nfiles
    dsinfo['nfilesfinished'] = nfinished
    dsinfo['nfilesfailed'] = nfailed
    dsinfo['pctfinished'] = int(100. * nfinished / nfiles) if nfiles > 0 else 0
    dsinfo['pctfailed'] = int(100. * nfailed / nfiles) if nfiles > 0 else 0

    dsinfo['neventsTot'] = neventsTot
    dsinfo['neventsUsedTot'] = neventsUsedTot

    return dsets, dsinfo


def input_summary_for_task(taskrec, dsets):
    """
    The function returns:
    Input event chunks list for separate table
    Input event chunks summary by states
    A dictionary with tk as key and list of input files IDs that is needed for jobList view filter
    """
    jeditaskid = taskrec['jeditaskid']
    # Getting statuses of inputfiles
    if taskrec['creationdate'] < datetime.strptime('2018-10-22 10:00:00', defaultDatetimeFormat):
        ifsquery = """
            select  
            ifs.jeditaskid,
            ifs.datasetid,
            ifs.fileid,
            ifs.lfn, 
            ifs.startevent, 
            ifs.endevent, 
            ifs.attemptnr, 
            ifs.maxattempt, 
            ifs.failedattempt, 
            ifs.maxfailure,
            case when cstatus not in ('running') then cstatus 
                 when cstatus in ('running') and esmergestatus is null then cstatus
                 when cstatus in ('running') and esmergestatus = 'esmerge_transferring' then 'transferring' 
                 when cstatus in ('running') and esmergestatus = 'esmerge_merging' then 'merging' 
            end as status
            from (
                select jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus, count(f.esmergestatus) as n
                  from
                (select jd.jeditaskid, jd.datasetid, jdc.fileid, 
                    jdc.lfn, jdc.startevent, jdc.endevent, 
                    jdc.attemptnr, jdc.maxattempt, jdc.failedattempt, jdc.maxfailure,
                    case when (jdc.maxattempt <= jdc.attemptnr or jdc.failedattempt >= jdc.maxfailure) and jdc.status = 'ready' then 'failed' else jdc.status end as cstatus
                 from atlas_panda.jedi_dataset_contents jdc, 
                     atlas_panda.jedi_datasets jd
                 where jd.datasetid = jdc.datasetid 
                    and jd.jeditaskid = {}
                    and jd.masterid is NULL
                    and jdc.type in ( 'input', 'pseudo_input')
                ) jdcf 
                LEFT JOIN
                (select f4.jeditaskid, f4.fileid, f4.datasetid, f4.pandaid, 
                    case when ja4.jobstatus = 'transferring' and ja4.eventservice = 2 then 'esmerge_transferring' when ja4.eventservice = 2 then 'esmerge_merging' else null end as esmergestatus
                 from atlas_panda.filestable4 f4, ATLAS_PANDA.jobsactive4 ja4 
                 where f4.pandaid = ja4.pandaid and f4.type in ( 'input', 'pseudo_input') 
                            and f4.jeditaskid = {}
                ) f
                on jdcf.datasetid = f.datasetid and jdcf.fileid = f.fileid
                group by jdcf.jeditaskid, jdcf.datasetid, jdcf.fileid, jdcf.lfn, jdcf.startevent, jdcf.endevent, 
                    jdcf.attemptnr, jdcf.maxattempt, jdcf.failedattempt, jdcf.maxfailure, jdcf.cstatus, f.esmergestatus
            ) ifs """.format(jeditaskid, jeditaskid)

        cur = connection.cursor()
        cur.execute(ifsquery)
        inputfiles = cur.fetchall()
        cur.close()

        inputfiles_names = ['jeditaskid', 'datasetid', 'fileid', 'lfn', 'startevent', 'endevent', 'attemptnr',
                            'maxattempt', 'failedattempt', 'maxfailure', 'procstatus']
        inputfiles_list = [dict(zip(inputfiles_names, row)) for row in inputfiles]

    else:
        ifsquery = {}
        ifsquery['jeditaskid'] = jeditaskid
        indsids = [ds['datasetid'] for ds in dsets if ds['type'] == 'input' and ds['masterid'] is None]
        ifsquery['datasetid__in'] = indsids if len(indsids) > 0 else [-1,]
        inputfiles_list = []
        inputfiles_list.extend(JediDatasetContents.objects.filter(**ifsquery).values())

    # counting of files in different states and building list of fileids for jobList
    inputfiles_counts = {}
    inputfilesids_states = {}
    dsids = []
    for inputfile in inputfiles_list:
        if inputfile['procstatus'] not in inputfiles_counts:
            inputfiles_counts[inputfile['procstatus']] = 0
        if inputfile['procstatus'] not in inputfilesids_states:
            inputfilesids_states[inputfile['procstatus']] = []
        if inputfile['datasetid'] not in dsids:
            dsids.append(inputfile['datasetid'])
        inputfiles_counts[inputfile['procstatus']] += 1
        inputfilesids_states[inputfile['procstatus']].append(inputfile['fileid'])

    inputfiles_tk = {}
    ifs_states = ['ready', 'queued', 'running', 'merging', 'transferring', 'finished', 'failed']
    ifs_summary = []
    for ifstate in ifs_states:
        ifstatecount = 0
        tk = random.randrange(100000000)
        if ifstate in inputfiles_counts.keys():
            ifstatecount = inputfiles_counts[ifstate]
            inputfiles_tk[tk] = inputfilesids_states[ifstate]
        ifs_summary.append({'name': ifstate, 'count': ifstatecount, 'tk': tk, 'ds': dsids})

    return inputfiles_list, ifs_summary, inputfiles_tk


def job_summary_for_task_light(taskrec):
    """
    Light version of jobSummary for ES tasks specifically. Nodrop mode by default. See ATLASPANDA-466 for details.
    :param taskrec:
    :return:
    """
    jeditaskidstr = str(taskrec['jeditaskid'])
    statelistlight = ['defined', 'assigned', 'activated', 'starting', 'running', 'holding', 'transferring', 'finished',
                      'failed', 'cancelled']
    estypes = ['es', 'esmerge', 'jumbo']
    jobSummaryLight = {}
    jobSummaryLightSplitted = {}
    for state in statelistlight:
        jobSummaryLight[str(state)] = 0

    for estype in estypes:
        jobSummaryLightSplitted[estype] = {}
        for state in statelistlight:
            jobSummaryLightSplitted[estype][str(state)] = 0

    jsquery = """
        select jobstatus, case eventservice when 1 then 'es' when 5 then 'es' when 2 then 'esmerge' when 4 then 'jumbo' else 'unknown' end, count(pandaid) as njobs from (
        (
        select pandaid, es as eventservice, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
        )
        union all
        (
        select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid
        minus
        select pandaid, eventservice, jobstatus from atlas_pandaarch.jobsarchived where jeditaskid = :jtid and pandaid in (
            select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 where jeditaskid = :jtid
            )
        )
        )
        group by jobstatus, eventservice
    """
    cur = connection.cursor()
    cur.execute(jsquery, {'jtid': jeditaskidstr})
    js_count = cur.fetchall()
    cur.close()

    js_count_names = ['state', 'es', 'count']
    js_count_list = [dict(zip(js_count_names, row)) for row in js_count]

    for row in js_count_list:
        if row['state'] in statelistlight:
            if not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLight[row['state']] += row['count']
            if row['es'] in estypes and not (row['state'] == 'cancelled' and row['es'] in ('es', 'esmerge')):
                jobSummaryLightSplitted[row['es']][row['state']] += row['count']

    # dict -> list for template
    jobsummarylight = [dict(name=state, count=jobSummaryLight[state]) for state in statelistlight]
    jobsummarylightsplitted = {}
    for estype, count_dict in jobSummaryLightSplitted.items():
        jobsummarylightsplitted[estype] = [dict(name=state, count=count_dict[state]) for state in statelistlight]

    return jobsummarylight, jobsummarylightsplitted


def get_top_memory_consumers(taskrec):

    jeditaskidstr = str(taskrec['jeditaskid'])
    topmemoryconsumedjobs = []
    tmcquerystr = """
    select jeditaskid, pandaid, computingsite, jobmaxpss, jobmaxpss_percore, sitemaxrss, sitemaxrss_percore, maxpssratio 
    from (
        select j.jeditaskid, j.pandaid, j.computingsite, j.jobmaxpss, j.jobmaxpss_percore, s.maxrss as sitemaxrss, 
            s.maxrss/s.corecount as sitemaxrss_percore, j.jobmaxpss_percore/(s.maxrss/s.corecount) as maxpssratio, 
            row_number() over (partition by jeditaskid order by j.jobmaxpss_percore/(s.maxrss/s.corecount) desc) as jobrank
        from atlas_pandameta.schedconfig s,
        (select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDA.jobsarchived4 
            where jeditaskid = :jdtsid and maxrss is not null
        union
        select pandaid, jeditaskid, computingsite, maxpss/1000 as jobmaxpss, maxpss/1000/actualcorecount as jobmaxpss_percore 
        from ATLAS_PANDAARCH.jobsarchived 
            where jeditaskid = :jdtsid  and maxrss is not null
        ) j
        where j.computingsite = s.nickname
    ) 
    where jobrank <= 3
    """
    try:
        cur = connection.cursor()
        cur.execute(tmcquerystr, {'jdtsid': jeditaskidstr})
        tmc_list = cur.fetchall()
        cur.close()
    except:
        tmc_list = []
    tmc_names = ['jeditaskid', 'pandaid', 'computingsite', 'jobmaxrss', 'jobmaxpss_percore',
                 'sitemaxrss', 'sitemaxrss_percore', 'maxrssratio']
    topmemoryconsumedjobs = [dict(zip(tmc_names, row)) for row in tmc_list]
    for row in topmemoryconsumedjobs:
        try:
            row['maxrssratio'] = int(row['maxrssratio'])
        except:
            row['maxrssratio'] = 0
    return topmemoryconsumedjobs


def get_harverster_workers_for_task(jeditaskid):
    """
    :param jeditaskid: int
    :return: harv_workers_list: list
    """
    jsquery = """
        select t4.*, t5.BATCHID, 
        CASE WHEN not t5.ENDTIME is null THEN t5.ENDTIME-t5.STARTTIME
             WHEN not t5.STARTTIME is null THEN (CAST(SYS_EXTRACT_UTC(systimestamp)AS DATE)-t5.STARTTIME)
             ELSE 0
        END AS WALLTIME,
        
        t5.NCORE, t5.NJOBS FROM (
        SELECT HARVESTERID, WORKERID, SUM(nevents) as sumevents from (
        
        select HARVESTERID, WORKERID, t1.PANDAID, t2.nevents from ATLAS_PANDA.harvester_rel_jobs_workers t1 JOIN 
        
                (    select pandaid, nevents from (
                        (
                        select pandaid, nevents from atlas_pandabigmon.combined_wait_act_def_arch4 where eventservice in (1,3,4,5) and jeditaskid = :jtid
                        )
                        union all
                        (
                            select pandaid, nevents from atlas_pandaarch.jobsarchived where eventservice in (1,3,4,5) and jeditaskid = :jtid
                            minus
                            select pandaid, nevents from atlas_pandaarch.jobsarchived where eventservice in (1,3,4,5) and jeditaskid = :jtid and pandaid in (
                                select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 where eventservice in (1,3,4,5) and jeditaskid = :jtid
                                )
                        )
                    )
                )t2 on t1.pandaid=t2.pandaid
        )t3 group by HARVESTERID, WORKERID) t4
        JOIN ATLAS_PANDA.harvester_workers t5 on t5.HARVESTERID=t4.HARVESTERID and t5.WORKERID = t4.WORKERID
    """

    cur = connection.cursor()
    cur.execute(jsquery, {'jtid': jeditaskid})
    harv_workers = cur.fetchall()
    cur.close()

    harv_workers_names = ['harvesterid', 'workerid', 'sumevents', 'batchid', 'walltime', 'ncore', 'njobs']
    harv_workers_list = [dict(zip(harv_workers_names, row)) for row in harv_workers]
    return harv_workers_list


def get_job_state_summary_for_tasklist(tasks):
    """
    Getting job state summary for list of tasks. Nodrop mode only
    :return: taskJobStateSummary : dictionary
    """

    taskids = [int(task['jeditaskid']) for task in tasks]
    trans_key = insert_to_temp_table(taskids)

    jsquery = """
        select  jeditaskid, jobstatus, count(pandaid) as njobs from (
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandabigmon.combined_wait_act_def_arch4 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
        )
        union all
        (
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
        minus
        select jeditaskid, pandaid, jobstatus from atlas_pandaarch.jobsarchived 
            where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk ) 
                and pandaid in (
                    select pandaid from atlas_pandabigmon.combined_wait_act_def_arch4 
                        where jeditaskid in (select id from ATLAS_PANDABIGMON.TMP_IDS1Debug where TRANSACTIONKEY = :tk )
            )
        )
        )
        group by jeditaskid, jobstatus
        """
    cur = connection.cursor()
    cur.execute(jsquery, {'tk': trans_key})
    js_count_bytask = cur.fetchall()
    cur.close()

    js_count_bytask_names = ['jeditaskid', 'jobstatus', 'count']
    js_count_bytask_list = [dict(zip(js_count_bytask_names, row)) for row in js_count_bytask]

    # list -> dict
    js_count_bytask_dict = {}
    for row in js_count_bytask_list:
        if row['jeditaskid'] not in js_count_bytask_dict:
            js_count_bytask_dict[row['jeditaskid']] = {}
        if row['jobstatus'] not in js_count_bytask_dict[row['jeditaskid']]:
            js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] = 0
        js_count_bytask_dict[row['jeditaskid']][row['jobstatus']] += int(row['count'])

    return js_count_bytask_dict


def get_task_params(jeditaskid):
    """
    Extract task and job parameter lists from CLOB in  Jedi_TaskParams table
    :param jeditaskid: int
    :return: taskparams_list: list
    :return: jobparams_list: list
    """

    query = {'jeditaskid': jeditaskid}
    taskparams = JediTaskparams.objects.filter(**query).values()

    taskparams_list = []
    jobparams_list = []
    if len(taskparams) > 0:
        taskparams = taskparams[0]['taskparams']

    try:
        taskparams = json.loads(taskparams)
    except ValueError:
        pass

    for k in taskparams:
        rec = {'name': k, 'value': taskparams[k]}
        taskparams_list.append(rec)
    taskparams_list = sorted(taskparams_list, key=lambda x: x['name'].lower())

    jobparams = taskparams['jobParameters']
    if 'log' in taskparams:
        jobparams.append(taskparams['log'])
    for p in jobparams:
        if p['type'] == 'constant':
            ptxt = p['value']
        elif p['type'] == 'template':
            ptxt = "<i>{} template:</i> value='{}' ".format(p['param_type'], p['value'])
            for v in p:
                if v in ['type', 'param_type', 'value']:
                    continue
                ptxt += "  {}='{}'".format(v, p[v])
        else:
            ptxt = '<i>unknown parameter type {}:</i> '.format(p['type'])
            for v in p:
                if v in ['type', ]:
                    continue
                ptxt += "  {}='{}'".format(v, p[v])
        jobparams_list.append(ptxt)
    jobparams_list = sorted(jobparams_list, key=lambda x: x.lower())

    return taskparams_list, jobparams_list


def get_hs06s_summary_for_task(jeditaskid):
    """"""
    hquery = {}
    hquery['jeditaskid'] = jeditaskid
    hquery['jobstatus__in'] = ('finished', 'failed')
    hs06sec_sum = []
    hs06sec_sum.extend(Jobsarchived.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))
    hs06sec_sum.extend(Jobsarchived4.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))
    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}
    if len(hs06sec_sum) > 0:
        for hs in hs06sec_sum:
            if hs['jobstatus'] == 'finished':
                hs06sSum['finished'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
            elif hs['jobstatus'] == 'failed':
                hs06sSum['failed'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0

    return hs06sSum
