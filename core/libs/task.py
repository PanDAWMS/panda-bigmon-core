
import logging
import copy
import random
import json
import re

from datetime import datetime, timedelta
from elasticsearch_dsl import Search

from django.db import connection
from django.db.models import Count, Sum
from core.common.models import JediDatasetContents, JediDatasets, JediTaskparams, JediDatasetLocality, JediTasks
from core.pandajob.models import Jobsactive4, Jobsarchived, Jobswaiting4, Jobsdefined4, Jobsarchived4

from core.libs.exlib import insert_to_temp_table, get_tmp_table_name
from core.libs.datetimestrings import parse_datetime
from core.libs.elasticsearch import create_esatlas_connection
from core.libs.job import drop_duplicates
from core.pandajob.utils import get_pandajob_arch_models_by_year
from core.filebrowser.ruciowrapper import ruciowrapper
from core.libs.taskflow import executeTF

import core.constants as const
from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def is_event_service_task(jeditaskid):
    """
    Check if a task is EventService
    :param jeditaskid: int
    :return: eventservice: bool
    """
    eventservice = False

    query = {'jeditaskid': jeditaskid}
    task = list(JediTasks.objects.filter(**query).values('eventservice'))
    if len(task) > 0 and 'eventservice' in task[0] and task[0]['eventservice'] is not None and task[0]['eventservice'] == 1:
        eventservice = True

    return eventservice


def cleanTaskList(tasks, **kwargs):

    add_datasets_info = False
    add_datasets_locality = False
    sortby = None

    if 'add_datasets_info' in kwargs:
        add_datasets_info = kwargs['add_datasets_info']
    if 'add_datasets_locality' in kwargs:
        add_datasets_locality = kwargs['add_datasets_locality']
        add_datasets_info = True
    if 'sortby' in kwargs:
        sortby = kwargs['sortby']

    for task in tasks:
        if task['transpath']:
            task['transpath'] = task['transpath'].split('/')[-1]
        if task['statechangetime'] is None:
            task['statechangetime'] = task['modificationtime']
        if 'eventservice' in task:
            if task['eventservice'] == 1:
                task['eventservice'] = 'eventservice'
            else:
                task['eventservice'] = 'ordinary'
        if 'reqid' in task and task['reqid'] and task['reqid'] < 100000 and task['reqid'] > 100 \
                and task['reqid'] != 300 and 'tasktype' in task and not task['tasktype'].startswith('anal'):
            task['deftreqid'] = task['reqid']
        if 'corecount' in task and task['corecount'] is None:
            task['corecount'] = 1
        task['age'] = get_task_age(task)
        if 'campaign' in task and task['campaign']:
            task['campaign_cut'] = ':'.join(task['campaign'].split(':')[1:]) if ':' in task['campaign'] else task['campaign']
        if 'workinggroup' in task and task['workinggroup'] is not None and task['workinggroup'] != '':
            task['owner'] = task['workinggroup']
        else:
            task['owner'] = task['username']

        if 'tasktype' in task and task['tasktype'].startswith('ana'):
            if 'workinggroup' in task and task['workinggroup'] is not None and task['workinggroup'] != '':
                task['category'] = 'group analysis' if 'username' in task and task['username'] not in ('artprod', 'atlevind', 'gangarbt') else 'service'
            else:
                task['category'] = 'user analysis'
        else:
            if 'workinggroup' in task and task['workinggroup'] is not None and task['workinggroup'].startswith('GP_'):
                task['category'] = 'group production'
            else:
                task['category'] = 'production'

    # Get status of input processing as indicator of task progress if requested
    if add_datasets_info:
        dvalues = ('jeditaskid', 'nfiles', 'nfilesfinished', 'nfilesfailed')
        dsquery = {
            'type__in': ['input', 'pseudo_input'],
            'masterid__isnull': True,
        }
        extra = '(1=1)'

        taskl = [t['jeditaskid'] for t in tasks if 'jeditaskid' in t]

        if len(taskl) <= settings.DB_N_MAX_IN_QUERY:
            dsquery['jeditaskid__in'] = taskl
        else:
            # Backend dependable
            tk = insert_to_temp_table(taskl)
            extra = "JEDITASKID in (SELECT ID FROM {} WHERE TRANSACTIONKEY={})".format(get_tmp_table_name(), tk)

        dsets = JediDatasets.objects.filter(**dsquery).extra(where=[extra]).values(*dvalues)
        dsinfo = {}
        if len(dsets) > 0:
            for ds in dsets:
                taskid = ds['jeditaskid']
                if taskid not in dsinfo:
                    dsinfo[taskid] = []
                dsinfo[taskid].append(ds)

        if add_datasets_locality:
            input_dataset_rse = get_dataset_locality(taskl)

        for task in tasks:
            if 'totevrem' not in task:
                task['totevrem'] = None
            dstotals = {
                'nfiles': 0,
                'nfilesfinished': 0,
                'nfilesfailed': 0,
                'pctfinished': 0,
                'pctfailed': 0,
            }
            if task['jeditaskid'] in dsinfo:
                nfiles = 0
                nfinished = 0
                nfailed = 0
                for ds in dsinfo[task['jeditaskid']]:
                    if int(ds['nfiles']) > 0:
                        nfiles += int(ds['nfiles'])
                        nfinished += int(ds['nfilesfinished'])
                        nfailed += int(ds['nfilesfailed'])
                if nfiles > 0:
                    dstotals['nfiles'] = nfiles
                    dstotals['nfilesfinished'] = nfinished
                    dstotals['nfilesfailed'] = nfailed
                    dstotals['pctfinished'] = round(100. * nfinished / nfiles, 2)
                    dstotals['pctfailed'] = round(100. * nfailed / nfiles, 2)

            task['dsinfo'] = dstotals
            task.update(dstotals)

    if sortby is not None:
        if sortby == 'creationdate-asc':
            tasks = sorted(tasks, key=lambda x: x['creationdate'])
        if sortby == 'time-ascending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'])
        if sortby == 'time-descending':
            tasks = sorted(tasks, key=lambda x: x['modificationtime'], reverse=True)
        if sortby == 'statetime-descending':
            tasks = sorted(tasks, key=lambda x: x['statechangetime'], reverse=True)
        elif sortby == 'priority':
            tasks = sorted(tasks, key=lambda x: x['taskpriority'], reverse=True)
        elif sortby == 'nfiles':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['nfiles'], reverse=True)
        elif sortby == 'pctfinished':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfinished'], reverse=True)
        elif sortby == 'pctfailed':
            tasks = sorted(tasks, key=lambda x: x['dsinfo']['pctfailed'], reverse=True)
        elif sortby == 'taskname':
            tasks = sorted(tasks, key=lambda x: x['taskname'])
        elif sortby == 'jeditaskid' or sortby == 'taskid' or sortby == 'jeditaskid-desc' or sortby == 'taskid-desc':
            tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])
        elif sortby == 'jeditaskid-asc' or sortby == 'taskid-asc':
            tasks = sorted(tasks, key=lambda x: x['jeditaskid'])
        elif sortby == 'cloud':
            tasks = sorted(tasks, key=lambda x: x['cloud'], reverse=True)
    else:
        tasks = sorted(tasks, key=lambda x: -x['jeditaskid'])

    return tasks


def task_summary_dict(request, tasks, fieldlist=None):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    numeric_fields_task = ['reqid', 'corecount', 'taskpriority', 'workqueue_id']

    if fieldlist:
        flist = fieldlist
    else:
        flist = copy.deepcopy(const.TASK_FIELDS_STANDARD)

    for task in tasks:
        for f in flist:
            if 'tasktype' in request.session['requestParams'] and request.session['requestParams']['tasktype'].startswith('analy'):
                # Remove the noisy useless parameters in analysis listings
                if flist in ('reqid', 'stream', 'tag'):
                    continue

            if 'taskname' in task and len(task['taskname'].split('.')) == 5:
                if f == 'project':
                    try:
                        if not f in sumd:
                            sumd[f] = {}
                        project = task['taskname'].split('.')[0]
                        if not project in sumd[f]:
                            sumd[f][project] = 0
                        sumd[f][project] += 1
                    except:
                        pass
                if f == 'stream':
                    try:
                        if not f in sumd:
                            sumd[f] = {}
                        stream = task['taskname'].split('.')[2]
                        if not re.match('[0-9]+', stream):
                            if not stream in sumd[f]:
                                sumd[f][stream] = 0
                            sumd[f][stream] += 1
                    except:
                        pass
                if f == 'tag':
                    try:
                        if not f in sumd:
                            sumd[f] = {}
                        tags = task['taskname'].split('.')[4]
                        if not tags.startswith('job_'):
                            tagl = tags.split('_')
                            tag = tagl[-1]
                            if not tag in sumd[f]:
                                sumd[f][tag] = 0
                            sumd[f][tag] += 1
                    except:
                        pass
            if f in task:
                val = task[f]
                if val is None or val == '':
                    val = 'Not specified'
                if val == 'anal':
                    val = 'analy'
                if f not in sumd:
                    sumd[f] = {}
                if val not in sumd[f]:
                    sumd[f][val] = 0
                sumd[f][val] += 1

    # convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        if f != 'ramcount':
            for ky in kys:
                iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
            iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())
        else:
            newvalues = {}
            for ky in kys:
                if ky != 'Not specified':
                    roundedval = int(ky / 1000)
                else:
                    roundedval = -1
                if roundedval in newvalues:
                    newvalues[roundedval] += sumd[f][ky]
                else:
                    newvalues[roundedval] = sumd[f][ky]
            for ky in newvalues:
                if ky >= 0:
                    iteml.append({'kname': str(ky) + '-' + str(ky + 1) + 'GB', 'kvalue': newvalues[ky]})
                else:
                    iteml.append({'kname': 'Not specified', 'kvalue': newvalues[ky]})
            iteml = sorted(iteml, key=lambda x: str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def wg_task_summary(request, fieldname='workinggroup', view='production', taskdays=3):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    query = {}
    hours = 24 * taskdays
    startdate = datetime.now() - timedelta(hours=hours)
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = datetime.now().strftime(settings.DATETIME_FORMAT)
    query['modificationtime__castdate__range'] = [startdate, enddate]
    if fieldname == 'workinggroup':
        query['workinggroup__isnull'] = False
    if view == 'production':
        query['tasktype'] = 'prod'
    elif view == 'analysis':
        query['tasktype'] = 'anal'

    if 'processingtype' in request.session['requestParams']:
        query['processingtype'] = request.session['requestParams']['processingtype']

    if 'workinggroup' in request.session['requestParams']:
        query['workinggroup'] = request.session['requestParams']['workinggroup']

    if 'project' in request.session['requestParams']:
        query['taskname__istartswith'] = request.session['requestParams']['project']

    summary = JediTasks.objects.filter(**query).values(fieldname, 'status').annotate(Count('status')).order_by(
        fieldname, 'status')
    totstates = {}
    tottasks = 0
    wgsum = {}
    for state in const.TASK_STATES:
        totstates[state] = 0
    for rec in summary:
        wg = rec[fieldname]
        status = rec['status']
        count = rec['status__count']
        if status not in const.TASK_STATES:
            continue
        tottasks += count
        totstates[status] += count
        if wg not in wgsum:
            wgsum[wg] = {}
            wgsum[wg]['name'] = wg
            wgsum[wg]['count'] = 0
            wgsum[wg]['states'] = {}
            wgsum[wg]['statelist'] = []
            for state in const.TASK_STATES:
                wgsum[wg]['states'][state] = {}
                wgsum[wg]['states'][state]['name'] = state
                wgsum[wg]['states'][state]['count'] = 0
        wgsum[wg]['count'] += count
        wgsum[wg]['states'][status]['count'] += count

    # convert to ordered lists
    suml = []
    for f in wgsum:
        itemd = {}
        itemd['field'] = f
        itemd['count'] = wgsum[f]['count']
        kys = copy.deepcopy(const.TASK_STATES)
        iteml = []
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': wgsum[f]['states'][ky]['count']})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


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
            if 'jobmetrics' in job and job['jobmetrics'] and 'scout=' in job['jobmetrics'] and \
                    jst in job['jobmetrics'][job['jobmetrics'].index('scout='):]:
                scouts_dict[jst].append(job['pandaid'])

    # remove scout type if no scouts
    st_to_remove = []
    for jst, jstd in scouts_dict.items():
        if len(jstd) == 0:
            st_to_remove.append(jst)
    for st in st_to_remove:
        if st in scouts_dict:
            del scouts_dict[st]

    return scouts_dict


def calculate_metrics(jobs, metrics_names):
    """
    Calculate job metrics for a task
    :param jobs:
    :param metrics_names:
    :return:
    """
    metrics_def_dict = {mn: {'metric': mn.split('_')[0], 'agg': mn.split('_')[1], 'data': [], 'value': -1} for mn in metrics_names}

    for job in jobs:
        if job['category'] == 'run' and job['jobstatus'] == 'finished':
            for mn, mdata in metrics_def_dict.items():
                if 'per' in mdata['metric']:
                    if mdata['metric'].split('per')[0] in job and mdata['metric'].split('per')[1] in job and job[mdata['metric'].split('per')[1]] > 0:
                        mdata['data'].append(job[mdata['metric'].split('per')[0]]/(1.0*job[mdata['metric'].split('per')[1]]))
                elif mdata['metric'] in job and job[mdata['metric']]:
                    mdata['data'].append(job[mdata['metric']])

    for mn, mdata in metrics_def_dict.items():
        if 'avg' in mdata['agg']:
            mdata['value'] = sum(mdata['data'])/(1.0*len(mdata['data'])) if len(mdata['data']) > 0 else -1
        if 'sum' in mdata['agg']:
            mdata['value'] = sum(mdata['data'])

    metrics = {}
    for mn, mdata in metrics_def_dict.items():
        if mdata['value'] > 0:
            if 'percent' in mdata['agg']:
                metrics[mn] = round(mdata['value'] * 100.0, 2)
            else:
                metrics[mn] = round(mdata['value'], 2)

    return metrics


def datasets_for_task(jeditaskid):
    """
    Getting list of datasets corresponding to a task and file state summary
    :param jeditaskid: int
    :return: dsets: list of dicts
    :return: dsinfo: dict
    """
    dsets = []
    dsinfo = {
        'nfiles': 0,
        'nfilesfinished': 0,
        'nfilesfailed': 0,
        'pctfinished': 0.0,
        'pctfailed': 0,
        'neventsTot': 0,
        'neventsUsedTot': 0,
        'neventsOutput': 0,
    }

    dsquery = {
        'jeditaskid': jeditaskid,
    }
    values = (
        'jeditaskid', 'datasetid', 'datasetname', 'containername', 'type', 'masterid', 'streamname', 'status',
        'storagetoken', 'nevents', 'neventsused', 'neventstobeused', 'nfiles', 'nfilesfinished', 'nfilesfailed'
    )
    dsets.extend(JediDatasets.objects.filter(**dsquery).values(*values))

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

            # input primary datasets
            if ds['type'] in ['input', 'pseudo_input'] and ds['masterid'] is None:
                if not ds['nevents'] is None and int(ds['nevents']) > 0:
                    dsinfo['neventsTot'] += int(ds['nevents'])
                if not ds['neventsused'] is None and int(ds['neventsused']) > 0:
                    dsinfo['neventsUsedTot'] += int(ds['neventsused'])

                if int(ds['nfiles']) > 0:
                    ds['percentfinished'] = int(100. * int(ds['nfilesfinished']) / int(ds['nfiles']))
                    dsinfo['nfiles'] += int(ds['nfiles'])
                    dsinfo['nfilesfinished'] += int(ds['nfilesfinished'])
                    dsinfo['nfilesfailed'] += int(ds['nfilesfailed'])
            elif ds['type'] in ('output', ) and ds['streamname'] is not None and ds['streamname'] == 'OUTPUT0':
                # OUTPUT0 - the first and the main steam of outputs
                dsinfo['neventsOutput'] += int(ds['nevents']) if ds['nevents'] and ds['nevents'] > 0 else 0

        dsets = newdslist
        dsets = sorted(dsets, key=lambda x: x['datasetname'].lower())

        dsinfo['pctfinished'] = round(100.*dsinfo['nfilesfinished']/dsinfo['nfiles'], 2) if dsinfo['nfiles'] > 0 else 0
        dsinfo['pctfailed'] = round(100.*dsinfo['nfilesfailed']/dsinfo['nfiles'], 2) if dsinfo['nfiles'] > 0 else 0

    return dsets, dsinfo


def get_datasets_for_tasklist(tasks):
    """
    Dump datasets for each task in the list into 'datasets' dict
    :param tasks: list of dicts
    :return: tasks
    """
    task_ids = [task['jeditaskid'] for task in tasks if 'jeditaskid' in task]

    query = {'type__in': ['pseudo_input', 'input', 'output']}
    extra_str = '1=1'
    if len(tasks) > settings.DB_N_MAX_IN_QUERY:
        # insert ids to tmp table, backend dependable
        tk = insert_to_temp_table(task_ids)
        extra_str = "JEDITASKID in (SELECT ID FROM {} WHERE TRANSACTIONKEY={})".format(get_tmp_table_name(), tk)
    else:
        query['jeditaskid__in'] = task_ids

    dsets = JediDatasets.objects.filter(**query).extra(where=[extra_str]).values()

    dsets_dict = {}
    for ds in dsets:
        if ds['jeditaskid'] not in dsets_dict:
            dsets_dict[ds['jeditaskid']] = []
        dsets_dict[ds['jeditaskid']].append(ds)

    for task in tasks:
        task['datasets'] = dsets_dict[task['jeditaskid']] if task['jeditaskid'] in dsets_dict else []

    return tasks


def input_summary_for_task(taskrec, dsets):
    """
    The function returns:
    Input event chunks list for separate table
    Input event chunks summary by states
    A dictionary with tk as key and list of input files IDs that is needed for jobList view filter
    """
    jeditaskid = taskrec['jeditaskid']
    # Getting statuses of inputfiles
    if datetime.strptime(taskrec['creationdate'], settings.DATETIME_FORMAT) < \
            datetime.strptime('2018-10-22 10:00:00', settings.DATETIME_FORMAT):
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
        indsids = [ds['datasetid'] for ds in dsets if ds['type'] == 'input' and (ds['masterid'] is None or ds['masterid'] == '')]
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


def get_task_params(jeditaskid):
    """
    Extract task and job parameter lists from CLOB in  Jedi_TaskParams table
    :param jeditaskid: int
    :return: taskparams: dict
    :return: jobparams: list
    """

    query = {'jeditaskid': jeditaskid}
    taskparams = JediTaskparams.objects.filter(**query).values()

    if len(taskparams) > 0:
        taskparams = taskparams[0]['taskparams']
    try:
        taskparams = json.loads(taskparams)
    except ValueError:
        pass

    return taskparams


def humanize_task_params(taskparams):
    """
    Prepare list of params for template output
    :param taskparams: dict
    :return: taskparams_list, jobparams_list
    """
    taskparams_list = []
    jobparams_list = []

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


def get_hs06s_summary_for_task(query):
    """"""
    hs06sSum = {'finished': 0, 'failed': 0, 'total': 0}

    hquery = copy.deepcopy(query)
    hquery['jobstatus__in'] = ('finished', 'failed')

    if 'jeditaskid' in hquery:

        hs06sec_sum = []
        # getting jobs. Can not use the .annotate() as there can be duplicates
        jobs = []
        jvalues = ('pandaid', 'jobstatus', 'hs06sec')
        jobs.extend(Jobsarchived4.objects.filter(**hquery).values(*jvalues))
        jobs.extend(Jobsarchived.objects.filter(**hquery).values(*jvalues))
        jobs = drop_duplicates(jobs)

        for job in jobs:
            hs06sSum['total'] += job['hs06sec'] if job['hs06sec'] is not None else 0
            if job['jobstatus'] == 'finished':
                hs06sSum['finished'] += job['hs06sec'] if job['hs06sec'] is not None else 0
            elif job['jobstatus'] == 'failed':
                hs06sSum['failed'] += job['hs06sec'] if job['hs06sec'] is not None else 0


        # getting data from ATLARC DB
        pj_models = get_pandajob_arch_models_by_year(query['modificationtime__castdate__range'])
        if len(pj_models) > 0:
            for pjm in pj_models:
                try:
                    hs06sec_sum.extend(pjm.objects.filter(**hquery).values('jobstatus').annotate(hs06secsum=Sum('hs06sec')))
                except Exception as ex:
                    _logger.exception('Failed to get hs06sec from {} at ATLARC DB:\n{}'.format(pjm, ex))

        if len(hs06sec_sum) > 0:
            for hs in hs06sec_sum:
                hs06sSum['total'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                if hs['jobstatus'] == 'finished':
                    hs06sSum['finished'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0
                elif hs['jobstatus'] == 'failed':
                    hs06sSum['failed'] += hs['hs06secsum'] if hs['hs06secsum'] is not None else 0


    return hs06sSum


def get_task_age(task):
    """
    :param task: dict of task params, creationtime is obligatory
    :return: age in days or -1 if not enough data provided
    """
    task_age = -1

    if 'creationdate' in task and task['creationdate'] is not None:
        creationtime = parse_datetime(task['creationdate']) if not isinstance(task['creationdate'], datetime) else task['creationdate']
    else:
        creationtime = None
    if 'endtime' in task and task['endtime'] is not None:
        endtime = parse_datetime(task['endtime']) if not isinstance(task['endtime'], datetime) else task['endtime']
    else:
        endtime = datetime.now()

    if 'status' in task and task['status'] not in const.TASK_STATES_FINAL:
        endtime = datetime.now()

    if endtime and creationtime:
        task_age = round((endtime-creationtime).total_seconds() / 60. / 60. / 24., 2)

    return task_age


def get_task_timewindow(task, **kwargs):
    """
    Return a list of two datetime when task run
    :param task:
    :return: timewindow: list of datetime or str
    """
    format_out = 'datetime'
    if 'format_out' in kwargs and kwargs['format_out'] == 'str':
        format_out = 'str'

    timewindow = [datetime.now(), datetime.now()]

    if 'creationdate' in task and task['creationdate']:
        timewindow[0] = task['creationdate'] if isinstance(task['creationdate'], datetime) else parse_datetime(task['creationdate'])
    else:
        timewindow[0] = datetime.now()

    if task['status'] in const.TASK_STATES_FINAL:
        if 'endtime' in task and task['endtime']:
            timewindow[1] = task['endtime'] if isinstance(task['endtime'], datetime) else parse_datetime(task['endtime'])
        elif 'modificationtime' in task and task['modificationtime']:
            timewindow[1] = task['modificationtime'] if isinstance(task['modificationtime'], datetime) else parse_datetime(task['modificationtime'])
        else:
            timewindow[1] = datetime.now()
    else:
        timewindow[1] = datetime.now()

    if format_out == 'str':
        timewindow = [t.strftime(settings.DATETIME_FORMAT) for t in timewindow]

    return timewindow


def get_task_time_archive_flag(task_timewindow):
    """
    Decide which tables query, if -1: only atlarc, 1: adcr, 0: both
    :param timewindow: list of two datetime
    :return: task_age_flag: -1, 0 or 1
    """
    #
    task_age_flag = 1
    if task_timewindow[1] < datetime.now() - timedelta(days=365*3):
        task_age_flag = -1
    elif task_timewindow[0] > datetime.now() - timedelta(days=365*3) and task_timewindow[1] < datetime.now() - timedelta(days=365*2):
        task_age_flag = 0
    elif task_timewindow[0] > datetime.now() - timedelta(days=365*2):
        task_age_flag = 1

    return task_age_flag


def get_dataset_locality(jeditaskid):
    """
    Getting RSEs for a task input datasets
    :return:
    """
    N_IN_MAX = 100
    query = {}
    extra_str = ' (1=1) '
    if isinstance(jeditaskid, list) or isinstance(jeditaskid, tuple):
        if len(jeditaskid) > N_IN_MAX:
            trans_key = insert_to_temp_table(jeditaskid)
            tmp_table = get_tmp_table_name()
            extra_str += ' AND jeditaskid IN (SELEECT id FROM {} WHERE transactionkey = {} )'.format(tmp_table, trans_key)
        else:
            query['jeditaskid__in'] = jeditaskid
    elif isinstance(jeditaskid, int):
        query['jeditaskid'] = jeditaskid

    rse_list = JediDatasetLocality.objects.filter(**query).extra(where=[extra_str]).values()

    rse_dict = {}
    if len(rse_list) > 0:
        for item in rse_list:
            if item['jeditaskid'] not in rse_dict:
                rse_dict[item['jeditaskid']] = {}
            if item['datasetid'] not in rse_dict[item['jeditaskid']]:
                rse_dict[item['jeditaskid']][item['datasetid']] = []
            rse_dict[item['jeditaskid']][item['datasetid']].append({'rse': item['rse'], 'timestamp': item['timestamp']})

    return rse_dict


def get_prod_slice_by_taskid(jeditaskid):
    try:
        jsquery = """
            SELECT tasks.taskid, tasks.PR_ID, tasks.STEP_ID, datasets.SLICE from ATLAS_DEFT.T_PRODUCTION_TASK tasks 
            JOIN ATLAS_DEFT.T_PRODUCTION_STEP steps on tasks.step_id = steps.step_id 
            JOIN ATLAS_DEFT.T_INPUT_DATASET datasets ON datasets.IND_ID=steps.IND_ID  
            where tasks.taskid=:taskid
        """
        cur = connection.cursor()
        cur.execute(jsquery, {'taskid': jeditaskid})
        task_prod_info = cur.fetchall()
        cur.close()
    except Exception as ex:
        task_prod_info = None
        _logger.exception('Failed to get slice by taskid from DEFT DB:\n{}'.format(ex))
    slice = None
    if task_prod_info:
        slice = task_prod_info[0][3]
    return slice


def get_logs_by_taskid(jeditaskid):

    tasks_logs = []

    connection = create_esatlas_connection()

    s = Search(using=connection, index='atlas_jedilogs-*')

    s = s.filter('term', **{'jediTaskID': jeditaskid})

    s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    response = s.execute()

    for agg in response['aggregations']['logName']['buckets']:
        for types in agg['type']['buckets']:
            type = types['key']
            for levelnames in types['logLevel']['buckets']:
                levelname = levelnames['key']
                tasks_logs.append({'jediTaskID': jeditaskid, 'logname': type, 'loglevel': levelname,
                                   'lcount': str(levelnames['doc_count'])})

    s = Search(using=connection, index='atlas_pandalogs-*')

    s = s.filter('term', **{'jediTaskID': jeditaskid})

    s.aggs.bucket('logName', 'terms', field='logName.keyword', size=1000) \
        .bucket('type', 'terms', field='fields.type.keyword') \
        .bucket('logLevel', 'terms', field='logLevel.keyword')

    response = s.execute()

    for agg in response['aggregations']['logName']['buckets']:
        for types in agg['type']['buckets']:
            type = types['key']
            for levelnames in types['logLevel']['buckets']:
                levelname = levelnames['key']
                tasks_logs.append({'jediTaskID': jeditaskid, 'logname': type, 'loglevel': levelname,
                                   'lcount': str(levelnames['doc_count'])})

    return tasks_logs


def taskNameDict(jobs):
    """
    Translate IDs to names. Awkward because models don't provide foreign keys to task records.
    :param jobs: list of dist
    :return:
    """
    jeditaskids = {}
    for job in jobs:
        if 'taskid' in job and job['taskid'] and job['taskid'] > 0:
            jeditaskids[job['taskid']] = 1
        if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0:
            jeditaskids[job['jeditaskid']] = 1
    jeditaskidl = list(jeditaskids.keys())

    # Write ids to temp table to avoid too many bind variables oracle error
    tasknamedict = {}
    if len(jeditaskidl) > 0:
        tquery = {}
        if len(jeditaskidl) < settings.DB_N_MAX_IN_QUERY:
            tquery['jeditaskid__in'] = jeditaskidl
            extra = "(1=1)"
        else:
            tmp_table_name = get_tmp_table_name()
            transaction_key = insert_to_temp_table(jeditaskidl)
            extra = 'JEDITASKID IN (SELECT ID FROM {} WHERE TRANSACTIONKEY = {})'.format(tmp_table_name, transaction_key)
        jeditasks = JediTasks.objects.filter(**tquery).extra(where=[extra]).values('taskname', 'jeditaskid')
        for t in jeditasks:
            tasknamedict[t['jeditaskid']] = t['taskname']

    return tasknamedict


def get_task_name_by_taskid(taskid):
    taskname = ''
    if taskid and taskid != 'None':
        tasks = JediTasks.objects.filter(jeditaskid=taskid).values('taskname')
        if len(tasks) > 0:
            taskname = tasks[0]['taskname']
    return taskname


def get_task_flow_data(jeditaskid):
    """
    Getting data for task data flow diagram
    RSE -> dataset -> PQ -> njobs in state
    :param jeditaskid: int
    :return: data for sankey diagram: list ['from', 'to', 'weight']
    """
    data = []
    # get datasets
    datasets = []
    dquery = {'jeditaskid': jeditaskid, 'type__in': ['input', 'pseudo_input'], 'masterid__isnull': True}
    datasets.extend(JediDatasets.objects.filter(**dquery).values('jeditaskid', 'datasetname', 'type'))

    dataset_dict = {}
    for d in datasets:
        dname = d['datasetname'] if ':' not in d['datasetname'] else d['datasetname'].split(':')[1]
        dataset_dict[dname] = {'replica': {}, 'jobs': {}}

    # get jobs aggregated by status, computingsite and proddblock (input dataset name)
    jobs = []
    jquery = {'jeditaskid': jeditaskid, 'prodsourcelabel__in': ['user', 'managed'], }
    extra_str = "( processingtype not in ('pmerge') )"
    jvalues = ['proddblock', 'computingsite', 'jobstatus']
    jobs.extend(Jobsarchived4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsarchived.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsactive4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobsdefined4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))
    jobs.extend(Jobswaiting4.objects.filter(**jquery).extra(where=[extra_str]).values(*jvalues).annotate(njobs=Count('pandaid')))

    if len(jobs) > 0:
        for j in jobs:
            if len(j['proddblock']) > 0:
                dname = j['proddblock'] if ':' not in j['proddblock'] else j['proddblock'].split(':')[1]
            else:
                dname = next(iter(dataset_dict)) if len(dataset_dict) > 0 else 'pseudo_dataset'
            if j['computingsite'] is not None and j['computingsite'] != '':
                if j['computingsite'] not in dataset_dict[dname]['jobs']:
                    dataset_dict[dname]['jobs'][j['computingsite']] = {}
                job_state = j['jobstatus'] if j['jobstatus'] in const.JOB_STATES_FINAL else 'active'
                if job_state not in dataset_dict[dname]['jobs'][j['computingsite']]:
                    dataset_dict[dname]['jobs'][j['computingsite']][job_state] = 0
                dataset_dict[dname]['jobs'][j['computingsite']][job_state] += j['njobs']

    # get RSE for datasets
    replicas = []
    if len(datasets) > 0:
        dids = []
        for d in datasets:
            if d['type'] == 'input':
                did = {
                    'scope': d['datasetname'].split(':')[0] if ':' in d['datasetname'] else d['datasetname'].split('.')[0],
                    'name': d['datasetname'].split(':')[1] if ':' in d['datasetname'] else d['datasetname'],
                    }
                dids.append(did)

        rw = ruciowrapper()
        replicas = rw.getRSEbyDID(dids)

        if replicas is not None and len(replicas) > 0:
            for r in replicas:
                if r['name'] in dataset_dict:
                    dataset_dict[r['name']]['replica'][r['rse']] = {
                        'state': r['state'],
                        'available_pct': round(100.0 * r['available_length']/r['length'], 1) if r['length'] > 0 else 0
                    }

    # transform data for plot and return
    return executeTF({'data': {'datasets': dataset_dict, } })

