"""
A set of functions to get jobs from JOBS* and group them by task
"""
import logging
import copy
from django.db.models import Count

from core.libs.task import taskNameDict
from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4

import core.constants as const

_logger = logging.getLogger('bigpandamon')


def task_summary(query, limit=999999, view='all', sortby='taskid'):

    tasksummarydata = task_summary_data(query, limit=limit)
    tasks = {}
    totstates = {}
    totjobs = 0
    for state in const.JOB_STATES_SITE:
        totstates[state] = 0

    taskids = []
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskids.append({'jeditaskid': rec['jeditaskid']})
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskids.append({'taskid': rec['taskid']})
    
    tasknamedict = taskNameDict(taskids)
    for rec in tasksummarydata:
        if 'jeditaskid' in rec and rec['jeditaskid'] and rec['jeditaskid'] > 0:
            taskid = rec['jeditaskid']
            tasktype = 'JEDI'
        elif 'taskid' in rec and rec['taskid'] and rec['taskid'] > 0:
            taskid = rec['taskid']
            tasktype = 'old'
        else:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if jobstatus not in const.JOB_STATES_SITE: 
            continue
        totjobs += count
        totstates[jobstatus] += count
        if taskid not in tasks:
            tasks[taskid] = {}
            tasks[taskid]['taskid'] = taskid
            if taskid in tasknamedict:
                tasks[taskid]['name'] = tasknamedict[taskid]
            else:
                tasks[taskid]['name'] = str(taskid)
            tasks[taskid]['count'] = 0
            tasks[taskid]['states'] = {}
            tasks[taskid]['statelist'] = []
            for state in const.JOB_STATES_SITE:
                tasks[taskid]['states'][state] = {}
                tasks[taskid]['states'][state]['name'] = state
                tasks[taskid]['states'][state]['count'] = 0
        tasks[taskid]['count'] += count
        tasks[taskid]['states'][jobstatus]['count'] += count
   
    if view == 'analysis':
        # Show only tasks starting with 'user.'
        kys = list(tasks.keys())
        for t in kys:
            if not str(tasks[t]['name'].encode('ascii', 'ignore')).startswith('user.'): del tasks[t]
    
    # Convert dict to summary list
    taskkeys = list(tasks.keys())
    taskkeys = sorted(taskkeys)
    fullsummary = []
    for taskid in taskkeys:
        for state in const.JOB_STATES_SITE:
            tasks[taskid]['statelist'].append(tasks[taskid]['states'][state])
        if tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count'] > 0:
            tasks[taskid]['pctfail'] = int(100. * float(tasks[taskid]['states']['failed']['count']) / (
                tasks[taskid]['states']['finished']['count'] + tasks[taskid]['states']['failed']['count']))
        else:
            tasks[taskid]['pctfail'] = 0
        fullsummary.append(tasks[taskid])
    
    # sorting
    if sortby in const.JOB_STATES_SITE:
        fullsummary = sorted(fullsummary, key=lambda x: x['states'][sortby], reverse=True)
    elif sortby == 'pctfail':
        fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)

    return fullsummary


def task_summary_data(query, limit=1000):
    summary = []
    querynotime = copy.deepcopy(query)
    del querynotime['modificationtime__castdate__range']
    
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])
    summary.extend(Jobsarchived4.objects.filter(**query).values('taskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('taskid', 'jobstatus')[:limit])

    summary.extend(Jobsactive4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    summary.extend(Jobsarchived4.objects.filter(**query).values('jeditaskid', 'jobstatus').annotate(
        Count('jobstatus')).order_by('jeditaskid', 'jobstatus')[:limit])
    
    return summary
