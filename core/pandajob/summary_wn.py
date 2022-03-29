"""
A set of functions to get jobs from JOBS* and group them by worker node
"""
import logging
import copy
from datetime import datetime, timedelta
from django.db.models import Count, Sum, Q

from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4, Jobsarchived
from core.libs.datetimestrings import parse_datetime
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def wn_summary_data(query):
    summary = []
    querynotime = copy.deepcopy(query)
    cores_running = Sum('actualcorecount', filter=Q(jobstatus__exact='running'))
    minramcount_running = Sum('minramcount', filter=Q(jobstatus__exact='running'))
    is_archive = False
    if 'modificationtime__castdate__range' in querynotime:
        if parse_datetime(querynotime['modificationtime__castdate__range'][0]) < (datetime.now() - timedelta(days=3)):
            is_archive = True
        try:
            del querynotime['modificationtime__castdate__range']    # creates inconsistency with job lists. Stick to advertised 12hrs
        except KeyError:
            _logger.warning('Failed to remove modificationtime range from query')

    summary.extend(Jobsdefined4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).annotate(rcores=cores_running).annotate(rminramcount=minramcount_running).order_by('modificationhost', 'jobstatus'))
    summary.extend(Jobswaiting4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).annotate(rcores=cores_running).annotate(rminramcount=minramcount_running).order_by('modificationhost', 'jobstatus'))
    summary.extend(Jobsactive4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).annotate(rcores=cores_running).annotate(rminramcount=minramcount_running).order_by('modificationhost', 'jobstatus'))
    summary.extend(Jobsarchived4.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
        Count('jobstatus')).annotate(rcores=cores_running).annotate(rminramcount=minramcount_running).order_by('modificationhost', 'jobstatus'))
    if is_archive:
        summary.extend(Jobsarchived.objects.filter(**query).values('modificationhost', 'jobstatus').annotate(
            Count('jobstatus')).annotate(rcores=cores_running).annotate(rminramcount=minramcount_running).order_by('modificationhost', 'jobstatus'))

    return summary


def wn_summary(wnname, query):
    """Make summary of job counts in different states by worker node name (extracted from modificationhost field)"""

    wnsummarydata = wn_summary_data(query)

    totstates = {}
    totjobs = 0
    totrcores = 0
    totrminramcount = 0
    wns = {}
    wnPlotFailed = {}
    wnPlotFinished = {}
    for state in const.JOB_STATES_SITE:
        totstates[state] = 0
    for rec in wnsummarydata:
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        rcores = rec['rcores'] if rec['rcores'] is not None else 0
        rminramcount = rec['rminramcount'] if rec['rminramcount'] is not None else 0
        wnfull = rec['modificationhost']
        wnsplit = wnfull.split('@')
        if len(wnsplit) == 2:
            if wnname == 'all':
                wn = wnsplit[1]
            else:
                wn = wnfull
            slot = wnsplit[0]
        else:
            wn = wnfull
            slot = '1'
        if wn.startswith('aipanda'):
            wn = 'Unknown'
            slot = ''
        if jobstatus == 'failed':
            if not wn in wnPlotFailed: wnPlotFailed[wn] = 0
            wnPlotFailed[wn] += count
        elif jobstatus == 'finished':
            if not wn in wnPlotFinished: wnPlotFinished[wn] = 0
            wnPlotFinished[wn] += count
        totjobs += count
        if jobstatus not in totstates:
            totstates[jobstatus] = 0
        totstates[jobstatus] += count
        totrcores += rcores
        totrminramcount += rminramcount
        if wn not in wns:
            wns[wn] = {}
            wns[wn]['name'] = wn
            wns[wn]['count'] = 0
            wns[wn]['states'] = {}
            wns[wn]['rcores'] = 0
            wns[wn]['rminramcount'] = 0
            wns[wn]['slotd'] = {}
            wns[wn]['statelist'] = []
            for state in const.JOB_STATES_SITE:
                wns[wn]['states'][state] = {}
                wns[wn]['states'][state]['name'] = state
                wns[wn]['states'][state]['count'] = 0
        if slot not in wns[wn]['slotd']: wns[wn]['slotd'][slot] = 0
        wns[wn]['slotd'][slot] += 1
        wns[wn]['count'] += count
        wns[wn]['rcores'] += rcores
        wns[wn]['rminramcount'] += rminramcount
        if jobstatus not in wns[wn]['states']:
            wns[wn]['states'][jobstatus] = {}
            wns[wn]['states'][jobstatus]['count'] = 0
        wns[wn]['states'][jobstatus]['count'] += count

    # Convert dict to summary list
    wnkeys = wns.keys()
    wnkeys = sorted(wnkeys)
    wntot = len(wnkeys)
    fullsummary = []

    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allwns = {}
    allwns['name'] = 'All'
    allwns['slotcount'] = sum([len(row['slotd']) for key, row in wns.items()])
    allwns['count'] = totjobs
    allwns['rcores'] = totrcores
    allwns['rminramcount'] = round(totrminramcount*1.0/1000, 2)
    allwns['states'] = totstates
    allwns['statelist'] = []
    for state in const.JOB_STATES_SITE:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allwns['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allwns['pctfail'] = int(100. * float(allstated['failed']) / (allstated['finished'] + allstated['failed']))
    else:
        allwns['pctfail'] = 0
    if wnname == 'all':
        fullsummary.append(allwns)
    avgwns = {'name': 'Average'}
    if wntot > 0:
        avgwns['count'] = "%0.2f" % (totjobs / wntot)
    else:
        avgwns['count'] = ''
    avgwns['states'] = totstates
    avgwns['statelist'] = []
    avgstates = {}
    for state in const.JOB_STATES_SITE:
        if wntot > 0:
            avgstates[state] = totstates[state] / wntot
        else:
            avgstates[state] = ''
        allstate = {'name': state}
        if wntot > 0:
            allstate['count'] = "%0.2f" % (int(totstates[state]) / wntot)
            allstated[state] = "%0.2f" % (int(totstates[state]) / wntot)
        else:
            allstate['count'] = ''
            allstated[state] = ''
        avgwns['statelist'].append(allstate)
        avgwns['pctfail'] = allwns['pctfail']
    if wnname == 'all':
        fullsummary.append(avgwns)

    for wn in wnkeys:
        outlier = ''
        wns[wn]['slotcount'] = len(wns[wn]['slotd'])
        wns[wn]['rminramcount'] = round(wns[wn]['rminramcount']*1.0/1000, 2)
        wns[wn]['pctfail'] = 0
        for state in const.JOB_STATES_SITE:
            wns[wn]['statelist'].append(wns[wn]['states'][state])
        if wns[wn]['states']['finished']['count'] + wns[wn]['states']['failed']['count'] > 0:
            wns[wn]['pctfail'] = int(100. * float(wns[wn]['states']['failed']['count']) / (
            wns[wn]['states']['finished']['count'] + wns[wn]['states']['failed']['count']))
        if float(wns[wn]['states']['finished']['count']) < float(avgstates['finished']) / 5.:
            outlier += " LowFinished "
        if float(wns[wn]['states']['failed']['count']) > max(float(avgstates['failed']) * 3., 5.):
            outlier += " HighFailed "
        wns[wn]['outlier'] = outlier
        fullsummary.append(wns[wn])

    plots_data = {'finished': wnPlotFinished, 'failed': wnPlotFailed}

    return fullsummary, plots_data
