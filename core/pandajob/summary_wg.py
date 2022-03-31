"""
A set of functions to get jobs from JOBS* and group them by working group
"""
import logging

from django.db.models import Count

from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def wg_summary(query):

    # get data
    wgsummarydata = wg_summary_data(query)

    # group jobs by status
    wgs = {}
    for rec in wgsummarydata:
        wg = rec['workinggroup']
        if wg is None:
            continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if wg not in wgs:
            wgs[wg] = {}
            wgs[wg]['name'] = wg
            wgs[wg]['count'] = 0
            wgs[wg]['states'] = {}
            wgs[wg]['statelist'] = []
            for state in const.JOB_STATES:
                wgs[wg]['states'][state] = {}
                wgs[wg]['states'][state]['name'] = state
                wgs[wg]['states'][state]['count'] = 0
        wgs[wg]['count'] += count
        wgs[wg]['states'][jobstatus]['count'] += count

    # Convert dict to summary list
    wgkeys = wgs.keys()
    wgkeys = sorted(wgkeys)
    wgsummary = []
    for wg in wgkeys:
        for state in const.JOB_STATES:
            wgs[wg]['statelist'].append(wgs[wg]['states'][state])
            if int(wgs[wg]['states']['finished']['count']) + int(wgs[wg]['states']['failed']['count']) > 0:
                wgs[wg]['pctfail'] = int(100. * float(wgs[wg]['states']['failed']['count']) / (
                wgs[wg]['states']['finished']['count'] + wgs[wg]['states']['failed']['count']))
        wgsummary.append(wgs[wg])

    if len(wgsummary) == 0:
        wgsummary = None

    return wgsummary


def wg_summary_data(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    summary.extend(
        Jobsdefined4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobsactive4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobswaiting4.objects.filter(**querynotime).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(
        Jobsarchived4.objects.filter(**query).values('workinggroup', 'jobstatus').annotate(Count('jobstatus')))
    return summary