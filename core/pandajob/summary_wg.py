"""
A set of functions to get jobs from JOBS* and group them by working group
"""
import logging

from django.db.models import Count

from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4
import core.constants as const

_logger = logging.getLogger('bigpandamon')


def wgTaskSummary(request, fieldname='workinggroup', view='production', taskdays=3):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    query = {}
    hours = 24 * taskdays
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query['modificationtime__castdate__range'] = [startdate, enddate]
    if fieldname == 'workinggroup': query['workinggroup__isnull'] = False
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
    for state in taskstatelist:
        totstates[state] = 0
    for rec in summary:
        wg = rec[fieldname]
        status = rec['status']
        count = rec['status__count']
        if status not in taskstatelist: continue
        tottasks += count
        totstates[status] += count
        if wg not in wgsum:
            wgsum[wg] = {}
            wgsum[wg]['name'] = wg
            wgsum[wg]['count'] = 0
            wgsum[wg]['states'] = {}
            wgsum[wg]['statelist'] = []
            for state in taskstatelist:
                wgsum[wg]['states'][state] = {}
                wgsum[wg]['states'][state]['name'] = state
                wgsum[wg]['states'][state]['count'] = 0
        wgsum[wg]['count'] += count
        wgsum[wg]['states'][status]['count'] += count

    ## convert to ordered lists
    suml = []
    for f in wgsum:
        itemd = {}
        itemd['field'] = f
        itemd['count'] = wgsum[f]['count']
        kys = taskstatelist
        iteml = []
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': wgsum[f]['states'][ky]['count']})
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


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