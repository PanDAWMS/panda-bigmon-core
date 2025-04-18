import logging
import pytz

from datetime import datetime, timedelta, timezone
from json import dumps as json_dumps  ### FIXME - cleanup

import re
from django.http import HttpResponse, JsonResponse

from .models import Jobsarchived4, Jobsactive4, Jobsdefined4
from core.common.utils import getPrefix, getContextVariables, subDictToStr
from core.libs.DateEncoder import DateEncoder

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

LAST_N_DAYS = settings.FILTER_UI_ENV['DAYS']  # FIXME: put to utils
LAST_N_HOURS = settings.FILTER_UI_ENV['HOURS']  # FIXME: put to utils
LAST_N_DAYS_MAX = settings.FILTER_UI_ENV['MAXDAYS']  # FIXME: put to utils


def maxpandaid(request):
    """
        maxpandaid:
        Support view to return maxpandaid in the jobsarchived4 table.
        Helps to collect core logs when "xrdfs ls" times out.
        :param request: Django HTTP request
    """
    try:
        pandaid = Jobsarchived4.objects.all().order_by("-pandaid").values()[0]['pandaid']
    except:
        pandaid = 0
    return JsonResponse({'maxpandaid': pandaid}, safe=False)


def jobInfoOrig(request, prodUserName, nhours=LAST_N_HOURS):
    """
        jobInfoOrig:
        Support view to return json dict with jobs info for panda_core_status.
        :param request: Django HTTP request
        :param prodUserName: prodUserName from the jobs tables
        :param nhours: #hours
    """
    _logger.debug('nhours: ...%s...' % (nhours))
    try:
        nhours = int(nhours)
        if (nhours > LAST_N_DAYS_MAX * 24):
            nhours = LAST_N_DAYS_MAX * 24
    except:
        _logger.error('Something wrong with nhours:' + str(nhours))

    ### replace + by space
    _logger.debug('prodUserName: ...%s...' % (prodUserName))
    try:
        prodUserName = re.sub('\\+', ' ', prodUserName)
    except:
        pass
    _logger.debug('prodUserName: ...%s...' % (prodUserName))

    jobs = []
    job = {}
    jobKeys = ['pandaid', 'jobstatus', 'cpuconsumptiontime', 'creationtime', 'starttime', \
               'endtime', 'modificationhost', 'computingsite', 'produsername']
    datetimeJobKeys = ['creationtime', 'starttime', 'endtime']
#    try:
#        ndays = int(nhours) * 24
#    except:
#        _logger.error('Something wrong with ndays:' + str(ndays))
    try:
        startdate = datetime.now(tz=timezone.utc) - timedelta(hours=nhours)
    except:
        _logger.error('Something wrong with startdate:')
        startdate = datetime.now(tz=timezone.utc) - timedelta(hours=LAST_N_HOURS)
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = datetime.now(tz=timezone.utc).strftime(settings.DATETIME_FORMAT)
    jobs.extend(Jobsactive4.objects.filter(\
                produsername=prodUserName, \
                modificationtime__range=[startdate, enddate] \
    ).values())
    jobs.extend(Jobsdefined4.objects.filter(\
                produsername=prodUserName, \
                modificationtime__range=[startdate, enddate] \
    ).values())
    jobs.extend(Jobsarchived4.objects.filter(\
                produsername=prodUserName, \
                modificationtime__range=[startdate, enddate] \
    ).values())

    ### Handle json output
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        jobInfo = []
        for job in jobs:
            ### slim job dict
            try:
                newJob = subDictToStr(job, jobKeys, datetimeJobKeys, "%s")
            except:
                _logger.error('Something went wrong with job slimming: keys %s job %s' % (jobKeys, job))
                newJob = job
            ### append job info
            jobInfo.append(newJob)
        return  HttpResponse(json_dumps(jobInfo), content_type="application/json", mimetype='text/html')

    ### Handle other outputs
    name = prodUserName
    jobs = sorted(jobs, key=lambda x:-x['pandaid'])
    data = {
            'prefix': getPrefix(request),
            'jobInfo': jobs, 'name': name, 'nhours': nhours,
    }
    return JsonResponse(data, encoder=DateEncoder, safe=False)


def jobInfoHoursOrig(request, prodUserName, nhours=LAST_N_HOURS):
    """
        jobInfoHoursOrig:
        Wrapper for jobInfoOrig.
        Support view to return json dict with jobs info for panda_core_status.
        :param request: Django HTTP request
        :param prodUserName: prodUserName from the jobs tables
        :param nhours: #hours
    """
    return jobInfoOrig(request, prodUserName, nhours)


def jobInfoDaysOrig(request, prodUserName, nhours=LAST_N_DAYS * 24):
    """
        jobInfoDaysOrig:
        Wrapper for jobInfoOrig.
        Support view to return json dict with jobs info for panda_core_status.
        :param request: Django HTTP request
        :param prodUserName: prodUserName from the jobs tables
        :param nhours: #hours
    """
    return jobInfoOrig(request, prodUserName, nhours * 24)


def jobUserOrig(request, vo='core', nhours=LAST_N_HOURS):
    """
        jobUserOrig:
        Support view to return json dict with jobs info for panda_core_status.
        :param request: Django HTTP request
        :param vo: VO, e.g. 'core'
        :param nhours: #hours
    """
    print ('vo: ...%s...' % (vo))
    print ('nhours: ...%s...' % (nhours))
    _logger.debug('nhours: ...%s...' % (nhours))
    try:
        nhours = int(nhours)
        if (nhours > LAST_N_DAYS_MAX * 24):
            nhours = LAST_N_DAYS_MAX * 24
    except:
        _logger.error('Something wrong with nhours:' + str(nhours))

    ### replace + by space
    _logger.debug('vo: ...%s...' % (vo))
    try:
        vo = re.sub('\\+', ' ', vo)
    except:
        pass
    _logger.debug('vo: ...%s...' % (vo))

    jobs = []
    job = {}
    jobKeys = ['pandaid', 'jobstatus', 'cpuconsumptiontime', 'creationtime', 'starttime', \
               'endtime', 'modificationhost', 'computingsite', 'produsername']
    datetimeJobKeys = ['creationtime', 'starttime', 'endtime']
#    try:
#        ndays = int(nhours) * 24
#    except:
#        _logger.error('Something wrong with ndays:' + str(ndays))
    try:
        startdate = datetime.now(tz=timezone.utc) - timedelta(hours=nhours)
    except:
        _logger.error('Something wrong with startdate:')
        startdate = datetime.now(tz=timezone.utc) - timedelta(hours=LAST_N_HOURS)
    startdate = startdate.strftime(settings.DATETIME_FORMAT)
    enddate = datetime.now(tz=timezone.utc).strftime(settings.DATETIME_FORMAT)

    print ('startdate=', startdate)
    print ('enddate=', enddate)

    jobs.extend(Jobsactive4.objects.filter(\
                vo=vo, \
                creationtime__range=[startdate, enddate] \
    ).values())
    jobs.extend(Jobsdefined4.objects.filter(\
                vo=vo, \
                creationtime__range=[startdate, enddate] \
    ).values())
    jobs.extend(Jobsarchived4.objects.filter(\
                vo=vo, \
                creationtime__range=[startdate, enddate] \
    ).values())

    ### Handle json output
    if request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        jobInfo = []
        for job in jobs:
            ### slim job dict
            try:
                newJob = subDictToStr(job, jobKeys, datetimeJobKeys, "%s")
            except:
                _logger.error('Something went wrong with job slimming: keys %s job %s' % (jobKeys, job))
                newJob = job
            ### append job info
            jobInfo.append(newJob)
        return  HttpResponse(json_dumps(jobInfo), content_type="application/json", mimetype='text/html')


    ### Handle other outputs
    name = vo
    jobs = sorted(jobs, key=lambda x:-x['pandaid'])
    data = {
            'prefix': getPrefix(request),
            'jobInfo': jobs, 'name': name, 'nhours': nhours,
    }
    data.update(getContextVariables(request))
    return JsonResponse(data, encoder=DateEncoder, safe=False)


def jobUserDaysOrig(request, vo, ndays=LAST_N_DAYS):
    """
        jobUserDaysOrig:
        Wrapper for jobUserOrig.
        Support view to return json dict with jobs info for panda_core_status.
        :param request: Django HTTP request
        :param vo: VO, e.g. 'core'
        :param nhours: #hours
    """
    try:
        nhours = int(ndays) * 24
    except:
        nhours = LAST_N_DAYS * 24
    return jobUserOrig(request, vo, nhours)

