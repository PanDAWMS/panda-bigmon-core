"""

"""
import json

from django.views.decorators.cache import never_cache
from django.http import HttpResponse
from django.shortcuts import render

from core.views import initRequest, DateEncoder

from core.reports import MC16aCPReport, ObsoletedTasksReport, TitanProgressReport


@never_cache
def report(request):
    initRequest(request)
    step = 0
    response = None

    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16':
        reportGen = MC16aCPReport.MC16aCPReport()
        response = reportGen.prepareReportJEDI(request)
        return response

    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16C':
        reportGen = MC16aCPReport.MC16aCPReport()
        response = reportGen.prepareReportJEDIMC16c(request)
        return response


    if 'requestParams' in request.session and 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign'].upper() == 'MC16A' and 'type' in request.session['requestParams'] and request.session['requestParams']['type'].upper() == 'DCC':
        reportGen = MC16aCPReport.MC16aCPReport()
        resp = reportGen.getDKBEventsSummaryRequestedBreakDownHashTag(request)
        dump = json.dumps(resp, cls=DateEncoder)
        return HttpResponse(dump, content_type='application/json')


    if 'requestParams' in request.session and 'obstasks' in request.session['requestParams']:
        reportGen = ObsoletedTasksReport.ObsoletedTasksReport()
        response = reportGen.prepareReport(request)
        return response

    if 'requestParams' in request.session and 'titanreport' in request.session['requestParams']:
        reportGen = TitanProgressReport.TitanProgressReport()
        response = reportGen.prepareReport(request)
        return response

    if 'requestParams' in request.session and 'step' in request.session['requestParams']:
        step = int(request.session['requestParams']['step'])
    if step == 0:
        response = render(request, 'reportWizard.html', {'nevents': 0}, content_type='text/html')
    else:
        if 'reporttype' in request.session['requestParams'] and request.session['requestParams']['reporttype'] == 'rep0':
            reportGen = MC16aCPReport.MC16aCPReport()
            response = reportGen.prepareReport()
    return response