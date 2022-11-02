"""

"""
import json
from datetime import datetime

from django.views.decorators.cache import never_cache
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from core.libs.DateEncoder import DateEncoder
from core.views import initRequest
from core.oauth.utils import login_customrequired
from core.reports.sendMail import send_mail_bp

from core.reports import MC16aCPReport, ObsoletedTasksReport, LargeScaleAthenaTestsReport

from django.conf import settings


@login_customrequired
def reports(request):
    """
    Reports wizard -> a form to select one of available reports and proceed with it
    :param request:
    :return: http response
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    available_reports = {}
    if 'ATLAS' in settings.DEPLOYMENT:
        available_reports = {
            # 'campaign': {
            #     'value': 'campaign', 'name': 'Campaign summary',
            #     'params': {
            #         'values': ['MC16', 'MC16A', 'MC16C', ],
            #         'delivery_options': ['page', 'email', ],
            #         'get_redirect': ['campaign', ],
            #     }
            # },
            'obstasks': {
                'value': 'obstasks', 'name': 'Obsoleted tasks',
                'params': {
                    'delivery_options': ['page', ]
                }
            },
            'lsat': {
                'value': 'lsat', 'name': 'Large-scale Athena test',
                'params': {
                    'delivery_options': ['page', 'json', 'email', 'export'],
                    'get_redirect': ['jeditaskid', ],
                }
            },
        }

    data = {
        'request': request,
        'requestParams': request.session['requestParams'],
        'viewParams': request.session['viewParams'],
        'available_reports': available_reports,
    }
    response = render(request, 'reportWizard.html', data)
    return response


@never_cache
def report(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    step = 0

    if 'report_type' in request.session['requestParams'] and request.session['requestParams']['report_type']:
        report_type = request.session['requestParams']['report_type']
    else:
        return JsonResponse({'status': 'error', 'message': 'No report_type specified'})

    delivery = ''
    if 'delivery' in request.session['requestParams'] and request.session['requestParams']['delivery']:
        delivery = request.session['requestParams']['delivery']

    if report_type == 'campaign':
        if 'campaign' in request.session['requestParams'] and request.session['requestParams']['campaign']:
            campaign = request.session['requestParams']['campaign']
        else:
            return JsonResponse({'status': 'error', 'message': 'No campaign specified'})

        if campaign.upper() == 'MC16':
            reportGen = MC16aCPReport.MC16aCPReport()
            response = reportGen.prepareReportJEDI(request)
            return response
        elif campaign.upper() == 'MC16C':
            reportGen = MC16aCPReport.MC16aCPReport()
            response = reportGen.prepareReportJEDIMC16c(request)
            return response
        elif campaign.upper() == 'MC16A':
            reportGen = MC16aCPReport.MC16aCPReport()
            resp = reportGen.getDKBEventsSummaryRequestedBreakDownHashTag(request)
            dump = json.dumps(resp, cls=DateEncoder)
            return HttpResponse(dump, content_type='application/json')
    elif report_type == 'obstasks':
        reportGen = ObsoletedTasksReport.ObsoletedTasksReport()
        response = reportGen.prepareReport(request)
        return response
    elif report_type == 'lsat':
        if 'jeditaskid' in request.session['requestParams'] and request.session['requestParams']['jeditaskid']:
            jeditaskid = request.session['requestParams']['jeditaskid']
        else:
            return JsonResponse({'status': 'error', 'message': 'No jeditaskid specified'})
        jeditaskid_str_list = jeditaskid.split(',')
        jeditaskid_list = []
        for tid_str in jeditaskid_str_list:
            try:
                tid = int(tid_str)
            except:
                return JsonResponse({'status': 'error',
                                     'message': 'At least one of provided jeditaskid is not valid, must be integer'})
            jeditaskid_list.append(tid)
        report_lsat = LargeScaleAthenaTestsReport.LargeScaleAthenaTestsReport(jeditaskid_list)
        message = report_lsat.collect_data()
        if len(message) > 0:
            return JsonResponse({'status': 'error', 'message': message})

        if delivery == 'page':
            data = {
                'summary': list(report_lsat.data.values()),
                'delivery': delivery,
                'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'message': message,
            }
            response = render(request, 'templated_email/reportLargeScaleAthenaTest.html', data)
        elif delivery == 'json':
            response = JsonResponse(report_lsat.data)
        elif delivery == 'export':
            result = report_lsat.export_data()
            response = JsonResponse(result)
        elif delivery == 'email':
            recipients = []
            if 'email' in request.session['requestParams'] and request.session['requestParams']['email']:
                recipients.append(request.session['requestParams']['email'])
            else:
                return JsonResponse({'status': 'error', 'message': 'No recipient email specified'})
            if 'cc_email_list' in request.session['requestParams'] and request.session['requestParams']['cc_email_list']:
                for cc in request.session['requestParams']['cc_email_list']:
                    if 'email' in cc and cc['email'] and isinstance(cc['email'], str) and len(cc['email']) > 0:
                        recipients.append(cc['email'])
            is_success = send_mail_bp(
                'templated_email/reportLargeScaleAthenaTest.html',
                '{} Report on large-scale Athena test(s) {}'.format(settings.EMAIL_SUBJECT_PREFIX, jeditaskid),
                list(report_lsat.data.values()),
                recipients,
                send_html=True
            )
            result = {}
            if is_success:
                result['status'] = 'success'
                result['message'] = 'The report is sent successfully!'
            else:
                result['status'] = 'error'
                result['message'] = 'Failed to send the report!'
            response = JsonResponse(result)
        else:
            response = JsonResponse({'status': 'error', 'message': 'Unsupported delivery type'})
        return response

    else:
        return JsonResponse({'status': 'error', 'message': 'No report is available for provided parameters'})

    return JsonResponse({'status': 'error', 'message': 'Something went wrong'})
