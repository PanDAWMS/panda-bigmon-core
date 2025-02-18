"""

"""

import logging
from datetime import datetime

from django.views.decorators.cache import never_cache
from django.http import JsonResponse
from django.shortcuts import render
from django.core.cache import cache
from core.views import initRequest, setupView
from core.utils import error_response
from core.oauth.utils import login_customrequired
from core.reports.models import ReportEmails
from core.reports.sendMail import send_mail_bp
from core.reports import ObsoletedTasksReport, LargeScaleAthenaTestsReport, ErrorClassificationReport, TasksRatedReport

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

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
            'obstasks': {
                'value': 'obstasks', 'name': 'Obsoleted tasks',
                'params': {
                    'delivery_options': ['page', ],
                    'get_redirect': [],
                }
            },
            'lsat': {
                'value': 'lsat', 'name': 'Large-scale Athena test',
                'params': {
                    'delivery_options': ['page', 'json', 'email', 'export'],
                    'get_redirect': ['jeditaskid', ],
                }
            },
            'error_classification': {
                'value': 'error_classification', 'name': 'Error classification',
                'params': {
                    'delivery_options': ['json'],
                    'get_redirect': ['hours', ],
                }
            }
        }
        if request.user.groups.filter(name='atlas-adc-dpa').exists():
            available_reports['rated_tasks'] = {
                'value': 'rated_tasks', 'name': 'Rated tasks',
                'params': {
                    'delivery_options': ['page', 'email'],
                    'get_redirect': ['days', 'rating_threshold'],
                }
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

    if 'report_type' in request.session['requestParams'] and request.session['requestParams']['report_type']:
        report_type = request.session['requestParams']['report_type']
    else:
        return JsonResponse({'status': 'error', 'message': 'No report_type specified'})

    delivery = ''
    if 'delivery' in request.session['requestParams'] and request.session['requestParams']['delivery']:
        delivery = request.session['requestParams']['delivery']

    if delivery == 'email':
        recipients = []
        if 'email' in request.session['requestParams'] and request.session['requestParams']['email']:
            recipients.append(request.session['requestParams']['email'])
        elif 'egroup' in request.session['requestParams'] and request.session['requestParams']['egroup'] == 'default':
            recipients.extend(ReportEmails.objects.filter(report=report_type, type='all').values_list('email', flat=True))
        else:
            return JsonResponse({'status': 'error', 'message': 'No recipient email specified'})
        if 'cc_email_list' in request.session['requestParams'] and request.session['requestParams']['cc_email_list']:
            for cc in request.session['requestParams']['cc_email_list']:
                if 'email' in cc and cc['email'] and isinstance(cc['email'], str) and len(cc['email']) > 0:
                    recipients.append(cc['email'])
    else:
        recipients = []

    if report_type == 'obstasks':
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

    elif report_type == 'error_classification':

        query = setupView(request, querytype='job', wildCardExt=False)

        report = ErrorClassificationReport.ErrorClassificationReport(query=query)
        report_data = report.prepare_report()
        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
        }
        data.update(report_data)

        if delivery == 'page':
            response = render(request, 'reportErrorClassification.html', data)
        elif delivery == 'json':
            response = JsonResponse(report_data)
        else:
            response = JsonResponse({'status': 'error', 'message': 'Unsupported delivery type'})
        return response

    elif report_type == 'rated_tasks':
        if 'days' in request.session['requestParams'] and request.session['requestParams']['days']:
            days = int(request.session['requestParams']['days'])
        else:
            days = 7
        if 'rating_threshold' in request.session['requestParams'] and request.session['requestParams']['rating_threshold']:
            rating_threshold = int(request.session['requestParams']['rating_threshold'])
        else:
            rating_threshold = 3

        report = TasksRatedReport.TasksRatedReport(days=days, rating_threshold=rating_threshold)

        if delivery == 'page':
            if not request.user.groups.filter(name='atlas-adc-dpa').exists():
                return error_response(request, "You are not allowed to view this report", status=403)
            ratings = report.prepare_data_page()
            data = {
                'request': request,
                'requestParams': request.session['requestParams'],
                'viewParams': request.session['viewParams'],
                'ratings': ratings,
                'built': datetime.now().strftime("%H:%M:%S"),
            }
            return render(request, 'reportRatedTasks.html', data)
        elif delivery == 'email':
            data_summary = report.prepare_data_email()
            result = {}
            do_submit = True
            cache_key = None
            # if tge report was sent to the default egroup, do not allow to send it again until the next week
            if 'egroup' in request.session['requestParams'] and request.session['requestParams']['egroup'] == 'default':
                cache_key = f"mail_sent_flag_{report_type}_default_egroup"
                if cache.get(cache_key, False):
                    do_submit = False
            if do_submit:
                is_success = send_mail_bp(
                    'templated_email/taskRatedReport.html',
                    '{} Task Ratings Report'.format(settings.EMAIL_SUBJECT_PREFIX),
                    data_summary,
                    recipients,
                    send_html=True
                )
            else:
                is_success = False
            if is_success:
                result['status'] = 'success'
                result['message'] = 'The report is sent successfully!'
                if cache_key:
                    cache.set(cache_key, "1", days * 24 * 3600 - 3600)
            else:
                result['status'] = 'error'
                result['message'] = 'Failed to send the report!'
                if cache_key:
                    result['status'] = 'error'
                    result['message'] = 'This report has already been sent!'
            response = JsonResponse(result)
        else:
            response = JsonResponse({'status': 'error', 'message': 'Unsupported delivery type'})
        return response
    else:
        return JsonResponse({'status': 'error', 'message': 'No report is available for provided parameters'})

    return JsonResponse({'status': 'error', 'message': 'Something went wrong'})

# @csrf_exempt
def send_report(request):
    """
    Send message in email
    :param request:
    :return:
    """
    valid, response = initRequest(request)
    if not valid:
        return response

    if 'remote' in request.session and request.session['remote'] in settings.CACHING_CRAWLER_HOSTS:
        _logger.debug('Request came from a cache crawler node, all is good')
    else:
        response = JsonResponse({'message': 'Bad request'}, status=400)
        return response

    if 'subject' in request.session['requestParams']:
        subject = request.session['requestParams']['subject']
    else:
        subject = 'Unidentified report'
    subject = f'[{settings.EMAIL_SUBJECT_PREFIX}] {subject}'


    if 'message' in request.session['requestParams'] and len(request.session['requestParams']['message']) > 0:
        message = request.session['requestParams']['message']
    else:
        response = JsonResponse({'message': 'Bad request'}, status=400)
        return response

    try:
        send_mail_bp(
            template='templated_email/error_alert.html',
            subject=subject,
            summary={'message': message},
            recipient=settings.ADMINS[0][1],
        )
    except Exception as ex:
        response = JsonResponse({'message': 'Error occured, developers were notified'}, status=500)
        return response

    response = JsonResponse({'message': 'Successfully sent'}, status=200)
    return response




