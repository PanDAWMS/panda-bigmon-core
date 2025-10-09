import logging
import ipaddress
import re
import psutil
import subprocess
from datetime import timedelta

from django.utils import timezone
from django.utils.deprecation import MiddlewareMixin
from django.db.models import Count
from django.db import connection, DatabaseError

from core.common.models import AllRequests
from core.utils import error_response

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

# We postpone JSON requests if server is overloaded
# Done for protection from a bunch of requests for JSON output


class TrafficControlMiddleware(object):
    """
    - Stores requests data to DB
    - Rejects requests with 429 in case of overload from the same client or address
    - Uses settings.TRAFFIC_CONTROL_ACTIVATE to activate/deactivate
    - Uses settings.TRAFFIC_CONTROL_WHITE_LIST to exclude IPs from checking
    - Uses settings.TRAFFIC_CONTROL_BLACK_LIST to always reject IPs
    - Uses settings.MAX_REQUESTS_PER_HOUR to set max allowed requests per hour from the
    """

    EXCEPTED_VIEWS = [
        '/grafana/img/',
        '/payloadlog/',
        '/statpixel/',
        '/idds/getiddsfortask/',
        '/api/dc/staginginfofortask/',
        '/art/',
        '/art/overview/',
        '/art/tasks/',
        '/art/registerarttest/',
        '/art/uploadtestresult/',
    ]
    USERAGENT_PARALLEL_LIMIT_APPLIED = ["EI-monitor/0.0.1",]
    MAX_ALLOWED_PARALLEL_REQUESTS = 2

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):

        hostname = subprocess.getoutput('hostname') or request.META.get('HTTP_HOST', '')
        x_referer = request.META.get('HTTP_REFERER', '')
        useragent = request.META.get('HTTP_USER_AGENT', '')

        # check if remote is a valid IP address
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for is None:
            x_forwarded_for = request.META.get('REMOTE_ADDR')  # in case one server config
        if x_forwarded_for is not None:
            try:
                ips_found = re.findall(
                    r'((?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{1,4})|((?:[0-9]{1,3}\.){3}[0-9])',
                    x_forwarded_for
                )
                ip = [i for i in ips_found[0] if i][0]  # filter out empty values
                ipaddress.ip_address(ip)
            except Exception as ex:
                _logger.info(f'Request HTTP_X_FORWARDED_FOR={x_forwarded_for} is not a valid IP address. \n{ex}')
                return error_response(request, message='Request remote address is wrong', status=400)

        # get incremented id for request and store its data to DB
        if settings.DEPLOYMENT == 'POSTGRES':
            sql_query_str = f"SELECT nextval('{settings.DB_SCHEMA}.\"all_requests_seq\"') as my_req_token;"
        else:
            sql_query_str = f"SELECT {settings.DB_SCHEMA}.ALL_REQUESTS_SEQ.NEXTVAL as my_req_token FROM dual;"

        cursor = connection.cursor()
        cursor.execute(sql_query_str)
        request_token = cursor.fetchall()
        request_token = request_token[0][0]
        cursor.close()

        reqs = AllRequests(
            id=request_token,
            server=hostname,
            remote=x_forwarded_for,
            qtime=timezone.now(),
            url=request.META.get('QUERY_STRING'),
            urlview=request.path,
            referrer=x_referer[:3900] if x_referer and len(x_referer) > 3900 else x_referer,
            useragent=request.META.get('HTTP_USER_AGENT'),
            is_rejected=0,
            load=psutil.cpu_percent(interval=1),
            mem=psutil.virtual_memory().percent,
            dbtotalsess=0,
            dbactivesess=0
        )
        try:
            reqs.save()
        except DatabaseError as ex:
            _logger.exception("Rejecting request since failed to save metadata to DB table because of \n{}".format(ex))
            return error_response(request, message='Rejected due to DB issue', status=400)

        # log full request path with id
        try:
            _logger.info(f'Request {request_token}: {request.get_full_path()}')
        except Exception as ex:
            _logger.info(f'Request {request_token}: ???, failed get full path of request: {ex}')

        # do not check requests from excepted views
        if settings.TRAFFIC_CONTROL_ENABLED and request.path not in self.EXCEPTED_VIEWS:

            # reject requests from blacklisted IPs
            if x_forwarded_for is not None and x_forwarded_for in settings.TRAFFIC_CONTROL_BLACK_LIST:
                reqs.is_rejected = 1
                reqs.save()
                _logger.info(f'Reject request {request_token} from {x_forwarded_for} with 403')
                return error_response(request, message='You are blacklisted, access denied', status=403)

            # restrict number of parallel requests for some user agents
            if useragent is not None and useragent in self.USERAGENT_PARALLEL_LIMIT_APPLIED:
                _logger.info('Checking request from agent: {}'.format(useragent))
                query = {
                    'qtime__range': [timezone.now() - timedelta(minutes=20), timezone.now()],
                    'useragent': useragent,
                    'is_rejected': 0,
                    'rtime': None,
                }
                rows = []
                rows.extend(AllRequests.objects.filter(**query).values('useragent').annotate(count=Count('useragent')))
                count_parallel_requests = rows[0]['count'] if len(rows) > 0 and 'count' in rows[0] else 0
                _logger.info(f'Found {count_parallel_requests} non rejected request for last 20 minutes')
                if count_parallel_requests > self.MAX_ALLOWED_PARALLEL_REQUESTS:
                    reqs.is_rejected = 1
                    reqs.save()
                    _logger.info(f'Reject request {request_token} from agent {useragent} with 429')
                    return error_response(request, message='you produce too many parallel requests', status=429)

            # We restrict number of requests per hour
            if x_forwarded_for is not None and x_forwarded_for not in settings.TRAFFIC_CONTROL_WHITE_LIST:
                query = {
                    'remote': x_forwarded_for,
                    'qtime__range': [timezone.now() - timedelta(hours=1), timezone.now()],
                    'is_rejected': 0,
                }
                rows = []
                rows.extend(AllRequests.objects.filter(**query).values('remote').annotate(Count('remote')))
                count_requests = rows[0]['remote__count'] if len(rows) > 0 and 'remote__count' in rows[0] else 0
                if count_requests > settings.TRAFFIC_CONTROL_MAX_REQUESTS_PER_HOUR:
                    reqs.is_rejected = 1
                    reqs.save()
                    _logger.info(f'Reject request {request_token} from {x_forwarded_for} with 429')
                    return error_response(request, message='too many requests per hour, please try later', status=429)

        response = self.get_response(request)
        reqs.rtime = timezone.now()
        reqs.save()
        return response



class RequestLoggingMiddleware(MiddlewareMixin):
    def process_exception(self, request, exception):
        _logger.error(
            f"Error occurred: {str(exception)}",
            exc_info=True,
            extra={'request_path': request.path, 'request_get': request.GET}
        )