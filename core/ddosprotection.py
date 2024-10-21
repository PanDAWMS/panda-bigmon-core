import logging
import ipaddress
import re
import json
import psutil
import subprocess
from datetime import timedelta

from django.utils import timezone
from django.db.models import Count
from django.db import connection
from django.http import HttpResponse
from django.db import DatabaseError

from core.common.models import AllRequests
from core.utils import is_json_request

from django.conf import settings

_logger = logging.getLogger('bigpandamon')

# We postpone JSON requests if server is overloaded
# Done for protection from a bunch of requests for JSON output


class DDOSMiddleware(object):

    sleepInterval = 5  # sec

    max_allowed_requests_per_hour = 600
    notcachedRemoteAddress = [
        '188.184.185.129', '188.185.80.72', '188.184.116.46', '188.184.28.86', '144.206.131.154',
        '188.184.90.172'  # J..h M......n request
    ]
    excepted_views = [
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
    blacklist = ['130.132.21.90', '192.170.227.149']

    useragent_parallel_limit_applied = ["EI-monitor/0.0.1",]
    max_allowed_parallel_requests = 2
    listOfServerBackendNodesIPs = settings.BIGMON_BACKEND_NODES_IP_LIST

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            _logger.info('Request: {}'.format(request.get_full_path()))
        except:
            _logger.exception('Can not get full path of request')

        # we limit number of requests per hour for a set of IPs
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
                ip = ipaddress.ip_address(ip)
            except:
                _logger.warning('Provided HTTP_X_FORWARDED_FOR={} is not a correct IP address.'.format(x_forwarded_for))
                return HttpResponse(
                    json.dumps({'message': 'provided remote address is wrong'}),
                    status=400,
                    content_type='application/json')

        try:
            x_referer = request.META.get('HTTP_REFERER')
        except:
            x_referer = ''

        try:
            hostname = subprocess.getoutput('hostname')
        except:
            hostname = request.META.get('HTTP_HOST')

        try:
            useragent = request.META.get('HTTP_USER_AGENT')
        except:
            useragent = None

        cursor = connection.cursor()
        # get number of sessions
        dbtotalsess, dbactivesess = 0, 0
        # if settings.DEPLOYMENT == 'ATLAS_ORACLE':
            # try:
            #     cursor.execute("SELECT SUM(NUM_ACTIVE_SESS), SUM(NUM_SESS) FROM ATLAS_DBA.COUNT_PANDAMON_SESSIONS")
            #     rows = cursor.fetchall()
            #     for row in rows:
            #         dbactivesess = row[0]
            #         dbtotalsess = row[1]
            #         break
            # except:
            #     _logger.warning('Failed to get connections number from ATLAS_DBA')

        if settings.DEPLOYMENT == 'POSTGRES':
            sqlRequest = f"SELECT nextval('{settings.DB_SCHEMA}.\"all_requests_seq\"') as my_req_token;"
        else:
            sqlRequest = f"SELECT {settings.DB_SCHEMA}.ALL_REQUESTS_SEQ.NEXTVAL as my_req_token FROM dual;"

        cursor.execute(sqlRequest)
        requestToken = cursor.fetchall()
        requestToken = requestToken[0][0]
        cursor.close()

        reqs = AllRequests(
            id=requestToken,
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
            dbtotalsess=dbtotalsess,
            dbactivesess=dbactivesess
        )
        try:
            reqs.save()
        except DatabaseError as ex:
            _logger.exception("Rejecting request since failed to save metadata to DB table because of \n{}".format(ex))
            return HttpResponse(json.dumps({'message': 'rejected'}), status=400, content_type='application/json')

        # do not check requests from excepted views and not JSON requests
        if request.path not in self.excepted_views and is_json_request(request):

            # Check against number of unprocessed requests to filebrowser from ART subsystem
            if request.path == '/filebrowser/' and x_forwarded_for in self.listOfServerBackendNodesIPs:
                startdate = timezone.now()- timedelta(minutes=20)
                enddate = timezone.now()
                query = {
                    'qtime__range': [startdate, enddate],
                    'is_rejected': 0,
                    'urlview': '/filebrowser/',
                    'rtime': None,
                }
                countRequest = []
                countRequest.extend(AllRequests.objects.filter(**query).values('remote').annotate(Count('urlview')))
                if len(countRequest) > 0:
                    if countRequest[0]['urlview__count'] > self.max_allowed_parallel_requests or x_forwarded_for in self.blacklist:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                            status=429,
                            content_type='application/json')
                response = self.get_response(request)
                reqs.rtime = timezone.now()
                reqs.save()
                return response

            # restrict number of parallel requests for some user agents
            if useragent is not None and useragent in self.useragent_parallel_limit_applied:
                _logger.info('[DDOS protection] got request from agent: {}'.format(useragent))
                startdate = timezone.now() - timedelta(hours=1)
                enddate = timezone.now()
                eiquery = {
                    'qtime__range': [startdate, enddate],
                    'useragent': useragent,
                    'is_rejected': 0,
                    'rtime': None,
                }
                data_raw = []
                data_raw.extend(AllRequests.objects.filter(**eiquery).values('useragent').annotate(count=Count('useragent')))
                if len(data_raw) > 0 and 'count' in data_raw[0]:
                    count_parallel_requests = data_raw[0]['count']
                    _logger.info(f'[DDOS protection] found number of non rejected request for last minute: {count_parallel_requests}')
                    if count_parallel_requests > self.max_allowed_parallel_requests:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'you produce too many parallel requests'}),
                            status=429,
                            content_type='application/json')
                response = self.get_response(request)
                reqs.rtime = timezone.now()
                reqs.save()
                return response

            # We restrict number of requests per hour
            if x_forwarded_for is not None and x_forwarded_for not in self.notcachedRemoteAddress:
                startdate = timezone.now() - timedelta(hours=1)
                enddate = timezone.now()
                query = {
                    'remote': x_forwarded_for,
                    'qtime__range': [startdate, enddate],
                    'is_rejected': 0,
                     }
                countRequest = []
                countRequest.extend(AllRequests.objects.filter(**query).values('remote').annotate(Count('remote')))

                # Check against general number of request
                if len(countRequest) > 0:
                    if countRequest[0]['remote__count'] > self.max_allowed_requests_per_hour or x_forwarded_for in self.blacklist:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                            status=429,
                            content_type='application/json')

        response = self.get_response(request)
        reqs.rtime = timezone.now()
        reqs.save()
        return response
