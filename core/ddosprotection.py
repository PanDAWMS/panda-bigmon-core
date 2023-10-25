import logging
import ipaddress
import re
import json
import psutil
import subprocess
from datetime import timedelta, datetime

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
    maxAllowedJSONRequstesPerHour = 600
    notcachedRemoteAddress = [
        '188.184.185.129', '188.185.80.72', '188.184.116.46', '188.184.28.86', '144.206.131.154',
        '188.184.90.172'  # J..h M......n request
    ]
    excepted_views = [
        '/grafana/img/', '/payloadlog/', '/statpixel/', '/idds/getiddsfortask/', '/api/dc/staginginfofortask/',
        '/art/tasks/', '/art/overview/'
    ]
    blacklist = ['130.132.21.90', '192.170.227.149']
    maxAllowedJSONRequstesParallel = 1
    maxAllowedSimultaneousRequestsToFileBrowser = 1
    listOfServerBackendNodesIPs = settings.BIGMON_BACKEND_NODES_IP_LIST

    restrictedIPs = ['137.138.77.2',  # Incident on 13-01-2020 14:30:00
                     '188.185.76.164',  # EI Machine
                     '147.156.116.63',  # EI Machine
                     '147.156.116.43',  # EI Machine
                     '147.156.116.44',  # EI Machine
                     '147.156.116.81',  # EI Machine
                     '147.156.116.83',  # EI Machine
                     ]

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
                startdate = datetime.utcnow() - timedelta(minutes=20)
                enddate = datetime.utcnow()
                query = {
                    'qtime__range': [startdate, enddate],
                    'is_rejected': 0,
                    'urlview': '/filebrowser/',
                    'rtime': None,
                }
                countRequest = []
                countRequest.extend(AllRequests.objects.filter(**query).values('remote').annotate(Count('urlview')))
                if len(countRequest) > 0:
                    if countRequest[0]['urlview__count'] > self.maxAllowedSimultaneousRequestsToFileBrowser or x_forwarded_for in self.blacklist:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                            status=429,
                            content_type='application/json')
                response = self.get_response(request)
                reqs.rtime = datetime.utcnow()
                reqs.save()
                return response

            if x_forwarded_for is not None and x_forwarded_for in self.restrictedIPs:
                _logger.info('[DDOS protection] got request from agent: {}'.format(useragent))
                countRestictedrequests = []
                startdate = datetime.utcnow() - timedelta(hours=1)
                enddate = datetime.utcnow()
                eiquery = {
                    'qtime__range': [startdate, enddate],
                    'remote': x_forwarded_for,
                    'is_rejected': 0,
                    'rtime': None,
                }
                countRestictedrequests.extend(
                    AllRequests.objects.filter(**eiquery).values('remote').annotate(Count('remote')))
                if len(countRestictedrequests) > 0 and 'remote__count' in countRestictedrequests[0]:
                    _logger.info('[DDOS protection] found number of non rejected request for last minute: {}'.format(
                        countRestictedrequests[0]['remote__count']))
                    if countRestictedrequests[0]['remote__count'] > self.maxAllowedJSONRequstesParallel:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                            status=429,
                            content_type='application/json')
                response = self.get_response(request)
                reqs.rtime = datetime.utcnow()
                reqs.save()
                return response

            # We restrict number of requests per hour
            if x_forwarded_for is not None and x_forwarded_for not in self.notcachedRemoteAddress:
                startdate = datetime.utcnow() - timedelta(hours=1)
                enddate = datetime.utcnow()
                query = {
                    'remote': x_forwarded_for,
                    'qtime__range': [startdate, enddate],
                    'is_rejected': 0,
                     }
                countRequest = []
                countRequest.extend(AllRequests.objects.filter(**query).values('remote').annotate(Count('remote')))

                # Check against general number of request
                if len(countRequest) > 0:
                    if countRequest[0]['remote__count'] > self.maxAllowedJSONRequstesPerHour or x_forwarded_for in self.blacklist:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(
                            json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                            status=429,
                            content_type='application/json')

        response = self.get_response(request)
        reqs.rtime = datetime.utcnow()
        reqs.save()
        return response
