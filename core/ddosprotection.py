import logging
from core.common.models import AllRequests

from django.utils import timezone
from datetime import timedelta, datetime
from django.db.models import Count, Sum
from django.http import HttpResponse
import json
import psutil
from django.db import connection

_logger = logging.getLogger('bigpandamon')
# We postpone JSON requests is server is overloaded
# Done for protection from bunch of requests from JSON


class DDOSMiddleware(object):

    sleepInterval = 5 #sec
    maxAllowedJSONRequstesPerHour = 400
    notcachedRemoteAddress = ['188.184.185.129', '188.185.80.72', '188.184.116.46', '188.184.28.86',
                              '188.184.90.172' # J..h M......n request
                              ]
    blacklist = ['130.132.21.90','192.170.227.149']
    maxAllowedJSONRequstesParallel = 1
    maxAllowedSimultaneousRequestsToFileBrowser = 5
    listOfServerBackendNodesIPs = ['188.184.93.101', '188.184.116.46', '188.184.104.150',
                                   '188.184.84.149', '188.184.108.134', '188.184.108.131']

    restrictedIPs = ['137.138.77.2', # Incident on 13-01-2020 14:30:00
                     '188.185.76.164', #EI Machine
                     '147.156.116.63', #EI Machine
                     '147.156.116.43', #EI Machine
                     '147.156.116.44', #EI Machine
                     '147.156.116.81', #EI Machine
                     '147.156.116.83', #EI Machine
                     ]

    def __init__(self, get_response):
        self.get_response = get_response


    def __call__(self, request):

        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        try:
            x_referer = request.META.get('HTTP_REFERER')
        except:
            x_referer = ''
        dbtotalsess, dbactivesess = 0, 0
        cursor = connection.cursor()
        cursor.execute("SELECT SUM(NUM_ACTIVE_SESS), SUM(NUM_SESS) FROM ATLAS_DBA.COUNT_PANDAMON_SESSIONS")
        rows = cursor.fetchall()
        for row in rows:
            dbactivesess = row[0]
            dbtotalsess = row[1]
            break

        sqlRequest = "SELECT ATLAS_PANDABIGMON.ALL_REQUESTS_SEQ.NEXTVAL as my_req_token FROM dual;"
        cursor.execute(sqlRequest)
        requestToken = cursor.fetchall()
        requestToken = requestToken[0][0]
        cursor.close()

        reqs = AllRequests(
            id = requestToken,
            server = request.META.get('HTTP_HOST'),
            remote = x_forwarded_for,
            qtime = timezone.now(),
            url= request.META.get('QUERY_STRING'),
            urlview = request.path,
            referrer = x_referer[:3900] if x_referer and len(x_referer) > 3900 else x_referer,
            useragent = request.META.get('HTTP_USER_AGENT'),
            is_rejected = 0,
            load=psutil.cpu_percent(interval=1),
            mem = psutil.virtual_memory().percent,
            dbtotalsess = dbtotalsess,
            dbactivesess = dbactivesess
        )
        reqs.save()


        # we limit number of requests per hour for a set of IPs
        try:
            useragent = request.META.get('HTTP_USER_AGENT')
        except:
            useragent = None

        if (not x_forwarded_for is None) and x_forwarded_for in self.restrictedIPs:
            _logger.debug('[DDOS protection] got request from agent: {}'.format(useragent))
            countRestictedrequests = []
            startdate = datetime.utcnow() - timedelta(hours=1)
            enddate = datetime.utcnow()
            eiquery = {
                'qtime__range': [startdate, enddate],
                'remote':x_forwarded_for,
                'is_rejected': 0,
                'rtime': None,
            }
            countRestictedrequests.extend(
                AllRequests.objects.filter(**eiquery).values('remote').exclude(urlview='/grafana/').annotate(
                    Count('remote')))
            if len(countRestictedrequests) > 0 and 'remote__count' in countRestictedrequests[0]:
                _logger.debug('[DDOS protection] found number of non rejected request for last minute: {}'.format(countRestictedrequests[0]['remote__count']))
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


        #We restrinct number of requets per hour
        if (not x_forwarded_for is None) and x_forwarded_for not in self.notcachedRemoteAddress:
                # x_forwarded_for = '141.108.38.22'
            startdate = datetime.utcnow() - timedelta(hours=1)
            enddate = datetime.utcnow()
            query = {
                'remote':x_forwarded_for,
                'qtime__range': [startdate, enddate],
                'is_rejected': 0,
                 }
            countRequest = []
            countRequest.extend(AllRequests.objects.filter(**query).values('remote').exclude(urlview='/grafana/').annotate(Count('remote')))

            #Check against general number of request
            if len(countRequest) > 0:
                if countRequest[0]['remote__count'] > self.maxAllowedJSONRequstesPerHour or x_forwarded_for in self.blacklist:
                    reqs.is_rejected = 1
                    reqs.save()
                    return HttpResponse(json.dumps({'message':'your IP produces too many requests per hour, please try later'}), status=429, content_type='application/json')


        #Check against number of unprocessed requests to filebrowser from ART subsystem
        #if 1==1:
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
                        status=429, content_type='application/json')

        response = self.get_response(request)
        reqs.rtime = datetime.utcnow()
        reqs.save()
        return response



