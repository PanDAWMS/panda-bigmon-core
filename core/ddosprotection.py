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
    notcachedRemoteAddress = ['188.184.185.129', '188.185.80.72', '188.184.116.46', '188.184.28.86']
    blacklist = ['130.132.21.90','192.170.227.149']
    maxAllowedJSONRequstesPerMinuteEI = 5

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
        cursor.close()

        reqs = AllRequests(
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

        # we limit number of requests per hour
        # temporary protection against EI monitor
        try:
            useragent = request.META.get('HTTP_USER_AGENT')
        except:
            useragent = None
            pass

        _logger.debug('[DDOS protection] got request from agent: {}'.format(useragent))
        if useragent and 'EI-monitor' in useragent:
            countEIrequests = []
            startdate = datetime.utcnow() - timedelta(minutes=1)
            enddate = datetime.utcnow()
            eiquery = {
                'qtime__range': [startdate, enddate],
                'useragent': useragent,
                'is_rejected': 0,
            }
            countEIrequests.extend(
                AllRequests.objects.filter(**eiquery).values('remote').exclude(urlview='/grafana/').annotate(
                    Count('remote')))
            if len(countEIrequests) > 0:
                _logger.debug('[DDOS protection] checked number of non rejected request for last minute: {}'.format(countEIrequests[0]['remote__count']))
                if countEIrequests[0]['remote__count'] > self.maxAllowedJSONRequstesPerMinuteEI:
                    reqs.is_rejected = 1
                    reqs.save()
                    return HttpResponse(
                        json.dumps({'message': 'your IP produces too many requests per hour, please try later'}),
                        status=429,
                        content_type='application/json')


        #if ('json' in request.GET):
        if (not x_forwarded_for is None) and x_forwarded_for not in self.notcachedRemoteAddress:
#                x_forwarded_for = '141.108.38.22'
            startdate = timezone.now() - timedelta(hours=2)
            enddate = timezone.now()
            query = {'remote':x_forwarded_for,
                     'qtime__range': [startdate, enddate],
                     }
            countRequest = []
            countRequest.extend(AllRequests.objects.filter(**query).values('remote').exclude(urlview='/grafana/').annotate(Count('remote')))
            if len(countRequest) > 0:
                if countRequest[0]['remote__count'] > self.maxAllowedJSONRequstesPerHour or x_forwarded_for in self.blacklist:
                    reqs.is_rejected = 1
                    reqs.save()
                    return HttpResponse(json.dumps({'message':'your IP produces too many requests per hour, please try later'}), status=429, content_type='application/json')

        response = self.get_response(request)
        reqs.rtime = timezone.now()
        reqs.save()
        return response



