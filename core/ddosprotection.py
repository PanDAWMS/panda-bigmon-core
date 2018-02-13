import time

import django.core.exceptions
import commands
import random
from core.common.models import RequestStat, AllRequests
from django.utils import timezone
from datetime import timedelta
from django.db.models import Count, Sum
from django.http import HttpResponse
import json

# We postpone JSON requests is server is overloaded
# Done for protection from bunch of requests from JSON


class DDOSMiddleware(object):

    sleepInterval = 5 #sec
    maxAllowedJSONRequstesPerHour = 200
    notcachedRemoteAddress = ['188.184.185.129', '188.185.80.72']
    clients = ['curl','PycURL','Python-urllib']
    isForbidden = False

    def __init__(self):
        pass

    def process_request(self, request):

        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')

        reqs = AllRequests(
            server = request.META.get('HTTP_HOST'),
            remote = x_forwarded_for,
            qtime = timezone.now(),
            url= request.META.get('QUERY_STRING'),
            urlview = request.path,
            referrer = request.META.get('HTTP_REFERER'),
            useragent = request.META.get('HTTP_USER_AGENT'),
            is_rejected = 0
        )
        try:
            for client in self.clients:
                if client in request.META.get('HTTP_USER_AGENT') and ('json' not in request.GET):
                    self.isForbidden = True
                    self.maxAllowedJSONRequstesPerHour = 50
        except:
            pass

        # we limit number of requests per hour
        if ('json' in request.GET) or self.isForbidden:
            if (not x_forwarded_for is None) and x_forwarded_for not in self.notcachedRemoteAddress:
#                x_forwarded_for = '141.108.38.22'
                startdate = timezone.now() - timedelta(hours=2)
                enddate = timezone.now()
                query = {'remote':x_forwarded_for,
                         'qtime__range': [startdate, enddate],
                         'id__gte':36207430}
                countRequest = []
                countRequest.extend(RequestStat.objects.filter(**query).values('remote').annotate(Count('remote')))
                if len(countRequest) > 0:
                    if countRequest[0]['remote__count'] > self.maxAllowedJSONRequstesPerHour:
                        reqs.is_rejected = 1
                        reqs.save()
                        return HttpResponse(json.dumps({'message':'your IP produces too many requests per hour, please try later'}), content_type='text/html')

        reqs.save()
        return None



