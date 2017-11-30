import time

import django.core.exceptions
import commands
import random
from core.common.models import RequestStat
from django.utils import timezone
from datetime import timedelta
from django.db.models import Count, Sum
from django.http import HttpResponse
import json

# We postpone JSON requests is server is overloaded
# Done for protection from bunch of requests from JSON


class DDOSMiddleware(object):

    sleepInterval = 5 #sec
    maxAllowedJSONRequstesPerHour = 600

    def __init__(self):
        pass

    def process_request(self, request):

        # we limit number of requests per hour
        if ('json' in request.GET):
            x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
            if not x_forwarded_for is None:
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
                        return HttpResponse(json.dumps({'message':'your IP produces too many requests per hour, please try later'}), content_type='text/html')
        return None



