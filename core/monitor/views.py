import json,re,logging, urllib2

from django.http import HttpResponse, JsonResponse
from django.views.decorators.cache import never_cache

from core.monitor.modelsMonitor import AtlasDBA
from core.views import DateEncoder, initRequest


@never_cache
def monitorJson(request):
    notcachedRemoteAddress = ['188.184.185.129', '188.185.80.72','188.185.165.248']
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    if ip in notcachedRemoteAddress:
        valid, response = initRequest(request)
        test = False
        if 'test' in request.GET:
            test = True
        totalSessionCount = 0
        totalActiveSessionCount = 0
        sesslist = ('num_active_sess','num_sess','machine','program')
        sessions = AtlasDBA.objects.filter().values(*sesslist)
        for session in sessions:
            totalSessionCount += session['num_sess']
            totalActiveSessionCount += session['num_active_sess']
        if totalSessionCount>=50 or test:
            logger = logging.getLogger('bigpandamon-error')
            message = 'Internal Server Error: ' + 'Attention!!! Total session count: ' + str(totalSessionCount) + ' Total active session count: ' + str (totalActiveSessionCount)
            logger.error(message)
        data = list(sessions)
    #url = "https://atlas-service-dbmonitor.web.cern.ch/atlas-service-dbmonitor/dashboard/show_sessions.php?user=ATLAS_PANDABIGMON_R&db=ADCR"
    #page = urllib2.urlopen(url)
    #from bs4 import BeautifulSoup
    #soup = BeautifulSoup(page)
    #all_tables = soup.find_all('table')
        response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='text/html')
        return response
    return HttpResponse(json.dumps({'message':'Forbidden!'}), content_type='text/html')



