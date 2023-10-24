import json
import logging
import pickle
import collections
import time
from datetime import datetime, timedelta

from django.http import HttpResponse
from django.views.decorators.cache import never_cache

from core.monitor.modelsMonitor import AtlasDBA
from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.DateEncoder import DateEncoder
from core.views import initRequest

from django.conf import settings

_logger_err = logging.getLogger('bigpandamon-error')

@never_cache
def monitorJson(request):
    """
    Return a JSON with service metrics
    :param request:
    :return:
    """
    valid, response = initRequest(request)
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')

    if ip not in settings.CACHING_CRAWLER_HOSTS:
        return HttpResponse(json.dumps({'message': 'Forbidden!'}), status=403, content_type='application/json')

    db_sessions = {}
    if settings.DEPLOYMENT == 'ORACLE_ATLAS':
        db_sessions['total'] = 0
        db_sessions['active'] = 0
        sessions = AtlasDBA.objects.filter().values('num_active_sess', 'num_sess','machine', 'program')
        for session in sessions:
            db_sessions['total'] += session['num_sess']
            db_sessions['active'] += session['num_active_sess']
        if db_sessions['total'] >= 50 or db_sessions['active'] >= 20:
            message = f"Internal Server Error: Attention!!! Total session count: {db_sessions['total']} Total active session count: {db_sessions['active']}"
            _logger_err.error(message)

    data = {
        'db_sessions': db_sessions,
        'response_time': time.time() - request.session['req_init_time'],
    }
    response = HttpResponse(json.dumps(data, cls=DateEncoder), content_type='application/json')
    return response


def serverStatusHealth(request):
    """
    This function dymanically calculates status of a particular server in order to make it idle and give an opportunity
    to restart wsgi daemon.

    WSGIDaemonProcess: inactivity-timeout=60 (this is for nginx health) restart-interval=14400 (the last one is for guarging from blocking requests)

    Nginx: https://www.nginx.com/resources/admin-guide/http-health-check/
    match server_ok {
        status 200;
        header Content-Type = text/html;
        body ~ "Normal operation";
    }
    location / {
        proxy_pass http://backend;
        health_check match=server_ok uri=/serverstatushealth/ interval=600 fails=10000 passes=1;
    }
    Then healthping = 10 min
    """

    initRequest(request)
    periodOfAllServWorkRestart = 15 #minutes.
    restartTimeWindow = 5

    debug = True

    # Here we should load all the servers from the settingsdjangosettings.
    # next is just for tests

    data = getCacheEntry(request, "StatusHealth")

    print ("serverStatusHealth ", datetime.now(), " runninghost:", request.session["hostname"], " ", data)

    if data is None:
        q = collections.deque()
        q.append("aipanda100")
        q.append("aipanda105")
        q.append("aipanda106")
        q.append("aipanda115")
        q.append("aipanda116")
        q.append("aipanda107")
        q.append("aipanda108")
        lastupdate = datetime.now()
        data['q'] = pickle.dumps(q)
        data['lastupdate'] = lastupdate
        setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
    else:
        data = json.loads(data)
        q = pickle.loads(data['q'])
        lastupdate = datetime.strptime(data['lastupdate'], settings.defaultDatetimeFormat)

    # end of test filling

    currenthost = q.popleft()
    runninghost = request.session["hostname"]

    if (currenthost == runninghost):
        if (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart)) and \
                        (datetime.now() - lastupdate) < timedelta(minutes=(periodOfAllServWorkRestart+restartTimeWindow)):
            return HttpResponse("Awaiting restart", content_type='text/html')
        elif (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart)) and \
                        (datetime.now() - lastupdate) > timedelta(minutes=(periodOfAllServWorkRestart+restartTimeWindow)):
            data = {}
            q.append(currenthost)
            data['q'] = pickle.dumps(q)
            data['lastupdate'] = datetime.now().strftime(settings.defaultDatetimeFormat)
            setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
            return HttpResponse("Normal operation", content_type='text/html')

    # rows = subprocess.check_output('ps -eo cmd,lstart --sort=start_time | grep httpd', shell=True).split('\n')[:-2]
    # print "serverStatusHealth ", datetime.now(), " rows:", rows
    #
    # if (currenthost == runninghost) and (datetime.now() - lastupdate) > timedelta(minutes=periodOfAllServWorkRestart):
    #
    #     if len(rows) > 0:
    #         httpdStartTime = list(datefinder.find_dates(rows[0]))[0]
    #         if (datetime.now() - httpdStartTime) < timedelta(minutes=periodOfAllServWorkRestart):
    #
    #             print "serverStatusHealth ", "httpdStartTime", httpdStartTime
    #
    #             data = {}
    #             data['q'] = pickle.dumps(q)
    #             data['lastupdate'] = datetime.now().strftime(defaultDatetimeFormat)
    #             setCacheEntry(request, "StatusHealth", json.dumps(data, cls=DateEncoder), 60 * 60)
    #
    #             print "serverStatusHealth ", "Normal operation0"
    #             return HttpResponse("Normal operation", content_type='text/html')
    #             # We think that wsgi daemon recently restarted and we can change order to the next server
    #             # q.put(currenthost)
    #             # q. put to cache
    #             # lastupdate put to cache
    #             # return success
    #
    #     # we return failed by default
    #     print "serverStatusHealth ", "Awaiting restart"
    #     return HttpResponse("Awaiting restart", content_type='text/html')
    #
    # print "serverStatusHealth ", "Normal operations1"
    return HttpResponse("Normal operation", content_type='text/html')
