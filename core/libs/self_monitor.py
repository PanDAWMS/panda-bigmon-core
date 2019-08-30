"""
The self monitor to register requests and their duration in DB
"""
import psutil
import datetime
from django.utils import timezone
from core.common.models import RequestStat


def init_self_monitor(request):
    if 'hostname' in request.session:
        server = request.session['hostname']
    else:
        server = '-'

    if 'HTTP_X_FORWARDED_FOR' in request.META:
        remote = request.META['HTTP_X_FORWARDED_FOR']
    else:
        remote = request.META['REMOTE_ADDR']

    urlProto = request.META['wsgi.url_scheme']
    if 'HTTP_X_FORWARDED_PROTO' in request.META:
        urlProto = request.META['HTTP_X_FORWARDED_PROTO']
    urlProto = str(urlProto) + "://"

    try:
        urls = urlProto + request.META['SERVER_NAME'] + request.META['REQUEST_URI']
    except:
        if 'SERVER_PORT' in request.META:
            port = ':' + request.META['SERVER_PORT']
        else:
            port = ''
        if 'PATH_INFO' in request.META:
            path = request.META['PATH_INFO']
        else:
            path = ''
        if 'QUERY_STRING' in request.META and request.META['QUERY_STRING'] != "":
            qstring = '?' + request.META['QUERY_STRING']
        else:
            qstring = ''
        urls = urlProto + request.META['SERVER_NAME'] + port + path + qstring
    print(urls)
    qtime = str(timezone.now())
    load = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory().percent
    if 'HTTP_REFERER' in request.META:
        refferer = request.META['HTTP_REFERER']
    else:
        refferer = '-'
    if 'HTTP_USER_AGENT' in request.META:
        useragent = request.META['HTTP_USER_AGENT']
    else:
        useragent = '-'
    request.session["qtime"] = qtime
    request.session["load"] = load
    request.session["remote"] = remote
    request.session["mem"] = mem
    request.session["urls"] = urls
    request.session["refferer"] = refferer
    request.session["useragent"] = useragent


def end_self_monitor(request):
    qduration = str(timezone.now())
    request.session['qduration'] = qduration
    try:
        duration = (datetime.strptime(request.session['qduration'], "%Y-%m-%d %H:%M:%S.%f") - datetime.strptime(
            request.session['qtime'], "%Y-%m-%d %H:%M:%S.%f")).seconds
    except:
        duration = 0
    if 'hostname' in request.session and 'remote' in request.session and request.session['remote'] is not None:
        reqs = RequestStat(
            server=request.session['hostname'] if 'hostname' in request.session else '-',
            qtime=request.session['qtime'] if 'qtime' in request.session else None,
            load=request.session['load'] if 'load' in request.session else None,
            mem=request.session['mem'] if 'mem' in request.session else None,
            qduration=request.session['qduration'] if 'qduration' in request.session else None,
            duration=duration,
            remote=request.session['remote'] if 'remote' in request.session and request.session[
                'remote'] is not None else '',
            urls=request.session['urls'] if 'urls' in request.session else '',
            description=' ',
            referrer=request.session['refferer'],
            useragent=request.session["useragent"]
        )
        reqs.save()