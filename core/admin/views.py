import logging, re, json, commands, os, copy
from datetime import datetime, timedelta
import time
import json
from django.http import HttpResponse
from django.shortcuts import render_to_response, render, redirect
from django.template import RequestContext, loader
from django.db.models import Count
from django import forms
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.utils.cache import patch_cache_control, patch_response_headers
from core.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from core.common.models import RequestStat
from core.settings.config import ENV
from time import gmtime, strftime
from core.common.models import Users

from core.views import initRequest
from core.views import extensibleURL
from django.http import HttpResponseRedirect


def login(request):

    if 'userdn' in request.session:
        userrec = Users.objects.filter(dn__startswith=request.session['userdn']).values()
        if len(userrec) > 0:
           request.session['username'] = userrec[0]['name']
           return True,None
        else:
             data = {
                     'viewParams' : request.session['viewParams'],
                     'requestParams' : request.session['requestParams'],
                     "errormessage" : "Sorry, we could not find your DN '%s' in database" % request.session['userdn'],\
                    }
             return False, render_to_response('adError.html', data, RequestContext(request))

    else:
        try:
           url="https://"+request.META['SERVER_NAME']+request.META['REQUEST_URI']
        except:
           url=''
        data = {
                'viewParams' : request.session['viewParams'],
                'requestParams' : request.session['requestParams'],
                'url': url,
                "errormessage" : "No valid client certificate found.",\
               }
        return False, render_to_response('adError.html', data, RequestContext(request))
 
def adMain(request):
   
    valid, response = initRequest(request)
    if not valid: return response

    valid, response = login(request)
    if not valid: return response

    
    data = {\
       'request': request,
       'user': request.session['username'],
       'url' : request.path,\
    }

    return render_to_response('adMain.html', data, RequestContext(request))

def listReqPlot(request):
    valid, response = initRequest(request)
    if not valid: return response

    valid, response = login(request)
    if not valid: return response

    sortby='id'
    if 'sortby' in request.GET:
        sortby=request.GET['sortby']
 
    LAST_N_HOURS_MAX=7*24
    limit=5000
    if 'hours' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['hours'])
    if 'days' in request.session['requestParams']:
        LAST_N_HOURS_MAX = int(request.session['requestParams']['days'])*24

    if u'display_limit' in request.session['requestParams']:
        display_limit = int(request.session['requestParams']['display_limit'])
    else:
        display_limit = 1000
    nmax = display_limit

    if LAST_N_HOURS_MAX>=168:
       flag=12
    elif LAST_N_HOURS_MAX>=48:
       flag=6
    else:
       flag=2

    startdate = None
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
    enddate = None
    if enddate == None:
        enddate = timezone.now()#.strftime(defaultDatetimeFormat)

    query = { 'qtime__range' : [startdate.strftime(defaultDatetimeFormat), enddate.strftime(defaultDatetimeFormat)] }

    values = 'urls', 'qtime','remote','qduration','duration'
    reqs=[]
    reqs = RequestStat.objects.filter(**query).order_by(sortby).reverse().values(*values)

    reqHist = {}
    drHist =[]

    mons=[]
    for req in reqs:
        mon={}
        #mon['duration'] = (req['qduration'] - req['qtime']).seconds
        mon['duration'] = req['duration']
        mon['urls'] = req['urls']
        mon['remote'] = req['remote']
        mon['qduration']=req['qduration'].strftime('%Y-%m-%d %H:%M:%S')
        mon['qtime'] =  req['qtime'].strftime('%Y-%m-%d %H:%M:%S')
        mons.append(mon)

        ##plot
        tm=req['qtime']
        tm = tm - timedelta(hours=tm.hour % flag, minutes=tm.minute, seconds=tm.second, microseconds=tm.microsecond)
        if not tm in reqHist: reqHist[tm] = 0
        reqHist[tm] += 1

        ##plot -view duration
        dr=int(mon['duration'])
        drHist.append(dr)

    kys = reqHist.keys()
    kys.sort()
    reqHists = []
    for k in kys:
        reqHists.append( [ k, reqHist[k] ] )

    drcount=[[x,drHist.count(x)] for x in set(drHist)]
    drcount.sort()

    #do paging

    paginator = Paginator(mons, 200)
    page = request.GET.get('page')
    try:
        reqPages = paginator.page(page)
    except PageNotAnInteger:
        reqPages = paginator.page(1)
    except EmptyPage:
        reqPages = paginator.page(paginator.num_pages)

    url= request.get_full_path()
    if url.count('?')>0:
       url += '&'
    else:
       url += '?'

    data = {\
       'mons': mons[:nmax],
       'nmax': nmax,
       'request': request,
       'user': request.session['username'],
       'reqPages': reqPages,
       'url' : url,
       'drHist': drcount,
       'reqHist': reqHists,\
    }

    return render_to_response('req_plot.html', data, RequestContext(request))

