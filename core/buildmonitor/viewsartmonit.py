from django.shortcuts import render, redirect
#from django.shortcuts import render_to_response, redirect
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.core.cache import cache
from collections import defaultdict
from operator import itemgetter, attrgetter
from pprint import pprint
import requests
import json,re,sys,datetime 
from django.views.decorators.cache import never_cache

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)

@never_cache
def artmonitviewDemo(request):
#https://bigpanda.cern.ch/art/tasks/?branch=master/Athena/x86_64-centos7-gcc8-opt&ntags=2020-01-22,2020-01-23,2020-01-24,2020-01-25&json  
#https://bigpanda.cern.ch/art/overview/?ntags=2020-01-14,2020-01-15&view=branches&json          
    ts_now = datetime.datetime.now()
    s_tstamp=''
    for i in range(10, -1, -1):
        ts = datetime.datetime.now()-datetime.timedelta(days=i)
        ts_f = datetime.datetime.strftime(ts,'%Y-%m-%d')
        if s_tstamp == '':
          s_tstamp=str(ts_f)
        else:
          s_tstamp=s_tstamp+','+str(ts_f)
#    print('string of tstamps ',s_tstamp)
    url10="https://bigpanda.cern.ch/art/overview/?ntags="+s_tstamp+"&view=branches&json"
#    print('TRYING requests.get('+url10+')')
    r = requests.get(url10, verify=False)
#    pprint(r)
    a0=json.loads(r.text)
    branch_dict=a0.get('artpackages',{})
    branch_list=branch_dict.keys()
#    print('Branch list:',branch_list)
    dict_result={}
    for branch in branch_list:
        url11="https://bigpanda.cern.ch/art/tasks/?branch="+branch+'&ntags='+s_tstamp+'&json'
#        print('TRYING requests.get('+url11+')')
        r = requests.get(url11, verify=False)
#        pprint(r)
        a=json.loads(r.text)
        tasks=a.get('arttasks',{})
#        cache.set('art-monit-dict', dict_branch, 1800)
        reslist=[]
        dict_branch={}
        for k,v in tasks.items():
          if isinstance(v, dict):
            for kx, ky in v.items():
                if kx == branch:
                    if isinstance(ky, dict):
                        for kxx, kyy in ky.items():
                            if isinstance(kyy, dict):
                                for kxxx, kyyy in kyy.items():
#                                    print('K ',kxx,kxxx)
                                    if re.search(kxx,kxxx):
#                                        pprint(kyyy)
                                        a0_branch=dict_branch.get(kxxx,{'active': 0, 'done': 0, 'failed': 0, 'finished': 0})
                                        s_active=kyyy['active']+a0_branch['active']
                                        s_done=kyyy['done']+a0_branch['done']
                                        s_failed=kyyy['failed']+a0_branch['failed']
                                        s_finished=kyyy['finished']+a0_branch['finished']
                                        dict_branch[kxxx]={'active': s_active, 'done': s_done, 'failed': s_failed, 'finished': s_finished}
#                                        cache.set('art-monit-dict', dict_branch, 1800)
                                        reslist.append([s_active, s_done, s_failed, s_finished])
        dict_result[branch]=dict_branch
    cache.set('art-monit-dict', dict_result, 1800)
#    dict_from_cache = cache.get('art-monit-dict')
#    pprint('===============================')
    list2view=[]
    for k46, v46 in dict_result.items():
        for kk, vv in v46.items():
            l1=[k46]
            l1.append(kk) 
            l1.extend([vv['active'], vv['done'], vv['failed'], vv['finished']])
#            print('L1 ',l1)
            list2view.append(l1)

    data={'viewParams': request.session['viewParams'], 'resltART':json.dumps(list2view, cls=DateEncoder)}

    return render(request,'artmonitviewDemo.html', data, content_type='text/html')
#    return render_to_response('globalviewDemo.html', data, content_type='text/html') 

