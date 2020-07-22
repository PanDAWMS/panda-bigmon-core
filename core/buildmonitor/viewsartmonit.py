from django.shortcuts import render, redirect
# from django.shortcuts import render_to_response, redirect
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
import json, re, sys, datetime
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
    # https://bigpanda.cern.ch/art/tasks/?branch=master/Athena/x86_64-centos7-gcc8-opt&ntags=2020-01-22,2020-01-23,2020-01-24,2020-01-25&json
    # https://bigpanda.cern.ch/art/overview/?ntags=2020-01-14,2020-01-15&view=branches&json

    valid, response = initRequest(request)

    ts_now = datetime.datetime.now()
    s_tstamp = ''
    for i in range(10, -1, -1):
        ts = datetime.datetime.now() - datetime.timedelta(days=i)
        ts_f = datetime.datetime.strftime(ts, '%Y-%m-%d')
        if s_tstamp == '':
            s_tstamp = str(ts_f)
        else:
            s_tstamp = s_tstamp + ',' + str(ts_f)
    #    print('string of tstamps ',s_tstamp)
    url10 = "https://bigpanda.cern.ch/art/overview/?ntags=" + s_tstamp + "&view=branches&json"
    #    print('TRYING requests.get('+url10+')')
    r = requests.get(url10, verify=False)
    #    pprint(r)
    a0 = json.loads(r.text)
    branch_dict = a0.get('artpackages', {})
    branch_list = branch_dict.keys()
    #    print('Branch list:',branch_list)
    dict_result = {}
    for branch in branch_list:
        url11 = "https://bigpanda.cern.ch/art/tasks/?branch=" + branch + '&ntags=' + s_tstamp + '&json'
        #        print('TRYING requests.get('+url11+')')
        r = requests.get(url11, verify=False)
        #        pprint(r)
        a = json.loads(r.text)
        tasks = a.get('arttasks', {})
        #        cache.set('art-monit-dict', dict_branch, 1800)
        reslist = []
        dict_branch = {}
        for k, v in tasks.items():
            if isinstance(v, dict):
                for kx, ky in v.items():
                    if kx == branch:
                        if isinstance(ky, dict):
                            for kxx, kyy in ky.items():
                                if isinstance(kyy, dict):
                                    for kxxx, kyyy in kyy.items():
                                        #                                    print('K ',kxx,kxxx)
                                        if re.search(kxx, kxxx):
                                            #                                        pprint(kyyy)
                                            a0_branch = dict_branch.get(kxxx, {'active': 0, 'succeeded': 0, 'failed': 0,
                                                                               'finished': 0})
                                            s_active = kyyy['active'] + a0_branch['active']
                                            s_done = kyyy['succeeded'] + a0_branch['succeeded']
                                            s_failed = kyyy['failed'] + a0_branch['failed']
                                            s_finished = kyyy['finished'] + a0_branch['finished']
                                            dict_branch[kxxx] = {'active': s_active, 'succeeded': s_done, 'failed': s_failed,
                                                                 'finished': s_finished}
                                            #                                        cache.set('art-monit-dict', dict_branch, 1800)
                                            reslist.append([s_active, s_done, s_failed, s_finished])
        dict_result[branch] = dict_branch
    cache.set('art-monit-dict', dict_result, 1800)
    #    dict_from_cache = cache.get('art-monit-dict')
    #    pprint('===============================')
    list2view = []
    for k46, v46 in dict_result.items():
        for kk, vv in v46.items():
            l1 = [k46]
            l1.append(kk)
            l1.extend([vv['active'], vv['succeeded'], vv['failed'], vv['finished']])
#            print('L1 ',l1)
            list2view.append(l1)
###########
    new_cur = connection.cursor() 
    query = """                                                                                                                                                
    select n.nname as \"BRANCH\", platf.pl,                                                                                             
    TO_CHAR(j.begdate,'DD-MON HH24:MI') as \"DATE\",                                                                                                           
    TO_CHAR(j.eb,'DD-MON HH24:MI') as \"BLD\",  
    a.relnstamp as \"TMSTAMP\",                                                                                                                                
    platf.lartwebarea,                                                                                                                                         
    TO_CHAR(j.ela,'DD-MON HH24:MI') as \"LA\",                                                                                                                 
    j.erla,j.sula                                                                                                                                              
    from nightlies@ATLR.CERN.CH n inner join                                                                                                                   
      ( releases@ATLR.CERN.CH a inner join                                                                                                                     
        ( jobstat@ATLR.CERN.CH j inner join projects@ATLR.CERN.CH p on j.projid = p.projid) on a.nid=j.nid and a.relid=j.relid )                               
      on n.nid=a.nid,                                                                                                                                          
    (select arch||'-'||os||'-'||comp||'-'||opt as pl, jid, lartwebarea from jobs@ATLR.CERN.CH ) platf                                                         
     WHERE                                                                                                                                                     
    j.jid BETWEEN to_number(to_char(SYSDATE-3, 'YYYYMMDD'))*10000000                                                                                          
     AND to_number(to_char(SYSDATE, 'YYYYMMDD')+1)*10000000                                                                                                    
     AND j.jid = platf.jid                                                                                                                                     
     AND j.begdate between sysdate-3 and sysdate                                                                                                              
     AND j.eb is not NULL 
     AND j.sula is not NULL order by j.eb desc                                                                                                                  
          """
    new_cur.execute(query)
    qresult = new_cur.fetchall() 
    dict_loc_result = {}
    for row in qresult:
        l_branch = row[0]
        l_rel = row[4]
        l_er = row[7]
        l_su = row[8]
        dict_inter = {}
        dict_inter[l_rel] = {'done': str(l_su), 'failed': str(l_er)}
        if l_branch in dict_loc_result:
            dict_inter1 = {}
            dict_inter1 = dict_loc_result[l_branch]
            dict_inter1.update(dict_inter)
            dict_loc_result[l_branch] = dict_inter1
        else:
            dict_loc_result[l_branch] = dict_inter
    cache.set('art-local-dict', dict_loc_result, 1800)

#    pprint(dict_loc_result) 
#    for k47, v47 in dict_loc_result.items():
#         print('L2',k47)
#         pprint(v47)
#        for kk, vv in v47.items():
#            print('L2 ', k47, kk, vv.get('done','UNDEF'), vv.get('failed','UNDEF')) 

    data = {'viewParams': request.session['viewParams'], 'resltART': json.dumps(list2view, cls=DateEncoder)}

    return render(request, 'artmonitviewDemo.html', data, content_type='text/html')
#    return render_to_response('globalviewDemo.html', data, content_type='text/html')
