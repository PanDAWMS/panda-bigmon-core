from django.shortcuts import render
from core.views import initRequest
from django.db import connection
from django.core.cache import cache
import requests
import json, re, datetime
import logging
from django.views.decorators.cache import never_cache
from core.libs.DateEncoder import DateEncoder

_logger = logging.getLogger('bigpandamon')


@never_cache
def artmonitviewDemo(request):
    # https://bigpanda.cern.ch/art/tasks/?branch=master/Athena/x86_64-centos7-gcc8-opt&ntags=2020-01-22,2020-01-23,2020-01-24,2020-01-25&json
    # https://bigpanda.cern.ch/art/overview/?ntags=2020-01-14,2020-01-15&view=branches&json

    valid, response = initRequest(request)
    if not valid:
        return response

    ts_now = datetime.datetime.now()
    s_tstamp = ''
    for i in range(10, -1, -1):
        ts = datetime.datetime.now() - datetime.timedelta(days=i)
        ts_f = datetime.datetime.strftime(ts, '%Y-%m-%d')
        if s_tstamp == '':
            s_tstamp = str(ts_f)
        else:
            s_tstamp = s_tstamp + ',' + str(ts_f)

    # getting branches
    url10 = "https://bigpanda.cern.ch/art/overview/?ntags=" + s_tstamp + "&view=branches&json"
    _logger.debug('getting branches from {}'.format(url10))
    n_attempts = 3
    is_success = False
    i_attempt = 0
    r = None
    while i_attempt < n_attempts and not is_success:
        r = requests.get(url10, verify=False)
        if r.status_code == 200:
            is_success = True

    if not is_success:
        _logger.error("Internal Server Error! Failed to get ART test results for buildmonitor from {} with\n{}".format(
            url10,
            str(r.text)
        ))
        return render(
            request,
            'artmonitviewDemo.html',
            {'viewParams': request.session['viewParams'],
            'resltART': []},
            content_type='text/html'
        )


    a0 = json.loads(r.text)
    branch_dict = a0.get('artpackages', {})
    branch_list = branch_dict.keys()

    # getting ART GRID test results per branch
    _logger.debug('Branch list:'.format(branch_list))
    dict_result = {}
    for branch in branch_list:
        url11 = "https://bigpanda.cern.ch/art/tasks/?branch=" + branch + '&ntags=' + s_tstamp + '&json'
        _logger.debug('TRYING requests.get({})'.format(url11))
        try:
            r = requests.get(url11, verify=False)
            r.raise_for_status()
        except requests.RequestException as e:
            _logger.exception("General Error\n{}".format(str(e)))
            r = None
        if r is not None:
            a = json.loads(r.text)
            tasks = a.get('arttasks', {})
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
                                            if re.search(kxx, kxxx):
                                                a0_branch = dict_branch.get(
                                                    kxxx,
                                                    {'active': 0, 'succeeded': 0, 'failed': 0, 'finished': 0})
                                                s_active = kyyy['active'] + a0_branch['active']
                                                s_done = kyyy['succeeded'] + a0_branch['succeeded']
                                                s_failed = kyyy['failed'] + a0_branch['failed']
                                                s_finished = kyyy['finished'] + a0_branch['finished']
                                                dict_branch[kxxx] = {
                                                    'active': s_active,
                                                    'succeeded': s_done,
                                                    'failed': s_failed,
                                                    'finished': s_finished
                                                }
                                                reslist.append([s_active, s_done, s_failed, s_finished])
            dict_result[branch] = dict_branch
    cache.set('art-monit-dict', dict_result, 1800)

    list2view = []
    for k46, v46 in dict_result.items():
        for kk, vv in v46.items():
            l1 = [k46]
            l1.append(kk)
            l1.extend([vv['active'], vv['succeeded'], vv['failed'], vv['finished']])
            list2view.append(l1)

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

    data = {'viewParams': request.session['viewParams'], 'resltART': json.dumps(list2view, cls=DateEncoder)}

    return render(request, 'artmonitviewDemo.html', data, content_type='text/html')
