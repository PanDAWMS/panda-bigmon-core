from django.shortcuts import render, redirect
#from django.shortcuts import render_to_response, redirect
from datetime import datetime
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.core.cache import cache
import json, re, os
from pprint import pprint
from collections import defaultdict
from operator import itemgetter, attrgetter

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def globalviewDemo(request):

    valid, response = initRequest(request)
    new_cur = connection.cursor()
    dict_from_cache = cache.get('art-monit-dict')
    check_icon='<div class="ui-widget ui-state-check" style="display:inline-block;"> <span style="display:inline-block;" title="OK" class="DataTables_sort_icon css_right ui-icon ui-icon-circle-check">ICON33</span></div>'
    clock_icon='<div class="ui-widget ui-state-hover" style="display:inline-block;"> <span style="display:inline-block;" title="UPDATING" class="DataTables_sort_icon css_right ui-icon ui-icon-clock">ICON39</span></div>'
    minorwarn_icon='<div class="ui-widget ui-state-highlight" style="display:inline-block;"> <span style="display:inline-block;" title="MINOR WARNING" class="DataTables_sort_icon css_right ui-icon ui-icon-alert">ICON34</span></div>'
    majorwarn_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span style="display:inline-block;" title="WARNING" class="DataTables_sort_icon css_right ui-icon ui-icon-lightbulb">ICON35</span></div>'
    error_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span style="display:inline-block;" title="ERROR" class="DataTables_sort_icon css_right ui-icon ui-icon-circle-close">ICON36</span></div>'
    radiooff_icon='<div class="ui-widget ui-state-default" style="display:inline-block";> <span title="N/A" class="ui-icon ui-icon-radio-off">ICONRO</span></div>'
    di_res = {'-1': clock_icon, 'N/A': radiooff_icon, '0': check_icon, '1': error_icon, '2': majorwarn_icon,'3': error_icon, '4': minorwarn_icon, '10': clock_icon}

    query = """
    select n.nname as \"BRANCH\", n.ngroup as \"GROUP\", platf.pl,
    TO_CHAR(j.begdate,'DD-MON HH24:MI') as \"DATE\",
    TO_CHAR(j.econf,'DD-MON HH24:MI') as \"CONF\", j.sconf as \"S.CONF\",
    TO_CHAR(j.eco,'DD-MON HH24:MI') as \"CO\", j.sco as \"S.CONF\",
    TO_CHAR(j.eext,'DD-MON HH24:MI') as \"EXT\",
    TO_CHAR(j.eb,'DD-MON HH24:MI') as \"BLD\",
    TO_CHAR(j.ei,'DD-MON HH24:MI') as \"TEST\",
    TO_CHAR(j.ecv,'DD-MON HH24:MI') as \"CVMFS\", j.SCV as \"S.CV\",
    a.tcrel as \"TAG\", a.name as \"REL\", p.projname \"PROJ\",
    cs.nc, cs.nc_er, cs.nc_pb,
    ts.nt, ts.nt_er, ts.nt_pb,
    TO_CHAR(j.jid),
    j.lastpj,
    j.relid,
    a.gitmrlink as \"GLINK\",
    a.relnstamp as \"TMSTAMP\",
    TO_CHAR(j.ecvkv,'DD-MON HH24:MI') as \"CVMCL\", j.SCVKV as \"S.CVMCL\",
    platf.lartwebarea,
    TO_CHAR(j.ela,'DD-MON HH24:MI') as \"LA\",
    j.erla,j.sula, j.wala, j.eim, j.sim, j.vext, j.suff, platf.webarea
    from nightlies@ATLR.CERN.CH n inner join
      ( releases@ATLR.CERN.CH a inner join
        ( jobstat@ATLR.CERN.CH j inner join projects@ATLR.CERN.CH p on j.projid = p.projid) on a.nid=j.nid and a.relid=j.relid )
      on n.nid=a.nid,
    (select arch||'-'||os||'-'||comp||'-'||opt as pl, jid, lartwebarea, webarea from jobs@ATLR.CERN.CH ) platf,
    (select ncompl as nc, ner as nc_er, npb as nc_pb, jid, projid from cstat@ATLR.CERN.CH ) cs,
    (select ncompl as nt, ner as nt_er, npb as nt_pb, jid, projid from tstat@ATLR.CERN.CH ) ts
     WHERE
    j.jid BETWEEN to_number(to_char(SYSDATE-10, 'YYYYMMDD'))*10000000
     AND to_number(to_char(SYSDATE, 'YYYYMMDD')+1)*10000000
     AND j.jid = platf.jid
     AND j.jid = cs.jid and j.projid = cs.projid
     AND j.jid = ts.jid and j.projid = ts.projid
     AND j.begdate between sysdate-10 and sysdate
     AND j.eb is not  NULL order by j.eb desc
          """
    new_cur.execute(query)
    result = new_cur.fetchall()
    first_row = result[0]
    rows_s = []
    dd = defaultdict(list)
    for row1 in result:
        if row1[0] not in dd:
            if row1[23] == 1:  # LAST PROJECT
                dd[row1[0]] += row1[1:]
        else:
            if row1[24] == dd[row1[0]][23]:
                dd[row1[0]][15] += row1[16]
                dd[row1[0]][16] += row1[17]
                dd[row1[0]][17] += row1[18]
                dd[row1[0]][18] += row1[19]
                dd[row1[0]][19] += row1[20]
                dd[row1[0]][20] += row1[21]
    reslt2 = []
    dict_g = {'SIMULATION': 'BAC', 'TIER0': 'BAAC', 'TRIGGER_RELEASE': 'BAB', 'CI': 'AA0', 'MASTER': 'AA01',
              'GIT': 'AA1', 'ATN': 'AA2', 'ATN_TESTS': 'AA3', 'ATN(UNDER_CONSTRUCTION)': 'AA4', 'DEVELOPMENT': 'AA5',
              'HLT_POINT1': 'D', 'PATCH': 'EA', 'PHYSICSANALYSIS': 'EB', 'UPGRADE_STUDIES': 'CC',
              'ROOT6_INTEGRATION': 'AAA', 'OTHER': 'GB', 'DOXYGEN': 'Z', '19.0.X_BUGFIX': 'BD', '19.1.X_BUGFIX': 'BC',
              '19.2.X_BUGFIX': 'BB', 'GAUDI_HIVE': 'BAAA', 'UPGRADE_INTEGRATION': 'BAAB'}
    for k, v in dd.items():
        ar1 = []
        ar1.append(k)
        row10u = v[0].upper()
        name_code = dict_g.get(row10u, 'Y' + row10u)
        v[0] = row10u
        #        v[13]='<a href="http://atlas-nightlies-browser.cern.ch/~platinum/nightlies/info?tp=g&nightly='+k+'&rel='+v[13]+'&ar=*">'+v[13]+'</a>'
        ar1.extend(v)
        m_tcompl = v[17]
        if m_tcompl == None or m_tcompl == 'N/A' or m_tcompl <= 0: 
            v[18]='N/A'; v[19]='N/A'
        ar1.append(name_code)
        reslt2.append(ar1)
        lar1 = len(ar1)
    #    print(reslt2)
    dict_cache_transf={}
    if dict_from_cache:
        for k46, v46 in dict_from_cache.items():
            for kk, vv in v46.items():
                kk_transf = re.sub('/','_',k46) 
                key_transf = kk_transf+'_'+kk
                string_vv = '<B><span style="color: blue">' + str(vv['active']) + '</span></B>'
                string_vv = string_vv + ',<B><span style="color: green">'+ str(vv['succeeded']) +'</span></B>,'
                string_vv = string_vv + '<B><span style="color: brown">' + str(vv['finished']) + '</span></B>'
                string_vv = string_vv +',<B><span style="color: red">' + str(vv['failed']) + '</span></B>'
                dict_cache_transf[key_transf] = [string_vv, k46]
#    pprint(dict_cache_transf)
    reslt3 = []
    for row in reslt2:
        list9 = []
        m_ncompl =  row[16]
        if m_ncompl == None or m_ncompl == 'N/A' or m_ncompl <= 0:
            row[17]='N/A'; row[18]='N/A';
        a0001 = str(row[17]) + ' (' + str(row[18]) + ')'
        m_tcompl = row[19]
        if m_tcompl == None or m_tcompl == 'N/A' or m_tcompl <= 0:
            row[20]='N/A'; row[21]='N/A';
        a0002 = str(row[20]) + ' (' + str(row[21]) + ')'
        
        t_cv_clie=row[27];s_cv_clie=row[28]
        t_cv_serv=row[11];s_cv_serv=row[12]
        tt_cv_serv='N/A' 
        if t_cv_serv != None and t_cv_serv != '': tt_cv_serv=t_cv_serv
        tt_cv_clie='N/A'
        if t_cv_clie != None and t_cv_clie != '': tt_cv_clie=t_cv_clie
        ss_cv_serv='N/A'
        if s_cv_serv != None and s_cv_serv != '': ss_cv_serv=str(s_cv_serv)
        ss_cv_clie='N/A'
        if s_cv_clie != None and s_cv_clie != '': ss_cv_clie=str(s_cv_clie)
        [i_cv_serv,i_cv_clie]=map(lambda x: di_res.get(str(x),str(x)), [ss_cv_serv,ss_cv_clie])
        if tt_cv_serv != 'N/A' : i_combo_cv_serv=tt_cv_serv+i_cv_serv
        else: i_combo_cv_serv=i_cv_serv
        if tt_cv_clie != 'N/A' : i_combo_cv_clie=tt_cv_clie+i_cv_clie
        else: i_combo_cv_clie=i_cv_clie

        lartwebarea=row[29]
        if lartwebarea == None or lartwebarea == '': lartwebarea="http://atlas-computing.web.cern.ch/atlas-computing/links/distDir\
ectory/gitwww/GITWebArea/nightlies"        
        erla=row[31];sula=row[32]; wala=row[33]; eim=row[34]; sim=row[35]; vext=row[36]; area_suffix=row[37]; webarea_cur=row[38]
        if erla == None or erla == '': erla='N/A'
        if sula == None or sula == '': sula='N/A'
        if wala == None or wala == '': wala = 'N/A'
        if wala.isdigit() and sula.isdigit():
            sula=str(int(sula)-int(wala))
        if eim == None or eim == '': eim='N/A'
        if sim == None or sim == '': sim='N/A'
        if vext == None or vext == '': vext = '0'
        if webarea_cur == None: webarea_cur = ''
        if area_suffix == None: area_suffix = '';
        brname = row[0]
        link_brname = brname
        link_to_ciInfo = reverse('BuildCI')
        link_to_nInfo = reverse('BuildN')
        if re.match('^.*CI.*$', brname):
            link_brname = "<a href=\"" + link_to_ciInfo + "\">" + brname + "</a>"
        else:
            link_brname = "<a href=\"" + link_to_nInfo + "?nightly=" + brname + "\">" + brname + "</a>"

        rname=row[14]
        rname_trun = re.sub(r'\([^)]*\)', '', rname)
        key_cache_transf=brname + '_' + rname
        val_cache_transf,nightly_name_art=dict_cache_transf.get(key_cache_transf,['N/A','N/A'])
        if val_cache_transf != 'N/A' and nightly_name_art != 'N/A': 
            vacasf = "<a href=\"https://bigpanda.cern.ch/art/overview/?branch=" 
            val_cache_transf = vacasf + nightly_name_art + "&ntag_full=" + rname + "\">" + val_cache_transf + "</a>"

        local_art_res=''
        if sula == 'N/A' and erla == 'N/A':
            local_art_res='N/A'
        elif sula == '0' and erla == '0':
            local_art_res='N/A'
        else:
            local_art_res=local_art_res+'<B><span style="color: green">'+ str(sula)+'</span></B>,'
            local_art_res = local_art_res +'<B><span style="color: brown">' + str(wala) + '</span></B>,'
            local_art_res=local_art_res+'<B><span style="color: red">'+ str(erla)+'</span></B>'
            arrk=re.split('_',brname)
            branch=arrk[0]
            loares="<a href=\""+lartwebarea+"/"+branch+"/"+rname+"/"+row[15]+"/"+row[2]+"/"+row[15]+"/art.log.html\">"
            local_art_res=loares+local_art_res+"</a>"

        image_res='N/A'
        if sim == 'N/A' or eim == 'N/A':
            image_res='N/A'
        elif sim == 0 or sim == 1 or sim == 2 or sim == 3 or sim == 4:
            image_res = di_res.get(str(sim), str(sim))
            if str(vext) == '1' :
                image_res= "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_image_build.html' + "\">" + image_res + "</a>"
            if isinstance(eim, datetime):
                image_res = image_res+" "+eim.strftime('%d-%b %H:%M').upper()
        else:
            image_res = di_res.get(str(sim), str(sim))
        list9.append(row[1]);
        list9.append(link_brname);
        list9.append(rname);
        list9.append(row[9]);
        list9.append(a0001);
        list9.append(a0002);
        list9.append(local_art_res);
        list9.append(val_cache_transf);
        list9.append(tt_cv_clie);
        list9.append(image_res);
        list9.append(row[39]);

        reslt3.append(list9)

#    if dict_from_cache:
#        for k46, v46 in dict_from_cache.items():
#            for kk, vv in v46.items():
#                l1=[k46]
#                l1.append(kk)
#                l1.extend([vv['active'], vv['done'], vv['failed'], vv['finished']])
#                print('L1 ',l1)    
#    dict_from_local_cache = cache.get('art-local-dict')
#    if dict_from_local_cache:
#        for k47, v47 in dict_from_local_cache.items():
#            print('L2',k47)
#            pprint(v47)

    data={'viewParams': request.session['viewParams'], 'reslt3':json.dumps(reslt3, cls=DateEncoder)}

    return render(request,'globalviewDemo.html', data, content_type='text/html')
#    return render_to_response('globalviewDemo.html', data, content_type='text/html') 

