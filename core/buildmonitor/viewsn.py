from django.shortcuts import render
from datetime import datetime
from core.views import initRequest
from core.oauth.utils import login_customrequired
from django.db import connection
from django.urls import reverse
from django.core.cache import cache
import json, re, os
from core.libs.DateEncoder import DateEncoder


@login_customrequired
def nviewDemo(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 100:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'master_Athena_x86_64-centos7-gcc11-opt'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 100:
        rname = request.session['requestParams']['rel']
    else:
        rname = '*'
    ar_sel="unknown"
    pjname='unknown'

    new_cur = connection.cursor()
    dict_from_cache = cache.get('art-monit-dict')
    check_icon='<div class="ui-widget ui-state-check" style="display:inline-block;"> <span s\
tyle="display:inline-block;" title="OK" class="DataTables_sort_icon css_right ui-icon ui-ico\
n-circle-check">ICON33</span></div>'
    clock_icon='<div class="ui-widget ui-state-hover" style="display:inline-block;"> <span s\
tyle="display:inline-block;" title="UPDATING" class="DataTables_sort_icon css_right ui-icon \
ui-icon-clock">ICON39</span></div>'
    minorwarn_icon='<div class="ui-widget ui-state-highlight" style="display:inline-block;"> <s\
pan style="display:inline-block;" title="MINOR WARNING" class="DataTables_sort_icon css_righ\
t ui-icon ui-icon-alert">ICON34</span></div>'
    warn_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span st\
yle="display:inline-block;" title="WARNING" class="DataTables_sort_icon css_right ui-icon ui\
-icon-lightbulb">ICON35</span></div>'
    error_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span s\
tyle="display:inline-block;" title="ERROR" class="DataTables_sort_icon css_right ui-icon ui-\
icon-circle-close">ICON36</span></div>'
    help_icon='<span style="display:inline-block;" title="HELP" class="DataTables_sort_icon \
css_right ui-icon ui-icon-help">ICONH</span>'
    clip_icon='<span style="display:inline-block; float: left; margin-right: .9em;" title="C\
LIP" class="DataTables_sort_icon ui-icon ui-icon-clipboard">ICONCL</span>'
    person_icon='<span style="display:inline-block; float: left; margin-right: .9em;" title=\
"MASTER ROLE" class="DataTables_sort_icon ui-icon ui-icon-person">ICONP</span>'
    person_icon1='<span style="display:inline-block;" title="MASTER ROLE" class="DataTables_\
sort_icon ui-icon ui-icon-person">ICONP1</span>'
    mailyes_icon='<span style="cursor:pointer; display:inline-block; " title="MAIL ENABLED" \
class="ui-icon ui-icon-mail-closed"> ICON1</span>'
    radiooff_icon='<div class="ui-widget ui-state-default" style="display:inline-block";> <s\
pan title="N/A" class="ui-icon ui-icon-radio-off">ICONRO</span></div>'
    majorwarn_icon=warn_icon
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':error_icon,'2':majorwarn_icon,'3':error_icon,'4':minorwarn_icon,'10':clock_icon}

    query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr,lartwebarea from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and tstamp between sysdate-11+1/24 and sysdate order by tstamp desc" % nname
    if rname != '*':
      query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr,lartwebarea from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and name ='%s' and tstamp between sysdate-11+1/24 and sysdate order by tstamp desc" % (nname,rname)
####HEADERS      <th>Release</th>
#                <th>Platform</th>
#                <th>Project</th>
#                <th>Job time stamp</th>
#                <th>git clone</th>
#                <th>Externals<BR>build</th>
#                <th>CMake<BR>config</th>
#                <th>Build time</th>
#                <th>Comp. Errors<BR>(w/warnings)</th>
#                <th>Test time</th>
#                <th>Pct. of Successful<BR>CTest tests<BR>(no warnings)</th>
#                <th>CVMFS time</th>
#                <th>Host</th>
#                <th>CPack</th>
    new_cur.execute(query)
    result = new_cur.fetchall()
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':error_icon,'2':majorwarn_icon,'3':error_icon,'4':minorwarn_icon,'10':clock_icon}

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
    ar_sel="unknown"
    pjname='unknown'
    i=0
    rows_s = []   
    for row in result:
      i+=1
      if i > 1000: break
      jid_sel = row[0]
      ar_sel = row[1]
      rname = row[4]
      rname_trun = re.sub(r'\([^)]*\)', '', rname)
      webarea_cur = row[5]
      if webarea_cur == None: webarea_cur = "";
      job_start = row[8]
      t_start='N/A'
      if job_start != None: t_start=job_start.strftime('%Y/%m/%d %H:%M')
##      mrlink = row[7]
      gitbr = row[13]
      lartwebarea=row[14]
      if lartwebarea is None or lartwebarea == '':
          lartwebarea = "https://atlas-art-data.web.cern.ch/atlas-art-data/local-output"
#      print("JIDX",jid_sel)
#
      query01="select to_char(jid),projname,stat,eb,sb,ei,si,ecv,ecvkv,suff,scv,scvkv,scb,sib,sco,ela,sla,erla,sula,wala,ecp,scp,eext,sext,vext,hname from jobstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = '%s' order by projname" % (jid_sel)
      new_cur.execute(query01)
      reslt1 = new_cur.fetchall()
      lenres=len(reslt1)
      if lenres != 0 and ( reslt1[0][2] == 'cancel' or reslt1[0][2] == 'CANCEL' or reslt1[0][2] == 'ABORT' or reslt1[0][2] == 'abort' ):
          pjname=reslt1[0][1]
          s_ext = reslt1[0][23]
          if s_ext == None or s_ext == '': s_ext = 'N/A'
          vext = reslt1[0][24]
          if vext == None or vext == '': vext = '0'
          s_checkout = 'N/A'
          if reslt1[0][14] != None: s_checkout = str(reslt1[0][14])
          s_config = 'N/A'
          s_inst = 'N/A'
          if str(vext) != 1: s_config = '0'; s_inst = '0'
          if reslt1[0][12] != None: s_config = str(reslt1[0][12])
          if reslt1[0][13] != None: s_inst = str(reslt1[0][13])
          s_cpack = 'N/A'
          if reslt1[0][21] != None: s_cpack = str(reslt1[0][21])
          hname=reslt1[0][25]
          if re.search(r'\.',hname):
              hname=(re.split(r'\.',hname))[0]
          area_suffix = reslt1[0][9]
          if area_suffix == None: area_suffix = "";
          [i_checkout, i_inst, i_config, i_ext, i_cpack] = \
              map(lambda x: di_res.get(str(x), str(x)),
                  [s_checkout, s_inst, s_config, s_ext, s_cpack])
          if i_checkout == None or i_checkout == "None": i_checkout = radiooff_icon;
          if i_inst == None or i_inst == "None": i_inst = radiooff_icon;
          if i_config == None or i_config == "None": i_config = radiooff_icon;
          if i_ext == None or i_ext == "None": i_ext = radiooff_icon;
          if i_cpack == None or i_cpack == "None": i_cpack = radiooff_icon;
          ii_checkout, ii_config, ii_ext, ii_cpack = i_checkout, i_config, i_ext, i_cpack
          if str(vext) != '1':
              ii_ext = i_inst
          else:
              if ii_checkout == check_icon or ii_checkout == error_icon or ii_checkout == majorwarn_icon or ii_checkout == minorwarn_icon:
                  ii_checkout = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_checkout.html' + "\">" + i_checkout + "</a>"
              if ii_ext == check_icon or ii_ext == error_icon or ii_ext == majorwarn_icon or ii_ext == minorwarn_icon:
                  ii_ext = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_externals_build.html' + "\">" + i_ext + "</a>"
              if ii_config == check_icon or ii_config == error_icon or ii_config == majorwarn_icon or ii_config == minorwarn_icon:
                  ii_config = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_cmake_config.html' + "\">" + i_config + "</a>"
              if ii_cpack == check_icon or ii_cpack == error_icon or ii_cpack == majorwarn_icon or ii_cpack == minorwarn_icon:
                  ii_cpack = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_cpack_combined.html' + "\">" + i_cpack + "</a>"

          if reslt1[0][2] == 'ABORT' or reslt1[0][2] == 'abort':
              row_cand = [rname, t_start, ii_checkout, ii_ext, ii_config, 'ABORTED', 'N/A', 'N/A', 'N/A', 'N/A',
                          'N/A', 'N/A', 'N/A', 'N/A', hname]
              rows_s.append(row_cand)
          else:
              row_cand=[rname,t_start,'NO NEW<BR>CODE','N/A','N/A','CANCELLED','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',hname]
              rows_s.append(row_cand)
      else: 
          query1="select to_char(jid),projname,ncompl,pccompl,npb,ner,pcpb,pcer from cstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = '%s' order by projname" % (jid_sel)
          new_cur.execute(query1)
          reslt_addtl = new_cur.fetchall()
          lenres_addtl=len(reslt_addtl)
          if lenres_addtl != 0:
              dict_jid01={}
              for row02 in reslt_addtl:
#                  print("------", row02[0],row02[1],row02[2])
                  pj2=row02[1]
                  if not pj2 in dict_jid01 : dict_jid01[pj2]=[]
                  dict_jid01[pj2]=[row02[2],row02[3],row02[5],row02[4],row02[7],row02[6]]
#
              query2="select to_char(jid),projname,ncompl,pccompl,npb,ner,pcpb,pcer from tstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = '%s' order by projname" % (jid_sel)
              new_cur.execute(query2)
              reslt2 = new_cur.fetchall()
              dict_jid02={}
              for row02 in reslt2:
#                  print("=======", row02[0],row02[1],row02[2])
                  pj2=row02[1]
                  if not pj2 in dict_jid02 : dict_jid02[pj2]=[]
                  dict_jid02[pj2]=[row02[2],row02[3],row02[5],row02[4],row02[7],row02[6]]
              reslt2 = {}
              for row01 in reslt1:
#                  print("JID",row01[0])  
                  pjname=row01[1]  
                  erla=row01[17]
                  if erla == None or erla == '': erla='N/A'
                  sula=row01[18]
                  if sula == None or sula == '': sula='N/A'
                  wala=row01[19]
                  if wala == None or wala == '': wala = 'N/A'
                  if wala.isdigit() and sula.isdigit():
                      sula = str(int(sula) - int(wala))
                  e_cpack = row01[20]
                  if e_cpack == None or e_cpack == '': e_cpack='N/A'
                  s_cpack=row01[21]
                  if s_cpack == None or s_cpack == '': s_cpack='N/A'
                  s_ext = row01[23]
                  if s_ext == None or s_ext == '': s_ext = 'N/A'
                  vext = row01[24]
                  if vext == None or vext == '': vext = '0'
                  hname=row01[25]
                  if re.search(r'\.', hname):
                      hname = (re.split(r'\.', hname))[0]
                  area_suffix = reslt1[0][9]
                  if area_suffix == None: area_suffix = "";
                  t_cv_serv=row01[7]
                  t_cv_clie=row01[8]
                  s_cv_serv=row01[10]
                  s_cv_clie=row01[11]
                  nccompl='0';cpccompl='0';nc_er='0';nc_pb='0';cpcer='0';cpcpb='0'
                  if pjname in dict_jid01 :
                      nccompl=dict_jid01[pjname][0]
                      cpccompl=dict_jid01[pjname][1]
                      nc_er=dict_jid01[pjname][2]
                      nc_pb=dict_jid01[pjname][3]
                      if nccompl == None or nccompl == 'N/A' or nccompl <= 0:
                          nc_er='N/A'
                          nc_pb='N/A'
                      cpcer=dict_jid01[pjname][4]
                      cpcpb=dict_jid01[pjname][5]
                  ntcompl='0';tpccompl='0';nt_er='0';nt_pb='0';tpcer='0';tpcpb='0'
                  if pjname in dict_jid02 :
                      ntcompl=dict_jid02[pjname][0]
                      tpccompl=dict_jid02[pjname][1]
                      nt_er=dict_jid02[pjname][2]
                      nt_pb=dict_jid02[pjname][3]
                      if ntcompl == None or ntcompl == 'N/A' or ntcompl <= 0: 
                          nt_er='N/A'
                          nt_pb='N/A'
                      tpcer=dict_jid02[pjname][4]
                      tpcpb=dict_jid02[pjname][5]
#                  [tpcer_s,tpcpb_s]=map(lambda c: 100 - c, [tpcer,tpcpb])
#                  [tpcer_sf,tpcpb_sf]=map(lambda c: format(c,'.1f'), [tpcer_s,tpcpb_s])
                  s_checkout='N/A'
                  if row01[14] != None: s_checkout=str(row01[14])
                  s_config='N/A'; s_inst='N/A'
                  if str(vext) != 1: s_config='0'; s_inst='0'
                  if row01[12] != None: s_config=str(row01[12])
                  if row01[13] != None: s_inst=str(row01[13])
                  e_cpack = row01[20]
                  if e_cpack == None or e_cpack == '': e_cpack = 'N/A'
                  s_cpack = row01[21]
                  if s_cpack == None or s_cpack == '': s_cpack = 'N/A'
                  t_build='N/A'
                  if row01[3] != None: t_build=row01[3].strftime('%Y/%m/%d %H:%M')
                  t_test='N/A'
                  if row01[5] != None: t_test=row01[5].strftime('%Y/%m/%d %H:%M')
                  tt_cv_serv='N/A'
                  if t_cv_serv != None and t_cv_serv != '': tt_cv_serv=t_cv_serv.strftime('%Y/%m/%d %H:%M')
                  tt_cv_clie='N/A'
                  if t_cv_clie != None and t_cv_clie != '': tt_cv_clie=t_cv_clie.strftime('%Y/%m/%d %H:%M')
                  ss_cv_serv='N/A'
                  if s_cv_serv != None and s_cv_serv != '': ss_cv_serv=str(s_cv_serv)
                  ss_cv_clie='N/A'
                  if s_cv_clie != None and s_cv_clie != '': ss_cv_clie=str(s_cv_clie)
#
                  combo_c=str(nc_er)+' ('+str(nc_pb)+')'  
                  combo_t=str(nt_er)+' ('+str(nt_pb)+')'
                  if nt_er == 'N/A': combo_t='N/A(N/A)'
#                  mrlink_a="<a href=\""+mrlink+"\">"+gitbr+"</a>" 
                  [i_checkout,i_inst,i_config,i_cv_serv,i_cv_clie,i_ext,i_cpack]=\
                      map(lambda x: di_res.get(str(x),str(x)), [s_checkout,s_inst,s_config,ss_cv_serv,ss_cv_clie,s_ext,s_cpack])
                  if i_checkout == None or i_checkout == "None" : i_checkout=radiooff_icon; 
                  if i_inst == None or i_inst == "None" : i_inst=radiooff_icon;
                  if i_config == None or i_config == "None" : i_config=radiooff_icon;
                  if i_ext == None or i_ext == "None" : i_ext=radiooff_icon;
                  if i_cpack == None or i_cpack == "None": i_cpack = radiooff_icon;
                  ii_checkout, ii_config, ii_ext, ii_cpack = i_checkout, i_config, i_ext, i_cpack
                  if str(vext) != '1' :
                      ii_ext = i_inst
                      if e_cpack != 'N/A':
                          if isinstance(e_cpack, datetime):
                              ii_cpack = ii_cpack + " " + e_cpack.strftime('%d-%b %H:%M').upper()
                  else :
                      if ii_checkout == check_icon or ii_checkout == error_icon or ii_checkout == majorwarn_icon or ii_checkout == minorwarn_icon:
                          ii_checkout = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_checkout.html' + "\">" + i_checkout + "</a>"
                      if ii_ext == check_icon or ii_ext == error_icon or ii_ext == majorwarn_icon or ii_ext == minorwarn_icon:
                          ii_ext = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_externals_build.html' + "\">" + i_ext + "</a>"
                      if ii_config == check_icon or ii_config == error_icon or ii_config == majorwarn_icon or ii_config == minorwarn_icon:
                          ii_config = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_cmake_config.html' + "\">" + i_config + "</a>"
                      if ii_cpack == check_icon or ii_cpack == error_icon or ii_cpack == majorwarn_icon or ii_cpack == minorwarn_icon:
                          ii_cpack = "<a href=\"" + webarea_cur + os.sep + 'ardoc_web_area' + area_suffix + os.sep + 'ARDOC_Log_' + rname_trun + os.sep + 'ardoc_cpack_combined.html' + "\">" + i_cpack + "</a>"
#                         DO NOT DISPLAY CPack completion time as its accuracy in the db is not guaranteed
#                          if e_cpack != 'N/A':
#                              if isinstance(e_cpack, datetime):
#                                  ii_cpack = ii_cpack + " " + e_cpack.strftime('%d-%b %H:%M').upper()
                  link_to_testsRes=reverse('TestsRes')
                  link_to_compsRes=reverse('CompsRes')
                  i_combo_t="<a href=\""+link_to_testsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"&proj="+pjname+"\">"+combo_t+"</a>"
                  if combo_t == 'N/A(N/A)': i_combo_t=combo_t
                  i_combo_c="<a href=\""+link_to_compsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"&proj="+pjname+"\">"+combo_c+"</a>"
                  if tt_cv_serv != 'N/A' : i_combo_cv_serv=tt_cv_serv+i_cv_serv
                  else: i_combo_cv_serv=i_cv_serv
                  if tt_cv_clie != 'N/A' : i_combo_cv_clie=tt_cv_clie+i_cv_clie
                  else: i_combo_cv_clie=i_cv_clie

                  key_cache_transf=nname + '_' + rname
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
                      local_art_res += '<B><span style="color: green">' + str(sula) + '</span></B>,'
                      local_art_res += '<B><span style="color: brown">' + str(wala) + '</span></B>,'
                      local_art_res += '<B><span style="color: red">' + str(erla) + '</span></B>'
                      branch = re.split('_', nname)[0]
                      # Local art results template: https://atlas-art-data.web.cern.ch/atlas-art-data/local-output/<branch>/<nightly_tag>/
                      loares = '<a href="' + lartwebarea + "/" + branch + "/" + pjname + "/" + ar_sel + "/" + rname + '/">'
                      local_art_res = loares + local_art_res + "</a>"

                  row_cand=[rname,t_start,ii_checkout,ii_ext,ii_config,t_build,i_combo_c,ii_cpack,t_test,i_combo_t,local_art_res,val_cache_transf,i_combo_cv_serv,tt_cv_clie,hname]
                  rows_s.append(row_cand)

    data = {
        "nightly": nname,
        "rel": rname,
        "platform": ar_sel,
        "project": pjname,
        'viewParams': request.session['viewParams'],
        'rows_s':json.dumps(rows_s, cls=DateEncoder)
    }
    return render(request,'nviewDemo.html', data, content_type='text/html')

