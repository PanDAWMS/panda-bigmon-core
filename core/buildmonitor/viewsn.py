from django.shortcuts import render, redirect
from datetime import datetime
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.core.cache import cache
import json, re
from collections import defaultdict
from operator import itemgetter, attrgetter

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def nviewDemo(request):
    valid, response = initRequest(request)
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 100:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'master_Athena_x86_64-centos7-gcc8-opt'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 100:
        rname = request.session['requestParams']['rel']
    else:
        rname = '*'

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

    query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and tstamp between sysdate-11+1/24 and sysdate order by tstamp desc" % nname
    if rname != '*':
      query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and name ='%s' and tstamp between sysdate-11+1/24 and sysdate order by tstamp desc" % (nname,rname)
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
    new_cur.execute(query)
    result = new_cur.fetchall()
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':error_icon,'2':majorwarn_icon,'3':error_icon,'4':minorwarn_icon,'10':clock_icon}

    dict_cache_transf={}
    for k46, v46 in dict_from_cache.items():
        for kk, vv in v46.items():
            kk_transf = re.sub('/','_',k46)
            key_transf = kk_transf+'_'+kk
            string_vv = '<span style="color: blue">' + str(vv['active']) + '</span>'
            string_vv = string_vv + ',<B><span style="color: green">'+ str(vv['done']) +'</span></B>,'
            string_vv = string_vv + '<span style="color: maroon">' + str(vv['finished']) + '</span>'
            string_vv = string_vv +',<B><span style="color: red">' + str(vv['failed']) + '</span></B>'
            dict_cache_transf[key_transf] = [string_vv, k46]
    i=0
    rows_s = []
    for row in result:
      i+=1
      if i > 1000: break
      jid_sel = row[0]
      ar_sel = row[1]
      rname = row[4]
      job_start = row[8]
##      mrlink = row[7]
      gitbr = row[13]
#      print "JID",jid_sel
      dict_p={'jid' : jid_sel}
      query1="select to_char(jid),projname,econf,eb,sb,ei,si,ecv,ecvkv,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco,hname from jobstat@ATLR.CERN.CH natural join cstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = :jid order by projname"
      new_cur.execute(query1, dict_p)
      reslt1 = new_cur.fetchall()
      lenres=len(reslt1)
#      print "length1 ", lenres
#      nccompl=reslt1[0][13]
#      cpccompl=reslt1[0][14]
#      nc_er=reslt1[0][16]
#      nc_pb=reslt1[0][15]
#      cpcer=reslt1[0][18]
#      cpcpb=reslt1[0][17]
#      pjname=reslt1[0][1]
#      print "========= ",nccompl,cpccompl,nc_pb,nc_er,cpcer,cpcpb
      dict_p={'jid' : jid_sel}
      query2="select to_char(jid),projname,econf,eb,sb,ei,si,ecv,ecvkv,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco from jobstat@ATLR.CERN.CH natural join tstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = :jid order by projname"
      new_cur.execute(query2, dict_p)
      reslt2 = new_cur.fetchall()
      dict_jid02={}
      for row02 in reslt2:
#          print("=======", row02[0],row02[1],row02[1],row02[13])
          pj2=row02[1]
          if not pj2 in dict_jid02 : dict_jid02[pj2]=[]
          dict_jid02[pj2]=[row02[13],row02[14],row02[16],row02[15],row02[18],row02[17]]
      reslt2 = {}
#      ntcompl=reslt2[0][13]
#      tpccompl=reslt2[0][14]
#      nt_er=reslt2[0][16]
#      nt_pb=reslt2[0][15]
#      tpcer=reslt2[0][18]
#      tpcpb=reslt2[0][17]
#      print "========= ",ntcompl,tpccompl,nt_pb,nt_er,tpcer,tpcpb
      for row01 in reslt1:
          nccompl=row01[13]
          cpccompl=row01[14]
          nc_er=row01[16]
          nc_pb=row01[15]
          cpcer=row01[18]
          cpcpb=row01[17]
          pjname=row01[1]  
          hname=row01[27]
          t_cv_serv=row01[7]
          t_cv_clie=row01[8]
          s_cv_serv=row01[22]
          s_cv_clie=row01[23]
          if re.search(r'\.',hname):
            hname=(re.split(r'\.',hname))[0]
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
#          [tpcer_s,tpcpb_s]=map(lambda c: 100 - c, [tpcer,tpcpb])
#          [tpcer_sf,tpcpb_sf]=map(lambda c: format(c,'.1f'), [tpcer_s,tpcpb_s])
          s_checkout='0'
          if row01[26] != None: s_checkout=str(row01[26])
          s_config='0'
          if row01[24] != None: s_config=str(row01[24])
          s_inst='0'
          if row01[25] != None: s_inst=str(row01[25])
          t_build='N/A'
          if row01[3] != None: t_build=row01[3].strftime('%Y/%m/%d %H:%M')
          t_test='N/A'
          if row01[5] != None: t_test=row01[5].strftime('%Y/%m/%d %H:%M')
          t_start='N/A'
          if job_start != None: t_start=job_start.strftime('%Y/%m/%d %H:%M')
          tt_cv_serv='N/A'
          if t_cv_serv != None and t_cv_serv != '': tt_cv_serv=t_cv_serv.strftime('%Y/%m/%d %H:%M')
          tt_cv_clie='N/A'
          if t_cv_clie != None and t_cv_clie != '': tt_cv_clie=t_cv_clie.strftime('%Y/%m/%d %H:%M')
          ss_cv_serv='N/A'
          if s_cv_serv != None and s_cv_serv != '': ss_cv_serv=str(s_cv_serv)
          ss_cv_clie='N/A'
          if s_cv_clie != None and s_cv_clie != '': ss_cv_clie=str(s_cv_clie)
#
          build_time_cell=t_build+'==='+s_checkout+s_config+s_inst
          combo_c=str(nc_er)+' ('+str(nc_pb)+')'  
          combo_t=str(nt_er)+' ('+str(nt_pb)+')'
          if nt_er == 'N/A': combo_t='N/A(N/A)'
#          mrlink_a="<a href=\""+mrlink+"\">"+gitbr+"</a>" 
          [i_checkout,i_inst,i_config,i_cv_serv,i_cv_clie]=map(lambda x: di_res.get(str(x),str(x)), [s_checkout,s_inst,s_config,ss_cv_serv,ss_cv_clie])
          if i_checkout == None or i_checkout == "None" : i_checkout=radiooff_icon; 
          if i_inst == None or i_inst == "None" : i_inst=radiooff_icon;
          if i_config == None or i_config == "None" : i_config=radiooff_icon;
          link_to_testsRes=reverse('TestsRes')
          link_to_compsRes=reverse('CompsRes')
          i_combo_t="<a href=\""+link_to_testsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"\">"+combo_t+"</a>"
          if combo_t == 'N/A(N/A)': i_combo_t=combo_t
          i_combo_c="<a href=\""+link_to_compsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"\">"+combo_c+"</a>"
          if tt_cv_serv != 'N/A' : i_combo_cv_serv=tt_cv_serv+i_cv_serv
          else: i_combo_cv_serv=i_cv_serv
          if tt_cv_clie != 'N/A' : i_combo_cv_clie=tt_cv_clie+i_cv_clie
          else: i_combo_cv_clie=i_cv_clie

          key_cache_transf=nname + '_' + rname
          val_cache_transf,nightly_name_art=dict_cache_transf.get(key_cache_transf,['N/A','N/A'])
          if val_cache_transf != 'N/A' and nightly_name_art != 'N/A':
              vacasf = "<a href=\"https://bigpanda.cern.ch/art/overview/?branch="
              val_cache_transf = vacasf + nightly_name_art + "&ntag_full=" + rname + "\">" + val_cache_transf + "</a>"

          row_cand=[rname,ar_sel,pjname,t_start,i_checkout,i_inst,i_config,t_build,i_combo_c,t_test,i_combo_t,val_cache_transf,i_combo_cv_serv,tt_cv_clie,hname]
          rows_s.append(row_cand)

    data={"nightly": nname, "rel": rname, 'viewParams': request.session['viewParams'],'rows_s':json.dumps(rows_s, cls=DateEncoder)}
    return render(request,'nviewDemo.html', data, content_type='text/html')

