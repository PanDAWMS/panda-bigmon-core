from django.shortcuts import render_to_response, redirect
from datetime import datetime
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
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


def civiewDemo(request):
    valid, response = initRequest(request)
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 50:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'
#    nname = 'MR-CI-builds'
    data={"nightly": nname}
    return render_to_response('civiewDemo.html', data, content_type='text/html') 

def civiewData(request):
    valid, response = initRequest(request)
    new_cur = connection.cursor()
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 50:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'

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

    query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and tstamp between sysdate-4+1/24 and sysdate order by tstamp desc" % nname
####HEADERS      <th>Release</th>
#                <th>Platform</th>
#                <th># Projects</th>
#                <th>git branch<BR>(link to MR)</th>
#                <th>Job time stamp</th>
#                <th>git clone</th>
#                <th>Externals<BR>build</th>
#                <th>CMake<BR>config</th>
#                <th>Build time</th>
#                <th>Compilation Errors<BR>(w/warnings)</th>
#                <th>Test time</th>
#                <th>Pct. of Successful<BR>CTest tests<BR>(no warnings)</th>
    new_cur.execute(query)
    result = new_cur.fetchall()
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':error_icon,'2':majorwarn_icon,'3':error_icon,'4':minorwarn_icon,'10':clock_icon}
    i=0
    rows_s = []
    for row in result:
      i+=1
      if i > 1000: break
      jid_sel = row[0]
      ar_sel = row[1]
      rname = row[4]
      job_start = row[8]
      mrlink = row[7]
      gitbr = row[13]
#      print "JID",jid_sel
      dict_p={'jid' : jid_sel}
      query1="select to_char(jid),projname,econf,eb,sb,ei,si,eafs,safs,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco,hname from jobstat@ATLR.CERN.CH natural join cstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = :jid order by projname"
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
      query2="select to_char(jid),projname,econf,eb,sb,ei,si,eafs,safs,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco from jobstat@ATLR.CERN.CH natural join tstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = :jid order by projname"
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
          ntcompl='0';tpccompl='0';nt_er='0';nt_pb='0';tpcer='0';tpcpb='0'
          if pjname in dict_jid02 :
              ntcompl=dict_jid02[pjname][0]
              tpccompl=dict_jid02[pjname][1]
              nt_er=dict_jid02[pjname][2]
              nt_pb=dict_jid02[pjname][3]
              tpcer=dict_jid02[pjname][4]
              tpcpb=dict_jid02[pjname][5]
          [tpcer_s,tpcpb_s]=map(lambda c: 100 - c, [tpcer,tpcpb])
          [tpcer_sf,tpcpb_sf]=map(lambda c: format(c,'.1f'), [tpcer_s,tpcpb_s])
          s_checkout='0'
          if row01[26] != None: s_checkout=str(row01[26])
          s_config='0'
          if row01[24] != None: s_config=str(row01[24])
          s_inst='0'
          if row01[25] != None: s_inst=str(row01[25])
          t_bstart='N/A'
          if row01[3] != None: t_bstart=row01[3].strftime('%Y/%m/%d %H:%M')
          t_test='N/A'
          if row01[5] != None: t_test=row01[5].strftime('%Y/%m/%d %H:%M')
          t_start='N/A'
#          if row01[2] != None: t_start=row01[2].strftime('%Y/%m/%d %H:%M')
          if job_start != None: t_start=job_start.strftime('%Y/%m/%d %H:%M')
          build_time_cell=t_bstart+'==='+s_checkout+s_config+s_inst
          combo_c=str(nc_er)+' ('+str(nc_pb)+')'
          combo_t=str(tpcer_sf)+' ('+str(tpcpb_sf)+')'
          mrlink_a="<a href=\""+mrlink+"\">"+gitbr+"</a>" 
          [i_checkout,i_inst,i_config]=map(lambda x: di_res.get(str(x),str(x)), [s_checkout,s_inst,s_config])
          if i_checkout == None or i_checkout == "None" : i_checkout=radiooff_icon; 
          if i_inst == None or i_inst == "None" : i_inst=radiooff_icon;
          if i_config == None or i_config == "None" : i_config=radiooff_icon;
          link_to_testsRes=reverse('TestsRes')
          link_to_compsRes=reverse('CompsRes')
          i_combo_t="<a href=\""+link_to_testsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"\">"+combo_t+"</a>"
          i_combo_c="<a href=\""+link_to_compsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"\">"+combo_c+"</a>"
          row_cand=[rname,ar_sel,pjname,mrlink_a,t_start,i_checkout,i_inst,i_config,t_bstart,i_combo_c,t_test,i_combo_t]
          rows_s.append(row_cand)

    return HttpResponse(json.dumps(rows_s, cls=DateEncoder), content_type='text/html')
####HEADERS  0    <th>Release</th>
#            1    <th>Platform</th>
#            2    <th># Projects</th>
#            3    <th>git branch<BR>(link to MR)</th>
#            4    <th>Job time stamp</th>
#            5    <th>git clone</th>
#            6    <th>Externals<BR>build</th>
#            7    <th>CMake<BR>config</th>
#            8    <th>Build time</th>
#            9    <th>Compilation Errors<BR>(w/warnings)</th>
#           10    <th>Test time</th>
#           11    <th>Pct. of Successful<BR>CTest tests<BR>(no warnings)</th>

