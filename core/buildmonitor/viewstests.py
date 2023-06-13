from django.shortcuts import render, redirect
from datetime import datetime
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
import json,re
from collections import defaultdict
from operator import itemgetter, attrgetter
from core.libs.DateEncoder import DateEncoder


def testviewDemo(request):
    valid, response = initRequest(request)
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 100:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 100:
        relname = request.session['requestParams']['rel']
    else:
        relname = 'unknown'
    if 'ar' in request.session['requestParams'] and len(request.session['requestParams']['ar']) < 100:
        arname = request.session['requestParams']['ar']
    else:
        arname = 'x86_64-centos7-gcc11-opt'
    if 'proj' in request.session['requestParams'] and len(request.session['requestParams']['proj']) < 100:
        pjname = request.session['requestParams']['proj']
    else:
        pjname = 'all'

    new_cur = connection.cursor()
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
    person_icon='<span style="display:inline-block; float: left; margin-right: .9em;" title=\
"MASTER ROLE" class="DataTables_sort_icon ui-icon ui-icon-person">ICONP</span>'
    person_icon1='<span style="display:inline-block;" title="MASTER ROLE" class="DataTables_\
sort_icon ui-icon ui-icon-person">ICONP1</span>'
    mailyes_icon='<span style="cursor:pointer; display:inline-block; " title="MAIL ENABLED" \
class="ui-icon ui-icon-mail-closed"> ICON1</span>'
    radiooff_icon='<div class="ui-widget ui-state-default" style="display:inline-block";> <s\
pan title="N/A" class="ui-icon ui-icon-radio-off">ICONRO</span></div>'
    blank_icon='<div class="ui-widget ui-state-disabled" style="display:inline-block";> <span\
 title="N/A" class="ui-icon ui-icon-blank">ICONBL</span></div>'
    note_icon='<div class="ui-widget ui-state-disabled" style="display:inline-block";> <span \
title="N/A" class="ui-icon ui-icon-note">ICONNT</span></div>'
    cancel_icon='<div class="ui-widget ui-state-check" style="display:inline-block";> <span \
title="N/A" class="ui-icon ui-icon-cancel">ICON20</span></div>'
    majorwarn_icon=warn_icon
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':minorwarn_icon,'2':majorwarn_icon,'3':error_icon,'10':clock_icon}
    di_excess={'N/A':radiooff_icon,'0':blank_icon,'1':note_icon,'2':cancel_icon}
    query="select * from (select to_char(j.jid),j.arch||'-'||os||'-'||comp||'-'||opt as AA, j.tstamp, n.nname as nname, r.name as RNAME, s.hname, j.buildarea, j.copyarea, r.relnstamp, j.gitbr from nightlies@ATLR.CERN.CH n inner join ( releases@ATLR.CERN.CH r inner join ( jobs@ATLR.CERN.CH j inner join jobstat@ATLR.CERN.CH s on j.jid=s.jid ) on r.nid=j.nid and r.relid=j.relid ) on n.nid=r.nid where nname ='%s' and j.tstamp between sysdate-11+1/24 and sysdate order by j.tstamp asc) where RNAME ='%s' and AA='%s'" % (nname,relname,arname)
#    print("Q ",query)
    new_cur.execute(query)
    reslt = new_cur.fetchall()
    reslt1 = {}
    host='N/A'
    buildareaSS='N/A'
    copyareaSS='N/A'
    gitbrSS='N/A'
    relnstamp=''
    lllr=len(reslt)
    if lllr > 0:
        rowmax=reslt[-1]
        host=re.split('\.',rowmax[5])[0]
        jid_top = rowmax[0]
        buildareaSS = rowmax[6]
        copyareaSS = rowmax[7]
        relnstamp = rowmax[8]
        if gitbrSS != None : gitbrSS=rowmax[9]
        tabname='testresults'
        if pjname == '*' or re.match('^all$', pjname, re.IGNORECASE):
            query1 = "select res,projname,nameln,pname,mngrs,contname,to_char(tstamp, 'RR/MM/DD HH24:MI'),\
             fname as tstamp,wdirln,excess from " + tabname + "@ATLR.CERN.CH natural join jobstat@ATLR.CERN.CH\
             natural join projects@ATLR.CERN.CH natural join packages@ATLR.CERN.CH where jid ='%s'" % jid_top
        else:
            query1 = "select res,projname,nameln,pname,mngrs,contname,to_char(tstamp, 'RR/MM/DD HH24:MI'),\
             fname as tstamp,wdirln,excess from " + tabname + "@ATLR.CERN.CH natural join jobstat@ATLR.CERN.CH\
             natural join projects@ATLR.CERN.CH natural join packages@ATLR.CERN.CH where jid ='%s' and projname ='%s'" % (jid_top, pjname)
        new_cur.execute(query1)
        reslt1 = new_cur.fetchall()
    relextend=relname
    if re.search('ATN',nname): relextend=relnstamp+'('+relname+')'
    CI_flag=False
    if re.search('CI',nname):
        CI_flag = True
        sComm='git branch '+gitbrSS
        cmmnt='ATLAS CI %s, release %s, platform %s (on %s)<BR><span style="font-size:  smaller">%s</span>' % ( nname, relextend, arname, host, sComm )
    else:
        cmmnt='ATLAS nightly %s, release %s, platform %s (on %s)' % ( nname, relextend, arname, host)
#  HEADERS
#  1. RES
#  2. PROJECT
#  3. Test name#file 
#  4. Optional or Required
#  5. Container
#  6. Time
    i=0
    rows_s = []
    for row in reslt1:
      i+=1
      if i > 10000: break
      result=row[0]
      proj=row[1]
      nameln=row[2]
      category='Required'
      if row[3] == 'OptionalTests': category='Optional'
      container=row[5]
      ttime=row[6]
      fname=row[7]
      excess_flag=row[9]
      i_result=di_res.get(str(result),result)
      if i_result == None or i_result == "None" :
          i_result=radiooff_icon;
      elif CI_flag:
          exc_result=di_excess.get(str(excess_flag),excess_flag)
          if exc_result == None or exc_result == "None":
              exc_result = blank_icon;
          if exc_result == cancel_icon:
              i_result = exc_result
          else:
              i_result=i_result+blank_icon+exc_result
      nameln1=nameln
      if fname != None and fname != '':
          nameln1=re.sub(fname+'#','',nameln,1)
      row_cand=[i_result,proj,nameln1,category,container,ttime]
      rows_s.append(row_cand)
    data={"nightly": nname, "rel": relname, "ar": arname, "proj": pjname, "cicon": cancel_icon, "nicon": note_icon, 'viewParams': request.session['viewParams'],'rows_s':json.dumps(rows_s, cls=DateEncoder)}
    return render(request,'testviewDemo.html', data, content_type='text/html')


