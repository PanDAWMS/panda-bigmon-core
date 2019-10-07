from django.shortcuts import render_to_response, redirect
from datetime import datetime
from core.views import initRequest
from django.db import connection, transaction
from django.http import HttpResponse
from django.http import HttpResponseRedirect
import json,re
from collections import defaultdict
from operator import itemgetter, attrgetter

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def compviewDemo(request):
    valid, response = initRequest(request)
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 50:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 50:
        relname = request.session['requestParams']['rel']
    else:
        relname = 'unknown'
    if 'ar' in request.session['requestParams'] and len(request.session['requestParams']['ar']) < 50:
        arname = request.session['requestParams']['ar']
    else:
        arname = 'x86_64-slc6-gcc62-opt'

    data={"nightly": nname, "rel": relname, "ar": arname, "ab123": 'abcd', 'viewParams': request.session['viewParams']}
    return render_to_response('compviewDemo.html', data, content_type='text/html') 

def compviewData(request):
    valid, response = initRequest(request)
    new_cur = connection.cursor()
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 50:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 50:
        relname = request.session['requestParams']['rel']
    else:
        relname = 'unknown'
    if 'ar' in request.session['requestParams'] and len(request.session['requestParams']['ar']) < 50:
        arname = request.session['requestParams']['ar']
    else:
        arname = 'x86_64-slc6-gcc62-opt'

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
    di_res={'-1':clock_icon,'N/A':radiooff_icon,'0':check_icon,'1':minorwarn_icon,'2':majorwarn_icon,'3':error_icon,'10':clock_icon}
    asdd='0'
    if asdd == '0':
     query="select * from (select to_char(j.jid),j.arch||'-'||os||'-'||comp||'-'||opt as AA, j.tstamp, n.nname as nname, r.name as RNAME, s.hname, j.buildarea, j.copyarea, r.relnstamp, j.gitbr from nightlies@ATLR.CERN.CH n inner join ( releases@ATLR.CERN.CH r inner join ( jobs@ATLR.CERN.CH j inner join jobstat@ATLR.CERN.CH s on j.jid=s.jid ) on r.nid=j.nid and r.relid=j.relid ) on n.nid=r.nid where nname ='%s' and j.tstamp between sysdate-7+1/24 and sysdate order by j.tstamp asc) where RNAME ='%s' and AA='%s'" % (nname,relname,arname)
#     print("Q ",query)
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
        tabname='compresults'
        query1="select res,projname,nameln,contname,corder from " + tabname + "@ATLR.CERN.CH natural join jobstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH natural join packages@ATLR.CERN.CH where jid ='%s'" % jid_top
        new_cur.execute(query1)
        reslt1 = new_cur.fetchall()
     relextend=relname
     if re.search('ATN',nname): relextend=relnstamp+'('+relname+')'
     if re.search('CI',nname):
        sComm='git branch '+gitbrSS
        cmmnt='ATLAS CI %s, release %s, platform %s (on %s)<BR><span style="font-size:  smaller">%s</span>' % ( nname, relextend, arname, host, sComm )
     else:
        cmmnt='ATLAS nightly %s, release %s, platform %s (on %s)' % ( nname, relextend, arname, host)
#  HEADERS
#  1. RES
#  2. PROJECT
#  3. Package 
#  4. Container
     i=0
     rows_s = []
     for row in reslt1:
      i+=1
      if i > 10000: break
      result=row[0]
      proj=row[1]
      nameln=row[2]
      container=row[3]
      corder=row[4]
      i_result=di_res.get(str(result),str(result))
      if i_result == None or i_result == "None" : i_result=radiooff_icon; 
      row_cand=[i_result,proj,nameln,container]
      rows_s.append(row_cand)
    rows_s.append(row_cand)
    return HttpResponse(json.dumps(rows_s, cls=DateEncoder), content_type='text/html')

