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


def globalviewDemo(request):
    valid, response = initRequest(request)
    nname = '20.20.X-VAL'
    data={"nightly": nname, 'viewParams': request.session['viewParams']}
    return render_to_response('globalviewDemo.html', data, content_type='text/html') 

def globalviewData(request):
    valid, response = initRequest(request)
    new_cur = connection.cursor()
    query="""
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
a.relnstamp as \"TMSTAMP\"
from nightlies@ATLR.CERN.CH n inner join
  ( releases@ATLR.CERN.CH a inner join
    ( jobstat@ATLR.CERN.CH j inner join projects@ATLR.CERN.CH p on j.projid = p.projid) on a.nid=j.nid and a.relid=j.relid )
  on n.nid=a.nid,
(select arch||'-'||os||'-'||comp||'-'||opt as pl, jid from jobs@ATLR.CERN.CH ) platf,
(select ncompl as nc, ner as nc_er, npb as nc_pb, jid, projid from cstat@ATLR.CERN.CH ) cs,
(select ncompl as nt, ner as nt_er, npb as nt_pb, jid, projid from tstat@ATLR.CERN.CH ) ts
 WHERE
j.jid BETWEEN to_number(to_char(SYSDATE-6, 'YYYYMMDD'))*10000000
 AND to_number(to_char(SYSDATE, 'YYYYMMDD')+1)*10000000
 AND j.jid = platf.jid
 AND j.jid = cs.jid and j.projid = cs.projid
 AND j.jid = ts.jid and j.projid = ts.projid
 AND j.begdate between sysdate-6 and sysdate
 AND j.eb is not  NULL order by j.eb desc
      """
    new_cur.execute(query)
    result = new_cur.fetchall()    
    first_row=result[0]
#    hdrs=['Branch', 'Recent<BR>Release', 'Build time', 'Ave.Compilation<BR>Error(w/warnings)','CTest or ATN tests<BR>(no warings)','ORDER']
    rows_s = []
    dd=defaultdict(list) 
    for row1 in result:
        if row1[0] not in dd:
           if row1[23] == 1: # LAST PROJECT 
               dd[row1[0]] += row1[1:]
        else:
           if row1[24] == dd[row1[0]][23]:
               dd[row1[0]][15] += row1[16]
               dd[row1[0]][16] += row1[17]
               dd[row1[0]][17] += row1[18]
               dd[row1[0]][18] += row1[19]
               dd[row1[0]][19] += row1[20]
               dd[row1[0]][20] += row1[21] 
    reslt2=[]
    dict_g={'SIMULATION':'BAC','TIER0':'BAAC','TRIGGER_RELEASE':'BAB','CI':'AA0','MASTER':'AA01','GIT':'AA1','ATN':'AA2','ATN_TESTS':'AA3','ATN(UNDER_CONSTRUCTION)':'AA4','DEVELOPMENT':'AA5','HLT_POINT1':'D','PATCH':'EA','PHYSICSANALYSIS':'EB','UPGRADE_STUDIES':'CC','ROOT6_INTEGRATION':'AAA','OTHER':'GB','DOXYGEN':'Z','19.0.X_BUGFIX':'BD','19.1.X_BUGFIX':'BC','19.2.X_BUGFIX':'BB','GAUDI_HIVE':'BAAA','UPGRADE_INTEGRATION':'BAAB'}
    for k,v in dd.items():
        ar1=[]
#        k11='<a href="http://atlas-nightlies-browser.cern.ch/~platinum/nightlies/info?tp=g&nightly='+k+'">'+k+'</a>'
#        if re.match('^.*CI.*$',k): k11=k
        k11=k
        ar1.append(k11)
        row10u=v[0].upper()
        name_code=dict_g.get(row10u,'Y'+row10u)
        v[0]=row10u
#        v[13]='<a href="http://atlas-nightlies-browser.cern.ch/~platinum/nightlies/info?tp=g&nightly='+k+'&rel='+v[13]+'&ar=*">'+v[13]+'</a>'
        ar1.extend(v)
        v38='N/A'; v39='N/A';v38s='N/A'; v39s='N/A';
        m_tcompl=v[18]
        if m_tcompl != None and m_tcompl != 'N/A' and m_tcompl > 0:
            nt=max(1,v[18])
#            print('vv ',str(nt)+' * '+str(v[19])+' * '+str(v[20])) 
            v38=float(100.-v[19]*100./nt)
            v39=float(100.-v[20]*100./nt)
            v38s=format(v38,'.0f')
            v39s=format(v39,'.0f')
        ar1.extend([v38s,v39s])
        ar1.append(name_code)
        reslt2.append(ar1)
        lar1=len(ar1)
#    print(reslt2)
    reslt3=[]
    for row in reslt2:
        list9=[]    
        a0001=str(row[17])+' ('+str(row[18])+')'
        a0002=str(row[27])+' ('+str(row[28])+')'
        brname=row[0]
        link_brname=brname
        link_to_ciInfo=reverse('BuildCI')
        link_to_nInfo=reverse('BuildN')
        if re.match('^.*CI.*$',brname): 
            link_brname="<a href=\""+link_to_ciInfo+"\">"+brname+"</a>"
        else:
            link_brname="<a href=\""+link_to_nInfo+"?nightly="+brname+"\">"+brname+"</a>"
        list9.append(row[1]);list9.append(link_brname);list9.append(row[14]);list9.append(row[9]);list9.append(a0001);list9.append(a0002);list9.append(row[29]); 
        reslt3.append(list9) 
    return HttpResponse(json.dumps(reslt3, cls=DateEncoder), content_type='text/html')
    #return render_to_response('demonstrator.html', data, content_type='text/html')
