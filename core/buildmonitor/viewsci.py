import json, re, os

from django.shortcuts import render
from core.views import initRequest
from django.db import connection
from django.http import JsonResponse
from django.urls import reverse
from core.libs.DateEncoder import DateEncoder
from core.utils import is_json_request


def civiewDemo(request):
    """
    CI Builds Summary
    :param request: 
    :return: 
    """
    valid, response = initRequest(request)
    if not valid:
        return response
    
    if 'nightly' in request.session['requestParams'] and len(request.session['requestParams']['nightly']) < 100:
        nname = request.session['requestParams']['nightly']
    else:
        nname = 'MR-CI-builds'
    if 'rel' in request.session['requestParams'] and len(request.session['requestParams']['rel']) < 100:
        rname = request.session['requestParams']['rel']
    else:
        rname = '*'
    pjname = '*'

    new_cur = connection.cursor()
    check_icon='<div class="ui-widget ui-state-check" style="display:inline-block;"> <span style="display:inline-block;" title="OK" class="DataTables_sort_icon css_right ui-icon ui-icon-circle-check">ICON33</span></div>'
    clock_icon='<div class="ui-widget ui-state-hover" style="display:inline-block;"> <span style="display:inline-block;" title="UPDATING" class="DataTables_sort_icon css_right ui-icon ui-icon-clock">ICON39</span></div>'
    minorwarn_icon='<div class="ui-widget ui-state-highlight" style="display:inline-block;"> <span style="display:inline-block;" title="MINOR WARNING" class="DataTables_sort_icon css_right ui-icon ui-icon-alert">ICON34</span></div>'
    warn_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span style="display:inline-block;" title="WARNING" class="DataTables_sort_icon css_right ui-icon ui-icon-lightbulb">ICON35</span></div>'
    error_icon='<div class="ui-widget ui-state-error" style="display:inline-block;"> <span style="display:inline-block;" title="ERROR" class="DataTables_sort_icon css_right ui-icon ui-icon-circle-close">ICON36</span></div>'
    help_icon='<span style="display:inline-block;" title="HELP" class="DataTables_sort_icon css_right ui-icon ui-icon-help">ICONH</span>'
    clip_icon='<span style="display:inline-block; float: left; margin-right: .9em;" title="CLIP" class="DataTables_sort_icon ui-icon ui-icon-clipboard">ICONCL</span>'
    person_icon='<span style="display:inline-block; float: left; margin-right: .9em;" title="MASTER ROLE" class="DataTables_sort_icon ui-icon ui-icon-person">ICONP</span>'
    person_icon1='<span style="display:inline-block;" title="MASTER ROLE" class="DataTables_sort_icon ui-icon ui-icon-person">ICONP1</span>'
    mailyes_icon='<span style="cursor:pointer; display:inline-block; " title="MAIL ENABLED" class="ui-icon ui-icon-mail-closed"> ICON1</span>'
    radiooff_icon='<div class="ui-widget ui-state-default" style="display:inline-block";> <span title="N/A" class="ui-icon ui-icon-radio-off">ICONRO</span></div>'
    majorwarn_icon = warn_icon
    di_res = {
        '-1': clock_icon,
        'N/A': radiooff_icon,
        '0': check_icon,
        '1': error_icon,
        '2': majorwarn_icon,
        '3': error_icon,
        '4': minorwarn_icon,
        '10':clock_icon
    }

    query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and tstamp between sysdate-7+1/24 and sysdate order by tstamp desc" % nname
    if rname != '*':
        query="select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH where nname ='%s' and name ='%s' and tstamp between sysdate-7+1/24 and sysdate order by tstamp desc" % (nname,rname)

    ####HEADERS
    header = [
        'Release',
        'Platform',
        'Project',
        'git branch<BR>(link to MR)',
        'Job time stamp',
        'git clone',
        'Externals build',
        'CMake config',
        'Build time',
        'Comp. Errors (w/warnings)',
        'Test time',
        'CI tests errors (w/warnings)',
        'Host',
    ]
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
        rname_trun = re.sub(r'\([^)]*\)', '', rname)
        webarea_cur = row[5]
        if webarea_cur is None: webarea_cur = "";
        job_start = row[8]
        mrlink = row[7]
        gitbr = row[13]
        dict_p={'jid' : jid_sel}
        query1="""
            select to_char(jid),projname,econf,eb,sb,ei,si,eafs,safs,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco,hname 
            from jobstat@ATLR.CERN.CH natural 
                join cstat@ATLR.CERN.CH natural 
                join projects@ATLR.CERN.CH 
            where jid = :jid order by projname
        """
        new_cur.execute(query1, dict_p)
        reslt1 = new_cur.fetchall()
        lenres = len(reslt1)

        dict_p = {'jid': jid_sel}
        query2 = """
            select to_char(jid),projname,econf,eb,sb,ei,si,eafs,safs,ekit,skit,erpm,srpm,ncompl,pccompl,npb,ner,pcpb,pcer,suff,skitinst,skitkv,scv,scvkv,scb,sib,sco 
            from jobstat@ATLR.CERN.CH natural 
                join tstat@ATLR.CERN.CH natural 
                join projects@ATLR.CERN.CH 
            where jid = :jid order by projname
            """
        new_cur.execute(query2, dict_p)
        reslt2 = new_cur.fetchall()
        dict_jid02={}
        for row02 in reslt2:
          pj2=row02[1]
          if not pj2 in dict_jid02 :
              dict_jid02[pj2]=[]
          dict_jid02[pj2]=[row02[13],row02[14],row02[16],row02[15],row02[18],row02[17]]

        for row01 in reslt1:
          nccompl=row01[13]
          cpccompl=row01[14]
          nc_er=row01[16]
          nc_pb=row01[15]
          cpcer=row01[18]
          cpcpb=row01[17]
          area_suffix=row01[19]
          hname=row01[27]
          if re.search(r'\.',hname):
            hname=(re.split(r'\.',hname))[0]
          if area_suffix is None:
              area_suffix = ""
          pjname=row01[1]
          ntcompl='0'
          tpccompl='0'
          nt_er='0'
          nt_pb='0'
          tpcer='0'
          tpcpb='0'
          if pjname in dict_jid02 :
              ntcompl=dict_jid02[pjname][0]
              tpccompl=dict_jid02[pjname][1]
              nt_er=dict_jid02[pjname][2]
              nt_pb=dict_jid02[pjname][3]
              if ntcompl is None or ntcompl == 'N/A' or ntcompl <= 0:
                  nt_er='N/A'
                  nt_pb='N/A'
              tpcer=dict_jid02[pjname][4]
              tpcpb=dict_jid02[pjname][5]
          s_checkout='0'
          if row01[26] is not None:
              s_checkout=str(row01[26])
          s_config='0'
          if row01[24] is not None:
              s_config=str(row01[24])
          s_inst='0'
          if row01[25] is not None:
              s_inst=str(row01[25])
          t_bstart='N/A'
          if row01[3] is not None:
              t_bstart=row01[3].strftime('%Y/%m/%d %H:%M')
          t_test='N/A'
          if row01[5] is not None:
              t_test=row01[5].strftime('%Y/%m/%d %H:%M')
          t_start='N/A'
          if job_start is not None:
              t_start=job_start.strftime('%Y/%m/%d %H:%M')
          build_time_cell=t_bstart + '===' + s_checkout + s_config + s_inst
          combo_c=str(nc_er)+' ('+str(nc_pb)+')'
          combo_t=str(nt_er)+' ('+str(nt_pb)+')'
          if nt_er == 'N/A':
              combo_t='N/A(N/A)'
          mrlink_a = "<a href=\""+mrlink+"\">"+gitbr+"</a>"
          [i_checkout,i_inst,i_config]=map(lambda x: di_res.get(str(x),str(x)), [s_checkout,s_inst,s_config])
          if i_checkout is None or i_checkout == "None":
              i_checkout=radiooff_icon
          if i_inst is None or i_inst == "None" :
              i_inst=radiooff_icon
          if i_config is None or i_config == "None" :
              i_config=radiooff_icon
          ii_checkout,ii_inst,ii_config=i_checkout,i_inst,i_config
          if ii_checkout == check_icon or ii_checkout == error_icon or ii_checkout == majorwarn_icon or ii_checkout == minorwarn_icon:
              ii_checkout = "<a href=\""+webarea_cur+os.sep+'nicos_web_area'+area_suffix+os.sep+'NICOS_Log_'+rname_trun+os.sep+'nicos_checkout.html'+"\">"+i_checkout+"</a>"
          if ii_inst == check_icon or ii_inst == error_icon or ii_inst == majorwarn_icon or ii_inst == minorwarn_icon:
              ii_inst = "<a href=\""+webarea_cur+os.sep+'nicos_web_area'+area_suffix+os.sep+'NICOS_Log_'+rname_trun+os.sep+'nicos_instbuild.html'+"\">"+i_inst+"</a>"
          if ii_config == check_icon or ii_config == error_icon or ii_config == majorwarn_icon or ii_config == minorwarn_icon:
              ii_config = "<a href=\""+webarea_cur+os.sep+'nicos_web_area'+area_suffix+os.sep+'NICOS_Log_'+rname_trun+os.sep+'nicos_confbuild.html'+"\">"+i_config+"</a>"
          link_to_testsRes = reverse('TestsRes')
          link_to_compsRes = reverse('CompsRes')
          i_combo_t="<a href=\""+link_to_testsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"&proj="+pjname+"\">"+combo_t+"</a>"
          if combo_t == 'N/A(N/A)': i_combo_t=combo_t
          i_combo_c="<a href=\""+link_to_compsRes+"?nightly="+nname+"&rel="+rname+"&ar="+ar_sel+"&proj="+pjname+"\">"+combo_c+"</a>"
          row_cand=[rname,ar_sel,pjname,mrlink_a,t_start,ii_checkout,ii_inst,ii_config,t_bstart,i_combo_c,t_test,i_combo_t,hname]
          rows_s.append(row_cand)

    if is_json_request(request):
        data = {
            "nightly": nname,
            "rel": rname,
            "project": pjname,
            'rows_s': [header,] + rows_s
        }
        return JsonResponse(data, encoder=DateEncoder, safe=False)
    else:
        data = {
            'request': request,
            'requestParams': request.session['requestParams'],
            'viewParams': request.session['viewParams'],
            "nightly": nname,
            "rel": rname,
            "project": pjname,
            'rows_s': json.dumps(rows_s, cls=DateEncoder)
        }
        return render(request,'civiewDemo.html', data, content_type='text/html')

