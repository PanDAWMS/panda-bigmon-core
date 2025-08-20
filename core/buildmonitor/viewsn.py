import json
import os
import re
from datetime import datetime

from django.http import JsonResponse

import core.buildmonitor.constants as const
from core.buildmonitor.utils import get_art_test_results
from core.libs.DateEncoder import DateEncoder
from core.oauth.utils import login_customrequired
from core.utils import is_json_request
from core.views import initRequest
from django.db import connection
from django.shortcuts import render
from django.urls import reverse


@login_customrequired
def nviewDemo(request):
    valid, response = initRequest(request)
    if not valid:
        return response
    if "nightly" in request.session["requestParams"] and len(request.session["requestParams"]["nightly"]) < 100:
        nname = request.session["requestParams"]["nightly"]
    else:
        nname = "master_Athena_x86_64-centos7-gcc11-opt"
    if "rel" in request.session["requestParams"] and len(request.session["requestParams"]["rel"]) < 100:
        rname = request.session["requestParams"]["rel"]
    else:
        rname = "*"
    is_json_output = is_json_request(request)
    output_dict = {'nightly': nname, 'rel': rname, 'data': {}}

    # get art test results from cache or DB
    art_results_dict = get_art_test_results(request)

    di_res = {
        "-1": const.CLOCK_ICON,
        "N/A": const.RADIOOFF_ICON,
        "0": const.CHECK_ICON,
        "1": const.ERROR_ICON,
        "2": const.MAJORWARN_ICON,
        "3": const.ERROR_ICON,
        "4": const.MINORWARN_ICON,
        "10": const.CLOCK_ICON,
    }
    new_cur = connection.cursor()
    query_rname_str = f" and name = '{rname}'" if rname != "*" else ""
    query = f"""
        select to_char(jid),arch||'-'||os||'-'||comp||'-'||opt as AA, to_char(tstamp, 'RR/MM/DD HH24:MI') as tstamp, nname, name, 
            webarea, webbuild, gitmrlink, tstamp as tst1,tcrel,tcrelbase,buildarea,relnstamp,gitbr,lartwebarea 
        from NIGHTLIES@ATLR.CERN.CH natural join releases@ATLR.CERN.CH natural join jobs@ATLR.CERN.CH 
        where nname ='{nname}' {query_rname_str} and tstamp between sysdate-11+1/24 and sysdate 
        order by tstamp desc
    """
    # ###HEADERS
    # <th>Release</th>
    # <th>Platform</th>
    # <th>Project</th>
    # <th>Job time stamp</th>
    # <th>git clone</th>
    # <th>Externals<BR>build</th>
    # <th>CMake<BR>config</th>
    # <th>Build time</th>
    # <th>Comp. Errors<BR>(w/warnings)</th>
    # <th>Test time</th>
    # <th>Pct. of Successful<BR>CTest tests<BR>(no warnings)</th>
    # <th>CVMFS time</th>
    # <th>Host</th>
    # <th>CPack</th>
    new_cur.execute(query)
    result = new_cur.fetchall()

    ar_sel = "unknown"
    pjname = "unknown"
    i = 0
    rows_s = []
    for row in result:
        i += 1
        if i > 1000:
            break
        jid_sel = row[0]
        ar_sel = row[1]
        rname = row[4]
        rname_trun = re.sub(r"\([^)]*\)", "", rname)
        webarea_cur = row[5]
        if webarea_cur is None:
            webarea_cur = ""
        job_start = row[8]
        t_start = "N/A"
        if job_start is not None:
            t_start = job_start.strftime("%Y/%m/%d %H:%M")

        query01 = f"""
            select to_char(jid),projname,stat,eb,sb,ei,si,ecv,ecvkv,suff,scv,scvkv,scb,sib,sco,ela,sla,erla,sula,wala,ecp,scp,eext,sext,vext,hname 
            from jobstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH 
            where jid = '{str(jid_sel)}' order by projname"""
        new_cur.execute(query01)
        reslt1 = new_cur.fetchall()
        # taking care of cancelled/aborted jobs
        lenres = len(reslt1)
        if lenres != 0 and reslt1[0][2].lower() in ('cancel', 'abort'):
            pjname = reslt1[0][1]
            s_ext = reslt1[0][23]
            if s_ext is None or s_ext == "":
                s_ext = "N/A"
            vext = reslt1[0][24]
            if vext is None or vext == "":
                vext = "0"
            s_checkout = "N/A"
            if reslt1[0][14] is not None:
                s_checkout = str(reslt1[0][14])
            s_config = "N/A"
            s_inst = "N/A"
            if str(vext) != 1:
                s_config = "0"
                s_inst = "0"
            if reslt1[0][12] is not None:
                s_config = str(reslt1[0][12])
            if reslt1[0][13] is not None:
                s_inst = str(reslt1[0][13])
            s_cpack = "N/A"
            if reslt1[0][21] is not None:
                s_cpack = str(reslt1[0][21])
            hname = reslt1[0][25]
            if re.search(r"\.", hname):
                hname = (re.split(r"\.", hname))[0]
            area_suffix = reslt1[0][9]
            if area_suffix is None:
                area_suffix = ""
            [i_checkout, i_inst, i_config, i_ext, i_cpack] = map(
                lambda x: di_res.get(str(x), str(x)),
                [s_checkout, s_inst, s_config, s_ext, s_cpack],
            )
            if i_checkout is None or i_checkout == "None":
                i_checkout = const.RADIOOFF_ICON
            if i_inst is None or i_inst == "None":
                i_inst = const.RADIOOFF_ICON
            if i_config is None or i_config == "None":
                i_config = const.RADIOOFF_ICON
            if i_ext is None or i_ext == "None":
                i_ext = const.RADIOOFF_ICON
            if i_cpack is None or i_cpack == "None":
                i_cpack = const.RADIOOFF_ICON
            ii_checkout, ii_config, ii_ext, ii_cpack = (
                i_checkout,
                i_config,
                i_ext,
                i_cpack,
            )
            if str(vext) != "1":
                ii_ext = i_inst
            else:
                if (
                    ii_checkout == const.CHECK_ICON
                    or ii_checkout == const.ERROR_ICON
                    or ii_checkout == const.MAJORWARN_ICON
                    or ii_checkout == const.MINORWARN_ICON
                ):
                    ii_checkout = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_checkout.html">{i_checkout}</a>'
                if (
                    ii_ext == const.CHECK_ICON
                    or ii_ext == const.ERROR_ICON
                    or ii_ext == const.MAJORWARN_ICON
                    or ii_ext == const.MINORWARN_ICON
                ):
                    ii_ext = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_externals_build.html">{i_ext}</a>'
                if (
                    ii_config == const.CHECK_ICON
                    or ii_config == const.ERROR_ICON
                    or ii_config == const.MAJORWARN_ICON
                    or ii_config == const.MINORWARN_ICON
                ):
                    ii_config = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_cmake_config.html">{i_config}</a>'

            if not is_json_output:
                rows_s.append([
                    rname,
                    t_start,
                    ii_checkout if reslt1[0][2].lower() == "abort" else "NO NEW<BR>CODE",
                    ii_ext if reslt1[0][2].lower() == "abort" else "N/A",
                    ii_config if reslt1[0][2].lower() == "abort" else "N/A",
                    "ABORTED" if reslt1[0][2].lower() == "abort" else "CANCELLED",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    hname,
                ])
            else:
                output_dict['data'][rname] = {
                    'release': rname,
                    'job_start': t_start,
                    'git_clone_status': str(s_checkout) if reslt1[0][2].lower() == "abort" else "NO NEW CODE",
                    'externals_build_status': str(s_ext),
                    'cmake_config_status': str(s_config),
                    'build_end': "ABORTED" if reslt1[0][2].lower() == "abort" else "CANCELLED",
                    'compilation': {'errors': "N/A", 'warnings': "N/A"},
                    'cpack_status': "N/A",
                    'test_time': "N/A",
                    'ctest_results': {'errors': "N/A", 'warnings': "N/A", 'timeouts': "N/A"},
                    'art_results_local': "N/A",
                    'art_results_grid': "N/A",
                    'cvmfs_server_publication_time': "N/A",
                    'cvmfs_client_publication_time': "N/A",
                    'host': hname,
                }
        else:
            query1 = f"""
                select to_char(jid),projname,ncompl,pccompl,npb,ner,pcpb,pcer 
                from cstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH where jid = '{str(jid_sel)}' order by projname
            """
            new_cur.execute(query1)
            reslt_addtl = new_cur.fetchall()
            if len(reslt_addtl) != 0:
                dict_jid01 = {}
                for row02 in reslt_addtl:
                    #                  print("------", row02[0],row02[1],row02[2])
                    pj2 = row02[1]
                    if not pj2 in dict_jid01:
                        dict_jid01[pj2] = []
                    dict_jid01[pj2] = [
                        row02[2],
                        row02[3],
                        row02[5],
                        row02[4],
                        row02[7],
                        row02[6],
                    ]

                query2 = (
                    f"""
                    select to_char(jid),projname,ncompl,pccompl,npb,ner,pcpb,pcer,nto,pcto 
                    from tstat@ATLR.CERN.CH natural join projects@ATLR.CERN.CH 
                    where jid = '{str(jid_sel)}' 
                    order by projname
                    """
                )
                new_cur.execute(query2)
                reslt2 = new_cur.fetchall()
                dict_jid02 = {}
                for row02 in reslt2:
                    #                  print("=======", row02[0],row02[1],row02[2])
                    pj2 = row02[1]
                    if not pj2 in dict_jid02:
                        dict_jid02[pj2] = []
                    dict_jid02[pj2] = [
                        row02[2],
                        row02[3],
                        row02[5],
                        row02[4],
                        row02[8],
                        row02[7],
                        row02[6],
                        row02[9],
                    ]
                for row01 in reslt1:
                    #                  print("JID",row01[0])
                    pjname = row01[1]
                    erla = row01[17]
                    sula = row01[18]
                    wala = row01[19]
                    e_cpack = row01[20]
                    s_cpack = row01[21]
                    s_ext = row01[23]
                    if s_ext is None or s_ext == "":
                        s_ext = "N/A"
                    vext = row01[24]
                    if vext is None or vext == "":
                        vext = "0"
                    hname = row01[25]
                    if re.search(r"\.", hname):
                        hname = (re.split(r"\.", hname))[0]
                    area_suffix = reslt1[0][9]
                    if area_suffix is None:
                        area_suffix = ""
                    t_cv_serv = row01[7]
                    t_cv_clie = row01[8]
                    s_cv_serv = row01[10]
                    s_cv_clie = row01[11]
                    nc_er = "0"
                    nc_pb = "0"
                    if pjname in dict_jid01:
                        nccompl = dict_jid01[pjname][0]
                        nc_er = dict_jid01[pjname][2]
                        nc_pb = dict_jid01[pjname][3]
                        if nccompl is None or nccompl == "N/A" or nccompl <= 0:
                            nc_er = "N/A"
                            nc_pb = "N/A"
                    nt_er = "0"
                    nt_pb = "0"
                    nt_to = "0"
                    if pjname in dict_jid02:
                        ntcompl = dict_jid02[pjname][0]
                        nt_er = dict_jid02[pjname][2]
                        nt_pb = dict_jid02[pjname][3]
                        nt_to = dict_jid02[pjname][4]
                        if nt_to is None:
                            nt_to = "0"
                        if ntcompl is None or ntcompl == "N/A" or ntcompl <= 0:
                            nt_er = "N/A"
                            nt_pb = "N/A"
                            nt_to = "N/A"
                    #                  [tpcer_s,tpcpb_s]=map(lambda c: 100 - c, [tpcer,tpcpb])
                    #                  [tpcer_sf,tpcpb_sf]=map(lambda c: format(c,'.1f'), [tpcer_s,tpcpb_s])
                    s_checkout = "N/A"
                    if row01[14] is not None:
                        s_checkout = str(row01[14])
                    s_config = "N/A"
                    s_inst = "N/A"
                    if str(vext) != 1:
                        s_config = "0"
                        s_inst = "0"
                    if row01[12] is not None:
                        s_config = str(row01[12])
                    if row01[13] is not None:
                        s_inst = str(row01[13])
                    e_cpack = row01[20]
                    if e_cpack is None or e_cpack == "":
                        e_cpack = "N/A"
                    s_cpack = row01[21]
                    if s_cpack is None or s_cpack == "":
                        s_cpack = "N/A"
                    t_build = "N/A"
                    if row01[3] is not None:
                        t_build = row01[3].strftime("%Y/%m/%d %H:%M")
                    t_test = "N/A"
                    if row01[5] is not None:
                        t_test = row01[5].strftime("%Y/%m/%d %H:%M")
                    tt_cv_serv = "N/A"
                    if t_cv_serv is not None and t_cv_serv != "":
                        tt_cv_serv = t_cv_serv.strftime("%Y/%m/%d %H:%M")
                    tt_cv_clie = "N/A"
                    if t_cv_clie is not None and t_cv_clie != "":
                        tt_cv_clie = t_cv_clie.strftime("%Y/%m/%d %H:%M")
                    ss_cv_serv = "N/A"
                    if s_cv_serv is not None and s_cv_serv != "":
                        ss_cv_serv = str(s_cv_serv)
                    ss_cv_clie = "N/A"
                    if s_cv_clie is not None and s_cv_clie != "":
                        ss_cv_clie = str(s_cv_clie)
                    #
                    combo_c = str(nc_er) + " (" + str(nc_pb) + ")"
                    combo_t = str(nt_er) + " (" + str(nt_pb) + ") " + str(nt_to)
                    if nt_er == "N/A":
                        combo_t = "N/A(N/A)N/A"
                    #                  mrlink_a="<a href=\""+mrlink+"\">"+gitbr+"</a>"
                    [
                        i_checkout,
                        i_inst,
                        i_config,
                        i_cv_serv,
                        i_cv_clie,
                        i_ext,
                        i_cpack,
                    ] = map(
                        lambda x: di_res.get(str(x), str(x)),
                        [
                            s_checkout,
                            s_inst,
                            s_config,
                            ss_cv_serv,
                            ss_cv_clie,
                            s_ext,
                            s_cpack,
                        ],
                    )
                    if i_checkout is None or i_checkout == "None":
                        i_checkout = const.RADIOOFF_ICON
                    if i_inst is None or i_inst == "None":
                        i_inst = const.RADIOOFF_ICON
                    if i_config is None or i_config == "None":
                        i_config = const.RADIOOFF_ICON
                    if i_ext is None or i_ext == "None":
                        i_ext = const.RADIOOFF_ICON
                    if i_cpack is None or i_cpack == "None":
                        i_cpack = const.RADIOOFF_ICON
                    ii_checkout, ii_config, ii_ext, ii_cpack = (
                        i_checkout,
                        i_config,
                        i_ext,
                        i_cpack,
                    )
                    if str(vext) != "1":
                        ii_ext = i_inst
                        if e_cpack != "N/A":
                            if isinstance(e_cpack, datetime):
                                ii_cpack = ii_cpack + " " + e_cpack.strftime("%d-%b %H:%M").upper()
                    else:
                        if (
                            ii_checkout == const.CHECK_ICON
                            or ii_checkout == const.ERROR_ICON
                            or ii_checkout == const.MAJORWARN_ICON
                            or ii_checkout == const.MINORWARN_ICON
                        ):
                            ii_checkout = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_checkout.html">{i_checkout}</a>'
                        if (
                            ii_ext == const.CHECK_ICON
                            or ii_ext == const.ERROR_ICON
                            or ii_ext == const.MAJORWARN_ICON
                            or ii_ext == const.MINORWARN_ICON
                        ):
                            ii_ext = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_externals_build.html">{i_ext}</a>'

                        if (
                            ii_config == const.CHECK_ICON
                            or ii_config == const.ERROR_ICON
                            or ii_config == const.MAJORWARN_ICON
                            or ii_config == const.MINORWARN_ICON
                        ):
                            ii_config = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_cmake_config.html">{i_config}</a>'

                        if (
                            ii_cpack == const.CHECK_ICON
                            or ii_cpack == const.ERROR_ICON
                            or ii_cpack == const.MAJORWARN_ICON
                            or ii_cpack == const.MINORWARN_ICON
                        ):
                            ii_cpack = f'<a href="{webarea_cur}{os.sep}ardoc_web_area{area_suffix}{os.sep}ARDOC_Log_{rname_trun}{os.sep}ardoc_cpack_combined.html">{i_cpack}</a>'
                            # DO NOT DISPLAY CPack completion time as its accuracy in the db is not guaranteed
                            #  if e_cpack != 'N/A':
                            #      if isinstance(e_cpack, datetime):
                            #          ii_cpack = ii_cpack + " " + e_cpack.strftime('%d-%b %H:%M').upper()
                    link_to_testsRes = reverse("TestsRes")
                    link_to_compsRes = reverse("CompsRes")
                    i_combo_t = f'<a href="{link_to_testsRes}?nightly={nname}&rel={rname}&ar={ar_sel}&proj={pjname}">{combo_t}</a>'
                    if combo_t == "N/A(N/A)N/A":
                        i_combo_t = combo_t
                    i_combo_c = f'<a href="{link_to_compsRes}?nightly={nname}&rel={rname}&ar={ar_sel}&proj={pjname}">{combo_c}</a>'
                    if tt_cv_serv != "N/A":
                        i_combo_cv_serv = tt_cv_serv + i_cv_serv
                    else:
                        i_combo_cv_serv = i_cv_serv

                    nightly_name_art = re.sub("_", "/", nname, 2)
                    art_results = art_results_dict.get(f"{nname}_{rname}", "N/A")
                    art_results_grid_str = "N/A"
                    art_results_local_str = "N/A"
                    if isinstance(art_results, dict) and len(art_results) > 0:
                        art_results_grid_str = (
                            (
                                f'<a href="https://bigpanda.cern.ch/art/overview/?test_type=grid&branch={nightly_name_art}&ntag_full={rname}">'
                                f'<b><span style="color: blue">{art_results["grid"]["active"]}</span></b>,'
                                f'<b><span style="color: green">{art_results["grid"]["succeeded"]}</span></b>,'
                                f'<b><span style="color: brown">{art_results["grid"]["finished"]}</span></b>,'
                                f'<b><span style="color: red">{art_results["grid"]["failed"]}</span></b>'
                                "</a>"
                            )
                            if "grid" in art_results  and sum(art_results["grid"].values()) > 0
                            else "N/A"
                        )
                        art_results_local_str = (
                            (
                                f'<a href="https://bigpanda.cern.ch/art/overview/?test_type=local&branch={nightly_name_art}&ntag_full={rname}">'
                                f'<b><span style="color: blue">{art_results["local"]["active"]}</span></b>,'
                                f'<b><span style="color: green">{art_results["local"]["succeeded"]}</span></b>,'
                                f'<b><span style="color: brown">{art_results["local"]["finished"]}</span></b>,'
                                f'<b><span style="color: red">{art_results["local"]["failed"]}</span></b>'
                                "</a>"
                            )
                            if "local" in art_results  and sum(art_results["local"].values()) > 0
                            else "N/A"
                        )

                    if not is_json_output:
                        rows_s.append([
                            rname,
                            t_start,
                            ii_checkout,
                            ii_ext,
                            ii_config,
                            t_build,
                            i_combo_c,
                            ii_cpack,
                            t_test,
                            i_combo_t,
                            art_results_local_str,
                            art_results_grid_str,
                            i_combo_cv_serv,
                            tt_cv_clie,
                            hname,
                        ])
                    else:
                        output_dict['data'][rname] = {
                            'release': rname,
                            'job_start': t_start,
                            'git_clone_status': str(s_checkout),
                            'externals_build_status': str(s_ext),
                            'cmake_config_status': str(s_config),
                            'build_end': str(t_build),
                            'compilation': {'errors': nc_er, 'warnings': nc_pb},
                            'cpack_status': s_cpack,
                            'test_time': t_test,
                            'ctest_results': {'errors': nt_er, 'warnings': nt_pb, 'timeouts': nt_to},
                            "art_results_local": art_results["local"] if isinstance(art_results, dict) and "local" in art_results else "N/A",
                            "art_results_grid": art_results["grid"] if isinstance(art_results, dict) and "grid" in art_results else "N/A",
                            'cvmfs_server_publication_time': tt_cv_serv,
                            'cvmfs_client_publication_time': tt_cv_clie,
                            'host': hname,
                        }

    if not is_json_output:
        data = {
            "nightly": nname,
            "rel": rname,
            "platform": ar_sel,
            "project": pjname,
            "viewParams": request.session["viewParams"],
            "rows_s": json.dumps(rows_s, cls=DateEncoder),
        }
        return render(request, "nviewDemo.html", data, content_type="text/html")
    else:
        return JsonResponse(output_dict)
