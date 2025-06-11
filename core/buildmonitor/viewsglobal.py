import json
import re
from collections import defaultdict

from core.buildmonitor.utils import get_art_test_results
from core.libs.DateEncoder import DateEncoder
from core.oauth.utils import login_customrequired
from core.utils import is_json_request
from core.views import initRequest
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse


@login_customrequired
def globalviewDemo(request):

    valid, response = initRequest(request)
    if not valid:
        return response

    art_results_dict = get_art_test_results()

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
    ts.nt, ts.nt_er, ts.nt_pb, ts.nt_to,
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
    (select ncompl as nt, ner as nt_er, npb as nt_pb, nto as nt_to, jid, projid from tstat@ATLR.CERN.CH ) ts
     WHERE
    j.jid BETWEEN to_number(to_char(SYSDATE-10, 'YYYYMMDD'))*10000000
     AND to_number(to_char(SYSDATE, 'YYYYMMDD')+1)*10000000
     AND j.jid = platf.jid
     AND j.jid = cs.jid and j.projid = cs.projid
     AND j.jid = ts.jid and j.projid = ts.projid
     AND j.begdate between sysdate-10 and sysdate
     AND j.eb is not  NULL order by j.eb desc
    """
    new_cur = connection.cursor()
    new_cur.execute(query)
    result = new_cur.fetchall()
    new_cur.close()
    dd = defaultdict(list)
    for row1 in result:
        # row1[22] is the number of timed-out test - row added in Jan 2025 - needed redefinition from None for old entries
        row122 = row1[22]
        if row122 is None or row122 is None:
            row122 = 0
        if row1[0] not in dd:
            if row1[24] == 1:  # LAST PROJECT
                dd[row1[0]] += row1[1:]
                dd[row1[0]][21] = row122
        else:
            if row1[25] == dd[row1[0]][24]:
                dd[row1[0]][15] += row1[16]
                dd[row1[0]][16] += row1[17]
                dd[row1[0]][17] += row1[18]
                dd[row1[0]][18] += row1[19]
                dd[row1[0]][19] += row1[20]
                dd[row1[0]][20] += row1[21]
                dd[row1[0]][21] += row122

    # add str for custom sorting
    reslt2 = []
    dict_g = {
        "CI": "AA0",
        r"^[\d_\-]*PRODUCTION.*$": "AA01",
        r"^[\d_\-]*DEVELOP.*$": "AA02",
        r"^[\d_\-]*MAIN.*$": "AA03",
        r"^[\d_\-]*ARM.*$": "AA05",
        "LANG": "AA06",
        "CENTOS": "AA07",
        "NEXT": "AA08",
        "LCG": "AA1",
        "BRAN": "AA2",
        "CONSTR": "Z1",
        "LEGACY": "B1",
        "ANALYSIS": "B2",
        "UPGRADE": "B3",
        "BUG": "B4",
        "GAUDI": "C0",
        "EXP": "C2",
        "OTHER": "C9",
        "TEST": "Z7",
        "TRAIN": "Z8",
        "DOXYGEN": "Z9",
    }
    for k, v in dd.items():
        ar1 = [
            k,
        ]
        row10u = v[0].upper()
        keyList = [k for k in dict_g.keys() if re.search(k, row10u)]
        key_name_code = next(iter(keyList), "Y")
        name_code = dict_g.get(key_name_code, "Y" + row10u)
        v[0] = row10u
        ar1.extend(v)
        ar1.append(name_code)
        reslt2.append(ar1)

    # prepare data for data table
    res_dict = {}
    is_json_output = is_json_request(request)
    reslt3 = []
    for row in reslt2:
        m_ncompl = row[16]
        if m_ncompl is None or m_ncompl == "N/A" or m_ncompl <= 0:
            row[17] = "N/A"
            row[18] = "N/A"
        a0001 = f"{str(row[17])} ({str(row[18])})"
        m_tcompl = row[19]
        if m_tcompl is None or m_tcompl == "N/A" or m_tcompl <= 0:
            row[20] = "N/A"
            row[21] = "N/A"
            row[22] = "N/A"
        a0002 = f"{str(row[20])} ({str(row[21])}) {str(row[22])}"

        t_cv_clie = row[28] if row[28] is not None and row[28] != "" else "N/A"

        brname = row[0]
        link_to_ciInfo = reverse("BuildCI")
        link_to_nInfo = reverse("BuildN")
        if re.match("^.*CI.*$", brname):
            link_brname = '<a href="' + link_to_ciInfo + '">' + brname + "</a>"
        else:
            link_brname = f'<a href="{link_to_nInfo}?nightly={brname}">{brname}</a>'

        rname = row[14]
        nightly_name_art = re.sub("_", "/", brname, 2)
        art_results = art_results_dict.get(f"{brname}_{rname}", "N/A")
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
                if "grid" in art_results
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
                if "local" in art_results
                else "N/A"
            )

        if is_json_output:
            res_dict[brname] = {
                "nightly_group": row[1],
                "branch_name": brname,
                "release": rname,
                "build_date": row[9],
                "compilation_results": {
                    "errors": row[17] if row[17] is not None else "N/A",
                    "warnings": row[18] if row[18] is not None else "N/A",
                },
                "ctest_results": {
                    "errors": row[20] if row[20] is not None else "N/A",
                    "warnings": row[21] if row[21] is not None else "N/A",
                    "timeouts": row[22] if row[22] is not None else "N/A",
                },
                "art_results_local": art_results["local"] if isinstance(art_results, dict) and "local" in art_results else "N/A",
                "art_results_grid": art_results["grid"] if isinstance(art_results, dict) and "grid" in art_results else "N/A",
                "cvmfs_publication_time": t_cv_clie,
            }
        else:
            reslt3.append(
                [
                    row[1],
                    link_brname,
                    rname,
                    row[9],
                    a0001,
                    a0002,
                    art_results_local_str,
                    art_results_grid_str,
                    t_cv_clie,
                    row[40],
                ]
            )

    if is_json_output:
        return JsonResponse(res_dict, safe=False)
    else:
        data = {
            "request": request,
            "viewParams": request.session["viewParams"],
            "reslt3": json.dumps(reslt3, cls=DateEncoder),
        }
        return render(request, "globalviewDemo.html", data, content_type="text/html")
