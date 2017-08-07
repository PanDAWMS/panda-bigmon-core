from django.template import RequestContext
from django.shortcuts import render_to_response
from django.db import connection


class TitanProgressReport:
    def __init__(self):
        pass

    def dictfetchall(self, cursor):
        "Returns all rows from a cursor as a dict"
        desc = cursor.description
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]

    def prepareReport(self, request):
        sqlRequest = """SELECT * FROM table(ATLAS_PANDABIGMON.get_partnamearcht())"""
        cur = connection.cursor()
        cur.execute(sqlRequest)
        precs = self.dictfetchall(cur)
        cur.close()

        data = {"countfinishev":0, "countfinishedj":0, "countfailedj":0, "datefrom": None, "dateto": None}

        sqlRequestTemplate = """SELECT SUM(CASE WHEN JOBSTATUS='finished' THEN NEVENTS ELSE 0 END) as countfinishev, 
                           SUM(CASE WHEN JOBSTATUS='finished' THEN 1 ELSE 0 END) as countfinishedj,
                           SUM(CASE WHEN JOBSTATUS='failed' THEN 1 ELSE 0 END) as countfailedj,
                           TRUNC(SYSDATE)-7 datefrom,
                           TRUNC(SYSDATE) dateto
                           FROM %s WHERE computingsite='ORNL_Titan_MCORE' AND 
                      ENDTIME >= TRUNC(SYSDATE)-7 and ENDTIME < TRUNC(SYSDATE)""";


        conditions = ["ATLAS_PANDAARCH.JOBSARCHIVED PARTITION (%s)" % partition['COLUMN_VALUE'] for partition in precs]
        conditions.append("ATLAS_PANDA.JOBSARCHIVED4")

        for condition in conditions:
            sqlRequest = sqlRequestTemplate % condition
            cur = connection.cursor()
            cur.execute(sqlRequest)
            result = self.dictfetchall(cur)
            cur.close()
            if len(result) > 0:
                data["countfinishev"] += 0 if result[0]['COUNTFINISHEV'] is None else result[0]['COUNTFINISHEV']
                data["countfinishedj"] += 0 if result[0]['COUNTFINISHEDJ'] is None else result[0]['COUNTFINISHEDJ']
                data["countfailedj"] += 0 if result[0]['COUNTFAILEDJ'] is None else result[0]['COUNTFAILEDJ']
                data["datefrom"] = result[0]['DATEFROM']
                data["dateto"] = result[0]['DATETO']
        data["failrate"] = '-' if not (data["countfinishedj"]+data["countfailedj"]) > 0 else int((data["countfailedj"]*1.0/(data["countfinishedj"]+data["countfailedj"]))*100)
        return render_to_response('titanreport.html', data, RequestContext(request))



