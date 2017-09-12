from django.template import RequestContext
from django.shortcuts import render_to_response
from django.db import connection
from collections import OrderedDict
from datetime import datetime
import time
import scipy.cluster.hierarchy as hcluster
import numpy as np



class ObsoletedTasksReport:
    def __init__(self):
        pass

    def prepareReportTasksV1(self, request, type):

        uniqueTasksCond = ""
        if type == "tasksview":
            uniqueTasksCond ="PART=1 and"


        sqlRequest = '''
                    SELECT * FROM (
            WITH RECONSTRUCTEDTASKCHAIN AS (
            SELECT TASKID, PR_ID, TASKNAME, CHAIN_TID, PARENT_TID, STATUS as TASKSTATUS, LEVEL as LEV, PPFLAG, CASE WHEN PPGRACEPERIOD = -1 THEN 48 ELSE PPGRACEPERIOD END as PPGRACEPERIOD FROm ATLAS_DEFT.T_PRODUCTION_TASK 
            START WITH PPFLAG > 0
            CONNECT BY NOCYCLE PRIOR TASKID=PARENT_TID ORDER SIBLINGS BY TASKID
            ) SELECT RECONSTRUCTEDTASKCHAIN.*, STATUS as DSSTATUS, TIMESTAMP, row_number() OVER(PARTITION BY RECONSTRUCTEDTASKCHAIN.TASKID order by t_production_dataset.TIMESTAMP) AS PART, t_production_dataset.NAME as dsname FROM ATLAS_DEFT.RECONSTRUCTEDTASKCHAIN, ATLAS_DEFT.t_production_dataset  WHERE t_production_dataset.TASKID=RECONSTRUCTEDTASKCHAIN.TASKID
            and instr(t_production_dataset.NAME,'.log.') = 0 
            ) WHERE '''+uniqueTasksCond+''' PPFLAG>=0 ORDER BY LEV DESC
        '''

        cur = connection.cursor()
        cur.execute(sqlRequest)
        stats = cur.fetchall()
        tasksInfoList = []

        timesecs = []

        i = 0
        for taskEntry in stats:
            timesecs.append(time.mktime(stats[i][10].timetuple()))
            i += 1

        minT = min(timesecs)
        timesecs[:] = [x - minT for x in timesecs]

        thresh = 21600

        data_run = [
            timesecs,
        ]
        np.asarray(data_run)

        clusters = hcluster.fclusterdata(np.transpose(np.asarray(data_run)), thresh, criterion="distance")


        cluserssummary = {}
        i = 0
        for taskEntry in stats:
            clusterID = clusters[i]
            tmpDict = {"reqid": taskEntry[1], "taskid": taskEntry[0], "taskname": taskEntry[2], "dsname": taskEntry[12], "clusterid": clusterID}
            tasksInfoList.append(tmpDict)

            if clusterID not in cluserssummary:
                cluserssummary[clusterID] = {"obsoleteStart":taskEntry[10], "obsoleteFinish":taskEntry[10], "requests":[taskEntry[1]], "tasks":[taskEntry[0]], "datasets":[taskEntry[12]]}
            else:
                if cluserssummary[clusterID]["obsoleteStart"] > taskEntry[10]:
                    cluserssummary[clusterID]["obsoleteStart"] = taskEntry[10]

                if cluserssummary[clusterID]["obsoleteFinish"] < taskEntry[10]:
                    cluserssummary[clusterID]["obsoleteFinish"] = taskEntry[10]

                if taskEntry[0] not in cluserssummary[clusterID]["tasks"]:
                    cluserssummary[clusterID]["tasks"].append(taskEntry[0])

                if taskEntry[12] not in cluserssummary[clusterID]["datasets"]:
                    cluserssummary[clusterID]["datasets"].append(taskEntry[12])

                if taskEntry[1] not in cluserssummary[clusterID]["requests"]:
                    cluserssummary[clusterID]["requests"].append(taskEntry[1])
            i += 1

        cluserssummaryList = []
        for id, cluster in cluserssummary.items():
            cluserssummaryList.append(cluster)

        cluserssummaryList = sorted(cluserssummaryList, key=lambda k: k['obsoleteStart'], reverse=True)

        data = {}
        data['tasksInfo'] = tasksInfoList
        data['built'] = datetime.now().strftime("%d %b %Y %H:%M:%S")
        data['type'] = type
        data['clusters'] = cluserssummaryList

        return render_to_response('reportObsoletedTasksv3.html', data, RequestContext(request))




    def prepareReportTasksV0(self, request):

        sqlRequest = '''
            SELECT * FROM (
            WITH RECONSTRUCTEDTASKCHAIN AS (
            SELECT TASKID, CHAIN_TID, PARENT_TID, STATUS as TASKSTATUS, LEVEL as LEV, PPFLAG, CASE WHEN PPGRACEPERIOD = -1 THEN 48 ELSE PPGRACEPERIOD END as PPGRACEPERIOD FROm ATLAS_DEFT.T_PRODUCTION_TASK 
            START WITH PPFLAG > 0
            CONNECT BY NOCYCLE PRIOR TASKID=PARENT_TID ORDER SIBLINGS BY TASKID
            ) SELECT RECONSTRUCTEDTASKCHAIN.*, STATUS as DSSTATUS, TIMESTAMP, row_number() OVER(PARTITION BY RECONSTRUCTEDTASKCHAIN.TASKID order by t_production_dataset.TIMESTAMP) AS PART, t_production_dataset.NAME as dsname FROM ATLAS_DEFT.RECONSTRUCTEDTASKCHAIN, ATLAS_DEFT.t_production_dataset  WHERE t_production_dataset.TASKID=RECONSTRUCTEDTASKCHAIN.TASKID
            and instr(t_production_dataset.NAME,'.log.') = 0 
            ) WHERE PART=1 and PPFLAG>=0 ORDER BY LEV ASC
        '''
        cur = connection.cursor()
        cur.execute(sqlRequest)
        stats = cur.fetchall()
        tasksInfo = OrderedDict()
        inversedMap = {}
        for taskEntry in stats:
            if taskEntry[4] == 1: #This is entry level of tasks chain
                if taskEntry[5] == 1:
                    tmpDict = {"tofdel":"task force obsoleting"}
                if taskEntry[5] == 2:
                    tmpDict = {"tofdel":"task chain obsoleting"}
                tmpDict["date"] = taskEntry[8]
                tmpDict["graceperiod"] = taskEntry[6]
                tmpDict["dsname"] = taskEntry[10]
                tmpDict["dsstatus"] = taskEntry[3]
                tasksInfo[taskEntry[0]] = tmpDict
            else:
                if taskEntry[2] in inversedMap: #here we check if parent task already assigned
                    inversedMap[taskEntry[0]] = inversedMap[taskEntry[2]]
                else:
                    inversedMap[taskEntry[0]] = taskEntry[2]
                tempDic = tasksInfo[inversedMap[taskEntry[0]]]
                if "childtasks" not in tempDic:
                    tempDic["childtasks"] = []
                tempDic["childtasks"].append(taskEntry[0])
                tempDic["date"] = taskEntry[8]
                ### If not deleted we should add graceperiod to date
                tasksInfo[inversedMap[taskEntry[0]]] = tempDic
        tasksInfo = sorted(tasksInfo.iteritems(), key=lambda x: x[1]['date'], reverse=True)
        tasksInfoList = []
        for (key, value) in tasksInfo:
            value['date'] = value['date'].strftime("%d %b %Y %H:%M:%S")
            value['rootTask'] = key
            tasksInfoList.append(value)
        data = {}
        data['tasksInfo'] = tasksInfoList
        data['built'] = datetime.now().strftime("%d %b %Y %H:%M:%S")
        return render_to_response('reportObsoletedTasks.html', data, RequestContext(request))


    def prepareReport(self, request):
        # if 'obstasks' in request.session['requestParams'] and request.session['requestParams']['obstasks'] == 'tasksview':
        #     return self.prepareReportTasksV1(request, "tasksview")
        # elif 'obstasks' in request.session['requestParams'] and request.session['requestParams']['obstasks'] == 'dsview':
        #     return self.prepareReportTasksV1(request, "dsview")
        # else:
        return self.prepareReportTasksV1(request, "tasksview")
