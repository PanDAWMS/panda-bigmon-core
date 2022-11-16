import operator
from core.reports import ReportsDataSource
import time
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from django.http import HttpResponse
from reportlab.pdfgen import canvas
from io import StringIO



class RunningMCProdTasks:
    def __init__(self):
        pass

    def prepareReport(self, campaigname, depthhours, mcordpd = True):

        lenlists = 10

        rp = ReportsDataSource.ReportsDataSource()
        dataSetWholeCompaing = rp.getCompaingTasksJobsStats(campaigname, depthhours, mcordpd)
        eventsSetWholeCompaing = rp.getCompaingTasksEventsStats(campaigname, mcordpd)

        totEvDone = 0
        totEvWereSched = 0 #
        totTasksFinished = 0
        totTasksRegistered = 0
        totTasksRunning = 0
        aveEvThrHours = 0
        estimProcTimeForQEv = 0
        topNSitesWorstPerf = {}
        avWallPerEv = 0
        avWallPerEvH = 0

        totNJobsFailed = 0
        totNJobsSucc = 0
        totWallTimeFailed = 0
        totWallTimeSucc = 0

        topNTasksWorstWPerEvH = {}
        topNTasksFailureRateHours = {}
        topNTasksFailureWallTimeHours = {}
        topNTasksErrorsHours = {}
        topNSitesWithHighestFailureHours = {}
        topNSitesWithHighestActivatedRunningRat = {}
        topNSitesWithHighestAssignedRunningRat = {}


        totEvDoneHours = 0
        totEvRemaining = 0

        taskFinishedSet = set()
        totTasksRegisteredSet = set()
        totTasksRunningSet = set()
        sitesDoneEvents = {}
        sitesSuccWall = {}
        totWallTime = 0
        totWallTimeH = 0


        tasksWallTime = {}
        taskSuccEvH = {}
        tasksSuccJobH = {}
        tasksFailedJobsH = {}
        tasksFailedWallH = {}
        sitesFailedWH = {}
        sitesActivateJobs = {}
        sitesRunningJobs = {}
        sitesAssignedJobs = {}
        taskSuccWallH = {}

        for row in eventsSetWholeCompaing:
            if row[3] > 0:
                totEvWereSched += row[3]
            if row[2] > 0:
                totEvRemaining += row[3]



        for row in dataSetWholeCompaing:

            totNJobsFailed += row[4]
            totNJobsSucc += row[3]
            totWallTimeFailed += row[7] if row[7] is not None else 0
            totWallTimeSucc += row[6] if row[6] is not None else 0

            if row[2] == 'finished':
                taskFinishedSet.add(row[0])
                totWallTime += row[6]
            elif row[2] == 'registered' or row[2] == 'submitting':
                totTasksRegisteredSet.add(row[0])
            elif row[2] == 'running':
                totTasksRunningSet.add(row[0])

            totEvDoneHours += row[8]
            totEvDone += row[9]

            if row[1] not in sitesDoneEvents:
                sitesDoneEvents[row[1]] = row[9]
            else:
                sitesDoneEvents[row[1]] += row[9]

            if row[1] not in sitesSuccWall:
                sitesSuccWall[row[1]] = row[6]
            else:
                sitesSuccWall[row[1]] += row[6]

            if row[0] not in taskSuccWallH:
                taskSuccWallH[row[0]] = row[15]
            else:
                taskSuccWallH[row[0]] += row[15]

            totWallTimeH += row[15]

            totastWalTime =  (row[6] if row[6] is not None else 0)+(row[7] if row[7] is not None else 0)
            if row[0] not in tasksWallTime:
                tasksWallTime[row[0]] = totastWalTime
            else:
                tasksWallTime[row[0]] += totastWalTime

            if row[0] not in taskSuccEvH:
                taskSuccEvH[row[0]] = row[9]
            else:
                taskSuccEvH[row[0]] += row[9]

            if row[0] not in tasksSuccJobH:
                tasksSuccJobH[row[0]] = row[11]
            else:
                tasksSuccJobH[row[0]] += row[11]

            if row[0] not in tasksFailedJobsH:
                tasksFailedJobsH[row[0]] = row[12]
            else:
                tasksFailedJobsH[row[0]] += row[12]

            if row[0] not in tasksFailedWallH:
                tasksFailedWallH[row[0]] = row[10]
            else:
                tasksFailedWallH[row[0]] += row[10]

            if row[1] not in sitesFailedWH:
                if row[10] is not None:
                    sitesFailedWH[row[1]] = row[10]
            else:
                if row[10] is not None:
                    sitesFailedWH[row[1]] += row[10]

            if row[1] not in sitesActivateJobs:
                sitesActivateJobs[row[1]] = row[13]
            else:
                sitesActivateJobs[row[1]] += row[13]

            if row[1] not in sitesAssignedJobs:
                sitesAssignedJobs[row[1]] = row[14]
            else:
                sitesAssignedJobs[row[1]] += row[14]

            if row[1] not in sitesRunningJobs:
                sitesRunningJobs[row[1]] = row[5]
            else:
                sitesRunningJobs[row[1]] += row[5]

        totTasksFinished = len(taskFinishedSet)
        totTasksRegistered = len(totTasksRegisteredSet)
        totTasksRunning = len(totTasksRunningSet)
        aveEvThrHours = totEvDoneHours/depthhours
        estProcTime = 100000000000000000
        if aveEvThrHours > 0:
            estProcTime = totEvRemaining/aveEvThrHours


        for taskid, succEv in sitesDoneEvents.items():
            if sitesSuccWall[taskid] > 0:
                topNSitesWorstPerf[taskid] = sitesDoneEvents[taskid]/sitesSuccWall[taskid]
        topNSitesWorstPerf = sorted(topNSitesWorstPerf.items(), key=operator.itemgetter(1))[0:lenlists]
        avWallPerEv = totEvDone/totWallTime
        avWallPerEvH = totEvDoneHours/totWallTimeH

        for taskid, succEv in taskSuccEvH.items():
            if succEv > 0:
                topNTasksWorstWPerEvH[taskid] = taskSuccWallH[taskid]/succEv
        topNTasksWorstWPerEvH = sorted(topNTasksWorstWPerEvH.items(), key=operator.itemgetter(1), reverse=True)[0:lenlists]


        for taskid, succEv in tasksSuccJobH.items():
            if tasksSuccJobH[taskid] > 0:
                topNTasksFailureRateHours[taskid] = tasksFailedJobsH[taskid]/tasksSuccJobH[taskid]
        sorted_x = sorted(topNTasksFailureRateHours.items(), key=operator.itemgetter(1), reverse=True)
        lenhTasks = lenlists if len(sorted_x) > lenlists else len(sorted_x)
        topNTasksFailureRateHours = {}
        for i in range(lenhTasks):
            topNTasksFailureRateHours[sorted_x[i][0]] = sorted_x[i][1]


        sorted_x = sorted(tasksFailedWallH.items(), key=operator.itemgetter(1), reverse=True)
        lenhTasks = lenlists if len(sorted_x) > lenlists else len(sorted_x)
        topNTasksFailureWallTimeHours = {}
        for i in range(lenhTasks):
            topNTasksFailureWallTimeHours[sorted_x[i][0]] = sorted_x[i][1]

        sitesActivateJobs = {}
        sitesRunningJobs = {}
        sitesAssignedJobs = {}

        sorted_x = sorted(sitesFailedWH.items(), key=operator.itemgetter(1), reverse=True)
        lenhTasks = lenlists if len(sorted_x) > lenlists else len(sorted_x)
        topNSitesWithHighestFailureHours = {}
        for i in range(lenhTasks):
            topNSitesWithHighestFailureHours[sorted_x[i][0]] = sorted_x[i][1]


        for taskid, succEv in sitesActivateJobs.items():
            if sitesRunningJobs[taskid]:
                topNSitesWithHighestActivatedRunningRat[taskid] = sitesActivateJobs[taskid]/sitesRunningJobs[taskid]
        sorted_x = sorted(topNSitesWithHighestActivatedRunningRat.items(), key=operator.itemgetter(1), reverse=True)
        lenhTasks = lenlists if len(sorted_x) > lenlists else len(sorted_x)
        topNSitesWithHighestActivatedRunningRat = {}
        for i in range(lenhTasks):
            topNSitesWithHighestActivatedRunningRat[sorted_x[i][0]] = sorted_x[i][1]


        for taskid, succEv in sitesActivateJobs.items():
            if sitesRunningJobs[taskid] > 0:
                topNSitesWithHighestAssignedRunningRat[taskid] = sitesAssignedJobs[taskid]/sitesRunningJobs[taskid]
        sorted_x = sorted(topNSitesWithHighestAssignedRunningRat.items(), key=operator.itemgetter(1), reverse=True)
        lenhTasks = lenlists if len(sorted_x) > lenlists else len(sorted_x)
        topNSitesWithHighestAssignedRunningRat = {}
        for i in range(lenhTasks):
            topNSitesWithHighestAssignedRunningRat[sorted_x[i][0]] = sorted_x[i][1]


        data = {
            'topNSitesWithHighestAssignedRunningRat':topNSitesWithHighestAssignedRunningRat,
            'topNSitesWithHighestActivatedRunningRat':topNSitesWithHighestActivatedRunningRat,
            'topNSitesWithHighestFailureHours':topNSitesWithHighestFailureHours,
            'topNTasksFailureWallTimeHours':topNTasksFailureWallTimeHours,
            'topNTasksFailureRateHours':topNTasksFailureRateHours,
            'topNTasksWorstWPerEvH':topNTasksWorstWPerEvH,
            'avWallPerEv':avWallPerEv,
            'topNSitesWorstPerf':topNSitesWorstPerf,
            'campaign':campaigname,
            'totEvDone':totEvDone,
            'totEvWereSched':totEvWereSched,
            'totTasksFinishedOrRunnung': totTasksFinished+totTasksRegistered,
            'totTasksRegistered':totTasksRegistered,
            'totTasksFinished':totTasksFinished,
            'totTasksRunning':totTasksRunning,
            'aveEvThrHours':aveEvThrHours,
            'estProcTime':estProcTime,
            'avWallPerEvH':avWallPerEvH,
            'avWallPerEv':avWallPerEv,
            'totJobsFailedSucc': totNJobsFailed/totNJobsSucc,
            'totWallTimeFailedSucc': totWallTimeFailed/totWallTimeSucc,
        }

        return self.renderPDF(data)


    def renderPDF(self, data):

        buff = StringIO.StringIO()
        doc = SimpleDocTemplate(buff, pagesize=letter,
                                rightMargin=72, leftMargin=72,
                                topMargin=72, bottomMargin=18)
        compaign = data['campaign']
        totEvDone = data['totEvDone']
        totEvWasScheduled = data['totEvWereSched']
        totTasksFinishedOrRunnung = data['totTasksFinishedOrRunnung']
        totTasksRegistered = data['totTasksRegistered']
        aveEvThrHours = data['aveEvThrHours']
        estProcTime = data['estProcTime']
        totTasksFinished = data['totTasksFinished']
        totTasksRunning = data['totTasksRunning']
        topNSitesWorstPerf = data['topNSitesWorstPerf']
        avWallPerEvH = data['avWallPerEvH']
        avWallPerEv = data['avWallPerEv']
        totJobsFailedSucc = data['totJobsFailedSucc']
        totWallTimeFailedSucc = data['totWallTimeFailedSucc']
        topNTasksWorstWPerEvH = data['topNTasksWorstWPerEvH']


        Report = []
        styles = getSampleStyleSheet()
        style = getSampleStyleSheet()['Normal']
        style.leading = 24

        Report.append(Paragraph('Report on campaign: ' + compaign, styles["Heading1"]))
        Report.append(Paragraph('Build on ' + time.ctime(), styles["Bullet"]))
        Report.append(Paragraph('Progress and loads', styles["Heading2"]))
        Report.append(Paragraph('Total events done: ' + str(round(totEvDone / 1000000, 2)) + 'M of ' + str(
            round(totEvWasScheduled / 1000000, 2)) + 'M in ' + str(totTasksFinishedOrRunnung) + ' tasks',
                                styles["Normal"]))
        Report.append(Paragraph('Total tasks in the queue: ' + str(totTasksRegistered), styles["Normal"]))
        Report.append(Paragraph('Total tasks running: ' + str(totTasksRunning), styles["Normal"]))
        Report.append(Paragraph('Total tasks done: ' + str(totTasksFinished), styles["Normal"]))

        Report.append(
            Paragraph('Average (of last 12 hours) events throughput: ' + str(round(aveEvThrHours, 2)) + '/h',
                      styles["Normal"]))
        Report.append(Paragraph('Estimated processing time of queued events: ' + str(estProcTime/24) + 'd', styles["Normal"]))
        Report.append(Paragraph('Average (of jobs finished in last 12 hours) events/walltime sec: ' + str(round(avWallPerEvH)), styles["Normal"]))
        Report.append(Paragraph('Average events/walltime sec: ' + str(round(avWallPerEv)), styles["Normal"]))

        Report.append(Paragraph('Issues: ', styles["Heading2"]))

        strTopNSitesWorstPerf = ""
        for (site, perf) in topNSitesWorstPerf:
            strTopNSitesWorstPerf += site + "(" + str(round(perf)) + ") "
        Report.append(Paragraph('Top 3 sites with worst events throughput: ' + strTopNSitesWorstPerf, styles["Normal"]))
        Report.append(Paragraph('Average rates for the whole campaign:', styles["Normal"]))
        Report.append(Paragraph('Number of jobs failed/finished:' + str(round(totJobsFailedSucc, 4)),  styles["Normal"], bulletText='-'))
        Report.append(Paragraph('Number of jobs walltime failed / finished:' + str(round(totWallTimeFailedSucc, 4)),  styles["Normal"]))

        strtopNTasksWorstWPerEvH = ""
        for (task, perf) in topNTasksWorstWPerEvH:
            strtopNTasksWorstWPerEvH += str(task) + "(" + str(round(perf, 4)) + ") "
        Report.append(Paragraph('Top 10 running tasks (of jobs finished in last 12 hours) with worst walltime per events:'+ strtopNTasksWorstWPerEvH,  styles["Normal"], bulletText='-'))

        Report.append(Paragraph('List of top sites with highest failure rate and the type of failures',  styles["Normal"]))



        doc.build(Report)

        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = 'attachment; filename="report.pdf"'
        response.write(buff.getvalue())
        buff.close()

        return response


"""

Top 10 tasks (of jobs finished in last 12 hours) with highest failure rate: taskid(failure rate, site, error, count of error),
Top 10 tasks (of jobs finished in last 12 hours) with highest failured walltime: taskid(failure walltime, site, error, count of error),
Top 10 errors of campaign (of jobs finished in last 12 hours): error, description (affected tasks) corresponding number of errors and the walltime spent for them (to evaluate relative importance of the errors)

List of top (whatever number) sites with highest failure rate and the type of failures
List of the top (whatever number) sites with the highest activated/running ratio (shows if we have jobs that are queued on a site but are not run)
List of the top (whatever number) sites with the highest assigned/running ratio (shows if there are hanging transfers)

"""