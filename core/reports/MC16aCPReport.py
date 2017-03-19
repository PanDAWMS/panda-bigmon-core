from django.db import connection
import time
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from django.http import HttpResponse
from reportlab.pdfgen import canvas
import StringIO
import humanize

class MC16aCPReport:
    def __init__(self):
        pass

    def prepareReport(self):

        requestList = [11034,11048,11049,11050,11051,11052,11198,11197,11222,11359]
        requestList = '(' + ','.join(map(str, requestList)) + ')'

        tasksCondition = "tasktype = 'prod' and WORKINGGROUP NOT IN('AP_REPR', 'AP_VALI', 'GP_PHYS', 'GP_THLT') and " \
                         "processingtype in ('evgen', 'pile', 'simul', 'recon') and REQID in %s" % requestList


        sqlRequest = '''
        select sum(enev), STATUS from (
        select SUM(NEVENTS) as enev, STATUS FROM (
        select NEVENTS, STATUS, t1.JEDITASKID from (
        select sum(decode(c.startevent,NULL,c.nevents,endevent-startevent+1)) nevents,c.status, d.jeditaskid  from atlas_panda.jedi_datasets d,atlas_panda.jedi_dataset_contents c where d.jeditaskid=c.jeditaskid and d.datasetid=c.datasetid and d.type in ('input','pseudo_input') and d.masterid is null group by c.status, d.jeditaskid) t1
        join
        (SELECT JEDITASKID FROM JEDI_TASKS where %s)t2 ON t1.JEDITASKID = t2.JEDITASKID
        ) group by JEDITASKID, STATUS)t3 group by STATUS
        ''' % tasksCondition

        cur = connection.cursor()
        cur.execute(sqlRequest)
        campaignsummary = cur.fetchall()

        finished = 0
        running = 0
        ready = 0

        for row in campaignsummary:
            if row[1] == 'finished':
                finished = row[0]
            if row[1] == 'running':
                running = row[0]
            if row[1] == 'ready':
                ready = row[0]

        data = {
            'finished':finished,
            'running':running,
            'ready':ready,
        }
        return self.renderPDF(data)

    def renderPDF(self, data):
        buff = StringIO.StringIO()
        doc = SimpleDocTemplate(buff, pagesize=letter,
                                rightMargin=72, leftMargin=72,
                                topMargin=72, bottomMargin=18)
        finished = data['finished']
        running = data['running']
        ready = data['ready']

        Report = []
        styles = getSampleStyleSheet()
        style = getSampleStyleSheet()['Normal']
        style.leading = 24

        Report.append(Paragraph('Report on Campaign: ' + "MC16a", styles["Heading1"]))
        Report.append(Paragraph('Build on ' + time.ctime() + " by BigPanDA", styles["Bullet"]))
        Report.append(Paragraph('Progress and loads', styles["Heading2"]))
        Report.append(Paragraph('Done events: ' + humanize.intcomma(int(finished/1000000)) +' M', styles["Normal"]))
        Report.append(Paragraph('Running events: ' + humanize.intcomma(int(running)/1000000) +' M', styles["Normal"]))
        Report.append(Paragraph('Ready for processing events: ' + humanize.intcomma(int(ready)/1000000)  +' M', styles["Normal"]))

        doc.build(Report)
        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = 'attachment; filename="report.pdf"'
        response.write(buff.getvalue())
        buff.close()
        return response

    def getName(self):
        return 'Simple campaign report'

    def getParameters(self):
        return {'Request List': ['11034,11048,11049,11050,11051,11052,11198,11197,11222,11359']}


