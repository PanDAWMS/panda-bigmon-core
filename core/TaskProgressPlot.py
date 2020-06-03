
#import cx_Oracle
import pandas as pd
import matplotlib.dates as md

import os
from django.db import connection
from core.common.models import JediTasks, TasksStatusLog
import io
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import datetime

class TaskProgressPlot:

    def __init__(self):
        pass

    def get_task_start(self,taskid):
        query={}
        query['jeditaskid'] = taskid
        starttime = JediTasks.objects.filter(**query).values('starttime')
        if len(starttime) > 0:
            if starttime[0]['starttime'] is None:
                crtime = JediTasks.objects.filter(**query).values('creationdate')
                starttime[0]['starttime'] = crtime[0]['creationdate'] if len(crtime) > 0 else None
                return starttime[0]
            return starttime[0]
        else:
            return None

    def get_task_retries(self, taskid):
        query = {}
        query['jeditaskid'] = taskid
        mtimeparam = 'modificationtime'
        task_status_log = TasksStatusLog.objects.filter(**query).order_by(mtimeparam).values()
        return task_status_log

    def prepare_task_profile(self,frame):
        if len(frame) > 0:
            firstrow = [(frame.STARTTIME[0], frame.STARTTIME[0], 0), ]
            return pd.DataFrame.from_records(firstrow, columns=frame.columns).append(frame, ignore_index=True)
        else:
            return frame


    def get_raw_task_profile(self,taskid):
        cur = connection.cursor()
        cur.execute("select starttime, endtime, row_number() over (PARTITION BY jeditaskid order by endtime ) as njobs from ATLAS_PANDAARCH.JOBSARCHIVED WHERE JEDITASKID={0} and JOBSTATUS='finished'" .format(taskid))
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=['starttime','endtime','njobs'])


    def get_raw_task_profile_fresh(self,taskid):
        cur = connection.cursor()
        cur.execute("SELECT  * FROM ("
                    "SELECT DISTINCT starttime, creationtime, endtime, pandaid, jeditaskid, EVENTSERVICE FROM JOBSARCHIVED WHERE JEDITASKID={0} and JOBSTATUS='finished' UNION "
                    "SELECT DISTINCT starttime, creationtime, endtime, pandaid, jeditaskid, EVENTSERVICE FROM JOBSARCHIVED4 WHERE JEDITASKID={0} and JOBSTATUS='finished'"
                    ")t ORDER BY pandaid asc".format(taskid))
        rows = cur.fetchall()

        starttime_run = []
        creationtime_run = []
        endtime_run = []
        idxjobs_run = []
        idxjobs_runcount = 0

        starttime_merge = []
        creationtime_merge = []
        endtime_merge = []
        idxjobs_merge = []
        idxjobs_mergecount = 0
        for row in rows:
            if row[5] == 2:
                starttime_merge.append(row[0])
                creationtime_merge.append(row[1])
                endtime_merge.append(row[2])
                idxjobs_merge.append(idxjobs_mergecount)
                idxjobs_mergecount += 1
            else:
                starttime_run.append(row[0])
                creationtime_run.append(row[1])
                endtime_run.append(row[2])
                idxjobs_run.append(idxjobs_runcount)
                idxjobs_runcount += 1

        if len(rows) > 0:

            data_run = {
                'starttime_run': starttime_run,
                'creationtime_run': creationtime_run,
                'endtime_run':endtime_run,
                'idxjobs_run':idxjobs_run
            }

            data_merge = {
                'starttime_merge':starttime_merge,
                'creationtime_merge':creationtime_merge,
                'endtime_merge':endtime_merge,
                'idxjobs_merge':idxjobs_merge
            }

            return {'run':pd.DataFrame(data_run, columns=['starttime_run','creationtime_run',
                                               'endtime_run', 'idxjobs_run']),
                    'merge':pd.DataFrame(data_merge, columns=['starttime_merge', 'creationtime_merge',
                                                'endtime_merge', 'idxjobs_merge'])

                    }
        else:
            None

    def get_raw_task_profile_full(self, taskid):
        """
        A method to form a non ES task profile
        :param taskid:
        :return:
        """
        qstr = """
            SELECT starttime, creationtime, endtime, pandaid, processingtype, transformation, nevents, jobstatus
            FROM (
                SELECT starttime, creationtime, endtime, pandaid, processingtype, transformation, nevents, jobstatus 
                FROM ATLAS_PANDAARCH.JOBSARCHIVED 
                    WHERE JEDITASKID={0} and JOBSTATUS in ('finished', 'failed', 'closed', 'cancelled') 
                UNION
                SELECT starttime, creationtime, endtime, pandaid, processingtype, transformation, nevents, jobstatus  
                FROM ATLAS_PANDA.JOBSARCHIVED4 
                    WHERE JEDITASKID={0} and JOBSTATUS in ('finished', 'failed', 'closed', 'cancelled') 
                ) t 
            ORDER BY endtime asc
        """.format(taskid)
        cur = connection.cursor()
        cur.execute(qstr)
        rows = cur.fetchall()
        tp_names = ['start', 'creation', 'end', 'pandaid', 'processingtype', 'transformation', 'nevents',
                    'jobstatus', 'indx']
        rows = [dict(zip(tp_names, row)) for row in rows]

        # add clear jobtype field and overwrite nevents to 0 for unfinished and build/merge jobs
        job_types = ['build', 'run', 'merge']
        job_timestamps = ['creation', 'start', 'end']
        for r in rows:
            if 'build' in r['transformation']:
                r['jobtype'] = 'build'
            elif r['processingtype'] == 'pmerge':
                r['jobtype'] = 'merge'
            else:
                r['jobtype'] = 'run'

            if r['jobtype'] in ('build', 'merge'):
                r['nevents'] = 0
            if r['jobstatus'] in ('failed', 'closed', 'cancelled'):
                r['nevents'] = 0

            if r['start'] is None:
                r['start'] = r['creation']

        # create task profile dict
        task_profile_dict = {}
        for jt in job_types:
            task_profile_dict[jt] = []
        fin_i = 0
        fin_ev = 0
        for r in rows:
            fin_i += 1 if r['jobstatus'] == 'finished' and r['jobtype'] == 'run' else 0
            fin_ev += r['nevents']
            temp = {}
            for jtm in job_timestamps:
                temp[jtm] = r[jtm]
            temp['nevents'] = fin_ev
            temp['indx'] = fin_i
            temp['jobstatus'] = r['jobstatus']
            task_profile_dict[r['jobtype']].append(temp)

        return task_profile_dict


    def get_es_raw_task_profile_fresh(self,taskid):
        cur = connection.cursor()

        cur.execute("""select MODIFICATIONTIME, SUM(ESEVENTS_MERGED) over (PARTITION BY jeditaskid order by MODIFICATIONTIME ) as NEVENTS from
                    (SELECT t1.ESEVENTS_MERGED, MODIFICATIONTIME, JEDITASKID FROM (SELECT SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS ESEVENTS_MERGED,PANDAID FROM ATLAS_PANDA.JEDI_EVENTS WHERE JEDITASKID={0} AND STATUS=9 GROUP BY PANDAID) t1
                    JOIN JOBSARCHIVED4 ON t1.PANDAID = JOBSARCHIVED4.PANDAID AND JEDITASKID={0} AND JOBSARCHIVED4.JOBSTATUS='finished'
                    UNION All
                    SELECT t2.ESEVENTS_MERGED, MODIFICATIONTIME, JEDITASKID FROM (SELECT SUM(DEF_MAX_EVENTID-DEF_MIN_EVENTID+1) AS ESEVENTS_MERGED,PANDAID FROM ATLAS_PANDA.JEDI_EVENTS WHERE JEDITASKID={0} AND STATUS=9 GROUP BY PANDAID) t2
                    JOIN JOBSARCHIVED ON t2.PANDAID = JOBSARCHIVED.PANDAID AND JEDITASKID={0} AND JOBSARCHIVED.JOBSTATUS='finished') t3""".format(taskid))
        rows = cur.fetchall()

        modificationtimeList = []
        neventsList = []
        if len(rows) > 0:
            for row in rows:
                if isinstance(row[0], datetime.datetime) and row[0] > (datetime.datetime.now() + datetime.timedelta(-10000)) and (row[0] not in modificationtimeList):
                    modificationtimeList.append(row[0])
                    neventsList.append(row[1])
            data = {'modificationtime': modificationtimeList,
                    'nevents': neventsList}
            if len(modificationtimeList) > 10000:
                return pd.DataFrame(data, columns=['modificationtime','nevents']).sample(n=10000)
            else:
                return pd.DataFrame(data, columns=['modificationtime', 'nevents'])
        else:
            None



    def make_profile_graph(self,frame, taskid):
        fig = plt.figure(figsize=(20, 15))
        plt.title('Execution profile for task {0}, NJOBS={1}'.format(taskid, len(frame) - 1), fontsize=24)
        plt.xlabel("Job completion time", fontsize=18)
        plt.ylabel("Number of completed jobs", fontsize=18)
        plt.plot(frame.ENDTIME, frame.NJOBS, '.r')
        return fig


    def make_verbose_profile_graph(self,frame, taskid, status=None, daterange=None):
        plt.style.use('fivethirtyeight')
        fig = plt.figure(figsize=(20, 18))
        plt.locator_params(axis='x', nbins=30)
        plt.locator_params(axis='y', nbins=30)
        if status is not None:
            plt.title('Execution profile for task {0}, NJOBS={1}, STATUS={2}'.format(taskid, len(frame['run'])+len(frame['merge']), status['status']), fontsize=24)
        else:
            plt.title('Execution profile for task {0}, NJOBS={1}'.format(taskid, len(frame['run'])+len(frame['merge'])), fontsize=24)
        plt.xlabel("Job completion time", fontsize=18)
        plt.ylabel("Number of completed jobs", fontsize=18)
        taskstart = self.get_task_start(taskid)['starttime']
        plt.axvline(x=taskstart, color='b', linewidth=4, label="Task start time")

        if len(frame['merge'].values[:,0:2])>0:
            mint = min(frame['run'].values[:,0:2].min(), frame['merge'].values[:,0:2].min(), taskstart)
            maxt = max(frame['run'].values[:, 0:3].max(), frame['merge'].values[:, 0:3].max())
        else:
            mint = min(frame['run'].values[:,0:3].min(), taskstart)
            maxt = frame['run'].values[:, 0:3].max()

        plt.xlim(xmax=maxt)
        plt.xlim(xmin=mint)
        plt.xticks(rotation=25)

        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        #plt.xlim(daterange)

        if len(frame['merge'].values[:,0:2])>0:
            plt.plot(frame['merge'].endtime_merge, frame['merge'].idxjobs_merge, '.r', label='Merge job ENDTIME', marker='+', markersize=8)
            plt.plot(frame['merge'].starttime_merge, frame['merge'].idxjobs_merge, '.g', label='Merge job STARTTIME', marker='+', markersize=8)
            plt.plot(frame['merge'].creationtime_merge, frame['merge'].idxjobs_merge, '.b', label='Merge job CREATIONTIME', marker='+', markersize=8)
        plt.plot(frame['run'].endtime_run, frame['run'].idxjobs_run, '.r', label='Job ENDTIME', markersize=8)
        plt.plot(frame['run'].starttime_run, frame['run'].idxjobs_run, '.g', label='Job STARTTIME', markersize=8)
        plt.plot(frame['run'].creationtime_run, frame['run'].idxjobs_run, '.b', label='Job CREATIONTIME', markersize=8)

        plt.legend(loc='lower right')
        return fig

    def make_es_verbose_profile_graph(self,frame, taskid, status=None, daterange=None):
        #plt.switch_backend('Cairo')
        plt.style.use('fivethirtyeight')
        fig = plt.figure(figsize=(20, 15))
        plt.locator_params(axis='x', nbins=30)
        plt.locator_params(axis='y', nbins=30)
        plt.title('Execution profile for task {0}'.format(taskid), fontsize=24)
        plt.xlabel("Event Merge time", fontsize=18)
        plt.ylabel("Number of merged events", fontsize=18)
        starttime = self.get_task_start(taskid)['starttime']
        plt.axvline(x=starttime, color='b', linewidth=4, label="Task start time")

        if not starttime is None:
            min = starttime
        else:
            min = frame.values[:,0].min()

        max = frame.values[:,0].max()
        plt.xlim(xmax=max)
        plt.xlim(xmin=min)
        plt.xticks(rotation=25)

        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)

        plt.plot(frame.modificationtime, frame.nevents, '.r', label='# merged events')
        plt.legend(loc='lower right')
        return fig


    def save_task_profile(self,taskid):
        frame = self.get_raw_task_profile(taskid)
        fig = self.make_profile_graph(frame, taskid)
        plt.savefig(os.path.join("task_profiles", str(taskid) + ".png"))
        plt.close(fig)


    def show_task_profile(self,taskid):
        frame = self.get_raw_task_profile(taskid)
        fig = self.make_profile_graph(frame, taskid)
        plt.show()
        plt.close(fig)


    def get_task_status(self,taskid):
        query={}
        query['jeditaskid'] = taskid
        status = JediTasks.objects.filter(**query).values('status')
        if len(status) > 0:
            return status[0]
        else:
            return None


    def show_verbose_task_profile(self,taskid, daterange=None):
        frame = self.get_raw_task_profile_fresh(taskid)
        fig = self.make_verbose_profile_graph(frame, taskid, self.get_task_status(taskid), daterange)
        plt.show()
        plt.close(fig)


    def get_task_profile(self,taskid):
        frame = self.get_raw_task_profile_fresh(taskid)
        if frame is not None:
            fig = self.make_verbose_profile_graph(frame, taskid, self.get_task_status(taskid))
            imgdata = io.BytesIO()
            fig.savefig(imgdata, format='png')
            imgdata.seek(0)
            return imgdata
        return None

    def get_es_task_profile(self,taskid):
        frame = self.get_es_raw_task_profile_fresh(taskid)
        if frame is not None:
            fig = self.make_es_verbose_profile_graph(frame, taskid, self.get_task_status(taskid))
            imgdata = io.BytesIO()
            fig.savefig(imgdata, format='png')
            imgdata.seek(0)
            return imgdata
        return None