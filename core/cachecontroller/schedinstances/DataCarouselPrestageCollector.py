from BaseTasksProvider import BaseTasksProvider
import threading
import logging
import pandas as pd
import urllib.request as urllibr
from urllib.error import HTTPError
import json
from settingscron import TIME_OUT_FOR_QUERY
import oracledb

class DataCarouselPrestageCollector(BaseTasksProvider):
    BASE_STAGE_INFO_URL = 'https://bigpanda.cern.ch/staginprogress/?jeditaskid='
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' DataCarouselPrestageCollector')

    def insertNewStagingEntries(self):
        connection = self.pool.acquire()
        cursor = connection.cursor()
        query = """
                insert into ATLAS_PANDABIGMON.DATACAR_ST_PROGRESS_ARCH 
                select tbig.DATASET, tbig.STATUS, tbig.STAGED_FILES, tbig.START_TIME, tbig.END_TIME, tbig.RRULE, tbig.TOTAL_FILES, tbig.SOURCE_RSE, tbig.TASKID, NULL as PROGRESS_RETRIEVED, NULL as PROGRESS_DATA from (
                SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE as RRULE, t1.TOTAL_FILES,
                 t1.SOURCE_RSE, t2.TASKID, ROW_NUMBER() OVER(PARTITION BY t1.DATASET_STAGING_ID ORDER BY t1.start_time DESC) AS occurence FROM ATLAS_DEFT.T_DATASET_STAGING t1
                INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
                INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID
                order by t1.START_TIME desc
                )tbig 
                LEFT OUTER JOIN ATLAS_PANDABIGMON.DATACAR_ST_PROGRESS_ARCH arch on arch.RRULE=tbig.RRULE
                where occurence=1 and tbig.status='done' and arch.RRULE is NULL and tbig.RRULE is not NULL
                """
        cursor.execute(query)
        connection.commit()
        cursor.close()
        self.pool.release(connection)

    def downloadProgressData(self, taskid):
        response = None
        try:
            req = urllibr.Request(self.BASE_STAGE_INFO_URL + taskid)
            response = urllibr.urlopen(req, timeout=TIME_OUT_FOR_QUERY).read()
            response = json.loads(response)
        except Exception or HTTPError as e:
            self.logger.error(e)
        return response

    def getStagingToRetrieve(self):
        query = """
        SELECT TASKID FROM atlas_pandabigmon.DATACAR_ST_PROGRESS_ARCH where PROGRESS_RETRIEVED is NULL order by START_TIME desc
        """
        connection = self.pool.acquire()
        cursor = connection.cursor()
        rows = cursor.execute(query)
        frame = pd.DataFrame(rows)
        self.pool.release(connection)
        return frame

    def retrieveStagingProfile(self, items):
        for index, row in items.iterrows():
            taskid = row['TASKID']
            staginginfo = self.downloadProgressData(str(row['TASKID']))
            staginginfo = json.dumps(staginginfo)
            connection = self.pool.acquire()
            cursor = connection.cursor()
            if len(staginginfo) > 60:
                cursor.setinputsizes(staginginfo=oracledb.LONG_STRING)
                cursor.execute(
                    """UPDATE atlas_pandabigmon.DATACAR_ST_PROGRESS_ARCH SET PROGRESS_DATA = :staginginfo, PROGRESS_RETRIEVED = :res where TASKID = :taskid""",
                    {'staginginfo': staginginfo, 'res': 1, 'taskid': taskid})
            else:
                cursor.execute(
                    """UPDATE atlas_pandabigmon.DATACAR_ST_PROGRESS_ARCH SET PROGRESS_RETRIEVED = :res where TASKID = :taskid""",
                    {'res': -1, 'taskid': int(taskid)})
            connection.commit()


    def processPayload(self):
        self.logger.info("DataCarouselPrestageCollector started")
        self.insertNewStagingEntries()
        staging_requests = self.getStagingToRetrieve()
        self.retrieveStagingProfile(staging_requests)
        self.logger.debug("DataCarouselPrestageCollector finished")
