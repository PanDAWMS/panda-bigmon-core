from core import settings
import cx_Oracle
import threading
import logging


class BaseTasksProvider(object):
    logger = logging.getLogger(__name__)

    # Retreive DB settings
    ORACLE_USERNAME = settings.local.dbaccess['default']['USER']
    ORACLE_PWD = settings.local.dbaccess['default']['PASSWORD']
    ORACLE_SNAME = settings.local.dbaccess['default']['NAME']
    ORACLE_CONNECTION_URL = "(DESCRIPTION=(ADDRESS= (PROTOCOL=TCP) (HOST=adcr-s.cern.ch) (PORT=10121) ) (LOAD_BALANCE=on)" \
                            "(ENABLE=BROKEN)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME="+ORACLE_SNAME+".cern.ch)))"
    pool = cx_Oracle.SessionPool(
        ORACLE_USERNAME, ORACLE_PWD, ORACLE_CONNECTION_URL, min=1, max=10, increment=1, threaded=True, events=False)
    #lock = threading.RLock() # should be instantiated in a nested class.
    # If instantiated here become the same over all child classes

    def getNumberOfActiveDBSessions(self):
        totalSessionCount, totalActiveSessionCount = 0, 0
        try:
            db = self.pool.acquire()
            cursor = db.cursor()
            cursor.execute("SELECT SUM(NUM_ACTIVE_SESS), SUM(NUM_SESS) FROM ATLAS_DBA.COUNT_PANDAMON_SESSIONS")
            for row in cursor:
                totalActiveSessionCount = row[0]
                totalSessionCount = row[1]
                break
            cursor.close()
        except:
            pass
        self.logger.debug("Number of DB sessions:" + str(totalSessionCount))
        if totalActiveSessionCount is None:
            totalActiveSessionCount = 0
            self.logger.debug("Reset number of active sessions to: " + str(totalActiveSessionCount))
        return totalActiveSessionCount

    def logActivity(self):
        raise NotImplementedError("Must override logActivity")

    def processPayload(self):
        raise NotImplementedError("Must override processPayload")

    def execute(self):
        if self.lock.acquire(blocking=False):
            try:
                self.processPayload()
            finally:
                self.lock.release()