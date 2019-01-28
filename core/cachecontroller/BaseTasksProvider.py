from core import settings
import cx_Oracle
import threading


class BaseTasksProvider(object):
    # Retreive DB settings
    ORACLE_USERNAME = settings.local.dbaccess['default']['USER']
    ORACLE_PWD = settings.local.dbaccess['default']['PASSWORD']
    ORACLE_SNAME = settings.local.dbaccess['default']['NAME']
    ORACLE_CONNECTION_URL = "(DESCRIPTION=(ADDRESS= (PROTOCOL=TCP) (HOST=adcr-s.cern.ch) (PORT=10121) ) (LOAD_BALANCE=on)" \
                            "(ENABLE=BROKEN)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME="+ORACLE_SNAME+".cern.ch)))"
    pool = cx_Oracle.SessionPool(ORACLE_USERNAME, ORACLE_PWD, ORACLE_CONNECTION_URL, 2, 3, 1, threaded=True)
    lock = threading.Semaphore(1) # We allow to pileup not more than 2 consecutive calls

    def getNumberOfActiveDBSessions(self):
        totalSessionCount = -1
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
        return  totalSessionCount

    def logActivity(self):
        pass

    def processPayload(self):
        pass

    def execute(self):
        if self.lock.acquire(blocking=False):
            self.processPayload()
            self.lock.release()




