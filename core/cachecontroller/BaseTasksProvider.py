from core import settings
import logging
import oracledb

_logger = logging.getLogger('bigpandamon')

try:
    oracledb.init_oracle_client(config_dir='/etc/tnsnames.ora')
except oracledb.exceptions.DatabaseError as e:
    _logger.error(f"Failed to initialize Oracle Client: {e}")
except Exception as e:
    _logger.error(f"An unexpected error occurred: {e}")

class BaseTasksProvider(object):
    logger = logging.getLogger(__name__)

    # retrieve DB settings
    ORACLE_USERNAME = settings.local.dbaccess['default']['USER']
    ORACLE_PWD = settings.local.dbaccess['default']['PASSWORD']
    ORACLE_NAME = settings.local.dbaccess['default']['NAME']
    ORACLE_CONNECTION_URL = "(DESCRIPTION=(ADDRESS= (PROTOCOL=TCP) (HOST=adcr-s.cern.ch) (PORT=10121) ) (LOAD_BALANCE=on)" \
                            "(ENABLE=BROKEN)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME="+ORACLE_NAME+".cern.ch)))"
    pool = oracledb.create_pool(
        user=ORACLE_USERNAME,
        password=ORACLE_PWD,
        dsn=ORACLE_CONNECTION_URL,
        min=1,
        max=10,
        increment=1,
        events=False
    )

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
        self.logger.debug("Number of DB sessions: {} active and {} in total".format(
            str(totalActiveSessionCount),
            str(totalSessionCount)
        ))
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