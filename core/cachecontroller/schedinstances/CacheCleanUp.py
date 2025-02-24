import threading
import logging

from BaseTasksProvider import BaseTasksProvider
from settingscron import MAX_NUMBER_OF_ACTIVE_DB_SESSIONS, TIMEOUT_WHEN_DB_LOADED
from datetime import datetime, timedelta, timezone

class CacheCleanUp(BaseTasksProvider):
    """
    Aggressive Django cache clean up task
    """
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' SQLAggregator')

    def processPayload(self):

        self.logger.info("CacheCleanUp started")
        while self.getNumberOfActiveDBSessions() > MAX_NUMBER_OF_ACTIVE_DB_SESSIONS:
            threading.sleep(TIMEOUT_WHEN_DB_LOADED)

        datetime_threshold_str = (datetime.now(tz=timezone.utc) - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"CacheCleanUp, DB sessions are low enough, deleting all cache entries expired before {datetime_threshold_str}")
        db = None
        try:
            query = f"delete from atlas_pandabigmon.djangocache where expires < to_date(:datetime_threshold_str, 'YYYY-MM-DD HH24:MI:SS')"
            # get DB connection from pool
            db = self.pool.acquire()
            # use context manager for cursor
            with db.cursor() as cursor:
                cursor.execute(query, {'datetime_threshold_str': datetime_threshold_str})
                n_deleted_rows = cursor.rowcount
                db.commit()
                self.logger.info(f"CacheCleanUp finished successfully, deleted {n_deleted_rows} rows")
        except Exception as e:
            self.logger.error(f"CacheCleanUp failed: {e}")
            return -1
        finally:
            # ensure to release the connection back to the pool
            if db:
                db.close()

        return 0
