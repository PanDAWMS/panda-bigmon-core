from BaseTasksProvider import BaseTasksProvider
import logging
import threading, os, time, shutil
from settingscron import PANDA_LOGGER_PATH, MAX_LOG_AGE_DAYS, PANDA_LOGGER_PATH_ANALYTICS, MAX_LOG_AGE_DAYS_ANALYTICS


class PandaLogsStorageCleanUp(BaseTasksProvider):
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' PandaLogsStorageCleanUp')

    def processPayload(self):
        self.logger.info("PandaLogsStorageCleanUp started")
        for r, d, f in os.walk(PANDA_LOGGER_PATH):
            for dir in d:
                dirpath = os.path.join(r, dir)
                if (time.time() - os.path.getctime(dirpath)) / 60 / 60 / 24 > MAX_LOG_AGE_DAYS:
                    shutil.rmtree(dirpath)
        self.logger.info("PandaLogsStorageCleanUp finished")

        self.logger.info("PandaLogsAnalyticsStorageCleanUp started")
        for r, d, f in os.walk(PANDA_LOGGER_PATH_ANALYTICS):
            for dir in d:
                dirpath = os.path.join(r, dir)
                if (time.time() - os.path.getctime(dirpath)) / 60 / 60 / 24 > MAX_LOG_AGE_DAYS_ANALYTICS:
                    shutil.rmtree(dirpath)
        self.logger.info("PandaLogsAnalyticsStorageCleanUp finished")
