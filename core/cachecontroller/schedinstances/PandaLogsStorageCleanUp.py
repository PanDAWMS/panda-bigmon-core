from BaseTasksProvider import BaseTasksProvider
import logging
import threading, os, time, shutil, psutil
from settingscron import PANDA_LOGGER_PATH, MAX_LOG_AGE_DAYS, PANDA_LOGGER_PATH_ANALYTICS, MAX_LOG_AGE_DAYS_ANALYTICS


class PandaLogsStorageCleanUp(BaseTasksProvider):
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' PandaLogsStorageCleanUp')

    def processPayload(self):
        self.logger.info("PandaLogsStorageCleanUp started")

        # in case low disk space, remove the top-20 biggest directories
        disk_usage = psutil.disk_usage(PANDA_LOGGER_PATH)
        if disk_usage.percent > 80:
            dirs = []
            for r, d, f in os.walk(PANDA_LOGGER_PATH):
                for dir in d:
                    dirpath = os.path.join(r, dir)
                    try:
                        size = sum(os.path.getsize(os.path.join(dirpath, file)) for file in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, file)))
                    except Exception as e:
                        self.logger.error("Error calculating size of directory %s: %s", dirpath, e)
                        continue
                    dirs.append((dirpath, size))
            dirs.sort(key=lambda x: x[1], reverse=True)
            if len(dirs) > 50:
                n_dirs_to_remove = 50
            else:
                n_dirs_to_remove = len(dirs)
            for dirpath, size in dirs[:n_dirs_to_remove]:
                self.logger.info(f"Removing directory: {dirpath}, size: {size}")
                shutil.rmtree(dirpath)

        # remove old logs
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
