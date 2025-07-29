from BaseTasksProvider import BaseTasksProvider
import logging
import threading, os, time, shutil, psutil
import settingscron as settings





class PandaLogsStorageCleanUp(BaseTasksProvider):
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' PandaLogsStorageCleanUp')

    def get_dir_size(self, path_to_analyze):
        """Recursively calculate total size of all files in the given directory."""
        total_size = 0
        for dirpath, _, filenames in os.walk(path_to_analyze):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.isfile(fp):
                    try:
                        total_size += os.path.getsize(fp)
                    except Exception as e:
                        self.logger.debug(f"Warning: Could not access file {fp}: {e}")
        return total_size


    def processPayload(self):
        self.logger.info("PandaLogsStorageCleanUp started")

        # in case low disk space, remove the top-20 biggest directories
        disk_usage = psutil.disk_usage(settings.PANDA_LOGGER_PATH)
        if disk_usage.percent > settings.DISK_USAGE_THRESHOLD_PERCENT:
            self.logger.info(
                f"Disk usage is above threshold ({disk_usage.percent}% > {settings.DISK_USAGE_THRESHOLD_PERCENT}%) removing largest ones"
            )
            dirs = []
            for entry in os.listdir(settings.PANDA_LOGGER_PATH):
                dirpath = os.path.join(settings.PANDA_LOGGER_PATH, entry)
                if os.path.isdir(dirpath):
                    size = self.get_dir_size(dirpath)
                    dirs.append((dirpath, size))

            dirs.sort(key=lambda x: x[1], reverse=True)
            n_dirs_to_remove = min(len(dirs), settings.N_TOP_DIRS_TO_REMOVE)
            for dirpath, size in dirs[:n_dirs_to_remove]:
                self.logger.info(f"Removing directory: {dirpath}, size: {size}")
                shutil.rmtree(dirpath)

        # remove old logs
        for r, d, f in os.walk(settings.PANDA_LOGGER_PATH):
            for log_dir in d:
                dirpath = os.path.join(r, log_dir)
                if (time.time() - os.path.getctime(dirpath)) / 60 / 60 / 24 > settings.MAX_LOG_AGE_DAYS:
                    shutil.rmtree(dirpath)
        self.logger.info("PandaLogsStorageCleanUp finished")
