import logging

class ServiceLogger:
    """
    Logger for service monitoring
    """
    def __init__(self, name, file, loglevel='DEBUG'):

        try:
            from core import settings
            dir_logs = settings.local.LOG_ROOT
        except ImportError:
            dir_logs = '/tmp/'

        self.dirpath = dir_logs + '/' if not dir_logs.endswith('/') else dir_logs
        self.logger = self.__get_logger(loglevel, name)

    # private method
    def __get_logger(self, loglevel, name=__name__, encoding='utf-8'):
        log = logging.getLogger(name)
        level = logging.getLevelName(loglevel)
        log.setLevel(level)

        formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-1s %(message)s')

        file_name = self.dirpath + name + '.log'

        if log.hasHandlers():
            log.handlers.clear()

        fh = logging.FileHandler(file_name, mode='a', encoding=encoding)
        fh.setFormatter(formatter)
        log.addHandler(fh)

        return log