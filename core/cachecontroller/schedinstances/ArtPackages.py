from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
import logging

class ArtPackages(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    N_DAYS_WINDOW = 14
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' ArtPackages')

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/art/updatejoblist/?ntag_to=' +
                       datetime.now().strftime('%Y-%m-%d') + '&ntag_from=' +
                       (datetime.now() - timedelta(days=self.N_DAYS_WINDOW)).strftime('%Y-%m-%d')))
        return urlsQueue

