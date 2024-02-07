from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
import logging

class ArtPackages(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    N_DAYS_WINDOW = 7
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' ArtPackages')

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/art/updatejoblist/?ntag_to={}&ntag_from={}'.format(
            datetime.now().strftime('%Y-%m-%d'),
            (datetime.now() - timedelta(days=self.N_DAYS_WINDOW)).strftime('%Y-%m-%d')
        )))
        return urlsQueue


class ArtLoadResults(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' ArtLoadSubResults')

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/art/loadsubresults/'))
        return urlsQueue