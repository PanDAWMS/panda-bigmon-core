from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
import logging

class ArtMails(BaseURLTasksProvider):
    lock = threading.RLock() #
    BASIC_PRIORITY = 1
    logger = logging.getLogger(__name__)

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/art/sendartreport/?json&ntag_from=' + (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d') + '&ntag_to=' + (datetime.now().strftime('%Y-%m-%d'))))
        return urlsQueue
