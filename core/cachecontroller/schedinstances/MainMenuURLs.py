from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
import logging

class MainMenuURLs(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__)

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        with open('mainmenurls.txt') as urls:
            for line in urls:
                line = line.rstrip('\r\n')
                urlsQueue.put((self.BASIC_PRIORITY, line))
        return urlsQueue

