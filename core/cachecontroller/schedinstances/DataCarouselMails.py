import threading
import queue
import logging

from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider


class DataCarouselMails(BaseURLTasksProvider):
    lock = threading.RLock()
    BASIC_PRIORITY = 1
    logger = logging.getLogger(__name__ + ' DataCarouselMails')

    def getpayload(self):
        self.logger.info("DataCarouselMails started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/dc/sendstalledreport/'))
        self.logger.info("DataCaruselMails finished")
        return urlsQueue







