import logging
import queue
import threading
from BaseURLTasksProvider import BaseURLTasksProvider


class DataCarouselAlert(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' DataCarouselAlert')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put(
            (self.BASIC_PRIORITY, '/dc/sendstalledreport/?json=1')
        )
        return urlsQueue
