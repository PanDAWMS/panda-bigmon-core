from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
import logging


class RatedTasks(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' RatedTasks')

    def getpayload(self):
        self.logger.info("getpayload started")

        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put(
            (self.BASIC_PRIORITY, '/report/?json=1&report_type=rated_tasks&delivery=email&egroup=default&days=7&rating_threshold=5')
        )

        return urlsQueue
