from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
from settingscron import HARVESTER_LIST_URL
import logging


class Harvester(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' Harvester')

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        harvList = self.downloadPayloadJSON(HARVESTER_LIST_URL)

        if harvList is not None:
            for hin in harvList:
                urlsQueue.put((self.BASIC_PRIORITY, '/harvesters/?instance='+str(hin['instance'])))

        return urlsQueue

