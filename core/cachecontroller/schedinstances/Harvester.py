from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
from settingscron import HARVESTER_LIST_URL


class Harvester(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock() #

    def getpayload(self):
        print("Harvester")
        urlsQueue = queue.PriorityQueue(-1)
        harvList = self.downloadPayloadJSON(HARVESTER_LIST_URL)

        if harvList is not None:
            for hin in harvList:
                urlsQueue.put((self.BASIC_PRIORITY, '/harvesters/?instance='+str(hin['instance'])))

        return urlsQueue

