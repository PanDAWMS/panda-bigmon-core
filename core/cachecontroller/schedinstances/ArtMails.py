from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta

class ArtMails(BaseURLTasksProvider):
    lock = threading.RLock() #
    BASIC_PRIORITY = 1

    def getpayload(self):
        urlsQueue = queue.PriorityQueue(-1)
        urlsQueue.put((self.BASIC_PRIORITY, '/art/sendartreport/?json&ntag_from=' + (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d') + '&ntag_to=' + (datetime.now().strftime('%Y-%m-%d'))))
        return urlsQueue
