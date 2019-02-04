import urllib2, socket
from BaseTasksProvider import BaseTasksProvider
import Queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from settingscron import MAX_NUMBER_OF_ACTIVE_DB_SESSIONS, TIME_OUT_FOR_QUERY, NUMBER_OF_ITEMS_TO_DRAIN

class AbstractURLTasksProvider(BaseTasksProvider):

    EXECUTIONCAP = 5
    isActive = False
    baseURL = "http://bigpanda.cern.ch"

    def __init__(self, executioncap):
        self.EXECUTIONCAP = executioncap

    def getpayload(self):
        return Queue.PriorityQueue(-1)

    def getvalidityperiod(self):
        pass

    def getaggressiveness(self):
        # tasks executed:
        # 0 - one-by-one basing on the articular queue priority
        # 1 - in parallel with basic competition
        # 2 - all tasks should be completed within the validity time interval in exception to 0 priority
        pass

    def processPayload(self):
        starttask = time.time()

        def fetchURL(jobtofetch):
            urltofetch = jobtofetch[1]
            priority = jobtofetch[0]
            timeout = False
            failedFetch = False
            start = time.time()
            numsess = self.getNumberOfActiveDBSessions()
            if numsess != -1 and numsess < MAX_NUMBER_OF_ACTIVE_DB_SESSIONS:
                try:
                    req = urllib2.Request(self.baseURL + urltofetch)
                    urllib2.urlopen(req, timeout=TIME_OUT_FOR_QUERY)
                except Exception or urllib2.HTTPError as e:
                    if isinstance(e.reason, socket.timeout):
                        timeout = True
                    else:
                        failedFetch = True
            else:
                #We postpone the job if DB is oveloaded
                payload.put(jobtofetch)
                time.sleep(10)
                return (None, None, None, None)  # Operation did not performe due to DB overload
            return (time.time() - start, timeout, failedFetch, urltofetch)

        futuresList = []
        payload = self.getpayload()
        executor = ThreadPoolExecutor(max_workers=self.EXECUTIONCAP)
        urlsfailed = 0
        utlsfimeout = 0
        totalurls = payload.qsize()

        while 1:
            while not payload.empty():
                item = executor.submit(fetchURL, (payload.get()))
                futuresList.append(item)
            for future in as_completed(futuresList):
                try:
                    (exectime, timeout, failure, urltofetch) = future.result(timeout=TIME_OUT_FOR_QUERY + 100)
                    if urltofetch:
                        print(urltofetch + " Done")
                    else:
                        print(" Yielding ")

                except TimeoutError:
                    success = False
            if payload.empty():
                break

        totalTime = time.time()-starttask
        return (starttask, totalTime, totalurls, utlsfimeout, urlsfailed)



