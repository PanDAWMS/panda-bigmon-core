import urllib.request as urllibr
from urllib.error import HTTPError
import socket
from BaseTasksProvider import BaseTasksProvider
import time, json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from settingscron import MAX_NUMBER_OF_ACTIVE_DB_SESSIONS, TIME_OUT_FOR_QUERY, BASE_URL, TIMEOUT_WHEN_DB_LOADED


class BaseURLTasksProvider(BaseTasksProvider):

    isActive = False

    def __init__(self, executioncap):
        self.EXECUTIONCAP = executioncap
        self.baselogger = logging.getLogger(__name__)

    def getpayload(self):
        raise NotImplementedError("Must override getpayload")


    def getvalidityperiod(self):
        raise NotImplementedError("Must override getvalidityperiod")


    def getaggressiveness(self):
        # tasks executed (to be implemented):
        # 0 - one-by-one basing on the articular queue priority
        # 1 - in parallel with basic competition
        # 2 - all tasks should be completed within the validity time interval in exception to 0 priority
        raise NotImplementedError("Must override getaggressiveness")


    def downloadPayloadJSON(self, URL):
        response = None
        try:
            req = urllibr.Request(BASE_URL + URL)
            response = urllibr.urlopen(req, timeout=TIME_OUT_FOR_QUERY).read()
            response = json.loads(response)
        except Exception or HTTPError as e:
            pass
        return response


    def processPayload(self):

        self.baselogger.info("started processPayload")

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
                    req = urllibr.Request(BASE_URL + urltofetch)
                    urllibr.urlopen(req, timeout=TIME_OUT_FOR_QUERY)
                except Exception or HTTPError as e:
                    if isinstance(e, socket.timeout):
                        timeout = True
                    else:
                        failedFetch = True
            else:
                # We postpone the job if DB is overloaded
                self.logger.warning('DB is overloaded with {} active sessions, postpone the job for {}s'.format(
                    numsess, TIMEOUT_WHEN_DB_LOADED
                ))
                time.sleep(TIMEOUT_WHEN_DB_LOADED)
                return (None, None, None, jobtofetch)  # Operation did not performe due to DB overload
            return (time.time() - start, timeout, failedFetch, jobtofetch)

        payload = self.getpayload()
        executor = ThreadPoolExecutor(max_workers=self.EXECUTIONCAP)
        urlsfailed = 0
        utlsfimeout = 0
        totalurls = payload.qsize()

        while 1:
            futuresList = []
            while not payload.empty():
                item = executor.submit(fetchURL, (payload.get()))
                futuresList.append(item)
            for future in as_completed(futuresList):
                try:
                    (exectime, timeout, failedFetch, jobtofetch) = future.result(timeout=TIME_OUT_FOR_QUERY + 100)
                    if not failedFetch is None:
                        totalurls += 1
                        self.baselogger.debug(jobtofetch[1] + " Done")
                        if failedFetch:
                            urlsfailed += 1
                    else:
                        payload.put((1, jobtofetch[1]))
                        self.baselogger.debug(jobtofetch[1] + " Yielding")
                    if timeout:
                        utlsfimeout += 1

                except TimeoutError:
                    success = False
            if payload.empty():
                break

        totalTime = time.time()-starttask
        self.baselogger.info("finished processPayload")
        return (starttask, totalTime, totalurls, utlsfimeout, urlsfailed)
