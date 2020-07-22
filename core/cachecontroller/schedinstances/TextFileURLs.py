from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
import logging

class TextFileURLs(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__+' MainMenuURLs')
    inputfile = 'mainmenurls.txt'

    def setInputFile(self, filename):
        self.inputfile = filename


    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)
        with open(self.inputfile) as urls:
            for line in urls:
                line = line.rstrip('\r\n')
                urlsQueue.put((self.BASIC_PRIORITY, line))
        return urlsQueue

