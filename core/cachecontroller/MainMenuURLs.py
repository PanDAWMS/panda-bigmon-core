from core.cachecontroller.AbstractURLTasksProvider import AbstractURLTasksProvider
import queue

class MainMenuURLs(AbstractURLTasksProvider):

    BASIC_PRIORITY = 1

    def getvalidityperiod(self):
        return 20

    def getaggressiveness(self):
        return 2

    def getpayload(self):
        urlsQueue = queue.PriorityQueue(-1)
        with open('mainmenurls.txt') as urls:
            for line in urls:
                line = line.rstrip('\r\n')
                urlsQueue.put((self.BASIC_PRIORITY, line))
        return urlsQueue

