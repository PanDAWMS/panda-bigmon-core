from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
from datetime import datetime, timedelta
from settingscron import URL_WITH_BIG_TASKS, URL_WITH_ES_TASKS
import logging


class BigTasks(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' BigTasks')

    def getpayload(self):
        self.logger.info("getpayload started")

        urlsQueue = queue.PriorityQueue(-1)
        esTaskList = self.downloadPayloadJSON(URL_WITH_ES_TASKS)

        if esTaskList:
            for task in esTaskList:
                urlsQueue.put((self.BASIC_PRIORITY, '/task/' + str(task['jeditaskid']) + '/'))

                # cache of jobsummary and plots for input-centric task page
                urlsQueue.put((self.BASIC_PRIORITY, '/getjobsummaryfortask/' + str(task['jeditaskid']) + '/?mode=nodrop&infotype=jobsummary'))
                urlsQueue.put((self.BASIC_PRIORITY, '/getjobsummaryfortask/' + str(task['jeditaskid']) + '/?mode=drop&infotype=jobsummary'))

        bigTaskList = self.downloadPayloadJSON(URL_WITH_BIG_TASKS)
        if bigTaskList:
            for task in bigTaskList:
                urlsQueue.put((self.BASIC_PRIORITY, '/task/' + str(task['jeditaskid'])+'/'))
                urlsQueue.put((self.BASIC_PRIORITY, '/task/' + str(task['jeditaskid']) + '/?mode=nodrop'))

        return urlsQueue
