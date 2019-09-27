from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
import logging


class GrafanaPlots(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' GrafanaPlots')
    plots = [
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?&orgId=17&panelId=81&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=83&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=82&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=84&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=85&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=86&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=87&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=88&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=89&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=90&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=91&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/adc-live-page-stats?panelId=92&orgId=17&width=1000&height=1000',
    ]

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)

        for plot in self.plots:
            urlsQueue.put((self.BASIC_PRIORITY, '/grafana/?url='+ plot))

        return urlsQueue
