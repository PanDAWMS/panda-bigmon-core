from core.cachecontroller.BaseURLTasksProvider import BaseURLTasksProvider
import queue, threading
import logging


class GrafanaPlots(BaseURLTasksProvider):

    BASIC_PRIORITY = 1
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' GrafanaPlots')
    plots = [
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=96&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=97&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=95&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=103&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=108&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=107&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=104&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=110&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=109&orgId=17&height=600',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=101&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=105&orgId=17&width=1000&height=1000',
        'https://monit-grafana.cern.ch/render/d-solo/000000806/bigpanda-monitoring?panelId=106&orgId=17&width=1000&height=1000',
    ]

    def getpayload(self):
        self.logger.info("getpayload started")
        urlsQueue = queue.PriorityQueue(-1)

        for plot in self.plots:
            urlsQueue.put((self.BASIC_PRIORITY, '/grafana/img/?url='+ plot))

        return urlsQueue
