"""
Install:
schedule
"""

import schedule
import time
import threading
import logging.config
from logging.handlers import RotatingFileHandler

from schedinstances.TextFileURLs import TextFileURLs
from schedinstances.ArtPackages import ArtPackages, ArtLoadResults, ArtRetentionPolicy
from schedinstances.ArtMails import ArtMails, ArtDevMails
from schedinstances.BigTasks import BigTasks
from schedinstances.Harvester import Harvester
from schedinstances.SQLAggregator import SQLAggregator
from schedinstances.SQLAggregatorCampaign import SQLAggregatorCampaign
from schedinstances.PandaLogsStorageCleanUp import PandaLogsStorageCleanUp
from schedinstances.GrafanaPlots import GrafanaPlots
from schedinstances.DataCarouselPrestageCollector import DataCarouselPrestageCollector
from schedinstances.MLFlowCleanup import MLFlowCleanup

from settingscron import EXECUTION_CAP_FOR_MAINMENUURLS
try:
    from core import settings
    LOG_PATH = settings.local.LOG_ROOT + '/cachecontroller.log'
except ImportError:
    from settingscron import LOG_PATH

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

logging.basicConfig(
    level=logging.DEBUG,
    # filename=LOG_PATH,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[RotatingFileHandler(LOG_PATH, maxBytes=100000000, backupCount=10)],
)

mainMenuURLs = TextFileURLs(EXECUTION_CAP_FOR_MAINMENUURLS)
infrequentURLS = TextFileURLs(EXECUTION_CAP_FOR_MAINMENUURLS)
infrequentURLS.setInputFile("infrequenturls.txt")

artPackages = ArtPackages(EXECUTION_CAP_FOR_MAINMENUURLS)
artLoadResults = ArtLoadResults(EXECUTION_CAP_FOR_MAINMENUURLS)
artRetentionPolicy = ArtRetentionPolicy(EXECUTION_CAP_FOR_MAINMENUURLS)
artMails = ArtMails(EXECUTION_CAP_FOR_MAINMENUURLS)
artDevMails = ArtDevMails(EXECUTION_CAP_FOR_MAINMENUURLS)
bigTasks = BigTasks(EXECUTION_CAP_FOR_MAINMENUURLS)
harvester = Harvester(EXECUTION_CAP_FOR_MAINMENUURLS)
grafanaPlots = GrafanaPlots(EXECUTION_CAP_FOR_MAINMENUURLS)
cephCleanUp = PandaLogsStorageCleanUp()
sQLAggregator = SQLAggregator()
sQLAggregatorCampaign = SQLAggregatorCampaign()
stageProgressCollector = DataCarouselPrestageCollector()
mlFlowCleanUp = MLFlowCleanup()


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.daemon = True
    job_thread.start()


schedule.every(10).minutes.do(run_threaded, mainMenuURLs.execute)
schedule.every(10).minutes.do(run_threaded, bigTasks.execute)
schedule.every(10).minutes.do(run_threaded, harvester.execute)
schedule.every(20).minutes.do(run_threaded, artPackages.execute)
schedule.every(10).minutes.do(run_threaded, artLoadResults.execute)
schedule.every().day.at("12:00").do(run_threaded, artRetentionPolicy.execute)
schedule.every(1).hours.do(run_threaded, artDevMails.execute)
schedule.every(1).hours.do(run_threaded, sQLAggregator.execute)
# schedule.every(1).hours.do(run_threaded, sQLAggregatorCampaign.execute)
schedule.every().hour.at(":05").do(run_threaded, grafanaPlots.execute)
schedule.every(1).hours.do(run_threaded, infrequentURLS.execute)
schedule.every().day.at("20:18").do(run_threaded, cephCleanUp.execute)
schedule.every().day.at("07:00").do(run_threaded, artMails.execute)  # UTC
schedule.every().day.at("10:00").do(run_threaded, artMails.execute)  # UTC
schedule.every(2).hours.do(run_threaded, stageProgressCollector.execute)
schedule.every(10).minutes.do(run_threaded, mlFlowCleanUp.execute)

while 1:
    schedule.run_pending()
    time.sleep(1)