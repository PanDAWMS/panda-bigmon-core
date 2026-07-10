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
from schedinstances.Harvester import Harvester
from schedinstances.SQLAggregator import SQLAggregator
from schedinstances.SQLAggregatorCampaign import SQLAggregatorCampaign
from schedinstances.PandaLogsStorageCleanUp import PandaLogsStorageCleanUp
from schedinstances.GrafanaPlots import GrafanaPlots
from schedinstances.DataCarousel import DataCarouselAlert
from schedinstances.MLFlowCleanup import MLFlowCleanup
from schedinstances.RatedTasks import RatedTasks
from schedinstances.CacheCleanUp import CacheCleanUp

from settingscron import EXECUTION_CAP_FOR_MAINMENUURLS

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
from django.conf import settings

LOG_PATH = getattr(settings, 'LOG_ROOT', '/tmp/') + 'cachecontroller.log'
AUTH_TOKEN = getattr(settings, 'SERVICE_TOKEN', None)

headers = {}
if AUTH_TOKEN:
    headers['Authorization'] = f'Token {AUTH_TOKEN}'

logging.basicConfig(
    level=logging.DEBUG,
    # filename=LOG_PATH,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[RotatingFileHandler(LOG_PATH, maxBytes=100000000, backupCount=10)],
)
print("Logging to file: " + LOG_PATH, f"token: {'ok' if AUTH_TOKEN else 'Not set'}")

mainMenuURLs = TextFileURLs(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
infrequentURLS = TextFileURLs(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
infrequentURLS.setInputFile("infrequenturls.txt")

artPackages = ArtPackages(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
artLoadResults = ArtLoadResults(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
artRetentionPolicy = ArtRetentionPolicy(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
artMails = ArtMails(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
artDevMails = ArtDevMails(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
harvester = Harvester(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
grafanaPlots = GrafanaPlots(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
cephCleanUp = PandaLogsStorageCleanUp()
cacheCleanUp = CacheCleanUp()
sQLAggregator = SQLAggregator()
sQLAggregatorCampaign = SQLAggregatorCampaign()
dataCarouselAlert = DataCarouselAlert(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)
mlFlowCleanUp = MLFlowCleanup()
ratedTasks = RatedTasks(EXECUTION_CAP_FOR_MAINMENUURLS, headers=headers)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.daemon = True
    job_thread.start()


schedule.every(10).minutes.do(run_threaded, mainMenuURLs.execute)
schedule.every(10).minutes.do(run_threaded, harvester.execute)
schedule.every(20).minutes.do(run_threaded, artPackages.execute)
schedule.every(10).minutes.do(run_threaded, artLoadResults.execute)
schedule.every().day.at("12:00").do(run_threaded, artRetentionPolicy.execute)
schedule.every(1).hours.do(run_threaded, artDevMails.execute)
schedule.every(1).hours.do(run_threaded, sQLAggregator.execute)
# schedule.every(1).hours.do(run_threaded, sQLAggregatorCampaign.execute)  # disabled as it is not used
schedule.every().hour.at(":05").do(run_threaded, grafanaPlots.execute)
schedule.every(1).hours.do(run_threaded, infrequentURLS.execute)
schedule.every(1).hours.do(run_threaded, cephCleanUp.execute)
schedule.every().day.at("07:00").do(run_threaded, artMails.execute)  # UTC
schedule.every().day.at("10:00").do(run_threaded, artMails.execute)  # UTC
schedule.every().day.at("10:00").do(run_threaded, dataCarouselAlert.execute)
# schedule.every(10).minutes.do(run_threaded, mlFlowCleanUp.execute)  # disabled as it is not used
schedule.every().monday.at("07:00").do(run_threaded, ratedTasks.execute)
schedule.every(1).hours.do(run_threaded, cacheCleanUp.execute)

while 1:
    schedule.run_pending()
    time.sleep(1)