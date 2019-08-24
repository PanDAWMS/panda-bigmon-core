import schedule
import time
import threading
import logging.config

from schedinstances.MainMenuURLs import MainMenuURLs
from schedinstances.ArtPackages import ArtPackages
from schedinstances.ArtMails import ArtMails
from schedinstances.BigTasks import BigTasks
from schedinstances.Harvester import Harvester
from schedinstances.SQLAggregator import SQLAggregator
from schedinstances.SQLAggregatorCampaign import SQLAggregatorCampaign
from schedinstances.PandaLogsStorageCleanUp import PandaLogsStorageCleanUp
from settingscron import EXECUTION_CAP_FOR_MAINMENUURLS
from settingscron import LOG_PATH


logging.basicConfig(level=logging.DEBUG, filename=LOG_PATH, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

mainMenuURLs = MainMenuURLs(EXECUTION_CAP_FOR_MAINMENUURLS)
artPackages = ArtPackages(EXECUTION_CAP_FOR_MAINMENUURLS)
artMails = ArtMails(EXECUTION_CAP_FOR_MAINMENUURLS)
bigTasks = BigTasks(EXECUTION_CAP_FOR_MAINMENUURLS)
harvester = Harvester(EXECUTION_CAP_FOR_MAINMENUURLS)
cephCleanUp = PandaLogsStorageCleanUp()
sQLAggregator = SQLAggregator()
sQLAggregatorCampaign = SQLAggregatorCampaign()

#mainMenuURLs.processPayload()

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.daemon = True
    job_thread.start()

schedule.every().day.at("20:18").do(run_threaded, cephCleanUp.execute)
schedule.every(10).minutes.do(run_threaded, mainMenuURLs.execute)
schedule.every(10).minutes.do(run_threaded, artPackages.execute)
schedule.every(10).minutes.do(run_threaded, bigTasks.execute)
schedule.every().day.at("09:00").do(run_threaded, artMails.execute)
schedule.every(10).minutes.do(run_threaded, harvester.execute)
schedule.every(1).hours.do(run_threaded, sQLAggregator.execute)
schedule.every(10).minutes.do(run_threaded, sQLAggregatorCampaign.execute)

while 1:
    schedule.run_pending()
    time.sleep(1)



"""
Install:
schedule
"""
