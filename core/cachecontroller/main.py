import schedule
import time
import threading
from schedinstances.MainMenuURLs import MainMenuURLs
from schedinstances.ArtPackages import ArtPackages
from schedinstances.ArtMails import ArtMails
from schedinstances.BigTasks import BigTasks
from schedinstances.Harvester import Harvester

from settingscron import EXECUTION_CAP_FOR_MAINMENUURLS


mainMenuURLs = MainMenuURLs(EXECUTION_CAP_FOR_MAINMENUURLS)
artPackages = ArtPackages(EXECUTION_CAP_FOR_MAINMENUURLS)
artMails = ArtMails(EXECUTION_CAP_FOR_MAINMENUURLS)
bigTasks = BigTasks(EXECUTION_CAP_FOR_MAINMENUURLS)
harvester = Harvester(EXECUTION_CAP_FOR_MAINMENUURLS)

#mainMenuURLs.processPayload()

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

# schedule.every(1200).seconds.do(run_threaded, mainMenuURLs.execute)
# schedule.every(1200).seconds.do(run_threaded, artPackages.execute)
# schedule.every(1200).seconds.do(run_threaded, bigTasks.execute)
# schedule.every().day.at("00:00").do(run_threaded, artMails.execute)
schedule.every(10).seconds.do(run_threaded, harvester.execute)

while 1:
    schedule.run_pending()
    time.sleep(1)



"""
Install:
schedule

"""
