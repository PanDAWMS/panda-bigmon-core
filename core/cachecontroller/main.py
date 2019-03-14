import schedule
import time
from MainMenuURLs import MainMenuURLs
import threading


mainMenuURLs = MainMenuURLs(5)
#mainMenuURLs.processPayload()

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()
schedule.every(10).seconds.do(run_threaded, mainMenuURLs.execute)

while 1:
    schedule.run_pending()
    time.sleep(1)



"""
Install:
schedule

"""
