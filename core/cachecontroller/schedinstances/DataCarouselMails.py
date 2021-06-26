import threading
import time
import logging
from core.art.artMail import send_mail_art
from core.cachecontroller.BaseTasksProvider import BaseTasksProvider
from core.settings.base import DATA_CAROUSEL_MAIL_DELAY_DAYS, DATA_CARUSEL_MAIL_RECIPIENTS, DATA_CAROUSEL_MAIL_REPEAT
from django.core.cache import cache

# DEBUG

#from django.core.wsgi import get_wsgi_application
#application = get_wsgi_application()



mail_template = "templated_email/dataCarouselStagingAlert.html"
max_mail_attempts = 10

class DataCarouselMails(BaseTasksProvider):
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' DataCaruselMails')

    def processPayload(self):
        self.logger.info("DataCaruselMails started")
        try:
            query = """SELECT t1.DATASET, t1.STATUS, t1.STAGED_FILES, t1.START_TIME, t1.END_TIME, t1.RSE as RSE, t1.TOTAL_FILES, 
                    t1.UPDATE_TIME, t1.SOURCE_RSE, t2.TASKID, t3.campaign, t3.PR_ID, ROW_NUMBER() OVER(PARTITION BY t1.DATASET_STAGING_ID ORDER BY t1.start_time DESC) AS occurence, (CURRENT_TIMESTAMP-t1.UPDATE_TIME) as UPDATE_TIME, t4.processingtype FROM ATLAS_DEFT.T_DATASET_STAGING t1
                    INNER join ATLAS_DEFT.T_ACTION_STAGING t2 on t1.DATASET_STAGING_ID=t2.DATASET_STAGING_ID
                    INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t3 on t2.TASKID=t3.TASKID 
                    INNER JOIN ATLAS_PANDA.JEDI_TASKS t4 on t2.TASKID=t4.JEDITASKID where END_TIME is NULL and (t1.STATUS = 'staging') and t1.START_TIME <= TRUNC(SYSDATE) - {}
                    """.format(DATA_CAROUSEL_MAIL_DELAY_DAYS)
            db = self.pool.acquire()
            cursor = db.cursor()
            rows = cursor.execute(query)
        except Exception as e:
            self.logger.error(e)
            return -1
        for r in rows:
            self.logger.debug("DataCaruselMails processes this Rucio Rule: {}".format(r[5]))
            data = {"SE":r[8], "RR":r[5], "START_TIME":r[3], "TASKID":r[9], "TOT_FILES": r[6], "STAGED_FILES": r[2]}
            self.send_email(data)
        self.logger.info("DataCaruselMails finished")


    def send_email(self, data):
        subject = "Data Carousel Alert for {}".format(data['SE'])
        for recipient in DATA_CARUSEL_MAIL_RECIPIENTS:
            cache_key = "mail_sent_flag_{RR}_{RECIPIENT}".format(RR=data["RR"], TASKID=data["TASKID"],
                                                                        RECIPIENT=recipient)
            if not cache.get(cache_key, False):
                is_sent = False
                i = 0
                while not is_sent:
                    i += 1
                    if i > 1:
                        time.sleep(10)
                    is_sent = send_mail_art(mail_template, subject, data, recipient, send_html=True)
                    self.logger.debug("Email to {} attempted to send with result {}".format(recipient, is_sent))
                    # put 10 seconds delay to bypass the message rate limit of smtp server
                    time.sleep(10)
                    if i >= max_mail_attempts:
                        break

                if is_sent:
                    cache.set(cache_key, "1", DATA_CAROUSEL_MAIL_REPEAT*24*3600)



