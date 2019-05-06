from BaseTasksProvider import BaseTasksProvider
from datetime import datetime, timedelta
import threading
from settingscron import MAX_NUMBER_OF_ACTIVE_DB_SESSIONS, TIMEOUT_WHEN_DB_LOADED
import logging

class SQLAggregator(BaseTasksProvider):

    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' SQLAggregator')

    def processPayload(self):

        self.logger.info("processPayload started")

        while (self.getNumberOfActiveDBSessions() > MAX_NUMBER_OF_ACTIVE_DB_SESSIONS):
            threading.sleep(TIMEOUT_WHEN_DB_LOADED)

        quotas = ('cpua1', 'cpua7', 'cpup1', 'cpup7')
        ## Pick up the users with nonzero usage and zero them out
        try:
            query = "SELECT name,dn FROM ATLAS_PANDAMETA.USERS where cpua1>0 or cpua7>0 or cpup1>0 or cpup7>0"
            db = self.pool.acquire()
            cursor = db.cursor()
            rows = cursor.execute(query)
        except Exception as e:
            self.logger.error(e)
            return -1

        users = {}
        for r in rows:
            users[r[0]] = {}
            users[r[0]]['name'] = r[0]
            for var_name in quotas:
                users[r[0]][var_name] = 0

        for days in (1, 7):
            current_time = datetime.utcnow()
            start_time = current_time - timedelta(days=days)

            tables = ('ATLAS_PANDA.JOBSACTIVE4', 'ATLAS_PANDA.JOBSARCHIVED4', 'ATLAS_PANDAARCH.JOBSARCHIVED')  # List of Tables

            for t in tables:  # For each table get the info from Database
                try:
                    # utils.initDB(t)
                    query = "SELECT prodUserName, SUM(cpuConsumptionTime) as cpuConsumptionTime, workingGroup \
                                FROM %s WHERE modificationTime > :start_time AND (prodSourceLabel = 'user' OR \
                                prodSourceLabel = 'panda') AND jobStatus != 'cancelled' GROUP BY workingGroup, prodUserName" % (t)
                    rows = cursor.execute(query, {'start_time':start_time})
                except Exception as e:
                    self.logger.error(e)
                    return -1

                for r in rows:
                    user = r[0].replace("'", "")
                    cpuconsumption = (r[1])
                    if not r[2] is None:
                        ## Include in group production stats
                        var_name = 'cpup%s' % days
                    else:
                        ## Include in personal analysis stats
                        var_name = 'cpua%s' % days
                    if user in users:
                        if not var_name in users[user]: users[user][var_name] = 0
                    else:
                        users[user] = {}
                        users[user]['name'] = user
                        users[user][var_name] = 0
                    users[user][var_name] += cpuconsumption

        userlist = []
        for u in users:
            userdict = {}
            userdict['name'] = u
            for days in (1, 7):
                for p in ('cpup%s' % days, 'cpua%s' % days):
                    if p in users[u]: userdict[p] = users[u][p]
            userlist.append(userdict)
        try:
            cursor.executemany("UPDATE ATLAS_PANDAMETA.USERS SET cpua1 = :cpua1, cpua7 = :cpua7, cpup1 = :cpup1, cpup7 = :cpup7 where name = :name", userlist)
            db.commit()
        except Exception as e:
            self.logger.error(e)

        cursor.close()
        return 0

