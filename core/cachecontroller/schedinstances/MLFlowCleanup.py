from core.cachecontroller.BaseTasksProvider import BaseTasksProvider
import queue, threading
from settingscron import MLFLOW_CLEANUP
import logging
from core.mlflowdynamic.openshiftcontroller import occlicalls
#MLFLOW_CLEANUP = 1

class MLFlowCleanup(BaseTasksProvider):
    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' MLFlowCleanUp')

    def processPayload(self):
        try:
            query = "SELECT jeditaskid,INSTANCEURL FROM mlflow_containers where status in ('active','spinning)' and spinned_at < CAST(SYSTIMESTAMP AT TIME ZONE 'UTC' AS DATE) - interval '%i' second" \
                    % MLFLOW_CLEANUP
            db = self.pool.acquire()
            cursor = db.cursor()
            rows = cursor.execute(query)
        except Exception as e:
            self.logger.error(e)
            return -1
        updatelist = []
        for r in rows:
            self.cleanUpDeployement(r[0], r[1])
            updatelist.append({'jeditaskid':r[0], 'instanceurl':r[1]})

        if updatelist:
            cursor.executemany(
                "UPDATE mlflow_containers SET status='deleted', deleted_at=CAST(SYSTIMESTAMP AT TIME ZONE 'UTC' AS DATE) where status='active' and  jeditaskid=:jeditaskid and INSTANCEURL=:instanceurl", updatelist)
            db.commit()

    def cleanUpDeployement(self, jeditaskid, instance_name):
        ocwrap = occlicalls(jeditaskid)
        ocwrap.remove_deployment(instance_name)
