from BaseTasksProvider import BaseTasksProvider
from datetime import datetime, timedelta
import threading
from settingscron import MAX_NUMBER_OF_ACTIVE_DB_SESSIONS, TIMEOUT_WHEN_DB_LOADED
import logging
import pandas as pd
import numpy as np
from django.core.cache import cache
from django.utils.six.moves import cPickle as pickle
import core.libs.CampaignPredictionHelper as cph
import sys

class SQLAggregatorCampaign(BaseTasksProvider):

    lock = threading.RLock()
    logger = logging.getLogger(__name__ + ' SQLAggregatorCampaign')

    def processPayload(self):

        self.logger.info("SQLAggregatorCampaign started")

        while (self.getNumberOfActiveDBSessions() > MAX_NUMBER_OF_ACTIVE_DB_SESSIONS):
            threading.sleep(TIMEOUT_WHEN_DB_LOADED)
        connection = self.pool.acquire()

        # we get recently alive campaigns
        campaigns = cph.getActiveCampaigns(connection)

        for campaign in campaigns:
            query = ""
            try:
                cursor = connection.cursor()
                cursor.execute("alter session set NLS_DATE_FORMAT = 'mm-dd-yyyy HH24:mi:ss'")
                cursor.execute("alter session set NLS_TIMESTAMP_FORMAT = 'mm-dd-yyyy HH24:mi:ss'")
                if campaign["subcampaign"]:
                    query = """
                        SELECT TASKID, START_TIME, ENDTIME, t1.STATUS, CAMPAIGN, SUBCAMPAIGN, TOTEVREM, TOTEV, t5.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 JOIN ATLAS_PANDABIGMON.geteventsfortask t2 
                        ON t1.TASKID=t2.JEDITASKID AND t1.campaign='%s' and t1.subcampaign='%s' 
                        JOIN ATLAS_DEFT.T_PRODUCTION_STEP t4 ON t1.STEP_ID=t4.STEP_ID JOIN ATLAS_DEFT.T_STEP_TEMPLATE t5 ON t4.STEP_T_ID=t5.STEP_T_ID
                    """ % (campaign['campaign'], campaign['subcampaign'])
                else:
                    query = """
                        SELECT TASKID, START_TIME, ENDTIME, t1.STATUS, CAMPAIGN, SUBCAMPAIGN, TOTEVREM, TOTEV, t5.STEP_NAME FROM ATLAS_DEFT.T_PRODUCTION_TASK t1 JOIN ATLAS_PANDABIGMON.geteventsfortask t2 
                        ON t1.TASKID=t2.JEDITASKID AND t1.campaign='%s' and t1.subcampaign is NULL 
                        JOIN ATLAS_DEFT.T_PRODUCTION_STEP t4 ON t1.STEP_ID=t4.STEP_ID JOIN ATLAS_DEFT.T_STEP_TEMPLATE t5 ON t4.STEP_T_ID=t5.STEP_T_ID
                    """ % campaign['campaign']
            except Exception as e:
                self.logger.error(e)
                return -1

            #We retrieve data from the DB
            campaign_df = pd.read_sql(query, con=connection)
            campaign_df['START_TIME'] = pd.to_datetime(campaign_df['START_TIME'], format="%m-%d-%Y %H:%M:%S")
            campaign_df['ENDTIME'] = pd.to_datetime(campaign_df['ENDTIME'], format="%m-%d-%Y %H:%M:%S")
            campaign_df['DONEEV'] = campaign_df['TOTEV'] - campaign_df['TOTEVREM']
            stepsname = campaign_df.STEP_NAME.unique().tolist()

            #We do reindexing to have number of events deliveres per day
            index = pd.date_range(start=campaign_df.START_TIME.min(), end=campaign_df.ENDTIME.max())
            new_df = pd.DataFrame(index=index, columns=stepsname)
            new_df_total_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D'))][
                'DONEEV'].sum(), axis=1)
            new_df_Evgen_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Evgen')]['DONEEV'].sum(), axis=1)
            new_df_Simul_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Simul')]['DONEEV'].sum(), axis=1)
            new_df_Reco_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Reco')]['DONEEV'].sum(), axis=1)
            new_df_Merge_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Merge')]['DONEEV'].sum(), axis=1)
            new_df_Deriv_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Deriv')]['DONEEV'].sum(), axis=1)
            new_df_Evgen_Merge_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Evgen Merge')]['DONEEV'].sum(), axis=1)
            new_df_Rec_Merge_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Rec Merge')]['DONEEV'].sum(), axis=1)
            new_df_Deriv_Merge_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Deriv Merge')]['DONEEV'].sum(), axis=1)
            new_df_Rec_TAG_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Rec TAG')]['DONEEV'].sum(), axis=1)
            new_df_Digi_ev = new_df.apply(lambda x: campaign_df[
                (campaign_df.ENDTIME >= x.name) & (campaign_df.ENDTIME <= x.name + np.timedelta64(1, 'D')) & (
                            campaign_df.STEP_NAME == 'Digi')]['DONEEV'].sum(), axis=1)

            reindexedbyDay_ev = pd.concat(
                [new_df_total_ev, new_df_Evgen_ev, new_df_Simul_ev, new_df_Reco_ev, new_df_Merge_ev,
                 new_df_Deriv_ev, new_df_Evgen_Merge_ev, new_df_Rec_Merge_ev, new_df_Deriv_Merge_ev, new_df_Rec_TAG_ev,
                 new_df_Digi_ev], axis=1, keys=['total', 'Evgen', 'Simul', 'Reco', 'Merge',
                                                'Deriv', 'Evgen Merge', 'Rec Merge', 'Deriv Merge', 'Rec TAG', 'Digi'])

            #Push data into the cache
            data = {"campaign_df":campaign_df,"reindexedbyDay_ev":reindexedbyDay_ev}
            cache.set( "concatenate_ev_"+str(campaign['campaign'])+"_"+str(campaign['subcampaign']), pickle.dumps(data, pickle.HIGHEST_PROTOCOL) , 60 * 60 * 72)
            self.logger.info("concatenate_ev_"+str(campaign['campaign'])+"_"+str(campaign['subcampaign']) + "  pushed into the cache")

        self.logger.info("SQLAggregatorCampaign finished")
        cursor.close()
        return 0

