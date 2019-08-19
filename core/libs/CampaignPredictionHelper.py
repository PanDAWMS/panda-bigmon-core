import logging

def getActiveCampaigns(connection):
    logger = logging.getLogger(__name__ + ' getActiveCampaigns')
    try:
        query = "SELECT count(*), CAMPAIGN, SUBCAMPAIGN FROM ATLAS_DEFT.T_PRODUCTION_TASK WHERE START_TIME > sysdate-5 group by CAMPAIGN, SUBCAMPAIGN order by count(*) desc"
        cursor = connection.cursor()
        rows = cursor.execute(query)
    except Exception as e:
        logger.error(e)
        return -1
    campaigns = []
    for r in rows:
        if r[1]:
            campaign = {"campaign": r[1], "subcampaign": r[2]}
            campaigns.append(campaign)
    return campaigns
