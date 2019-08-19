"""
    Created on 14.08.2019 by Sergey Padolski
"""

from django.shortcuts import render_to_response
from django.db import connection
from core.views import initRequest, setupView
from django.views.decorators.cache import never_cache
import libs.CampaignPredictionHelper as cph
from django.core.cache import cache
import operator
from django.utils.six.moves import cPickle as pickle
from django.http import JsonResponse


taskFinalStates = ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']

def campaignPredictionInfo(request):
    initRequest(request)

    if 'campaign' in request.GET:
        campaign = request.GET['campaign']
    else:
        campaign = None

    if 'subcampaign' in request.GET:
        subcampaign = request.GET['subcampaign']
    else:
        subcampaign = None

    campaignInfo = {}
    if (campaign):
        data = cache.get("concatenate_ev_" + str(campaign['campaign']) + "_" + str(campaign['subcampaign']), None)
        if data:
            data = pickle.loads(data)
            campaign_df = data['campaign_df']
            concatenate_ev = data['reindexedbyDay_ev']

            numberOfDoneEventsPerStep = {}
            numberOfSubmittedEventsPerStep = {}
            numberOfRemainingEventsPerStep = {}
            numberOfTotalEventsPerStep = {}
            progressOfEventsPerStep = {}

            for step in campaign_df.STEP_NAME.unique().tolist():
                numberOfDoneEventsPerStep[step] = campaign_df.loc[(campaign_df['STEP_NAME'] == step)]['DONEEV'].sum()
                numberOfSubmittedEventsPerStep[step] = \
                    campaign_df.loc[(campaign_df['STEP_NAME'] == step) & (~campaign_df['STATUS'].isin(taskFinalStates))][
                        'TOTEV'].sum()
                numberOfRemainingEventsPerStep[step] = \
                    campaign_df.loc[(campaign_df['STEP_NAME'] == step) & (~campaign_df['STATUS'].isin(taskFinalStates))][
                        'TOTEVREM'].sum()
                numberOfTotalEventsPerStep[step] = numberOfDoneEventsPerStep[step] + numberOfRemainingEventsPerStep[step]

            stepWithMaxEvents = max(numberOfTotalEventsPerStep.items(), key=operator.itemgetter(1))[0]
            maxEvents = numberOfTotalEventsPerStep[stepWithMaxEvents]

            for step in campaign_df.STEP_NAME.unique().tolist():
                progressOfEventsPerStep[step] = (numberOfDoneEventsPerStep[step]) / maxEvents

            remainingForSubmitting = {}
            remainingForMaxPossible = {}
            for step in campaign_df.STEP_NAME.unique().tolist():
                if concatenate_ev[step].rolling(12, win_type='triang').mean()[-1] > 0:
                    remainingForSubmitting[step] = numberOfRemainingEventsPerStep[step] / \
                                                   concatenate_ev[step].rolling(12, win_type='triang').mean()[-1]
                    remainingForMaxPossible[step] = (maxEvents - numberOfDoneEventsPerStep[step]) / \
                                                    concatenate_ev[step].rolling(12, win_type='triang').mean()[-1]

            campaignInfo['remainingForSubmitting'] = remainingForSubmitting
            campaignInfo['remainingForMaxPossible'] = remainingForMaxPossible
            campaignInfo['numberOfDoneEventsPerStep'] = numberOfDoneEventsPerStep
            campaignInfo['numberOfSubmittedEventsPerStep'] = numberOfSubmittedEventsPerStep
            campaignInfo['numberOfRemainingEventsPerStep'] = numberOfRemainingEventsPerStep
            campaignInfo['numberOfTotalEventsPerStep'] = numberOfTotalEventsPerStep
            campaignInfo['steps'] = campaign_df.STEP_NAME.unique().tolist()
            campaignInfo['subcampaign'] = subcampaign
            campaignInfo['campaign'] = campaign

    return JsonResponse(campaignInfo, safe=False)


@never_cache
def campaignPredictionDash(request):
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''

    # 1. We find all active campaigns
    campaigns = cph.getActiveCampaigns(connection)

    # 2. We select only campaigns with precached data.
    campaigns_info = {}

    for campaign in campaigns:
        cache_key = "concatenate_ev_" + str(campaign['campaign']) + "_" + str(campaign['subcampaign'])
        if cache_key in cache:
            subcampaign = campaign['subcampaign'] if campaign['subcampaign'] else ''
            campaigns_info.setdefault(campaign['campaign'], []).append(subcampaign)
    data = {
        'campaigns_info':campaigns_info,
        'request': request,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render_to_response('CampaignCalculator.html', data, content_type='text/html')
    return response

