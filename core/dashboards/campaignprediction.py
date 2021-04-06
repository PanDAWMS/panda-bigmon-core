"""
    Created on 14.08.2019 by Sergey Padolski
"""

from django.shortcuts import render_to_response
from django.db import connection
from core.views import initRequest, setupView
from django.views.decorators.cache import never_cache
import core.libs.CampaignPredictionHelper as cph
from django.core.cache import cache
import operator
from django.utils.six.moves import cPickle as pickle
from django.http import JsonResponse
import numpy as np
import humanize
from core.auth.utils import login_customrequired

taskFinalStates = ['cancelled', 'failed', 'broken', 'aborted', 'finished', 'done']
stepsOrder = ['Evgen', 'Evgen Merge', 'Simul', 'Merge', 'Digi', 'Reco', 'Rec Merge', 'Deriv', 'Deriv Merge', 'Rec TAG', 'Atlfast', 'Atlf Merge']


@never_cache
def campaignPredictionInfo(request):
    initRequest(request)

    if 'campaign' in request.GET:
        campaign = request.GET['campaign']
    else:
        campaign = None

    subcampaign = 'None'

    campaignInfo = {}
    if (campaign):
        data = cache.get("concatenate_ev_" + str(campaign) + "_" + str(None) + "_v1", None)
        if data:
            data = pickle.loads(data)
            numberOfDoneEventsPerStep = data['numberOfDoneEventsPerStep']
            numberOfRunningEventsPerStep = data['numberOfRunningEventsPerStep']
            numberOfTotalEventsPerStep = data['numberOfTotalEventsPerStep']
            progressOfEventsPerStep = data['progressOfEventsPerStep']
            numberOfRemainingEventsPerStep = data['numberOfRemainingEventsPerStep']
            remainingForSubmitting = data['remainingForSubmitting']
            remainingForMaxPossible = data['remainingForMaxPossible']
            progressForSubmitted = data['progressForSubmitted']
            progressForMax = data['progressForMax']
            stepWithMaxEvents = data['progressForMax']
            maxEvents = data['maxEvents']
            eventsPerDay = data['eventsPerDay']

            #Here we ordering steps
            uniqueStepsInCampaig = ['Reco']
            orderedSteps = [step for step in stepsOrder if step in uniqueStepsInCampaig]
            missingSteps = [step for step in uniqueStepsInCampaig if step not in orderedSteps]
            orderedSteps = orderedSteps + missingSteps

            #Fill out the output dictionary
            campaignInfo['remainingForSubmitting'] = convertTypes(remainingForSubmitting)
            campaignInfo['remainingForMaxPossible'] = convertTypes(remainingForMaxPossible)
            campaignInfo['numberOfDoneEventsPerStep'] = convertTypes(numberOfDoneEventsPerStep)
            campaignInfo['numberOfSubmittedEventsPerStep'] = convertTypes(numberOfRunningEventsPerStep)
            campaignInfo['numberOfRemainingEventsPerStep'] = convertTypes(numberOfRemainingEventsPerStep)
            campaignInfo['numberOfTotalEventsPerStep'] = convertTypes(numberOfTotalEventsPerStep)
            campaignInfo['steps'] = orderedSteps
            campaignInfo['subcampaign'] = subcampaign
            campaignInfo['campaign'] = campaign
            campaignInfo['progressForSubmitted'] = progressForSubmitted
            campaignInfo['progressForMax'] = progressForMax
            campaignInfo['stepWithMaxEvents'] = stepWithMaxEvents
            campaignInfo['eventsPerDay'] = eventsPerDay
    return JsonResponse(campaignInfo, safe=False)



@never_cache
def campaignPredictionInfo_v1(request):
    initRequest(request)

    if 'campaign' in request.GET:
        campaign = request.GET['campaign']
    else:
        campaign = None

    if 'subcampaign' in request.GET and len(request.GET['subcampaign']) > 1:
        subcampaign = request.GET['subcampaign']
    else:
        subcampaign = 'None'

    campaignInfo = {}
    if (campaign):
        data = cache.get("concatenate_ev_" + str(campaign) + "_" + str(subcampaign), None)
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
            progressForSubmitted = {}
            eventsPerDay = {}
            progressForMax = {}
            for step in concatenate_ev:
                rollingRes = concatenate_ev[step].rolling(12, win_type='triang').mean()
                if len(rollingRes) > 2 and rollingRes[-1] > 0 and step in numberOfRemainingEventsPerStep:
                    remainingForSubmitting[step] = str( round(numberOfRemainingEventsPerStep[step] / \
                                                   rollingRes[-1], 2)) + " d"
                    remainingForMaxPossible[step] = str(round((maxEvents - numberOfDoneEventsPerStep[step]) / \
                                                    rollingRes[-1], 2)) + " d"
                    progressForSubmitted[step] = round(numberOfDoneEventsPerStep[step]*100.0/(numberOfDoneEventsPerStep[step] + numberOfRemainingEventsPerStep[step]),1)
                    progressForMax[step] = int(numberOfDoneEventsPerStep[step]*100.0/maxEvents)
                    eventsPerDay[step] = humanize.intcomma(int(rollingRes[-1]))

            #Here we ordering steps
            uniqueStepsInCampaig = campaign_df.STEP_NAME.unique().tolist()
            orderedSteps = [step for step in stepsOrder if step in uniqueStepsInCampaig]
            missingSteps = [step for step in uniqueStepsInCampaig if step not in orderedSteps]
            orderedSteps = orderedSteps + missingSteps

            #Fill out the output dictionary
            campaignInfo['remainingForSubmitting'] = convertTypes(remainingForSubmitting)
            campaignInfo['remainingForMaxPossible'] = convertTypes(remainingForMaxPossible)
            campaignInfo['numberOfDoneEventsPerStep'] = convertTypes(numberOfDoneEventsPerStep)
            campaignInfo['numberOfSubmittedEventsPerStep'] = convertTypes(numberOfSubmittedEventsPerStep)
            campaignInfo['numberOfRemainingEventsPerStep'] = convertTypes(numberOfRemainingEventsPerStep)
            campaignInfo['numberOfTotalEventsPerStep'] = convertTypes(numberOfTotalEventsPerStep)
            campaignInfo['steps'] = orderedSteps
            campaignInfo['subcampaign'] = subcampaign
            campaignInfo['campaign'] = campaign
            campaignInfo['progressForSubmitted'] = progressForSubmitted
            campaignInfo['progressForMax'] = progressForMax
            campaignInfo['stepWithMaxEvents'] = stepWithMaxEvents
            campaignInfo['eventsPerDay'] = eventsPerDay
    return JsonResponse(campaignInfo, safe=False)

def convertTypes(object):
    for k, v in object.items():
        if type(v) in [np.int64, int]:
            object.update({k: humanize.intcomma(int(v))})
        elif type(v) == np.float64:
            object.update({k: float(v)})
    return object


@login_customrequired
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

    response = render_to_response('CampaignCalculator_reduced.html', data, content_type='text/html')
    return response

