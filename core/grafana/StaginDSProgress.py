from requests import post, get
from json import loads
from core.settings.local import GRAFANA as token
from django.http import JsonResponse
from core.views import login_customrequired, initRequest,  DateTimeEncoder
from core.libs.exlib import dictfetchall
from django.db import connection
from django.utils import timezone
from datetime import timedelta
from core.settings import defaultDatetimeFormat
from django.shortcuts import render_to_response
from core.views import login_customrequired, initRequest, setupView
import pandas as pd


def run_query(rules):
    base = "https://monit-grafana.cern.ch"
    url = "api/datasources/proxy/8428/_msearch"

    rulequery = ""
    for rule in rules:
        rulequery += " data.rule_id: %s OR" % rule

    rulequery = rulequery[:-3]
    paramQuery = """{"filter":[{"query_string":{"analyze_wildcard":true,"query":"data.event_type:rule_progress AND (%s)"}}]}""" % rulequery
    query = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_rucio_raw_events*"]}\n{"size":500,"query":{"bool":"""+paramQuery+"""},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{},"docvalue_fields":["metadata.timestamp"]}\n"""
    headers = token
    headers['Content-Type'] = 'application/json'
    headers['Accept'] = 'application/json'

    request_url = "%s/%s" % (base, url)
    r = post(request_url, headers=headers, data=query)
    resultdict = {}
    if r.ok:
        results = loads(r.text)['responses'][0]['hits']['hits']
        for result in results:
            dictEntry = resultdict.get(result['_source']['data']['rule_id'], {})
            dictEntry[result['_source']['data']['created_at']] = result['_source']['data']['progress']
            resultdict[result['_source']['data']['rule_id']] = dictEntry
        result = resultdict
    else:
        result = None
    return result

def __getRucioRuleByTaskID(taskid):
    new_cur = connection.cursor()
    new_cur.execute(""" SELECT RSE FROM ATLAS_DEFT.T_DATASET_STAGING where DATASET IN (select PRIMARY_INPUT FROM ATLAS_DEFT.t_production_task where TASKID=%i)""" % int(taskid))
    rucioRule = dictfetchall(new_cur)
    if rucioRule and len(rucioRule) > 0:
        return rucioRule[0]['RSE']
    else:
        return None


def __getRucioRulesBySourceSEAndTimeWindow(source, hours):
    new_cur = connection.cursor()
    new_cur.execute(""" SELECT RSE FROM ATLAS_DEFT.T_DATASET_STAGING where SOURCE_RSE='%s' 
    and (START_TIME>TO_DATE('%s','YYYY-mm-dd HH24:MI:SS') or END_TIME is NULL)""" % (source, (timezone.now() - timedelta(hours=hours)).strftime(defaultDatetimeFormat)))

    """
    SELECT t1.RSE, t2.taskid FROM ATLAS_DEFT.T_DATASET_STAGING t1 LEFT JOIN ATLAS_DEFT.t_production_task t2 ON t2.PRIMARY_INPUT=t1.DATASET
    and t1.SOURCE_RSE='BNL-OSG2_DATATAPE'
    and t1.START_TIME>TO_DATE('2019-07-10 12:57:54','YYYY-mm-dd HH24:MI:SS')
    """

    rucioRulesRows = dictfetchall(new_cur)
    rucioRules = []
    if rucioRulesRows and len(rucioRulesRows) > 0:
        for rucioRulesRow in rucioRulesRows:
            rucioRules.append(rucioRulesRow['RSE'])
        return rucioRules
    else:
        return None



def getStageProfileData(request):
    valid, response = initRequest(request)
    RRules = []
    #RuleToTasks = {}
    if 'jeditaskid' in request.session['requestParams']:
        rucioRule = __getRucioRuleByTaskID(int(request.session['requestParams']['jeditaskid']))
        if rucioRule:
            RRules.append(rucioRule)
            #RuleToTasks[rucioRule] = int(request.session['requestParams']['jeditaskid'])
    elif ('stagesource' in request.session['requestParams'] and 'hours' in request.session['requestParams']):
        RRules = __getRucioRulesBySourceSEAndTimeWindow(
            request.session['requestParams']['stagesource'].strip().replace("'","''"),
            int(request.session['requestParams']['hours']))
    chunksize = 50
    chunks = [RRules[i:i + chunksize] for i in range(0, len(RRules), chunksize)]
    resDict = {}
    for chunk in chunks:
        resDict = {**resDict, **run_query(chunk)}

    """
    s1 = pd.Series([0,1], index=list('AB'))
    s2 = pd.Series([2,3], index=list('AC'))
    
    result = pd.concat([s1, s2], join='outer', axis=1, sort=False)
    print(result)
    """
    pandaDFs = {}
    RRuleNames = []
    result = []
    for RRule, progEvents in resDict.items():
        timesList = list(progEvents.keys())
        progList = list(progEvents.values())
        pandasDF = pd.Series(progList, index=timesList)
        pandaDFs[RRule] = pandasDF
        RRuleNames.append(RRule)
    if pandaDFs:
        result = pd.concat(pandaDFs.values(), join='outer', axis=1, sort=True)
        result.index = pd.to_datetime(result.index)
        result = result.resample('15min').last().reset_index().fillna(method='ffill').fillna(0)
        result['index'] = result['index'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result = [['TimeStamp',] + RRuleNames] + result.values.tolist()
    return JsonResponse(result, safe=False)

@login_customrequired
def getDATASetsProgressPlot(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''
    reqparams = ''
    if 'jeditaskid' in request.session['requestParams']:
        reqparams = 'jeditaskid='+str(int(request.session['requestParams']['jeditaskid']))
    elif ('stagesource' in request.session['requestParams'] and 'hours' in request.session['requestParams']):
        reqparams = 'stagesource='+request.session['requestParams']['stagesource'] + \
                    '&hours=' + request.session['requestParams']['hours']
    data = {
        'request': request,
        'reqparams': reqparams,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render_to_response('DSProgressplot.html', data, content_type='text/html')
    #patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 5)
    return response
