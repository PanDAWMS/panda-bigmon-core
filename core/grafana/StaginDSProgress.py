from datetime import timedelta
from requests import post, get
from json import loads
import pandas as pd

from django.http import JsonResponse
from django.db import connection
from django.utils import timezone
from django.shortcuts import render

from core.oauth.decorators import login_customrequired
from core.libs.exlib import dictfetchall
from core.views import initRequest, setupView

from django.conf import settings
from django.views.decorators.cache import never_cache

def run_query(rules):
    base = "https://monit-grafana.cern.ch"
    url = "api/datasources/proxy/8428/_msearch"

    rulequery = " OR ".join([f"data.rule_id: {rule}" for rule in rules])

    paramQuery = """{"filter":[{"query_string":{"analyze_wildcard":true,"query":"data.event_type:rule_progress AND (%s)"}}]}""" % rulequery
    query = (
        '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_ddm_enr_transfer*"]}\n'
        f'{{"size":500,"query":{{"bool":{paramQuery}}},"sort":{{"metadata.timestamp":{{"order":"desc","unmapped_type":"boolean"}}}},"script_fields":{{}},"docvalue_fields":["metadata.timestamp"]}}\n'
    )
    headers = settings.GRAFANA
    headers['Content-Type'] = 'application/json'
    headers['Accept'] = 'application/json'

    request_url = "%s/%s" % (base, url)
    r = post(request_url, headers=headers, data=query)
    resultdict = {}

    if r.ok:
        results = loads(r.text)['responses'][0]['hits']['hits']
        for result in results:
            dictEntry = resultdict.get(result['_source']['data']['rule_id'], {})
            dictEntry[result['_source']['data']['event_created_at']] = result['_source']['data'].get('progress')
            resultdict[result['_source']['data']['rule_id']] = dictEntry
        result = resultdict
    else:
        result = None
    return result


def __getRucioRuleByTaskID(taskid):
    """
    Retrieves the ddm_rule_id for a given task ID using the new schema.
    """
    new_cur = connection.cursor()
    query = """
        SELECT ddm_rule_id FROM atlas_panda.data_carousel_requests 
        WHERE request_id IN (
            SELECT request_id FROM atlas_panda.data_carousel_relations WHERE task_id = :taskid
        )
    """
    new_cur.execute(query, {'taskid': int(taskid)})
    rucioRules = dictfetchall(new_cur, style='lowercase')
    if rucioRules and len(rucioRules) > 0:
        return [row['ddm_rule_id'] for row in rucioRules if row['ddm_rule_id'] is not None]
    return None


def __getRucioRulesBySourceSEAndTimeWindow(source, hours):
    """
    Retrieves a list of ddm_rule_ids matching a source RSE and a starting time window.
    """
    new_cur = connection.cursor()
    time_threshold = (timezone.now() - timedelta(hours=hours)).strftime(settings.DATETIME_FORMAT)
    query = """ 
        SELECT ddm_rule_id
        FROM atlas_panda.data_carousel_requests 
        WHERE source_rse = :source 
          AND (start_time > TO_DATE(:time_threshold, 'YYYY-MM-DD HH24:MI:SS') OR end_time IS NULL)
    """
    new_cur.execute(query, {'source': source, 'time_threshold': time_threshold})
    rucioRules = dictfetchall(new_cur, style='lowercase')

    if rucioRules:
        return [row['ddm_rule_id'] for row in rucioRules if row['ddm_rule_id'] is not None]
    return None


@login_customrequired
def getStageProfileData(request):
    valid, response = initRequest(request)
    status = 200
    RRules = []
    if 'jeditaskid' in request.session['requestParams']:
        rucioRules = __getRucioRuleByTaskID(request.session['requestParams']['jeditaskid'])
        if rucioRules:
            RRules.extend(rucioRules)
    elif 'stagesource' in request.session['requestParams'] and 'hours' in request.session['requestParams']:
        RRules = __getRucioRulesBySourceSEAndTimeWindow(
            request.session['requestParams']['stagesource'].strip().replace("'","''"),
            int(request.session['requestParams']['hours']))
    chunksize = 50
    chunks = [RRules[i:i + chunksize] for i in range(0, len(RRules), chunksize)]
    resDict = {}
    try:
        for chunk in chunks:
            resDict = {**resDict, **run_query(chunk)}
    except:
        resDict = None

    pandaDFs = {}
    RRuleNames = []
    result = []
    if resDict is not None:
        for RRule, progEvents in resDict.items():
            timesList = list(progEvents.keys())
            progList = list(progEvents.values())
            pandasDF = pd.Series(progList, index=timesList)
            pandaDFs[RRule] = pandasDF
            RRuleNames.append(RRule)
        if pandaDFs:
            result = pd.concat(pandaDFs.values(), join='outer', axis=1, sort=True)
            result.index = pd.to_datetime(result.index, unit='ms')
            result = result.resample('15min').last().reset_index().ffill().fillna(0)
            result['index'] = result['index'].dt.strftime('%Y-%m-%d %H:%M:%S')
            result = [['TimeStamp',] + RRuleNames] + result.values.tolist()
    else:
        status = 500
    return JsonResponse(result, safe=False, status=status)


@login_customrequired
@never_cache
def getDATASetsProgressPlot(request):
    initRequest(request)
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=4, limit=9999999, querytype='task', wildCardExt=True)
    request.session['viewParams']['selection'] = ''
    reqparams = ''
    if 'jeditaskid' in request.session['requestParams']:
        reqparams = 'jeditaskid='+str(int(request.session['requestParams']['jeditaskid']))
    elif 'stagesource' in request.session['requestParams'] and 'hours' in request.session['requestParams']:
        reqparams = 'stagesource='+request.session['requestParams']['stagesource'] + \
                    '&hours=' + request.session['requestParams']['hours']
    data = {
        'request': request,
        'reqparams': reqparams,
        'viewParams': request.session['viewParams'] if 'viewParams' in request.session else None,
    }

    response = render(request, 'DSProgressplot.html', data, content_type='text/html')
    return response
