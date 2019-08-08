from requests import post, get
from json import loads
from core.settings.local import GRAFANA as token
from django.http import JsonResponse
from core.views import login_customrequired, initRequest,  DateTimeEncoder

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


@login_customrequired
def getStageProfile(request):
    valid, response = initRequest(request)
    chunksize = 50

    #RRules = ["c1a5f2fb2063454b8f6d44355ba76d76","33f21b4385af4836a69af45772be5398"]
    chunks = [RRules[i:i + chunksize] for i in range(0, len(RRules), chunksize)]
    resDict = {}
    for chunk in chunks:
        resDict = {**resDict, **run_query(chunk)}

    return JsonResponse(resDict)
