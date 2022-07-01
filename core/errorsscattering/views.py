"""
    Created on 06.06.2018
    :author Tatiana Korchuganova
    A set of views showed errors scattering matrix on different levels of grouping
"""

import random, json, math
from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render_to_response, redirect
from django.db import connection
from django.utils.cache import patch_response_headers
from core.libs.cache import getCacheEntry, setCacheEntry, setCacheData
from core.libs.exlib import dictfetchall, get_tmp_table_name, create_temporary_table
from core.libs.DateEncoder import DateEncoder
from core.oauth.utils import login_customrequired
from core.views import initRequest, setupView
from core.common.models import JediTasksOrdered
from core.schedresource.utils import get_pq_clouds
from core.settings.config import DEPLOYMENT, DB_SCHEMA_PANDA, DB_SCHEMA_PANDA_ARCH, DB_SCHEMA_PANDA_META


@login_customrequired
def tasksErrorsScattering(request):
    initRequest(request)
    limit = 100000
    hours = 4
    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='task', wildCardExt=True)
    query['tasktype'] = 'prod'
    query['superstatus__in'] = ['submitting', 'running']
    tasks = JediTasksOrdered.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values("jeditaskid")

    random.seed()
    tmpTableName = get_tmp_table_name()

    transactionKey = random.randrange(1000000)
    executionData = []
    for id in tasks:
        executionData.append((id['jeditaskid'], transactionKey))

    new_cur = connection.cursor()
    query = """INSERT INTO """ + tmpTableName + """(ID,TRANSACTIONKEY) VALUES (%s, %s)"""
    new_cur.executemany(query, executionData)
    connection.commit()

    query = """
        select sum(failedc) / sum(allc) as fperc, computingsite, jeditaskid, sum(failedc) as failedc from (

            select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, sum(1) as allc, computingsite, jeditaskid 
                from {}.jobsarchived4 
                where jeditaskid in (select id from {} where transactionkey={}) 
                group by computingsite, jeditaskid
            union
            select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, sum(1) as allc, computingsite, jeditaskid 
                from {}.jobsarchived 
                where jeditaskid in (select id from {} where transactionkey={}) 
                group by computingsite, jeditaskid
        ) group by computingsite, jeditaskid
    """.format(DB_SCHEMA_PANDA, tmpTableName, transactionKey, DB_SCHEMA_PANDA_ARCH, tmpTableName, transactionKey)

    new_cur.execute(query)

    errorsRaw = dictfetchall(new_cur)
    new_cur.execute("delete from %s where transactionkey=%i" % (tmpTableName, transactionKey))

    computingSites = []
    taskserrors = {}

    # we fill here the dict
    for errorEntry in errorsRaw:
        jeditaskid = errorEntry['JEDITASKID']
        if jeditaskid not in taskserrors:
            taskentry = {}
            taskserrors[jeditaskid] = taskentry
        labelForLink = (str(int(errorEntry['FPERC'] * 100)) + "%" + " ("+str(int(errorEntry['FAILEDC']))+")") if errorEntry['FPERC'] else " "
        taskserrors[jeditaskid][errorEntry['COMPUTINGSITE']] = labelForLink

    tasksToDel = []

    # make cleanup of full none erroneous tasks
    for jeditaskid,taskentry  in taskserrors.items():
        notNone = False
        for sitename, siteval in taskentry.items():
            if siteval != " ":
                notNone = True
        if not notNone:
            tasksToDel.append(jeditaskid)

    for taskToDel in tasksToDel:
        del taskserrors[taskToDel]

    for jeditaskid,taskentry in taskserrors.items():
        for sitename, siteval in taskentry.items():
            computingSites.append(sitename)

    computingSites = set(computingSites)

    for jeditaskid,taskentry  in taskserrors.items():
        for computingSite in computingSites:
            if not computingSite in taskentry:
                taskentry[computingSite] = ' '

    data = {
        'request': request,
        'computingSites': computingSites,
        'taskserrors':taskserrors,
    }

    response = render_to_response('tasksscatteringmatrix.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@login_customrequired
def errorsScattering(request):
    initRequest(request)

    # Here we try to get cached data
    data = getCacheEntry(request, "errorsScattering")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('errorsScattering.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    limit = 100000
    if 'hours' in request.session['requestParams']:
        try:
            hours = int(request.session['requestParams']['hours'])
        except:
            hours = 8
    else:
        hours = 8

    isExcludeScouts = False
    if 'scouts' in request.session['requestParams']:
        if request.session['requestParams']['scouts'] == 'exclude':
            isExcludeScouts = True
        try:
            del request.session['requestParams']['scouts']
        except:
            pass

    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='task', wildCardExt=True)
    query['tasktype'] = 'prod'
    query['superstatus__in'] = ['submitting', 'running']
    # exclude paused tasks
    wildCardExtension += ' and status != \'paused\''
    tasks = JediTasksOrdered.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values("jeditaskid", "reqid")

    # print ('tasks found %i') % len(tasks)

    random.seed()
    taskListByReq = {}
    transactionKey = random.randrange(1000000)
    executionData = []
    for id in tasks:
        executionData.append((id['jeditaskid'], transactionKey))
        # full the list of jeditaskids for each reqid to put into cache for consistentcy with jobList
        if id['reqid'] not in taskListByReq:
            taskListByReq[id['reqid']] = ''
        taskListByReq[id['reqid']] += str(id['jeditaskid']) + ','

    new_cur = connection.cursor()
    tmpTableName = get_tmp_table_name()    
    if DEPLOYMENT == "POSTGRES":
        create_temporary_table(new_cur, tmpTableName)
    ins_query = """insert into """ + tmpTableName + """(id,transactionkey) values (%s, %s)"""
    new_cur.executemany(ins_query, executionData)
    connection.commit()

    jcondition = '(1=1)'
    if isExcludeScouts:
        jcondition = """specialhandling not like '%%sj'"""

    querystr = """
        select j.finishedc, j.reqid, j.failedc, sc.cloud as cloud, j.jeditaskid, j.computingsite from (
            select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, 
                   sum(case when jobstatus = 'finished' then 1 else 0 end) as finishedc, 
                   sum(case when jobstatus in ('finished', 'failed') then 1 else 0 end) as allc, 
                   computingsite, reqid, jeditaskid 
              from {}.jobsarchived4 where jeditaskid != reqid and jeditaskid in (
                select id from {} where transactionkey={}) and modificationtime > to_date('{}', 'YYYY-MM-DD HH24:MI:SS') and {}
                    group by computingsite, reqid, jeditaskid
            union
            select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, 
                   sum(case when jobstatus = 'finished' then 1 else 0 end) as finishedc, 
                   sum(case when jobstatus in ('finished', 'failed') then 1 else 0 end) as allc, 
                   computingsite, reqid, jeditaskid 
              from {}.jobsarchived 
              where jeditaskid != reqid and jeditaskid in (
                  select id from {} where transactionkey={}) and modificationtime > to_date('{}', 'YYYY-MM-DD HH24:MI:SS') and {}
                    group by computingsite, reqid, jeditaskid
        ) j,
        ( select siteid, cloud from {}.schedconfig  
        ) sc
        where j.computingsite = sc.siteid and j.allc > 0    
    """.format(DB_SCHEMA_PANDA, tmpTableName, transactionKey, query['modificationtime__castdate__range'][0], jcondition,
               DB_SCHEMA_PANDA_ARCH, tmpTableName, transactionKey, query['modificationtime__castdate__range'][0], jcondition,
               DB_SCHEMA_PANDA_META)

    new_cur.execute(querystr)

    errorsRaw = dictfetchall(new_cur)
    # new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))

    pq_clouds = get_pq_clouds()
    clouds = sorted(list(set(pq_clouds.values())))
    reqerrors = {}
    clouderrors = {}
    successrateIntervals = {'green': [80, 100], 'yellow':[50,79], 'red':[0, 49]}

    # we fill here the dict
    for errorEntry in errorsRaw:
        rid = errorEntry['REQID']
        if rid not in reqerrors:
            reqentry = {}
            reqerrors[rid] = reqentry
            reqerrors[rid]['reqid'] = rid
            reqerrors[rid]['totalstats'] = {}
            reqerrors[rid]['totalstats']['percent'] = 0
            reqerrors[rid]['totalstats']['minpercent'] = 100
            reqerrors[rid]['totalstats']['finishedc'] = 0
            reqerrors[rid]['totalstats']['failedc'] = 0
            reqerrors[rid]['totalstats']['allc'] = 0
            reqerrors[rid]['totalstats']['greenc'] = 0
            reqerrors[rid]['totalstats']['yellowc'] = 0
            reqerrors[rid]['totalstats']['redc'] = 0
            reqerrors[rid]['tasks'] = {}
            for cloudname in clouds:
                reqerrors[rid][cloudname] = {}
                reqerrors[rid][cloudname]['percent'] = 0
                reqerrors[rid][cloudname]['finishedc'] = 0
                reqerrors[rid][cloudname]['failedc'] = 0
                reqerrors[rid][cloudname]['allc'] = 0
        if errorEntry['JEDITASKID'] not in reqerrors[rid]['tasks']:
            reqerrors[rid]['tasks'][errorEntry['JEDITASKID']] = {}
            reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['finishedc'] = 0
            reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['allc'] = 0
        reqerrors[rid][errorEntry['CLOUD']]['finishedc'] += errorEntry['FINISHEDC']
        reqerrors[rid][errorEntry['CLOUD']]['failedc'] += errorEntry['FAILEDC']
        reqerrors[rid][errorEntry['CLOUD']]['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

        reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['finishedc'] += errorEntry['FINISHEDC']
        reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

        reqerrors[rid]['totalstats']['finishedc'] += reqerrors[rid][errorEntry['CLOUD']]['finishedc']
        reqerrors[rid]['totalstats']['failedc'] += reqerrors[rid][errorEntry['CLOUD']]['failedc']
        reqerrors[rid]['totalstats']['allc'] += reqerrors[rid][errorEntry['CLOUD']]['allc']

        if errorEntry['CLOUD'] not in clouderrors:
            clouderrors[errorEntry['CLOUD']] = {}
        if errorEntry['COMPUTINGSITE'] not in clouderrors[errorEntry['CLOUD']]:
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']] = {}
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['finishedc'] = 0
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['failedc'] = 0
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['allc'] = 0
        clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['finishedc'] += errorEntry['FINISHEDC']
        clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['failedc'] += errorEntry['FAILEDC']
        clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['allc'] += (errorEntry['FINISHEDC'] + errorEntry['FAILEDC'])

    for rid, reqentry in reqerrors.items():
        reqerrors[rid]['totalstats']['percent'] = int(math.ceil(reqerrors[rid]['totalstats']['finishedc']*100./reqerrors[rid]['totalstats']['allc'])) if reqerrors[rid]['totalstats']['allc'] > 0 else 0
        reqerrors[rid]['totalstats']['minpercent'] = min(int(tstats['finishedc'] * 100. / tstats['allc']) for tstats in reqentry['tasks'].values())
        for tstats in reqentry['tasks'].values():
            srpct = int(tstats['finishedc'] * 100. / tstats['allc'])
            for color, srint in successrateIntervals.items():
                reqerrors[rid]['totalstats'][color + 'c'] += 1 if (srpct >= srint[0] and srpct <= srint[1]) else 0
        for cloudname, stats in reqentry.items():
            if cloudname not in ('reqid', 'totalstats', 'tasks'):
                reqerrors[rid][cloudname]['percent'] = int(stats['finishedc'] * 100. / stats['allc']) if stats['allc'] > 0 else -1

    reqsToDel = []

    #make cleanup of full none erroneous requests
    for rid, reqentry in reqerrors.items():
        notNone = False
        if reqentry['totalstats']['allc'] != 0 and reqentry['totalstats']['allc'] != reqentry['totalstats']['finishedc']:
            notNone = True
        # for cname, cval in reqentry.items():
        #     if cval['allc'] != 0:
        #         notNone = True
        if not notNone:
            reqsToDel.append(rid)

    for reqToDel in reqsToDel:
        del reqerrors[reqToDel]

    ### calculate stats for clouds
    columnstats = {}
    for cn in clouds:
        cns = str(cn)
        columnstats[cns] = {}
        columnstats[cns]['percent'] = 0
        columnstats[cns]['finishedc'] = 0
        columnstats[cns]['failedc'] = 0
        columnstats[cns]['allc'] = 0
        columnstats[cns]['minpercent'] = 100
        for color, srint in successrateIntervals.items():
            columnstats[cns][color + 'c'] = 0

    for cloudname, sites in clouderrors.items():
        for sitename, sstats in sites.items():
            columnstats[cloudname]['finishedc'] += sstats['finishedc']
            columnstats[cloudname]['failedc'] += sstats['failedc']
            columnstats[cloudname]['allc'] += sstats['allc']
            srpct = int(sstats['finishedc'] * 100. / sstats['allc'])
            for color, srint in successrateIntervals.items():
                columnstats[cloudname][color + 'c'] += 1 if (srpct >= srint[0] and srpct <= srint[1]) else 0
        columnstats[cloudname]['minpercent'] = min(int(cstats['finishedc'] * 100. / cstats['allc']) for cstats in sites.values())
    for cn, stats in columnstats.items():
        columnstats[cn]['percent'] = int(math.ceil(columnstats[cn]['finishedc']*100./columnstats[cn]['allc'])) if columnstats[cn]['allc'] > 0 else 0


    ### Introducing unique tk for each reqid
    for rid, reqentry in reqerrors.items():
        if rid in taskListByReq and len(taskListByReq[rid]) > 0:
            tk = setCacheData(request, lifetime=60*20, jeditaskid=taskListByReq[rid][:-1])
            reqentry['tk'] = tk

    ### transform requesterrors dict to list for sorting on template
    reqErrorsList = []
    for rid, reqEntry in reqerrors.items():
        reqErrorsList.append(reqEntry)
    reqErrorsList = sorted(reqErrorsList, key=lambda x: x['totalstats']['percent'])

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'clouds' : clouds,
        'columnstats': columnstats,
        'reqerrors': reqErrorsList,
        'scouts': 'exclude' if isExcludeScouts else 'include',
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    setCacheEntry(request, "errorsScattering", json.dumps(data, cls=DateEncoder), 60 * 20)
    response = render_to_response('errorsScattering.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response


@login_customrequired
def errorsScatteringDetailed(request, cloud, reqid):
    valid, response = initRequest(request)
    if not valid: return response

    # Here we try to get cached data
    data = getCacheEntry(request, "errorsScatteringDetailed")
    if data is not None:
        data = json.loads(data)
        data['request'] = request
        response = render_to_response('errorsScatteringDetailed.html', data, content_type='text/html')
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response

    grouping = []

    cloudsDict ={}
    pq_clouds = get_pq_clouds()
    clouds = sorted(list(set(pq_clouds.values())))
    for pq, cloud_name in pq_clouds.items():
        if cloud_name not in cloudsDict:
            cloudsDict[cloud_name] = []
        cloudsDict[cloud_name].append(pq)

    sitesDictForOrdering = {}
    i = 0
    for cloudname in sorted(cloudsDict.keys()):
        for sitename in sorted(cloudsDict[cloudname]):
            sitesDictForOrdering[sitename] = i
            i += 1

    condition = '(1=1)'
    if cloud == '' or len(cloud)==0:
        return HttpResponse("No cloud supplied", content_type='text/html')
    elif cloud == 'ALL':
        grouping.append('reqid')
    elif cloud not in clouds:
        return HttpResponse("The provided cloud name does not exist", content_type='text/html')

    if reqid == '' or len(reqid)==0:
        return HttpResponse("No request ID supplied", content_type='text/html')
    elif reqid == 'ALL':
        grouping.append('cloud')
    else:
        try:
            reqid = int(reqid)
        except:
            return HttpResponse("The provided request ID is not valid", content_type='text/html')
    view = None
    if 'view' in request.session['requestParams'] and request.session['requestParams']['view'] == 'queues':
        view = 'queues'

    if len(grouping) == 2 and view != 'queues':
        return redirect('/errorsscat/')

    limit = 100000
    if 'hours' in request.session['requestParams']:
        try:
            hours = int(request.session['requestParams']['hours'])
        except:
            hours = 8
    else:
        hours = 8

    isExcludeScouts = False
    if 'scouts' in request.session['requestParams']:
        if request.session['requestParams']['scouts'] == 'exclude':
            isExcludeScouts = True
        try:
            del request.session['requestParams']['scouts']
        except:
            pass


    query, wildCardExtension, LAST_N_HOURS_MAX = setupView(request, hours=hours, limit=9999999, querytype='task', wildCardExt=True)
    query['tasktype'] = 'prod'
    query['superstatus__in'] = ['submitting', 'running']
    # exclude paused tasks
    wildCardExtension += ' and status != \'paused\''
    if reqid != 'ALL':
        query['reqid'] = reqid
        request.session['requestParams']['reqid'] = reqid
    if cloud != 'ALL':
        request.session['requestParams']['region'] = cloud
        cloudstr = ''
        for sn, cn in pq_clouds.items():
            if cn == cloud:
                cloudstr += "\'%s\'," % (str(sn))
        if cloudstr.endswith(','):
            cloudstr = cloudstr[:-1]
        condition = "computingsite in ( %s )" % (str(cloudstr))


    tasks = JediTasksOrdered.objects.filter(**query).extra(where=[wildCardExtension])[:limit].values("jeditaskid", "reqid")

    print ('tasks found %i' % (len(tasks)))

    random.seed()
    tmpTableName = get_tmp_table_name()

    taskListByReq = {}
    transactionKey = random.randrange(1000000)
    executionData = []
    for id in tasks:
        executionData.append((id['jeditaskid'], transactionKey))
        # full the list of jeditaskids for each reqid to put into cache for consistentcy with jobList
        if id['reqid'] not in taskListByReq:
            taskListByReq[id['reqid']] = ''
        taskListByReq[id['reqid']] += str(id['jeditaskid']) + ','

    new_cur = connection.cursor()
    insquery = """insert into """ + tmpTableName + """(id,transactionkey) values (%s, %s)"""
    new_cur.executemany(insquery, executionData)
    connection.commit()

    jcondition = '(1=1)'
    if isExcludeScouts:
        jcondition = """specialhandling NOT LIKE '%%sj'"""

    querystr = """
            select sum(finishedc) as finishedc, 
                   sum(failedc) as failedc,
                   sum(allc) as allc,  
                   reqid, jeditaskid, computingsite, sc.cloud as cloud from (
                        select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, 
                               sum(case when jobstatus = 'finished' then 1 else 0 end) as finishedc, 
                               sum(case when jobstatus in ('finished', 'failed') then 1 else 0 end) as allc,  
                               computingsite, reqid, jeditaskid 
                        from atlas_panda.jobsarchived4 where jeditaskid in (
                            select id from %s where transactionkey=%i) and modificationtime > to_date('%s', 'YYYY-MM-DD HH24:MI:SS') and %s
                                group by computingsite, jeditaskid, reqid
                        union
                        select sum(case when jobstatus = 'failed' then 1 else 0 end) as failedc, 
                               sum(case when jobstatus = 'finished' then 1 else 0 end) as finishedc,  
                               sum(case when jobstatus in ('finished', 'failed') then 1 else 0 end) as allc,
                               computingsite, reqid, jeditaskid 
                        from atlas_pandaarch.jobsarchived where jeditaskid in (
                              select id from %s where transactionkey=%i) and modificationtime > to_date('%s', 'YYYY-MM-DD HH24:MI:SS') and %s
                                group by computingsite, jeditaskid, reqid
            ) j,
            ( select siteid, cloud from atlas_pandameta.schedconfig  
            ) sc
            where j.computingsite = sc.siteid and j.allc > 0  and %s
            group by jeditaskid, computingsite, reqid, cloud
    """ % (tmpTableName, transactionKey, query['modificationtime__castdate__range'][0], jcondition, tmpTableName, transactionKey, query['modificationtime__castdate__range'][0], jcondition, condition)

    new_cur.execute(querystr)

    errorsRaw = dictfetchall(new_cur)
    # new_cur.execute("DELETE FROM %s WHERE TRANSACTIONKEY=%i" % (tmpTableName, transactionKey))

    computingSites = []
    tasksErrorsList = []
    taskserrors = {}
    reqErrorsList = []
    reqerrors = {}

    successrateIntervals = {'green': [80, 100], 'yellow':[50,79], 'red':[0, 49]}

    statsParams = ['percent', 'finishedc', 'failedc', 'allc']

    if len(grouping) == 0 or (len(grouping) == 1 and 'reqid' in grouping and view == 'queues'):

        # we fill here the dict
        for errorEntry in errorsRaw:
            jeditaskid = errorEntry['JEDITASKID']
            if jeditaskid not in taskserrors:
                taskentry = {}
                taskserrors[jeditaskid] = taskentry
                taskserrors[jeditaskid]['jeditaskid'] = jeditaskid
                taskserrors[jeditaskid]['columns'] = {}
                taskserrors[jeditaskid]['totalstats'] = {}
                for param in statsParams:
                    taskserrors[jeditaskid]['totalstats'][param] = 0
            if errorEntry['COMPUTINGSITE'] not in taskserrors[jeditaskid]['columns']:
                taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']] = {}
                for param in statsParams:
                    taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']][param] = 0
            taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['allc'] = errorEntry['FINISHEDC'] + errorEntry['FAILEDC']
            taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['percent'] = int(math.ceil(
                errorEntry['FINISHEDC'] * 100. / taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['allc'])) if \
                    taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['allc'] > 0 else 0
            taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['finishedc'] = errorEntry['FINISHEDC']
            taskserrors[jeditaskid]['columns'][errorEntry['COMPUTINGSITE']]['failedc'] = errorEntry['FAILEDC']
            taskserrors[jeditaskid]['totalstats']['finishedc'] += errorEntry['FINISHEDC']
            taskserrors[jeditaskid]['totalstats']['failedc'] += errorEntry['FAILEDC']
            taskserrors[jeditaskid]['totalstats']['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

        ### calculate totalstats
        for jeditaskid, taskEntry in taskserrors.items():
            taskserrors[jeditaskid]['totalstats']['percent'] = int(math.ceil(
                taskEntry['totalstats']['finishedc']*100./taskEntry['totalstats']['allc'])) if taskEntry['totalstats']['allc'] > 0 else 0

        tasksToDel = []

        # make cleanup of full none erroneous tasks
        for jeditaskid, taskEntry in taskserrors.items():
            notNone = False
            if taskEntry['totalstats']['allc'] == 0:
                notNone = True
            if notNone:
                tasksToDel.append(jeditaskid)

        for taskToDel in tasksToDel:
            del taskserrors[taskToDel]

        for jeditaskid, taskentry in taskserrors.items():
            for sitename, siteval in taskentry['columns'].items():
                computingSites.append(sitename)

        computingSites = sorted(set(computingSites), key=lambda x: sitesDictForOrdering.get(x))

        ### fill
        for jeditaskid, taskentry in taskserrors.items():
            for computingSite in computingSites:
                if computingSite not in taskentry['columns']:
                    taskserrors[jeditaskid]['columns'][computingSite] = {}
                    for param in statsParams:
                        taskserrors[jeditaskid]['columns'][computingSite][param] = 0

        ### calculate stats for column
        columnstats = {}
        for cn in computingSites:
            cns = str(cn)
            columnstats[cns] = {}
            for param in statsParams:
                columnstats[cns][param] = 0
        for jeditaskid, taskEntry in taskserrors.items():
            for cs in computingSites:
                for cname, cEntry in taskEntry['columns'].items():
                    if cs == cname:
                        columnstats[cs]['finishedc'] += cEntry['finishedc']
                        columnstats[cs]['failedc'] += cEntry['failedc']
                        columnstats[cs]['allc'] += cEntry['allc']
        for csn, stats in columnstats.items():
            columnstats[csn]['percent'] = int(
                math.ceil(columnstats[csn]['finishedc'] * 100. / columnstats[csn]['allc'])) if \
                    columnstats[csn]['allc'] > 0 else 0


        ### transform requesterrors dict to list for sorting on template
        for jeditaskid, taskEntry in taskserrors.items():
            columnlist = []
            for columnname, stats in taskEntry['columns'].items():
                stats['computingsite'] = columnname
                columnlist.append(stats)
            taskEntry['columns'] = sorted(columnlist, key=lambda x: sitesDictForOrdering.get(x['computingsite']))

        for jeditaskid, taskEntry in taskserrors.items():
            tasksErrorsList.append(taskEntry)

        tasksErrorsList = sorted(tasksErrorsList, key=lambda x: x['totalstats']['percent'])

    elif len(grouping) == 1 and 'reqid' in grouping:

        clouderrors = {}
        # we fill here the dict
        for errorEntry in errorsRaw:
            jeditaskid = errorEntry['JEDITASKID']
            if jeditaskid not in taskserrors:
                taskentry = {}
                taskserrors[jeditaskid] = taskentry
                taskserrors[jeditaskid]['jeditaskid'] = jeditaskid
                taskserrors[jeditaskid]['columns'] = {}
                taskserrors[jeditaskid]['totalstats'] = {}
                for param in statsParams:
                    taskserrors[jeditaskid]['totalstats'][param] = 0
            if errorEntry['CLOUD'] not in taskserrors[jeditaskid]['columns']:
                taskserrors[jeditaskid]['columns'][errorEntry['CLOUD']] = {}
                for param in statsParams:
                    taskserrors[jeditaskid]['columns'][errorEntry['CLOUD']][param] = 0
            taskserrors[jeditaskid]['columns'][errorEntry['CLOUD']]['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']
            taskserrors[jeditaskid]['columns'][errorEntry['CLOUD']]['finishedc'] += errorEntry['FINISHEDC']
            taskserrors[jeditaskid]['columns'][errorEntry['CLOUD']]['failedc'] += errorEntry['FAILEDC']
            taskserrors[jeditaskid]['totalstats']['finishedc'] += errorEntry['FINISHEDC']
            taskserrors[jeditaskid]['totalstats']['failedc'] += errorEntry['FAILEDC']
            taskserrors[jeditaskid]['totalstats']['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

            if errorEntry['CLOUD'] not in clouderrors:
                clouderrors[errorEntry['CLOUD']] = {}
            if errorEntry['COMPUTINGSITE'] not in clouderrors[errorEntry['CLOUD']]:
                clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']] = {}
                clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['finishedc'] = 0
                clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['failedc'] = 0
                clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['allc'] = 0
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['finishedc'] += errorEntry['FINISHEDC']
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['failedc'] += errorEntry['FAILEDC']
            clouderrors[errorEntry['CLOUD']][errorEntry['COMPUTINGSITE']]['allc'] += (errorEntry['FINISHEDC'] + errorEntry['FAILEDC'])

        ### calculate totalstats
        for jeditaskid, taskEntry in taskserrors.items():
            taskserrors[jeditaskid]['totalstats']['percent'] = int(
                math.ceil(taskEntry['totalstats']['finishedc'] * 100. / taskEntry['totalstats']['allc'])) if \
                    taskEntry['totalstats']['allc'] > 0 else 0

        tasksToDel = []

        #make cleanup of full none erroneous tasks
        for jeditaskid, taskEntry  in taskserrors.items():
            notNone = False
            if taskEntry['totalstats']['allc'] == 0:
                notNone = True
            if notNone:
                tasksToDel.append(jeditaskid)

        for taskToDel in tasksToDel:
            del taskserrors[taskToDel]


        for jeditaskid, taskentry in taskserrors.items():
            for c in clouds:
                if not c in taskentry['columns']:
                    taskentry['columns'][c] = {}
                    for param in statsParams:
                        taskentry['columns'][c][param] = 0
                else:
                    taskentry['columns'][c]['percent'] = int(math.ceil(taskentry['columns'][c]['finishedc']*100./taskentry['columns'][c]['allc'])) if \
                        taskentry['columns'][c]['allc'] > 0 else 0

        ### calculate stats for columns
        columnstats = {}
        for cn in clouds:
            cns = str(cn)
            columnstats[cns] = {}
            for param in statsParams:
                columnstats[cns][param] = 0

            columnstats[cns]['minpercent'] = 100
            for color, srint in successrateIntervals.items():
                columnstats[cns][color + 'c'] = 0

        for cloudname, sites in clouderrors.items():
            for sitename, sstats in sites.items():
                columnstats[cloudname]['finishedc'] += sstats['finishedc']
                columnstats[cloudname]['failedc'] += sstats['failedc']
                columnstats[cloudname]['allc'] += sstats['allc']
                srpct = int(sstats['finishedc'] * 100. / sstats['allc'])
                for color, srint in successrateIntervals.items():
                    columnstats[cloudname][color + 'c'] += 1 if (srpct >= srint[0] and srpct <= srint[1]) else 0
            columnstats[cloudname]['minpercent'] = min(
                int(cstats['finishedc'] * 100. / cstats['allc']) for cstats in sites.values())

        for cn, stats in columnstats.items():
            columnstats[cn]['percent'] = int(
                math.ceil(columnstats[cn]['finishedc'] * 100. / columnstats[cn]['allc'])) if \
                    columnstats[cn]['allc'] > 0 else 0

        ### transform requesterrors dict to list for sorting on template
        for jeditaskid, taskEntry in taskserrors.items():
            tasksErrorsList.append(taskEntry)

        tasksErrorsList = sorted(tasksErrorsList, key=lambda x: x['totalstats']['percent'])

    elif 'cloud' in grouping or view == 'queues':

        print ('%s starting data aggregation' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        # we fill here the dict
        for errorEntry in errorsRaw:
            rid = errorEntry['REQID']
            if rid not in reqerrors:
                reqentry = {}
                reqerrors[rid] = reqentry
                reqerrors[rid]['columns'] = {}
                reqerrors[rid]['reqid'] = rid
                reqerrors[rid]['totalstats'] = {}
                reqerrors[rid]['totalstats']['greenc'] = 0
                reqerrors[rid]['totalstats']['yellowc'] = 0
                reqerrors[rid]['totalstats']['redc'] = 0
                reqerrors[rid]['tasks'] = {}
                for param in statsParams:
                    reqerrors[rid]['totalstats'][param] = 0
            if errorEntry['COMPUTINGSITE'] not in reqerrors[rid]['columns']:
                reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']] = {}
                for param in statsParams:
                    reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']][param] = 0
            if errorEntry['JEDITASKID'] not in reqerrors[rid]['tasks']:
                reqerrors[rid]['tasks'][errorEntry['JEDITASKID']] = {}
                reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['finishedc'] = 0
                reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['allc'] = 0
            reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['finishedc'] += errorEntry['FINISHEDC']
            reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['failedc'] += errorEntry['FAILEDC']
            reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

            reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['finishedc'] += errorEntry['FINISHEDC']
            reqerrors[rid]['tasks'][errorEntry['JEDITASKID']]['allc'] += errorEntry['FINISHEDC'] + errorEntry['FAILEDC']

            reqerrors[rid]['totalstats']['finishedc'] += reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['finishedc']
            reqerrors[rid]['totalstats']['failedc'] += reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['failedc']
            reqerrors[rid]['totalstats']['allc'] += reqerrors[rid]['columns'][errorEntry['COMPUTINGSITE']]['allc']

        for rid, reqentry in reqerrors.items():
            reqerrors[rid]['totalstats']['percent'] = int(
                math.ceil(reqerrors[rid]['totalstats']['finishedc'] * 100. / reqerrors[rid]['totalstats']['allc'])) if \
                    reqerrors[rid]['totalstats']['allc'] > 0 else 0
            reqerrors[rid]['totalstats']['minpercent'] = min(
                int(tstats['finishedc'] * 100. / tstats['allc']) for tstats in reqentry['tasks'].values())
            for tstats in reqentry['tasks'].values():
                srpct = int(tstats['finishedc'] * 100. / tstats['allc'])
                for color, srint in successrateIntervals.items():
                    reqerrors[rid]['totalstats'][color + 'c'] += 1 if (srpct >= srint[0] and srpct <= srint[1]) else 0


        print ('%s starting cleaning of non-errorneous requests' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        reqsToDel = []

        #make cleanup of full none erroneous tasks
        for rid, reqentry in reqerrors.items():
            notNone = False
            if reqentry['totalstats']['allc'] == 0:
                notNone = True
            if notNone:
                reqsToDel.append(rid)

        for reqToDel in reqsToDel:
            del reqerrors[reqToDel]

        print ('%s starting calculation of row average stats' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        for rid, reqentry in reqerrors.items():
            for sn, sv in reqentry['columns'].items():
                computingSites.append(str(sn))

        computingSites = sorted(set(computingSites), key=lambda x: sitesDictForOrdering.get(x))

        for rid, reqentry  in reqerrors.items():
            for s in computingSites:
                if not s in reqentry['columns']:
                    reqentry['columns'][s] = {}
                    for param in statsParams:
                        reqentry['columns'][s][param] = 0
                else:
                    reqentry['columns'][s]['percent'] = int(math.ceil(reqentry['columns'][s]['finishedc']*100./reqentry['columns'][s]['allc'])) if \
                        reqentry['columns'][s]['allc'] > 0 else 0

        print ('%s starting calculation of columns average stats' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        ### calculate stats for columns
        columnstats = {}
        for cn in computingSites:
            cns = str(cn)
            columnstats[cns] = {}
            for param in statsParams:
                columnstats[cns][param] = 0
        for rid, reqEntry in reqerrors.items():
            for cn in computingSites:
                for cname, cEntry in reqEntry['columns'].items():
                    if cn == cname:
                        columnstats[cn]['finishedc'] += cEntry['finishedc']
                        columnstats[cn]['failedc'] += cEntry['failedc']
                        columnstats[cn]['allc'] += cEntry['allc']
        for cn, stats in columnstats.items():
            columnstats[cn]['percent'] = int(
                math.ceil(columnstats[cn]['finishedc'] * 100. / columnstats[cn]['allc'])) if \
                    columnstats[cn]['allc'] > 0 else 0

        print ('%s starting set unique cache for each request' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        ### Introducing unique tk for each reqid
        for rid, reqentry in reqerrors.items():
            if rid in taskListByReq and len(taskListByReq[rid]) > 0:
                tk = setCacheData(request, lifetime=60*20, jeditaskid=taskListByReq[rid][:-1])
                reqentry['tk'] = tk

        print ('%s starting transform dict to list' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        ### transform requesterrors dict to list for sorting on template
        for rid, reqEntry in reqerrors.items():
            columnlist = []
            for columnname, stats in reqEntry['columns'].items():
                stats['computingsite'] = columnname
                columnlist.append(stats)
            reqEntry['columns'] = sorted(columnlist, key=lambda x: sitesDictForOrdering.get(x['computingsite']))
        reqErrorsList = []
        for rid, reqEntry in reqerrors.items():
            reqErrorsList.append(reqEntry)
        reqErrorsList = sorted(reqErrorsList, key=lambda x: x['totalstats']['percent'])

    data = {
        'request': request,
        'viewParams': request.session['viewParams'],
        'requestParams': request.session['requestParams'],
        'cloud': cloud,
        'reqid': reqid,
        'grouping': grouping,
        'view': view,
        'computingSites': computingSites,
        'clouds': clouds,
        'columnstats': columnstats,
        'taskserrors': tasksErrorsList,
        'reqerrors': reqErrorsList,
        'scouts': 'exclude' if isExcludeScouts else 'include',
        'nrows': max(len(tasksErrorsList), len(reqErrorsList)),
        'built': datetime.now().strftime("%H:%M:%S"),
    }
    print ('%s starting rendering of the page' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    setCacheEntry(request, "errorsScatteringDetailed", json.dumps(data, cls=DateEncoder), 60 * 20)
    response = render_to_response('errorsScatteringDetailed.html', data, content_type='text/html')
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
    return response

