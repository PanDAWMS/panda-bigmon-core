from datetime import datetime
import decimal

import numpy as np
import re
from decimal import Decimal

import urllib3
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from django.db import connection

from core.libs.cache import getCacheEntry, setCacheEntry
from core.libs.CustomJSONSerializer import DecimalEncoder
from core.views import initRequest, setupView, login_customrequired, extensibleURL, DateEncoder
from django.core.cache import cache
import json

from core.globalshares import GlobalShares

@login_customrequired
def globalshares(request):
    valid, response = initRequest(request)
    data = getCacheEntry(request, "globalshares")
    if data is not None:
        data = json.loads(data)
        data['request'] = request

    if not valid: return response
    setupView(request, hours=180 * 24, limit=9999999)
    gs, tablerows = __get_hs_leave_distribution()
    gsPlotData = {
        'pieChartActualHS06': [],
        'barChartActualVSTarget': [],
        'barChartActualVSTargetSplit': {'resourceTypeList': [], 'data': {}}
    }

    for shareName, shareValue in gs.items():
        shareValue['delta'] = shareValue['executing'] - shareValue['pledged']
        shareValue['used'] = shareValue['ratio'] if 'ratio' in shareValue else None
        # prepare data for charts
        if int(shareValue['executing']) > 0:
            gsPlotData['pieChartActualHS06'].append([str(shareName), int(shareValue['executing'])])
            gsPlotData['barChartActualVSTarget'].append({
                'GS': str(shareName),
                'Actual': int(shareValue['executing']),
                'Target': int(shareValue['pledged'])
            })
            gsPlotData['barChartActualVSTargetSplit']['data'][str(shareName)] = {
                'Target': int(shareValue['pledged']),
                'Actual': {}
            }

    for shareValue in tablerows:
        shareValue['used'] = shareValue['ratio']*Decimal(shareValue['value'])/100 if 'ratio' in shareValue else None
    ordtablerows ={}
    ordtablerows['childlist']=[]
    level1=''
    level2=''
    level3=''

    for shareValue in tablerows:
        if len(shareValue['level1'])!=0:
            level1 = shareValue['level1']
            ordtablerows[level1] = {}
            ordtablerows['childlist'].append(level1)
            ordtablerows[level1]['childlist'] = []
        if len(shareValue['level2'])!=0:
            level2 = shareValue['level2']
            ordtablerows[level1][level2] = {}
            ordtablerows[level1]['childlist'].append(level2)
            ordtablerows[level1][level2]['childlist'] = []
        if len(shareValue['level3'])!=0:
            level3 = shareValue['level3']
            ordtablerows[level1][level2][level3] = {}
            ordtablerows[level1][level2]['childlist'].append(level3)

    resources_list, resources_dict = get_resources_gshare()

    # add data to bar plot
    for gs, rd in resources_dict.items():
        if gs in gsPlotData['barChartActualVSTargetSplit']['data']:
            for r, values in rd.items():
                if 'executing' in values and values['executing'] > 0:
                    gsPlotData['barChartActualVSTargetSplit']['data'][gs]['Actual'][r] = int(values['executing'])
                    if r not in gsPlotData['barChartActualVSTargetSplit']['resourceTypeList']:
                        gsPlotData['barChartActualVSTargetSplit']['resourceTypeList'].append(r)

    newTablesRow =[]
    for ordValueLevel1 in sorted(ordtablerows['childlist']):
        for shareValue in tablerows:
            if ordValueLevel1 in shareValue['level1']:
                ord1Short = re.sub('\[(.*)\]', '', ordValueLevel1).rstrip().lower()
                shareValue['level'] = 'level1'
                shareValue['gshare'] = ord1Short.replace(' ', '_')
                if len(ordtablerows[ordValueLevel1]['childlist']) == 0:
                    shareValue['link'] = '?gshare={}&display_limit=100'.format(ord1Short)
                newTablesRow.append(shareValue)
                tablerows.remove(shareValue)
                if len(ordtablerows[ordValueLevel1]['childlist']) == 0:
                    add_resources(ord1Short,newTablesRow,resources_list,shareValue['level'])
                else:
                    childsgsharelist = []
                    get_child_elements(ordtablerows[ordValueLevel1],childsgsharelist)
                    resources_dict = get_child_sumstats(childsgsharelist,resources_dict,ord1Short)
                    short_resource_list= resourcesDictToList(resources_dict)
                    add_resources(ord1Short, newTablesRow, short_resource_list, shareValue['level'])
                break
        for ordValueLevel2 in sorted(ordtablerows[ordValueLevel1]['childlist']):
            for shareValue in tablerows:
                if ordValueLevel2 in shareValue['level2']:
                    if len(ordtablerows[ordValueLevel1][ordValueLevel2]['childlist'])==0:
                        ord1Short = re.sub('\[(.*)\]','',ordValueLevel1).rstrip().lower()
                        ord2Short = re.sub('\[(.*)\]', '', ordValueLevel2).rstrip().lower()
                        link = "?jobtype=%s&display_limit=100&gshare=%s"%(ord1Short,ord2Short)
                        shareValue['link'] = link
                        shareValue['level'] = 'level2'
                        shareValue['gshare'] = ord2Short.replace(' ', '_')
                    newTablesRow.append(shareValue)
                    tablerows.remove(shareValue)
                    if 'level' in shareValue:
                        add_resources(ord2Short, newTablesRow, resources_list, shareValue['level'])
                    break
            for ordValueLevel3 in sorted(ordtablerows[ordValueLevel1][ordValueLevel2]['childlist']):
                for shareValue in tablerows:
                    if ordValueLevel3 in shareValue['level3']:
                        if len(ordtablerows[ordValueLevel1][ordValueLevel2]['childlist']) > 0:
                            ord1Short = re.sub('\[(.*)\]', '', ordValueLevel1).rstrip().lower()
                            ord3Short = re.sub('\[(.*)\]', '', ordValueLevel3).rstrip().lower()
                            link = "?jobtype=%s&display_limit=100&gshare=%s" % (ord1Short, ord3Short)
                            shareValue['link'] = link
                            shareValue['level'] = 'level3'
                            shareValue['gshare'] = ord3Short.replace(' ', '_')
                        newTablesRow.append(shareValue)
                        tablerows.remove(shareValue)
                        if 'level' in shareValue:
                            add_resources(ord3Short, newTablesRow, resources_list, shareValue['level'])
                        break

    tablerows = newTablesRow

    gsPlotData['pieChartActualHS06'] = sorted(gsPlotData['pieChartActualHS06'], key=lambda x: x[0])
    gsPlotData['barChartActualVSTarget'] = sorted(gsPlotData['barChartActualVSTarget'], key=lambda x: -x['Actual'])
    gsPlotData['barChartActualVSTargetSplit']['resourceTypeList'] = sorted(gsPlotData['barChartActualVSTargetSplit']['resourceTypeList'])

    del request.session['TFIRST']
    del request.session['TLAST']
    if (not (('HTTP_ACCEPT' in request.META) and (request.META.get('HTTP_ACCEPT') in ('application/json'))) and (
                'json' not in request.session['requestParams'])):
        data = {
            'request': request,
            'viewParams': request.session['viewParams'],
            'requestParams': request.session['requestParams'],
            'globalshares': gs,
            'xurl': extensibleURL(request),
            'gsPlotData':gsPlotData,
            'tablerows':tablerows,
            'built': datetime.now().strftime("%H:%M:%S"),
        }
        response = render_to_response('globalshares.html', data, content_type='text/html')
        setCacheEntry(request, "globalshares", json.dumps(data, cls=DateEncoder), 60 * 20)
        patch_response_headers(response, cache_timeout=request.session['max_age_minutes'] * 60)
        return response
    else:
        return HttpResponse(DecimalEncoder().encode(gs), content_type='application/json')


def get_child_elements(tree,childsgsharelist):
    for gshare in tree:
        if gshare!='childlist':
            shortGshare = re.sub('\[(.*)\]', '', gshare).rstrip()
            if 'childlist' in tree[gshare] and len(tree[gshare]['childlist'])==0:#len(tree[gshare])==0:
                childsgsharelist.append(shortGshare)
            elif 'childlist' not in tree[gshare]:
                childsgsharelist.append(shortGshare)
            else:
                get_child_elements(tree[gshare],childsgsharelist)

def get_child_sumstats(childsgsharelist,resourcesdict,gshare):
    parentgshare = {}
    parentgshare[gshare] = {}
    for child in childsgsharelist:
        if child in resourcesdict:
            for resource in resourcesdict[child]:
                if resource not in parentgshare[gshare]:
                    parentgshare[gshare][resource] = {}
                    for k in resourcesdict[child][resource].keys():
                        parentgshare[gshare][resource][k] = resourcesdict[child][resource][k]
                else:
                    for k in resourcesdict[child][resource].keys():
                            parentgshare[gshare][resource][k] += resourcesdict[child][resource][k]
    return parentgshare

def get_resources_gshare():
    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'
    resourcesDictSites =get_CRIC_resources()
    sqlRequest = """
    SELECT gshare, computingsite, jobstatus_grouped, SUM(HS) 
    FROM (SELECT gshare, computingsite, HS, CASE WHEN jobstatus IN('activated') THEN 'queued' WHEN jobstatus IN('sent', 'running') THEN 'executing'
    ELSE 'ignore' END jobstatus_grouped FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS) GROUP BY gshare,computingsite, jobstatus_grouped order by gshare
    """
    cur = connection.cursor()
    cur.execute(sqlRequest)
    # get the hs distribution data into a dictionary structure
    hs_distribution_raw = cur.fetchall()
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    total_hs = 0
    newresourecurcetype = ''
    resourcecnt = 0
    for hs_entry in hs_distribution_raw:
        gshare, computingsite, status_group, hs = hs_entry
        try:
            resourcetype = resourcesDictSites[computingsite]
        except:
            continue
        hs_distribution_dict.setdefault(gshare,{})
        hs_distribution_dict[gshare].setdefault(resourcetype, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0, IGNORE:0})
        #hs_distribution_dict[gshare][resourcetype][status_group] = hs

        total_hs += hs

        if status_group == QUEUED:
            hs_queued_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs
        else:
            hs_ignore_total += hs
            hs_distribution_dict[gshare][resourcetype][status_group] += hs

    hs_distribution_list=resourcesDictToList(hs_distribution_dict)

    return hs_distribution_list, hs_distribution_dict

def resourcesDictToList(hs_distribution_dict):
    ignore = 0
    pled = 0
    executing = 0
    queued = 0
    total_hs =0
    for gshare in hs_distribution_dict.keys():
        for resource in hs_distribution_dict[gshare].keys():
            sum_hs = 0
            pled += hs_distribution_dict[gshare][resource]['pledged']
            ignore += hs_distribution_dict[gshare][resource]['ignore']
            executing += hs_distribution_dict[gshare][resource]['executing']
            queued += hs_distribution_dict[gshare][resource]['queued']
            sum_hs = float(hs_distribution_dict[gshare][resource]['pledged']) + \
                 float(hs_distribution_dict[gshare][resource]['ignore']) + \
                 float(hs_distribution_dict[gshare][resource]['executing']) + \
                 float(hs_distribution_dict[gshare][resource]['queued'])
            total_hs+=sum_hs
            hs_distribution_dict[gshare][resource]['total_hs'] = sum_hs

    hs_distribution_list = {}
    for gshare in hs_distribution_dict.keys():
        for resource in hs_distribution_dict[gshare].keys():
            hs_distribution_dict[gshare][resource]['ignore_percent'] =  (hs_distribution_dict[gshare][resource]['ignore']/ignore)* 100
            hs_distribution_dict[gshare][resource]['executing_percent'] =  (hs_distribution_dict[gshare][resource]['executing'] /executing) * 100
            hs_distribution_dict[gshare][resource]['queued_percent'] = (hs_distribution_dict[gshare][resource]['queued']/queued) * 100

            hs_distribution_list.setdefault(str(gshare).lower(),[]).append({'resource':resource, 'pledged':hs_distribution_dict[gshare][resource]['pledged'],
                                     'ignore':hs_distribution_dict[gshare][resource]['ignore'],
                                     'ignore_percent':hs_distribution_dict[gshare][resource]['ignore_percent'],
                                     'executing':hs_distribution_dict[gshare][resource]['executing'],
                                     'executing_percent': hs_distribution_dict[gshare][resource]['executing_percent'],
                                     'queued':hs_distribution_dict[gshare][resource]['queued'],
                                     'queued_percent':hs_distribution_dict[gshare][resource]['queued_percent'],
                                     'total_hs':hs_distribution_dict[gshare][resource]['total_hs'],
                                     'total_hs_percent': (hs_distribution_dict[gshare][resource]['total_hs']/total_hs)*100
                                     })
    return hs_distribution_list

def add_resources(gshare,tableRows,resourceslist,level):
    gshare = str(gshare).replace('_', ' ')
    if gshare in resourceslist:
        resourcesForGshare = resourceslist[gshare]
        resourcesForGshareList = []
        if level == 'level1':
            for resource in resourcesForGshare:
                resource['level1'] = resource['resource']
                resource['level2'] = ''
                resource['level3'] = ''
        if level == 'level2':
            for resource in resourcesForGshare:
                resource['level1'] = ''
                resource['level2'] = resource['resource']
                resource['level3'] = ''
        if level == 'level3':
            for resource in resourcesForGshare:
                resource['level1'] = ''
                resource['level2'] = ''
                resource['level3'] = resource['resource']

        for row in tableRows:
            if 'gshare' in row and gshare.replace(' ', '_') == row['gshare']:
                row['resources'] = resourcesForGshare

def get_shares(parents=''):
    comment = ' /* DBProxy.get_shares */'
    methodName = comment.split(' ')[-2].split('.')[-1]

    sql = """
           SELECT NAME, VALUE, PARENT, PRODSOURCELABEL, WORKINGGROUP, CAMPAIGN, PROCESSINGTYPE
           FROM ATLAS_PANDA.GLOBAL_SHARES
           """
    var_map = None

    if parents == '':
        # Get all shares
        pass
    elif parents is None:
        # Get top level shares
        sql += "WHERE parent IS NULL"

    elif type(parents) == np.unicode:
        # Get the children of a specific share
        var_map = {':parent': parents}
        sql += "WHERE parent = :parent"

    elif type(parents) in (list, tuple):
        # Get the children of a list of shares
        i = 0
        var_map = {}
        for parent in parents:
            key = ':parent{0}'.format(i)
            var_map[key] = parent
            i += 1

        parentBindings = ','.join(':parent{0}'.format(i) for i in xrange(len(parents)))
        sql += "WHERE parent IN ({0})".format(parentBindings)

    cur = connection.cursor()
    cur.execute(sql, var_map)
    resList = cur.fetchall()
    cur.close()

    return resList

def __load_branch(share):
    """
    Recursively load a branch
    """
    node = GlobalShares.Share(share.name, share.value, share.parent, share.prodsourcelabel,
                              share.workinggroup, share.campaign, share.processingtype)

    children = get_shares(parents=share.name)
    if not children:
        return node

    for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in children:
        child = GlobalShares.Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
        node.children.append(__load_branch(child))

    return node

def __get_hs_leave_distribution():
    """
    Get the current HS06 distribution for running and queued jobs
    """

    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'

    comment = ' /* DBProxy.get_hs_leave_distribution */'

    tree = GlobalShares.Share('root', 100, None, None, None, None, None)
    shares_top_level = get_shares(parents=None)
    for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in shares_top_level:
        share = GlobalShares.Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
        tree.children.append(__load_branch(share))

    tree.normalize()
    leave_shares = tree.get_leaves()

    sql_hs_distribution = "SELECT gshare, jobstatus_grouped, SUM(HS) FROM (SELECT gshare, HS, CASE WHEN jobstatus IN('activated') THEN 'queued' WHEN jobstatus IN('sent', 'running') THEN 'executing' ELSE 'ignore' END jobstatus_grouped FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS) GROUP BY gshare, jobstatus_grouped"

    cur = connection.cursor()
    cur.execute(sql_hs_distribution)
    hs_distribution_raw = cur.fetchall()
    cur.close()

    # get the hs distribution data into a dictionary structure
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    for hs_entry in hs_distribution_raw:
        gshare, status_group, hs = hs_entry
        hs_distribution_dict.setdefault(gshare, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
        hs_distribution_dict[gshare][status_group] = hs
        # calculate totals
        if status_group == QUEUED:
            hs_queued_total += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
        else:
            hs_ignore_total += hs

    # Calculate the ideal HS06 distribution based on shares.

    for share_node in leave_shares:
        share_name, share_value = share_node.name, share_node.value
        hs_pledged_share = hs_executing_total * decimal.Decimal(str(share_value)) / decimal.Decimal(str(100.0))

        hs_distribution_dict.setdefault(share_name, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
        # Pledged HS according to global share definitions
        hs_distribution_dict[share_name]['pledged'] = hs_pledged_share

    getChildStat(tree, hs_distribution_dict, 0)
    rows = []
    stripTree(tree, rows)
    return hs_distribution_dict, rows

def stripTree(node, rows):
    row = {}
    if hasattr(node,'level'):
        if node.level > 0:
            if node.level == 1:
                row['level1'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
                row['level2'] = ''
                row['level3'] = ''
            if node.level == 2:
                row['level1'] = ''
                row['level2'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
                row['level3'] = ''
            if node.level == 3:
                row['level1'] = ''
                row['level2'] = ''
                row['level3'] = node.name + ' [' + ("%0.1f" % node.rawvalue) + '%]'
            row['executing'] = node.executing
            row['pledged'] = node.pledged
            row['delta'] = node.delta
            row['queued'] = node.queued
            row['ratio'] = node.ratio
            row['value'] = node.value
            rows.append(row)
    for item in node.children:
        stripTree(item, rows)


def getChildStat(node, hs_distribution_dict, level):
    executing = 0
    pledged = 0
    delta = 0
    queued = 0
    ratio = 0
    if node.name in hs_distribution_dict and len(node.children) == 0:
        executing = hs_distribution_dict[node.name]['executing']
        pledged = hs_distribution_dict[node.name]['pledged']
        delta = hs_distribution_dict[node.name]['executing'] - hs_distribution_dict[node.name]['pledged']
        queued = hs_distribution_dict[node.name]['queued']
    else:
        for item in node.children:
            getChildStat(item, hs_distribution_dict, level+1)
            executing += item.executing
            pledged += item.pledged
            delta += item.delta
            queued += item.queued
            #ratio = item.ratio if item.ratio!=None else 0

    node.executing = executing
    node.pledged = pledged
    node.delta = delta
    node.queued = queued
    node.level = level

    if (pledged != 0):
        ratio = executing / pledged *100
    else:
        ratio = None

    node.ratio = ratio


###JSON for Datatables globalshares###
def detailedInformationJSON(request):
    fullListGS = []
    sqlRequest = '''
SELECT gshare, corecount, jobstatus, count(*), sum(HS06)  FROM 
(select gshare,  (CASE 
WHEN corecount is null THEN 1 else corecount END 
) as corecount, 
 (CASE 
  WHEN jobstatus in ('defined','waiting','pending','assigned','throttled','activated','merging','starting','holding','transferring') THEN 'scheduled'
 WHEN jobstatus in ('sent','running') THEN 'running'
 WHEN jobstatus in ('finished','failed','cancelled','closed') THEN 'did run'
END) as jobstatus,HS06
from
atlas_panda.jobsactive4 
UNION ALL
select gshare,  (CASE 
WHEN corecount is null THEN 1 else corecount END 
) as corecount, 
(CASE 
 WHEN jobstatus in ('defined','waiting','pending','assigned','throttled','activated','merging','starting','holding','transferring') THEN 'scheduled'
 WHEN jobstatus in ('sent','running') THEN 'running'
 WHEN jobstatus in ('finished','failed','cancelled','closed') THEN 'did run'
END) as jobstatus,HS06
from
atlas_panda.JOBSDEFINED4
UNION ALL
select gshare,  (CASE 
   WHEN corecount is null THEN 1 else corecount END  
) as corecount, (CASE 
 WHEN jobstatus in ('defined','waiting','pending','assigned','throttled','activated','merging','starting','holding','transferring') THEN 'scheduled'
 WHEN jobstatus in ('sent','running') THEN 'running'
 WHEN jobstatus in ('finished','failed','cancelled','closed') THEN 'did run'
END) as jobstatus,HS06 from
atlas_panda.JOBSWAITING4) 
group by gshare, corecount, jobstatus
order by gshare, corecount, jobstatus
'''
    #if isJobsss:
    #sqlRequest += ' WHERE '+ codename + '='+codeval
    # INPUT_EVENTS, TOTAL_EVENTS, STEP
    shortListErrors = []
    #sqlRequestFull = sqlRequest.format(condition)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    globalSharesList = cur.fetchall()
    for gs in globalSharesList:
        if gs[1] == 1:
            corecount = 'Singlecore'
        elif gs[1]==0:
            corecount = 'Multicore'
        else:
            corecount = 'Multicore (' + str(gs[1]) + ')'
        rowDict = {"gshare": gs[0], "corecount": corecount, "jobstatus": gs[2], "count": gs[3], "hs06":gs[4]}
        fullListGS.append(rowDict)
    return HttpResponse(json.dumps(fullListGS), content_type='application/json')

def sharesDistributionJSON(request):
    fullListGS = []
    sqlRequest = '''
SELECT gshare,COMPUTINGSITE, corecount, jobstatus, COUNT(*), SUM(HS06)
FROM (select gshare,COMPUTINGSITE, (CASE 
   WHEN corecount is null THEN 1 else corecount END   
) as corecount, 
 (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus, HS06
from
atlas_panda.jobsactive4 
UNION ALL
select gshare,COMPUTINGSITE, (CASE 
  WHEN corecount is null THEN 1 else corecount END  
) as corecount, 
 (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus, HS06
from
atlas_panda.JOBSDEFINED4
UNION ALL
select gshare,COMPUTINGSITE, (CASE 
 WHEN corecount is null THEN 1 else corecount END   
) as corecount, (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus, HS06 from
atlas_panda.JOBSWAITING4
) group by gshare,COMPUTINGSITE, corecount, jobstatus
order by gshare,COMPUTINGSITE, corecount, jobstatus
'''
    #if isJobsss:
    #sqlRequest += ' WHERE '+ codename + '='+codeval
    # INPUT_EVENTS, TOTAL_EVENTS, STEP
    shortListErrors = []
    #sqlRequestFull = sqlRequest.format(condition)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    globalSharesList = cur.fetchall()
    resources = get_CRIC_resources()
    hs06count  = 0
    for gs in globalSharesList:
        if gs[2] == 1:
            corecount = 'Singlecore'
        elif gs[2]==0:
            corecount = 'Multicore'
        else:
            corecount = 'Multicore (' + str(gs[2]) + ')'
        if gs[1] in resources.keys():
            resource = resources[gs[1]]
        else: resource = None
        if gs[5] != None:
            hs06count= gs[5] / gs[4]
        else:
            hs06count= 0
        rowDict = {"gshare": gs[0],"computingsite": gs[1],"resources":resource, "corecount": str(corecount), "jobstatus": gs[3], "count": gs[4], "hs06":gs[5],"hs06/count": hs06count}
        fullListGS.append(rowDict)
    return HttpResponse(json.dumps(fullListGS), content_type='application/json')

def siteWorkQueuesJSON(request):
    fullListGS = []
    sqlRequest = '''
SELECT COMPUTINGSITE,gshare, corecount, jobstatus,COUNT (*)
FROM (select COMPUTINGSITE,gshare, (CASE 
   WHEN corecount is null THEN 1 else corecount END 
) as corecount, 
 (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus
from
atlas_panda.jobsactive4 
UNION ALL
select COMPUTINGSITE,gshare, (CASE 
  WHEN corecount is null THEN 1 else corecount END 
) as corecount, 
 (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus
from
atlas_panda.JOBSDEFINED4
UNION ALL
select COMPUTINGSITE,gshare, (CASE 
WHEN corecount is null THEN 1 else corecount END  
) as corecount, (CASE jobstatus
  WHEN 'running' THEN 'running'
  ELSE 'scheduled'
END) as jobstatus from
atlas_panda.JOBSWAITING4
) group by COMPUTINGSITE,gshare, corecount, jobstatus
order by COMPUTINGSITE,gshare, corecount, jobstatus
'''
    #if isJobsss:
    #sqlRequest += ' WHERE '+ codename + '='+codeval
    # INPUT_EVENTS, TOTAL_EVENTS, STEP
    shortListErrors = []
    #sqlRequestFull = sqlRequest.format(condition)
    cur = connection.cursor()
    cur.execute(sqlRequest)
    globalSharesList = cur.fetchall()
    for gs in globalSharesList:
        if gs[2]==1:
            corecount = 'Singlecore'
        elif gs[2]==0:
            corecount = 'Multicore'
        else: corecount = 'Multicore ('+str(gs[2])+')'
        rowDict = {"computingsite": gs[0],"gshare": gs[1], "corecount": str(corecount), "jobstatus": gs[3], "count": gs[4]}
        fullListGS.append(rowDict)
    return HttpResponse(json.dumps(fullListGS), content_type='application/json')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def get_CRIC_resources():
    resourcesDictSites = cache.get('resourcesDictSites')
    if not (resourcesDictSites):
        url = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        http = urllib3.PoolManager()
        data = {}
        try:
            resourcesDict = {}
            resourcesDictSites = {}
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for cs in data.keys():
                resourcesDict.setdefault(data[cs]['resource_type'], []).append(cs)
                resourcesDictSites[data[cs]['siteid']] = data[cs]['resource_type']
            cache.set('resourcesDictSites', resourcesDictSites, 3600)
        except Exception as exc:
            print (exc)
    return resourcesDictSites

def get_CRIC_fairsharepolicy():
    fairsharepolicyDictSites = cache.get('fairsharepolicyDictSites')
    if not (fairsharepolicyDictSites):
        url = "https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        http = urllib3.PoolManager()
        data = {}
        try:
            fairsharepolicyDict = {}
            fairsharepolicyDictSites = {}
            r = http.request('GET', url)
            data = json.loads(r.data.decode('utf-8'))
            for cs in data.keys():
                fairsharepolicyDict.setdefault(data[cs]['fairsharepolicy'], []).append(cs)
                fairsharepolicyDictSites[data[cs]['siteid']] = data[cs]['fairsharepolicy']
            cache.set('fairsharepolicyDictSites', fairsharepolicyDictSites, 3600)
        except Exception as exc:
            print (exc)
    return fairsharepolicyDictSites

def resourcesType(request):
    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'
    resourcesList = []
    resourcesDictSites = get_CRIC_resources()
    sqlRequest = """SELECT computingsite, jobstatus_grouped, SUM(HS) 
    FROM (SELECT computingsite, HS, CASE WHEN jobstatus IN('activated') THEN 'queued' WHEN jobstatus IN('sent', 'running') THEN 'executing'
    ELSE 'ignore' END jobstatus_grouped FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS) GROUP BY computingsite, jobstatus_grouped"""
    cur = connection.cursor()
    cur.execute(sqlRequest)
    # get the hs distribution data into a dictionary structure
    hs_distribution_raw = cur.fetchall()
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    total_hs = 0
    newresourecurcetype = ''
    resourcecnt = 0
    for hs_entry in hs_distribution_raw:
        computingsite, status_group, hs = hs_entry
        try:
            resourcetype = resourcesDictSites[computingsite]
        except:
            continue
        hs_distribution_dict.setdefault(resourcetype, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0,IGNORE:0})
        #hs_distribution_dict[resourcetype][status_group] = hs

        total_hs += hs

        # calculate totals
        if status_group == QUEUED:
            hs_queued_total += hs
            hs_distribution_dict[resourcetype][status_group] += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
            hs_distribution_dict[resourcetype][status_group] += hs
        else:
            hs_ignore_total += hs
            hs_distribution_dict[resourcetype][status_group] += hs
    ignore = 0
    pled = 0
    executing = 0
    queued = 0
    total_hs =0
    for hs_entry in hs_distribution_dict.keys():
        sum_hs = 0
        pled += hs_distribution_dict[hs_entry]['pledged']
        ignore += hs_distribution_dict[hs_entry]['ignore']
        executing += hs_distribution_dict[hs_entry]['executing']
        queued += hs_distribution_dict[hs_entry]['queued']
        sum_hs = float(hs_distribution_dict[hs_entry]['pledged']) + \
                 float(hs_distribution_dict[hs_entry]['ignore']) + \
                 float(hs_distribution_dict[hs_entry]['executing']) + \
                 float(hs_distribution_dict[hs_entry]['queued'])
        total_hs+=sum_hs
        hs_distribution_dict[hs_entry]['total_hs'] = sum_hs

    hs_distribution_list = []
    for hs_entry in hs_distribution_dict.keys():
       # hs_distribution_dict[hs_entry]['pledged_percent'] = pled * 100 / hs_distribution_dict[hs_entry]['pledged']
        hs_distribution_dict[hs_entry]['ignore_percent'] =  (hs_distribution_dict[hs_entry]['ignore']/ignore)* 100
        hs_distribution_dict[hs_entry]['executing_percent'] =  (hs_distribution_dict[hs_entry]['executing'] /executing) * 100
        hs_distribution_dict[hs_entry]['queued_percent'] = (hs_distribution_dict[hs_entry]['queued']/queued) * 100
        hs_distribution_list.append({'resource':hs_entry, 'pledged':hs_distribution_dict[hs_entry]['pledged'],
                                     'ignore':hs_distribution_dict[hs_entry]['ignore'],
                                     'ignore_percent':round(hs_distribution_dict[hs_entry]['ignore_percent'],2),
                                     'executing':hs_distribution_dict[hs_entry]['executing'],
                                     'executing_percent': round(hs_distribution_dict[hs_entry]['executing_percent'],2),
                                     'queued':hs_distribution_dict[hs_entry]['queued'],
                                     'queued_percent':round(hs_distribution_dict[hs_entry]['queued_percent'],2),
                                     'total_hs':hs_distribution_dict[hs_entry]['total_hs'],
                                     'total_hs_percent': round((hs_distribution_dict[hs_entry]['total_hs']/total_hs)*100,2)
                                     })
    return HttpResponse(json.dumps(hs_distribution_list, cls=DecimalEncoder), content_type='application/json')

def fairsharePolicy(request):
    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'
    #sqlrequest ="""select SITEID, fairsharepolicy  from atlas_pandameta.schedconfig"""
    fairsharepolicyDict = get_CRIC_fairsharepolicy()
    newfairsharepolicyDict = {}
    fairsharepolicies = fairsharepolicyDict.values()
    for site in fairsharepolicyDict.keys():
        if site not in newfairsharepolicyDict:
            newfairsharepolicyDict[site] = {}
        policy = fairsharepolicyDict[site]
        if policy != '':
            policies = policy.split(',')
            for pol in policies:
                try:
                    key, value = pol.split(':')
                    value = int(value.strip('%'))
                    if 'priority' not in key:
                        newfairsharepolicyDict[site][str(key)] = value
                except:
                    keys = pol.split(':')
                    for key in keys:
                        if key == 'group=any':
                           newfairsharepolicyDict[site]['group=any'] = 100
                        elif key == 'type=any':
                           newfairsharepolicyDict[site]['type=any'] = 60

        else: newfairsharepolicyDict[site]['type=any'] = 100
    sqlRequest = """SELECT computingsite, jobstatus_grouped, SUM(HS) 
     FROM (SELECT computingsite, HS, CASE WHEN jobstatus IN('activated') THEN 'queued' WHEN jobstatus IN('sent', 'running') THEN 'executing'
     ELSE 'ignore' END jobstatus_grouped FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS) GROUP BY computingsite, jobstatus_grouped"""
    cur = connection.cursor()
    cur.execute(sqlRequest)
    # get the hs distribution data into a dictionary structure
    hs_distribution_raw = cur.fetchall()
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    total_hs = 0
    newresourecurcetype = ''
    resourcecnt = 0
    for hs_entry in hs_distribution_raw:
        computingsite, status_group, hs = hs_entry
        try:
            for fairpolicies in newfairsharepolicyDict[computingsite]:
                hs_distribution_dict.setdefault(fairpolicies, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0, IGNORE: 0})
            # hs_distribution_dict[resourcetype][status_group] = hs
                total_hs += hs
                if newfairsharepolicyDict[computingsite][fairpolicies]==0:
                    newhs = hs * int(newfairsharepolicyDict[computingsite][fairpolicies])/100
                else: newhs = hs
                #newhs = hs * int(newfairsharepolicyDict[computingsite][fairpolicies]) / 100
            # calculate totals
                if status_group == QUEUED:
                    hs_queued_total += hs
                    hs_distribution_dict[fairpolicies][status_group] += newhs
                elif status_group == EXECUTING:
                    hs_executing_total += hs
                    hs_distribution_dict[fairpolicies][status_group] += newhs
                else:
                    hs_ignore_total += hs
                    hs_distribution_dict[fairpolicies][status_group] += newhs
        except:
            continue
    ignore = 0
    pled = 0
    executing = 0
    queued = 0
    total_hs =0
    for hs_entry in hs_distribution_dict.keys():
        sum_hs = 0
        pled += float(hs_distribution_dict[hs_entry]['pledged'])
        ignore += float(hs_distribution_dict[hs_entry]['ignore'])
        executing += float(hs_distribution_dict[hs_entry]['executing'])
        queued += float(hs_distribution_dict[hs_entry]['queued'])
        sum_hs = float(hs_distribution_dict[hs_entry]['pledged']) + \
                 float(hs_distribution_dict[hs_entry]['ignore']) + \
                 float(hs_distribution_dict[hs_entry]['executing']) + \
                 float(hs_distribution_dict[hs_entry]['queued'])
        total_hs+=sum_hs
        hs_distribution_dict[hs_entry]['total_hs'] = sum_hs

    hs_distribution_list = []
    for hs_entry in hs_distribution_dict.keys():
       # hs_distribution_dict[hs_entry]['pledged_percent'] = pled * 100 / hs_distribution_dict[hs_entry]['pledged']
        hs_distribution_dict[hs_entry]['ignore_percent'] = (float(hs_distribution_dict[hs_entry]['ignore'])/ignore)* 100
        hs_distribution_dict[hs_entry]['executing_percent'] =  (float(hs_distribution_dict[hs_entry]['executing']) /executing) * 100
        hs_distribution_dict[hs_entry]['queued_percent'] = (float(hs_distribution_dict[hs_entry]['queued'])/queued) * 100
        hs_distribution_list.append({'policy':hs_entry, 'pledged':hs_distribution_dict[hs_entry]['pledged'],
                                     'ignore':hs_distribution_dict[hs_entry]['ignore'],
                                     'ignore_percent': round(hs_distribution_dict[hs_entry]['ignore_percent'],2),
                                     'executing':hs_distribution_dict[hs_entry]['executing'],
                                     'executing_percent': round(hs_distribution_dict[hs_entry]['executing_percent'],2),
                                     'queued':hs_distribution_dict[hs_entry]['queued'],
                                     'queued_percent':round(hs_distribution_dict[hs_entry]['queued_percent'],2),
                                     'total_hs':hs_distribution_dict[hs_entry]['total_hs'],
                                     'total_hs_percent': round((hs_distribution_dict[hs_entry]['total_hs']/total_hs)*100,2)
                                     })
    return  HttpResponse(json.dumps(hs_distribution_list, cls=DecimalEncoder), content_type='application/json')



    return None
def coreTypes(request):
    EXECUTING = 'executing'
    QUEUED = 'queued'
    PLEDGED = 'pledged'
    IGNORE = 'ignore'
    sqlRequest = """
    SELECT corecount, jobstatus_grouped, SUM(HS)  FROM (SELECT 
   jj.ts,
    jj.gshare,
    jj.computingsite,
    (CASE WHEN jj.jobstatus IN('activated') THEN 'queued' WHEN jj.jobstatus IN('sent', 'running') THEN 'executing' ELSE 'ignore' END) as jobstatus_grouped, 
    jj.maxpriority,
    jj.njobs,
    jj.hs,
    jj.vo,
    jj.workqueue_id,
    jj.resource_type,
   (CASE WHEN gg.corecount =1 THEN 'SCORE' WHEN gg.corecount>1 and gg.catchall not like 'unifiedPandaQueue'  THEN 'MCORE' WHEN gg.catchall like 'unifiedPandaQueue' THEN 'UCORE' ELSE 'SCORE' END) as corecount,
    gg.fairsharepolicy,
    gg.CATCHALL
    FROM
    atlas_panda.jobs_share_stats jj,
    atlas_pandameta.schedconfig gg
    where jj.COMPUTINGSITE =gg.siteid) GROUP BY corecount, jobstatus_grouped order by corecount
    """
    cur = connection.cursor()
    cur.execute(sqlRequest)
    # get the hs distribution data into a dictionary structure
    hs_distribution_raw = cur.fetchall()
    hs_distribution_dict = {}
    hs_queued_total = 0
    hs_executing_total = 0
    hs_ignore_total = 0
    total_hs = 0
    newresourecurcetype = ''
    resourcecnt = 0
    for hs_entry in hs_distribution_raw:
        corecount, status_group, hs = hs_entry
        hs_distribution_dict.setdefault(corecount, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0, IGNORE:0})
        #hs_distribution_dict[corecount][status_group] = hs
        total_hs += hs

        # calculate totals
        if status_group == QUEUED:
            hs_queued_total += hs
            hs_distribution_dict[corecount][status_group] += hs
        elif status_group == EXECUTING:
            hs_executing_total += hs
            hs_distribution_dict[corecount][status_group] += hs
        else:
            hs_ignore_total += hs
            hs_distribution_dict[corecount][status_group] += hs
    ignore = 0
    pled = 0
    executing = 0
    queued = 0
    total_hs =0
    for hs_entry in hs_distribution_dict.keys():
        sum_hs = 0
        pled += hs_distribution_dict[hs_entry]['pledged']
        ignore += hs_distribution_dict[hs_entry]['ignore']
        executing += hs_distribution_dict[hs_entry]['executing']
        queued += hs_distribution_dict[hs_entry]['queued']
        sum_hs = float(hs_distribution_dict[hs_entry]['pledged']) + \
                 float(hs_distribution_dict[hs_entry]['ignore']) + \
                 float(hs_distribution_dict[hs_entry]['executing']) + \
                 float(hs_distribution_dict[hs_entry]['queued'])
        total_hs+=sum_hs
        hs_distribution_dict[hs_entry]['total_hs'] = sum_hs

    hs_distribution_list = []
    for hs_entry in hs_distribution_dict.keys():
       # hs_distribution_dict[hs_entry]['pledged_percent'] = pled * 100 / hs_distribution_dict[hs_entry]['pledged']
        hs_distribution_dict[hs_entry]['ignore_percent'] =  (hs_distribution_dict[hs_entry]['ignore']/ignore)* 100
        hs_distribution_dict[hs_entry]['executing_percent'] =  (hs_distribution_dict[hs_entry]['executing'] /executing) * 100
        hs_distribution_dict[hs_entry]['queued_percent'] = (hs_distribution_dict[hs_entry]['queued']/queued) * 100
        hs_distribution_list.append({'coretypes':hs_entry, 'pledged':hs_distribution_dict[hs_entry]['pledged'],
                                     'ignore':hs_distribution_dict[hs_entry]['ignore'],
                                     'ignore_percent': round(hs_distribution_dict[hs_entry]['ignore_percent'],2),
                                     'executing':hs_distribution_dict[hs_entry]['executing'],
                                     'executing_percent': round(hs_distribution_dict[hs_entry]['executing_percent'],2),
                                     'queued':hs_distribution_dict[hs_entry]['queued'],
                                     'queued_percent':round(hs_distribution_dict[hs_entry]['queued_percent'],2),
                                     'total_hs':hs_distribution_dict[hs_entry]['total_hs'],
                                     'total_hs_percent': round((hs_distribution_dict[hs_entry]['total_hs']/total_hs)*100,2)
                                     })
    return HttpResponse(json.dumps(hs_distribution_list, cls=DecimalEncoder), content_type='application/json')

