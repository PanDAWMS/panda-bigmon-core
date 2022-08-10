"""
A set of functions to get jobs from JOBS* and group them by a site and cloud
"""
import logging
import copy
import time
import itertools
import re

from django.db.models import Count
from django.core.cache import cache

from core.schedresource.utils import getCRICSites, get_basic_info_for_pqs, get_pq_clouds, get_panda_queues
from core.libs.exlib import getPilotCounts
from core.pandajob.models import Jobswaiting4, Jobsdefined4, Jobsactive4, Jobsarchived4

import core.constants as const

_logger = logging.getLogger('bigpandamon')


def site_summary_dict(sites, vo_mode='atlas', sortby='alpha'):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    sumd['copytool'] = {}
    for site in sites:
        for f in const.SITE_FIELDS_STANDARD:
            if f in site and site[f] is not None:
                if f not in sumd:
                    sumd[f] = {}
                if site[f] not in sumd[f]:
                    sumd[f][site[f]] = 0
                sumd[f][site[f]] += 1
        if 'copytool' in const.SITE_FIELDS_STANDARD:
            if 'copytools' in site and site['copytools'] and len(site['copytools']) > 0:
                copytools = list(site['copytools'].keys())
                for cp in copytools:
                    if cp not in sumd['copytool']:
                        sumd['copytool'][cp] = 0
                    sumd['copytool'][cp] += 1

    if vo_mode != 'atlas':
        try:
            del sumd['cloud']
        except:
            _logger.exception('Failed to remove cloud key from dict')

    # convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        for ky in kys:
            iteml.append({'kname': ky, 'kvalue': sumd[f][ky]})
        # sorting
        if sortby == 'count':
            iteml = sorted(iteml, key=lambda x: -x['kvalue'])
        else:
            iteml = sorted(iteml, key=lambda x: x['kname'])
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x: x['field'])
    return suml


def cloud_site_summary(query, extra='(1=1)', view='all', cloudview='region', notime=True, sortby='cloud'):
    start_time = time.time()

    ucoreComputingSites, harvesterComputingSites, typeComputingSites, _ = getCRICSites()

    _logger.debug('Got CRIC json: {}'.format(time.time() - start_time))

    siteinfol = get_basic_info_for_pqs([])
    siteinfo = {}
    for s in siteinfol:
        siteinfo[s['pq_name']] = s['status']

    _logger.debug('Got list of sites: {}'.format(time.time() - start_time))

    sitesummarydata = site_summary_data(query, notime, extra)

    pilots = getPilotCounts(view)
    nojobabshash = {}
    for site in pilots:
        nojobabshash[site] = pilots[site]['count_' + 'nojobabs']

    _logger.debug('Got njobsabs for for sites: {}'.format(time.time() - start_time))

    mismatchedSites = []
    clouds = {}
    totstates = {}
    totjobs = 0
    cloudsresources = {}
    pq_clouds = get_pq_clouds()
    for state in const.JOB_STATES_SITE:
        totstates[state] = 0
    for rec in sitesummarydata:

        if cloudview == 'region':
            if rec['computingsite'] in pq_clouds:
                cloud = pq_clouds[rec['computingsite']]
            else:
                _logger.debug("ERROR ComputingSite {} not found in Shedconfig, adding to mismatched sites".format(rec['computingsite']), rec)
                mismatchedSites.append([rec['computingsite'], rec['cloud']])
                cloud = ''
        else:
            cloud = rec['cloud']
        site = rec['computingsite']
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        resources = rec['resource']
        if jobstatus not in const.JOB_STATES_SITE: continue
        totjobs += count
        totstates[jobstatus] += count

        if cloud not in clouds:
            clouds[cloud] = {}
            clouds[cloud]['name'] = cloud
            clouds[cloud]['count'] = 0
            clouds[cloud]['pilots'] = 0
            clouds[cloud]['nojobabs'] = 0
            clouds[cloud]['sites'] = {}
            clouds[cloud]['states'] = {}
            clouds[cloud]['statelist'] = []
            cloudsresources[cloud] = {}
            cloudsresources[cloud]['sites'] = {}
            for state in const.JOB_STATES_SITE:
                clouds[cloud]['states'][state] = {}
                clouds[cloud]['states'][state]['name'] = state
                clouds[cloud]['states'][state]['count'] = 0
        clouds[cloud]['count'] += count
        clouds[cloud]['states'][jobstatus]['count'] += count
        if site not in clouds[cloud]['sites']:
            clouds[cloud]['sites'][site] = {}
            cloudsresources[cloud]['sites'][site] = {}
            cloudsresources[cloud]['sites'][site]['sumres'] = set()
            clouds[cloud]['sites'][site]['name'] = site
            if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
            clouds[cloud]['sites'][site]['count'] = 0
            if site in pilots:
                clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                clouds[cloud]['pilots'] += pilots[site]['count']
            else:
                clouds[cloud]['sites'][site]['pilots'] = 0

            if site in nojobabshash:
                clouds[cloud]['sites'][site]['nojobabs'] = nojobabshash[site]
                clouds[cloud]['nojobabs'] += nojobabshash[site]
            else:
                clouds[cloud]['sites'][site]['nojobabs'] = 0

            if site in harvesterComputingSites:
                clouds[cloud]['sites'][site]['isHarvester'] = True

            if site in typeComputingSites.keys():
                clouds[cloud]['sites'][site]['type'] = typeComputingSites[site]

            clouds[cloud]['sites'][site]['states'] = {}
            for state in const.JOB_STATES_SITE:
                clouds[cloud]['sites'][site]['states'][state] = {}
                clouds[cloud]['sites'][site]['states'][state]['name'] = state
                clouds[cloud]['sites'][site]['states'][state]['count'] = 0
        clouds[cloud]['sites'][site]['count'] += count
        clouds[cloud]['sites'][site]['states'][jobstatus]['count'] += count

        if checkUcoreSite(site, ucoreComputingSites):
            if 'resources' not in clouds[cloud]['sites'][site]['states'][jobstatus]:
                clouds[cloud]['sites'][site]['states'][jobstatus]['resources'] = {}
                clouds[cloud]['sites'][site]['states'][jobstatus]['resources'] = resources

                for reshash in resources.keys():
                    ressite = site + ' ' + reshash
                    if ressite not in clouds[cloud]['sites']:
                        clouds[cloud]['sites'][ressite] = {}
                        clouds[cloud]['sites'][ressite]['states'] = {}
                        clouds[cloud]['sites'][ressite]['resource'] = reshash
                        for parentjobstatus in clouds[cloud]['sites'][site]['states']:
                            if parentjobstatus not in clouds[cloud]['sites'][ressite]['states']:
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus] = {}
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['count'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['corecount'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['name'] = parentjobstatus
                        clouds[cloud]['sites'][ressite]['count'] = resources[reshash]['jobstatus__count']

                        clouds[cloud]['sites'][ressite]['name'] = ressite
                        clouds[cloud]['sites'][ressite]['nojobabs'] = -1
                        clouds[cloud]['sites'][ressite]['parent'] = site
                        clouds[cloud]['sites'][ressite]['parent_type'] = typeComputingSites[site]

                        if site in siteinfo:
                            clouds[cloud]['sites'][ressite]['status'] = siteinfo[site]
                        else:
                            clouds[cloud]['sites'][ressite]['status'] = ''
                        clouds[cloud]['sites'][ressite]['pilots'] = -1
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash]['corecount']
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash]['jobstatus__count']
                    else:
                        clouds[cloud]['sites'][ressite]['states'][jobstatus] = {}
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash]['jobstatus__count']
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['name'] = jobstatus
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash]['corecount']
                        clouds[cloud]['sites'][ressite]['count'] += resources[reshash]['jobstatus__count']
            else:
                hashreskeys = clouds[cloud]['sites'][site]['states'][jobstatus]['resources'].keys()
                for reshash in resources.keys():
                    if reshash in hashreskeys:
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['jobstatus__count'] += resources[reshash]['jobstatus__count']
                    else:
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash] = {}
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['jobstatus__count'] = resources[reshash]['jobstatus__count']
                        clouds[cloud]['sites'][site]['states'][jobstatus]['resources'][reshash]['corecount'] = resources[reshash]['corecount']
                    ressite = site + ' ' + reshash
                    if ressite not in clouds[cloud]['sites']:
                        clouds[cloud]['sites'][ressite] = {}
                        clouds[cloud]['sites'][ressite]['states'] = {}
                        clouds[cloud]['sites'][ressite]['resource'] = reshash
                        for parentjobstatus in clouds[cloud]['sites'][site]['states']:
                            if parentjobstatus not in clouds[cloud]['sites'][ressite]['states']:
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus] = {}
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['count'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['corecount'] = 0
                                clouds[cloud]['sites'][ressite]['states'][parentjobstatus]['name'] = parentjobstatus
                        clouds[cloud]['sites'][ressite]['count'] = resources[reshash]['jobstatus__count']

                        clouds[cloud]['sites'][ressite]['name'] = ressite
                        clouds[cloud]['sites'][ressite]['nojobabs'] = -1
                        clouds[cloud]['sites'][ressite]['parent'] = site
                        if site in siteinfo:
                            clouds[cloud]['sites'][ressite]['status'] = siteinfo[site]
                        else:
                            clouds[cloud]['sites'][ressite]['status'] = ''
                        clouds[cloud]['sites'][ressite]['pilots'] = -1
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['corecount'] = resources[reshash]['corecount']

                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] = resources[reshash]['jobstatus__count']
                    else:
                        clouds[cloud]['sites'][ressite]['states'][jobstatus]['count'] += resources[reshash]['jobstatus__count']
                        clouds[cloud]['sites'][ressite]['count'] += resources[reshash]['jobstatus__count']
            if 'sumres' not in clouds[cloud]['sites'][site]:
                clouds[cloud]['sites'][site]['sumres'] = set()
                for res in resources.keys():
                    clouds[cloud]['sites'][site]['sumres'].add(res)
                    cloudsresources[cloud]['sites'][site]['sumres'].add(res)
            else:
                for res in resources.keys():
                    clouds[cloud]['sites'][site]['sumres'].add(res)
                    cloudsresources[cloud]['sites'][site]['sumres'].add(res)

    for cloud in clouds.keys():
        for site in clouds[cloud]['sites'].keys():
            if 'sumres' in clouds[cloud]['sites'][site]:
                clouds[cloud]['sites'][site]['sumres'] = list(clouds[cloud]['sites'][site]['sumres'])
            for jobstate in clouds[cloud]['sites'][site]['states'].keys():
                if 'resources' in clouds[cloud]['sites'][site]['states'][jobstate]:
                    for res in cloudsresources[cloud]['sites'][site]['sumres']:
                        if res not in clouds[cloud]['sites'][site]['states'][jobstate]['resources'].keys():
                            clouds[cloud]['sites'][site]['states'][jobstate]['resources'][res] = {'jobstatus__count':0, 'corecount':0}

    _logger.debug('Precessed data for site summary: {}'.format(time.time() - start_time))

    updateCacheWithListOfMismatchedCloudSites(mismatchedSites)

    _logger.debug('Updated Cache with  mistmatched cloud|sites : {}'.format(time.time() - start_time))

    # Go through the sites, add any that are missing (because they have no jobs in the interval)
    panda_queues = get_panda_queues()
    if cloudview != 'cloud':
        for site in panda_queues:
            if view.find('test') < 0:
                if view != 'analysis' and site.startswith('ANALY'): continue
                if view == 'analysis' and not site.startswith('ANALY'): continue
            cloud = panda_queues[site]['cloud']
            if cloud not in clouds:
                # Bail. Adding sites is one thing; adding clouds is another
                continue
            if site not in clouds[cloud]['sites']:
                clouds[cloud]['sites'][site] = {}
                clouds[cloud]['sites'][site]['name'] = site
                if site in siteinfo: clouds[cloud]['sites'][site]['status'] = siteinfo[site]
                clouds[cloud]['sites'][site]['count'] = 0
                clouds[cloud]['sites'][site]['pctfail'] = 0

                if site in nojobabshash:
                    clouds[cloud]['sites'][site]['nojobabs'] = nojobabshash[site]
                    clouds[cloud]['nojobabs'] += nojobabshash[site]
                else:
                    clouds[cloud]['sites'][site]['nojobabs'] = 0

                if site in pilots:
                    clouds[cloud]['sites'][site]['pilots'] = pilots[site]['count']
                    clouds[cloud]['pilots'] += pilots[site]['count']
                else:
                    clouds[cloud]['sites'][site]['pilots'] = 0

                clouds[cloud]['sites'][site]['states'] = {}
                for state in const.JOB_STATES_SITE:
                    clouds[cloud]['sites'][site]['states'][state] = {}
                    clouds[cloud]['sites'][site]['states'][state]['name'] = state
                    clouds[cloud]['sites'][site]['states'][state]['count'] = 0

    # Convert dict to summary list
    cloudkeys = clouds.keys()
    cloudkeys = sorted(cloudkeys)
    fullsummary = []
    allstated = {}
    allstated['finished'] = allstated['failed'] = 0
    allclouds = {}
    allclouds['name'] = 'All'
    allclouds['count'] = totjobs
    allclouds['pilots'] = 0
    allclouds['nojobabs'] = 0

    allclouds['sites'] = {}
    allclouds['states'] = totstates
    allclouds['statelist'] = []
    for state in const.JOB_STATES_SITE:
        allstate = {}
        allstate['name'] = state
        allstate['count'] = totstates[state]
        allstated[state] = totstates[state]
        allclouds['statelist'].append(allstate)
    if int(allstated['finished']) + int(allstated['failed']) > 0:
        allclouds['pctfail'] = int(100. * float(allstated['failed']) / (allstated['finished'] + allstated['failed']))
    else:
        allclouds['pctfail'] = 0
    for cloud in cloudkeys:
        allclouds['pilots'] += clouds[cloud]['pilots']
    fullsummary.append(allclouds)

    for cloud in cloudkeys:
        for state in const.JOB_STATES_SITE:
            clouds[cloud]['statelist'].append(clouds[cloud]['states'][state])
        sites = clouds[cloud]['sites']
        sitekeys = list(sites.keys())
        sitekeys = sorted(sitekeys)
        cloudsummary = []
        for site in sitekeys:
            sitesummary = []
            for state in const.JOB_STATES_SITE:
                sitesummary.append(sites[site]['states'][state])
            sites[site]['summary'] = sitesummary
            if sites[site]['states']['finished']['count'] + sites[site]['states']['failed']['count'] > 0:
                sites[site]['pctfail'] = int(100. * float(sites[site]['states']['failed']['count']) / (
                sites[site]['states']['finished']['count'] + sites[site]['states']['failed']['count']))
            else:
                sites[site]['pctfail'] = 0

            cloudsummary.append(sites[site])
        clouds[cloud]['summary'] = cloudsummary
        if clouds[cloud]['states']['finished']['count'] + clouds[cloud]['states']['failed']['count'] > 0:
            clouds[cloud]['pctfail'] = int(100. * float(clouds[cloud]['states']['failed']['count']) / (
                clouds[cloud]['states']['finished']['count'] + clouds[cloud]['states']['failed']['count']))
        else:
            clouds[cloud]['pctfail'] = 0

        fullsummary.append(clouds[cloud])

    _logger.debug('Finished cloud|sites summary: {}'.format(time.time() - start_time))

    if sortby in const.JOB_STATES:
        fullsummary = sorted(fullsummary, key=lambda x: x['states'][sortby]['count'], reverse=True)
        for cloud in clouds:
            clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'], key=lambda x: x['states'][sortby]['count'], reverse=True)
    elif sortby == 'pctfail':
        fullsummary = sorted(fullsummary, key=lambda x: x['pctfail'], reverse=True)
        for cloud in clouds:
            clouds[cloud]['summary'] = sorted(clouds[cloud]['summary'], key=lambda x: x['pctfail'], reverse=True)

    _logger.debug('Sorted cloud|sites summary: {}'.format(time.time() - start_time))

    return fullsummary


def checkUcoreSite(site, usites):
    isUsite = False
    if site in usites:
       isUsite = True
    return isUsite


def updateCacheWithListOfMismatchedCloudSites(mismatchedSites):
    try:
        listOfCloudSitesMismatched = cache.get('mismatched-cloud-sites-list')
    except:
        listOfCloudSitesMismatched = None
    if (listOfCloudSitesMismatched is None) or (len(listOfCloudSitesMismatched) == 0):
        cache.set('mismatched-cloud-sites-list', mismatchedSites, 31536000)
    else:
        listOfCloudSitesMismatched.extend(mismatchedSites)
        listOfCloudSitesMismatched = sorted(listOfCloudSitesMismatched)
        cache.set('mismatched-cloud-sites-list',
                  list(listOfCloudSitesMismatched for listOfCloudSitesMismatched, _ in itertools.groupby(listOfCloudSitesMismatched)), 31536000)


def site_summary_data(query, notime=True, extra="(1=1)"):
    """
    Summary of jobs in different states for errors page to indicate if the errors caused by massive site failures or not
    """
    summary = []
    summaryResources = []
    # remove jobstatus from the query
    if 'jobstatus__in' in query:
        del query['jobstatus__in']
    # remove the time window limit for active jobs table
    querynotime = copy.deepcopy(query)
    if notime:
        if 'modificationtime__castdate__range' in querynotime:
            del querynotime['modificationtime__castdate__range']
    ejquery = {'jobstatus__in': ['failed', 'finished', 'closed', 'cancelled']}
    jvalues = ('cloud', 'computingsite', 'jobstatus', 'resourcetype', 'corecount')
    orderby = ('cloud', 'computingsite', 'jobstatus')
    summaryResources.extend(
        Jobsactive4.objects.filter(**querynotime).exclude(**ejquery).values(*jvalues).extra(where=[extra]).annotate(Count('jobstatus')).order_by(*orderby))
    summaryResources.extend(
        Jobsactive4.objects.filter(**query).filter(**ejquery).values(*jvalues).extra(where=[extra]).annotate(Count('jobstatus')).order_by(*orderby))
    summaryResources.extend(
        Jobsdefined4.objects.filter(**querynotime).values(*jvalues).extra(where=[extra]).annotate(Count('jobstatus')).order_by(*orderby))
    summaryResources.extend(
        Jobswaiting4.objects.filter(**querynotime).values(*jvalues).extra(where=[extra]).annotate(Count('jobstatus')).order_by(*orderby))
    summaryResources.extend(
        Jobsarchived4.objects.filter(**query).values(*jvalues).extra(where=[extra]).annotate(Count('jobstatus')).order_by(*orderby))

    summaryResourcesDict = {}
    actualcorecount = 0
    for sumS in summaryResources:
        if sumS['corecount'] is None:
            actualcorecount = 1
        else:
            actualcorecount = sumS['corecount']

        if sumS['cloud'] not in summaryResourcesDict:
            summaryResourcesDict[sumS['cloud']] = {}
        if sumS['computingsite'] not in summaryResourcesDict[sumS['cloud']]:
            summaryResourcesDict[sumS['cloud']][sumS['computingsite']] = {}
        if sumS['jobstatus'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']]:
            summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']] = {}
        if sumS['resourcetype'] not in summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']]:
            summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']] = {
                'jobstatus__count': 0,
                'corecount': actualcorecount
            }
        summaryResourcesDict[sumS['cloud']][sumS['computingsite']][sumS['jobstatus']][sumS['resourcetype']]['jobstatus__count'] += sumS['jobstatus__count']

    summaryList = []
    obj = {}
    for cloud in summaryResourcesDict.keys():
        for site in summaryResourcesDict[cloud].keys():
            for jobstatus in summaryResourcesDict[cloud][site].keys():
                jobscount =0
                obj['resource'] = {}
                for i, resource in enumerate(summaryResourcesDict[cloud][site][jobstatus]):
                    if resource not in obj['resource']:
                        obj['resource'][resource] = {}
                        obj['resource'][resource]['jobstatus__count'] = {}
                    if resource not in obj['resource']:
                        obj['resource'][resource] = {}
                        obj['resource'][resource]['corecount'] = {}
                    obj['resource'][resource]['jobstatus__count'] = summaryResourcesDict[cloud][site][jobstatus][resource]['jobstatus__count']
                    obj['resource'][resource]['corecount'] = summaryResourcesDict[cloud][site][jobstatus][resource]['corecount']
                    jobscount += summaryResourcesDict[cloud][site][jobstatus][resource]['jobstatus__count']
                    if i == len(summaryResourcesDict[cloud][site][jobstatus]) - 1:
                        obj['cloud'] = cloud
                        obj['computingsite'] = site
                        obj['jobstatus'] = jobstatus
                        obj['jobstatus__count'] = jobscount
                        summaryList.append(obj)
                        obj = {}
    return summaryList


def vo_summary(query, sortby='name'):
    vosummarydata = vo_summary_data(query)
    vos = {}
    for rec in vosummarydata:
        vo = rec['vo']
        # if vo == None: vo = 'Unassigned'
        if vo == None: continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if vo not in vos:
            vos[vo] = {}
            vos[vo]['name'] = vo
            vos[vo]['count'] = 0
            vos[vo]['states'] = {}
            vos[vo]['statelist'] = []
            for state in const.JOB_STATES_SITE:
                vos[vo]['states'][state] = {}
                vos[vo]['states'][state]['name'] = state
                vos[vo]['states'][state]['count'] = 0
        vos[vo]['count'] += count
        vos[vo]['states'][jobstatus]['count'] += count

    # Convert dict to summary list
    vokeys = list(vos.keys())
    vokeys = sorted(vokeys)
    vosummary = []
    for vo in vokeys:
        for state in const.JOB_STATES_SITE:
            vos[vo]['statelist'].append(vos[vo]['states'][state])
            if int(vos[vo]['states']['finished']['count']) + int(vos[vo]['states']['failed']['count']) > 0:
                vos[vo]['pctfail'] = int(100. * float(vos[vo]['states']['failed']['count']) / (
                        vos[vo]['states']['finished']['count'] + vos[vo]['states']['failed']['count']))
        vosummary.append(vos[vo])

    # Sort
    if sortby in const.JOB_STATES_SITE:
        vosummary = sorted(vosummary, key=lambda x: x['states'][sortby], reverse=True)
    elif sortby == 'pctfail':
        vosummary = sorted(vosummary, key=lambda x: x['pctfail'], reverse=True)
    else:
        vosummary = sorted(vosummary, key=lambda x: x['name'])

    return vosummary


def vo_summary_data(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__castdate__range']
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('vo', 'jobstatus').annotate(Count('jobstatus')))
    return summary