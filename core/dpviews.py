#
# dpviews.py
#
# Prototyping for Data Product Catalog and associated functionality
#
import logging, re, json, commands, os, copy
from datetime import datetime, timedelta
from datetime import tzinfo
import time
import json
from urlparse import urlparse

from django.http import HttpResponse
from django.shortcuts import render_to_response, render, redirect
from django.template import RequestContext, loader
from django.db.models import Count, Sum
from django import forms
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.utils.cache import patch_cache_control, patch_response_headers
from django.utils import timezone
from django.contrib import messages

import boto.dynamodb2
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import STRING
from boto.dynamodb2.types import STRING_SET

from settings.local import aws

from core.common.models import TRequest, TProject, RequestStatus, ProductionTask, StepTemplate, StepExecution, InputRequestList, ProductionContainer, ProductionDataset, Ttrfconfig

from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from core.common.models import JediTasks
from core.common.models import Filestable4
from core.common.models import FilestableArch
from core.common.models import JediDatasets
from core.settings.config import ENV
from core.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat

import views as coreviews

ENV['MON_VO'] = 'ATLAS'
viewParams = {}
viewParams['MON_VO'] = ENV['MON_VO']

req_fields = [ 'project_id', 'phys_group', 'campaign', 'manager', 'provenance', 'request_type', 'project', 'is_fast' ]
prodtask_fields = [ 'campaign', 'simulation_type', 'status', 'phys_group', ]
jeditask_fields = [ 'jeditaskid', 'cloud', 'processingtype', 'superstatus', 'status', 'ramcount', 'walltime', 'currentpriority', 'transhome', 'corecount', 'progress', 'failurerate', ]

# entity types supported in searches
entitytypes = [
    [ 'dataset', 'Datasets and containers' ],
    ]

## Open Amazon DynamoDB databases
dyndb = DynamoDBConnection(aws_access_key_id=aws['AWS_ACCESS_KEY_ATLAS'], aws_secret_access_key=aws['AWS_SECRET_KEY_ATLAS'])
usertable = Table('user', connection=dyndb)
projecttable = Table('project', connection=dyndb)
requesttable = Table('request', connection=dyndb)

def doRequest(request):

    ## Set default page lifetime in the http header, for the use of the front end cache
    request.session['max_age_minutes'] = 6

    ## by default, show main intro
    mode = 'intro'
    if 'mode' in request.GET: mode = request.GET['mode']
    query = {}
    dataset = None
    scope = None
    tid = None
    tidnum = None
    reqid = None
    dataset_form = DatasetForm()

    show_form = True
    if request.method == 'POST':

        ## if POST, we have a form input to process
        formdata = request.POST.copy()
        request.session['requestParams'] = formdata
        #if 'action' in request.POST and request.POST['action'] == 'edit_from_db':

        if request.user.is_authenticated():
            formdata['requester'] = request.user.get_full_name()

        if formdata['type'] in ('dataset', ''):
            formdata['type'] = 'dataset'
            dataset_form = DatasetForm(formdata)
            ## proceed with the dataset search
            if dataset_form.is_valid():
                formdata = dataset_form.cleaned_data.copy()
                mode = 'dataset'
                dataset = formdata['dataset'].strip()
            else:
                messages.warning(request, "The requested dataset search is not valid")
                print 'POST', request.POST
        else:
            messages.warning(request, "Unrecognized form %s" % request.POST['type'])

    else:
        ## GET
        formdata = request.GET.copy()
        request.session['requestParams'] = formdata

        if request.user.is_authenticated():
            formdata['requester'] = request.user.get_full_name()

        for f in req_fields:
            if f in request.GET:
                query[f] = request.GET[f]
        if 'reqid' in request.GET:
            reqid = request.GET['reqid']
            query['reqid'] = reqid
            mode = 'reqid'
        elif 'dataset' in request.GET:
            dataset = request.GET['dataset']
            formdata = request.GET.copy()
            formdata['type'] = 'dataset'
            dataset_form = DatasetForm(formdata)
            mode = 'dataset'
            show_form = True
        else:
            query['reqid__gte'] = 920

    if 'nosearch' in request.session['requestParams']: nosearch = True
    else: nosearch = False

    #projects = projecttable.scan()
    #projects = TProject.objects.using('deft_adcr').all().values()
    projectd = {}
    #for p in projects:
    #    projectd[p['project']] = p['description']
    projects = TProject.objects.using('deft_adcr').all().values()
    for p in projects:
        projectd[p['project']] = p

    reqs = []
    indatasets = []
    thisProject = None
    if mode in ('request', 'reqid'):
        reqs = TRequest.objects.using('deft_adcr').filter(**query).order_by('reqid').reverse().values()

    for r in reqs:
        if 'project_id' in r and r['project_id']:
            r['projectdata'] = projectd[r['project_id']]
        else:
            r['projectdata'] = None
    if len(reqs) > 0 and reqid: thisProject = reqs[0]['projectdata']

    reqstatd = {}
    query = {}
    if reqid: query['request_id'] = reqid
    reqstats = RequestStatus.objects.using('deft_adcr').filter(**query).values()
    for rs in reqstats:
        reqstatd[rs['request_id']] = rs
    for r in reqs:
        rid = r['reqid']
        if r['ref_link']:
            refparsed = urlparse(r['ref_link'])
            r['ref_link_path'] = os.path.basename(refparsed.path)[:18]
        if rid in reqstatd:
            r['comment'] = reqstatd[rid]['comment']
            r['status'] = reqstatd[rid]['status']
            r['timestamp'] = reqstatd[rid]['timestamp']
            r['owner'] = reqstatd[rid]['owner']
        else:
            r['timestamp'] = timezone.now() - timedelta(days=365*20)
            r['status'] = '?'
        if 'comment' in r and r['comment'].endswith('by WebUI'): r['comment'] = ''

    ## get event count for each slice
    sliceevs = InputRequestList.objects.using('deft_adcr').filter(request_id=reqid).order_by('slice').reverse().values('id','dataset__events')

    datasets = containers = tasks = jeditasks = jedidatasets = steps = slices = files = dsslices = []
    events_processed = None
    request_columns = None
    jobsum = []
    jobsumd = {}
    jeditaskstatus = {}
    cloudtodo = {}
    dsevents = {}
    dseventsprodsys = {}
    totalfiles = None
    if reqid:
        events_processed = {}
        ## Prepare information for the particular request
        datasets = ProductionDataset.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        containers = ProductionContainer.objects.using('deft_adcr').filter(rid=reqid).order_by('name').values()
        tasks = ProductionTask.objects.using('deft_adcr').filter(request_id=reqid).order_by('id').values()
        steps = StepExecution.objects.using('deft_adcr').filter(request_id=reqid).values()
        slices = InputRequestList.objects.using('deft_adcr').filter(request_id=reqid).order_by('slice').reverse().values()
        jeditasks = JediTasks.objects.filter(reqid=reqid,tasktype='prod').values(*jeditask_fields)
        amitags = Ttrfconfig.objects.using('grisli').all().values()
        amitagd = {}
        for t in amitags:
            t['params'] = ""
            lparams = t['lparams'].split(',')
            vparams = t['vparams'].split(',')
            i=0
            for p in lparams:
                if vparams[i] != 'NONE':
                    txt = "&nbsp; %s=%s" % ( lparams[i], vparams[i] )
                    t['params'] += txt
                i += 1
            ctag = "%s%s" % ( t['tag'], t['cid'] )
            amitagd[ctag] = t
        jeditaskd = {}
        for t in jeditasks:
            jeditaskd[t['jeditaskid']] = t
            jeditaskstatus[t['jeditaskid']] = t['superstatus']

        indsdict = {}
        for t in tasks:
            if t['id'] in jeditaskd: t['jeditask'] = jeditaskd[t['id']]
            indsdict[t['inputdataset']] = 1

        ## get records for input datasets
        for s in slices:
            if s['dataset_id']: indsdict[s['dataset_id']] = 1
        indslist = indsdict.keys()
        indatasets = ProductionDataset.objects.using('deft_adcr').filter(name__in=indslist).values()

        ## get records for input containers
        indslist = []
        for s in slices:
            if s['dataset_id']: indslist.append(s['dataset_id'])
        incontainers = ProductionContainer.objects.using('deft_adcr').filter(name__in=indslist).values()

        ## get task datasets
        taskdsdict = {}
        for ds in datasets:
            if ds['task_id'] not in taskdsdict: taskdsdict[ds['task_id']] = []
            taskdsdict[ds['task_id']].append(ds)

        for t in tasks:
            if t['id'] in taskdsdict: t['dslist'] = taskdsdict[t['id']]

        ## get input, output dataset info associated with tasks
        taskl = []
        for t in tasks:
            taskl.append(t['id'])
        tdsquery = {}
        tdsquery['jeditaskid__in'] = taskl
        tdsquery['type__in'] = ['input','output']
        tdsets = JediDatasets.objects.filter(**tdsquery).values('cloud','jeditaskid','nfiles','nfilesfinished','nfilesfailed','nevents','type','datasetname', 'streamname')
        taskoutputdsd = {}
        taskinputdsd = {}
        taskinputdsl = []
        totalfiles = {}
        if len(tdsets) > 0:
            for ds in tdsets:
                proctype = jeditaskd[ds['jeditaskid']]['processingtype']
                if proctype not in totalfiles:
                    totalfiles[proctype] = {}
                    totalfiles[proctype]['total_in'] = 0
                    totalfiles[proctype]['finished'] = 0
                    totalfiles[proctype]['failed'] = 0
                    totalfiles[proctype]['total_out'] = 0
                if ds['type'] == 'input':
                    totalfiles[proctype]['total_in'] += ds['nfiles']
                    if reqid not in dsevents: dsevents[reqid] = 0
                    dsevents[reqid] += ds['nevents']
                    if ds['jeditaskid'] not in taskinputdsd: taskinputdsd[ds['jeditaskid']] = []
                    if ds['streamname'] != 'DBR': taskinputdsl.append(ds['datasetname'])
                    taskinputdsd[ds['jeditaskid']].append(ds)
                elif ds['type'] == 'output':
                    totalfiles[proctype]['total_out'] += ds['nfiles']
                    totalfiles[proctype]['finished'] += ds['nfilesfinished']
                    totalfiles[proctype]['failed'] += ds['nfilesfailed']
                    if ds['jeditaskid'] not in taskoutputdsd: taskoutputdsd[ds['jeditaskid']] = []
                    taskoutputdsd[ds['jeditaskid']].append(ds)
                if jeditaskd[ds['jeditaskid']]['superstatus'] in ( 'broken', 'aborted' ) : continue
                cloud = jeditaskd[ds['jeditaskid']]['cloud']  
                if cloud not in cloudtodo:
                    cloudtodo[cloud] = {}
                    cloudtodo[cloud]['nfiles'] = 0
                    cloudtodo[cloud]['nfilesfinished'] = 0
                    cloudtodo[cloud]['nfilesfailed'] = 0
                cloudtodo[cloud]['nfiles'] += ds['nfiles']
                cloudtodo[cloud]['nfilesfinished'] += ds['nfilesfinished']
                cloudtodo[cloud]['nfilesfailed'] += ds['nfilesfailed']
            for t in taskoutputdsd:
                taskoutputdsd[t].sort()
            for t in taskinputdsd:
                taskinputdsd[t].sort()
            for t in tasks:
                if t['id'] in taskoutputdsd:
                    t['outdslist'] = taskoutputdsd[t['id']]
                if t['id'] in taskinputdsd:
                    t['indslist'] = taskinputdsd[t['id']]

        totalfiles_list = []
        tfkeys = totalfiles.keys()
        tfkeys.sort()
        for k in tfkeys:
            totalfiles[k]['processingtype'] = k
            if totalfiles[k]['total_in'] and totalfiles[k]['total_in'] > 0:
                totalfiles[k]['progress'] = int(100.*float(totalfiles[k]['finished'])/float(totalfiles[k]['total_in']))
            else:
                totalfiles[k]['progress'] = 0
            totalfiles_list.append(totalfiles[k])
        totalfiles = totalfiles_list

        if False: # total waste of time. Takes forever and the event counts come back zero anyway.
            ## fetch input datasets, and count up the events
            query = { 'datasetname__in' : taskinputdsl }
            indsets = JediDatasets.objects.filter(**query).values()
            dsevs = {}
            for ds in indsets:
                if ds['datasetname'] not in dsevs: dsevs[ds['datasetname']] = 0
                if ds['nevents'] > dsevs[ds['datasetname']]: dsevs[ds['datasetname']] = ds['nevents']
            total_events = 0
            for d in dsevs:
                total_events += dsevs[d]
            for ds in taskinputdsl:
                if ds not in dsevs: print 'missing dataset event count', ds

        ## add info to slices
        sliceids = {}
        for s in slices:
            sliceids[s['id']] = s['slice']
        clones = {}
        for s in slices:
            if s['dataset_id']:
                s['dataset_id_html'] = s['dataset_id'].replace(s['brief'],'<b>%s</b>' % s['brief'])
            for ds in indatasets:
                if ds['name'] == s['dataset_id']: s['dataset_data'] = ds
            if 'cloned_from_id' in s and s['cloned_from_id'] and s['cloned_from_id'] in sliceids:
                    s['cloned_from'] = sliceids[s['cloned_from_id']]
                    if not sliceids[s['cloned_from_id']] in clones: clones[sliceids[s['cloned_from_id']]] = []
                    clones[sliceids[s['cloned_from_id']]].append(sliceids[s['id']])
        for s in slices:
            if s['slice'] in clones: s['clones'] = clones[s['slice']]

        taskjobd = {}
        if reqid:
            ## job counts per task
            tjquery = { 'reqid' : reqid, 'prodsourcelabel' : 'managed' }
            taskjobs = Jobsarchived4.objects.filter(**tjquery).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('jeditaskid','jobstatus')
            taskjobs_arch = Jobsarchived.objects.filter(**tjquery).values('jeditaskid','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('jeditaskid','jobstatus')
            tjd = {}
            tjda = {}
            for j in taskjobs_arch:
                if j['jeditaskid'] not in tjda:
                    tjda[j['jeditaskid']] = {}
                    tjda[j['jeditaskid']]['totjobs'] = 0
                tjda[j['jeditaskid']]['totjobs'] += j['jobstatus__count']
                tjda[j['jeditaskid']][j['jobstatus']] = {}
                tjda[j['jeditaskid']][j['jobstatus']]['nevents'] = j['nevents__sum']
                tjda[j['jeditaskid']][j['jobstatus']]['njobs'] = j['jobstatus__count']
            for j in taskjobs:
                if j['jeditaskid'] not in tjda:
                    tjda[j['jeditaskid']] = {}
                    tjda[j['jeditaskid']]['totjobs'] = 0
                tjda[j['jeditaskid']]['totjobs'] += j['jobstatus__count']
            for j in taskjobs:
                if j['jeditaskid'] not in tjd: tjd[j['jeditaskid']] = {}
                if j['jeditaskid'] in tjda and j['jobstatus'] in tjda[j['jeditaskid']]:
                    j['nevents__sum'] += tjda[j['jeditaskid']][j['jobstatus']]['nevents']
                    j['jobstatus__count'] += tjda[j['jeditaskid']][j['jobstatus']]['njobs']
                pct = 100.*float(j['jobstatus__count'])/float(tjda[j['jeditaskid']]['totjobs'])
                pct = int(pct)
                if j['nevents__sum'] > 0:
                    tjd[j['jeditaskid']][j['jobstatus']] = "<span class='%s'>%s:%.0f%% (%s)/%s evs</span>" % ( j['jobstatus'], j['jobstatus'], pct, j['jobstatus__count'], j['nevents__sum'] )
                else:
                    tjd[j['jeditaskid']][j['jobstatus']] = "<span class='%s'>%s:%.0f%% (%s)</span>" % ( j['jobstatus'], j['jobstatus'], pct, j['jobstatus__count'] )
            for t in tjd:
                tstates = []
                for s in tjd[t]:
                    tstates.append(tjd[t][s])
                tstates.sort()
                taskjobd[t] = tstates

        ## get the needed step templates (ctags)
        tasklist = []
        ctagd = {}
        for st in steps:
            ctagd[st['step_template_id']] = 1
        ctagl = ctagd.keys
        ctags = StepTemplate.objects.using('deft_adcr').filter(id__in=ctagl).order_by('ctag').values()
        for st in steps:
            ## add ctags to steps
            for ct in ctags:
                if st['step_template_id'] == ct['id']:
                    st['ctag'] = ct
            ## add ctag details
            if 'ctag' in st and st['ctag']['ctag'] in amitagd: st['ctagdetails'] = amitagd[st['ctag']['ctag']]
            evtag = "%s %s" % (st['ctag']['step'],st['ctag']['ctag'])
            ## add tasks to steps
            st['tasks'] = []
            for t in tasks:
                if t['id'] in jeditaskstatus:
                    t['jedistatus'] = jeditaskstatus[t['id']]
                if t['id'] in taskjobd: t['jobstats'] = taskjobd[t['id']]
                if t['step_id'] == st['id']:
                    st['tasks'].append(t)
                    tasklist.append(t['name'])
                    if t['status'] not in ( 'aborted', 'broken' ):
                        if evtag not in events_processed: events_processed[evtag] = 0
                        events_processed[evtag] += t['total_events']            

        ## for each slice, add its steps
        for sl in slices:
            sl['steps'] = []
            for st in steps:
                if st['slice_id'] == sl['id']: sl['steps'].append(st)
            ## prepare dump of all fields
            reqd = {}
            colnames = []
            request_columns = []
            try:
                req = reqs[0]
                colnames = req.keys()
                colnames.sort()
                for k in colnames:
                    val = req[k]
                    if req[k] == None:
                        val = ''
                        continue
                    pair = { 'name' : k, 'value' : val }
                    request_columns.append(pair)
            except IndexError:
                reqd = {}

    ## gather summary info for request listings
    ptasksuml = 0
    if reqid or (mode == 'request'):
        query = {}
        if reqid: query = { 'request' : reqid }

        ## slice counts per request
        slicecounts = InputRequestList.objects.using('deft_adcr').filter(**query).values('request').annotate(Count('request'))   
        nsliced = {}
        for s in slicecounts:
            nsliced[s['request']] = s['request__count']
        for r in reqs:
            if r['reqid'] in nsliced:
                r['nslices'] = nsliced[r['reqid']]
            else:
                r['nslices'] = None

        ## requested event counts, from slice info, where not set to -1
        reqevents = InputRequestList.objects.using('deft_adcr').filter(**query).values('request').annotate(Sum('input_events'))
        nreqevd = {}
        for t in reqevents:
            if t['input_events__sum'] > 0:
                nreqevd[t['request']] = t['input_events__sum']
            else:
                if t['request'] in dsevents: nreqevd[t['request']] = dsevents[t['request']]
        for r in reqs:
            if r['reqid'] in nreqevd:
                nEvents = float(nreqevd[r['reqid']])/1000.
                r['nrequestedevents'] = nEvents
            else:
                r['nrequestedevents'] = None

        ## task counts
        taskcounts = ProductionTask.objects.using('deft_adcr').filter(**query).values('request','step__step_template__step','status').annotate(Count('status')).order_by('request','step__step_template__step','status')
        ntaskd = {}
        for t in taskcounts:
            if t['request'] not in ntaskd: ntaskd[t['request']] = {}
            if t['step__step_template__step'] not in ntaskd[t['request']]: ntaskd[t['request']][t['step__step_template__step']] = {}
            ntaskd[t['request']][t['step__step_template__step']][t['status']] = t['status__count']
        for r in reqs:
            if r['reqid'] in ntaskd:
                stepl = []
                for istep in ntaskd[r['reqid']]:
                    statel = []
                    for istate in ntaskd[r['reqid']][istep]:
                        statel.append([istate, ntaskd[r['reqid']][istep][istate]])
                    statel.sort()
                    stepl.append([istep, statel])
                stepl.sort()
                r['ntasks'] = stepl
            else:
                r['ntasks'] = None

        ## task event counts
        tequery = {}
        if reqid: tequery = { 'request' : reqid }
        taskevs = ProductionTask.objects.using('deft_adcr').filter(**tequery).values(*prodtask_fields).annotate(Sum('total_events'))

        ptasksuml = attSummaryDict(request, taskevs, prodtask_fields)

        taskevd = {}
        for t in taskevs:
            if t['campaign'].lower().startswith('mc') and t['campaign'].lower().find('valid') < 0:
                campaign = t['campaign'].lower()[:3]
            else:
                continue
            if campaign not in taskevd: taskevd[campaign] = {}
            if t['simulation_type'] not in taskevd[campaign]: taskevd[campaign]['simulation_type'] = 0
            taskevd[campaign]['simulation_type'] += t['total_events__sum']

        ## cloud and core count info from JEDI tasks
        tcquery = { 'prodsourcelabel' : 'managed' }
        if reqid: tcquery['reqid'] = reqid
        startdate = timezone.now() - timedelta(hours=30*24)
        startdate = startdate.strftime(defaultDatetimeFormat)
        tcquery['modificationtime__gte'] = startdate
        taskcounts = JediTasks.objects.filter(**tcquery).values('reqid','processingtype','cloud','corecount','superstatus').annotate(Count('superstatus')).order_by('reqid','processingtype','cloud','corecount','superstatus')
        ntaskd = {}
        for t in taskcounts:
            if t['reqid'] not in ntaskd: ntaskd[t['reqid']] = {}
            if t['processingtype'] not in ntaskd[t['reqid']]: ntaskd[t['reqid']][t['processingtype']] = {}
            if t['cloud'] not in ntaskd[t['reqid']][t['processingtype']]: ntaskd[t['reqid']][t['processingtype']][t['cloud']] = {}
            if t['corecount'] not in ntaskd[t['reqid']][t['processingtype']][t['cloud']]: ntaskd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']] = {}
            ntaskd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']][t['superstatus']] = t['superstatus__count']

        for r in reqs:
            if r['reqid'] in ntaskd:
                ## get the input totals for each step
                steptotd = {}
                for typ, tval in ntaskd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        if cloud in cloudtodo:
                            if typ not in steptotd: steptotd[typ] = 0
                            steptotd[typ] += cloudtodo[cloud]['nfiles']         
                ## build the table
                r['clouddist'] = ntaskd[r['reqid']]
                cdtxt = []
                for typ, tval in ntaskd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        if cloud in cloudtodo:
                            tobedone = cloudtodo[cloud]['nfiles'] - cloudtodo[cloud]['nfilesfinished']
                            failtxt = ""
                            todotxt = ""
                            progresstxt = ""
                            if cloud != '' and cloudtodo[cloud]['nfiles'] > 0:
                                if tobedone > 0:
                                    done = cloudtodo[cloud]['nfilesfinished']
                                    donepct = 100. * ( float(done) / float(cloudtodo[cloud]['nfiles']) )
                                    todotxt = "<td> %.0f%% &nbsp; <a href='/tasks/?reqid=%s&cloud=%s&processingtype=%s&days=90'>%s/%s</a> " % (donepct, r['reqid'], cloud, typ, done, cloudtodo[cloud]['nfiles'])
                                    width = int(200.*cloudtodo[cloud]['nfiles']/steptotd[typ])
                                    progresstxt = "</td><td width=210><progress style='width:%spx' max='100' value='%s'></progress>" % (width, donepct )
                                else:
                                    todotxt = "<td> done <a href='/tasks/?reqid=%s&cloud=%s&processingtype=%s&days=90'>%s/%s</a>" % ( r['reqid'], cloud, typ, cloudtodo[cloud]['nfilesfinished'], cloudtodo[cloud]['nfiles'])
                                    width = int(200.*cloudtodo[cloud]['nfiles']/steptotd[typ])
                                    progresstxt = "</td><td width=210><progress style='width:%spx' max='100' value='%s'></progress>" % (width, 100 )
                                if cloudtodo[cloud]['nfilesfailed'] > 0:
                                    failtxt = " &nbsp; <font color=red>%.0f%% fail (%s)</font>" % ( 100.*float(cloudtodo[cloud]['nfilesfailed'])/float(cloudtodo[cloud]['nfiles']), cloudtodo[cloud]['nfilesfailed'] )
                            if cloud != '':
                                txt = "<tr><td>%s</td><td>%s</td><!-- 2 -->%s  %s %s</td>" % ( typ, cloud, todotxt, failtxt, progresstxt)
                                txt += "<tr>"
                                cdtxt.append(txt)
                        for ncore, nval in cval.items():
                            txt = "<tr><td width=100>%s</td><td width=70>%s</td><!-- 1 --><td colspan=20> <b>%s-core: " % ( typ, cloud, ncore )
                            states = nval.keys()
                            states.sort()
                            for s in states:
                                txt += " &nbsp; <span class='%s'>%s</span>:%s" % ( s, s, nval[s] )
                            txt += "</b></td></tr>"
                            cdtxt.append(txt)
                cdtxt.sort()
                r['clouddisttxt'] = cdtxt
            else:
                r['clouddist'] = None

        ## cloud and core count event production info from PanDA jobs
        tcquery = { 'prodsourcelabel' : 'managed' }
        if reqid: tcquery['reqid'] = reqid
        jobcounts = Jobsarchived4.objects.filter(**tcquery).values('reqid','processingtype','cloud','corecount','jobstatus').annotate(Count('jobstatus')).annotate(Sum('nevents')).order_by('reqid','processingtype','cloud','corecount','jobstatus')
        njobd = {}
        for t in jobcounts:
            if t['reqid'] not in njobd: njobd[t['reqid']] = {}
            if t['processingtype'] not in njobd[t['reqid']]: njobd[t['reqid']][t['processingtype']] = {}
            if t['cloud'] not in njobd[t['reqid']][t['processingtype']]: njobd[t['reqid']][t['processingtype']][t['cloud']] = {}
            if t['corecount'] not in njobd[t['reqid']][t['processingtype']][t['cloud']]: njobd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']] = {}
            nev = float(t['nevents__sum'])/1000.
            njobd[t['reqid']][t['processingtype']][t['cloud']][t['corecount']][t['jobstatus']] = { 'events' : nev, 'jobs' : t['jobstatus__count'] }

        for r in reqs:
            if r['reqid'] in njobd:
                cdtxt = []
                for typ, tval in njobd[r['reqid']].items():
                    for cloud, cval in tval.items():
                        for ncore, nval in cval.items():
                            txt = "%s </td><td> %s </td><td> %s-core </td><td> " % ( typ, cloud, ncore )
                            states = nval.keys()
                            states.sort()
                            for s in states:
                                txt += " &nbsp; <span class='%s'>%s</span>:%sk evs (%s jobs)" % ( s, s, nval[s]['events'], nval[s]['jobs'] )
                            cdtxt.append(txt)
                cdtxt.sort()
                r['jobdisttxt'] = cdtxt

        ## processed event counts, from prodsys task info
        eventcounts = ProductionTask.objects.using('deft_adcr').filter(**query).exclude(status__in=['aborted','broken']).values('request','step__step_template__step').annotate(Sum('total_events'))
        ntaskd = {}
        for t in eventcounts:
            if t['request'] not in ntaskd: ntaskd[t['request']] = {}
            ntaskd[t['request']][t['step__step_template__step']] = t['total_events__sum']
        for r in reqs:
            if r['reqid'] in ntaskd:
                stepl = []
                for istep in ntaskd[r['reqid']]:
                    nEvents = float(ntaskd[r['reqid']][istep])/1000.
                    stepl.append([istep, nEvents ])
                stepl.sort()
                r['nprocessedevents'] = stepl
                if r['nrequestedevents'] and r['nrequestedevents'] > 0:
                    if 'completedevpct' not in r: r['completedevpct'] = []
                    for istep in ntaskd[r['reqid']]:
                        ndone = float(ntaskd[r['reqid']][istep])/1000.
                        nreq = r['nrequestedevents']
                        pct = int(ndone / nreq * 100.)
                        r['completedevpct'].append([ istep, pct ])
                    r['completedevpct'].sort()
            else:
                r['nprocessedevents'] = None

        ## processed event counts, from job info (last 3 days only)
        query = {}
        if reqid: query['reqid'] = reqid
        query['jobstatus'] = 'finished'
        query['prodsourcelabel'] = 'managed'
        values = [ 'reqid', 'processingtype' ]
        jobsum = Jobsarchived4.objects.filter(**query).values(*values).annotate(Sum('nevents')).order_by('reqid', 'processingtype')
        jobsumd = {}
        for j in jobsum:
            j['nevents__sum'] = float(j['nevents__sum'])/1000.
            if j['reqid'] not in jobsumd: jobsumd[j['reqid']] = []
            jobsumd[j['reqid']].append([ j['processingtype'], j['nevents__sum'] ])
        for r in reqs:
            if r['reqid'] in jobsumd:
                r['jobsumd'] = jobsumd[r['reqid']]
            else:
                r['jobsumd'] = None

    ## dataset search mode
    njeditasks = 0
    if dataset:
        if dataset.endswith('/'): dataset = dataset.strip('/')
        if dataset.find(':') >= 0:
            scope = dataset[:dataset.find(':')]
            dataset = dataset[dataset.find(':')+1:]
        mat = re.match('.*(_tid[0-9\_]+)$', dataset)
        if mat:
            tid = mat.group(1)
            dataset = dataset.replace(tid,'')        
            mat = re.match('_tid[0]*([0-9]+)', tid)
            if mat: tidnum = int(mat.group(1))

        wildcard = False
        if dataset.find('*') >= 0: wildcard = True

        jeditasks = []
        if not wildcard:
            fields = dataset.split('.')
            if len(fields) == 6:
                # try to interpret
                format = fields[5]
                taskname = '%s.%s.%s.%s.%s' % ( fields[0], fields[1], fields[2], fields[3], fields[5] )
                jeditasks = JediTasks.objects.filter(taskname=taskname,tasktype='prod').order_by('jeditaskid').values()
        else:
            extraCondition = "( %s )" % preprocessWildCardStringV2(dataset, 'taskname', JediTasks._meta.db_table)
            q = JediTasks.objects.extra(where=[extraCondition])
            jeditasks = q.values()

        if len(jeditasks) > 0:
            # get associated datasets
            tlist = []
            for t in jeditasks: tlist.append(t['jeditaskid'])
            dsquery = {}
            dsquery['jeditaskid__in'] = tlist
            dsets = JediDatasets.objects.filter(**dsquery).values()
            dsetd = {}
            for ds in dsets:
                if ds['type'] == 'pseudo_input': continue
                if ds['jeditaskid'] not in dsetd: dsetd[ds['jeditaskid']] = []
                dsetd[ds['jeditaskid']].append(ds)
            for t in jeditasks:
                if t['jeditaskid'] in dsetd:
                    dsetd[t['jeditaskid']] = sorted(dsetd[t['jeditaskid']], key=lambda x:x['datasetname'], reverse=True)
                    t['datasets'] = dsetd[t['jeditaskid']]
            # mark the tasks that have the exact dataset (modulo tid)
            for t in jeditasks:
                if not wildcard:
                    has_dataset = False
                    for ds in t['datasets']:
                        if ds['datasetname'].startswith(dataset):
                            has_dataset = True
                            njeditasks += 1
                else:
                    has_dataset = True
                    njeditasks += 1
                t['has_dataset'] = has_dataset

        if wildcard:
            extraCondition = "( %s )" % preprocessWildCardStringV2(dataset, 'name', ProductionDataset._meta.db_table)
            q = ProductionDataset.objects.using('deft_adcr').extra(where=[extraCondition])
            datasets = q.values()
        else:
            q = ProductionDataset.objects.using('deft_adcr').filter(name__startswith=dataset)
            datasets = q.values()
        if len(datasets) == 0:
            messages.info(request, "No matching prodsys datasets found")
        else:
            messages.info(request, "%s matching prodsys datasets found" % len(datasets))

        if len(datasets) > 0:
            # get production tasks associated with datasets
            tlist = []
            for ds in datasets:
                step = ds
                tlist.append(ds['task_id'])
            tquery = {}
            tquery['id__in'] = tlist
            ptasks = ProductionTask.objects.using('deft_adcr').filter(**tquery)
            taskd = {}
            for t in ptasks:
                taskd[t.id] = t
            for ds in datasets:
                if ds['task_id'] in taskd: ds['ptask'] = taskd[ds['task_id']]

        if wildcard:
            extraCondition = "( %s )" % preprocessWildCardStringV2(dataset, 'name', ProductionContainer._meta.db_table)
            containers = ProductionContainer.objects.using('deft_adcr').extra(where=[extraCondition]).values()        
        else:
            containers = ProductionContainer.objects.using('deft_adcr').filter(name__startswith=dataset).values()        
        if len(containers) == 0:
            messages.info(request, "No matching containers found")
        else:
            messages.info(request, "%s matching containers found" % len(containers))

        dsslices = InputRequestList.objects.using('deft_adcr').filter(dataset__name__startswith=dataset).values()
        if len(dsslices) == 0:
            pass # messages.info(request, "No slices using this dataset found")
        else:
            messages.info(request, "%s slices found" % len(dsslices))


        if wildcard:
            extraCondition = "( %s )" % preprocessWildCardStringV2(dataset, 'datasetname', JediDatasets._meta.db_table)
            dsquery = {}
            q = JediDatasets.objects.extra(where=[extraCondition])
            jedidatasets = q.order_by('jeditaskid').values()
        else:
            q = JediDatasets.objects.filter(datasetname__startswith=dataset)
            jedidatasets = q.order_by('jeditaskid').values()
        if len(jedidatasets) == 0:
            messages.info(request, "No matching JEDI datasets found")
        else:
            messages.info(request, "%s matching JEDI datasets found" % len(jedidatasets))

        ## check for jobs with output destined for this dataset
        files = []
        query = { 'dataset' : dataset }
        if 'pandaid' in request.session['requestParams']: query['pandaid'] = request.session['requestParams']['pandaid']
        #files.extend(Filestable4.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #files.extend(FilestableArch.objects.filter(dataset=dataset,type='output').order_by('pandaid').values())
        #if len(files) == 0: messages.info(request, "No PanDA jobs creating files for this dataset found")

    reqsuml = attSummaryDict(request, reqs, req_fields)
    showfields = list(jeditask_fields)
    showfields.remove('jeditaskid')
    jtasksuml = attSummaryDict(request, jeditasks, showfields)

    runjobs = queuejobs = None
    totjobs = totqjobs = None
    totcpu = None
    sumjobs = cpujobs = sumevents = projevents = None
    sumjobsl = cpujobsl = sumeventsl = projeventsl = None
    finalstates = [ 'finished', 'failed', 'cancelled' ]
    cpupct = jobpct = {}
    cpu_total = job_total = totrunjobs = 0
    recentjobs = []
    totevents = 0
    if mode == 'processing':
        query = {}
        query['reqid__gte'] = 920
        reqinfo = TRequest.objects.using('deft_adcr').filter(**query).order_by('reqid').reverse().values()
        reqinfod = {}
        for r in reqinfo:
            reqinfod[r['reqid']] = r

        ## queued jobs
        totqjobs = 0
        ## jobs queued, all pre-run stages
        queuejobs = []
        queuejobs.extend(Jobsdefined4.objects.filter(prodsourcelabel='managed').values('reqid').annotate(Count('reqid')).order_by('reqid'))
        queuejobs.extend(Jobswaiting4.objects.filter(prodsourcelabel='managed').values('reqid').annotate(Count('reqid')).order_by('reqid'))
        queuejobs.extend(Jobsactive4.objects.filter(prodsourcelabel='managed')\
        .exclude(jobstatus__in=['running','transferring','holding','starting']).values('reqid').annotate(Count('reqid')).order_by('reqid'))

        for r in queuejobs:
            totqjobs += r['reqid__count']
        queuejobs = sorted(queuejobs, key=lambda x:x['reqid__count'], reverse=True)
        for r in queuejobs:
            r['fraction'] = 100.*r['reqid__count']/totqjobs
            if r['reqid'] in reqinfod: r['reqdata'] = reqinfod[r['reqid']]

        ## running jobs
        ## job slot usage by request
        runjobs = Jobsactive4.objects.filter(jobstatus='running',prodsourcelabel='managed').values('reqid').annotate(Count('reqid')).order_by('reqid')
        for r in runjobs:
            totrunjobs += r['reqid__count']
        runjobs = sorted(runjobs, key=lambda x:x['reqid__count'], reverse=True)
        for r in runjobs:
            r['fraction'] = 100.*r['reqid__count']/totrunjobs
            if r['reqid'] in reqinfod: r['reqdata'] = reqinfod[r['reqid']]

        ## recent jobs
        sumjobs = {}
        cpujobs = {}
        totjobs = {}
        totcpu = {}
        sumevents = {}
        projevents = {}
        ## recent jobs by count and walltime
        recentjobs = Jobsarchived4.objects.filter(prodsourcelabel='managed').values('reqid','jobstatus').annotate(Count('jobstatus')).annotate(Sum('cpuconsumptiontime')).annotate(Sum('nevents')).order_by('reqid','jobstatus')
        for r in recentjobs:
            if r['jobstatus'] == 'finished':
                if r['reqid'] not in sumevents:
                    sumevents[r['reqid']] = {}
                    sumevents[r['reqid']]['nevents'] = 0
                sumevents[r['reqid']]['nevents'] += r['nevents__sum']
                totevents += r['nevents__sum']
            if r['jobstatus'] not in sumjobs:
                sumjobs[r['jobstatus']] = []
                cpujobs[r['jobstatus']] = []
                totjobs[r['jobstatus']] = 0
                totcpu[r['jobstatus']] = 0
            sumjobs[r['jobstatus']].append(r)
            cpujobs[r['jobstatus']].append(r)
            totjobs[r['jobstatus']] += r['jobstatus__count']
            totcpu[r['jobstatus']] += r['cpuconsumptiontime__sum']

        projeventstot = 0
        for r in sumevents:
            sumevents[r]['reqid'] = r
            sumevents[r]['fraction'] = int(100.*sumevents[r]['nevents']/totevents)
            sumevents[r]['nevents'] = int(sumevents[r]['nevents']/1000)
            if r in reqinfod:
                sumevents[r]['reqdata'] = reqinfod[r]
                project = reqinfod[r]['project_id']
                if project not in projevents:
                    projevents[project] = {}
                    projevents[project]['nevents'] = 0
                projevents[project]['nevents'] += sumevents[r]['nevents']
                projeventstot += sumevents[r]['nevents']

        for p in projevents:
            projevents[p]['project'] = p
            projevents[p]['fraction'] = int(100.*projevents[p]['nevents']/projeventstot)

        projeventsl = []
        for p in projevents:
            projeventsl.append(projevents[p])
        projeventsl = sorted(projeventsl, key=lambda x:x['nevents'], reverse=True)

        cpusum = {}
        jobsum = {}
        for s in sumjobs:
            cpusum[s] = 0
            jobsum[s] = 0
            for r in sumjobs[s]:
                cpusum[s] += r['cpuconsumptiontime__sum']
                jobsum[s] += r['jobstatus__count']
                cpu_total += r['cpuconsumptiontime__sum']
                job_total += r['jobstatus__count']
                r['fraction'] = 100.*r['jobstatus__count']/totjobs[s]
                r['cpufraction'] = 100.*r['cpuconsumptiontime__sum']/totcpu[s]
                if r['reqid'] in reqinfod: r['reqdata'] = reqinfod[r['reqid']]
        cpusum['all'] = 0
        jobsum['all'] = 0
        for s in sumjobs:
            cpusum['all'] += cpusum[s]
            jobsum['all'] += jobsum[s]
        for s in sumjobs:
            sumjobs[s] = sorted(sumjobs[s], key=lambda x:x['jobstatus__count'], reverse=True)
            cpujobs[s] = sorted(cpujobs[s], key=lambda x:x['cpuconsumptiontime__sum'], reverse=True)
            cpupct[s] = int(100.*cpusum[s]/cpu_total)
            jobpct[s] = int(100.*jobsum[s]/job_total)
        sumjobsl = []
        cpujobsl = []
        for s in finalstates:
            sumjobsl.append({ 'status' : s, 'recs' : sumjobs[s] })
            cpujobsl.append({ 'status' : s, 'recs' : cpujobs[s] })
        sumeventsl = []
        for s in sumevents:
            sumeventsl.append(sumevents[s])
        sumeventsl = sorted(sumeventsl, key=lambda x:x['nevents'], reverse=True)

    if events_processed:
        # Convert from dict to ordered list
        evkeys = events_processed.keys()
        evkeys.sort()
        evpl = []
        for e in evkeys:
            evpl.append([e, float(events_processed[e])/1000.])
        events_processed = evpl

    if 'sortby' in request.session['requestParams']:
        if reqs:
            if request.session['requestParams']['sortby'] == 'reqid':
                reqs = sorted(reqs, key=lambda x:x['reqid'], reverse=True)
            if request.session['requestParams']['sortby'] == 'timestamp':
                reqs = sorted(reqs, key=lambda x:x['timestamp'], reverse=True)
    if len(reqs) > 0 and 'info_fields' in reqs[0] and reqs[0]['info_fields']:
        info_fields = json.loads(reqs[0]['info_fields'])
    else:
        info_fields = None
    if len(reqs) > 0:
        req = reqs[0]
    else:
        req = None
    xurl = coreviews.extensibleURL(request)
    nosorturl = coreviews.removeParam(xurl, 'sortby',mode='extensible')
    data = {
        'viewParams' : viewParams,
        'xurl' : xurl,
        'nosorturl' : nosorturl,
        'mode' : mode,
        'reqid' : reqid,
        'dataset' : dataset,
        'thisProject' : thisProject,
        'projects' : projectd,
        'request' : req,
        'info_fields' : info_fields,
        'requests' : reqs,
        'reqsuml' : reqsuml,
        'jtasksuml' : jtasksuml,
        'ptasksuml' : ptasksuml,
        'datasets' : datasets,
        'jedidatasets' : jedidatasets,
        'jeditasks' : jeditasks,
        'njeditasks' : njeditasks,
        'containers' : containers,
        'tasks' : tasks,
        'steps' : steps,
        'slices' : slices,
        'files' : files,
        'dataset_form' : dataset_form,
        'events_processed' : events_processed,
        'request_columns' : request_columns,
        'jobsum' : jobsum,
        'jobsumd' : jobsumd,
        'dsslices' : dsslices,
        'scope' : scope,
        'tid' : tid,
        'tidnum' : tidnum,
        'totalfiles' : totalfiles,
        'runjobs' : runjobs,
        'totrunjobs' : totrunjobs,
        'recentjobs' : recentjobs,
        'totjobs' : totjobs,
        'totcpu' : totcpu,
        'sumjobs' : sumjobsl,
        'cpujobs' : cpujobsl,
        'cpupct' : cpupct,
        'jobpct' : jobpct,
        'finalstates' : finalstates,
        'cpu_total' : cpu_total,
        'job_total' : job_total,
        'queuejobs' : queuejobs,
        'totqjobs' : totqjobs,
        'show_form' : show_form,
        'sumeventsl' : sumeventsl,
        'projeventsl' : projeventsl,
    }
    response = render_to_response('dpMain.html', data, RequestContext(request))
    patch_response_headers(response, cache_timeout=request.session['max_age_minutes']*60)
    return response

def attSummaryDict(request, reqs, flist):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    sumd = {}
    
    for req in reqs:
        for f in flist:
            if f in req and req[f]:
                if not f in sumd: sumd[f] = {}
                if not req[f] in sumd[f]: sumd[f][req[f]] = 0
                sumd[f][req[f]] += 1

    ## convert to ordered lists
    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        kys.sort()
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[f][ky] })
        if 'sortby' in request.GET and request.GET['sortby'] == 'count':
            iteml = sorted(iteml, key=lambda x:x['kvalue'], reverse=True)
        else:
            iteml = sorted(iteml, key=lambda x:str(x['kname']).lower())
        itemd['list'] = iteml
        suml.append(itemd)
        suml = sorted(suml, key=lambda x:x['field'])
    return suml

class DatasetForm(forms.Form):
    #class Media:
    #    css = {"all": ("app.css",)}

    #error_css_class = 'error'
    #required_css_class = 'required'

    def clean_entname(self):
        exists, txt = checkExistingTag(self.cleaned_data['dataset'])
        return self.cleaned_data['dataset']

    type = forms.ChoiceField(label='Entry type', widget=forms.HiddenInput(), choices=entitytypes, initial='dataset' )

    dataset = forms.CharField(label='Dataset', max_length=250, \
            help_text="Enter dataset or container name. Use * for wildcard")

def preprocessWildCardStringV2(strToProcess, fieldToLookAt, tablename):
    if (len(strToProcess)==0):
        return '(1=1)'
    #strToProcess = strToProcess.replace('_','\_')
    cardParametersRaw = strToProcess.split('*')
    cardRealParameters = [s for s in cardParametersRaw if len(s) > 1]
    countRealParameters = len(cardRealParameters)
    countParameters = len(cardParametersRaw)

    if (countParameters==0):
        return '(1=1)'
    currentRealParCount = 0
    currentParCount = 0
    extraQueryString = '('
    
    for parameter in cardParametersRaw:
        leadStar = False
        trailStar = False
        if len(parameter) > 0:
            
            if (currentParCount-1 >= 0):
#                if len(cardParametersRaw[currentParCount-1]) == 0:
                leadStar = True

            if (currentParCount+1 < countParameters):
#                if len(cardParametersRaw[currentParCount+1]) == 0:
                trailStar = True

            if fieldToLookAt.lower() == 'PRODUSERID':
                leadStar = True
                trailStar = True


            if (leadStar and trailStar):
                extraQueryString += '( "'+tablename.upper()+'"."'+fieldToLookAt.upper()+'"  LIKE TRANSLATE(\'%%' + parameter +'%%\' USING NCHAR_CS)   )'

            elif ( not leadStar and not trailStar):
                extraQueryString += '( "'+tablename.upper()+'"."'+fieldToLookAt.upper()+'"  LIKE TRANSLATE(\'' + parameter +'%%\' USING NCHAR_CS) )'

            elif (leadStar and not trailStar):
                extraQueryString += '( "'+tablename.upper()+'"."'+fieldToLookAt.upper()+'"  LIKE TRANSLATE(\'%%' + parameter +'%%\' USING NCHAR_CS) )'
                
            elif (not leadStar and trailStar):
                extraQueryString += '( "'+tablename.upper()+'"."'+fieldToLookAt.upper()+'"  LIKE TRANSLATE(\'' + parameter +'%%\' USING NCHAR_CS) )'

            currentRealParCount+=1
            if currentRealParCount < countRealParameters:
                extraQueryString += ' AND '
        currentParCount+=1
    extraQueryString += ")"
    return extraQueryString
