"""
Created on 06.04.2018
:author Tatiana Korchuganova
Here are the models for runningprod views
"""


from __future__ import unicode_literals
from django.db import models


class RunningProdTasksModel(models.Model):
    campaign = models.CharField(max_length=72, db_column='CAMPAIGN', blank=True)
    reqid = models.IntegerField(null=True, db_column='REQID', blank=True)
    runnumber = models.IntegerField(null=True, db_column='RUNNUMBER', blank=True)
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    rjobs = models.IntegerField(null=True, db_column='RJOBS', blank=True)
    slots = models.IntegerField(null=True, db_column='SLOTS', blank=True)
    aslots = models.IntegerField(null=True, db_column='ASLOTS', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS')
    superstatus = models.CharField(max_length=192, db_column='SUPERSTATUS')
    neventstobeused = models.BigIntegerField(db_column='NEVENTSTOBEUSED')
    neventsused = models.BigIntegerField(db_column='NEVENTSUSED')
    nevents = models.BigIntegerField(db_column='NEVENTS')
    percentage = models.BigIntegerField(db_column='PERCENTAGE')
    nfilesfailed = models.IntegerField(null=True, db_column='NFILESFAILED', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='WORKINGGROUP', blank=True)
    priority = models.IntegerField(null=True, db_column='CURRENTPRIORITY', blank=True)
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True)
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)
    age = models.IntegerField(db_column='AGE', blank=True)
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    username = models.CharField(max_length=384, db_column='USERNAME')
    cputime = models.IntegerField(null=True,db_column='CPUTIME', blank=True)
    site = models.CharField(max_length=384, db_column='SITE', blank=True)
    outputdatasettype = models.CharField(max_length=384, db_column='OUTPUTDATASETTYPE')
    ptag = models.CharField(max_length=72, db_column='PTAG', blank=True)
    simtype = models.CharField(max_length=72, db_column='SIMTYPE', blank=True)
    gshare = models.CharField(max_length=72, db_column='GSHARE', blank=True)
    hashtags = models.CharField(max_length=400, db_column='HASHTAGS', blank=True)
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True)
    neventsrunning = models.BigIntegerField(db_column='NRUNNINGEVENTS')
    jumbo = models.IntegerField(null=True, db_column='JUMBO', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."RUNNINGPRODTASKS"'


class FrozenProdTasksModel(models.Model):
    campaign = models.CharField(max_length=72, db_column='CAMPAIGN', blank=True)
    reqid = models.IntegerField(null=True, db_column='REQID', blank=True)
    runnumber = models.IntegerField(null=True, db_column='RUNNUMBER', blank=True)
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    rjobs = models.IntegerField(null=True, db_column='RJOBS', blank=True)
    slots = models.IntegerField(null=True, db_column='SLOTS', blank=True)
    aslots = models.IntegerField(null=True, db_column='ASLOTS', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS')
    superstatus = models.CharField(max_length=192, db_column='SUPERSTATUS')
    neventstobeused = models.BigIntegerField(db_column='NEVENTSTOBEUSED')
    neventsused = models.BigIntegerField(db_column='NEVENTSUSED')
    nevents = models.BigIntegerField(db_column='NEVENTS')
    percentage = models.BigIntegerField(db_column='PERCENTAGE')
    nfilesfailed = models.IntegerField(null=True, db_column='NFILESFAILED', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='WORKINGGROUP', blank=True)
    priority = models.IntegerField(null=True, db_column='CURRENTPRIORITY', blank=True)
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True)
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    username = models.CharField(max_length=384, db_column='USERNAME')
    cputime = models.IntegerField(null=True,db_column='CPUTIME', blank=True)
    site = models.CharField(max_length=384, db_column='SITE', blank=True)
    outputdatasettype = models.CharField(max_length=384, db_column='OUTPUTDATASETTYPE')
    ptag = models.CharField(max_length=72, db_column='PTAG', blank=True)
    simtype = models.CharField(max_length=72, db_column='SIMTYPE', blank=True)
    gshare = models.CharField(max_length=72, db_column='GSHARE', blank=True)
    hashtags = models.CharField(max_length=400, db_column='HASHTAGS', blank=True)
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jumbo = models.IntegerField(null=True, db_column='JUMBO', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."FROZENPRODTASKS"'


class RunningProdRequestsModel(models.Model):
    reqid = models.IntegerField(db_column='REQID', primary_key=True)
    ntasks = models.IntegerField(null=True, db_column='NTASKS', blank=True)
    rjobs = models.IntegerField(null=True, db_column='RJOBS', blank=True)
    slots = models.IntegerField(null=True, db_column='SLOTS', blank=True)
    aslots = models.IntegerField(null=True, db_column='ASLOTS', blank=True)
    neventstobeused = models.BigIntegerField(db_column='NEVENTSTOBEUSED')
    neventsrunning = models.BigIntegerField(db_column='NEVENTSRUNNING')
    neventsused = models.BigIntegerField(db_column='NEVENTSUSED')
    nevents = models.BigIntegerField(db_column='NEVENTS')
    percentage = models.BigIntegerField(db_column='PERCENTAGE')
    age = models.IntegerField(db_column='AGE', blank=True)
    nfilesfailed = models.IntegerField(null=True, db_column='NFILESFAILED', blank=True)
    avgpriority = models.IntegerField(null=True, db_column='AVGPRIORITY', blank=True)
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    cputime = models.IntegerField(null=True,db_column='CPUTIME', blank=True)
    status = models.CharField(max_length=32, db_column='STATUS', null=False, blank=True)
    provenance = models.CharField(max_length=32, db_column='PROVENANCE', null=False, blank=True)
    requesttype = models.CharField(max_length=32, db_column='REQUEST_TYPE', null=False, blank=True)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False, blank=True)
    subcampaign = models.CharField(max_length=32, db_column='SUB_CAMPAIGN', null=False, blank=True)
    physgroup = models.CharField(max_length=20, db_column='PHYS_GROUP', null=False, blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."RUNNINGPRODREQUESTS"'


class ProdNeventsHistory(models.Model):
    id = models.IntegerField(null=True, db_column='ID', primary_key=True)
    processingtype = models.CharField(max_length=192, db_column='PROCESSING_TYPE')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    neventsused = models.BigIntegerField(db_column='NEVENTS_USED')
    neventswaiting = models.BigIntegerField(db_column='NEVENTS_WAITING')
    neventstotal = models.BigIntegerField(db_column='NEVENTS_TOTAL')
    neventsrunning = models.BigIntegerField(db_column='NEVENTS_RUNNING')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."PROD_NEVENTS_HISTORY"'