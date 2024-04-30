"""
Created on 06.04.2018
:author Tatiana Korchuganova
Here are the models for runningprod views
"""


from __future__ import unicode_literals
from django.db import models
from django.conf import settings


class RunningProdTasksModel(models.Model):
    campaign = models.CharField(max_length=72, db_column='campaign', blank=True)
    reqid = models.IntegerField(null=True, db_column='reqid', blank=True)
    runnumber = models.IntegerField(null=True, db_column='runnumber', blank=True)
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    rjobs = models.IntegerField(null=True, db_column='rjobs', blank=True)
    slots = models.IntegerField(null=True, db_column='slots', blank=True)
    aslots = models.IntegerField(null=True, db_column='aslots', blank=True)
    status = models.CharField(max_length=192, db_column='status')
    superstatus = models.CharField(max_length=192, db_column='superstatus')
    neventstobeused = models.BigIntegerField(db_column='neventstobeused')
    neventsused = models.BigIntegerField(db_column='neventsused')
    nevents = models.BigIntegerField(db_column='nevents')
    percentage = models.BigIntegerField(db_column='percentage')
    nfilesfailed = models.IntegerField(null=True, db_column='nfilesfailed', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='workinggroup', blank=True)
    priority = models.IntegerField(null=True, db_column='currentpriority', blank=True)
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True)
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True)
    age = models.IntegerField(db_column='age', blank=True)
    creationdate = models.DateTimeField(db_column='creationdate')
    username = models.CharField(max_length=384, db_column='username')
    cputime = models.IntegerField(null=True,db_column='cputime', blank=True)
    site = models.CharField(max_length=384, db_column='site', blank=True)
    outputdatasettype = models.CharField(max_length=384, db_column='outputdatasettype')
    ptag = models.CharField(max_length=72, db_column='ptag', blank=True)
    atag = models.CharField(max_length=72, db_column='atag', blank=True)
    rtag = models.CharField(max_length=72, db_column='rtag', blank=True)
    simtype = models.CharField(max_length=72, db_column='simtype', blank=True)
    gshare = models.CharField(max_length=72, db_column='gshare', blank=True)
    hashtags = models.CharField(max_length=400, db_column='hashtags', blank=True)
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)
    neventsrunning = models.BigIntegerField(db_column='nrunningevents')
    neventsfinished = models.BigIntegerField(db_column='nfinishedevents')
    neventsfailed = models.BigIntegerField(db_column='nfailedevents')
    neventswaiting = models.BigIntegerField(db_column='nwaitingevents')
    jumbo = models.IntegerField(null=True, db_column='jumbo', blank=True)
    container_name = models.BigIntegerField(db_column='container_name')
    stepid = models.IntegerField(null=True, db_column='stepid', blank=True)
    sliceid = models.IntegerField(null=True, db_column='sliceid', blank=True)
    scope = models.CharField(max_length=256, db_column='scope')
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."runningprodtasks"'


class FrozenProdTasksModel(models.Model):
    campaign = models.CharField(max_length=72, db_column='campaign', blank=True)
    reqid = models.IntegerField(null=True, db_column='reqid', blank=True)
    runnumber = models.IntegerField(null=True, db_column='runnumber', blank=True)
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    rjobs = models.IntegerField(null=True, db_column='rjobs', blank=True)
    slots = models.IntegerField(null=True, db_column='slots', blank=True)
    aslots = models.IntegerField(null=True, db_column='aslots', blank=True)
    status = models.CharField(max_length=192, db_column='status')
    superstatus = models.CharField(max_length=192, db_column='superstatus')
    neventstobeused = models.BigIntegerField(db_column='neventstobeused')
    neventsused = models.BigIntegerField(db_column='neventsused')
    nevents = models.BigIntegerField(db_column='nevents')
    percentage = models.BigIntegerField(db_column='percentage')
    nfilesfailed = models.IntegerField(null=True, db_column='nfilesfailed', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='workinggroup', blank=True)
    priority = models.IntegerField(null=True, db_column='currentpriority', blank=True)
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True)
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True)
    creationdate = models.DateTimeField(db_column='creationdate')
    username = models.CharField(max_length=384, db_column='username')
    cputime = models.IntegerField(null=True,db_column='cputime', blank=True)
    site = models.CharField(max_length=384, db_column='site', blank=True)
    outputdatasettype = models.CharField(max_length=384, db_column='outputdatasettype')
    ptag = models.CharField(max_length=72, db_column='ptag', blank=True)
    atag = models.CharField(max_length=72, db_column='atag', blank=True)
    rtag = models.CharField(max_length=72, db_column='rtag', blank=True)
    simtype = models.CharField(max_length=72, db_column='simtype', blank=True)
    gshare = models.CharField(max_length=72, db_column='gshare', blank=True)
    hashtags = models.CharField(max_length=400, db_column='hashtags', blank=True)
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)
    neventsrunning = models.BigIntegerField(db_column='nrunningevents')
    neventsfinished = models.BigIntegerField(db_column='nfinishedevents')
    neventsfailed = models.BigIntegerField(db_column='nfailedevents')
    neventswaiting = models.BigIntegerField(db_column='nwaitingevents')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jumbo = models.IntegerField(null=True, db_column='jumbo', blank=True)
    container_name = models.BigIntegerField(db_column='container_name')
    stepid = models.IntegerField(null=True, db_column='stepid', blank=True)
    sliceid = models.IntegerField(null=True, db_column='sliceid', blank=True)
    scope = models.CharField(max_length=256, db_column='scope')
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."frozenprodtasks"'


class ProdNeventsHistory(models.Model):
    id = models.IntegerField(db_column='id', primary_key=True)
    processingtype = models.CharField(max_length=192, db_column='processing_type')
    timestamp = models.DateTimeField(db_column='timestamp')
    neventsused = models.BigIntegerField(db_column='nevents_used')
    neventswaiting = models.BigIntegerField(db_column='nevents_waiting')
    neventstotal = models.BigIntegerField(db_column='nevents_total')
    neventsrunning = models.BigIntegerField(db_column='nevents_running')
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."prod_nevents_history"'
