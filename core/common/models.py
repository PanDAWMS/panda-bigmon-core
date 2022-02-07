# Create your models here.
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.

from __future__ import unicode_literals

from ..pandajob.columns_config import COLUMNS, ORDER_COLUMNS, COL_TITLES, FILTERS

import json
from django.core.exceptions import ObjectDoesNotExist
from django.db import connections
from django.utils import timezone
from django.db import models

from core.settings.config import DB_SCHEMA, DEPLOYMENT, DB_SCHEMA_PANDA, DB_SCHEMA_PANDA_ARCH

models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)


class Cloudconfig(models.Model):
    name = models.CharField(max_length=60, primary_key=True, db_column='NAME')
    description = models.CharField(max_length=150, db_column='DESCRIPTION')
    tier1 = models.CharField(max_length=60, db_column='TIER1')
    tier1se = models.CharField(max_length=1200, db_column='TIER1SE')
    relocation = models.CharField(max_length=30, db_column='RELOCATION', blank=True)
    weight = models.IntegerField(db_column='WEIGHT')
    server = models.CharField(max_length=300, db_column='SERVER')
    status = models.CharField(max_length=60, db_column='STATUS')
    transtimelo = models.IntegerField(db_column='TRANSTIMELO')
    transtimehi = models.IntegerField(db_column='TRANSTIMEHI')
    waittime = models.IntegerField(db_column='WAITTIME')
    comment_field = models.CharField(max_length=600, db_column='COMMENT_', blank=True)  # Field renamed because it was a Python reserved word.
    space = models.IntegerField(db_column='SPACE')
    moduser = models.CharField(max_length=90, db_column='MODUSER', blank=True)
    modtime = models.DateTimeField(db_column='MODTIME')
    validation = models.CharField(max_length=60, db_column='VALIDATION', blank=True)
    mcshare = models.IntegerField(db_column='MCSHARE')
    countries = models.CharField(max_length=240, db_column='COUNTRIES', blank=True)
    fasttrack = models.CharField(max_length=60, db_column='FASTTRACK', blank=True)
    nprestage = models.BigIntegerField(db_column='NPRESTAGE')
    pilotowners = models.CharField(max_length=900, db_column='PILOTOWNERS', blank=True)
    dn = models.CharField(max_length=300, db_column='DN', blank=True)
    email = models.CharField(max_length=180, db_column='EMAIL', blank=True)
    fairshare = models.CharField(max_length=384, db_column='FAIRSHARE', blank=True)
    class Meta:
        db_table = u'cloudconfig'
        app_label = 'panda'


class Cloudtasks(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    taskname = models.CharField(max_length=384, db_column='TASKNAME', blank=True)
    taskid = models.IntegerField(null=True, db_column='TASKID', blank=True)
    cloud = models.CharField(max_length=60, db_column='CLOUD', blank=True)
    status = models.CharField(max_length=60, db_column='STATUS', blank=True)
    tmod = models.DateTimeField(db_column='TMOD')
    tenter = models.DateTimeField(db_column='TENTER')
    class Meta:
        db_table = u'cloudtasks'


class Datasets(models.Model):
    vuid = models.CharField(max_length=120, db_column='VUID', primary_key=True)
    name = models.CharField(max_length=765, db_column='NAME')
    version = models.CharField(max_length=30, db_column='VERSION', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE')
    status = models.CharField(max_length=30, db_column='STATUS', blank=True)
    numberfiles = models.IntegerField(null=True, db_column='NUMBERFILES', blank=True)
    currentfiles = models.IntegerField(null=True, db_column='CURRENTFILES', blank=True)
    creationdate = models.DateTimeField(null=True, db_column='CREATIONDATE', blank=True)
    modificationdate = models.DateTimeField(db_column='MODIFICATIONDATE')
    moverid = models.BigIntegerField(db_column='MOVERID')
    transferstatus = models.IntegerField(db_column='TRANSFERSTATUS')
    subtype = models.CharField(max_length=15, db_column='SUBTYPE', blank=True)
    class Meta:
        db_table = u'datasets'
        unique_together = ('vuid', 'modificationdate')
        app_label = 'panda'


class Filestable4(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    guid = models.CharField(max_length=192, db_column='GUID', blank=True)
    lfn = models.CharField(max_length=768, db_column='LFN', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE', blank=True)
    dataset = models.CharField(max_length=765, db_column='DATASET', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS', blank=True)
    proddblock = models.CharField(max_length=765, db_column='PRODDBLOCK', blank=True)
    proddblocktoken = models.CharField(max_length=750, db_column='PRODDBLOCKTOKEN', blank=True)
    dispatchdblock = models.CharField(max_length=765, db_column='DISPATCHDBLOCK', blank=True)
    dispatchdblocktoken = models.CharField(max_length=750, db_column='DISPATCHDBLOCKTOKEN', blank=True)
    destinationdblock = models.CharField(max_length=765, db_column='DESTINATIONDBLOCK', blank=True)
    destinationdblocktoken = models.CharField(max_length=750, db_column='DESTINATIONDBLOCKTOKEN', blank=True)
    destinationse = models.CharField(max_length=750, db_column='DESTINATIONSE', blank=True)
    fsize = models.BigIntegerField(db_column='FSIZE')
    md5sum = models.CharField(max_length=108, db_column='MD5SUM', blank=True)
    checksum = models.CharField(max_length=108, db_column='CHECKSUM', blank=True)
    scope = models.CharField(max_length=90, db_column='SCOPE', blank=True)
    jeditaskid = models.BigIntegerField(null=True, db_column='JEDITASKID', blank=True)
    datasetid = models.BigIntegerField(null=True, db_column='DATASETID', blank=True)
    fileid = models.BigIntegerField(null=True, db_column='FILEID', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='ATTEMPTNR', blank=True)
    class Meta:
        db_table = u'filestable4'
        unique_together = ('row_id', 'modificationtime')
        app_label = 'panda'


class FilestableArch(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID') 
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    creationtime = models.DateTimeField(db_column='CREATIONTIME')
    guid = models.CharField(max_length=64, db_column='GUID', blank=True) 
    lfn = models.CharField(max_length=256, db_column='LFN', blank=True) 
    type = models.CharField(max_length=20, db_column='TYPE', blank=True) 
    dataset = models.CharField(max_length=255, db_column='DATASET', blank=True) 
    status = models.CharField(max_length=64, db_column='STATUS', blank=True) 
    proddblock = models.CharField(max_length=255, db_column='PRODDBLOCK', blank=True) 
    proddblocktoken = models.CharField(max_length=250, db_column='PRODDBLOCKTOKEN', blank=True) 
    dispatchdblock = models.CharField(max_length=265, db_column='DISPATCHDBLOCK', blank=True) 
    dispatchdblocktoken = models.CharField(max_length=250, db_column='DISPATCHDBLOCKTOKEN', blank=True) 
    destinationdblock = models.CharField(max_length=265, db_column='DESTINATIONDBLOCK', blank=True) 
    destinationdblocktoken = models.CharField(max_length=250, db_column='DESTINATIONDBLOCKTOKEN', blank=True) 
    destinationse = models.CharField(max_length=250, db_column='DESTINATIONSE', blank=True) 
    fsize = models.BigIntegerField(db_column='FSIZE') 
    md5sum = models.CharField(max_length=40, db_column='MD5SUM', blank=True) 
    checksum = models.CharField(max_length=40, db_column='CHECKSUM', blank=True) 
    scope = models.CharField(max_length=30, db_column='SCOPE', blank=True) 
    jeditaskid = models.BigIntegerField(null=True, db_column='JEDITASKID', blank=True) 
    datasetid = models.BigIntegerField(null=True, db_column='DATASETID', blank=True) 
    fileid = models.BigIntegerField(null=True, db_column='FILEID', blank=True) 
    attemptnr = models.IntegerField(null=True, db_column='ATTEMPTNR', blank=True) 

    class Meta:
        db_table = u'filestable_arch'
        unique_together = ('row_id', 'modificationtime')
        app_label = 'panda'


class History(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    entrytime = models.DateTimeField(db_column='ENTRYTIME')
    starttime = models.DateTimeField(db_column='STARTTIME')
    endtime = models.DateTimeField(db_column='ENDTIME')
    cpu = models.BigIntegerField(null=True, db_column='CPU', blank=True)
    cpuxp = models.BigIntegerField(null=True, db_column='CPUXP', blank=True)
    space = models.IntegerField(null=True, db_column='SPACE', blank=True)
    class Meta:
        db_table = u'history'


class Incidents(models.Model):
    at_time = models.DateTimeField(primary_key=True, db_column='AT_TIME')
    typekey = models.CharField(max_length=60, db_column='TYPEKEY', blank=True)
    description = models.CharField(max_length=600, db_column='DESCRIPTION', blank=True)
    class Meta:
        db_table = u'incidents'
        app_label = 'panda'


class JediDatasetContents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID')
    fileid = models.BigIntegerField(db_column='FILEID')
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    lastattempttime = models.DateTimeField(null=True, db_column='LASTATTEMPTTIME', blank=True)
    lfn = models.CharField(max_length=768, db_column='LFN')
    guid = models.CharField(max_length=192, db_column='GUID', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE')
    status = models.CharField(max_length=192, db_column='STATUS')
    fsize = models.BigIntegerField(null=True, db_column='FSIZE', blank=True)
    checksum = models.CharField(max_length=108, db_column='CHECKSUM', blank=True)
    scope = models.CharField(max_length=90, db_column='SCOPE', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='ATTEMPTNR', blank=True)
    maxattempt = models.IntegerField(null=True, db_column='MAXATTEMPT', blank=True)
    nevents = models.IntegerField(null=True, db_column='NEVENTS', blank=True)
    keeptrack = models.IntegerField(null=True, db_column='KEEPTRACK', blank=True)
    startevent = models.IntegerField(null=True, db_column='STARTEVENT', blank=True)
    endevent = models.IntegerField(null=True, db_column='ENDEVENT', blank=True)
    firstevent = models.IntegerField(null=True, db_column='FIRSTEVENT', blank=True)
    boundaryid = models.BigIntegerField(null=True, db_column='BOUNDARYID', blank=True)
    pandaid = models.BigIntegerField(db_column='PANDAID', blank=True)
    jobsetid = models.BigIntegerField(db_column='JOBSETID', blank=True)
    maxfailure = models.IntegerField(null=True, db_column='MAXFAILURE', blank=True)
    failedattempt = models.IntegerField(null=True, db_column='FAILEDATTEMPT', blank=True)
    lumiblocknr = models.IntegerField(null=True, db_column='LUMIBLOCKNR', blank=True)
    procstatus = models.CharField(max_length=192, db_column='PROC_STATUS')

    class Meta:
        db_table = u'jedi_dataset_contents'
        unique_together = ('jeditaskid', 'datasetid', 'fileid')
        app_label = 'jedi'


class JediDatasets(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    datasetid = models.BigIntegerField(db_column='datasetid')
    datasetname = models.CharField(max_length=765, db_column='datasetname')
    type = models.CharField(max_length=60, db_column='type')
    creationtime = models.DateTimeField(db_column='CREATIONTIME')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    masterid = models.BigIntegerField(null=True, db_column='masterid', blank=True)
    provenanceid = models.BigIntegerField(null=True, db_column='PROVENANCEID', blank=True)
    containername = models.CharField(max_length=396, db_column='containername', blank=True)
    status = models.CharField(max_length=60, db_column='status', blank=True)
    state = models.CharField(max_length=60, db_column='STATE', blank=True)
    statechecktime = models.DateTimeField(null=True, db_column='STATECHECKTIME', blank=True)
    statecheckexpiration = models.DateTimeField(null=True, db_column='STATECHECKEXPIRATION', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='FROZENTIME', blank=True)
    nfiles = models.IntegerField(null=True, db_column='nfiles', blank=True)
    nfilestobeused = models.IntegerField(null=True, db_column='NFILESTOBEUSED', blank=True)
    nfilesused = models.IntegerField(null=True, db_column='NFILESUSED', blank=True)
    nevents = models.BigIntegerField(null=True, db_column='nevents', blank=True)
    neventstobeused = models.BigIntegerField(null=True, db_column='neventstobeused', blank=True)
    neventsused = models.BigIntegerField(null=True, db_column='neventsused', blank=True)
    lockedby = models.CharField(max_length=120, db_column='LOCKEDBY', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='LOCKEDTIME', blank=True)
    nfilesfinished = models.IntegerField(null=True, db_column='nfilesfinished', blank=True)
    nfilesfailed = models.IntegerField(null=True, db_column='nfilesfailed', blank=True)
    attributes = models.CharField(max_length=300, db_column='ATTRIBUTES', blank=True)
    streamname = models.CharField(max_length=60, db_column='streamname', blank=True)
    storagetoken = models.CharField(max_length=180, db_column='storagetoken', blank=True)
    destination = models.CharField(max_length=180, db_column='DESTINATION', blank=True)
    nfilesonhold = models.IntegerField(null=True, db_column='NFILESONHOLD', blank=True)
    templateid = models.BigIntegerField(db_column='TEMPLATEID', blank=True)
    class Meta:
        db_table = u'jedi_datasets'
        unique_together = ('jeditaskid', 'datasetid')
        app_label = 'jedi'


class JediEvents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    fileid = models.BigIntegerField(db_column='FILEID')
    job_processid = models.IntegerField(db_column='JOB_PROCESSID')
    def_min_eventid = models.IntegerField(null=True, db_column='DEF_MIN_EVENTID', blank=True)
    def_max_eventid = models.IntegerField(null=True, db_column='DEF_MAX_EVENTID', blank=True)
    processed_upto_eventid = models.IntegerField(null=True, db_column='PROCESSED_UPTO_EVENTID', blank=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', blank=True)
    status = models.IntegerField(db_column='STATUS', blank=True)
    attemptnr = models.IntegerField(db_column='ATTEMPTNR', blank=True)
    eventoffset = models.IntegerField(db_column='EVENT_OFFSET', blank=True)
    isjumbo = models.IntegerField(db_column='IS_JUMBO', blank=True)
    objstore_id = models.IntegerField(db_column='OBJSTORE_ID', blank=True)
    file_not_deleted = models.CharField(max_length=48, db_column='FILE_NOT_DELETED')
    error_code = models.IntegerField(db_column='ERROR_CODE', blank=True)

    class Meta:
        db_table = u'jedi_events'
        unique_together = ('jeditaskid', 'pandaid', 'fileid', 'job_processid')
        app_label = 'jedi'


class JediDatasetLocality(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID')
    rse = models.CharField(max_length=1000, db_column='RSE', blank=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP')

    class Meta:
        db_table = u'"ATLAS_PANDA"."jedi_dataset_locality"'
        unique_together = ('jeditaskid', 'datasetid', 'rse')
        app_label = 'jedi'


class JediJobRetryHistory(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    oldpandaid = models.BigIntegerField(db_column='oldpandaid')
    newpandaid = models.BigIntegerField(db_column='newpandaid')
    ins_utc_tstamp = models.BigIntegerField(db_column='ins_utc_tstamp', blank=True)
    relationtype = models.CharField(max_length=48, db_column='relationtype')
    class Meta:
        db_table = u'jedi_job_retry_history'
        unique_together = ('jeditaskid', 'oldpandaid', 'newpandaid')
        app_label = 'jedi'


class JediTaskparams(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='jeditaskid')
    taskparams = models.TextField(db_column='taskparams', blank=True)
    class Meta:
        db_table = u'jedi_taskparams'
        app_label = 'jedi'


class JediTasksBase(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='jeditaskid')
    taskname = models.CharField(max_length=384, db_column='taskname', blank=True)
    status = models.CharField(max_length=192, db_column='status')
    username = models.CharField(max_length=384, db_column='username')
    creationdate = models.DateTimeField(db_column='creationdate')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    reqid = models.IntegerField(null=True, db_column='reqid', blank=True)
    oldstatus = models.CharField(max_length=192, db_column='oldstatus', blank=True)
    cloud = models.CharField(max_length=30, db_column='cloud', blank=True)
    site = models.CharField(max_length=180, db_column='site', blank=True)
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True)
    endtime = models.DateTimeField(null=True, db_column='endtime', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='frozentime', blank=True)
    prodsourcelabel = models.CharField(max_length=60, db_column='prodsourcelabel', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='workinggroup', blank=True)
    vo = models.CharField(max_length=48, db_column='vo', blank=True)
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True)
    tasktype = models.CharField(max_length=192, db_column='tasktype', blank=True)
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True)
    taskpriority = models.IntegerField(null=True, db_column='taskpriority', blank=True)
    currentpriority = models.IntegerField(null=True, db_column='currentpriority', blank=True)
    architecture = models.CharField(max_length=768, db_column='architecture', blank=True)
    transuses = models.CharField(max_length=192, db_column='transuses', blank=True)
    transhome = models.CharField(max_length=384, db_column='transhome', blank=True)
    transpath = models.CharField(max_length=384, db_column='transpath', blank=True)
    lockedby = models.CharField(max_length=120, db_column='lockedby', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='lockedtime', blank=True)
    termcondition = models.CharField(max_length=300, db_column='termcondition', blank=True)
    splitrule = models.CharField(max_length=300, db_column='splitrule', blank=True)
    walltime = models.IntegerField(null=True, db_column='walltime', blank=True)
    walltimeunit = models.CharField(max_length=96, db_column='walltimeunit', blank=True)
    outdiskcount = models.IntegerField(null=True, db_column='outdiskcount', blank=True)
    outdiskunit = models.CharField(max_length=96, db_column='outdiskunit', blank=True)
    workdiskcount = models.IntegerField(null=True, db_column='workdiskcount', blank=True)
    workdiskunit = models.CharField(max_length=96, db_column='workdiskunit', blank=True)
    ramcount = models.IntegerField(null=True, db_column='ramcount', blank=True)
    ramunit = models.CharField(max_length=96, db_column='ramunit', blank=True)
    iointensity = models.IntegerField(null=True, db_column='iointensity', blank=True)
    iointensityunit = models.CharField(max_length=96, db_column='iointensityunit', blank=True)
    workqueue_id = models.IntegerField(null=True, db_column='workqueue_id', blank=True)
    progress = models.IntegerField(null=True, db_column='progress', blank=True)
    failurerate = models.IntegerField(null=True, db_column='failurerate', blank=True)
    errordialog = models.CharField(max_length=765, db_column='errordialog', blank=True)
    countrygroup = models.CharField(max_length=20, db_column='countrygroup', blank=True)
    parent_tid = models.BigIntegerField(db_column='parent_tid', blank=True)
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)
    ticketid = models.CharField(max_length=50, db_column='ticketid', blank=True)
    ticketsystemtype = models.CharField(max_length=16, db_column='ticketsystemtype', blank=True)
    statechangetime = models.DateTimeField(null=True, db_column='statechangetime', blank=True)
    superstatus = models.CharField(max_length=64, db_column='superstatus', blank=True)
    campaign = models.CharField(max_length=72, db_column='campaign', blank=True)
    gshare = models.CharField(max_length=72, db_column='gshare', blank=True)
    cputime = models.IntegerField(null=True, db_column='cputime', blank=True)
    cputimeunit = models.CharField(max_length=72, db_column='cputimeunit', blank=True)
    basewalltime = models.IntegerField(null=True, db_column='basewalltime', blank=True)
    cpuefficiency = models.IntegerField(null=True, db_column='cpuefficiency', blank=True)
    nucleus = models.CharField(max_length=72, db_column='nucleus', blank=True)
    ttcrequested = models.DateTimeField(null=True, db_column='ttcrequested', blank=True)
    ttcpredicted = models.DateTimeField(null=True, db_column='ttcpredicted', blank=True)
    ttcpredictiondate = models.DateTimeField(null=True, db_column='ttcpredictiondate', blank=True)
    resquetime = models.DateTimeField(null=True, db_column='rescuetime', blank=True)
    requesttype = models.CharField(max_length=72, db_column='requesttype', blank=True)
    resourcetype = models.CharField(max_length=300, db_column='resource_type', blank=True)
    usejumbo = models.CharField(max_length=10, db_column='usejumbo', blank=True)
    diskio = models.IntegerField(null=True, db_column='diskio', blank=True)
    diskiounit = models.CharField(max_length=96, db_column='diskiounit', blank=True)
    container_name = models.CharField(max_length=200, db_column='container_name', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='attemptnr', blank=True)

    def get_fields_by_type(self, ftype='integer'):
        field_list = [str(f.name) for f in self._meta.fields if ftype in str(f.description).lower()]
        return field_list

    class Meta:
        abstract = True

class JediTasks(JediTasksBase):
    class Meta:
        db_table = u'jedi_tasks'
        app_label = 'jedi'


class JediTasksOrdered(JediTasksBase):
    class Meta:
        db_table = f'"{DB_SCHEMA}"."jedi_tasks_ordered"'
        app_label = 'pandamon'


class GetEventsForTask(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    totevrem = models.BigIntegerField(db_column='totevrem')
    totev = models.BigIntegerField(db_column='totev')
    class Meta:
        db_table = f'"{DB_SCHEMA}"."geteventsfortask"'
        app_label = 'pandamon'


class TasksStatusLog(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True)
    status = models.CharField(max_length=64, db_column='STATUS', blank=True)
    attemptnr = models.IntegerField(db_column='ATTEMPTNR', blank=True)
    reason = models.CharField(max_length=600, db_column='REASON', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDA"."TASKS_STATUSLOG"'
        app_label = 'jedi'


#class BPToken(models.Model):
#    key = models.CharField(max_length=40, primary_key=True)
#    user = models.OneToOneField(BPUser)
#    created = models.DateTimeField(auto_now_add=True)

#    def save(self, *args, **kwargs):
#        if not self.key:
#            self.key = self.generate_key()
#        return super(BPToken, self).save(*args, **kwargs)

#    def generate_key(self):
#        unique = uuid.uuid4()
#        return hmac.new(unique.bytes, digestmod=sha1).hexdigest()

#    def __unicode__(self):
#        return self.key
#    class Meta:
#        db_table = u'"ATLAS_PANDABIGMON"."AUTHTOKEN_TOKEN"'

class JediWorkQueue(models.Model):
    queue_id = models.IntegerField(primary_key=True, db_column='QUEUE_ID')
    queue_name = models.CharField(max_length=16, db_column='QUEUE_NAME') 
    queue_type = models.CharField(max_length=16, db_column='QUEUE_TYPE') 
    vo = models.CharField(max_length=16, db_column='VO') 
    status = models.CharField(max_length=64, db_column='STATUS', blank=True) 
    partitionid = models.IntegerField(null=True, db_column='PARTITIONID', blank=True)
    stretchable = models.IntegerField(null=True, db_column='STRETCHABLE', blank=True)
    queue_share = models.IntegerField(null=True, db_column='QUEUE_SHARE', blank=True)
    queue_order = models.IntegerField(null=True, db_column='QUEUE_ORDER', blank=True)
    criteria = models.CharField(max_length=256, db_column='CRITERIA', blank=True) 
    variables = models.CharField(max_length=256, db_column='VARIABLES', blank=True) 
    class Meta:
        db_table = u'jedi_work_queue'
        app_label = 'jedi'


class Jobclass(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    name = models.CharField(max_length=90, db_column='NAME')
    description = models.CharField(max_length=90, db_column='DESCRIPTION')
    rights = models.CharField(max_length=90, db_column='RIGHTS', blank=True)
    priority = models.IntegerField(null=True, db_column='PRIORITY', blank=True)
    quota1 = models.BigIntegerField(null=True, db_column='QUOTA1', blank=True)
    quota7 = models.BigIntegerField(null=True, db_column='QUOTA7', blank=True)
    quota30 = models.BigIntegerField(null=True, db_column='QUOTA30', blank=True)
    class Meta:
        db_table = u'jobclass'

class Jobparamstable(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True)
    class Meta:
        db_table = u'jobparamstable'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class JobparamstableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True)
    class Meta:
        db_table = u'jobparamstable_arch'
        app_label = 'panda'


class JobsStatuslog(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS')
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    cloud = models.CharField(max_length=150, db_column='CLOUD', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True)
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True)
    modiftime_extended = models.DateTimeField(db_column='MODIFTIME_EXTENDED')
    class Meta:
        db_table = u'"ATLAS_PANDA"."JOBS_STATUSLOG"'
        app_label = 'panda'


class Jobsarchived4WnlistStats(models.Model):
    modificationtime = models.DateTimeField(primary_key=True, db_column='MODIFICATIONTIME')
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True)
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True)
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS')
    transexitcode = models.CharField(max_length=384, db_column='TRANSEXITCODE', blank=True)
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    num_of_jobs = models.IntegerField(null=True, db_column='NUM_OF_JOBS', blank=True)
    max_modificationtime = models.DateTimeField(null=True, db_column='MAX_MODIFICATIONTIME', blank=True)
    cur_date = models.DateTimeField(null=True, db_column='CUR_DATE', blank=True)
    class Meta:
        db_table = u'jobsarchived4_wnlist_stats'


class Jobsdebug(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='PANDAID')
    stdout = models.CharField(max_length=6144, db_column='STDOUT', blank=True)
    class Meta:
        db_table = u'jobsdebug'
        app_label = 'panda'


class Logstable(models.Model):
    pandaid = models.IntegerField(primary_key=True, db_column='PANDAID') 
    log1 = models.TextField(db_column='LOG1') 
    log2 = models.TextField(db_column='LOG2') 
    log3 = models.TextField(db_column='LOG3') 
    log4 = models.TextField(db_column='LOG4') 
    class Meta:
        db_table = u'logstable'
        app_label = 'panda'


class Members(models.Model):
    uname = models.CharField(max_length=90, db_column='UNAME', primary_key=True)
    gname = models.CharField(max_length=90, db_column='GNAME')
    rights = models.CharField(max_length=90, db_column='RIGHTS', blank=True)
    since = models.DateTimeField(db_column='SINCE')
    class Meta:
        db_table = u'members'
        unique_together = ('uname', 'gname')


class Metatable(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    metadata = models.TextField(db_column='METADATA', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."metatable"'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class MetatableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    metadata = models.TextField(db_column='METADATA', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_ARCH}"."metatable_arch"'
        app_label = 'panda'


class Metrics(models.Model):
    computingsite = models.CharField(db_column='computingsite')
    gshare = models.CharField(db_column='gshare')
    metric = models.CharField(db_column='metric')
    json = models.TextField(db_column='value_json', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."metrics"'
        app_label = 'panda'
        unique_together = ('computingsite', 'gshare')


class MvJobsactive4Stats(models.Model):
    id = models.BigIntegerField(primary_key=True, db_column='ID')
    cur_date = models.DateTimeField(db_column='CUR_DATE')
    cloud = models.CharField(max_length=150, db_column='CLOUD', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True)
    countrygroup = models.CharField(max_length=60, db_column='COUNTRYGROUP', blank=True)
    workinggroup = models.CharField(max_length=60, db_column='WORKINGGROUP', blank=True)
    relocationflag = models.IntegerField(null=True, db_column='RELOCATIONFLAG', blank=True)
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS')
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True)
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    currentpriority = models.IntegerField(null=True, db_column='CURRENTPRIORITY', blank=True)
    num_of_jobs = models.IntegerField(null=True, db_column='NUM_OF_JOBS', blank=True)
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    workqueue_id = models.IntegerField(null=True, db_column='WORKQUEUE_ID', blank=True)
    class Meta:
        db_table = u'mv_jobsactive4_stats'

class OldSubcounter(models.Model):
    subid = models.BigIntegerField(primary_key=True, db_column='SUBID')
    class Meta:
        db_table = u'old_subcounter'

class Pandaconfig(models.Model):
    name = models.CharField(max_length=180, primary_key=True, db_column='NAME')
    controller = models.CharField(max_length=60, db_column='CONTROLLER')
    pathena = models.CharField(max_length=60, db_column='PATHENA', blank=True)
    class Meta:
        db_table = u'pandaconfig'

class PandaidsDeleted(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='PANDAID')
    tstamp_datadel = models.DateTimeField(null=True, db_column='TSTAMP_DATADEL', blank=True)
    class Meta:
        db_table = u'pandaids_deleted'

class PandaidsModiftime(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modiftime = models.DateTimeField(db_column='MODIFTIME')
    class Meta:
        db_table = u'pandaids_modiftime'
        unique_together = ('pandaid', 'modiftime')

class Pandalog(models.Model):
    bintime = models.DateTimeField(db_column='BINTIME', primary_key=True)
    name = models.CharField(max_length=90, db_column='NAME', blank=True)
    module = models.CharField(max_length=90, db_column='MODULE', blank=True)
    loguser = models.CharField(max_length=240, db_column='LOGUSER', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE', blank=True)
    pid = models.BigIntegerField(db_column='PID')
    loglevel = models.IntegerField(db_column='LOGLEVEL')
    levelname = models.CharField(max_length=90, db_column='LEVELNAME', blank=True)
    time = models.CharField(max_length=90, db_column='TIME', blank=True)
    filename = models.CharField(max_length=300, db_column='FILENAME', blank=True)
    line = models.IntegerField(db_column='LINE')
    message = models.CharField(max_length=12000, db_column='MESSAGE', blank=True)
    class Meta:
        db_table = u'pandalog'
        app_label = 'panda'


class RucioAccounts(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    certificatedn = models.CharField(max_length=40, db_column='CERTIFICATEDN')
    rucio_account = models.CharField(max_length=40, db_column='RUCIO_ACCOUNT')
    create_time = models.DateTimeField(db_column='CREATE_TIME')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."RUCIO_ACCOUNTS"'
        app_label = 'pandamon'


class AllRequests(models.Model):
    id = models.IntegerField(primary_key=True, db_column='id')
    server = models.CharField(max_length=40, db_column='server')
    remote = models.CharField(max_length=40, db_column='remote')
    qtime = models.DateTimeField(db_column='qtime')
    rtime = models.DateTimeField(db_column='rtime')
    url = models.CharField(max_length=2500, db_column='url')
    referrer = models.CharField(max_length=4000, db_column='referrer')
    useragent = models.CharField(max_length=250, db_column='useragent')
    is_rejected = models.IntegerField(db_column='is_rejected')
    urlview = models.CharField(max_length=40, db_column='urlview')
    load = models.FloatField(db_column='load')
    mem = models.FloatField(db_column='mem')
    dbactivesess = models.IntegerField(db_column='dbactivesess')
    dbtotalsess = models.IntegerField(db_column='dbtotalsess')
    class Meta:
        db_table = f'"{DB_SCHEMA}"."all_requests_daily"'
        app_label = 'pandamon'


class Sitedata(models.Model):
    site = models.CharField(max_length=90, db_column='SITE', primary_key=True)
    flag = models.CharField(max_length=60, db_column='FLAG')
    hours = models.IntegerField(db_column='HOURS')
    nwn = models.IntegerField(null=True, db_column='NWN', blank=True)
    memmin = models.IntegerField(null=True, db_column='MEMMIN', blank=True)
    memmax = models.IntegerField(null=True, db_column='MEMMAX', blank=True)
    si2000min = models.IntegerField(null=True, db_column='SI2000MIN', blank=True)
    si2000max = models.IntegerField(null=True, db_column='SI2000MAX', blank=True)
    os = models.CharField(max_length=90, db_column='OS', blank=True)
    space = models.CharField(max_length=90, db_column='SPACE', blank=True)
    minjobs = models.IntegerField(null=True, db_column='MINJOBS', blank=True)
    maxjobs = models.IntegerField(null=True, db_column='MAXJOBS', blank=True)
    laststart = models.DateTimeField(null=True, db_column='LASTSTART', blank=True)
    lastend = models.DateTimeField(null=True, db_column='LASTEND', blank=True)
    lastfail = models.DateTimeField(null=True, db_column='LASTFAIL', blank=True)
    lastpilot = models.DateTimeField(null=True, db_column='LASTPILOT', blank=True)
    lastpid = models.IntegerField(null=True, db_column='LASTPID', blank=True)
    nstart = models.IntegerField(db_column='NSTART')
    finished = models.IntegerField(db_column='FINISHED')
    failed = models.IntegerField(db_column='FAILED')
    defined = models.IntegerField(db_column='DEFINED')
    assigned = models.IntegerField(db_column='ASSIGNED')
    waiting = models.IntegerField(db_column='WAITING')
    activated = models.IntegerField(db_column='ACTIVATED')
    holding = models.IntegerField(db_column='HOLDING')
    running = models.IntegerField(db_column='RUNNING')
    transferring = models.IntegerField(db_column='TRANSFERRING')
    getjob = models.IntegerField(db_column='GETJOB')
    updatejob = models.IntegerField(db_column='UPDATEJOB')
    nojob = models.IntegerField(null=True, db_column='NOJOB', blank=True)
    lastmod = models.DateTimeField(db_column='LASTMOD')
    ncpu = models.IntegerField(null=True, db_column='NCPU', blank=True)
    nslot = models.IntegerField(null=True, db_column='NSLOT', blank=True)
    getjobabs = models.IntegerField(db_column='GETJOBABS')
    updatejobabs = models.IntegerField(db_column='UPDATEJOBABS')
    nojobabs = models.IntegerField(null=True, db_column='NOJOBABS', blank=True)
    class Meta:
        db_table = u'sitedata'
        unique_together = ('site', 'flag', 'hours')
        app_label = 'panda'


class Sysconfig(models.Model):
    name = models.CharField(max_length=180, db_column='NAME', primary_key=True)
    system = models.CharField(max_length=60, db_column='SYSTEM')
    config = models.CharField(max_length=12000, db_column='CONFIG', blank=True)
    class Meta:
        db_table = u'sysconfig'
        unique_together = ('name', 'system')


class Users(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    name = models.CharField(max_length=180, db_column='NAME')
    dn = models.CharField(max_length=450, db_column='DN', blank=True)
    email = models.CharField(max_length=180, db_column='EMAIL', blank=True)
    url = models.CharField(max_length=300, db_column='URL', blank=True)
    location = models.CharField(max_length=180, db_column='LOCATION', blank=True)
    classa = models.CharField(max_length=90, db_column='CLASSA', blank=True)
    classp = models.CharField(max_length=90, db_column='CLASSP', blank=True)
    classxp = models.CharField(max_length=90, db_column='CLASSXP', blank=True)
    sitepref = models.CharField(max_length=180, db_column='SITEPREF', blank=True)
    gridpref = models.CharField(max_length=60, db_column='GRIDPREF', blank=True)
    queuepref = models.CharField(max_length=180, db_column='QUEUEPREF', blank=True)
    scriptcache = models.CharField(max_length=300, db_column='SCRIPTCACHE', blank=True)
    types = models.CharField(max_length=180, db_column='TYPES', blank=True)
    sites = models.CharField(max_length=750, db_column='SITES', blank=True)
    njobsa = models.IntegerField(null=True, db_column='NJOBSA', blank=True)
    njobsp = models.IntegerField(null=True, db_column='NJOBSP', blank=True)
    njobs1 = models.IntegerField(null=True, db_column='NJOBS1', blank=True)
    njobs7 = models.IntegerField(null=True, db_column='NJOBS7', blank=True)
    njobs30 = models.IntegerField(null=True, db_column='NJOBS30', blank=True)
    cpua1 = models.BigIntegerField(null=True, db_column='CPUA1', blank=True)
    cpua7 = models.BigIntegerField(null=True, db_column='CPUA7', blank=True)
    cpua30 = models.BigIntegerField(null=True, db_column='CPUA30', blank=True)
    cpup1 = models.BigIntegerField(null=True, db_column='CPUP1', blank=True)
    cpup7 = models.BigIntegerField(null=True, db_column='CPUP7', blank=True)
    cpup30 = models.BigIntegerField(null=True, db_column='CPUP30', blank=True)
    cpuxp1 = models.BigIntegerField(null=True, db_column='CPUXP1', blank=True)
    cpuxp7 = models.BigIntegerField(null=True, db_column='CPUXP7', blank=True)
    cpuxp30 = models.BigIntegerField(null=True, db_column='CPUXP30', blank=True)
    quotaa1 = models.BigIntegerField(null=True, db_column='QUOTAA1', blank=True)
    quotaa7 = models.BigIntegerField(null=True, db_column='QUOTAA7', blank=True)
    quotaa30 = models.BigIntegerField(null=True, db_column='QUOTAA30', blank=True)
    quotap1 = models.BigIntegerField(null=True, db_column='QUOTAP1', blank=True)
    quotap7 = models.BigIntegerField(null=True, db_column='QUOTAP7', blank=True)
    quotap30 = models.BigIntegerField(null=True, db_column='QUOTAP30', blank=True)
    quotaxp1 = models.BigIntegerField(null=True, db_column='QUOTAXP1', blank=True)
    quotaxp7 = models.BigIntegerField(null=True, db_column='QUOTAXP7', blank=True)
    quotaxp30 = models.BigIntegerField(null=True, db_column='QUOTAXP30', blank=True)
    space1 = models.IntegerField(null=True, db_column='SPACE1', blank=True)
    space7 = models.IntegerField(null=True, db_column='SPACE7', blank=True)
    space30 = models.IntegerField(null=True, db_column='SPACE30', blank=True)
    lastmod = models.DateTimeField(db_column='LASTMOD')
    firstjob = models.DateTimeField(db_column='FIRSTJOB')
    latestjob = models.DateTimeField(db_column='LATESTJOB')
    pagecache = models.TextField(db_column='PAGECACHE', blank=True)
    cachetime = models.DateTimeField(db_column='CACHETIME')
    ncurrent = models.IntegerField(db_column='NCURRENT')
    jobid = models.IntegerField(db_column='JOBID')
    status = models.CharField(max_length=60, db_column='STATUS', blank=True)
    vo = models.CharField(max_length=60, db_column='VO', blank=True)

    class Meta:
        db_table = u'users'
##FIXME: reenable this after proper dbproxies are introduced!###        db_table = u'"ATLAS_PANDAMETA"."USERS"'
        allColumns = COLUMNS['ActiveUsers-all']
        primaryColumns = [ 'name']
        secondaryColumns = []
        orderColumns = ORDER_COLUMNS['ActiveUsers-all']
        columnTitles = COL_TITLES['ActiveUsers-all']
        filterFields = FILTERS['ActiveUsers-all']


    def __str__(self):
        return 'User: ' + str(self.name) + '[' + str(self.status) + ']'


class Userstats(models.Model):
    name = models.CharField(max_length=180, db_column='NAME', primary_key=True)
    label = models.CharField(max_length=60, db_column='LABEL', blank=True)
    yr = models.IntegerField(db_column='YR')
    mo = models.IntegerField(db_column='MO')
    jobs = models.BigIntegerField(null=True, db_column='JOBS', blank=True)
    idlo = models.BigIntegerField(null=True, db_column='IDLO', blank=True)
    idhi = models.BigIntegerField(null=True, db_column='IDHI', blank=True)
    info = models.CharField(max_length=300, db_column='INFO', blank=True)
    class Meta:
        db_table = u'userstats'
        unique_together = ('name', 'yr', 'mo')


def prefetch_id(db, seq_name, table_name=None, id_field_name=None):
    """ Fetch the next value in a django id oracle sequence """
    conn =  connections[db]
    cursor = connections[db].cursor()
    new_id = None
    if cursor.db.client.executable_name != 'sqlite3':

        try:
            query = "SELECT %s.nextval FROM dual" % seq_name
            cursor.execute(query)
            rows = cursor.fetchall()
            new_id = rows[0][0]
        finally:
            if cursor:
                cursor.close()
    else:
        #only for tests
        try:
            query = "SELECT MAX(%s) AS max_id FROM %s"%(id_field_name,table_name)
            cursor.execute(query)
            rows = cursor.fetchall()
            if not(rows[0][0]):
                new_id = 1
            else:
                new_id = rows[0][0] + 1

        finally:
            if cursor:
                cursor.close()
    return new_id


class TProject(models.Model):
    project = models.CharField(max_length=60, db_column='PROJECT', primary_key=True)
    begin_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='BEGIN_TIME')
    end_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='END_TIME')
    status = models.CharField(max_length=8, db_column='STATUS')
    status = models.CharField(max_length=500, db_column='DESCRIPTION')
    time_stamp = models.DecimalField(decimal_places=0, max_digits=10, db_column='TIMESTAMP')

    def save(self):
        raise Exception

    def __str__(self):
        return "%s" % self.project

    class Meta:

        db_table = u'T_PROJECTS'

class TRequest(models.Model):
    # PHYS_GROUPS=[(x,x) for x in ['physics','BPhysics','Btagging','DPC','Detector','EGamma','Exotics','HI','Higgs',
    #                              'InDet','JetMet','LAr','MuDet','Muon','SM','Susy','Tau','Top','Trigger','TrackingPerf',
    #                              'reprocessing','trig-hlt','Validation']]
    PHYS_GROUPS=[(x,x) for x in ['BPHY',
                                 'COSM',
                                 'DAPR',
                                 'EGAM',
                                 'EXOT',
                                 'FTAG',
                                 'HIGG',
                                 'HION',
                                 'IDET',
                                 'IDTR',
                                 'JETM',
                                 'LARG',
                                 'MCGN',
                                 'SIMU',
                                 'MDET',
                                 'MUON',
                                 'PHYS',
                                 'REPR',
                                 'STDM',
                                 'SUSY',
                                 'TAUP',
                                 'TCAL',
                                 'TDAQ',
                                 'TOPQ',
                                 'THLT',
                                 'TRIG',
                                 'VALI',
                                 'UPGR']]

    REQUEST_TYPE = [(x,x) for x in ['MC','GROUP','REPROCESSING','ANALYSIS','HLT']]
    PROVENANCE_TYPE = [(x,x) for x in ['AP','GP','XP']]

    reqid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', primary_key=True)
    manager = models.CharField(max_length=32, db_column='MANAGER', null=False, blank=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION', null=True, blank=True)
    ref_link = models.CharField(max_length=256, db_column='REFERENCE_LINK', null=True, blank=True)
    cstatus = models.CharField(max_length=32, db_column='STATUS', null=False, blank=True)
    provenance = models.CharField(max_length=32, db_column='PROVENANCE', null=False, blank=True,choices=PROVENANCE_TYPE)
    request_type = models.CharField(max_length=32, db_column='REQUEST_TYPE',choices=REQUEST_TYPE, null=False, blank=True)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False, blank=True)
    subcampaign = models.CharField(max_length=32, db_column='SUB_CAMPAIGN', null=False, blank=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=False, choices=PHYS_GROUPS, blank=True)
    energy_gev = models.DecimalField(decimal_places=0, max_digits=8, db_column='ENERGY_GEV', null=False, blank=True)
    project = models.ForeignKey(TProject,db_column='PROJECT', null=True, blank=False, on_delete=models.DO_NOTHING)
    is_error = models.NullBooleanField(db_column='EXCEPTION', null=True, blank=False)
    jira_reference = models.CharField(max_length=50, db_column='REFERENCE', null=True, blank=True)

    def save(self, *args, **kwargs):
        if not self.reqid:
            self.reqid = prefetch_id('deft',u'ATLAS_DEFT.T_PRODMANAGER_REQUEST_ID_SEQ','T_PRODMANAGER_REQUEST','PR_ID')

        super(TRequest, self).save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODMANAGER_REQUEST'


class RequestStatus(models.Model):
    STATUS_TYPES = (
                    ('Created', 'Created'),
                    ('Pending', 'Pending'),
                    ('Unknown', 'Unknown'),
                    ('Approved', 'Approved'),
                    )
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='REQ_S_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    comment = models.CharField(max_length=256, db_column='COMMENT', null=True)
    owner = models.CharField(max_length=32, db_column='OWNER', null=False)
    status = models.CharField(max_length=32, db_column='STATUS', choices=STATUS_TYPES, null=False)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODMANAGER_REQ_STAT_ID_SEQ','T_PRODMANAGER_REQUEST_STATUS','REQ_S_ID')
        super(RequestStatus, self).save(*args, **kwargs)

    def save_with_current_time(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        self.save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODMANAGER_REQUEST_STATUS'

class StepTemplate(models.Model):
    id =  models.DecimalField(decimal_places=0, max_digits=12,  db_column='STEP_T_ID', primary_key=True)
    step = models.CharField(max_length=12, db_column='STEP_NAME', null=False)
    def_time = models.DateTimeField(db_column='DEF_TIME', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    ctag = models.CharField(max_length=12, db_column='CTAG', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=7, db_column='CPU_PER_EVENT', null=True)
    output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', null=True)
    trf_name = models.CharField(max_length=128, db_column='TRF_NAME', null=True)
    lparams = models.CharField(max_length=2000, db_column='LPARAMS', null=True)
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True)
    swrelease = models.CharField(max_length=80, db_column='SWRELEASE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_STEP_TEMPLATE_ID_SEQ','T_STEP_TEMPLATE','STEP_T_ID')
        super(StepTemplate, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_STEP_TEMPLATE'
        db_table = u'T_STEP_TEMPLATE'

class Ttrfconfig(models.Model):
    tag = models.CharField(max_length=1, db_column='TAG', default='-')
    cid = models.DecimalField(decimal_places=0, max_digits=5, db_column='CID', primary_key=True, default=0)
    trf = models.CharField(max_length=80, db_column='TRF', null=True, default='transformation')
    lparams = models.CharField(max_length=1024, db_column='LPARAMS', null=True, default='parameter list')
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True, default='')
    trfv = models.CharField(max_length=40, db_column='TRFV', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    ami_flag = models.DecimalField(decimal_places=0, max_digits=10, db_column='AMI_FLAG', null=True)
    createdby = models.CharField(max_length=60, db_column='CREATEDBY', null=True)
    input = models.CharField(max_length=20, db_column='INPUT', null=True)
    step = models.CharField(max_length=12, db_column='STEP', null=True)
    formats = models.CharField(max_length=256, db_column='FORMATS', null=True)
    cache = models.CharField(max_length=32, db_column='CACHE', null=True)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=5, db_column='CPU_PER_EVENT', null=True, default=1)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', default=1000)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', default=100)
    events_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='EVENTS_PER_JOB', default=1000)

    class Meta:
        app_label = 'grisli'
        db_table = u'T_TRF_CONFIG'


class TDataFormatAmi(models.Model):
    format = models.CharField(max_length=32, db_column='FORMAT', primary_key=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION')
    status = models.CharField(max_length=8, db_column='STATUS')
    last_modified = models.DateTimeField(db_column='LASTMODIFIED')

    class Meta:
        app_label = 'grisli'
        db_table = u'T_DATA_FORMAT_AMI'


class ProductionDataset(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    #task = models.ForeignKey(ProducitonTask,db_column='TASK_ID')
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', null=True)
    #parent_task = models.ForeignKey(ProducitonTask,db_column='TASK_ID')
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    events = models.DecimalField(decimal_places=0, max_digits=7, db_column='EVENTS', null=True)
    files = models.DecimalField(decimal_places=0, max_digits=7, db_column='FILES', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_DATASET'


class ProductionContainer(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_CONTAINER'


class InputRequestList(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='IND_ID', primary_key=True)
    dataset = models.ForeignKey(ProductionDataset, db_column='INPUTDATASET',null=True, on_delete=models.DO_NOTHING)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    slice = models.DecimalField(decimal_places=0, max_digits=12, db_column='SLICE', null=False)
    brief = models.CharField(max_length=150, db_column='BRIEF')
    phys_comment = models.CharField(max_length=256, db_column='PHYSCOMMENT')
    comment = models.CharField(max_length=512, db_column='SLICECOMMENT')
    input_data = models.CharField(max_length=150, db_column='INPUTDATA')
    project_mode = models.CharField(max_length=256, db_column='PROJECT_MODE')
    priority = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY')
    input_events = models.DecimalField(decimal_places=0, max_digits=12, db_column='INPUT_EVENTS')
    is_hide = models.NullBooleanField(db_column='HIDED', null=True, blank=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_INPUT_DATASET_ID_SEQ','T_INPUT_DATASET','IND_ID')
        super(InputRequestList, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_INPUT_DATASET'
        db_table = u'T_INPUT_DATASET'


class StepExecution(models.Model):
    STEPS = ['Evgen',
             'Simul',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Rec TAG',
             'Atlfast',
             'Atlf Merge',
             'Atlf TAG']
    STEPS_STATUS = ['NotChecked','NotCheckedSkipped','Skipped','Approved']
    STEPS_APPROVED_STATUS = ['Skipped','Approved']
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    step_template = models.ForeignKey(StepTemplate, db_column='STEP_T_ID', on_delete=models.DO_NOTHING)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False, on_delete=models.DO_NOTHING)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    step_def_time = models.DateTimeField(db_column='STEP_DEF_TIME', null=False)
    step_appr_time = models.DateTimeField(db_column='STEP_APPR_TIME', null=True)
    step_exe_time = models.DateTimeField(db_column='STEP_EXE_TIME', null=True)
    step_done_time = models.DateTimeField(db_column='STEP_DONE_TIME', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='INPUT_EVENTS', null=True)
    task_config = models.CharField(max_length=2000, db_column='TASK_CONFIG')
    step_parent = models.ForeignKey('self', db_column='STEP_PARENT_ID', on_delete=models.DO_NOTHING)

    def set_task_config(self, update_dict):
        if not self.task_config:
            self.task_config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.task_config)
        currrent_dict.update(update_dict)
        self.task_config = json.dumps(currrent_dict)

    def save_with_current_time(self, *args, **kwargs):
        if not self.step_def_time:
            self.step_def_time = timezone.now()
        if self.status == 'Approved':
            if not self.step_appr_time:
                self.step_appr_time = timezone.now()
        self.save(*args, **kwargs)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_STEP_ID_SEQ','T_PRODUCTION_STEP','STEP_ID')
        if not self.step_parent_id:
            self.step_parent_id = self.id
        super(StepExecution, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = u'T_PRODUCTION_STEP'


class TTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    _jedi_task_parameters = models.TextField(db_column='JEDI_TASK_PARAMETERS')

    @property
    def jedi_task_parameters(self):
        try:
            params = json.loads(self._jedi_task_parameters)
        except:
            return
        return params

    @property
    def input_dataset(self):
        return self._get_dataset('input') or ""

    @property
    def output_dataset(self):
        return self._get_dataset('output') or ""

    def _get_dataset(self, ds_type):
        if ds_type not in ['input', 'output']:
            return
        params = self.jedi_task_parameters
        job_params = params.get('jobParameters')
        if not job_params:
            return
        for param in job_params:
            param_type, dataset = [ param.get(x) for x in ('param_type', 'dataset') ]
            if (param_type == ds_type) and (dataset is not None):
                return dataset.rstrip('/')
        return None

    def save(self, **kwargs):
        """ Read-only access to the table """
        raise NotImplementedError

    class Meta:
        managed = False
        db_table =  u'"ATLAS_DEFT"."T_TASK"'
        app_label = 'taskmon'


class ProductionTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='STEP_ID', on_delete=models.DO_NOTHING)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=models.DO_NOTHING)
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=False)
    chain_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=False)
    name = models.CharField(max_length=130, db_column='TASKNAME', null=True)
    project = models.CharField(max_length=60, db_column='PROJECT', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    dsn = models.CharField(max_length=12, db_column='DSN', null=True)
    phys_short = models.CharField(max_length=80, db_column='PHYS_SHORT', null=True)
    simulation_type = models.CharField(max_length=20, db_column='SIMULATION_TYPE', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    provenance = models.CharField(max_length=12, db_column='PROVENANCE', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    pptimestamp = models.DateTimeField(db_column='PPTIMESTAMP', null=True)
    postproduction = models.CharField(max_length=128, db_column='POSTPRODUCTION', null=True)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=True)
    current_priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='CURRENT_PRIORITY', null=True)
    update_time = models.DateTimeField(db_column='UPDATE_TIME', null=True)
    update_owner = models.CharField(max_length=24, db_column='UPDATE_OWNER', null=True)
    comments = models.CharField(max_length=256, db_column='COMMENTS', null=True)
    inputdataset = models.CharField(max_length=150, db_column='INPUTDATASET', null=True)
    physics_tag = models.CharField(max_length=20, db_column='PHYSICS_TAG', null=True)
    reference = models.CharField(max_length=150, db_column='REFERENCE', null=False)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False, blank=True)

    def save(self):
        raise NotImplementedError

    @property
    def input_dataset(self):
        try:
            dataset = TTask.objects.get(id=self.id).input_dataset
        except:
            return ""
        return dataset

    @property
    def output_dataset(self):
        try:
            dataset = TTask.objects.get(id=self.id).output_dataset
        except:
            return ""
        return dataset

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = u'T_PRODUCTION_TASK'


class MCPattern(models.Model):
    STEPS = ['Evgen',
             'Simul',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Rec TAG',
             'Atlfast',
             'Atlf Merge',
             'Atlf TAG']
    STATUS = [(x,x) for x in ['IN USE','Obsolete']]
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='MCP_ID', primary_key=True)
    pattern_name =  models.CharField(max_length=150, db_column='PATTERN_NAME', unique=True)
    pattern_dict = models.CharField(max_length=2000, db_column='PATTERN_DICT')
    pattern_status = models.CharField(max_length=20, db_column='PATTERN_STATUS', choices=STATUS)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_MCP_ID_SEQ','T_PRODUCTION_MC_PATTERN','MCP_ID')
        super(MCPattern, self).save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODUCTION_MC_PATTERN'


class MCPriority(models.Model):
    STEPS = ['Evgen',
             'Simul',
             'Simul(Fast)',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Rec TAG',
             'Atlfast',
             'Atlf Merge',
             'Atlf TAG']
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='MCPRIOR_ID', primary_key=True)
    priority_key = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY_KEY', unique=True)
    priority_dict = models.CharField(max_length=2000, db_column='PRIORITY_DICT')

    def save(self, *args, **kwargs):
        if self.priority_key == -1:
            return
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_MCPRIOR_ID_SEQ','T_PRODUCTION_MC_PRIORITY','MCPRIOR_ID')
        super(MCPriority, self).save(*args, **kwargs)

    def priority(self, step, tag):
        priority_py_dict = json.loads(self.priority_dict)
        if step == 'Simul' and tag[0] == 'a':
            step = 'Simul(Fast)'
        if step in priority_py_dict:
            return priority_py_dict[step]
        else:
            raise LookupError('No step %s in priority dict' % step)

    class Meta:
        db_table = u'T_PRODUCTION_MC_PRIORITY'


def get_priority_object(priority_key):
    try:
        mcp = MCPriority.objects.get(priority_key=priority_key)
    except ObjectDoesNotExist:
        priority_py_dict = {}
        for step in MCPriority.STEPS:
            priority_py_dict.update({step:int(priority_key)})
        mcp=MCPriority.objects.create(priority_key=-1,priority_dict=json.dumps(priority_py_dict))
    except Exception as e:
        raise e
    return mcp


def get_default_nEventsPerJob_dict():
    defult_dict = {
        'Evgen':5000,
        'Simul':100,
        'Merge':1000,
        'Digi':500,
        'Reco':500,
        'Rec Merge':5000,
        'Rec TAG':25000,
        'Atlfast':500,
        'Atlf Merge':5000,
        'Atlf TAG':25000
    }
    return defult_dict


def get_default_project_mode_dict():
    default_dict = {
         'Evgen':'spacetoken=ATLASDATADISK',
         'Simul':'spacetoken=ATLASDATADISK',
         'Merge':'spacetoken=ATLASMCTAPE',
         'Digi':'Npileup=5;spacetoken=ATLASDATADISK',
         'Reco':'Npileup=5;spacetoken=ATLASDATADISK',
         'Rec Merge':'spacetoken=ATLASDATADISK',
         'Rec TAG':'spacetoken=ATLASDATADISK',
         'Atlfast':'Npileup=5;spacetoken=ATLASDATADISK',
         'Atlf Merge':'spacetoken=ATLASDATADISK',
         'Atlf TAG':'spacetoken=ATLASDATADISK'
    }
    return default_dict


