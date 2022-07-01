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

from core.settings.config import DB_SCHEMA, DB_SCHEMA_PANDA, DB_SCHEMA_PANDA_ARCH, DB_SCHEMA_PANDA_META

models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)


class Cloudconfig(models.Model):
    name = models.CharField(max_length=60, primary_key=True, db_column='name')
    description = models.CharField(max_length=150, db_column='description')
    tier1 = models.CharField(max_length=60, db_column='tier1')
    tier1se = models.CharField(max_length=1200, db_column='tier1se')
    relocation = models.CharField(max_length=30, db_column='relocation', blank=True)
    weight = models.IntegerField(db_column='weight')
    server = models.CharField(max_length=300, db_column='server')
    status = models.CharField(max_length=60, db_column='status')
    transtimelo = models.IntegerField(db_column='transtimelo')
    transtimehi = models.IntegerField(db_column='transtimehi')
    waittime = models.IntegerField(db_column='waittime')
    comment_field = models.CharField(max_length=600, db_column='comment_', blank=True)  # Field renamed because it was a Python reserved word.
    space = models.IntegerField(db_column='space')
    moduser = models.CharField(max_length=90, db_column='moduser', blank=True)
    modtime = models.DateTimeField(db_column='modtime')
    validation = models.CharField(max_length=60, db_column='validation', blank=True)
    mcshare = models.IntegerField(db_column='mcshare')
    countries = models.CharField(max_length=240, db_column='countries', blank=True)
    fasttrack = models.CharField(max_length=60, db_column='fasttrack', blank=True)
    nprestage = models.BigIntegerField(db_column='nprestage')
    pilotowners = models.CharField(max_length=900, db_column='pilotowners', blank=True)
    dn = models.CharField(max_length=300, db_column='dn', blank=True)
    email = models.CharField(max_length=180, db_column='email', blank=True)
    fairshare = models.CharField(max_length=384, db_column='fairshare', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_META}"."cloudconfig"'
        app_label = 'panda'


class Datasets(models.Model):
    vuid = models.CharField(max_length=120, db_column='vuid', primary_key=True)
    name = models.CharField(max_length=765, db_column='name')
    version = models.CharField(max_length=30, db_column='version', blank=True)
    type = models.CharField(max_length=60, db_column='type')
    status = models.CharField(max_length=30, db_column='status', blank=True)
    numberfiles = models.IntegerField(null=True, db_column='numberfiles', blank=True)
    currentfiles = models.IntegerField(null=True, db_column='currentfiles', blank=True)
    creationdate = models.DateTimeField(null=True, db_column='creationdate', blank=True)
    modificationdate = models.DateTimeField(db_column='modificationdate')
    moverid = models.BigIntegerField(db_column='moverid')
    transferstatus = models.IntegerField(db_column='transferstatus')
    subtype = models.CharField(max_length=15, db_column='subtype', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."datasets"'
        unique_together = ('vuid', 'modificationdate')
        app_label = 'panda'


class Filestable4(models.Model):
    row_id = models.BigIntegerField(db_column='row_id', primary_key=True)
    pandaid = models.BigIntegerField(db_column='pandaid')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    guid = models.CharField(max_length=192, db_column='guid', blank=True)
    lfn = models.CharField(max_length=768, db_column='lfn', blank=True)
    type = models.CharField(max_length=60, db_column='type', blank=True)
    dataset = models.CharField(max_length=765, db_column='dataset', blank=True)
    status = models.CharField(max_length=192, db_column='status', blank=True)
    proddblock = models.CharField(max_length=765, db_column='proddblock', blank=True)
    proddblocktoken = models.CharField(max_length=750, db_column='proddblocktoken', blank=True)
    dispatchdblock = models.CharField(max_length=765, db_column='dispatchdblock', blank=True)
    dispatchdblocktoken = models.CharField(max_length=750, db_column='dispatchdblocktoken', blank=True)
    destinationdblock = models.CharField(max_length=765, db_column='destinationdblock', blank=True)
    destinationdblocktoken = models.CharField(max_length=750, db_column='destinationdblocktoken', blank=True)
    destinationse = models.CharField(max_length=750, db_column='destinationse', blank=True)
    fsize = models.BigIntegerField(db_column='fsize')
    md5sum = models.CharField(max_length=108, db_column='md5sum', blank=True)
    checksum = models.CharField(max_length=108, db_column='checksum', blank=True)
    scope = models.CharField(max_length=90, db_column='scope', blank=True)
    jeditaskid = models.BigIntegerField(null=True, db_column='jeditaskid', blank=True)
    datasetid = models.BigIntegerField(null=True, db_column='datasetid', blank=True)
    fileid = models.BigIntegerField(null=True, db_column='fileid', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='attemptnr', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."filestable4"'
        unique_together = ('row_id', 'modificationtime')
        app_label = 'panda'


class FilestableArch(models.Model):
    row_id = models.BigIntegerField(db_column='row_id', primary_key=True)
    pandaid = models.BigIntegerField(db_column='pandaid') 
    modificationtime = models.DateTimeField(db_column='modificationtime')
    creationtime = models.DateTimeField(db_column='creationtime')
    guid = models.CharField(max_length=64, db_column='guid', blank=True) 
    lfn = models.CharField(max_length=256, db_column='lfn', blank=True) 
    type = models.CharField(max_length=20, db_column='type', blank=True) 
    dataset = models.CharField(max_length=255, db_column='dataset', blank=True) 
    status = models.CharField(max_length=64, db_column='status', blank=True) 
    proddblock = models.CharField(max_length=255, db_column='proddblock', blank=True) 
    proddblocktoken = models.CharField(max_length=250, db_column='proddblocktoken', blank=True) 
    dispatchdblock = models.CharField(max_length=265, db_column='dispatchdblock', blank=True) 
    dispatchdblocktoken = models.CharField(max_length=250, db_column='dispatchdblocktoken', blank=True) 
    destinationdblock = models.CharField(max_length=265, db_column='destinationdblock', blank=True) 
    destinationdblocktoken = models.CharField(max_length=250, db_column='destinationdblocktoken', blank=True) 
    destinationse = models.CharField(max_length=250, db_column='destinationse', blank=True) 
    fsize = models.BigIntegerField(db_column='fsize') 
    md5sum = models.CharField(max_length=40, db_column='md5sum', blank=True) 
    checksum = models.CharField(max_length=40, db_column='checksum', blank=True) 
    scope = models.CharField(max_length=30, db_column='scope', blank=True) 
    jeditaskid = models.BigIntegerField(null=True, db_column='jeditaskid', blank=True) 
    datasetid = models.BigIntegerField(null=True, db_column='datasetid', blank=True) 
    fileid = models.BigIntegerField(null=True, db_column='fileid', blank=True) 
    attemptnr = models.IntegerField(null=True, db_column='attemptnr', blank=True) 

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_ARCH}"."filestable_arch"'
        unique_together = ('row_id', 'modificationtime')
        app_label = 'panda'


class Incidents(models.Model):
    at_time = models.DateTimeField(primary_key=True, db_column='at_time')
    typekey = models.CharField(max_length=60, db_column='typekey', blank=True)
    description = models.CharField(max_length=600, db_column='description', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_META}"."incidents"'
        app_label = 'panda'


class JediDatasetContents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    datasetid = models.BigIntegerField(db_column='datasetid')
    fileid = models.BigIntegerField(db_column='fileid')
    creationdate = models.DateTimeField(db_column='creationdate')
    lastattempttime = models.DateTimeField(null=True, db_column='lastattempttime', blank=True)
    lfn = models.CharField(max_length=768, db_column='lfn')
    guid = models.CharField(max_length=192, db_column='guid', blank=True)
    type = models.CharField(max_length=60, db_column='type')
    status = models.CharField(max_length=192, db_column='status')
    fsize = models.BigIntegerField(null=True, db_column='fsize', blank=True)
    checksum = models.CharField(max_length=108, db_column='checksum', blank=True)
    scope = models.CharField(max_length=90, db_column='scope', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='attemptnr', blank=True)
    maxattempt = models.IntegerField(null=True, db_column='maxattempt', blank=True)
    nevents = models.IntegerField(null=True, db_column='nevents', blank=True)
    keeptrack = models.IntegerField(null=True, db_column='keeptrack', blank=True)
    startevent = models.IntegerField(null=True, db_column='startevent', blank=True)
    endevent = models.IntegerField(null=True, db_column='endevent', blank=True)
    firstevent = models.IntegerField(null=True, db_column='firstevent', blank=True)
    boundaryid = models.BigIntegerField(null=True, db_column='boundaryid', blank=True)
    pandaid = models.BigIntegerField(db_column='pandaid', blank=True)
    jobsetid = models.BigIntegerField(db_column='jobsetid', blank=True)
    maxfailure = models.IntegerField(null=True, db_column='maxfailure', blank=True)
    failedattempt = models.IntegerField(null=True, db_column='failedattempt', blank=True)
    lumiblocknr = models.IntegerField(null=True, db_column='lumiblocknr', blank=True)
    procstatus = models.CharField(max_length=192, db_column='proc_status')
    ramcount = models.IntegerField(null=True, db_column='ramcount', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_dataset_contents"'
        unique_together = ('jeditaskid', 'datasetid', 'fileid')
        app_label = 'jedi'


class JediDatasets(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    datasetid = models.BigIntegerField(db_column='datasetid')
    datasetname = models.CharField(max_length=765, db_column='datasetname')
    type = models.CharField(max_length=60, db_column='type')
    creationtime = models.DateTimeField(db_column='creationtime')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    vo = models.CharField(max_length=48, db_column='vo', blank=True)
    cloud = models.CharField(max_length=30, db_column='cloud', blank=True)
    site = models.CharField(max_length=180, db_column='site', blank=True)
    masterid = models.BigIntegerField(null=True, db_column='masterid', blank=True)
    provenanceid = models.BigIntegerField(null=True, db_column='provenanceid', blank=True)
    containername = models.CharField(max_length=396, db_column='containername', blank=True)
    status = models.CharField(max_length=60, db_column='status', blank=True)
    state = models.CharField(max_length=60, db_column='state', blank=True)
    statechecktime = models.DateTimeField(null=True, db_column='statechecktime', blank=True)
    statecheckexpiration = models.DateTimeField(null=True, db_column='statecheckexpiration', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='frozentime', blank=True)
    nfiles = models.IntegerField(null=True, db_column='nfiles', blank=True)
    nfilestobeused = models.IntegerField(null=True, db_column='nfilestobeused', blank=True)
    nfilesused = models.IntegerField(null=True, db_column='nfilesused', blank=True)
    nevents = models.BigIntegerField(null=True, db_column='nevents', blank=True)
    neventstobeused = models.BigIntegerField(null=True, db_column='neventstobeused', blank=True)
    neventsused = models.BigIntegerField(null=True, db_column='neventsused', blank=True)
    lockedby = models.CharField(max_length=120, db_column='lockedby', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='lockedtime', blank=True)
    nfilesfinished = models.IntegerField(null=True, db_column='nfilesfinished', blank=True)
    nfilesfailed = models.IntegerField(null=True, db_column='nfilesfailed', blank=True)
    attributes = models.CharField(max_length=300, db_column='attributes', blank=True)
    streamname = models.CharField(max_length=60, db_column='streamname', blank=True)
    storagetoken = models.CharField(max_length=180, db_column='storagetoken', blank=True)
    destination = models.CharField(max_length=180, db_column='destination', blank=True)
    nfilesonhold = models.IntegerField(null=True, db_column='nfilesonhold', blank=True)
    templateid = models.BigIntegerField(db_column='templateid', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_datasets"'
        unique_together = ('jeditaskid', 'datasetid')
        app_label = 'jedi'


class JediEvents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    pandaid = models.BigIntegerField(db_column='pandaid')
    fileid = models.BigIntegerField(db_column='fileid')
    job_processid = models.IntegerField(db_column='job_processid')
    def_min_eventid = models.IntegerField(null=True, db_column='def_min_eventid', blank=True)
    def_max_eventid = models.IntegerField(null=True, db_column='def_max_eventid', blank=True)
    processed_upto_eventid = models.IntegerField(null=True, db_column='processed_upto_eventid', blank=True)
    datasetid = models.BigIntegerField(db_column='datasetid', blank=True)
    status = models.IntegerField(db_column='status', blank=True)
    attemptnr = models.IntegerField(db_column='attemptnr', blank=True)
    eventoffset = models.IntegerField(db_column='event_offset', blank=True)
    isjumbo = models.IntegerField(db_column='is_jumbo', blank=True)
    objstore_id = models.IntegerField(db_column='objstore_id', blank=True)
    file_not_deleted = models.CharField(max_length=48, db_column='file_not_deleted')
    error_code = models.IntegerField(db_column='error_code', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_events"'
        unique_together = ('jeditaskid', 'pandaid', 'fileid', 'job_processid')
        app_label = 'jedi'


class JediDatasetLocality(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    datasetid = models.BigIntegerField(db_column='datasetid')
    rse = models.CharField(max_length=1000, db_column='rse', blank=True)
    timestamp = models.DateTimeField(db_column='timestamp')

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_dataset_locality"'
        unique_together = ('jeditaskid', 'datasetid', 'rse')
        app_label = 'jedi'


class JediJobRetryHistory(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    oldpandaid = models.BigIntegerField(db_column='oldpandaid')
    newpandaid = models.BigIntegerField(db_column='newpandaid')
    ins_utc_tstamp = models.BigIntegerField(db_column='ins_utc_tstamp', blank=True)
    relationtype = models.CharField(max_length=48, db_column='relationtype')
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_job_retry_history"'
        unique_together = ('jeditaskid', 'oldpandaid', 'newpandaid')
        app_label = 'jedi'


class JediTaskparams(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='jeditaskid')
    taskparams = models.TextField(db_column='taskparams', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_taskparams"'
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
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_tasks"'
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
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    modificationhost = models.CharField(max_length=384, db_column='modificationhost', blank=True)
    status = models.CharField(max_length=64, db_column='status', blank=True)
    attemptnr = models.IntegerField(db_column='attemptnr', blank=True)
    reason = models.CharField(max_length=600, db_column='reason', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."TASKS_STATUSLOG"'
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
    queue_id = models.IntegerField(primary_key=True, db_column='queue_id')
    queue_name = models.CharField(max_length=16, db_column='queue_name') 
    queue_type = models.CharField(max_length=16, db_column='queue_type') 
    vo = models.CharField(max_length=16, db_column='vo') 
    status = models.CharField(max_length=64, db_column='status', blank=True) 
    partitionid = models.IntegerField(null=True, db_column='partitionid', blank=True)
    stretchable = models.IntegerField(null=True, db_column='stretchable', blank=True)
    queue_share = models.IntegerField(null=True, db_column='queue_share', blank=True)
    queue_order = models.IntegerField(null=True, db_column='queue_order', blank=True)
    criteria = models.CharField(max_length=256, db_column='criteria', blank=True) 
    variables = models.CharField(max_length=256, db_column='variables', blank=True) 
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jedi_work_queue"'
        app_label = 'jedi'


class Jobparamstable(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jobparameters = models.TextField(db_column='jobparameters', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jobparamstable"'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class JobparamstableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jobparameters = models.TextField(db_column='jobparameters', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_ARCH}"."jobparamstable_arch"'
        app_label = 'panda'


class JobsStatuslog(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jobstatus = models.CharField(max_length=45, db_column='jobstatus')
    prodsourcelabel = models.CharField(max_length=60, db_column='prodsourcelabel', blank=True)
    cloud = models.CharField(max_length=150, db_column='cloud', blank=True)
    computingsite = models.CharField(max_length=384, db_column='computingsite', blank=True)
    modificationhost = models.CharField(max_length=384, db_column='modificationhost', blank=True)
    modiftime_extended = models.DateTimeField(db_column='modiftime_extended')
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."JOBS_STATUSLOG"'
        app_label = 'panda'


class Jobsdebug(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='pandaid')
    stdout = models.CharField(max_length=6144, db_column='stdout', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."jobsdebug"'
        app_label = 'panda'


class Logstable(models.Model):
    pandaid = models.IntegerField(primary_key=True, db_column='pandaid') 
    log1 = models.TextField(db_column='log1') 
    log2 = models.TextField(db_column='log2') 
    log3 = models.TextField(db_column='log3') 
    log4 = models.TextField(db_column='log4') 
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_META}"."logstable"'
        app_label = 'panda'


class Metatable(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    metadata = models.TextField(db_column='metadata', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."metatable"'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class MetatableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    metadata = models.TextField(db_column='metadata', blank=True)
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


class Pandalog(models.Model):
    bintime = models.DateTimeField(db_column='bintime', primary_key=True)
    name = models.CharField(max_length=90, db_column='name', blank=True)
    module = models.CharField(max_length=90, db_column='module', blank=True)
    loguser = models.CharField(max_length=240, db_column='loguser', blank=True)
    type = models.CharField(max_length=60, db_column='type', blank=True)
    pid = models.BigIntegerField(db_column='pid')
    loglevel = models.IntegerField(db_column='loglevel')
    levelname = models.CharField(max_length=90, db_column='levelname', blank=True)
    time = models.CharField(max_length=90, db_column='time', blank=True)
    filename = models.CharField(max_length=300, db_column='filename', blank=True)
    line = models.IntegerField(db_column='line')
    message = models.CharField(max_length=12000, db_column='message', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."pandalog"'
        app_label = 'panda'


class RucioAccounts(models.Model):
    id = models.IntegerField(primary_key=True, db_column='id')
    certificatedn = models.CharField(max_length=40, db_column='certificatedn')
    rucio_account = models.CharField(max_length=40, db_column='rucio_account')
    create_time = models.DateTimeField(db_column='create_time')
    class Meta:
        db_table = f'"{DB_SCHEMA}"."RUCIO_ACCOUNTS"'
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
    site = models.CharField(max_length=90, db_column='site', primary_key=True)
    flag = models.CharField(max_length=60, db_column='flag')
    hours = models.IntegerField(db_column='hours')
    nwn = models.IntegerField(null=True, db_column='nwn', blank=True)
    memmin = models.IntegerField(null=True, db_column='memmin', blank=True)
    memmax = models.IntegerField(null=True, db_column='memmax', blank=True)
    si2000min = models.IntegerField(null=True, db_column='si2000min', blank=True)
    si2000max = models.IntegerField(null=True, db_column='si2000max', blank=True)
    os = models.CharField(max_length=90, db_column='os', blank=True)
    space = models.CharField(max_length=90, db_column='space', blank=True)
    minjobs = models.IntegerField(null=True, db_column='minjobs', blank=True)
    maxjobs = models.IntegerField(null=True, db_column='maxjobs', blank=True)
    laststart = models.DateTimeField(null=True, db_column='laststart', blank=True)
    lastend = models.DateTimeField(null=True, db_column='lastend', blank=True)
    lastfail = models.DateTimeField(null=True, db_column='lastfail', blank=True)
    lastpilot = models.DateTimeField(null=True, db_column='lastpilot', blank=True)
    lastpid = models.IntegerField(null=True, db_column='lastpid', blank=True)
    nstart = models.IntegerField(db_column='nstart')
    finished = models.IntegerField(db_column='finished')
    failed = models.IntegerField(db_column='failed')
    defined = models.IntegerField(db_column='defined')
    assigned = models.IntegerField(db_column='assigned')
    waiting = models.IntegerField(db_column='waiting')
    activated = models.IntegerField(db_column='activated')
    holding = models.IntegerField(db_column='holding')
    running = models.IntegerField(db_column='running')
    transferring = models.IntegerField(db_column='transferring')
    getjob = models.IntegerField(db_column='getjob')
    updatejob = models.IntegerField(db_column='updatejob')
    nojob = models.IntegerField(null=True, db_column='nojob', blank=True)
    lastmod = models.DateTimeField(db_column='lastmod')
    ncpu = models.IntegerField(null=True, db_column='ncpu', blank=True)
    nslot = models.IntegerField(null=True, db_column='nslot', blank=True)
    getjobabs = models.IntegerField(db_column='getjobabs')
    updatejobabs = models.IntegerField(db_column='updatejobabs')
    nojobabs = models.IntegerField(null=True, db_column='nojobabs', blank=True)
    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_META}"."sitedata"'
        unique_together = ('site', 'flag', 'hours')
        app_label = 'panda'


class Users(models.Model):
    id = models.IntegerField(primary_key=True, db_column='id')
    name = models.CharField(max_length=180, db_column='name')
    dn = models.CharField(max_length=450, db_column='dn', blank=True)
    email = models.CharField(max_length=180, db_column='email', blank=True)
    url = models.CharField(max_length=300, db_column='url', blank=True)
    location = models.CharField(max_length=180, db_column='location', blank=True)
    classa = models.CharField(max_length=90, db_column='classa', blank=True)
    classp = models.CharField(max_length=90, db_column='classp', blank=True)
    classxp = models.CharField(max_length=90, db_column='classxp', blank=True)
    sitepref = models.CharField(max_length=180, db_column='sitepref', blank=True)
    gridpref = models.CharField(max_length=60, db_column='gridpref', blank=True)
    queuepref = models.CharField(max_length=180, db_column='queuepref', blank=True)
    scriptcache = models.CharField(max_length=300, db_column='scriptcache', blank=True)
    types = models.CharField(max_length=180, db_column='types', blank=True)
    sites = models.CharField(max_length=750, db_column='sites', blank=True)
    njobsa = models.IntegerField(null=True, db_column='njobsa', blank=True)
    njobsp = models.IntegerField(null=True, db_column='njobsp', blank=True)
    njobs1 = models.IntegerField(null=True, db_column='njobs1', blank=True)
    njobs7 = models.IntegerField(null=True, db_column='njobs7', blank=True)
    njobs30 = models.IntegerField(null=True, db_column='njobs30', blank=True)
    cpua1 = models.BigIntegerField(null=True, db_column='cpua1', blank=True)
    cpua7 = models.BigIntegerField(null=True, db_column='cpua7', blank=True)
    cpua30 = models.BigIntegerField(null=True, db_column='cpua30', blank=True)
    cpup1 = models.BigIntegerField(null=True, db_column='cpup1', blank=True)
    cpup7 = models.BigIntegerField(null=True, db_column='cpup7', blank=True)
    cpup30 = models.BigIntegerField(null=True, db_column='cpup30', blank=True)
    cpuxp1 = models.BigIntegerField(null=True, db_column='cpuxp1', blank=True)
    cpuxp7 = models.BigIntegerField(null=True, db_column='cpuxp7', blank=True)
    cpuxp30 = models.BigIntegerField(null=True, db_column='cpuxp30', blank=True)
    quotaa1 = models.BigIntegerField(null=True, db_column='quotaa1', blank=True)
    quotaa7 = models.BigIntegerField(null=True, db_column='quotaa7', blank=True)
    quotaa30 = models.BigIntegerField(null=True, db_column='quotaa30', blank=True)
    quotap1 = models.BigIntegerField(null=True, db_column='quotap1', blank=True)
    quotap7 = models.BigIntegerField(null=True, db_column='quotap7', blank=True)
    quotap30 = models.BigIntegerField(null=True, db_column='quotap30', blank=True)
    quotaxp1 = models.BigIntegerField(null=True, db_column='quotaxp1', blank=True)
    quotaxp7 = models.BigIntegerField(null=True, db_column='quotaxp7', blank=True)
    quotaxp30 = models.BigIntegerField(null=True, db_column='quotaxp30', blank=True)
    space1 = models.IntegerField(null=True, db_column='space1', blank=True)
    space7 = models.IntegerField(null=True, db_column='space7', blank=True)
    space30 = models.IntegerField(null=True, db_column='space30', blank=True)
    lastmod = models.DateTimeField(db_column='lastmod')
    firstjob = models.DateTimeField(db_column='firstjob')
    latestjob = models.DateTimeField(db_column='latestjob')
    pagecache = models.TextField(db_column='pagecache', blank=True)
    cachetime = models.DateTimeField(db_column='cachetime')
    ncurrent = models.IntegerField(db_column='ncurrent')
    jobid = models.IntegerField(db_column='jobid')
    status = models.CharField(max_length=60, db_column='status', blank=True)
    vo = models.CharField(max_length=60, db_column='vo', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA_META}"."users"'
        allColumns = COLUMNS['ActiveUsers-all']
        primaryColumns = ['name']
        secondaryColumns = []
        orderColumns = ORDER_COLUMNS['ActiveUsers-all']
        columnTitles = COL_TITLES['ActiveUsers-all']
        filterFields = FILTERS['ActiveUsers-all']

    def __str__(self):
        return 'User: ' + str(self.name) + '[' + str(self.status) + ']'


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
    project = models.CharField(max_length=60, db_column='project', primary_key=True)
    begin_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='begin_time')
    end_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='end_time')
    status = models.CharField(max_length=8, db_column='status')
    status = models.CharField(max_length=500, db_column='description')
    time_stamp = models.DecimalField(decimal_places=0, max_digits=10, db_column='timestamp')

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

    reqid = models.DecimalField(decimal_places=0, max_digits=12, db_column='pr_id', primary_key=True)
    manager = models.CharField(max_length=32, db_column='manager', null=False, blank=True)
    description = models.CharField(max_length=256, db_column='description', null=True, blank=True)
    ref_link = models.CharField(max_length=256, db_column='reference_link', null=True, blank=True)
    cstatus = models.CharField(max_length=32, db_column='status', null=False, blank=True)
    provenance = models.CharField(max_length=32, db_column='provenance', null=False, blank=True,choices=PROVENANCE_TYPE)
    request_type = models.CharField(max_length=32, db_column='request_type',choices=REQUEST_TYPE, null=False, blank=True)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)
    subcampaign = models.CharField(max_length=32, db_column='sub_campaign', null=False, blank=True)
    phys_group = models.CharField(max_length=20, db_column='phys_group', null=False, choices=PHYS_GROUPS, blank=True)
    energy_gev = models.DecimalField(decimal_places=0, max_digits=8, db_column='energy_gev', null=False, blank=True)
    project = models.ForeignKey(TProject,db_column='project', null=True, blank=False, on_delete=models.DO_NOTHING)
    is_error = models.NullBooleanField(db_column='exception', null=True, blank=False)
    jira_reference = models.CharField(max_length=50, db_column='reference', null=True, blank=True)

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
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='req_s_id', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='pr_id', on_delete=models.DO_NOTHING)
    comment = models.CharField(max_length=256, db_column='comment', null=True)
    owner = models.CharField(max_length=32, db_column='owner', null=False)
    status = models.CharField(max_length=32, db_column='status', choices=STATUS_TYPES, null=False)
    timestamp = models.DateTimeField(db_column='timestamp', null=False)

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
    id =  models.DecimalField(decimal_places=0, max_digits=12,  db_column='step_t_id', primary_key=True)
    step = models.CharField(max_length=12, db_column='step_name', null=False)
    def_time = models.DateTimeField(db_column='def_time', null=False)
    status = models.CharField(max_length=12, db_column='status', null=False)
    ctag = models.CharField(max_length=12, db_column='ctag', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='priority', null=False)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=7, db_column='cpu_per_event', null=True)
    output_formats = models.CharField(max_length=250, db_column='output_formats', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='memory', null=True)
    trf_name = models.CharField(max_length=128, db_column='trf_name', null=True)
    lparams = models.CharField(max_length=2000, db_column='lparams', null=True)
    vparams = models.CharField(max_length=4000, db_column='vparams', null=True)
    swrelease = models.CharField(max_length=80, db_column='swrelease', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_STEP_TEMPLATE_ID_SEQ','T_STEP_TEMPLATE','STEP_T_ID')
        super(StepTemplate, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_STEP_TEMPLATE'
        db_table = u'T_STEP_TEMPLATE'




class ProductionDataset(models.Model):
    name = models.CharField(max_length=150, db_column='name', primary_key=True)
    #task = models.ForeignKey(ProducitonTask,db_column='task_id')
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='taskid', null=True)
    #parent_task = models.ForeignKey(ProducitonTask,db_column='task_id')
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='parent_tid', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='pr_id', null=True)
    phys_group = models.CharField(max_length=20, db_column='phys_group', null=True)
    events = models.DecimalField(decimal_places=0, max_digits=7, db_column='events', null=True)
    files = models.DecimalField(decimal_places=0, max_digits=7, db_column='files', null=False)
    status = models.CharField(max_length=12, db_column='status', null=True)
    timestamp = models.DateTimeField(db_column='timestamp', null=False)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_DATASET'


class ProductionContainer(models.Model):
    name = models.CharField(max_length=150, db_column='name', primary_key=True)
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='parent_tid', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='pr_id', null=True)
    phys_group = models.CharField(max_length=20, db_column='phys_group', null=True)
    status = models.CharField(max_length=12, db_column='status', null=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_CONTAINER'


class InputRequestList(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ind_id', primary_key=True)
    dataset = models.ForeignKey(ProductionDataset, db_column='inputdataset',null=True, on_delete=models.DO_NOTHING)
    request = models.ForeignKey(TRequest, db_column='pr_id', on_delete=models.DO_NOTHING)
    slice = models.DecimalField(decimal_places=0, max_digits=12, db_column='slice', null=False)
    brief = models.CharField(max_length=150, db_column='brief')
    phys_comment = models.CharField(max_length=256, db_column='physcomment')
    comment = models.CharField(max_length=512, db_column='slicecomment')
    input_data = models.CharField(max_length=150, db_column='inputdata')
    project_mode = models.CharField(max_length=256, db_column='project_mode')
    priority = models.DecimalField(decimal_places=0, max_digits=12, db_column='priority')
    input_events = models.DecimalField(decimal_places=0, max_digits=12, db_column='input_events')
    is_hide = models.NullBooleanField(db_column='hided', null=True, blank=False)

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
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='step_id', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='pr_id', on_delete=models.DO_NOTHING)
    step_template = models.ForeignKey(StepTemplate, db_column='step_t_id', on_delete=models.DO_NOTHING)
    status = models.CharField(max_length=12, db_column='status', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='ind_id', null=False, on_delete=models.DO_NOTHING)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='priority', null=False)
    step_def_time = models.DateTimeField(db_column='step_def_time', null=False)
    step_appr_time = models.DateTimeField(db_column='step_appr_time', null=True)
    step_exe_time = models.DateTimeField(db_column='step_exe_time', null=True)
    step_done_time = models.DateTimeField(db_column='step_done_time', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='input_events', null=True)
    task_config = models.CharField(max_length=2000, db_column='task_config')
    step_parent = models.ForeignKey('self', db_column='step_parent_id', on_delete=models.DO_NOTHING)

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
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='taskid', primary_key=True)
    _jedi_task_parameters = models.TextField(db_column='jedi_task_parameters')

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
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='taskid', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='step_id', on_delete=models.DO_NOTHING)
    request = models.ForeignKey(TRequest, db_column='pr_id', on_delete=models.DO_NOTHING)
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='parent_tid', null=False)
    chain_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='chain_tid', null=False)
    name = models.CharField(max_length=130, db_column='taskname', null=True)
    project = models.CharField(max_length=60, db_column='project', null=True)
    username = models.CharField(max_length=128, db_column='username', null=True)
    dsn = models.CharField(max_length=12, db_column='dsn', null=True)
    phys_short = models.CharField(max_length=80, db_column='phys_short', null=True)
    simulation_type = models.CharField(max_length=20, db_column='simulation_type', null=True)
    phys_group = models.CharField(max_length=20, db_column='phys_group', null=True)
    provenance = models.CharField(max_length=12, db_column='provenance', null=True)
    status = models.CharField(max_length=12, db_column='status', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='total_events', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='total_req_jobs', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='total_done_jobs', null=True)
    submit_time = models.DateTimeField(db_column='submit_time', null=False)
    start_time = models.DateTimeField(db_column='start_time', null=True)
    timestamp = models.DateTimeField(db_column='timestamp', null=True)
    pptimestamp = models.DateTimeField(db_column='pptimestamp', null=True)
    postproduction = models.CharField(max_length=128, db_column='postproduction', null=True)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='priority', null=True)
    current_priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='current_priority', null=True)
    update_time = models.DateTimeField(db_column='update_time', null=True)
    update_owner = models.CharField(max_length=24, db_column='update_owner', null=True)
    comments = models.CharField(max_length=256, db_column='comments', null=True)
    inputdataset = models.CharField(max_length=150, db_column='inputdataset', null=True)
    physics_tag = models.CharField(max_length=20, db_column='physics_tag', null=True)
    reference = models.CharField(max_length=150, db_column='reference', null=False)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)

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
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='mcp_id', primary_key=True)
    pattern_name =  models.CharField(max_length=150, db_column='pattern_name', unique=True)
    pattern_dict = models.CharField(max_length=2000, db_column='pattern_dict')
    pattern_status = models.CharField(max_length=20, db_column='pattern_status', choices=STATUS)

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
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='mcprior_id', primary_key=True)
    priority_key = models.DecimalField(decimal_places=0, max_digits=12, db_column='priority_key', unique=True)
    priority_dict = models.CharField(max_length=2000, db_column='priority_dict')

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


