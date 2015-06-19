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
from django.db import connection
from django.db import connections
from django.utils import timezone


from django.db import models
models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)

class Cache(models.Model):
    type = models.CharField(db_column='TYPE', max_length=250) 
    value = models.CharField(db_column='VALUE', max_length=250) 
    qurl = models.CharField(db_column='QURL', max_length=250) 
    modtime = models.DateTimeField(db_column='MODTIME')
    usetime = models.DateTimeField(db_column='USETIME')
    updmin = models.IntegerField(null=True, db_column='UPDMIN', blank=True)
    data = models.TextField(db_column='DATA', blank=True)
    class Meta:
        db_table = u'cache'

class Certificates(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    cert = models.CharField(max_length=12000, db_column='CERT')
    class Meta:
        db_table = u'certificates'

class Classlist(models.Model):
    class_field = models.CharField(max_length=90, db_column='CLASS', primary_key=True)  # Field renamed because it was a Python reserved word.
    name = models.CharField(max_length=180, db_column='NAME', primary_key=True)
    rights = models.CharField(max_length=90, db_column='RIGHTS')
    priority = models.IntegerField(null=True, db_column='PRIORITY', blank=True)
    quota1 = models.BigIntegerField(null=True, db_column='QUOTA1', blank=True)
    quota7 = models.BigIntegerField(null=True, db_column='QUOTA7', blank=True)
    quota30 = models.BigIntegerField(null=True, db_column='QUOTA30', blank=True)
    class Meta:
        db_table = u'classlist'
        unique_together = ('class_field', 'name')

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

class Cloudspace(models.Model):
    cloud = models.CharField(max_length=60, db_column='CLOUD', primary_key=True)
    store = models.CharField(max_length=150, db_column='STORE', primary_key=True)
    space = models.IntegerField(db_column='SPACE')
    freespace = models.IntegerField(db_column='FREESPACE')
    moduser = models.CharField(max_length=90, db_column='MODUSER')
    modtime = models.DateTimeField(db_column='MODTIME')
    class Meta:
        db_table = u'cloudspace'
        unique_together = ('cloud', 'store')


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
    modificationdate = models.DateTimeField(db_column='MODIFICATIONDATE', primary_key=True)
    moverid = models.BigIntegerField(db_column='MOVERID')
    transferstatus = models.IntegerField(db_column='TRANSFERSTATUS')
    subtype = models.CharField(max_length=15, db_column='SUBTYPE', blank=True)
    class Meta:
        db_table = u'datasets'
        unique_together = ('vuid', 'modificationdate')

class DeftDataset(models.Model):
    dataset_id = models.CharField(db_column='DATASET_ID', primary_key=True, max_length=255) 
    dataset_meta = models.BigIntegerField(db_column='DATASET_META', blank=True, null=True) 
    dataset_state = models.CharField(db_column='DATASET_STATE', max_length=16, blank=True) 
    dataset_source = models.BigIntegerField(db_column='DATASET_SOURCE', blank=True, null=True) 
    dataset_target = models.BigIntegerField(db_column='DATASET_TARGET', blank=True, null=True) 
    dataset_comment = models.CharField(db_column='DATASET_COMMENT', max_length=128, blank=True) 
    class Meta:
        managed = False
        db_table = 'deft_dataset'

class DeftMeta(models.Model):
    meta_id = models.BigIntegerField(primary_key=True, db_column='META_ID')
    meta_state = models.CharField(max_length=48, db_column='META_STATE', blank=True)
    meta_comment = models.CharField(max_length=384, db_column='META_COMMENT', blank=True)
    meta_req_ts = models.DateTimeField(null=True, db_column='META_REQ_TS', blank=True)
    meta_upd_ts = models.DateTimeField(null=True, db_column='META_UPD_TS', blank=True)
    meta_requestor = models.CharField(max_length=48, db_column='META_REQUESTOR', blank=True)
    meta_manager = models.CharField(max_length=48, db_column='META_MANAGER', blank=True)
    meta_vo = models.CharField(max_length=48, db_column='META_VO', blank=True)
    class Meta:
        db_table = u'deft_meta'

class DeftTask(models.Model):
    task_id = models.BigIntegerField(primary_key=True, db_column='TASK_ID')
    task_meta = models.BigIntegerField(null=True, db_column='TASK_META', blank=True)
    task_state = models.CharField(max_length=48, db_column='TASK_STATE', blank=True)
    task_param = models.TextField(db_column='TASK_PARAM', blank=True)
    task_tag = models.CharField(max_length=48, db_column='TASK_TAG', blank=True)
    task_comment = models.CharField(max_length=384, db_column='TASK_COMMENT', blank=True)
    task_vo = models.CharField(max_length=48, db_column='TASK_VO', blank=True)
    task_transpath = models.CharField(max_length=384, db_column='TASK_TRANSPATH', blank=True)
    class Meta:
        db_table = u'deft_task'

class Dslist(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    duid = models.CharField(max_length=120, db_column='DUID', blank=True)
    name = models.CharField(max_length=600, db_column='NAME')
    ugid = models.IntegerField(null=True, db_column='UGID', blank=True)
    priority = models.IntegerField(null=True, db_column='PRIORITY', blank=True)
    status = models.CharField(max_length=30, db_column='STATUS', blank=True)
    lastuse = models.DateTimeField(db_column='LASTUSE')
    pinstate = models.CharField(max_length=30, db_column='PINSTATE', blank=True)
    pintime = models.DateTimeField(db_column='PINTIME')
    lifetime = models.DateTimeField(db_column='LIFETIME')
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    par1 = models.CharField(max_length=90, db_column='PAR1', blank=True)
    par2 = models.CharField(max_length=90, db_column='PAR2', blank=True)
    par3 = models.CharField(max_length=90, db_column='PAR3', blank=True)
    par4 = models.CharField(max_length=90, db_column='PAR4', blank=True)
    par5 = models.CharField(max_length=90, db_column='PAR5', blank=True)
    par6 = models.CharField(max_length=90, db_column='PAR6', blank=True)
    class Meta:
        db_table = u'dslist'

class Etask(models.Model):
    taskid = models.IntegerField(primary_key=True, db_column='TASKID')
    creationtime = models.DateTimeField(db_column='CREATIONTIME')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    taskname = models.CharField(max_length=768, db_column='TASKNAME', blank=True)
    status = models.CharField(max_length=384, db_column='STATUS', blank=True)
    username = models.CharField(max_length=768, db_column='USERNAME', blank=True)
    usergroup = models.CharField(max_length=96, db_column='USERGROUP', blank=True) 
    userrole = models.CharField(max_length=96, db_column='USERROLE', blank=True) 
    actualpars = models.CharField(max_length=6000, db_column='ACTUALPARS', blank=True)
    cpucount = models.IntegerField(db_column='CPUCOUNT', blank=True)
    cpuunit = models.CharField(max_length=96, db_column='CPUUNIT', blank=True)
    diskcount = models.IntegerField(db_column='DISKCOUNT', blank=True)
    diskunit = models.CharField(max_length=96, db_column='DISKUNIT', blank=True)
    ramcount = models.IntegerField(db_column='RAMCOUNT', blank=True)
    ramunit = models.CharField(max_length=96, db_column='RAMUNIT', blank=True)
    outip = models.CharField(max_length=9, db_column='OUTIP', blank=True)
    tasktype = models.CharField(max_length=96, db_column='TASKTYPE', blank=True)
    grid = models.CharField(max_length=96, db_column='GRID', blank=True)
    transfk = models.IntegerField(db_column='TRANSFK', blank=True)
    transuses = models.CharField(max_length=768, db_column='TRANSUSES', blank=True)
    transhome = models.CharField(max_length=768, db_column='TRANSHOME', blank=True)
    transpath = models.CharField(max_length=768, db_column='TRANSPATH', blank=True)
    transformalpars = models.CharField(max_length=768, db_column='TRANSFORMALPARS', blank=True)
    tier = models.CharField(max_length=36, db_column='TIER', blank=True)
    ndone = models.IntegerField(db_column='NDONE', blank=True)
    ntotal = models.IntegerField(db_column='NTOTAL', blank=True)
    nevents = models.BigIntegerField(db_column='NEVENTS', blank=True)
    relpriority = models.CharField(max_length=30, db_column='RELPRIORITY', blank=True)
    expevtperjob = models.BigIntegerField(db_column='EXPEVTPERJOB', blank=True)
    tasktransinfo = models.CharField(max_length=1536, db_column='TASKTRANSINFO', blank=True)
    extid1 = models.BigIntegerField(db_column='EXTID1', blank=True)
    reqid = models.BigIntegerField(db_column='REQID', blank=True)
    expntotal = models.BigIntegerField(db_column='EXPNTOTAL', blank=True)
    cmtconfig = models.CharField(max_length=768, db_column='CMTCONFIG', blank=True)
    site = models.CharField(max_length=384, db_column='SITE', blank=True)
    tasktype2 = models.CharField(max_length=192, db_column='TASKTYPE2', blank=True)
    taskpriority = models.IntegerField(db_column='TASKPRIORITY', blank=True)
    partid = models.CharField(max_length=192, db_column='PARTID', blank=True)
    taskpars = models.CharField(max_length=3072, db_column='TASKPARS', blank=True)
    fillstatus = models.CharField(max_length=192, db_column='FILLSTATUS', blank=True)
    rw = models.BigIntegerField(db_column='RW', blank=True)
    jobsremaining = models.BigIntegerField(db_column='JOBSREMAINING', blank=True)
    cpuperjob = models.IntegerField(db_column='CPUPERJOB', blank=True)

    class Meta:
        db_table = u'etask'

class Filestable4(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
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

class FilestableArch(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID') 
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
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

class Groups(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    name = models.CharField(max_length=180, db_column='NAME')
    description = models.CharField(max_length=360, db_column='DESCRIPTION')
    url = models.CharField(max_length=300, db_column='URL', blank=True)
    classa = models.CharField(max_length=90, db_column='CLASSA', blank=True)
    classp = models.CharField(max_length=90, db_column='CLASSP', blank=True)
    classxp = models.CharField(max_length=90, db_column='CLASSXP', blank=True)
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
    allcpua1 = models.BigIntegerField(null=True, db_column='ALLCPUA1', blank=True)
    allcpua7 = models.BigIntegerField(null=True, db_column='ALLCPUA7', blank=True)
    allcpua30 = models.BigIntegerField(null=True, db_column='ALLCPUA30', blank=True)
    allcpup1 = models.BigIntegerField(null=True, db_column='ALLCPUP1', blank=True)
    allcpup7 = models.BigIntegerField(null=True, db_column='ALLCPUP7', blank=True)
    allcpup30 = models.BigIntegerField(null=True, db_column='ALLCPUP30', blank=True)
    allcpuxp1 = models.BigIntegerField(null=True, db_column='ALLCPUXP1', blank=True)
    allcpuxp7 = models.BigIntegerField(null=True, db_column='ALLCPUXP7', blank=True)
    allcpuxp30 = models.BigIntegerField(null=True, db_column='ALLCPUXP30', blank=True)
    quotaa1 = models.BigIntegerField(null=True, db_column='QUOTAA1', blank=True)
    quotaa7 = models.BigIntegerField(null=True, db_column='QUOTAA7', blank=True)
    quotaa30 = models.BigIntegerField(null=True, db_column='QUOTAA30', blank=True)
    quotap1 = models.BigIntegerField(null=True, db_column='QUOTAP1', blank=True)
    quotap7 = models.BigIntegerField(null=True, db_column='QUOTAP7', blank=True)
    quotap30 = models.BigIntegerField(null=True, db_column='QUOTAP30', blank=True)
    quotaxp1 = models.BigIntegerField(null=True, db_column='QUOTAXP1', blank=True)
    quotaxp7 = models.BigIntegerField(null=True, db_column='QUOTAXP7', blank=True)
    quotaxp30 = models.BigIntegerField(null=True, db_column='QUOTAXP30', blank=True)
    allquotaa1 = models.BigIntegerField(null=True, db_column='ALLQUOTAA1', blank=True)
    allquotaa7 = models.BigIntegerField(null=True, db_column='ALLQUOTAA7', blank=True)
    allquotaa30 = models.BigIntegerField(null=True, db_column='ALLQUOTAA30', blank=True)
    allquotap1 = models.BigIntegerField(null=True, db_column='ALLQUOTAP1', blank=True)
    allquotap7 = models.BigIntegerField(null=True, db_column='ALLQUOTAP7', blank=True)
    allquotap30 = models.BigIntegerField(null=True, db_column='ALLQUOTAP30', blank=True)
    allquotaxp1 = models.BigIntegerField(null=True, db_column='ALLQUOTAXP1', blank=True)
    allquotaxp7 = models.BigIntegerField(null=True, db_column='ALLQUOTAXP7', blank=True)
    allquotaxp30 = models.BigIntegerField(null=True, db_column='ALLQUOTAXP30', blank=True)
    space1 = models.IntegerField(null=True, db_column='SPACE1', blank=True)
    space7 = models.IntegerField(null=True, db_column='SPACE7', blank=True)
    space30 = models.IntegerField(null=True, db_column='SPACE30', blank=True)
    class Meta:
        db_table = u'groups'

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

class InfomodelsSitestatus(models.Model):
    id = models.BigIntegerField(primary_key=True, db_column='ID')
    sitename = models.CharField(max_length=180, db_column='SITENAME', blank=True)
    active = models.IntegerField(null=True, db_column='ACTIVE', blank=True)
    class Meta:
        db_table = u'infomodels_sitestatus'

class Installedsw(models.Model):
    siteid = models.CharField(max_length=180, db_column='SITEID', primary_key=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    release = models.CharField(max_length=30, db_column='RELEASE', primary_key=True)
    cache = models.CharField(max_length=120, db_column='CACHE', primary_key=True)
    validation = models.CharField(max_length=30, db_column='VALIDATION', blank=True)
    cmtconfig = models.CharField(max_length=120, db_column='CMTCONFIG', primary_key=True)
    class Meta:
        db_table = u'installedsw'
        unique_together = ('siteid', 'release', 'cache', 'cmtconfig')

class Jdllist(models.Model):
    name = models.CharField(max_length=180, primary_key=True, db_column='NAME')
    host = models.CharField(max_length=180, db_column='HOST', blank=True)
    system = models.CharField(max_length=60, db_column='SYSTEM')
    jdl = models.CharField(max_length=12000, db_column='JDL', blank=True)
    class Meta:
        db_table = u'jdllist'

class JediAuxStatusMintaskid(models.Model):
    status = models.CharField(max_length=192, primary_key=True, db_column='STATUS')
    min_jeditaskid = models.BigIntegerField(db_column='MIN_JEDITASKID')
    class Meta:
        db_table = u'jedi_aux_status_mintaskid'

class JediDatasetContents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', primary_key=True)
    fileid = models.BigIntegerField(db_column='FILEID', primary_key=True)
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
    class Meta:
        db_table = u'jedi_dataset_contents'
        unique_together = ('jeditaskid', 'datasetid', 'fileid')

class JediDatasets(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', primary_key=True)
    datasetname = models.CharField(max_length=765, db_column='DATASETNAME')
    type = models.CharField(max_length=60, db_column='TYPE')
    creationtime = models.DateTimeField(db_column='CREATIONTIME')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    masterid = models.BigIntegerField(null=True, db_column='MASTERID', blank=True)
    provenanceid = models.BigIntegerField(null=True, db_column='PROVENANCEID', blank=True)
    containername = models.CharField(max_length=396, db_column='CONTAINERNAME', blank=True)
    status = models.CharField(max_length=60, db_column='STATUS', blank=True)
    state = models.CharField(max_length=60, db_column='STATE', blank=True)
    statechecktime = models.DateTimeField(null=True, db_column='STATECHECKTIME', blank=True)
    statecheckexpiration = models.DateTimeField(null=True, db_column='STATECHECKEXPIRATION', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='FROZENTIME', blank=True)
    nfiles = models.IntegerField(null=True, db_column='NFILES', blank=True)
    nfilestobeused = models.IntegerField(null=True, db_column='NFILESTOBEUSED', blank=True)
    nfilesused = models.IntegerField(null=True, db_column='NFILESUSED', blank=True)
    nevents = models.BigIntegerField(null=True, db_column='NEVENTS', blank=True)
    neventstobeused = models.BigIntegerField(null=True, db_column='NEVENTSTOBEUSED', blank=True)
    neventsused = models.BigIntegerField(null=True, db_column='NEVENTSUSED', blank=True)
    lockedby = models.CharField(max_length=120, db_column='LOCKEDBY', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='LOCKEDTIME', blank=True)
    nfilesfinished = models.IntegerField(null=True, db_column='NFILESFINISHED', blank=True)
    nfilesfailed = models.IntegerField(null=True, db_column='NFILESFAILED', blank=True)
    attributes = models.CharField(max_length=300, db_column='ATTRIBUTES', blank=True)
    streamname = models.CharField(max_length=60, db_column='STREAMNAME', blank=True)
    storagetoken = models.CharField(max_length=180, db_column='STORAGETOKEN', blank=True)
    destination = models.CharField(max_length=180, db_column='DESTINATION', blank=True)
    nfilesonhold = models.IntegerField(null=True, db_column='NFILESONHOLD', blank=True)
    templateid = models.BigIntegerField(db_column='TEMPLATEID', blank=True)
    class Meta:
        db_table = u'jedi_datasets'
        unique_together = ('jeditaskid', 'datasetid')

class JediEvents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    fileid = models.BigIntegerField(db_column='FILEID', primary_key=True)
    job_processid = models.IntegerField(db_column='JOB_PROCESSID', primary_key=True)
    def_min_eventid = models.IntegerField(null=True, db_column='DEF_MIN_EVENTID', blank=True)
    def_max_eventid = models.IntegerField(null=True, db_column='DEF_MAX_EVENTID', blank=True)
    processed_upto_eventid = models.IntegerField(null=True, db_column='PROCESSED_UPTO_EVENTID', blank=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', blank=True)
    status = models.IntegerField(db_column='STATUS', blank=True)
    attemptnr = models.IntegerField(db_column='ATTEMPTNR', blank=True)
    class Meta:
        db_table = u'jedi_events'
        unique_together = ('jeditaskid', 'pandaid', 'fileid', 'job_processid')

class JediJobparamsTemplate(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='JEDITASKID')
    jobparamstemplate = models.TextField(db_column='JOBPARAMSTEMPLATE', blank=True)
    class Meta:
        db_table = u'jedi_jobparams_template'

class JediJobRetryHistory(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    oldpandaid = models.BigIntegerField(db_column='OLDPANDAID', primary_key=True)
    newpandaid = models.BigIntegerField(db_column='NEWPANDAID', primary_key=True)
    ins_utc_tstamp = models.BigIntegerField(db_column='INS_UTC_TSTAMP', blank=True) 
    relationtype = models.CharField(max_length=48, db_column='RELATIONTYPE')
    class Meta:
        db_table = u'jedi_job_retry_history'
        unique_together = ('jeditaskid', 'oldpandaid', 'newpandaid')

class JediOutputTemplate(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', primary_key=True)
    outtempid = models.BigIntegerField(db_column='OUTTEMPID', primary_key=True)
    filenametemplate = models.CharField(max_length=768, db_column='FILENAMETEMPLATE')
    maxserialnr = models.IntegerField(null=True, db_column='MAXSERIALNR', blank=True)
    serialnr = models.IntegerField(null=True, db_column='SERIALNR', blank=True)
    sourcename = models.CharField(max_length=768, db_column='SOURCENAME', blank=True)
    streamname = models.CharField(max_length=60, db_column='STREAMNAME', blank=True)
    outtype = models.CharField(max_length=60, db_column='OUTTYPE', blank=True)
    class Meta:
        db_table = u'jedi_output_template'
        unique_together = ('jeditaskid', 'datasetid', 'outtempid')

class JediTaskparams(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='JEDITASKID')
    taskparams = models.TextField(db_column='TASKPARAMS', blank=True)
    class Meta:
        db_table = u'jedi_taskparams'

class JediTasks(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='JEDITASKID')
    taskname = models.CharField(max_length=384, db_column='TASKNAME', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS')
    username = models.CharField(max_length=384, db_column='USERNAME')
    creationdate = models.DateTimeField(db_column='CREATIONDATE') 
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    reqid = models.IntegerField(null=True, db_column='REQID', blank=True)
    oldstatus = models.CharField(max_length=192, db_column='OLDSTATUS', blank=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    starttime = models.DateTimeField(null=True, db_column='STARTTIME', blank=True)
    endtime = models.DateTimeField(null=True, db_column='ENDTIME', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='FROZENTIME', blank=True)
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='WORKINGGROUP', blank=True)
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)
    tasktype = models.CharField(max_length=192, db_column='TASKTYPE', blank=True)
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True)
    taskpriority = models.IntegerField(null=True, db_column='TASKPRIORITY', blank=True)
    currentpriority = models.IntegerField(null=True, db_column='CURRENTPRIORITY', blank=True)
    architecture = models.CharField(max_length=768, db_column='ARCHITECTURE', blank=True)
    transuses = models.CharField(max_length=192, db_column='TRANSUSES', blank=True)
    transhome = models.CharField(max_length=384, db_column='TRANSHOME', blank=True)
    transpath = models.CharField(max_length=384, db_column='TRANSPATH', blank=True)
    lockedby = models.CharField(max_length=120, db_column='LOCKEDBY', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='LOCKEDTIME', blank=True)
    termcondition = models.CharField(max_length=300, db_column='TERMCONDITION', blank=True)
    splitrule = models.CharField(max_length=300, db_column='SPLITRULE', blank=True)
    walltime = models.IntegerField(null=True, db_column='WALLTIME', blank=True)
    walltimeunit = models.CharField(max_length=96, db_column='WALLTIMEUNIT', blank=True)
    outdiskcount = models.IntegerField(null=True, db_column='OUTDISKCOUNT', blank=True)
    outdiskunit = models.CharField(max_length=96, db_column='OUTDISKUNIT', blank=True)
    workdiskcount = models.IntegerField(null=True, db_column='WORKDISKCOUNT', blank=True)
    workdiskunit = models.CharField(max_length=96, db_column='WORKDISKUNIT', blank=True)
    ramcount = models.IntegerField(null=True, db_column='RAMCOUNT', blank=True)
    ramunit = models.CharField(max_length=96, db_column='RAMUNIT', blank=True)
    iointensity = models.IntegerField(null=True, db_column='IOINTENSITY', blank=True)
    iointensityunit = models.CharField(max_length=96, db_column='IOINTENSITYUNIT', blank=True)
    workqueue_id = models.IntegerField(null=True, db_column='WORKQUEUE_ID', blank=True)
    progress = models.IntegerField(null=True, db_column='PROGRESS', blank=True)
    failurerate = models.IntegerField(null=True, db_column='FAILURERATE', blank=True)
    errordialog = models.CharField(max_length=765, db_column='ERRORDIALOG', blank=True)
    countrygroup = models.CharField(max_length=20, db_column='COUNTRYGROUP', blank=True) 
    parent_tid = models.BigIntegerField(db_column='PARENT_TID', blank=True) 
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True)
    ticketid = models.CharField(max_length=50, db_column='TICKETID', blank=True) 
    ticketsystemtype = models.CharField(max_length=16, db_column='TICKETSYSTEMTYPE', blank=True) 
    statechangetime = models.DateTimeField(null=True, db_column='STATECHANGETIME', blank=True) 
    superstatus = models.CharField(max_length=64, db_column='SUPERSTATUS', blank=True) 
    campaign = models.CharField(max_length=72, db_column='CAMPAIGN', blank=True)
    class Meta:
        db_table = u'jedi_tasks'


class GetEventsForTask(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    totevrem = models.BigIntegerField(db_column='totevrem')
    totev = models.BigIntegerField(db_column='totev')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."GETEVENTSFORTASK"'


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
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True)
    class Meta:
        db_table = u'jobparamstable'
        unique_together = ('pandaid', 'modificationtime')

class JobparamstableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True)
    class Meta:
        db_table = u'jobparamstable_arch'

class JobsStatuslog(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS')
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    cloud = models.CharField(max_length=150, db_column='CLOUD', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True)
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True)
    class Meta:
        db_table = u'jobs_statuslog'


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

class Logstable(models.Model):
    pandaid = models.IntegerField(primary_key=True, db_column='PANDAID') 
    log1 = models.TextField(db_column='LOG1') 
    log2 = models.TextField(db_column='LOG2') 
    log3 = models.TextField(db_column='LOG3') 
    log4 = models.TextField(db_column='LOG4') 
    class Meta:
        db_table = u'logstable'

class Members(models.Model):
    uname = models.CharField(max_length=90, db_column='UNAME', primary_key=True)
    gname = models.CharField(max_length=90, db_column='GNAME', primary_key=True)
    rights = models.CharField(max_length=90, db_column='RIGHTS', blank=True)
    since = models.DateTimeField(db_column='SINCE')
    class Meta:
        db_table = u'members'
        unique_together = ('uname', 'gname')


class Metatable(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
    metadata = models.TextField(db_column='METADATA', blank=True)
    class Meta:
        db_table = u'metatable'
        unique_together = ('pandaid', 'modificationtime')

class MetatableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    metadata = models.TextField(db_column='METADATA', blank=True)
    class Meta:
        db_table = u'metatable_arch'

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
    modiftime = models.DateTimeField(db_column='MODIFTIME', primary_key=True)
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

class Passwords(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    pass_field = models.CharField(max_length=180, db_column='PASS')  # Field renamed because it was a Python reserved word.
    class Meta:
        db_table = u'passwords'

class Pilotqueue(models.Model):
    jobid = models.CharField(db_column='JOBID', max_length=100, primary_key=True)
    tpid = models.CharField(max_length=180, db_column='TPID')
    url = models.CharField(max_length=600, db_column='URL', blank=True)
    nickname = models.CharField(max_length=180, db_column='NICKNAME', primary_key=True)
    system = models.CharField(max_length=60, db_column='SYSTEM')
    user_field = models.CharField(max_length=180, db_column='USER_')  # Field renamed because it was a Python reserved word.
    host = models.CharField(max_length=180, db_column='HOST')
    submithost = models.CharField(max_length=180, db_column='SUBMITHOST')
    queueid = models.CharField(max_length=180, db_column='QUEUEID')
    type = models.CharField(max_length=60, db_column='TYPE')
    pandaid = models.IntegerField(null=True, db_column='PANDAID', blank=True)
    tcheck = models.DateTimeField(db_column='TCHECK')
    state = models.CharField(max_length=90, db_column='STATE')
    tstate = models.DateTimeField(db_column='TSTATE')
    tenter = models.DateTimeField(db_column='TENTER')
    tsubmit = models.DateTimeField(db_column='TSUBMIT')
    taccept = models.DateTimeField(db_column='TACCEPT')
    tschedule = models.DateTimeField(db_column='TSCHEDULE')
    tstart = models.DateTimeField(db_column='TSTART')
    tend = models.DateTimeField(db_column='TEND')
    tdone = models.DateTimeField(db_column='TDONE')
    tretrieve = models.DateTimeField(db_column='TRETRIEVE')
    status = models.CharField(max_length=60, db_column='STATUS')
    errcode = models.IntegerField(db_column='ERRCODE')
    errinfo = models.CharField(max_length=450, db_column='ERRINFO')
    message = models.CharField(max_length=12000, db_column='MESSAGE', blank=True)
    schedd_name = models.CharField(max_length=180, db_column='SCHEDD_NAME')
    workernode = models.CharField(max_length=180, db_column='WORKERNODE')
    class Meta:
        db_table = u'pilotqueue'
        unique_together = ('jobid', 'nickname')

class PilotqueueBnl(models.Model):
    jobid = models.CharField(max_length=300, db_column='JOBID')
    tpid = models.CharField(max_length=180, primary_key=True, db_column='TPID')
    url = models.CharField(max_length=600, db_column='URL')
    nickname = models.CharField(max_length=180, db_column='NICKNAME')
    system = models.CharField(max_length=60, db_column='SYSTEM')
    user_field = models.CharField(max_length=180, db_column='USER_')  # Field renamed because it was a Python reserved word.
    host = models.CharField(max_length=180, db_column='HOST')
    submithost = models.CharField(max_length=180, db_column='SUBMITHOST')
    schedd_name = models.CharField(max_length=180, db_column='SCHEDD_NAME')
    queueid = models.CharField(max_length=180, db_column='QUEUEID')
    type = models.CharField(max_length=60, db_column='TYPE')
    pandaid = models.IntegerField(null=True, db_column='PANDAID', blank=True)
    tcheck = models.DateTimeField(db_column='TCHECK')
    state = models.CharField(max_length=90, db_column='STATE')
    tstate = models.DateTimeField(db_column='TSTATE')
    tenter = models.DateTimeField(db_column='TENTER')
    tsubmit = models.DateTimeField(db_column='TSUBMIT')
    taccept = models.DateTimeField(db_column='TACCEPT')
    tschedule = models.DateTimeField(db_column='TSCHEDULE')
    tstart = models.DateTimeField(db_column='TSTART')
    tend = models.DateTimeField(db_column='TEND')
    tdone = models.DateTimeField(db_column='TDONE')
    tretrieve = models.DateTimeField(db_column='TRETRIEVE')
    status = models.CharField(max_length=60, db_column='STATUS')
    errcode = models.IntegerField(db_column='ERRCODE')
    errinfo = models.CharField(max_length=450, db_column='ERRINFO')
    message = models.CharField(max_length=12000, db_column='MESSAGE', blank=True)
    workernode = models.CharField(max_length=180, db_column='WORKERNODE')
    class Meta:
        db_table = u'pilotqueue_bnl'

class Pilottoken(models.Model):
    token = models.CharField(max_length=192, primary_key=True, db_column='TOKEN')
    schedulerhost = models.CharField(max_length=300, db_column='SCHEDULERHOST', blank=True)
    scheduleruser = models.CharField(max_length=450, db_column='SCHEDULERUSER', blank=True)
    usages = models.IntegerField(db_column='USAGES')
    created = models.DateTimeField(db_column='CREATED')
    expires = models.DateTimeField(db_column='EXPIRES')
    schedulerid = models.CharField(max_length=240, db_column='SCHEDULERID', blank=True)
    class Meta:
        db_table = u'pilottoken'

class Pilottype(models.Model):
    name = models.CharField(max_length=180, primary_key=True, db_column='NAME')
    script = models.CharField(max_length=180, db_column='SCRIPT')
    url = models.CharField(max_length=450, db_column='URL')
    system = models.CharField(max_length=180, db_column='SYSTEM')
    class Meta:
        db_table = u'pilottype'

class PoolCollLock(models.Model):
    id = models.CharField(max_length=150, primary_key=True, db_column='ID')
    collection = models.CharField(max_length=1500, db_column='COLLECTION', blank=True)
    client_info = models.CharField(max_length=1500, db_column='CLIENT_INFO', blank=True)
    locktype = models.CharField(max_length=60, db_column='LOCKTYPE', blank=True)
    timestamp = models.DateTimeField(null=True, db_column='TIMESTAMP', blank=True)
    class Meta:
        db_table = u'pool_coll_lock'

class PoolCollectionData(models.Model):
    id = models.DecimalField(decimal_places=0, primary_key=True, db_column='ID', max_digits=11)
    oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='OID_1', blank=True)
    oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='OID_2', blank=True)
    var_1_oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_1_OID_1', blank=True)
    var_1_oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_1_OID_2', blank=True)
    var_2_oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_2_OID_1', blank=True)
    var_2_oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_2_OID_2', blank=True)
    var_3 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_3', blank=True)
    var_4 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_4', blank=True)
    var_5 = models.FloatField(null=True, db_column='VAR_5', blank=True)
    var_6 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_6', blank=True)
    var_7 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_7', blank=True)
    var_8 = models.FloatField(null=True, db_column='VAR_8', blank=True)
    var_9 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_9', blank=True)
    var_10 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_10', blank=True)
    var_11 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_11', blank=True)
    var_12 = models.FloatField(null=True, db_column='VAR_12', blank=True)
    var_13 = models.FloatField(null=True, db_column='VAR_13', blank=True)
    var_14 = models.FloatField(null=True, db_column='VAR_14', blank=True)
    var_15 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_15', blank=True)
    var_16 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_16', blank=True)
    var_17 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_17', blank=True)
    var_18 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_18', blank=True)
    var_19 = models.FloatField(null=True, db_column='VAR_19', blank=True)
    var_20 = models.FloatField(null=True, db_column='VAR_20', blank=True)
    var_21 = models.FloatField(null=True, db_column='VAR_21', blank=True)
    var_22 = models.FloatField(null=True, db_column='VAR_22', blank=True)
    var_23 = models.FloatField(null=True, db_column='VAR_23', blank=True)
    var_24 = models.FloatField(null=True, db_column='VAR_24', blank=True)
    var_25 = models.FloatField(null=True, db_column='VAR_25', blank=True)
    var_26 = models.FloatField(null=True, db_column='VAR_26', blank=True)
    var_27 = models.FloatField(null=True, db_column='VAR_27', blank=True)
    var_28 = models.FloatField(null=True, db_column='VAR_28', blank=True)
    var_29 = models.FloatField(null=True, db_column='VAR_29', blank=True)
    var_30 = models.FloatField(null=True, db_column='VAR_30', blank=True)
    var_31 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_31', blank=True)
    var_32 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_32', blank=True)
    var_33 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_33', blank=True)
    var_34 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_34', blank=True)
    var_35 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_35', blank=True)
    var_36 = models.FloatField(null=True, db_column='VAR_36', blank=True)
    var_37 = models.FloatField(null=True, db_column='VAR_37', blank=True)
    var_38 = models.FloatField(null=True, db_column='VAR_38', blank=True)
    var_39 = models.FloatField(null=True, db_column='VAR_39', blank=True)
    var_40 = models.FloatField(null=True, db_column='VAR_40', blank=True)
    var_41 = models.FloatField(null=True, db_column='VAR_41', blank=True)
    var_42 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_42', blank=True)
    var_43 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_43', blank=True)
    var_44 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_44', blank=True)
    var_45 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_45', blank=True)
    var_46 = models.FloatField(null=True, db_column='VAR_46', blank=True)
    var_47 = models.FloatField(null=True, db_column='VAR_47', blank=True)
    var_48 = models.FloatField(null=True, db_column='VAR_48', blank=True)
    var_49 = models.FloatField(null=True, db_column='VAR_49', blank=True)
    var_50 = models.FloatField(null=True, db_column='VAR_50', blank=True)
    var_51 = models.FloatField(null=True, db_column='VAR_51', blank=True)
    var_52 = models.FloatField(null=True, db_column='VAR_52', blank=True)
    var_53 = models.FloatField(null=True, db_column='VAR_53', blank=True)
    var_54 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_54', blank=True)
    var_55 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_55', blank=True)
    var_56 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_56', blank=True)
    var_57 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_57', blank=True)
    var_58 = models.FloatField(null=True, db_column='VAR_58', blank=True)
    var_59 = models.FloatField(null=True, db_column='VAR_59', blank=True)
    var_60 = models.FloatField(null=True, db_column='VAR_60', blank=True)
    var_61 = models.FloatField(null=True, db_column='VAR_61', blank=True)
    var_62 = models.FloatField(null=True, db_column='VAR_62', blank=True)
    var_63 = models.FloatField(null=True, db_column='VAR_63', blank=True)
    var_64 = models.FloatField(null=True, db_column='VAR_64', blank=True)
    var_65 = models.FloatField(null=True, db_column='VAR_65', blank=True)
    var_66 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_66', blank=True)
    var_67 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_67', blank=True)
    var_68 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_68', blank=True)
    var_69 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_69', blank=True)
    var_70 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_70', blank=True)
    var_71 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_71', blank=True)
    var_72 = models.FloatField(null=True, db_column='VAR_72', blank=True)
    var_73 = models.FloatField(null=True, db_column='VAR_73', blank=True)
    var_74 = models.FloatField(null=True, db_column='VAR_74', blank=True)
    var_75 = models.FloatField(null=True, db_column='VAR_75', blank=True)
    var_76 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_76', blank=True)
    var_77 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_77', blank=True)
    var_78 = models.FloatField(null=True, db_column='VAR_78', blank=True)
    var_79 = models.FloatField(null=True, db_column='VAR_79', blank=True)
    var_80 = models.FloatField(null=True, db_column='VAR_80', blank=True)
    var_81 = models.FloatField(null=True, db_column='VAR_81', blank=True)
    var_82 = models.FloatField(null=True, db_column='VAR_82', blank=True)
    var_83 = models.FloatField(null=True, db_column='VAR_83', blank=True)
    var_84 = models.FloatField(null=True, db_column='VAR_84', blank=True)
    var_85 = models.FloatField(null=True, db_column='VAR_85', blank=True)
    var_86 = models.FloatField(null=True, db_column='VAR_86', blank=True)
    var_87 = models.FloatField(null=True, db_column='VAR_87', blank=True)
    var_88 = models.FloatField(null=True, db_column='VAR_88', blank=True)
    var_89 = models.FloatField(null=True, db_column='VAR_89', blank=True)
    var_90 = models.FloatField(null=True, db_column='VAR_90', blank=True)
    var_91 = models.FloatField(null=True, db_column='VAR_91', blank=True)
    var_92 = models.FloatField(null=True, db_column='VAR_92', blank=True)
    var_93 = models.FloatField(null=True, db_column='VAR_93', blank=True)
    var_94 = models.FloatField(null=True, db_column='VAR_94', blank=True)
    var_95 = models.FloatField(null=True, db_column='VAR_95', blank=True)
    var_96 = models.FloatField(null=True, db_column='VAR_96', blank=True)
    var_97 = models.FloatField(null=True, db_column='VAR_97', blank=True)
    var_98 = models.FloatField(null=True, db_column='VAR_98', blank=True)
    var_99 = models.FloatField(null=True, db_column='VAR_99', blank=True)
    var_100 = models.FloatField(null=True, db_column='VAR_100', blank=True)
    var_101 = models.FloatField(null=True, db_column='VAR_101', blank=True)
    var_102 = models.FloatField(null=True, db_column='VAR_102', blank=True)
    var_103 = models.FloatField(null=True, db_column='VAR_103', blank=True)
    var_104 = models.FloatField(null=True, db_column='VAR_104', blank=True)
    var_105 = models.FloatField(null=True, db_column='VAR_105', blank=True)
    var_106 = models.FloatField(null=True, db_column='VAR_106', blank=True)
    var_107 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_107', blank=True)
    var_108 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_108', blank=True)
    var_109 = models.FloatField(null=True, db_column='VAR_109', blank=True)
    var_110 = models.FloatField(null=True, db_column='VAR_110', blank=True)
    var_111 = models.FloatField(null=True, db_column='VAR_111', blank=True)
    var_112 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_112', blank=True)
    var_113 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_113', blank=True)
    var_114 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_114', blank=True)
    var_115 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_115', blank=True)
    var_116 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_116', blank=True)
    var_117 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_117', blank=True)
    var_118 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_118', blank=True)
    var_119 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_119', blank=True)
    var_120 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_120', blank=True)
    var_121 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_121', blank=True)
    var_122 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_122', blank=True)
    var_123 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_123', blank=True)
    var_124 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_124', blank=True)
    var_125 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_125', blank=True)
    var_126 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_126', blank=True)
    var_127 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_127', blank=True)
    var_128 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_128', blank=True)
    var_129 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_129', blank=True)
    var_130 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_130', blank=True)
    var_131 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_131', blank=True)
    var_132 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_132', blank=True)
    var_133 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_133', blank=True)
    var_134 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_134', blank=True)
    var_135 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_135', blank=True)
    var_136 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_136', blank=True)
    var_137 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_137', blank=True)
    var_138 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_138', blank=True)
    var_139 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_139', blank=True)
    var_140 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_140', blank=True)
    var_141 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_141', blank=True)
    var_142 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_142', blank=True)
    var_143 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_143', blank=True)
    var_144 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_144', blank=True)
    var_145 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_145', blank=True)
    var_146 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_146', blank=True)
    var_147 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_147', blank=True)
    var_148 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_148', blank=True)
    var_149 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_149', blank=True)
    var_150 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_150', blank=True)
    var_151 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_151', blank=True)
    var_152 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_152', blank=True)
    var_153 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_153', blank=True)
    var_154 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_154', blank=True)
    var_155 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_155', blank=True)
    var_156 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_156', blank=True)
    var_157 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_157', blank=True)
    var_158 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_158', blank=True)
    var_159 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_159', blank=True)
    var_160 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_160', blank=True)
    var_161 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_161', blank=True)
    var_162 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_162', blank=True)
    var_163 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_163', blank=True)
    var_164 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_164', blank=True)
    var_165 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_165', blank=True)
    var_166 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_166', blank=True)
    var_167 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_167', blank=True)
    var_168 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_168', blank=True)
    var_169 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_169', blank=True)
    var_170 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_170', blank=True)
    var_171 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_171', blank=True)
    var_172 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_172', blank=True)
    var_173 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_173', blank=True)
    var_174 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_174', blank=True)
    var_175 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_175', blank=True)
    var_176 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_176', blank=True)
    var_177 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_177', blank=True)
    var_178 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_178', blank=True)
    var_179 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_179', blank=True)
    var_180 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_180', blank=True)
    var_181 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_181', blank=True)
    var_182 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_182', blank=True)
    var_183 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_183', blank=True)
    var_184 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_184', blank=True)
    var_185 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_185', blank=True)
    var_186 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_186', blank=True)
    var_187 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_187', blank=True)
    var_188 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_188', blank=True)
    var_189 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_189', blank=True)
    var_190 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_190', blank=True)
    var_191 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_191', blank=True)
    var_192 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_192', blank=True)
    var_193 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_193', blank=True)
    var_194 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_194', blank=True)
    var_195 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_195', blank=True)
    var_196 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_196', blank=True)
    var_197 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_197', blank=True)
    var_198 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_198', blank=True)
    var_199 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_199', blank=True)
    var_200 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_200', blank=True)
    var_201 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_201', blank=True)
    var_202 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_202', blank=True)
    var_203 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_203', blank=True)
    var_204 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_204', blank=True)
    var_205 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_205', blank=True)
    var_206 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_206', blank=True)
    var_207 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_207', blank=True)
    var_208 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_208', blank=True)
    var_209 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_209', blank=True)
    var_210 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_210', blank=True)
    var_211 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_211', blank=True)
    var_212 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_212', blank=True)
    var_213 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_213', blank=True)
    var_214 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_214', blank=True)
    var_215 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_215', blank=True)
    var_216 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_216', blank=True)
    var_217 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_217', blank=True)
    var_218 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_218', blank=True)
    var_219 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_219', blank=True)
    var_220 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_220', blank=True)
    var_221 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_221', blank=True)
    var_222 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_222', blank=True)
    var_223 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_223', blank=True)
    var_224 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_224', blank=True)
    var_225 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_225', blank=True)
    var_226 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_226', blank=True)
    var_227 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_227', blank=True)
    var_228 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_228', blank=True)
    var_229 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_229', blank=True)
    var_230 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_230', blank=True)
    var_231 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_231', blank=True)
    var_232 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_232', blank=True)
    var_233 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_233', blank=True)
    var_234 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_234', blank=True)
    var_235 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_235', blank=True)
    var_236 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_236', blank=True)
    var_237 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_237', blank=True)
    var_238 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_238', blank=True)
    var_239 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_239', blank=True)
    var_240 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_240', blank=True)
    var_241 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_241', blank=True)
    var_242 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_242', blank=True)
    var_243 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_243', blank=True)
    var_244 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_244', blank=True)
    var_245 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_245', blank=True)
    var_246 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_246', blank=True)
    var_247 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_247', blank=True)
    var_248 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_248', blank=True)
    var_249 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_249', blank=True)
    var_250 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_250', blank=True)
    var_251 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_251', blank=True)
    var_252 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_252', blank=True)
    var_253 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_253', blank=True)
    var_254 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_254', blank=True)
    var_255 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_255', blank=True)
    var_256 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_256', blank=True)
    var_257 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_257', blank=True)
    var_258 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_258', blank=True)
    var_259 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_259', blank=True)
    var_260 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_260', blank=True)
    var_261 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_261', blank=True)
    var_262 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_262', blank=True)
    var_263 = models.FloatField(null=True, db_column='VAR_263', blank=True)
    class Meta:
        db_table = u'pool_collection_data'

class PoolCollectionData1(models.Model):
    id = models.DecimalField(decimal_places=0, primary_key=True, db_column='ID', max_digits=11)
    oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='OID_1', blank=True)
    oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='OID_2', blank=True)
    var_1_oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_1_OID_1', blank=True)
    var_1_oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_1_OID_2', blank=True)
    var_2_oid_1 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_2_OID_1', blank=True)
    var_2_oid_2 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_2_OID_2', blank=True)
    var_3 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_3', blank=True)
    var_4 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_4', blank=True)
    var_5 = models.FloatField(null=True, db_column='VAR_5', blank=True)
    var_6 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_6', blank=True)
    var_7 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_7', blank=True)
    var_8 = models.FloatField(null=True, db_column='VAR_8', blank=True)
    var_9 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_9', blank=True)
    var_10 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_10', blank=True)
    var_11 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_11', blank=True)
    var_12 = models.FloatField(null=True, db_column='VAR_12', blank=True)
    var_13 = models.FloatField(null=True, db_column='VAR_13', blank=True)
    var_14 = models.FloatField(null=True, db_column='VAR_14', blank=True)
    var_15 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_15', blank=True)
    var_16 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_16', blank=True)
    var_17 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_17', blank=True)
    var_18 = models.DecimalField(decimal_places=0, null=True, max_digits=2, db_column='VAR_18', blank=True)
    var_19 = models.FloatField(null=True, db_column='VAR_19', blank=True)
    var_20 = models.FloatField(null=True, db_column='VAR_20', blank=True)
    var_21 = models.FloatField(null=True, db_column='VAR_21', blank=True)
    var_22 = models.FloatField(null=True, db_column='VAR_22', blank=True)
    var_23 = models.FloatField(null=True, db_column='VAR_23', blank=True)
    var_24 = models.FloatField(null=True, db_column='VAR_24', blank=True)
    var_25 = models.FloatField(null=True, db_column='VAR_25', blank=True)
    var_26 = models.FloatField(null=True, db_column='VAR_26', blank=True)
    var_27 = models.FloatField(null=True, db_column='VAR_27', blank=True)
    var_28 = models.FloatField(null=True, db_column='VAR_28', blank=True)
    var_29 = models.FloatField(null=True, db_column='VAR_29', blank=True)
    var_30 = models.FloatField(null=True, db_column='VAR_30', blank=True)
    var_31 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_31', blank=True)
    var_32 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_32', blank=True)
    var_33 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_33', blank=True)
    var_34 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_34', blank=True)
    var_35 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_35', blank=True)
    var_36 = models.FloatField(null=True, db_column='VAR_36', blank=True)
    var_37 = models.FloatField(null=True, db_column='VAR_37', blank=True)
    var_38 = models.FloatField(null=True, db_column='VAR_38', blank=True)
    var_39 = models.FloatField(null=True, db_column='VAR_39', blank=True)
    var_40 = models.FloatField(null=True, db_column='VAR_40', blank=True)
    var_41 = models.FloatField(null=True, db_column='VAR_41', blank=True)
    var_42 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_42', blank=True)
    var_43 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_43', blank=True)
    var_44 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_44', blank=True)
    var_45 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_45', blank=True)
    var_46 = models.FloatField(null=True, db_column='VAR_46', blank=True)
    var_47 = models.FloatField(null=True, db_column='VAR_47', blank=True)
    var_48 = models.FloatField(null=True, db_column='VAR_48', blank=True)
    var_49 = models.FloatField(null=True, db_column='VAR_49', blank=True)
    var_50 = models.FloatField(null=True, db_column='VAR_50', blank=True)
    var_51 = models.FloatField(null=True, db_column='VAR_51', blank=True)
    var_52 = models.FloatField(null=True, db_column='VAR_52', blank=True)
    var_53 = models.FloatField(null=True, db_column='VAR_53', blank=True)
    var_54 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_54', blank=True)
    var_55 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_55', blank=True)
    var_56 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_56', blank=True)
    var_57 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_57', blank=True)
    var_58 = models.FloatField(null=True, db_column='VAR_58', blank=True)
    var_59 = models.FloatField(null=True, db_column='VAR_59', blank=True)
    var_60 = models.FloatField(null=True, db_column='VAR_60', blank=True)
    var_61 = models.FloatField(null=True, db_column='VAR_61', blank=True)
    var_62 = models.FloatField(null=True, db_column='VAR_62', blank=True)
    var_63 = models.FloatField(null=True, db_column='VAR_63', blank=True)
    var_64 = models.FloatField(null=True, db_column='VAR_64', blank=True)
    var_65 = models.FloatField(null=True, db_column='VAR_65', blank=True)
    var_66 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_66', blank=True)
    var_67 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_67', blank=True)
    var_68 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_68', blank=True)
    var_69 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_69', blank=True)
    var_70 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_70', blank=True)
    var_71 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_71', blank=True)
    var_72 = models.FloatField(null=True, db_column='VAR_72', blank=True)
    var_73 = models.FloatField(null=True, db_column='VAR_73', blank=True)
    var_74 = models.FloatField(null=True, db_column='VAR_74', blank=True)
    var_75 = models.FloatField(null=True, db_column='VAR_75', blank=True)
    var_76 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_76', blank=True)
    var_77 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_77', blank=True)
    var_78 = models.FloatField(null=True, db_column='VAR_78', blank=True)
    var_79 = models.FloatField(null=True, db_column='VAR_79', blank=True)
    var_80 = models.FloatField(null=True, db_column='VAR_80', blank=True)
    var_81 = models.FloatField(null=True, db_column='VAR_81', blank=True)
    var_82 = models.FloatField(null=True, db_column='VAR_82', blank=True)
    var_83 = models.FloatField(null=True, db_column='VAR_83', blank=True)
    var_84 = models.FloatField(null=True, db_column='VAR_84', blank=True)
    var_85 = models.FloatField(null=True, db_column='VAR_85', blank=True)
    var_86 = models.FloatField(null=True, db_column='VAR_86', blank=True)
    var_87 = models.FloatField(null=True, db_column='VAR_87', blank=True)
    var_88 = models.FloatField(null=True, db_column='VAR_88', blank=True)
    var_89 = models.FloatField(null=True, db_column='VAR_89', blank=True)
    var_90 = models.FloatField(null=True, db_column='VAR_90', blank=True)
    var_91 = models.FloatField(null=True, db_column='VAR_91', blank=True)
    var_92 = models.FloatField(null=True, db_column='VAR_92', blank=True)
    var_93 = models.FloatField(null=True, db_column='VAR_93', blank=True)
    var_94 = models.FloatField(null=True, db_column='VAR_94', blank=True)
    var_95 = models.FloatField(null=True, db_column='VAR_95', blank=True)
    var_96 = models.FloatField(null=True, db_column='VAR_96', blank=True)
    var_97 = models.FloatField(null=True, db_column='VAR_97', blank=True)
    var_98 = models.FloatField(null=True, db_column='VAR_98', blank=True)
    var_99 = models.FloatField(null=True, db_column='VAR_99', blank=True)
    var_100 = models.FloatField(null=True, db_column='VAR_100', blank=True)
    var_101 = models.FloatField(null=True, db_column='VAR_101', blank=True)
    var_102 = models.FloatField(null=True, db_column='VAR_102', blank=True)
    var_103 = models.FloatField(null=True, db_column='VAR_103', blank=True)
    var_104 = models.FloatField(null=True, db_column='VAR_104', blank=True)
    var_105 = models.FloatField(null=True, db_column='VAR_105', blank=True)
    var_106 = models.FloatField(null=True, db_column='VAR_106', blank=True)
    var_107 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_107', blank=True)
    var_108 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_108', blank=True)
    var_109 = models.FloatField(null=True, db_column='VAR_109', blank=True)
    var_110 = models.FloatField(null=True, db_column='VAR_110', blank=True)
    var_111 = models.FloatField(null=True, db_column='VAR_111', blank=True)
    var_112 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_112', blank=True)
    var_113 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_113', blank=True)
    var_114 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_114', blank=True)
    var_115 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_115', blank=True)
    var_116 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_116', blank=True)
    var_117 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_117', blank=True)
    var_118 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_118', blank=True)
    var_119 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_119', blank=True)
    var_120 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_120', blank=True)
    var_121 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_121', blank=True)
    var_122 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_122', blank=True)
    var_123 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_123', blank=True)
    var_124 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_124', blank=True)
    var_125 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_125', blank=True)
    var_126 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_126', blank=True)
    var_127 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_127', blank=True)
    var_128 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_128', blank=True)
    var_129 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_129', blank=True)
    var_130 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_130', blank=True)
    var_131 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_131', blank=True)
    var_132 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_132', blank=True)
    var_133 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_133', blank=True)
    var_134 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_134', blank=True)
    var_135 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_135', blank=True)
    var_136 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_136', blank=True)
    var_137 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_137', blank=True)
    var_138 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_138', blank=True)
    var_139 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_139', blank=True)
    var_140 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_140', blank=True)
    var_141 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_141', blank=True)
    var_142 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_142', blank=True)
    var_143 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_143', blank=True)
    var_144 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_144', blank=True)
    var_145 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_145', blank=True)
    var_146 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_146', blank=True)
    var_147 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_147', blank=True)
    var_148 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_148', blank=True)
    var_149 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_149', blank=True)
    var_150 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_150', blank=True)
    var_151 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_151', blank=True)
    var_152 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_152', blank=True)
    var_153 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_153', blank=True)
    var_154 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_154', blank=True)
    var_155 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_155', blank=True)
    var_156 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_156', blank=True)
    var_157 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_157', blank=True)
    var_158 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_158', blank=True)
    var_159 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_159', blank=True)
    var_160 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_160', blank=True)
    var_161 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_161', blank=True)
    var_162 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_162', blank=True)
    var_163 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_163', blank=True)
    var_164 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_164', blank=True)
    var_165 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_165', blank=True)
    var_166 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_166', blank=True)
    var_167 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_167', blank=True)
    var_168 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_168', blank=True)
    var_169 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_169', blank=True)
    var_170 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_170', blank=True)
    var_171 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_171', blank=True)
    var_172 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_172', blank=True)
    var_173 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_173', blank=True)
    var_174 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_174', blank=True)
    var_175 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_175', blank=True)
    var_176 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_176', blank=True)
    var_177 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_177', blank=True)
    var_178 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_178', blank=True)
    var_179 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_179', blank=True)
    var_180 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_180', blank=True)
    var_181 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_181', blank=True)
    var_182 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_182', blank=True)
    var_183 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_183', blank=True)
    var_184 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_184', blank=True)
    var_185 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_185', blank=True)
    var_186 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_186', blank=True)
    var_187 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_187', blank=True)
    var_188 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_188', blank=True)
    var_189 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_189', blank=True)
    var_190 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_190', blank=True)
    var_191 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_191', blank=True)
    var_192 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_192', blank=True)
    var_193 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_193', blank=True)
    var_194 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_194', blank=True)
    var_195 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_195', blank=True)
    var_196 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_196', blank=True)
    var_197 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_197', blank=True)
    var_198 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_198', blank=True)
    var_199 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_199', blank=True)
    var_200 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_200', blank=True)
    var_201 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_201', blank=True)
    var_202 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_202', blank=True)
    var_203 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_203', blank=True)
    var_204 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_204', blank=True)
    var_205 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_205', blank=True)
    var_206 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_206', blank=True)
    var_207 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_207', blank=True)
    var_208 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_208', blank=True)
    var_209 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_209', blank=True)
    var_210 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_210', blank=True)
    var_211 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_211', blank=True)
    var_212 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_212', blank=True)
    var_213 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_213', blank=True)
    var_214 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_214', blank=True)
    var_215 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_215', blank=True)
    var_216 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_216', blank=True)
    var_217 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_217', blank=True)
    var_218 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_218', blank=True)
    var_219 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_219', blank=True)
    var_220 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_220', blank=True)
    var_221 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_221', blank=True)
    var_222 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_222', blank=True)
    var_223 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_223', blank=True)
    var_224 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_224', blank=True)
    var_225 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_225', blank=True)
    var_226 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_226', blank=True)
    var_227 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_227', blank=True)
    var_228 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_228', blank=True)
    var_229 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_229', blank=True)
    var_230 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_230', blank=True)
    var_231 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_231', blank=True)
    var_232 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_232', blank=True)
    var_233 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_233', blank=True)
    var_234 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_234', blank=True)
    var_235 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_235', blank=True)
    var_236 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_236', blank=True)
    var_237 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_237', blank=True)
    var_238 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_238', blank=True)
    var_239 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_239', blank=True)
    var_240 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_240', blank=True)
    var_241 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_241', blank=True)
    var_242 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_242', blank=True)
    var_243 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_243', blank=True)
    var_244 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_244', blank=True)
    var_245 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_245', blank=True)
    var_246 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_246', blank=True)
    var_247 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_247', blank=True)
    var_248 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_248', blank=True)
    var_249 = models.DecimalField(decimal_places=0, null=True, max_digits=6, db_column='VAR_249', blank=True)
    var_250 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_250', blank=True)
    var_251 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_251', blank=True)
    var_252 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_252', blank=True)
    var_253 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_253', blank=True)
    var_254 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_254', blank=True)
    var_255 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_255', blank=True)
    var_256 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_256', blank=True)
    var_257 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_257', blank=True)
    var_258 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_258', blank=True)
    var_259 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_259', blank=True)
    var_260 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_260', blank=True)
    var_261 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_261', blank=True)
    var_262 = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VAR_262', blank=True)
    var_263 = models.FloatField(null=True, db_column='VAR_263', blank=True)
    class Meta:
        db_table = u'pool_collection_data_1'

class PoolCollections(models.Model):
    collection_name = models.CharField(db_column='COLLECTION_NAME', primary_key=True, max_length=255) 
    data_table_name = models.CharField(max_length=1200, db_column='DATA_TABLE_NAME', blank=True)
    links_table_name = models.CharField(max_length=1200, db_column='LINKS_TABLE_NAME', blank=True)
    records_written = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='RECORDS_WRITTEN', blank=True)
    records_deleted = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='RECORDS_DELETED', blank=True)
    child_collection_name = models.CharField(max_length=1200, db_column='CHILD_COLLECTION_NAME', blank=True)
    foreign_key_name = models.CharField(max_length=1200, db_column='FOREIGN_KEY_NAME', blank=True)
    class Meta:
        db_table = u'pool_collections'

class PoolCollectionsDesc(models.Model):
    collection_name = models.CharField(max_length=255, primary_key=True, db_column='COLLECTION_NAME') 
    variable_name = models.CharField(max_length=1200, db_column='VARIABLE_NAME', blank=True)
    variable_type = models.CharField(max_length=1200, db_column='VARIABLE_TYPE', blank=True)
    variable_maximum_size = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VARIABLE_MAXIMUM_SIZE', blank=True)
    variable_size_is_fixed = models.CharField(max_length=15, db_column='VARIABLE_SIZE_IS_FIXED', blank=True)
    variable_position = models.DecimalField(decimal_places=0, null=True, max_digits=11, db_column='VARIABLE_POSITION', blank=True)
    variable_annotation = models.CharField(max_length=12000, db_column='VARIABLE_ANNOTATION', blank=True)
    class Meta:
        db_table = u'pool_collections_desc'

class ProdsysComm(models.Model):
    comm_task = models.BigIntegerField(primary_key=True, db_column='COMM_TASK')
    comm_meta = models.BigIntegerField(null=True, db_column='COMM_META', blank=True)
    comm_owner = models.CharField(max_length=48, db_column='COMM_OWNER', blank=True)
    comm_cmd = models.CharField(max_length=768, db_column='COMM_CMD', blank=True)
    comm_ts = models.BigIntegerField(null=True, db_column='COMM_TS', blank=True)
    class Meta:
        db_table = u'prodsys_comm'

class Productiondatasets(models.Model):
    name = models.CharField(max_length=255, primary_key=True, db_column='NAME') 
    version = models.IntegerField(null=True, db_column='VERSION', blank=True)
    vuid = models.CharField(max_length=120, db_column='VUID')
    files = models.IntegerField(null=True, db_column='FILES', blank=True)
    gb = models.IntegerField(null=True, db_column='GB', blank=True)
    events = models.IntegerField(null=True, db_column='EVENTS', blank=True)
    site = models.CharField(max_length=30, db_column='SITE', blank=True)
    sw_release = models.CharField(max_length=60, db_column='SW_RELEASE', blank=True)
    geometry = models.CharField(max_length=60, db_column='GEOMETRY', blank=True)
    jobid = models.IntegerField(null=True, db_column='JOBID', blank=True)
    pandaid = models.IntegerField(null=True, db_column='PANDAID', blank=True)
    prodtime = models.DateTimeField(null=True, db_column='PRODTIME', blank=True)
    timestamp = models.IntegerField(null=True, db_column='TIMESTAMP', blank=True)
    class Meta:
        db_table = u'productiondatasets'

class Proxykey(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    dn = models.CharField(max_length=300, db_column='DN')
    credname = models.CharField(max_length=120, db_column='CREDNAME')
    created = models.DateTimeField(db_column='CREATED')
    expires = models.DateTimeField(db_column='EXPIRES')
    origin = models.CharField(max_length=240, db_column='ORIGIN')
    myproxy = models.CharField(max_length=240, db_column='MYPROXY')
    class Meta:
        db_table = u'proxykey'

class Redirect(models.Model):
    service = models.CharField(db_column='SERVICE', max_length=30) 
    type = models.CharField(db_column='TYPE', max_length=30) 
    site = models.CharField(db_column='SITE', max_length=30) 
    description = models.CharField(db_column='DESCRIPTION', max_length=120) 
    url = models.CharField(db_column='URL', primary_key=True, max_length=250) 
    testurl = models.CharField(db_column='TESTURL', max_length=250, blank=True) 
    response = models.CharField(db_column='RESPONSE', max_length=30) 
    aliveresponse = models.CharField(db_column='ALIVERESPONSE', max_length=30) 
    responsetime = models.IntegerField(db_column='RESPONSETIME', blank=True, null=True) 
    rank = models.IntegerField(db_column='RANK', blank=True, null=True) 
    performance = models.IntegerField(db_column='PERFORMANCE', blank=True, null=True) 
    status = models.CharField(db_column='STATUS', max_length=30) 
    log = models.CharField(db_column='LOG', max_length=250, blank=True) 
    statustime = models.DateTimeField(db_column='STATUSTIME')
    usetime = models.DateTimeField(db_column='USETIME')
    class Meta:
        db_table = u'redirect'

class RequestStat(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    server = models.CharField(max_length=40, db_column='server')
    remote = models.CharField(max_length=40, db_column='remote')
    qtime = models.DateTimeField(db_column='qtime')
    qduration = models.DateTimeField(db_column='qduration')
    duration = models.IntegerField(db_column='duration')
    load = models.CharField(max_length=40, db_column='load')
    mem = models.CharField(max_length=40, db_column='mem')
    urls = models.CharField(max_length=40, db_column='url')
    description = models.CharField(max_length=12000, db_column='description')
    class Meta:
        db_table = u'request_stats'

class Savedpages(models.Model):
    name = models.CharField(max_length=90, db_column='NAME', primary_key=True)
    flag = models.CharField(max_length=60, db_column='FLAG', primary_key=True)
    hours = models.IntegerField(db_column='HOURS', primary_key=True)
    html = models.TextField(db_column='HTML')
    lastmod = models.DateTimeField(null=True, db_column='LASTMOD', blank=True)
    interval = models.IntegerField(null=True, db_column='INTERVAL', blank=True)
    class Meta:
        db_table = u'savedpages'
        unique_together = ('name', 'flag', 'hours')

class Servicelist(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    name = models.CharField(max_length=180, db_column='NAME')
    host = models.CharField(max_length=300, db_column='HOST', blank=True)
    pid = models.IntegerField(null=True, db_column='PID', blank=True)
    userid = models.CharField(max_length=120, db_column='USERID', blank=True)
    type = models.CharField(max_length=90, db_column='TYPE', blank=True)
    grp = models.CharField(max_length=60, db_column='GRP', blank=True)
    description = models.CharField(max_length=600, db_column='DESCRIPTION', blank=True)
    url = models.CharField(max_length=600, db_column='URL', blank=True)
    testurl = models.CharField(max_length=600, db_column='TESTURL', blank=True)
    response = models.CharField(max_length=600, db_column='RESPONSE', blank=True)
    tresponse = models.IntegerField(null=True, db_column='TRESPONSE', blank=True)
    tstart = models.DateTimeField(db_column='TSTART')
    tstop = models.DateTimeField(db_column='TSTOP')
    tcheck = models.DateTimeField(db_column='TCHECK')
    cyclesec = models.IntegerField(null=True, db_column='CYCLESEC', blank=True)
    status = models.CharField(max_length=60, db_column='STATUS')
    lastmod = models.DateTimeField(db_column='LASTMOD')
    config = models.CharField(max_length=600, db_column='CONFIG', blank=True)
    message = models.CharField(max_length=12000, db_column='MESSAGE', blank=True)
    restartcmd = models.CharField(max_length=12000, db_column='RESTARTCMD', blank=True)
    doaction = models.CharField(max_length=12000, db_column='DOACTION', blank=True)
    class Meta:
        db_table = u'servicelist'

class Siteaccess(models.Model):
    id = models.BigIntegerField(primary_key=True, db_column='ID')
    dn = models.CharField(max_length=300, db_column='DN', blank=True)
    pandasite = models.CharField(max_length=300, db_column='PANDASITE', blank=True)
    poffset = models.BigIntegerField(db_column='POFFSET')
    rights = models.CharField(max_length=90, db_column='RIGHTS', blank=True)
    status = models.CharField(max_length=60, db_column='STATUS', blank=True)
    workinggroups = models.CharField(max_length=300, db_column='WORKINGGROUPS', blank=True)
    created = models.DateTimeField(null=True, db_column='CREATED', blank=True)
    class Meta:
        db_table = u'siteaccess'

class Sitedata(models.Model):
    site = models.CharField(max_length=90, db_column='SITE', primary_key=True)
    flag = models.CharField(max_length=60, db_column='FLAG', primary_key=True)
    hours = models.IntegerField(db_column='HOURS', primary_key=True)
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
    lastmod = models.DateTimeField(db_column='LASTMOD')
    ncpu = models.IntegerField(null=True, db_column='NCPU', blank=True)
    nslot = models.IntegerField(null=True, db_column='NSLOT', blank=True)
    class Meta:
        db_table = u'sitedata'
        unique_together = ('site', 'flag', 'hours')

class Siteddm(models.Model):
    name = models.CharField(max_length=180, primary_key=True, db_column='NAME')
    incmd = models.CharField(max_length=180, db_column='INCMD')
    inpath = models.CharField(max_length=600, db_column='INPATH', blank=True)
    inopts = models.CharField(max_length=180, db_column='INOPTS', blank=True)
    outcmd = models.CharField(max_length=180, db_column='OUTCMD')
    outopts = models.CharField(max_length=180, db_column='OUTOPTS', blank=True)
    outpath = models.CharField(max_length=600, db_column='OUTPATH')
    class Meta:
        db_table = u'siteddm'

class Sitehistory(models.Model):
    site = models.CharField(max_length=90, db_column='SITE', primary_key=True)
    flag = models.CharField(max_length=60, db_column='FLAG', primary_key=True)
    time = models.DateTimeField(db_column='TIME', primary_key=True)
    hours = models.IntegerField(db_column='HOURS', primary_key=True)
    nwn = models.IntegerField(null=True, db_column='NWN', blank=True)
    memmin = models.IntegerField(null=True, db_column='MEMMIN', blank=True)
    memmax = models.IntegerField(null=True, db_column='MEMMAX', blank=True)
    si2000min = models.IntegerField(null=True, db_column='SI2000MIN', blank=True)
    si2000max = models.IntegerField(null=True, db_column='SI2000MAX', blank=True)
    si2000a = models.IntegerField(null=True, db_column='SI2000A', blank=True)
    si2000p = models.IntegerField(null=True, db_column='SI2000P', blank=True)
    walla = models.IntegerField(null=True, db_column='WALLA', blank=True)
    wallp = models.IntegerField(null=True, db_column='WALLP', blank=True)
    os = models.CharField(max_length=90, db_column='OS')
    space = models.CharField(max_length=90, db_column='SPACE')
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
    running = models.IntegerField(db_column='RUNNING')
    getjob = models.IntegerField(db_column='GETJOB')
    updatejob = models.IntegerField(db_column='UPDATEJOB')
    subtot = models.IntegerField(db_column='SUBTOT')
    subdef = models.IntegerField(db_column='SUBDEF')
    subdone = models.IntegerField(db_column='SUBDONE')
    filemods = models.IntegerField(db_column='FILEMODS')
    ncpu = models.IntegerField(null=True, db_column='NCPU', blank=True)
    nslot = models.IntegerField(null=True, db_column='NSLOT', blank=True)
    class Meta:
        db_table = u'sitehistory'
        unique_together = ('site', 'time', 'flag', 'hours')

class Sitesinfo(models.Model):
    name = models.CharField(db_column='NAME', primary_key=True, max_length=120) 
    nick = models.CharField(db_column='NICK', max_length=20) 
    contact = models.CharField(db_column='CONTACT', max_length=30, blank=True) 
    email = models.CharField(db_column='EMAIL', max_length=30, blank=True) 
    status = models.CharField(db_column='STATUS', max_length=12, blank=True) 
    lrc = models.CharField(db_column='LRC', max_length=120, blank=True) 
    gridcat = models.IntegerField(db_column='GRIDCAT', blank=True, null=True) 
    monalisa = models.CharField(db_column='MONALISA', max_length=20, blank=True) 
    computingsite = models.CharField(db_column='COMPUTINGSITE', max_length=20, blank=True) 
    mainsite = models.CharField(db_column='MAINSITE', max_length=20, blank=True) 
    home = models.CharField(db_column='HOME', max_length=120, blank=True) 
    ganglia = models.CharField(db_column='GANGLIA', max_length=120, blank=True) 
    goc = models.CharField(db_column='GOC', max_length=20, blank=True) 
    gocconfig = models.IntegerField(db_column='GOCCONFIG', blank=True, null=True) 
    prodsys = models.CharField(db_column='PRODSYS', max_length=20, blank=True) 
    dq2svc = models.CharField(db_column='DQ2SVC', max_length=20, blank=True) 
    usage = models.CharField(db_column='USAGE', max_length=40, blank=True) 
    updtime = models.IntegerField(db_column='UPDTIME', blank=True, null=True) 
    ndatasets = models.IntegerField(db_column='NDATASETS', blank=True, null=True) 
    nfiles = models.IntegerField(db_column='NFILES', blank=True, null=True) 
    timestamp = models.IntegerField(db_column='TIMESTAMP', blank=True, null=True) 
    class Meta:
        db_table = u'sitesinfo'

class Sitestats(models.Model):
    cloud = models.CharField(max_length=30, primary_key=True, db_column='CLOUD')
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    at_time = models.DateTimeField(null=True, db_column='AT_TIME', blank=True)
    twidth = models.IntegerField(null=True, db_column='TWIDTH', blank=True)
    tjob = models.IntegerField(null=True, db_column='TJOB', blank=True)
    tgetjob = models.IntegerField(null=True, db_column='TGETJOB', blank=True)
    tstagein = models.IntegerField(null=True, db_column='TSTAGEIN', blank=True)
    trun = models.IntegerField(null=True, db_column='TRUN', blank=True)
    tstageout = models.IntegerField(null=True, db_column='TSTAGEOUT', blank=True)
    twait = models.IntegerField(null=True, db_column='TWAIT', blank=True)
    nusers = models.IntegerField(null=True, db_column='NUSERS', blank=True)
    nwn = models.IntegerField(null=True, db_column='NWN', blank=True)
    njobs = models.IntegerField(null=True, db_column='NJOBS', blank=True)
    nfinished = models.IntegerField(null=True, db_column='NFINISHED', blank=True)
    nfailed = models.IntegerField(null=True, db_column='NFAILED', blank=True)
    nfailapp = models.IntegerField(null=True, db_column='NFAILAPP', blank=True)
    nfailsys = models.IntegerField(null=True, db_column='NFAILSYS', blank=True)
    nfaildat = models.IntegerField(null=True, db_column='NFAILDAT', blank=True)
    ntimeout = models.IntegerField(null=True, db_column='NTIMEOUT', blank=True)
    efficiency = models.IntegerField(null=True, db_column='EFFICIENCY', blank=True)
    siteutil = models.IntegerField(null=True, db_column='SITEUTIL', blank=True)
    jobtype = models.CharField(max_length=90, db_column='JOBTYPE', blank=True)
    proctype = models.CharField(max_length=270, db_column='PROCTYPE', blank=True)
    username = models.CharField(max_length=270, db_column='USERNAME', blank=True)
    ngetjob = models.IntegerField(null=True, db_column='NGETJOB', blank=True)
    nupdatejob = models.IntegerField(null=True, db_column='NUPDATEJOB', blank=True)
    release = models.CharField(max_length=270, db_column='RELEASE', blank=True)
    nevents = models.BigIntegerField(null=True, db_column='NEVENTS', blank=True)
    spectype = models.CharField(max_length=270, db_column='SPECTYPE', blank=True)
    tsetup = models.IntegerField(null=True, db_column='TSETUP', blank=True)
    class Meta:
        db_table = u'sitestats'

class Submithosts(models.Model):
    name = models.CharField(max_length=180, db_column='NAME')
    nickname = models.CharField(max_length=60, db_column='NICKNAME')
    host = models.CharField(max_length=180, primary_key=True, db_column='HOST')
    system = models.CharField(max_length=180, db_column='SYSTEM')
    rundir = models.CharField(max_length=600, db_column='RUNDIR')
    runurl = models.CharField(max_length=600, db_column='RUNURL')
    jdltxt = models.CharField(max_length=12000, db_column='JDLTXT', blank=True)
    pilotqueue = models.CharField(max_length=60, db_column='PILOTQUEUE', blank=True)
    outurl = models.CharField(max_length=600, db_column='OUTURL', blank=True)
    class Meta:
        db_table = u'submithosts'

class Sysconfig(models.Model):
    name = models.CharField(max_length=180, db_column='NAME', primary_key=True)
    system = models.CharField(max_length=60, db_column='SYSTEM', primary_key=True)
    config = models.CharField(max_length=12000, db_column='CONFIG', blank=True)
    class Meta:
        db_table = u'sysconfig'
        unique_together = ('name', 'system')

class TM4RegionsReplication(models.Model):
    tier2 = models.CharField(max_length=150, primary_key=True, db_column='TIER2')
    cloud = models.CharField(max_length=90, db_column='CLOUD')
    percentage = models.FloatField(null=True, db_column='PERCENTAGE', blank=True)
    tier1 = models.CharField(max_length=150, db_column='TIER1')
    nsubs = models.IntegerField(null=True, db_column='NSUBS', blank=True)
    subsoption = models.CharField(max_length=960, db_column='SUBSOPTION', blank=True)
    status = models.CharField(max_length=36, db_column='STATUS', blank=True)
    timestamp = models.IntegerField(null=True, db_column='TIMESTAMP', blank=True)
    stream_pattern = models.CharField(max_length=96, db_column='STREAM_PATTERN', blank=True)
    nreplicas = models.IntegerField(null=True, db_column='NREPLICAS', blank=True)
    nsubs_aod = models.IntegerField(null=True, db_column='NSUBS_AOD', blank=True)
    nsubs_dpd = models.IntegerField(null=True, db_column='NSUBS_DPD', blank=True)
    upd_flag = models.CharField(max_length=12, db_column='UPD_FLAG', blank=True)
    esd = models.IntegerField(null=True, db_column='ESD', blank=True)
    esd_subsoption = models.CharField(max_length=960, db_column='ESD_SUBSOPTION', blank=True)
    desd = models.IntegerField(null=True, db_column='DESD', blank=True)
    desd_subsoption = models.CharField(max_length=960, db_column='DESD_SUBSOPTION', blank=True)
    prim_flag = models.IntegerField(null=True, db_column='PRIM_FLAG', blank=True)
    t2group = models.BigIntegerField(null=True, db_column='T2GROUP', blank=True)
    class Meta:
        db_table = u't_m4regions_replication'

class TTier2Groups(models.Model):
    name = models.CharField(max_length=36, primary_key=True, db_column='NAME')
    gid = models.BigIntegerField(null=True, db_column='GID', blank=True)
    ntup_share = models.BigIntegerField(null=True, db_column='NTUP_SHARE', blank=True)
    timestmap = models.BigIntegerField(null=True, db_column='TIMESTMAP', blank=True)
    class Meta:
        db_table = u't_tier2_groups'

class Tablepart4Copying(models.Model):
    table_name = models.CharField(max_length=90, db_column='TABLE_NAME', primary_key=True)
    partition_name = models.CharField(max_length=90, db_column='PARTITION_NAME', primary_key=True)
    copied_to_arch = models.CharField(max_length=30, db_column='COPIED_TO_ARCH')
    copying_done_on = models.DateTimeField(null=True, db_column='COPYING_DONE_ON', blank=True)
    deleted_on = models.DateTimeField(null=True, db_column='DELETED_ON', blank=True)
    data_verif_passed = models.CharField(max_length=9, db_column='DATA_VERIF_PASSED', blank=True)
    data_verified_on = models.DateTimeField(null=True, db_column='DATA_VERIFIED_ON', blank=True)
    class Meta:
        db_table = u'tablepart4copying'
        unique_together = ('table_name', 'partition_name')

class Taginfo(models.Model):
    tag = models.CharField(max_length=90, primary_key=True, db_column='TAG')
    description = models.CharField(max_length=300, db_column='DESCRIPTION')
    nqueues = models.IntegerField(db_column='NQUEUES')
    queues = models.CharField(max_length=12000, db_column='QUEUES', blank=True)
    class Meta:
        db_table = u'taginfo'

class Tags(models.Model):
    id = models.IntegerField(primary_key=True, db_column='ID')
    name = models.CharField(max_length=60, db_column='NAME')
    description = models.CharField(max_length=180, db_column='DESCRIPTION')
    ugid = models.IntegerField(null=True, db_column='UGID', blank=True)
    type = models.CharField(max_length=30, db_column='TYPE')
    itemid = models.IntegerField(null=True, db_column='ITEMID', blank=True)
    created = models.DateTimeField(db_column='CREATED')
    class Meta:
        db_table = u'tags'

class Transfercosts(models.Model):
    sourcesite = models.CharField(db_column='SOURCESITE', max_length=256) 
    destsite = models.CharField(db_column='DESTSITE', max_length=256) 
    type = models.CharField(db_column='TYPE', max_length=256) 
    status = models.CharField(db_column='STATUS', max_length=64, blank=True) 
    last_update = models.DateTimeField(db_column='LAST_UPDATE', blank=True, null=True) 
    cost = models.BigIntegerField(db_column='COST')
    max_cost = models.BigIntegerField(db_column='MAX_COST', blank=True, null=True) 
    min_cost = models.BigIntegerField(db_column='MIN_COST', blank=True, null=True) 
    class Meta:
        db_table = u'transfercosts'

class TransfercostsHistory(models.Model):
    sourcesite = models.CharField(db_column='SOURCESITE', primary_key=True, max_length=255) 
    destsite = models.CharField(max_length=768, db_column='DESTSITE')
    type = models.CharField(max_length=768, db_column='TYPE', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS', blank=True)
    last_update = models.DateTimeField(null=True, db_column='LAST_UPDATE', blank=True)
    cost = models.BigIntegerField(db_column='COST')
    max_cost = models.BigIntegerField(null=True, db_column='MAX_COST', blank=True)
    min_cost = models.BigIntegerField(null=True, db_column='MIN_COST', blank=True)
    class Meta:
        db_table = u'transfercosts_history'

class TriggersDebug(models.Model):
    when = models.DateTimeField(primary_key=True, db_column='WHEN')
    what = models.CharField(max_length=300, db_column='WHAT', blank=True)
    value = models.CharField(max_length=600, db_column='VALUE', blank=True)
    class Meta:
        db_table = u'triggers_debug'

class Usagereport(models.Model):
    entry = models.IntegerField(primary_key=True, db_column='ENTRY')
    flag = models.CharField(max_length=60, db_column='FLAG')
    hours = models.IntegerField(null=True, db_column='HOURS', blank=True)
    tstart = models.DateTimeField(null=True, db_column='TSTART', blank=True)
    tend = models.DateTimeField(null=True, db_column='TEND', blank=True)
    tinsert = models.DateTimeField(db_column='TINSERT')
    site = models.CharField(max_length=90, db_column='SITE')
    nwn = models.IntegerField(null=True, db_column='NWN', blank=True)
    class Meta:
        db_table = u'usagereport'

class Usercacheusage(models.Model):
    username = models.CharField(max_length=384, db_column='USERNAME')
    filename = models.CharField(db_column='FILENAME', max_length=255, primary_key=True)
    hostname = models.CharField(max_length=192, db_column='HOSTNAME', primary_key=True)
    creationtime = models.DateTimeField(db_column='CREATIONTIME', primary_key=True)
    modificationtime = models.DateTimeField(null=True, db_column='MODIFICATIONTIME', blank=True)
    filesize = models.BigIntegerField(null=True, db_column='FILESIZE', blank=True)
    checksum = models.CharField(max_length=108, db_column='CHECKSUM', blank=True)
    aliasname = models.CharField(max_length=768, db_column='ALIASNAME', blank=True)
    class Meta:
        db_table = u'usercacheusage'
        unique_together = ('filename', 'hostname', 'creationtime')

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
    yr = models.IntegerField(db_column='YR', primary_key=True)
    mo = models.IntegerField(db_column='MO', primary_key=True)
    jobs = models.BigIntegerField(null=True, db_column='JOBS', blank=True)
    idlo = models.BigIntegerField(null=True, db_column='IDLO', blank=True)
    idhi = models.BigIntegerField(null=True, db_column='IDHI', blank=True)
    info = models.CharField(max_length=300, db_column='INFO', blank=True)
    class Meta:
        db_table = u'userstats'
        unique_together = ('name', 'yr', 'mo')

class Usersubs(models.Model):
    datasetname = models.CharField(max_length=255, db_column='DATASETNAME', primary_key=True)
    site = models.CharField(max_length=192, db_column='SITE', primary_key=True)
    creationdate = models.DateTimeField(null=True, db_column='CREATIONDATE', blank=True)
    modificationdate = models.DateTimeField(null=True, db_column='MODIFICATIONDATE', blank=True)
    nused = models.IntegerField(null=True, db_column='NUSED', blank=True)
    state = models.CharField(max_length=90, db_column='STATE', blank=True)
    class Meta:
        db_table = u'usersubs'
        unique_together = ('datasetname', 'site')

class VoToSite(models.Model):
    site_name = models.CharField(max_length=96, db_column='SITE_NAME', primary_key=True)
    queue = models.CharField(max_length=192, db_column='QUEUE', primary_key=True)
    vo_name = models.CharField(max_length=96, db_column='VO_NAME', primary_key=True)
    class Meta:
        db_table = u'vo_to_site'
        unique_together = ('site_name', 'queue', 'vo_name')

class Vorspassfail(models.Model):
    site_name = models.CharField(max_length=96, primary_key=True, db_column='SITE_NAME')
    passfail = models.CharField(max_length=12, db_column='PASSFAIL')
    last_checked = models.DateTimeField(null=True, db_column='LAST_CHECKED', blank=True)
    class Meta:
        db_table = u'vorspassfail'

class Wndata(models.Model):
    site = models.CharField(max_length=90, db_column='SITE', primary_key=True)
    wn = models.CharField(max_length=150, db_column='WN', primary_key=True)
    flag = models.CharField(max_length=60, db_column='FLAG', primary_key=True)
    hours = models.IntegerField(db_column='HOURS', primary_key=True)
    mem = models.IntegerField(null=True, db_column='MEM', blank=True)
    si2000 = models.IntegerField(null=True, db_column='SI2000', blank=True)
    os = models.CharField(max_length=90, db_column='OS', blank=True)
    space = models.CharField(max_length=90, db_column='SPACE', blank=True)
    maxjobs = models.IntegerField(null=True, db_column='MAXJOBS', blank=True)
    laststart = models.DateTimeField(null=True, db_column='LASTSTART', blank=True)
    lastend = models.DateTimeField(null=True, db_column='LASTEND', blank=True)
    lastfail = models.DateTimeField(null=True, db_column='LASTFAIL', blank=True)
    lastpilot = models.DateTimeField(null=True, db_column='LASTPILOT', blank=True)
    lastpid = models.IntegerField(null=True, db_column='LASTPID', blank=True)
    nstart = models.IntegerField(db_column='NSTART')
    finished = models.IntegerField(db_column='FINISHED')
    failed = models.IntegerField(db_column='FAILED')
    holding = models.IntegerField(db_column='HOLDING')
    running = models.IntegerField(db_column='RUNNING')
    transferring = models.IntegerField(db_column='TRANSFERRING')
    getjob = models.IntegerField(db_column='GETJOB')
    updatejob = models.IntegerField(db_column='UPDATEJOB')
    lastmod = models.DateTimeField(db_column='LASTMOD')
    ncpu = models.IntegerField(null=True, db_column='NCPU', blank=True)
    ncpucurrent = models.IntegerField(null=True, db_column='NCPUCURRENT', blank=True)
    nslot = models.IntegerField(null=True, db_column='NSLOT', blank=True)
    nslotcurrent = models.IntegerField(null=True, db_column='NSLOTCURRENT', blank=True)
    class Meta:
        db_table = u'wndata'
        unique_together = ('site', 'wn', 'flag', 'hours')



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
    project = models.ForeignKey(TProject,db_column='PROJECT', null=True, blank=False)
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
    request = models.ForeignKey(TRequest, db_column='PR_ID')
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
    dataset = models.ForeignKey(ProductionDataset, db_column='INPUTDATASET',null=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID')
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
    request = models.ForeignKey(TRequest, db_column='PR_ID')
    step_template = models.ForeignKey(StepTemplate, db_column='STEP_T_ID')
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    step_def_time = models.DateTimeField(db_column='STEP_DEF_TIME', null=False)
    step_appr_time = models.DateTimeField(db_column='STEP_APPR_TIME', null=True)
    step_exe_time = models.DateTimeField(db_column='STEP_EXE_TIME', null=True)
    step_done_time = models.DateTimeField(db_column='STEP_DONE_TIME', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='INPUT_EVENTS', null=True)
    task_config = models.CharField(max_length=2000, db_column='TASK_CONFIG')
    step_parent = models.ForeignKey('self', db_column='STEP_PARENT_ID')

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
    step = models.ForeignKey(StepExecution, db_column='STEP_ID')
    request = models.ForeignKey(TRequest, db_column='PR_ID')
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
    except Exception,e:
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


