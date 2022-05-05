# Create your models here.
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.

from .columns_config import COLUMNS, ORDER_COLUMNS, COL_TITLES, FILTERS
from core.settings.config import DEPLOYMENT

from django.db import models
models.options.DEFAULT_NAMES += (
    'allColumns', 'orderColumns', 'primaryColumns', 'secondaryColumns', 'columnTitles', 'filterFields',
)


class MonitorUsers(models.Model):
    userid = models.BigIntegerField(db_column='ID')
    dname = models.CharField(max_length=200, db_column='DNAME', blank=True)
    email = models.CharField(max_length=100, db_column='EMAIL', blank=True)     
    username = models.CharField(max_length=200, db_column='USERNAME', blank=True) 
    firstdate = models.DateTimeField(db_column='FIRSTLOGINDATE') # Field name made lowercase.
    isactive = models.SmallIntegerField(db_column='ISACTIVE') # Field name made lowercase.
    class Meta:
        db_table = u'MONITOR_USERS'
        app_label = 'panda'


class Getfailedjobshspec(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    timeinhepspec = models.FloatField(default=None, db_column='TIMEINHEPSPEC')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."GETFAILEDJOBSHSPEC"'
        app_label = 'pandamon'


class Getfailedjobshspecarch(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    timeinhepspec = models.FloatField(default=None, db_column='TIMEINHEPSPEC')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."GETFAILEDJOBSHSPECARCH"'
        app_label = 'pandamon'



class GetRWWithPrioJedi3DAYS(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    datasetid = models.BigIntegerField(db_column='DATASETID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    cloud = models.CharField(max_length=10, db_column='CLOUD', blank=True)
    corecount = models.IntegerField(db_column='CORECOUNT')
    processingtype = models.CharField(max_length=64, db_column='PROCESSINGTYPE')
    ramcount = models.IntegerField(db_column='RAMCOUNT')
    ramunit = models.CharField(max_length=32, db_column='RAMUNIT')
    prodsourcelabel = models.CharField(max_length=20, db_column='PRODSOURCELABEL')
    nrem = models.BigIntegerField(db_column='NREM')
    walltime = models.BigIntegerField(db_column='WALLTIME')
    fsize = models.BigIntegerField(db_column='FSIZE')
    startevent = models.BigIntegerField(db_column='STARTEVENT')
    endevent = models.BigIntegerField(db_column='ENDEVENT')
    nevents = models.BigIntegerField(db_column='NEVENTS')
    taskname = models.CharField(max_length=132, db_column='TASKNAME')
    workinggroup = models.CharField(max_length=132, db_column='WORKINGGROUP')
    produsername = models.CharField(max_length=132, db_column='username')


    def get_all_fields(self):
        """Returns a list of all field names on the instance."""
        fields = []
        kys = {}
        for f in self._meta.fields:
            kys[f.name] = f
        kys1 = kys.keys()
        kys1.sort()
        for k in kys1:
            f = kys[k]
            fname = f.name        
            # resolve picklists/choices, with get_xyz_display() function
            get_choice = 'get_'+fname+'_display'
            if hasattr( self, get_choice):
                value = getattr( self, get_choice)()
            else:
                try :
                    value = getattr(self, fname)
                except User.DoesNotExist:
                    value = None

            # only display fields with values and skip some fields entirely
            if f.editable and value :

                fields.append(
                  {
                   'label':f.verbose_name, 
                   'name':f.name, 
                   'value':value,
                  }
                )
        return fields

    # __getattr__
    def __getattr__(self, name):
        return super(PandaJob, self).__getattr__(name)

    # __getitem__
    def __getitem__(self, name):
#        return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]

    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."GETRWWITHPRIOJEDI3DAYS"'
        app_label = 'pandamon'


class RemainedEventsPerCloud3dayswind(models.Model):
    cloud = models.CharField(max_length=10, db_column='CLOUD', blank=True)
    nrem = models.BigIntegerField(db_column='REMNORMEV')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."REMEVPCL3DAYSWIND"'
        app_label = 'pandamon'


class JobsWorldView(models.Model):
    nucleus = models.CharField(max_length=10, db_column='NUCLEUS', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True) # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS') # Field name made lowercase.
    countjobsinstate = models.IntegerField(db_column='COUNTJOBSINSTATE')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."JOBSWORLDVIEW"'
        app_label = 'pandamon'


class CombinedWaitActDefArch4(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID')
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    jobname = models.CharField(max_length=384, db_column='JOBNAME', blank=True)
    nucleus = models.CharField(max_length=10, db_column='NUCLEUS', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True) # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS') # Field name made lowercase.
    tasktype = models.CharField(max_length=64, db_column='TASKTYPE')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME') # Field name made lowercase.
    es = models.IntegerField(db_column='ES')
    nevents = models.IntegerField(db_column='NEVENTS')
    isarchive = models.IntegerField(db_column='ISARCHIVE')
    username = models.CharField(max_length=168, db_column='USERNAME')
    resourcetype = models.CharField(max_length=64, db_column='RESOURCE_TYPE')
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."COMBINED_WAIT_ACT_DEF_ARCH4"'
        app_label = 'pandamon'


class JobsWorldViewTaskType(models.Model):
    nucleus = models.CharField(max_length=10, db_column='NUCLEUS', blank=True)
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True) # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS') # Field name made lowercase.
    countjobsinstate = models.IntegerField(db_column='COUNTJOBSINSTATE')
    tasktype = models.CharField(max_length=64, db_column='TASKTYPE')
    es = models.IntegerField(db_column='ES')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."JOBSWORLDVIEWTASKTYPE"'
        app_label = 'pandamon'


class PandaJob(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='pandaid') # Field name made lowercase.
    jobdefinitionid = models.BigIntegerField(db_column='JOBDEFINITIONID') # Field name made lowercase.
    schedulerid = models.CharField(max_length=384, db_column='schedulerid', blank=True) # Field name made lowercase.
    pilotid = models.CharField(max_length=600, db_column='pilotid', blank=True) # Field name made lowercase.
    creationtime = models.DateTimeField(db_column='creationtime') # Field name made lowercase.
    creationhost = models.CharField(max_length=384, db_column='CREATIONHOST', blank=True) # Field name made lowercase.
    modificationtime = models.DateTimeField(db_column='modificationtime') # Field name made lowercase.
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True) # Field name made lowercase.
    atlasrelease = models.CharField(max_length=192, db_column='atlasrelease', blank=True) # Field name made lowercase.
    transformation = models.CharField(max_length=750, db_column='transformation', blank=True) # Field name made lowercase.
    homepackage = models.CharField(max_length=240, db_column='homepackage', blank=True) # Field name made lowercase.
    prodserieslabel = models.CharField(max_length=60, db_column='PRODSERIESLABEL', blank=True) # Field name made lowercase.
    prodsourcelabel = models.CharField(max_length=60, db_column='prodsourcelabel', blank=True) # Field name made lowercase.
    produserid = models.CharField(max_length=750, db_column='PRODUSERID', blank=True) # Field name made lowercase.
    gshare = models.CharField(max_length=750, db_column='gshare', blank=True)
    assignedpriority = models.IntegerField(db_column='ASSIGNEDPRIORITY') # Field name made lowercase.
    currentpriority = models.IntegerField(db_column='currentpriority') # Field name made lowercase.
    attemptnr = models.IntegerField(db_column='attemptnr') # Field name made lowercase.
    maxattempt = models.IntegerField(db_column='maxattempt') # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='jobstatus') # Field name made lowercase.
    jobname = models.CharField(max_length=768, db_column='jobname', blank=True) # Field name made lowercase.
    maxcpucount = models.IntegerField(db_column='MAXCPUCOUNT') # Field name made lowercase.
    maxcpuunit = models.CharField(max_length=96, db_column='MAXCPUUNIT', blank=True) # Field name made lowercase.
    maxdiskcount = models.IntegerField(db_column='MAXDISKCOUNT') # Field name made lowercase.
    maxdiskunit = models.CharField(max_length=12, db_column='MAXDISKUNIT', blank=True) # Field name made lowercase.
    ipconnectivity = models.CharField(max_length=15, db_column='IPCONNECTIVITY', blank=True) # Field name made lowercase.
    minramcount = models.IntegerField(db_column='minramcount') # Field name made lowercase.
    minramunit = models.CharField(max_length=6, db_column='MINRAMUNIT', blank=True) # Field name made lowercase.
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True) # Field name made lowercase.
    endtime = models.DateTimeField(null=True, db_column='endtime', blank=True) # Field name made lowercase.
    cpuconsumptiontime = models.BigIntegerField(db_column='cpuconsumptiontime') # Field name made lowercase.
    cpuconsumptionunit = models.CharField(max_length=384, db_column='cpuconsumptionunit', blank=True) # Field name made lowercase.
    commandtopilot = models.CharField(max_length=750, db_column='COMMANDTOPILOT', blank=True) # Field name made lowercase.
    transexitcode = models.CharField(max_length=384, db_column='transexitcode', blank=True) # Field name made lowercase.
    piloterrorcode = models.IntegerField(db_column='piloterrorcode') # Field name made lowercase.
    piloterrordiag = models.CharField(max_length=1500, db_column='piloterrordiag', blank=True) # Field name made lowercase.
    exeerrorcode = models.IntegerField(db_column='exeerrorcode') # Field name made lowercase.
    exeerrordiag = models.CharField(max_length=1500, db_column='exeerrordiag', blank=True) # Field name made lowercase.
    superrorcode = models.IntegerField(db_column='superrorcode') # Field name made lowercase.
    superrordiag = models.CharField(max_length=750, db_column='superrordiag', blank=True) # Field name made lowercase.
    ddmerrorcode = models.IntegerField(db_column='ddmerrorcode') # Field name made lowercase.
    ddmerrordiag = models.CharField(max_length=1500, db_column='ddmerrordiag', blank=True) # Field name made lowercase.
    brokerageerrorcode = models.IntegerField(db_column='brokerageerrorcode') # Field name made lowercase.
    brokerageerrordiag = models.CharField(max_length=750, db_column='brokerageerrordiag', blank=True) # Field name made lowercase.
    jobdispatchererrorcode = models.IntegerField(db_column='jobdispatchererrorcode') # Field name made lowercase.
    jobdispatchererrordiag = models.CharField(max_length=750, db_column='jobdispatchererrordiag', blank=True) # Field name made lowercase.
    taskbuffererrorcode = models.IntegerField(db_column='taskbuffererrorcode') # Field name made lowercase.
    taskbuffererrordiag = models.CharField(max_length=900, db_column='taskbuffererrordiag', blank=True) # Field name made lowercase.
    computingsite = models.CharField(max_length=384, db_column='computingsite', blank=True) # Field name made lowercase.
    computingelement = models.CharField(max_length=384, db_column='computingelement', blank=True) # Field name made lowercase.
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True) # Field name made lowercase.
    metadata = models.TextField(db_column='METADATA', blank=True) # Field name made lowercase.
    proddblock = models.CharField(max_length=765, db_column='proddblock', blank=True) # Field name made lowercase.
    dispatchdblock = models.CharField(max_length=765, db_column='DISPATCHDBLOCK', blank=True) # Field name made lowercase.
    destinationdblock = models.CharField(max_length=765, db_column='destinationdblock', blank=True) # Field name made lowercase.
    destinationse = models.CharField(max_length=750, db_column='destinationse', blank=True) # Field name made lowercase.
    nevents = models.IntegerField(db_column='nevents') # Field name made lowercase.
    grid = models.CharField(max_length=150, db_column='GRID', blank=True) # Field name made lowercase.
    cloud = models.CharField(max_length=150, db_column='cloud', blank=True) # Field name made lowercase.
    cpuconversion = models.DecimalField(decimal_places=4, null=True, max_digits=11, db_column='CPUCONVERSION', blank=True) # Field name made lowercase.
    sourcesite = models.CharField(max_length=108, db_column='SOURCESITE', blank=True) # Field name made lowercase.
    destinationsite = models.CharField(max_length=108, db_column='DESTINATIONSITE', blank=True) # Field name made lowercase.
    transfertype = models.CharField(max_length=30, db_column='TRANSFERTYPE', blank=True) # Field name made lowercase.
    taskid = models.IntegerField(null=True, db_column='taskid', blank=True) # Field name made lowercase.
    cmtconfig = models.CharField(max_length=750, db_column='cmtconfig', blank=True) # Field name made lowercase.
    statechangetime = models.DateTimeField(null=True, db_column='statechangetime', blank=True) # Field name made lowercase.
    proddbupdatetime = models.DateTimeField(null=True, db_column='PRODDBUPDATETIME', blank=True) # Field name made lowercase.
    lockedby = models.CharField(max_length=384, db_column='LOCKEDBY', blank=True) # Field name made lowercase.
    relocationflag = models.IntegerField(null=True, db_column='RELOCATIONFLAG', blank=True) # Field name made lowercase.
    jobexecutionid = models.BigIntegerField(null=True, db_column='JOBEXECUTIONID', blank=True) # Field name made lowercase.
    vo = models.CharField(max_length=48, db_column='vo', blank=True) # Field name made lowercase.
    pilottiming = models.CharField(max_length=300, db_column='PILOTTIMING', blank=True) # Field name made lowercase.
    workinggroup = models.CharField(max_length=60, db_column='workinggroup', blank=True) # Field name made lowercase.
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True) # Field name made lowercase.
    produsername = models.CharField(max_length=180, db_column='produsername', blank=True) # Field name made lowercase.
    ninputfiles = models.IntegerField(null=True, db_column='NINPUTFILES', blank=True) # Field name made lowercase.
    countrygroup = models.CharField(max_length=60, db_column='COUNTRYGROUP', blank=True) # Field name made lowercase.
    batchid = models.CharField(max_length=240, db_column='BATCHID', blank=True) # Field name made lowercase.
    parentid = models.BigIntegerField(null=True, db_column='parentid', blank=True) # Field name made lowercase.
    specialhandling = models.CharField(max_length=240, db_column='specialhandling', blank=True) # Field name made lowercase.
    jobsetid = models.BigIntegerField(null=True, db_column='jobsetid', blank=True) # Field name made lowercase.
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True) # Field name made lowercase.
    ninputdatafiles = models.IntegerField(null=True, db_column='NINPUTDATAFILES', blank=True) # Field name made lowercase.
    inputfiletype = models.CharField(max_length=96, db_column='inputfiletype', blank=True) # Field name made lowercase.
    inputfileproject = models.CharField(max_length=192, db_column='inputfileproject', blank=True) # Field name made lowercase.
    inputfilebytes = models.BigIntegerField(null=True, db_column='INPUTFILEBYTES', blank=True) # Field name made lowercase.
    noutputdatafiles = models.IntegerField(null=True, db_column='noutputdatafiles', blank=True) # Field name made lowercase.
    outputfilebytes = models.BigIntegerField(null=True, db_column='OUTPUTFILEBYTES', blank=True) # Field name made lowercase.
    jobmetrics = models.CharField(max_length=1500, db_column='jobmetrics', blank=True) # Field name made lowercase.
    workqueue_id = models.IntegerField(null=True, db_column='WORKQUEUE_ID', blank=True) # Field name made lowercase.
    jeditaskid = models.BigIntegerField(null=True, db_column='jeditaskid', blank=True) # Field name made lowercase.
    actualcorecount = models.IntegerField(null=True, db_column='actualcorecount', blank=True)
    reqid = models.BigIntegerField(null=True, db_column='reqid', blank=True) # Field name made lowercase.
    nucleus = models.CharField(max_length=200, db_column='nucleus', blank=True) # Field name made lowercase.
    jobsubstatus = models.CharField(null=True, max_length=80, db_column='jobsubstatus', blank=True)
    hs06 = models.BigIntegerField(null=True, db_column='hs06', blank=True) # Field name made lowercase.
    maxrss = models.BigIntegerField(null=True, db_column='maxrss', blank=True) # Field name made lowercase.
    maxvmem = models.BigIntegerField(null=True, db_column='maxvmem', blank=True) # Field name made lowercase.
    maxswap = models.BigIntegerField(null=True, db_column='maxswap', blank=True) # Field name made lowercase.
    maxpss = models.BigIntegerField(null=True, db_column='maxpss', blank=True) # Field name made lowercase.
    avgrss = models.BigIntegerField(null=True, db_column='avgrss', blank=True) # Field name made lowercase.
    avgvmem = models.BigIntegerField(null=True, db_column='avgvmem', blank=True) # Field name made lowercase.
    avgswap = models.BigIntegerField(null=True, db_column='avgswap', blank=True) # Field name made lowercase.
    avgpss = models.BigIntegerField(null=True, db_column='avgpss', blank=True) # Field name made lowercase.
    maxwalltime = models.BigIntegerField(null=True, db_column='maxwalltime', blank=True) # Field name made lowercase.
    resourcetype = models.CharField(null=True, max_length=80, db_column='resource_type', blank=True)
    failedattempt = models.IntegerField(null=True, db_column='FAILEDATTEMPT', blank=True) # Field name made lowercase.
    totrchar = models.BigIntegerField(null=True, db_column='TOTRCHAR', blank=True) # Field name made lowercase.
    totwchar = models.BigIntegerField(null=True, db_column='TOTWCHAR', blank=True) # Field name made lowercase.
    totrbytes = models.BigIntegerField(null=True, db_column='TOTRBYTES', blank=True) # Field name made lowercase.
    totwbytes = models.BigIntegerField(null=True, db_column='TOTWBYTES', blank=True) # Field name made lowercase.
    raterchar = models.BigIntegerField(null=True, db_column='RATERCHAR', blank=True) # Field name made lowercase.
    ratewchar = models.BigIntegerField(null=True, db_column='RATEWCHAR', blank=True) # Field name made lowercase.
    raterbytes = models.BigIntegerField(null=True, db_column='RATERBYTES', blank=True) # Field name made lowercase.
    ratewbytes = models.BigIntegerField(null=True, db_column='RATEWBYTES', blank=True) # Field name made lowercase.
    diskio = models.BigIntegerField(null=True, db_column='DISKIO', blank=True) # Field name made lowercase.
    memoryleak = models.BigIntegerField(null=True, db_column='MEMORY_LEAK', blank=True)
    memoryleakx2 = models.BigIntegerField(null=True, db_column='MEMORY_LEAK_X2', blank=True)
    container_name = models.CharField(max_length=765, db_column='container_name', blank=True)
    hs06sec = models.BigIntegerField(null=True, db_column='hs06sec', blank=True)  # Field name made lowercase.
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True) # Field name made lowercase.

    def __str__(self):
        return 'PanDA:' + str(self.pandaid)

    # __setattr__
    def __setattr__(self, name, value):
        super(PandaJob, self).__setattr__(name, value)

    # __getattr__
    def __getattr__(self, name):
        return super(PandaJob, self).__getattr__(name)

    # __getitem__
    def __getitem__(self, name):
#        return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]

    def get_all_fields(self):
        """Returns a list of all field names on the instance."""
        fields = []
        kys = {}
        for f in self._meta.fields:
            kys[f.name] = f
        kys1 = kys.keys()
        kys1.sort()
        for k in kys1:
            f = kys[k]
            fname = f.name        
            # resolve picklists/choices, with get_xyz_display() function
            get_choice = 'get_'+fname+'_display'
            if hasattr( self, get_choice):
                value = getattr( self, get_choice)()
            else:
                try :
                    value = getattr(self, fname)
                except User.DoesNotExist:
                    value = None

            # only display fields with values and skip some fields entirely
            if f.editable and value :

                fields.append(
                  {
                   'label':f.verbose_name, 
                   'name':f.name, 
                   'value':value,
                  }
                )
        return fields

    def get_fields_by_type(self, ftype='integer'):
        field_list = [str(f.name) for f in self._meta.fields if ftype in str(f.description).lower()]
        return field_list

    class Meta:
        abstract = True
        allColumns = COLUMNS['PanDAjob-all']
        primaryColumns = [ \
            'pandaid', 'jobdefinitionid', 'creationtime', 'produserid', \
            'currentpriority', 'jobstatus', 'modificationtime', 'cloud', \
            'destinationsite'
                ]
        secondaryColumns = []
        orderColumns = ORDER_COLUMNS['PanDAjob-all']
        columnTitles = COL_TITLES['PanDAjob-all']
        filterFields = FILTERS['PanDAjob-all']


class Jobsactive4(PandaJob):
    class Meta:
        db_table = u'jobsactive4'
        app_label = 'panda'


class Jobsarchived(PandaJob):
    class Meta:
        db_table = u'jobsarchived'
        app_label = 'panda'


class Jobsarchived4(PandaJob):
    class Meta:
        db_table = u'jobsarchived4'
        app_label = 'panda'


class Jobsdefined4(PandaJob):
    class Meta:
        db_table = u'jobsdefined4'
        app_label = 'panda'

    # __getitem__
    def __getitem__(self, name):
        # return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]


class Jobswaiting4(PandaJob):
    class Meta:
        db_table = u'jobswaiting4'
        app_label = 'panda'


# ATLARC DB

class PandaJobArch(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='PANDAID') # Field name made lowercase.
    creationtime = models.DateTimeField(db_column='CREATIONTIME') # Field name made lowercase.
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME') # Field name made lowercase.
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True) # Field name made lowercase.
    transformation = models.CharField(max_length=750, db_column='TRANSFORMATION', blank=True) # Field name made lowercase.
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True) # Field name made lowercase.
    produserid = models.CharField(max_length=750, db_column='PRODUSERID', blank=True) # Field name made lowercase.
    attemptnr = models.IntegerField(db_column='ATTEMPTNR') # Field name made lowercase.
    maxattempt = models.IntegerField(db_column='MAXATTEMPT') # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS') # Field name made lowercase.
    jobname = models.CharField(max_length=768, db_column='JOBNAME', blank=True) # Field name made lowercase.
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True) # Field name made lowercase.
    endtime = models.DateTimeField(null=True, db_column='ENDTIME', blank=True) # Field name made lowercase.
    cpuconsumptiontime = models.BigIntegerField(db_column='CPUCONSUMPTIONTIME') # Field name made lowercase.
    cpuconsumptionunit = models.CharField(max_length=384, db_column='CPUCONSUMPTIONUNIT', blank=True) # Field name made lowercase.
    transexitcode = models.CharField(max_length=384, db_column='TRANSEXITCODE', blank=True) # Field name made lowercase.
    piloterrorcode = models.IntegerField(db_column='piloterrorcode') # Field name made lowercase.
    piloterrordiag = models.CharField(max_length=1500, db_column='PILOTERRORDIAG', blank=True) # Field name made lowercase.
    exeerrorcode = models.IntegerField(db_column='EXEERRORCODE') # Field name made lowercase.
    exeerrordiag = models.CharField(max_length=1500, db_column='EXEERRORDIAG', blank=True) # Field name made lowercase.
    superrorcode = models.IntegerField(db_column='SUPERRORCODE') # Field name made lowercase.
    superrordiag = models.CharField(max_length=750, db_column='SUPERRORDIAG', blank=True) # Field name made lowercase.
    ddmerrorcode = models.IntegerField(db_column='ddmerrorcode') # Field name made lowercase.
    ddmerrordiag = models.CharField(max_length=1500, db_column='DDMERRORDIAG', blank=True) # Field name made lowercase.
    brokerageerrorcode = models.IntegerField(db_column='BROKERAGEERRORCODE') # Field name made lowercase.
    brokerageerrordiag = models.CharField(max_length=750, db_column='BROKERAGEERRORDIAG', blank=True) # Field name made lowercase.
    jobdispatchererrorcode = models.IntegerField(db_column='JOBDISPATCHERERRORCODE') # Field name made lowercase.
    jobdispatchererrordiag = models.CharField(max_length=750, db_column='JOBDISPATCHERERRORDIAG', blank=True) # Field name made lowercase.
    taskbuffererrorcode = models.IntegerField(db_column='TASKBUFFERERRORCODE') # Field name made lowercase.
    taskbuffererrordiag = models.CharField(max_length=900, db_column='TASKBUFFERERRORDIAG', blank=True) # Field name made lowercase.
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True) # Field name made lowercase.
    computingelement = models.CharField(max_length=384, db_column='COMPUTINGELEMENT', blank=True) # Field name made lowercase.
    nevents = models.IntegerField(db_column='nevents') # Field name made lowercase.
    taskid = models.IntegerField(null=True, db_column='TASKID', blank=True) # Field name made lowercase.
    statechangetime = models.DateTimeField(null=True, db_column='STATECHANGETIME', blank=True) # Field name made lowercase.
    pilottiming = models.CharField(max_length=300, db_column='PILOTTIMING', blank=True) # Field name made lowercase.
    workinggroup = models.CharField(max_length=60, db_column='WORKINGGROUP', blank=True) # Field name made lowercase.
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True) # Field name made lowercase.
    produsername = models.CharField(max_length=180, db_column='produsername', blank=True) # Field name made lowercase.
    parentid = models.BigIntegerField(null=True, db_column='parentid', blank=True) # Field name made lowercase.
    specialhandling = models.CharField(max_length=240, db_column='SPECIALHANDLING', blank=True) # Field name made lowercase.
    jobsetid = models.BigIntegerField(null=True, db_column='JOBSETID', blank=True) # Field name made lowercase.
    jobmetrics = models.CharField(max_length=1500, db_column='jobmetrics', blank=True) # Field name made lowercase.
    jeditaskid = models.BigIntegerField(null=True, db_column='JEDITASKID', blank=True) # Field name made lowercase.
    actualcorecount = models.IntegerField(null=True, db_column='ACTUALCORECOUNT', blank=True)
    reqid = models.BigIntegerField(null=True, db_column='REQID', blank=True) # Field name made lowercase.
    nucleus = models.CharField(max_length=200, db_column='nucleus', blank=True) # Field name made lowercase.
    jobsubstatus = models.CharField(null=True, max_length=80, db_column='JOBSUBSTATUS', blank=True)
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True) # Field name made lowercase.
    hs06 = models.BigIntegerField(null=True, db_column='HS06', blank=True) # Field name made lowercase.
    hs06sec = models.BigIntegerField(null=True, db_column='HS06SEC', blank=True) # Field name made lowercase.
    maxpss = models.BigIntegerField(null=True, db_column='maxpss', blank=True) # Field name made lowercase.

    class Meta:
        abstract = True


class Jobsarchived_y2014(PandaJobArch):
    class Meta:
        db_table = u'"atlas_pandaarch"."y2014_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2015(PandaJobArch):
    class Meta:
        db_table = u'"atlas_pandaarch"."y2015_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2016(PandaJobArch):
    class Meta:
        db_table = u'"atlas_pandaarch"."y2016_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2017(PandaJobArch):
    class Meta:
        db_table = u'"atlas_pandaarch"."y2017_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2018(PandaJobArch):
    class Meta:
        db_table = u'"atlas_pandaarch"."y2018_jobsarchived"'
        app_label = 'pandaarch'