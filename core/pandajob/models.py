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
from django.conf import settings
from django.db import models
models.options.DEFAULT_NAMES += (
    'allColumns', 'orderColumns', 'primaryColumns', 'secondaryColumns', 'columnTitles', 'filterFields',
)


class CombinedWaitActDefArch4(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid')
    jeditaskid = models.BigIntegerField(db_column='jeditaskid')
    jobname = models.CharField(max_length=384, db_column='jobname', blank=True)
    nucleus = models.CharField(max_length=10, db_column='nucleus', blank=True)
    computingsite = models.CharField(max_length=384, db_column='computingsite', blank=True)  
    jobstatus = models.CharField(max_length=45, db_column='jobstatus')  
    tasktype = models.CharField(max_length=64, db_column='tasktype')
    modificationtime = models.DateTimeField(db_column='modificationtime')  
    es = models.IntegerField(db_column='es')
    nevents = models.IntegerField(db_column='nevents')
    isarchive = models.IntegerField(db_column='isarchive')
    username = models.CharField(max_length=168, db_column='username')
    resourcetype = models.CharField(max_length=64, db_column='resource_type')
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."combined_wait_act_def_arch4"'
        app_label = 'pandamon'


class PandaJob(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='pandaid')  
    jobdefinitionid = models.BigIntegerField(db_column='jobdefinitionid')  
    schedulerid = models.CharField(max_length=384, db_column='schedulerid', blank=True)  
    pilotid = models.CharField(max_length=600, db_column='pilotid', blank=True)  
    creationtime = models.DateTimeField(db_column='creationtime')  
    creationhost = models.CharField(max_length=384, db_column='creationhost', blank=True)  
    modificationtime = models.DateTimeField(db_column='modificationtime')  
    modificationhost = models.CharField(max_length=384, db_column='modificationhost', blank=True)  
    atlasrelease = models.CharField(max_length=192, db_column='atlasrelease', blank=True)  
    transformation = models.CharField(max_length=750, db_column='transformation', blank=True)  
    homepackage = models.CharField(max_length=240, db_column='homepackage', blank=True)  
    prodserieslabel = models.CharField(max_length=60, db_column='prodserieslabel', blank=True)  
    prodsourcelabel = models.CharField(max_length=60, db_column='prodsourcelabel', blank=True)  
    produserid = models.CharField(max_length=750, db_column='produserid', blank=True)  
    gshare = models.CharField(max_length=750, db_column='gshare', blank=True)
    assignedpriority = models.IntegerField(db_column='assignedpriority')  
    currentpriority = models.IntegerField(db_column='currentpriority')  
    attemptnr = models.IntegerField(db_column='attemptnr')  
    maxattempt = models.IntegerField(db_column='maxattempt')  
    jobstatus = models.CharField(max_length=45, db_column='jobstatus')  
    jobname = models.CharField(max_length=768, db_column='jobname', blank=True)  
    maxcpucount = models.IntegerField(db_column='maxcpucount')  
    maxcpuunit = models.CharField(max_length=96, db_column='maxcpuunit', blank=True)  
    maxdiskcount = models.IntegerField(db_column='maxdiskcount')  
    maxdiskunit = models.CharField(max_length=12, db_column='maxdiskunit', blank=True)  
    ipconnectivity = models.CharField(max_length=15, db_column='ipconnectivity', blank=True)  
    minramcount = models.IntegerField(db_column='minramcount')  
    minramunit = models.CharField(max_length=6, db_column='minramunit', blank=True)  
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True)  
    endtime = models.DateTimeField(null=True, db_column='endtime', blank=True)  
    cpuconsumptiontime = models.BigIntegerField(db_column='cpuconsumptiontime')  
    cpuconsumptionunit = models.CharField(max_length=384, db_column='cpuconsumptionunit', blank=True)  
    commandtopilot = models.CharField(max_length=750, db_column='commandtopilot', blank=True)  
    transexitcode = models.CharField(max_length=384, db_column='transexitcode', blank=True)  
    piloterrorcode = models.IntegerField(db_column='piloterrorcode')  
    piloterrordiag = models.CharField(max_length=1500, db_column='piloterrordiag', blank=True)  
    exeerrorcode = models.IntegerField(db_column='exeerrorcode')  
    exeerrordiag = models.CharField(max_length=1500, db_column='exeerrordiag', blank=True)  
    superrorcode = models.IntegerField(db_column='superrorcode')  
    superrordiag = models.CharField(max_length=750, db_column='superrordiag', blank=True)  
    ddmerrorcode = models.IntegerField(db_column='ddmerrorcode')  
    ddmerrordiag = models.CharField(max_length=1500, db_column='ddmerrordiag', blank=True)  
    brokerageerrorcode = models.IntegerField(db_column='brokerageerrorcode')  
    brokerageerrordiag = models.CharField(max_length=750, db_column='brokerageerrordiag', blank=True)  
    jobdispatchererrorcode = models.IntegerField(db_column='jobdispatchererrorcode')  
    jobdispatchererrordiag = models.CharField(max_length=750, db_column='jobdispatchererrordiag', blank=True)  
    taskbuffererrorcode = models.IntegerField(db_column='taskbuffererrorcode')  
    taskbuffererrordiag = models.CharField(max_length=900, db_column='taskbuffererrordiag', blank=True)  
    computingsite = models.CharField(max_length=384, db_column='computingsite', blank=True)  
    computingelement = models.CharField(max_length=384, db_column='computingelement', blank=True)  
    # jobparameters = models.TextField(db_column='jobparameters', blank=True)  # deprecated (always None)
    # metadata = models.TextField(db_column='metadata', blank=True)  # deprecated (always None)
    proddblock = models.CharField(max_length=765, db_column='proddblock', blank=True)  
    dispatchdblock = models.CharField(max_length=765, db_column='dispatchdblock', blank=True)  
    destinationdblock = models.CharField(max_length=765, db_column='destinationdblock', blank=True)  
    destinationse = models.CharField(max_length=750, db_column='destinationse', blank=True)  
    nevents = models.IntegerField(db_column='nevents')  
    grid = models.CharField(max_length=150, db_column='grid', blank=True)  
    cloud = models.CharField(max_length=150, db_column='cloud', blank=True)  
    cpuconversion = models.DecimalField(decimal_places=4, null=True, max_digits=11, db_column='cpuconversion', blank=True)  
    sourcesite = models.CharField(max_length=108, db_column='sourcesite', blank=True)  
    destinationsite = models.CharField(max_length=108, db_column='destinationsite', blank=True)  
    transfertype = models.CharField(max_length=30, db_column='transfertype', blank=True)  
    taskid = models.IntegerField(null=True, db_column='taskid', blank=True)  
    cmtconfig = models.CharField(max_length=750, db_column='cmtconfig', blank=True)  
    statechangetime = models.DateTimeField(null=True, db_column='statechangetime', blank=True)  
    proddbupdatetime = models.DateTimeField(null=True, db_column='proddbupdatetime', blank=True)  
    lockedby = models.CharField(max_length=384, db_column='lockedby', blank=True)  
    relocationflag = models.IntegerField(null=True, db_column='relocationflag', blank=True)  
    jobexecutionid = models.BigIntegerField(null=True, db_column='jobexecutionid', blank=True)  
    vo = models.CharField(max_length=48, db_column='vo', blank=True)  
    pilottiming = models.CharField(max_length=300, db_column='pilottiming', blank=True)  
    workinggroup = models.CharField(max_length=60, db_column='workinggroup', blank=True)  
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True)  
    produsername = models.CharField(max_length=180, db_column='produsername', blank=True)  
    ninputfiles = models.IntegerField(null=True, db_column='ninputfiles', blank=True)  
    countrygroup = models.CharField(max_length=60, db_column='countrygroup', blank=True)  
    batchid = models.CharField(max_length=240, db_column='batchid', blank=True)  
    parentid = models.BigIntegerField(null=True, db_column='parentid', blank=True)  
    specialhandling = models.CharField(max_length=240, db_column='specialhandling', blank=True)  
    jobsetid = models.BigIntegerField(null=True, db_column='jobsetid', blank=True)  
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True)  
    ninputdatafiles = models.IntegerField(null=True, db_column='ninputdatafiles', blank=True)  
    inputfiletype = models.CharField(max_length=96, db_column='inputfiletype', blank=True)  
    inputfileproject = models.CharField(max_length=192, db_column='inputfileproject', blank=True)  
    inputfilebytes = models.BigIntegerField(null=True, db_column='inputfilebytes', blank=True)  
    noutputdatafiles = models.IntegerField(null=True, db_column='noutputdatafiles', blank=True)  
    outputfilebytes = models.BigIntegerField(null=True, db_column='outputfilebytes', blank=True)
    outputfiletype= models.CharField(max_length=32, null=True, db_column='outputfiletype', blank=True)
    jobmetrics = models.CharField(max_length=1500, db_column='jobmetrics', blank=True)  
    workqueue_id = models.IntegerField(null=True, db_column='workqueue_id', blank=True)  
    jeditaskid = models.BigIntegerField(null=True, db_column='jeditaskid', blank=True)  
    actualcorecount = models.IntegerField(null=True, db_column='actualcorecount', blank=True)
    reqid = models.BigIntegerField(null=True, db_column='reqid', blank=True)  
    nucleus = models.CharField(max_length=200, db_column='nucleus', blank=True)  
    jobsubstatus = models.CharField(null=True, max_length=80, db_column='jobsubstatus', blank=True)
    hs06 = models.BigIntegerField(null=True, db_column='hs06', blank=True)  
    maxrss = models.BigIntegerField(null=True, db_column='maxrss', blank=True)  
    maxvmem = models.BigIntegerField(null=True, db_column='maxvmem', blank=True)  
    maxswap = models.BigIntegerField(null=True, db_column='maxswap', blank=True)  
    maxpss = models.BigIntegerField(null=True, db_column='maxpss', blank=True)  
    avgrss = models.BigIntegerField(null=True, db_column='avgrss', blank=True)  
    avgvmem = models.BigIntegerField(null=True, db_column='avgvmem', blank=True)  
    avgswap = models.BigIntegerField(null=True, db_column='avgswap', blank=True)  
    avgpss = models.BigIntegerField(null=True, db_column='avgpss', blank=True)  
    maxwalltime = models.BigIntegerField(null=True, db_column='maxwalltime', blank=True)  
    resourcetype = models.CharField(null=True, max_length=80, db_column='resource_type', blank=True)
    failedattempt = models.IntegerField(null=True, db_column='failedattempt', blank=True)  
    totrchar = models.BigIntegerField(null=True, db_column='totrchar', blank=True)  
    totwchar = models.BigIntegerField(null=True, db_column='totwchar', blank=True)  
    totrbytes = models.BigIntegerField(null=True, db_column='totrbytes', blank=True)  
    totwbytes = models.BigIntegerField(null=True, db_column='totwbytes', blank=True)  
    raterchar = models.BigIntegerField(null=True, db_column='raterchar', blank=True)  
    ratewchar = models.BigIntegerField(null=True, db_column='ratewchar', blank=True)  
    raterbytes = models.BigIntegerField(null=True, db_column='raterbytes', blank=True)  
    ratewbytes = models.BigIntegerField(null=True, db_column='ratewbytes', blank=True)  
    diskio = models.BigIntegerField(null=True, db_column='diskio', blank=True)  
    memoryleak = models.BigIntegerField(null=True, db_column='memory_leak', blank=True)
    memoryleakx2 = models.BigIntegerField(null=True, db_column='memory_leak_x2', blank=True)
    container_name = models.CharField(max_length=765, db_column='container_name', blank=True)
    hs06sec = models.BigIntegerField(null=True, db_column='hs06sec', blank=True)  
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)
    job_label = models.CharField(max_length=20, db_column='job_label', blank=True)
    meancorecount = models.BigIntegerField(null=True, db_column='meancorecount', blank=True)
    gco2_regional = models.BigIntegerField(null=True, db_column='gco2_regional', blank=True)
    gco2_global = models.BigIntegerField(null=True, db_column='gco2_global', blank=True)
    cpu_architecture_level = models.CharField(max_length=20, db_column='cpu_architecture_level', blank=True)

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
        primaryColumns = [
            'pandaid', 'jobdefinitionid', 'creationtime', 'produserid',
            'currentpriority', 'jobstatus', 'modificationtime', 'cloud',
            'destinationsite'
            ]
        secondaryColumns = []
        orderColumns = ORDER_COLUMNS['PanDAjob-all']
        columnTitles = COL_TITLES['PanDAjob-all']
        filterFields = FILTERS['PanDAjob-all']


class Jobsactive4(PandaJob):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobsactive4"'
        app_label = 'panda'


class Jobsarchived(PandaJob):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."jobsarchived"'
        app_label = 'panda'


class Jobsarchived4(PandaJob):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobsarchived4"'
        app_label = 'panda'


class Jobsdefined4(PandaJob):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobsdefined4"'
        app_label = 'panda'

    # __getitem__
    def __getitem__(self, name):
        # return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]


class Jobswaiting4(PandaJob):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobswaiting4"'
        app_label = 'panda'


# ATLARC DB

class PandaJobArch(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='pandaid')  
    creationtime = models.DateTimeField(db_column='creationtime')  
    modificationtime = models.DateTimeField(db_column='modificationtime')  
    modificationhost = models.CharField(max_length=384, db_column='modificationhost', blank=True)  
    transformation = models.CharField(max_length=750, db_column='transformation', blank=True)  
    prodsourcelabel = models.CharField(max_length=60, db_column='prodsourcelabel', blank=True)  
    produserid = models.CharField(max_length=750, db_column='produserid', blank=True)  
    attemptnr = models.IntegerField(db_column='attemptnr')  
    maxattempt = models.IntegerField(db_column='maxattempt')  
    jobstatus = models.CharField(max_length=45, db_column='jobstatus')  
    jobname = models.CharField(max_length=768, db_column='jobname', blank=True)  
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True)  
    endtime = models.DateTimeField(null=True, db_column='endtime', blank=True)  
    cpuconsumptiontime = models.BigIntegerField(db_column='cpuconsumptiontime')  
    cpuconsumptionunit = models.CharField(max_length=384, db_column='cpuconsumptionunit', blank=True)  
    transexitcode = models.CharField(max_length=384, db_column='transexitcode', blank=True)  
    piloterrorcode = models.IntegerField(db_column='piloterrorcode')  
    piloterrordiag = models.CharField(max_length=1500, db_column='piloterrordiag', blank=True)  
    exeerrorcode = models.IntegerField(db_column='exeerrorcode')  
    exeerrordiag = models.CharField(max_length=1500, db_column='exeerrordiag', blank=True)  
    superrorcode = models.IntegerField(db_column='superrorcode')  
    superrordiag = models.CharField(max_length=750, db_column='superrordiag', blank=True)  
    ddmerrorcode = models.IntegerField(db_column='ddmerrorcode')  
    ddmerrordiag = models.CharField(max_length=1500, db_column='ddmerrordiag', blank=True)  
    brokerageerrorcode = models.IntegerField(db_column='brokerageerrorcode')  
    brokerageerrordiag = models.CharField(max_length=750, db_column='brokerageerrordiag', blank=True)  
    jobdispatchererrorcode = models.IntegerField(db_column='jobdispatchererrorcode')  
    jobdispatchererrordiag = models.CharField(max_length=750, db_column='jobdispatchererrordiag', blank=True)  
    taskbuffererrorcode = models.IntegerField(db_column='taskbuffererrorcode')  
    taskbuffererrordiag = models.CharField(max_length=900, db_column='taskbuffererrordiag', blank=True)  
    computingsite = models.CharField(max_length=384, db_column='computingsite', blank=True)  
    computingelement = models.CharField(max_length=384, db_column='computingelement', blank=True)  
    nevents = models.IntegerField(db_column='nevents')  
    taskid = models.IntegerField(null=True, db_column='taskid', blank=True)  
    statechangetime = models.DateTimeField(null=True, db_column='statechangetime', blank=True)  
    pilottiming = models.CharField(max_length=300, db_column='pilottiming', blank=True)  
    workinggroup = models.CharField(max_length=60, db_column='workinggroup', blank=True)  
    processingtype = models.CharField(max_length=192, db_column='processingtype', blank=True)  
    produsername = models.CharField(max_length=180, db_column='produsername', blank=True)  
    parentid = models.BigIntegerField(null=True, db_column='parentid', blank=True)  
    specialhandling = models.CharField(max_length=240, db_column='specialhandling', blank=True)  
    jobsetid = models.BigIntegerField(null=True, db_column='jobsetid', blank=True)  
    jobmetrics = models.CharField(max_length=1500, db_column='jobmetrics', blank=True)  
    jeditaskid = models.BigIntegerField(null=True, db_column='jeditaskid', blank=True)  
    actualcorecount = models.IntegerField(null=True, db_column='actualcorecount', blank=True)
    reqid = models.BigIntegerField(null=True, db_column='reqid', blank=True)  
    nucleus = models.CharField(max_length=200, db_column='nucleus', blank=True)  
    jobsubstatus = models.CharField(null=True, max_length=80, db_column='jobsubstatus', blank=True)
    eventservice = models.IntegerField(null=True, db_column='eventservice', blank=True)  
    hs06 = models.BigIntegerField(null=True, db_column='hs06', blank=True)  
    hs06sec = models.BigIntegerField(null=True, db_column='hs06sec', blank=True)  
    maxpss = models.BigIntegerField(null=True, db_column='maxpss', blank=True)


    class Meta:
        abstract = True


class Jobsarchived_y2014(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2014_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2015(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2015_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2016(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2016_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2017(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2017_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2018(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2018_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2019(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2019_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2020(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2020_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2021(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2021_jobsarchived"'
        app_label = 'pandaarch'