# Create your models here.
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# into your database.
from __future__ import unicode_literals

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


class Jobsarchived_y2022(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2022_jobsarchived"'
        app_label = 'pandaarch'


class Jobsarchived_y2023(PandaJobArch):
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."y2023_jobsarchived"'
        app_label = 'pandaarch'

class ErrorDescription(models.Model):
    """
    ErrorDescription model
    """
    id = models.AutoField(primary_key=True, db_column='id')
    component = models.CharField(max_length=32, db_column='component', blank=False, null=False)
    code = models.IntegerField(db_column='code', blank=False, null=False)
    acronym = models.CharField(max_length=64, db_column='acronym', blank=True, null=True)
    diagnostics = models.CharField(max_length=255, db_column='diagnostics', blank=True, null=True)
    description = models.CharField(max_length=4000, db_column='description', blank=True, null=True)
    category = models.IntegerField(db_column='category', blank=True, null=True)

    class Meta:
        managed = False
        app_label = 'panda'
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."error_descriptions"'
        unique_together = (('component', 'code'),)


class PandaDBVersion(models.Model):
    """Version of PanDA DB"""
    component = models.CharField(db_column='component', max_length=100, primary_key=True)
    major = models.IntegerField(db_column='major')
    minor = models.IntegerField(db_column='minor')
    patch = models.IntegerField(db_column='patch')
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."pandadb_version"'
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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."filestable4"'
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
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."filestable_arch"'
        unique_together = ('row_id', 'modificationtime')
        app_label = 'panda'


class Jobparamstable(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jobparameters = models.TextField(db_column='jobparameters', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobparamstable"'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class JobparamstableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid')
    modificationtime = models.DateTimeField(db_column='modificationtime')
    jobparameters = models.TextField(db_column='jobparameters', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."jobparamstable_arch"'
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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobs_statuslog"'
        app_label = 'panda'


class Jobsdebug(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='pandaid')
    stdout = models.CharField(max_length=6144, db_column='stdout', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobsdebug"'
        app_label = 'panda'


class Metatable(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    metadata = models.TextField(db_column='metadata', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."metatable"'
        unique_together = ('pandaid', 'modificationtime')
        app_label = 'panda'


class MetatableArch(models.Model):
    pandaid = models.BigIntegerField(db_column='pandaid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    metadata = models.TextField(db_column='metadata', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA_ARCH}"."metatable_arch"'
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
        db_table = f'"{settings.DB_SCHEMA_PANDA_META}"."users"'
        app_label = 'panda'

    def __str__(self):
        return 'User: ' + str(self.name) + '[' + str(self.status) + ']'


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
        db_table = f'"{settings.DB_SCHEMA_PANDA_META}"."sitedata"'
        unique_together = ('site', 'flag', 'hours')
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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."datasets"'
        unique_together = ('vuid', 'modificationdate')
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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_dataset_contents"'
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
    nfileswaiting = models.IntegerField(null=True, db_column='nfileswaiting', blank=True)
    nfilesmissing = models.IntegerField(null=True, db_column='nfilesmissing', blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_datasets"'
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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_events"'
        unique_together = ('jeditaskid', 'pandaid', 'fileid', 'job_processid')
        app_label = 'jedi'


class JediDatasetLocality(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    datasetid = models.BigIntegerField(db_column='datasetid')
    rse = models.CharField(max_length=1000, db_column='rse', blank=True)
    timestamp = models.DateTimeField(db_column='timestamp')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_dataset_locality"'
        unique_together = ('jeditaskid', 'datasetid', 'rse')
        app_label = 'jedi'


class JediJobRetryHistory(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    oldpandaid = models.BigIntegerField(db_column='oldpandaid')
    newpandaid = models.BigIntegerField(db_column='newpandaid')
    ins_utc_tstamp = models.BigIntegerField(db_column='ins_utc_tstamp', blank=True)
    relationtype = models.CharField(max_length=48, db_column='relationtype')
    originpandaid = models.BigIntegerField(db_column='originpandaid')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_job_retry_history"'
        unique_together = ('jeditaskid', 'oldpandaid', 'newpandaid')
        app_label = 'jedi'


class JediTaskparams(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='jeditaskid')
    taskparams = models.TextField(db_column='taskparams', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_taskparams"'
        app_label = 'jedi'


class JediTasksBase(models.Model):
    jeditaskid = models.BigIntegerField(primary_key=True, db_column='jeditaskid')
    taskname = models.CharField(max_length=384, db_column='taskname', blank=True)
    status = models.CharField(max_length=192, db_column='status')
    username = models.CharField(max_length=384, db_column='username')
    creationdate = models.DateTimeField(db_column='creationdate')
    # modificationtime = models.DateTimeField(db_column='modificationtime')  # use realmodificationtime instead
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
    memoryleakcore = models.BigIntegerField(null=True, db_column='memory_leak_core', blank=True)
    memoryleakx2 = models.BigIntegerField(null=True, db_column='memory_leak_x2', blank=True)
    modificationtime = models.DateTimeField(db_column='realmodificationtime')
    framework = models.CharField(max_length=100, db_column='framework', blank=True)

    def get_fields_by_type(self, ftype='integer'):
        field_list = [str(f.name) for f in self._meta.fields if ftype in str(f.description).lower()]
        return field_list

    class Meta:
        abstract = True


class JediTasks(JediTasksBase):

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_tasks"'
        app_label = 'jedi'


class GetEventsForTask(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    totevrem = models.BigIntegerField(db_column='totevrem')
    totev = models.BigIntegerField(db_column='totev')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."geteventsfortask"'
        app_label = 'pandamon'


class TasksStatusLog(models.Model):
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    modificationtime = models.DateTimeField(db_column='modificationtime')
    modificationhost = models.CharField(max_length=384, db_column='modificationhost', blank=True)
    status = models.CharField(max_length=64, db_column='status', blank=True)
    attemptnr = models.IntegerField(db_column='attemptnr', blank=True)
    reason = models.CharField(max_length=600, db_column='reason', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."tasks_statuslog"'
        app_label = 'jedi'


class TaskAttempts(models.Model):
    """
    Task attempts
    """
    jeditaskid = models.BigIntegerField(db_column='jeditaskid', primary_key=True)
    attemptnr = models.IntegerField(db_column='attemptnr', primary_key=True)
    starttime = models.DateTimeField(db_column='starttime', blank=True)
    endtime = models.DateTimeField(db_column='endtime', blank=True)
    startstatus = models.CharField(max_length=32, db_column='startstatus', blank=True)
    endstatus = models.CharField(max_length=32, db_column='endstatus', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."task_attempts"'
        app_label = 'jedi'


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
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jedi_work_queue"'
        app_label = 'jedi'


class Metrics(models.Model):
    computingsite = models.CharField(db_column='computingsite')
    gshare = models.CharField(db_column='gshare')
    metric = models.CharField(db_column='metric')
    json = models.TextField(db_column='value_json', blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."metrics"'
        app_label = 'panda'
        unique_together = ('computingsite', 'gshare')

class TProject(models.Model):
    project = models.CharField(max_length=60, db_column='project', primary_key=True)
    begin_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='begin_time')
    end_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='end_time')
    status = models.CharField(max_length=8, db_column='status')
    status = models.CharField(max_length=500, db_column='description')
    time_stamp = models.DecimalField(decimal_places=0, max_digits=10, db_column='timestamp')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_DEFT}"."t_projects"'
        app_label = 'deft'

class TRequest(models.Model):
    reqid = models.DecimalField(decimal_places=0, max_digits=12, db_column='pr_id', primary_key=True)
    manager = models.CharField(max_length=32, db_column='manager', null=False, blank=True)
    description = models.CharField(max_length=256, db_column='description', null=True, blank=True)
    ref_link = models.CharField(max_length=256, db_column='reference_link', null=True, blank=True)
    cstatus = models.CharField(max_length=32, db_column='status', null=False, blank=True)
    provenance = models.CharField(max_length=32, db_column='provenance', null=False, blank=True,)
    request_type = models.CharField(max_length=32, db_column='request_type', null=False, blank=True)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)
    subcampaign = models.CharField(max_length=32, db_column='sub_campaign', null=False, blank=True)
    phys_group = models.CharField(max_length=20, db_column='phys_group', null=False, blank=True)
    energy_gev = models.DecimalField(decimal_places=0, max_digits=8, db_column='energy_gev', null=False, blank=True)
    project = models.ForeignKey(TProject, db_column='project', null=True, blank=False, on_delete=models.DO_NOTHING)
    is_error = models.BooleanField(db_column='exception', null=True, blank=False)
    reference = models.CharField(max_length=50, db_column='reference', null=True, blank=True)
    reference_link = models.CharField(max_length=50, db_column='reference_link', null=True, blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_DEFT}"."t_prodmanager_request"'
        app_label = 'deft'


class ResourceTypes(models.Model):
    resource_name= models.CharField(max_length=56, db_column='resource_name', primary_key=True)
    mincore = models.IntegerField(db_column='mincore')
    maxcore = models.IntegerField(db_column='maxcore')
    minrampercore = models.IntegerField(db_column='minrampercore')
    maxrampercore = models.IntegerField(db_column='maxrampercore')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."resource_types"'
        app_label = 'panda'
