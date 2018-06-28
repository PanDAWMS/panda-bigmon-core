from django.db import models
models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)

class HarvesterRelJobsWorkers(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='HARVESTERID', null=False, blank=True)
    workerid = models.DecimalField(decimal_places=0, max_digits=11, db_column='WORKERID', null=False)
    lastupdate = models.DateTimeField(null=True, db_column='LASTUPDATE', blank=True)
    pandaid = models.IntegerField(null=True, db_column='PANDAID', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDA"."HARVESTER_REL_JOBS_WORKERS"'

class HarvesterWorkers(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='HARVESTERID', null=False, blank=True)
    workerid = models.DecimalField(decimal_places=0, max_digits=11, db_column='WORKERID', null=False)
    lastupdate = models.DateTimeField(null=True, db_column='LASTUPDATE', blank=True)
    status = models.CharField(max_length=80, db_column='STATUS', null=False, blank=True)
    batchid = models.CharField(max_length=80, db_column='BATCHID', null=False, blank=True)
    nodeid = models.CharField(max_length=80, db_column='NODEID', null=False, blank=True)
    queuename = models.CharField(max_length=80, db_column='QUEUENAME', null=False, blank=True)
    computingsite = models.CharField(max_length=128, db_column='COMPUTINGSITE', null=False, blank=True)
    submittime = models.DateTimeField(null=True, db_column='SUBMITTIME', blank=True)
    starttime = models.DateTimeField(null=True, db_column='STARTTIME', blank=True)
    endtime = models.DateTimeField(null=True, db_column='ENDTIME', blank=True)
    ncore = models.DecimalField(decimal_places=0, max_digits=6, db_column='NCORE', null=False)
    errorcode = models.DecimalField(decimal_places=0, max_digits=7, db_column='ERRORCODE', null=False)
    stdout = models.CharField(max_length=250, db_column='STDOUT', null=True, blank=True)
    stderr = models.CharField(max_length=250, db_column='STDERR', null=True, blank=True)
    batchlog = models.CharField(max_length=250, db_column='BATCHLOG', null=True, blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDA"."HARVESTER_WORKERS"'

class HarvesterDialogs (models.Model):
    harvesterid = models.CharField(max_length=50, db_column='HARVESTER_ID', null=False, blank=True)
    diagid = models.IntegerField(db_column='DIAGID', null=False, blank=True)
    modulename = models.CharField(max_length=100, db_column='MODULENAME')
    identifier = models.CharField(max_length=100, db_column='IDENTIFIER')
    creationtime = models.DateTimeField(null=True, db_column='CREATIONTIME', blank=True)
    messagelevel = models.CharField(max_length=10, db_column='MESSAGELEVEL')
    diagmessage = models.CharField(max_length=500, db_column='DIAGMESSAGE')
    class Meta:
        db_table = u'"ATLAS_PANDA"."HARVESTER_DIALOGS"'
class HarvesterWorkerStats (models.Model):
    harvesterid = models.CharField(max_length=50, db_column='HARVESTER_ID', null=False, blank=True)
    computingsite = models.CharField(max_length=128, db_column='COMPUTINGSITE', null=False, blank=True)
    resourcetype = models.CharField(max_length=128, db_column='RESOURCETYPE', null=False, blank=True)
    status = models.CharField(max_length=80, db_column='STATUS', null=False, blank=True)
    nworkers = models.IntegerField(db_column='N_WORKERS', null=False)
    lastupdate = models.DateTimeField(null=True, db_column='LASTUPDATE', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDA"."HARVESTER_WORKER_STATS"'

class HarvesterRelJobsWorkers (models.Model):
    harvesterid = models.CharField(max_length=50, db_column='HARVESTERID', null=False, blank=True)
    workerid = models.DecimalField(decimal_places=0, max_digits=11, db_column='WORKERID', null=False)
    pandaid = models.DecimalField(decimal_places=0, max_digits=11, db_column='PANDAID', null=False)
    lastupdate = models.DateTimeField(null=True, db_column='LASTUPDATE', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDA"."HARVESTER_REL_JOBS_WORKERS"'