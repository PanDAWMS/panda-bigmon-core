from django.db import models
from django.conf import settings


class HarvesterWorkers(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='harvesterid', null=False, blank=True)
    workerid = models.DecimalField(decimal_places=0, max_digits=11, db_column='workerid', null=False, primary_key=True)
    lastupdate = models.DateTimeField(null=True, db_column='lastupdate', blank=True)
    status = models.CharField(max_length=80, db_column='status', null=False, blank=True)
    batchid = models.CharField(max_length=80, db_column='batchid', null=False, blank=True)
    nodeid = models.CharField(max_length=80, db_column='nodeid', null=False, blank=True)
    queuename = models.CharField(max_length=80, db_column='queuename', null=False, blank=True)
    computingsite = models.CharField(max_length=128, db_column='computingsite', null=False, blank=True)
    submittime = models.DateTimeField(null=True, db_column='submittime', blank=True)
    starttime = models.DateTimeField(null=True, db_column='starttime', blank=True)
    endtime = models.DateTimeField(null=True, db_column='endtime', blank=True)
    ncore = models.DecimalField(decimal_places=0, max_digits=6, db_column='ncore', null=False)
    errorcode = models.DecimalField(decimal_places=0, max_digits=7, db_column='errorcode', null=False)
    stdout = models.CharField(max_length=250, db_column='stdout', null=True, blank=True)
    stderr = models.CharField(max_length=250, db_column='stderr', null=True, blank=True)
    batchlog = models.CharField(max_length=250, db_column='batchlog', null=True, blank=True)
    resourcetype = models.CharField(max_length=56, db_column='resourcetype', null=False, blank=True)
    nativeexitcode = models.IntegerField(db_column='nativeexitcode', null=False)
    nativestatus = models.CharField(max_length=80, db_column='nativestatus', null=False, blank=True)
    diagmessage = models.CharField(max_length=500, db_column='diagmessage', null=False, blank=True)
    computingelement = models.CharField(max_length=128, db_column='computingelement', null=False, blank=True)
    njobs = models.IntegerField( db_column='njobs', null=False)
    jobtype = models.CharField(max_length=128, db_column='jobtype', null=False, blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."harvester_workers"'



class HarvesterDialogs(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='harvester_id', null=False, blank=True)
    diagid = models.IntegerField(db_column='diagid', null=False, blank=True)
    modulename = models.CharField(max_length=100, db_column='modulename')
    identifier = models.CharField(max_length=100, db_column='identifier')
    creationtime = models.DateTimeField(null=True, db_column='creationtime', blank=True)
    messagelevel = models.CharField(max_length=10, db_column='messagelevel')
    diagmessage = models.CharField(max_length=500, db_column='diagmessage')
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."harvester_dialogs"'



class HarvesterWorkerStats(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='harvester_id', null=False, blank=True)
    computingsite = models.CharField(max_length=128, db_column='computingsite', null=False, blank=True)
    resourcetype = models.CharField(max_length=128, db_column='resourcetype', null=False, blank=True)
    status = models.CharField(max_length=80, db_column='status', null=False, blank=True)
    jobtype = models.CharField(max_length=128, db_column='jobtype', null=False, blank=True)
    nworkers = models.IntegerField(db_column='n_workers', null=False)
    lastupdate = models.DateTimeField(null=True, db_column='lastupdate', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."harvester_worker_stats"'



class HarvesterRelJobsWorkers(models.Model):
    harvesterid = models.CharField(max_length=50, db_column='harvesterid', null=False, blank=True)
    workerid = models.DecimalField(decimal_places=0, max_digits=11, db_column='workerid', null=False)
    pandaid = models.DecimalField(decimal_places=0, max_digits=11, db_column='pandaid', null=False, primary_key=True)
    lastupdate = models.DateTimeField(null=True, db_column='lastupdate', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."harvester_rel_jobs_workers"'



class HarvesterSlots(models.Model):
    pandaqueuename = models.CharField(max_length=50, db_column='pandaqueuename', null=False, blank=True)
    gshare = models.CharField(max_length=50, db_column='gshare', null=False, blank=True)
    resourcetype = models.CharField(max_length=50, db_column='resourcetype', blank=True)
    numslots = models.DecimalField(decimal_places=0, max_digits=11, db_column='numslots', null=False)
    modificationtime = models.DateTimeField(null=True, db_column='modificationtime', blank=True)
    expirationtime = models.DateTimeField(null=True, db_column='expirationtime', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."harvester_slots"'
