from django.db import models
from django.conf import settings


class GlobalSharesModel(models.Model):
    name = models.CharField(max_length=32, db_column='name', null=False)
    value = models.IntegerField(db_column='value', null=False)
    parent = models.CharField(max_length=32, db_column='parent', null=True, blank=False)
    prodsourcelabel = models.CharField(max_length=100, db_column='prodsourcelabel', null=True, blank=True)
    workinggroup = models.CharField(max_length=100, db_column='workinggroup', null=True, blank=True)
    campaign = models.CharField(max_length=100, db_column='campaign', null=True, blank=True)
    processingtype = models.CharField(max_length=100, db_column='processingtype', null=True, blank=True)
    vo = models.CharField(max_length=32, db_column='vo', null=True, blank=True)
    queueid = models.IntegerField(db_column='queue_id', null=False, blank=True, primary_key=True)
    throttled = models.CharField(max_length=1, db_column='throttled', null=True, blank=True)
    transpath = models.CharField(max_length=128, db_column='transpath', null=True, blank=True)
    rtype = models.CharField(max_length=16, db_column='rtype', null=True, blank=True)

    class Meta:
        app_label = 'panda'
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."global_shares"'


class JobsShareStats(models.Model):
    ts = models.DateTimeField(db_column='ts', null=True, blank=True)
    gshare = models.CharField(max_length=32, db_column='gshare', null=True)
    computingsite = models.CharField(max_length=128, db_column='computingsite', null=True)
    jobstatus = models.CharField(max_length=15, db_column='jobstatus', null=False)
    maxpriority = models.IntegerField(db_column='maxpriority', null=True)
    njobs = models.IntegerField(db_column='njobs', null=True)
    hs = models.DecimalField(db_column='hs', max_digits=5, null=True)
    vo = models.CharField(max_length=32, db_column='vo', null=True)
    workqueueid = models.IntegerField(db_column='workqueue_id', null=False)
    resourcetype = models.CharField(max_length=16, db_column='resource_type', null=True)

    class Meta:
        app_label = 'panda'
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."jobs_share_stats"'