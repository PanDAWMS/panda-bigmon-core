from django.db import models

class MLFlowContainers(models.Model):
    entryid = models.AutoField( db_column='ENTRYID', primary_key=True)
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    instanceurl = models.CharField(max_length=10, db_column='INSTANCEURL')
    status = models.CharField(max_length=35, db_column='STATUS')
    spinnedAt = models.DateTimeField(null=True, db_column='SPINNED_AT', blank=True)
    deletedAt = models.DateTimeField(null=True, db_column='DELETED_AT', blank=True)
    errorAt = models.DateTimeField(null=True, db_column='ERROR_AT', blank=True)
    class Meta:
        db_table = u'MLFLOW_CONTAINERS'

    # We assume the following status of containers:
    # - active
    # - spinning
    # - error
    # - deleted
