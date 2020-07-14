from django.db import models

class MLFlowContainers(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    urlpath = models.CharField(max_length=10, db_column='URLPATH')
    status = models.CharField(max_length=35, db_column='STATUS')
    spinnedAt = models.DateTimeField(null=True, db_column='SPINNED_AT', blank=True)
    deletedAt = models.DateTimeField(null=True, db_column='DELETED_AT', blank=True)
    errorAt = models.DateTimeField(null=True, db_column='ERROR_AT', blank=True)
    entryid = models.DecimalField(decimal_places=0, max_digits=12, db_column='ENTRYID', primary_key=True)
    class Meta:
        db_table = u'MLFLOW_CONTAINERS'

    # We assume the following status of containers:
    # - data_downloading
    # - data_pushing
    # - registering_configuration_map
    # - spinning_container
    # - container_active
    # - deleted
