"""
topology.models -- for Schedconfig and other topology-related objects

"""

from django.db import models
from django.conf import settings

class SchedconfigJson(models.Model):
    pandaqueue = models.CharField(max_length=180, db_column='panda_queue', primary_key=True)
    data = models.TextField(db_column='data', blank=True)
    lastupdate = models.DateField(db_column='last_update')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."schedconfig_json"'
        app_label = 'panda'
