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


class DdmEndpoint(models.Model):
    ddm_endpoint_name = models.CharField(max_length=52, db_column='ddm_endpoint_name', primary_key=True)
    site_name = models.CharField(max_length=52, db_column='site_name')
    ddm_spacetoken_name = models.CharField(max_length=52, db_column='ddm_spacetoken_name')
    space_total = models.IntegerField(db_column='space_total', null=True, blank=True)
    space_free = models.IntegerField(db_column='space_free', null=True, blank=True)
    space_used = models.IntegerField(db_column='space_used', null=True, blank=True)
    is_tape = models.CharField(max_length=1, db_column='is_tape')
    type = models.CharField(max_length=20, db_column='type')
    blacklisted = models.CharField(max_length=1, db_column='blacklisted')
    space_expired = models.IntegerField(db_column='space_expired', null=True, blank=True)
    space_timestamp = models.DateField(db_column='space_timestamp')
    blacklisted_read = models.CharField(max_length=1, db_column='blacklisted_read')
    blacklisted_write = models.CharField(max_length=1, db_column='blacklisted_write')
    detailed_status = models.TextField(db_column='detailed_status', blank=True, null=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."ddm_endpoint"'
        app_label = 'panda'
