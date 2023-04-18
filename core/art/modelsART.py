"""
ART related models
"""

from __future__ import unicode_literals
from django.db import models
from django.conf import settings


class ARTResults(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID')
    pandaid = models.BigIntegerField(db_column='PANDAID')
    result = models.CharField(max_length=2000, db_column='RESULT_JSON', null=True)
    is_task_finished = models.BigIntegerField(db_column='IS_TASK_FINISHED', null=True)
    is_job_finished = models.BigIntegerField(db_column='IS_JOB_FINISHED')
    testname = models.CharField(max_length=300, db_column='TESTNAME', null=True)
    task_flag_updated = models.DateTimeField(null=True, db_column='TASK_FLAG_UPDATED', blank=True)
    job_flag_updated = models.DateTimeField(null=True, db_column='JOB_FLAG_UPDATED', blank=True)
    is_locked = models.IntegerField(db_column='is_locked')
    lock_time = models.DateTimeField(null=True, db_column='lock_time', blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."ART_RESULTS"'


class ARTSubResult(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    subresult = models.CharField(max_length=4000, db_column='SUBRESULT_JSON', null=True)
    result = models.TextField(db_column='RESULT_JSON', blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."ART_SUBRESULT"'


class ARTResultsQueue(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    is_locked = models.IntegerField(db_column='is_locked')
    lock_time = models.DateTimeField(null=True, db_column='lock_time', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."ART_RESULTS_QUEUE"'


class ARTTests(models.Model):
    pandaid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PANDAID', primary_key=True)
    jeditaskid = models.DecimalField(decimal_places=0, max_digits=12, db_column='JEDITASKID')
    testname = models.CharField(max_length=300, db_column='TEST_NAME', null=True)
    nightly_release_short = models.CharField(max_length=24, db_column='NIGHTLY_RELEASE_SHORT', null=True)
    project = models.CharField(max_length=256, db_column='PROJECT', null=True)
    platform = models.CharField(max_length=150, db_column='PLATFORM', null=True)
    nightly_tag = models.CharField(max_length=32, db_column='NIGHTLY_TAG', null=True)
    nightly_tag_display = models.CharField(max_length=32, db_column='NIGHTLY_TAG_DISPLAY', null=True)
    package = models.CharField(max_length=32, db_column='PACKAGE', null=False, blank=True)
    extrainfo = models.CharField(max_length=1000, db_column='EXTRA_INFO', null=True, blank=True)
    created = models.DateTimeField(null=True, db_column='CREATED')

    # subresult = models.OneToOneField('ARTSubResult', related_name='pandaid_sr', on_delete=models.DO_NOTHING, db_column='pandaid')
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."ART_TESTS"'
