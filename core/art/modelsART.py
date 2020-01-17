from __future__ import unicode_literals

from ..pandajob.columns_config import COLUMNS, ORDER_COLUMNS, COL_TITLES, FILTERS

import json
from django.core.exceptions import ObjectDoesNotExist
from django.db import connection
from django.db import connections
from django.utils import timezone
from django.contrib.auth.models import AbstractUser
#from rest_framework.authtoken.models import Token
#import uuid
#from hashlib import sha1
#import hmac


from django.db import models
models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)




class ARTTask(models.Model):
    art_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ART_ID', primary_key=True)
    nightly_release_short = models.CharField(max_length=24, db_column='NIGHTLY_RELEASE_SHORT', null=True)
    project = models.CharField(max_length=256, db_column='PROJECT', null=True)
    platform = models.CharField(max_length=150, db_column='PLATFORM', null=True)
    nightly_tag = models.CharField(max_length=20, db_column='NIGHTLY_TAG', null=True)
    sequence_tag = models.CharField(max_length=150, db_column='SEQUENCE_TAG', null=False)
    package = models.CharField(max_length=32, db_column='PACKAGE', null=False, blank=True)
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASK_ID')
    class Meta:
        db_table = u'"ATLAS_DEFT"."T_ART"'

class ARTTest(models.Model):
    test_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TEST_ID', primary_key=True)
    test_index = models.DecimalField(decimal_places=0, max_digits=12, db_column='TEST_INDEX')
    test_name = models.CharField(max_length=24, db_column='TEST_NAME', null=True)
    art_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ART_ID')
    class Meta:
        db_table = u'"ATLAS_DEFT"."T_ART_TEST_IN_TASKS"'


class ARTTasks(models.Model):
    package = models.CharField(max_length=32, db_column='PACKAGE', null=False, blank=True)
    branch = models.CharField(max_length=150, db_column='BRANCH', null=True)
    ntag = models.DateTimeField(db_column='NTAG', null=True)
    nightly_tag = models.CharField(max_length=20, db_column='NIGHTLY_TAG', null=True)
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASK_ID')
    nfilesfinished = models.DecimalField(decimal_places=0, max_digits=12, db_column='NFILESFINISHED')
    nfilesfailed = models.DecimalField(decimal_places=0, max_digits=12, db_column='NFILESFAILED')
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."ARTTasks"'

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
        db_table = u'"ATLAS_PANDABIGMON"."ART_RESULTS"'


class ARTSubResult(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    subresult = models.CharField(max_length=4000, db_column='SUBRESULT_JSON', null=True)

    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."ART_SUBRESULT"'


class ARTResultsQueue(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    is_locked = models.IntegerField(db_column='is_locked')
    lock_time = models.DateTimeField(null=True, db_column='lock_time', blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."ART_RESULTS_QUEUE"'


class ARTTests(models.Model):
    pandaid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PANDAID', primary_key=True)
    jeditaskid = models.DecimalField(decimal_places=0, max_digits=12, db_column='JEDITASKID')
    testname = models.CharField(max_length=300, db_column='TEST_NAME', null=True)
    nightly_release_short = models.CharField(max_length=24, db_column='NIGHTLY_RELEASE_SHORT', null=True)
    project = models.CharField(max_length=256, db_column='PROJECT', null=True)
    platform = models.CharField(max_length=150, db_column='PLATFORM', null=True)
    nightly_tag = models.CharField(max_length=20, db_column='NIGHTLY_TAG', null=True)
    package = models.CharField(max_length=32, db_column='PACKAGE', null=False, blank=True)
    extrainfo = models.CharField(max_length=1000, db_column='EXTRA_INFO', null=True, blank=True)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."ART_TESTS"'


class ReportEmails(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ID', primary_key=True)
    report = models.CharField(max_length=150, db_column='REPORT', null=False)
    type = models.CharField(max_length=150, db_column='TYPE', null=False)
    email = models.CharField(max_length=256, db_column='EMAIL', null=False)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."REPORT_EMAIL"'