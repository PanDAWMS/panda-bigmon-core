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


class Transforms(models.Model):
    transform_id = models.BigIntegerField(primary_key=True, null=True, db_column='TRANSFORM_ID')
    transform_type = models.SmallIntegerField(db_column='TRANSFORM_TYPE')
    transform_tag = models.CharField(null=True, max_length=20, db_column='TRANSFORM_TAG')
    priority = models.IntegerField(null=True, db_column='PRIORITY')
    safe2get_output_from_input = models.BigIntegerField(null=True, db_column='SAFE2GET_OUTPUT_FROM_INPUT')
    status = models.SmallIntegerField(db_column='STATUS')
    substatus = models.SmallIntegerField(null=True, db_column='SUBSTATUS')
    locking = models.SmallIntegerField(null=True, db_column='LOCKING')
    retries = models.IntegerField(null=True, db_column='RETRIES')
    created_at = models.DateTimeField(db_column='CREATED_AT')
    updated_at = models.DateTimeField(db_column='UPDATED_AT')
    started_at = models.DateTimeField(null=True,db_column='STARTED_AT')
    finished_at = models.DateTimeField(null=True,db_column='FINISHED_AT')
    expired_at = models.DateTimeField(null=True, db_column='EXPIRED_AT')
    transform_metadata = models.TextField(db_column='TRANSFORM_METADATA', blank=True)
    class Meta:
        db_table = u'"ATLAS_IDDS"."TRANSFORMS"'
        app_label = 'idds_intr'


class Collections(models.Model):
    coll_id = models.BigIntegerField(primary_key=True, db_column='COLL_ID')
    coll_type = models.SmallIntegerField(null=True, db_column='COLL_TYPE')
    transform_id = models.ForeignKey(Transforms, related_name='transform_id', on_delete=models.DO_NOTHING, db_column='TRANSFORM_ID')
    relation_type = models.SmallIntegerField(null=True, db_column='RELATION_TYPE')
    scope = models.CharField(max_length=25, db_column='SCOPE')
    name = models.CharField(max_length=255, db_column='NAME')
    bytes = models.BigIntegerField(null=True, db_column='BYTES')
    status = models.SmallIntegerField(null=True, db_column='STATUS')
    substatus = models.SmallIntegerField(null=True, db_column='SUBSTATUS')
    locking = models.SmallIntegerField(null=True, db_column='LOCKING')
    total_files = models.BigIntegerField(null=True, db_column='TOTAL_FILES')
    storage_id = models.IntegerField(null=True, db_column='STORAGE_ID')
    new_files = models.IntegerField(null=True, db_column='NEW_FILES')
    processed_files = models.IntegerField(null=True, db_column='PROCESSED_FILES')
    processing_files = models.IntegerField(null=True, db_column='PROCESSING_FILES')
    processing_id = models.BigIntegerField(null=True, db_column='PROCESSING_ID')
    retries = models.IntegerField(null=True, db_column='RETRIES')
    created_at = models.DateTimeField(db_column='CREATED_AT')
    updated_at = models.DateTimeField(db_column='UPDATED_AT')
    accessed_at = models.DateTimeField(null=True, db_column='ACCESSED_AT')
    expired_at = models.DateTimeField(null=True, db_column='EXPIRED_AT')
    coll_metadata = models.TextField(db_column='COLL_METADATA', blank=True)
    class Meta:
        db_table = u'"ATLAS_IDDS"."COLLECTIONS"'
        app_label = 'idds_intr'


class Contents(models.Model):
    content_id = models.BigIntegerField(primary_key=True, db_column='CONTENT_ID')
    coll_id = models.BigIntegerField(db_column='COLL_ID')
    scope = models.CharField(max_length=25, db_column='SCOPE')
    name = models.CharField(max_length=255, db_column='NAME')
    min_id = models.IntegerField(db_column='MIN_ID')
    max_id = models.IntegerField(db_column='MIN_ID')
    type = models.SmallIntegerField(db_column='TYPE')
    status = models.SmallIntegerField(null=True, db_column='STATUS')
    substatus = models.SmallIntegerField(null=True, db_column='SUBSTATUS')
    locking = models.SmallIntegerField(null=True, db_column='LOCKING')
    bytes = models.BigIntegerField(null=True, db_column='BYTES')
    md5 = models.CharField(max_length=32, db_column='MD5')
    adler32 = models.CharField(max_length=8, db_column='ADLER32')
    processing_id = models.BigIntegerField(null=True, db_column='PROCESSING_ID')
    storage_id = models.IntegerField(null=True, db_column='STORAGE_ID')
    retries = models.IntegerField(null=True, db_column='RETRIES')
    path = models.CharField(max_length=4000, db_column='PATH')
    created_at = models.DateTimeField(db_column='CREATED_AT')
    updated_at = models.DateTimeField(db_column='UPDATED_AT')
    accessed_at = models.DateTimeField(null=True, db_column='ACCESSED_AT')
    expired_at = models.DateTimeField(null=True, db_column='EXPIRED_AT')
    content_metadata = models.TextField(db_column='content_metadata', blank=True)
    class Meta:
        db_table = u'"ATLAS_IDDS"."CONTENTS"'
        app_label = 'idds_intr'


class Processings(models.Model):
    processing_id = models.BigIntegerField(primary_key=True, null=True, db_column='PROCESSING_ID')
    transform_id = models.BigIntegerField(null=True,db_column='TRANSFORM_ID')
    status = models.SmallIntegerField(null=True, db_column='STATUS')
    substatus = models.SmallIntegerField(null=True, db_column='SUBSTATUS')
    submitter = models.CharField(max_length=20, db_column='SUBMITTER')
    submitted_id = models.BigIntegerField(null=True,db_column='SUBMITTED_ID')
    granularity = models.BigIntegerField(null=True,db_column='GRANULARITY')
    granularity_type = models.SmallIntegerField(null=True, db_column='GRANULARITY_TYPE')
    created_at = models.DateTimeField(db_column='CREATED_AT')
    updated_at = models.DateTimeField(db_column='UPDATED_AT')
    submitted_at = models.DateTimeField(db_column='SUBMITTED_AT')
    finished_at = models.DateTimeField(db_column='FINISHED_AT')
    expired_at = models.DateTimeField(db_column='EXPIRED_AT')
    processing_metadata = models.TextField(db_column='PROCESSING_METADATA', blank=True)
    output_metadata = models.TextField(db_column='OUTPUT_METADATA', blank=True)
    class Meta:
        db_table = u'"ATLAS_IDDS"."PROCESSINGS"'
        app_label = 'idds_intr'


class Requests(models.Model):
    request_id = models.BigIntegerField(primary_key=True, null=True, db_column='REQUEST_ID')
    scope = models.CharField(null=True, max_length=25, db_column='SCOPE')
    name = models.CharField(null=True, max_length=255, db_column='NAME')
    requester = models.CharField(null=True, max_length=20, db_column='REQUESTER')
    request_type = models.SmallIntegerField(db_column='REQUEST_TYPE')
    transform_tag = models.CharField(null=True, max_length=10, db_column='TRANSFORM_TAG')
    workload_id = models.BigIntegerField(null=True, db_column='WORKLOAD_ID')
    priority = models.IntegerField(null=True, db_column='PRIORITY')
    status = models.SmallIntegerField(db_column='STATUS')
    substatus = models.SmallIntegerField(null=True, db_column='SUBSTATUS')
    locking = models.SmallIntegerField(null=True, db_column='LOCKING')
    created_at = models.DateTimeField(db_column='CREATED_AT')
    updated_at = models.DateTimeField(db_column='UPDATED_AT')
    accessed_at = models.DateTimeField(null=True, db_column='ACCESSED_AT')
    expired_at = models.DateTimeField(null=True, db_column='EXPIRED_AT')
    errors = models.CharField(null=True, max_length=1024, db_column='ERRORS')
    request_metadata = models.TextField(db_column='REQUEST_METADATA', blank=True)
    processing_metadata = models.TextField(db_column='PROCESSING_METADATA', blank=True)
    class Meta:
        db_table = u'"ATLAS_IDDS"."REQUESTS"'
        app_label = 'idds_intr'



class Req2transforms(models.Model):
    request_id_fk = models.ForeignKey(Requests, related_name='request_id', on_delete=models.DO_NOTHING, db_column='REQUEST_ID')
    transform_id_fk = models.ForeignKey(Transforms, related_name='transform_id', on_delete=models.DO_NOTHING, db_column='TRANSFORM_ID')
    class Meta:
        db_table = u'"ATLAS_IDDS"."REQ2TRANSFORMS"'
        app_label = 'idds_intr'
        unique_together = (('request_id_fk', 'transform_id_fk'),)
        managed = False



