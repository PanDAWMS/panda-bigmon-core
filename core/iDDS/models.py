"""
iDDS related models
"""
from django.db import models
from django.conf import settings

# for postgres-based installations the iDDS has separate DB, but with Oracle it is just in a separate schema in PanDA DB
# that is why we tip dbrouter which connection to use depending on deployment via app_label
app_label_idds = 'panda'
if 'idds' in settings.DATABASES:
    app_label_idds = 'idds'


class Transforms(models.Model):
    transform_id = models.BigIntegerField(primary_key=True, db_column='transform_id')
    request_id = models.BigIntegerField(null=True, db_column='request_id')
    workload_id = models.BigIntegerField(null=True, db_column='workload_id')
    transform_type = models.SmallIntegerField(db_column='transform_type')
    transform_tag = models.CharField(null=True, max_length=20, db_column='transform_tag')
    priority = models.IntegerField(null=True, db_column='priority')
    safe2get_output_from_input = models.BigIntegerField(null=True, db_column='safe2get_output_from_input')
    status = models.SmallIntegerField(db_column='status')
    substatus = models.SmallIntegerField(null=True, db_column='substatus')
    locking = models.SmallIntegerField(null=True, db_column='locking')
    retries = models.IntegerField(null=True, db_column='retries')
    created_at = models.DateTimeField(db_column='created_at')
    updated_at = models.DateTimeField(db_column='updated_at')
    started_at = models.DateTimeField(null=True,db_column='started_at')
    finished_at = models.DateTimeField(null=True,db_column='finished_at')
    expired_at = models.DateTimeField(null=True, db_column='expired_at')
    transform_metadata = models.TextField(db_column='transform_metadata', blank=True)
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_IDDS}"."transforms"'
        app_label = app_label_idds


class Collections(models.Model):
    coll_id = models.BigIntegerField(primary_key=True, db_column='coll_id')
    coll_type = models.SmallIntegerField(null=True, db_column='coll_type')
    transform_id = models.ForeignKey(Transforms, related_name='transform_id_transforms_fk', on_delete=models.DO_NOTHING, db_column='transform_id')
    relation_type = models.SmallIntegerField(null=True, db_column='relation_type')
    scope = models.CharField(max_length=25, db_column='scope')
    name = models.CharField(max_length=255, db_column='name')
    bytes = models.BigIntegerField(null=True, db_column='bytes')
    status = models.SmallIntegerField(null=True, db_column='status')
    substatus = models.SmallIntegerField(null=True, db_column='substatus')
    locking = models.SmallIntegerField(null=True, db_column='locking')
    total_files = models.BigIntegerField(null=True, db_column='total_files')
    storage_id = models.IntegerField(null=True, db_column='storage_id')
    new_files = models.IntegerField(null=True, db_column='new_files')
    processed_files = models.IntegerField(null=True, db_column='processed_files')
    processing_files = models.IntegerField(null=True, db_column='processing_files')
    processing_id = models.BigIntegerField(null=True, db_column='processing_id')
    retries = models.IntegerField(null=True, db_column='retries')
    created_at = models.DateTimeField(db_column='created_at')
    updated_at = models.DateTimeField(db_column='updated_at')
    accessed_at = models.DateTimeField(null=True, db_column='accessed_at')
    expired_at = models.DateTimeField(null=True, db_column='expired_at')
    coll_metadata = models.TextField(db_column='coll_metadata', blank=True)
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_IDDS}"."collections"'
        app_label = app_label_idds


class Contents(models.Model):
    content_id = models.BigIntegerField(primary_key=True, db_column='content_id')
    coll_id = models.BigIntegerField(db_column='coll_id')
    scope = models.CharField(max_length=25, db_column='scope')
    name = models.CharField(max_length=255, db_column='name')
    min_id = models.IntegerField(db_column='min_id')
    max_id = models.IntegerField(db_column='max_id')
    type = models.SmallIntegerField(db_column='type')
    status = models.SmallIntegerField(null=True, db_column='status')
    substatus = models.SmallIntegerField(null=True, db_column='substatus')
    locking = models.SmallIntegerField(null=True, db_column='locking')
    bytes = models.BigIntegerField(null=True, db_column='bytes')
    md5 = models.CharField(max_length=32, db_column='md5')
    adler32 = models.CharField(max_length=8, db_column='adler32')
    processing_id = models.BigIntegerField(null=True, db_column='processing_id')
    storage_id = models.IntegerField(null=True, db_column='storage_id')
    retries = models.IntegerField(null=True, db_column='retries')
    path = models.CharField(max_length=4000, db_column='path')
    created_at = models.DateTimeField(db_column='created_at')
    updated_at = models.DateTimeField(db_column='updated_at')
    accessed_at = models.DateTimeField(null=True, db_column='accessed_at')
    expired_at = models.DateTimeField(null=True, db_column='expired_at')
    content_metadata = models.TextField(db_column='content_metadata', blank=True)
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_IDDS}"."contents"'
        app_label = app_label_idds


class Processings(models.Model):
    processing_id = models.BigIntegerField(primary_key=True, db_column='processing_id')
    transform_id = models.BigIntegerField(null=True,db_column='transform_id')
    status = models.SmallIntegerField(null=True, db_column='status')
    substatus = models.SmallIntegerField(null=True, db_column='substatus')
    submitter = models.CharField(max_length=20, db_column='submitter')
    submitted_id = models.BigIntegerField(null=True,db_column='submitted_id')
    granularity = models.BigIntegerField(null=True,db_column='granularity')
    granularity_type = models.SmallIntegerField(null=True, db_column='granularity_type')
    created_at = models.DateTimeField(db_column='created_at')
    updated_at = models.DateTimeField(db_column='updated_at')
    submitted_at = models.DateTimeField(db_column='submitted_at')
    finished_at = models.DateTimeField(db_column='finished_at')
    expired_at = models.DateTimeField(db_column='expired_at')
    processing_metadata = models.TextField(db_column='processing_metadata', blank=True)
    output_metadata = models.TextField(db_column='output_metadata', blank=True)
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_IDDS}"."processings"'
        app_label = app_label_idds


class Requests(models.Model):
    request_id = models.BigIntegerField(primary_key=True, db_column='request_id')
    scope = models.CharField(null=True, max_length=25, db_column='scope')
    name = models.CharField(null=True, max_length=255, db_column='name')
    requester = models.CharField(null=True, max_length=20, db_column='requester')
    request_type = models.SmallIntegerField(db_column='request_type')
    transform_tag = models.CharField(null=True, max_length=10, db_column='transform_tag')
    workload_id = models.BigIntegerField(null=True, db_column='workload_id')
    priority = models.IntegerField(null=True, db_column='priority')
    status = models.SmallIntegerField(db_column='status')
    substatus = models.SmallIntegerField(null=True, db_column='substatus')
    locking = models.SmallIntegerField(null=True, db_column='locking')
    created_at = models.DateTimeField(db_column='created_at')
    updated_at = models.DateTimeField(db_column='updated_at')
    accessed_at = models.DateTimeField(null=True, db_column='accessed_at')
    expired_at = models.DateTimeField(null=True, db_column='expired_at')
    errors = models.CharField(null=True, max_length=1024, db_column='errors')
    request_metadata = models.TextField(db_column='request_metadata', blank=True)
    processing_metadata = models.TextField(db_column='processing_metadata', blank=True)
    
    class Meta:
        db_table = f'"{settings.DB_SCHEMA_IDDS}"."requests"'
        app_label = app_label_idds
